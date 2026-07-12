package sync

import (
	"fmt"
	"sync"
	"time"
)

const (
	// BaseSyncTolerance is the floor for local / low-latency links.
	BaseSyncTolerance = 120 * time.Millisecond
	// NetworkMargin covers clock-offset estimation noise and mpv read jitter.
	NetworkMargin = 80 * time.Millisecond
	// MinTicksBeforeCorrect requires sustained drift, not a single noisy sample.
	MinTicksBeforeCorrect = 6 // 6 × 50ms tick = 300ms sustained
)

const MaxSeekDelta = 2 * time.Second

type PeerPosition struct {
	PeerID    string
	Position  time.Duration
	Paused    bool
	SampledAt time.Time
}

type CorrectionType uint8

const (
	CorrectionNone CorrectionType = iota
	CorrectionMicroSeek
	CorrectionHardResync
	CorrectionPause
	CorrectionResume
)

func (c CorrectionType) String() string {
	switch c {
	case CorrectionNone:
		return "none"
	case CorrectionMicroSeek:
		return "micro-seek"
	case CorrectionHardResync:
		return "hard-resync"
	case CorrectionPause:
		return "pause"
	case CorrectionResume:
		return "resume"
	default:
		return "unknown"
	}
}

type Correction struct {
	Type      CorrectionType
	TargetPos time.Duration
	Delta     time.Duration
	PeerID    string
	EmittedAt time.Time
}

type PositionFunc func() (pos time.Duration, paused bool, err error)

type peerDriftState struct {
	rtt             time.Duration
	consecutiveOver int
}

type DeltaEngine struct {
	isHost   bool
	localID  string
	getPos   PositionFunc
	tickRate time.Duration

	mu          sync.RWMutex
	peers       map[string]*PeerPosition
	drift       map[string]*peerDriftState
	hostID      string
	lastCorrect time.Time
	cooldown    time.Duration

	CorrectionCh chan Correction
	stopCh       chan struct{}
}

func NewDeltaEngine(localID, hostID string, isHost bool, getPos PositionFunc) *DeltaEngine {
	return &DeltaEngine{
		isHost:       isHost,
		localID:      localID,
		hostID:       hostID,
		getPos:       getPos,
		tickRate:     50 * time.Millisecond,
		peers:        make(map[string]*PeerPosition),
		drift:        make(map[string]*peerDriftState),
		cooldown:     1200 * time.Millisecond, // was 150ms — gave the feedback loop no time to settle
		CorrectionCh: make(chan Correction, 32),
		stopCh:       make(chan struct{}),
	}
}

func (e *DeltaEngine) Start() {
	go e.loop()
}

func (e *DeltaEngine) Stop() {
	close(e.stopCh)
}

func (e *DeltaEngine) UpdatePeer(p PeerPosition) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.peers[p.PeerID] = &p
}

func (e *DeltaEngine) RemovePeer(peerID string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	delete(e.peers, peerID)
	delete(e.drift, peerID)
}

func (e *DeltaEngine) SetPeerRTT(peerID string, rtt time.Duration) {
	if rtt <= 0 {
		return
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	ds := e.drift[peerID]
	if ds == nil {
		ds = &peerDriftState{}
		e.drift[peerID] = ds
	}
	ds.rtt = rtt
}

func (e *DeltaEngine) Deltas() map[string]time.Duration {
	localPos, _, err := e.getPos()
	if err != nil {
		return nil
	}

	e.mu.RLock()
	defer e.mu.RUnlock()

	out := make(map[string]time.Duration, len(e.peers))
	for id, p := range e.peers {
		estimated := estimatedPos(p)
		out[id] = localPos - estimated
	}
	return out
}

func (e *DeltaEngine) loop() {
	ticker := time.NewTicker(e.tickRate)
	defer ticker.Stop()

	for {
		select {
		case <-e.stopCh:
			return
		case <-ticker.C:
			e.tick()
		}
	}
}

// tick runs the SAME symmetric sync logic regardless of host/peer role.
// Whichever side changes state (pause/resume/seek), the other follows.
// This replaces the old asymmetric tickHost/tickPeer split.
func (e *DeltaEngine) tick() {
	localPos, _, err := e.getPos()
	if err != nil {
		return
	}

	if time.Since(e.lastCorrect) < e.cooldown {
		return
	}

	// Full lock, not RLock: this function creates/mutates entries in
	// e.drift, so it needs exclusive access, not just read access.
	e.mu.Lock()
	defer e.mu.Unlock()

	for id, p := range e.peers {
		// Skip peers we have no real sample for yet (avoids the
		// zero-SampledAt bug producing a multi-year "delta").
		if p.SampledAt.IsZero() {
			continue
		}

		estimated := estimatedPos(p)
		delta := localPos - estimated
		absDelta := abs(delta)
		tolerance := e.syncTolerance(id)

		ds := e.drift[id]
		if ds == nil {
			ds = &peerDriftState{}
			e.drift[id] = ds
		}

		if absDelta <= tolerance {
			ds.consecutiveOver = 0
			continue
		}

		ds.consecutiveOver++
		if ds.consecutiveOver < MinTicksBeforeCorrect {
			continue
		}
		ds.consecutiveOver = 0

		switch {
		case absDelta > MaxSeekDelta:
			e.emit(Correction{
				Type:      CorrectionHardResync,
				TargetPos: estimated,
				Delta:     delta,
				PeerID:    id,
				EmittedAt: time.Now(),
			})

		default:
			target := estimated + (delta / 2)
			e.emit(Correction{
				Type:      CorrectionMicroSeek,
				TargetPos: target,
				Delta:     delta,
				PeerID:    id,
				EmittedAt: time.Now(),
			})
		}

		// Pause/resume is synced via explicit MsgPause/MsgPlay messages
		// (edge-triggered in broadcastState). Doing it here caused a feedback
		// loop: the side that paused locally still had stale peer state showing
		// playing, so it emitted CorrectionResume and fought its own pause.
	}
}

// syncTolerance scales with observed RTT: one-way transit alone can be
// rtt/2, and clock-offset jitter adds more. Fixed 120ms thresholds were
// routinely exceeded on 250-400ms links, causing spurious micro-seeks.
func (e *DeltaEngine) syncTolerance(peerID string) time.Duration {
	ds := e.drift[peerID]
	if ds == nil || ds.rtt <= 0 {
		return BaseSyncTolerance
	}
	adaptive := ds.rtt/2 + NetworkMargin
	if adaptive < BaseSyncTolerance {
		return BaseSyncTolerance
	}
	return adaptive
}

func (e *DeltaEngine) emit(c Correction) {
	e.lastCorrect = time.Now()
	select {
	case e.CorrectionCh <- c:
	default:
	}
}

type SyncReport struct {
	LocalPos time.Duration
	Peers    []PeerSyncStatus
}

type PeerSyncStatus struct {
	PeerID    string
	Delta     time.Duration
	Synced    bool
	Buffering bool
}

func (e *DeltaEngine) Report() (SyncReport, error) {
	localPos, _, err := e.getPos()
	if err != nil {
		return SyncReport{}, fmt.Errorf("delta: get local pos: %w", err)
	}

	e.mu.RLock()
	defer e.mu.RUnlock()

	report := SyncReport{LocalPos: localPos}
	for id, p := range e.peers {
		estimated := estimatedPos(p)
		delta := localPos - estimated
		report.Peers = append(report.Peers, PeerSyncStatus{
			PeerID:    id,
			Delta:     delta,
			Synced:    abs(delta) <= e.syncTolerance(id),
			Buffering: p.Paused,
		})
	}
	return report, nil
}

func estimatedPos(p *PeerPosition) time.Duration {
	if p.SampledAt.IsZero() {
		return p.Position
	}
	if p.Paused {
		return p.Position
	}
	return p.Position + time.Since(p.SampledAt)
}

