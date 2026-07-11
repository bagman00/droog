package sync

import (
	"fmt"
	"sync"
	"time"
)

const SyncTolerance = 120 * time.Millisecond      // was 40ms — too sensitive to local jitter

const MicroSeekThreshold = 60 * time.Millisecond  // was 5ms — was firing on noise, not real drift


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

type DeltaEngine struct {
	isHost   bool
	localID  string
	getPos   PositionFunc
	tickRate time.Duration

	mu          sync.RWMutex
	peers       map[string]*PeerPosition
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
		cooldown:     1200 * time.Millisecond,  // was 150ms — gave the feedback loop no time to settle
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

	e.mu.RLock()
	defer e.mu.RUnlock()

	for id, p := range e.peers {
		// Skip peers we have no real sample for yet (avoids the
		// zero-SampledAt bug producing a multi-year "delta").
		if p.SampledAt.IsZero() {
			continue
		}

		estimated := estimatedPos(p)
		delta := localPos - estimated
		absDelta := abs(delta)

		switch {
		case absDelta < MicroSeekThreshold:

		case absDelta <= SyncTolerance:

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
			Synced:    abs(delta) <= SyncTolerance,
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

