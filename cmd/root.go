package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "droog",
	Short: "watch anything together, no servers, no accounts",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("droog is alive")
	},
}

func Execute() {
	rootCmd.Execute()
}