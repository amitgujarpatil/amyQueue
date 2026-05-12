package main

import (
	"fmt"
	"os"

	"github.com/yourusername/amyqueue/src/internal/config"
)

var (
	Version   = "dev"
	BuildTime = "unknown"
)

func main() {
	fmt.Printf("AmyQueue Controller v%s (built: %s)\n", Version, BuildTime)

	cfg, err := config.Load("")
	if err != nil {
		fmt.Fprintf(os.Stderr, "config error: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Node ID      : %s\n", cfg.NodeID)
	fmt.Printf("Role         : %s\n", cfg.NodeRole)
	fmt.Printf("Peer nodes   : %v\n", cfg.PeerNodes)
	fmt.Printf("HTTP port    : %d\n", cfg.HTTPPort)
	fmt.Printf("gRPC port    : %d\n", cfg.GRPCPort)
	fmt.Printf("Raft port    : %d\n", cfg.RaftPort)
	fmt.Printf("Log level    : %s\n", cfg.LogLevel)
	fmt.Println()
	fmt.Println("Starting controller node...")

	// TODO: Phase 2 - Raft node initialization
	// TODO: Phase 3 - Metadata state machine
	// TODO: Phase 4 - gRPC server setup
	// TODO: Phase 5 - HTTP API server
	// TODO: Phase 6 - Signal handling and graceful shutdown

	os.Exit(0)
}
