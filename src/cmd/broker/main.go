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
	fmt.Printf("AmyQueue Broker v%s (built: %s)\n", Version, BuildTime)

	cfg, err := config.Load("")
	if err != nil {
		fmt.Fprintf(os.Stderr, "config error: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Node ID      : %s\n", cfg.NodeID)
	fmt.Printf("Role         : %s\n", cfg.NodeRole)
	fmt.Printf("Controller   : %s:%d\n", cfg.ControllerHost, cfg.ControllerPort)
	fmt.Printf("Peer nodes   : %v\n", cfg.PeerNodes)
	fmt.Printf("HTTP port    : %d\n", cfg.HTTPPort)
	fmt.Printf("gRPC port    : %d\n", cfg.GRPCPort)
	fmt.Printf("Log level    : %s\n", cfg.LogLevel)
	fmt.Println()
	fmt.Println("Starting broker node...")

	// TODO: Phase 2 - Register with controller
	// TODO: Phase 3 - Partition log storage
	// TODO: Phase 4 - Producer API (gRPC)
	// TODO: Phase 5 - Consumer API (gRPC)
	// TODO: Phase 6 - Replication service
	// TODO: Phase 7 - Consumer group management
	// TODO: Phase 8 - Periodic heartbeat to controller

	os.Exit(0)
}
