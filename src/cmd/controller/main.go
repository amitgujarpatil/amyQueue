package main

import (
	"fmt"
	"os"
)

// Version information (set via ldflags during build)
var (
	Version   = "dev"
	BuildTime = "unknown"
)

// Let's Build Amit's Kafka!
//
// AmyQueue - A distributed message queue system built from scratch
//
// This is the controller node - responsible for:
//   - Raft consensus for cluster coordination
//   - Managing cluster metadata (topics, partitions, brokers)
//   - Partition leader election
//   - Broker health monitoring
//   - Client metadata requests
//
// Controllers form a Raft cluster (typically 3 or 5 nodes) for high availability.
// They don't handle actual message data - that's the broker's job.
//
// Architecture:
//   ┌─────────────────────────────────────────┐
//   │     Controller Cluster (Raft)           │
//   │  ┌──────┐  ┌──────┐  ┌──────┐          │
//   │  │Ctrl-1│  │Ctrl-2│  │Ctrl-3│          │
//   │  │Leader│  │Follow│  │Follow│          │
//   │  └───┬──┘  └───┬──┘  └───┬──┘          │
//   └──────┼─────────┼─────────┼─────────────┘
//          │         │         │
//          └─────────┴─────────┘
//                    │
//          Metadata Distribution
//                    │
//          ┌─────────┴─────────┐
//          ▼                   ▼
//      ┌──────┐            ┌──────┐
//      │Broker│            │Broker│
//      │  -1  │            │  -2  │
//      └──────┘            └──────┘
//
// Usage:
//   controller --config controller-1.yaml
//   controller --node-id controller-1 --http-port 8080 --raft-port 8081
//
// Environment:
//   See .env.example for all configuration options
//
func main() {
	fmt.Printf("AmyQueue Controller v%s (built: %s)\n", Version, BuildTime)
	fmt.Println("Let's Build Amit's Kafka! 🚀")
	fmt.Println()
	fmt.Println("Starting controller node...")
	fmt.Println("⚠️  Implementation in progress - check docs/ROADMAP.md for status")
	fmt.Println()

	// TODO: Implementation phases
	// Phase 1: Configuration loading
	// Phase 2: Raft node initialization
	// Phase 3: Metadata state machine
	// Phase 4: gRPC server setup
	// Phase 5: HTTP API server
	// Phase 6: Signal handling and graceful shutdown

	fmt.Println("Controller started successfully")
	fmt.Println("Press Ctrl+C to stop")

	// Temporary: Just exit for now
	os.Exit(0)
}
