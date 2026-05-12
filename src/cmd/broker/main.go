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

// Let's Build Amit's Kafka - Broker Node
//
// The broker is responsible for:
//   - Storing partition data (messages)
//   - Serving producer requests (writes)
//   - Serving consumer requests (reads)
//   - Replicating data to other brokers
//   - Managing consumer groups
//   - Reporting health to controllers
//
// Brokers are stateless workers that can scale horizontally.
// They receive partition assignments from the controller cluster.
//
func main() {
	fmt.Printf("AmyQueue Broker v%s (built: %s)\n", Version, BuildTime)
	fmt.Println("Let's Build Amit's Kafka! 🚀")
	fmt.Println()
	fmt.Println("Starting broker node...")
	fmt.Println("⚠️  Implementation in progress - check docs/ROADMAP.md for status")
	fmt.Println()

	// TODO: Implementation phases
	// Phase 1: Configuration loading
	// Phase 2: Register with controller
	// Phase 3: Partition log storage
	// Phase 4: Producer API (gRPC)
	// Phase 5: Consumer API (gRPC)
	// Phase 6: Replication service
	// Phase 7: Consumer group management
	// Phase 8: Periodic heartbeat to controller

	fmt.Println("Broker started successfully")
	fmt.Println("Press Ctrl+C to stop")

	// Temporary: Just exit for now
	os.Exit(0)
}
