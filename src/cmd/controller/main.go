package main

import (
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/yourusername/amyqueue/src/internal/config"
	"github.com/yourusername/amyqueue/src/internal/raft"
	"github.com/yourusername/amyqueue/src/internal/raft/tcp"
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

	logger := buildLogger(cfg.LogLevel)

	logger.Info("controller starting",
		"node_id", cfg.NodeID,
		"role", cfg.NodeRole,
		"raft_port", cfg.RaftPort,
		"peers", cfg.PeerNodes,
	)

	listenAddr := fmt.Sprintf(":%d", cfg.RaftPort)
	transport := tcp.New(listenAddr)

	node := raft.NewNode(raft.Config{
		ID:                cfg.NodeID,
		Peers:             cfg.PeerNodes,
		ElectionTimeoutMs: cfg.RaftElectionTimeoutMs,
		HeartbeatMs:       cfg.RaftHeartbeatMs,
	}, transport, logger)

	if err := node.Start(); err != nil {
		logger.Error("failed to start raft node", "err", err)
		os.Exit(1)
	}

	// wait for SIGINT / SIGTERM
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig

	logger.Info("shutting down")
	node.Stop()
}

func buildLogger(level string) *slog.Logger {
	var l slog.Level
	switch level {
	case "debug":
		l = slog.LevelDebug
	case "warn":
		l = slog.LevelWarn
	case "error":
		l = slog.LevelError
	default:
		l = slog.LevelInfo
	}
	return slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: l}))
}
