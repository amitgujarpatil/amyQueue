package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/yourusername/amyqueue/src/internal/broker"
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

	logger := buildLogger(cfg.LogLevel, cfg.LogFormat)

	// Step 1 — validate required broker config
	if cfg.BrokerID == "" {
		logger.Error("AMYQUEUE_BROKER_ID must be set; broker cannot start without an explicit ID")
		os.Exit(1)
	}
	if cfg.ClusterToken == "" {
		logger.Error("AMYQUEUE_CLUSTER_TOKEN must be set")
		os.Exit(1)
	}

	logger.Info("broker starting",
		"broker_id", cfg.BrokerID,
		"host", cfg.BrokerHost,
		"port", cfg.BrokerPort,
		"controller", fmt.Sprintf("%s:%d", cfg.ControllerHost, cfg.HTTPPort),
	)

	b := broker.New(cfg)

	// Step 2 — register with controller (retry with backoff + leader redirect)
	regCtx, regCancel := context.WithTimeout(context.Background(), 2*time.Minute)
	if err := b.Register(regCtx); err != nil {
		regCancel()
		logger.Error("failed to register with controller", "err", err)
		os.Exit(1)
	}
	regCancel()

	logger.Info("registered with controller", "epoch", b.Epoch)

	// TODO: Phase 5 — start heartbeat goroutine
	// TODO: Phase 9 — wait for LeaderAndISR push
	// TODO: Phase 3+ — start accepting client connections

	// Wait for shutdown signal
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig

	logger.Info("SIGTERM received, starting controlled shutdown")

	// Controlled shutdown: notify controller before exiting
	shutCtx, shutCancel := context.WithTimeout(context.Background(),
		time.Duration(cfg.ShutdownTimeoutMs)*time.Millisecond)
	defer shutCancel()

	if err := b.Shutdown(shutCtx); err != nil {
		logger.Warn("controlled shutdown failed, proceeding with dirty exit", "err", err)
	} else {
		logger.Info("controller acknowledged shutdown")
	}

	logger.Info("broker stopped")
}

func buildLogger(level, format string) *slog.Logger {
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
	opts := &slog.HandlerOptions{Level: l}
	if format == "json" {
		return slog.New(slog.NewJSONHandler(os.Stdout, opts))
	}
	return slog.New(slog.NewTextHandler(os.Stdout, opts))
}
