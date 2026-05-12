package main

import (
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"os/signal"
	"runtime"
	"strings"
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

	if cfg.KillPortOnStart {
		killPort(cfg.RaftPort, logger)
	}

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

// killPort frees any process listening on the given TCP port.
// macOS uses lsof; Linux uses fuser. Failures are logged but never fatal —
// this is a dev convenience, not a hard requirement.
func killPort(port int, logger *slog.Logger) {
	portStr := fmt.Sprintf("%d", port)
	var pids []string

	switch runtime.GOOS {
	case "darwin":
		out, err := exec.Command("lsof", "-ti", fmt.Sprintf("TCP:%s", portStr)).Output()
		if err != nil || len(out) == 0 {
			return
		}
		for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
			if pid := strings.TrimSpace(line); pid != "" {
				pids = append(pids, pid)
			}
		}
	default: // linux
		out, err := exec.Command("fuser", portStr+"/tcp").Output()
		if err != nil || len(out) == 0 {
			return
		}
		for _, pid := range strings.Fields(string(out)) {
			pids = append(pids, strings.TrimSpace(pid))
		}
	}

	for _, pid := range pids {
		if err := exec.Command("kill", "-9", pid).Run(); err != nil {
			logger.Warn("could not kill process on port", "port", port, "pid", pid, "err", err)
		} else {
			logger.Info("killed process occupying port", "port", port, "pid", pid)
		}
	}
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
