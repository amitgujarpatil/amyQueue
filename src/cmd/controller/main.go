package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"os/signal"
	"runtime"
	"strings"
	"syscall"
	"time"

	"github.com/yourusername/amyqueue/src/internal/api/metadata/http"
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
		"cluster_mode", cfg.ClusterMode,
		"raft_port", cfg.RaftPort,
		"peers", cfg.PeerNodes,
	)

	if cfg.KillPortOnStart {
		killPort(cfg.RaftPort, logger)
	}

	raftListenAddr := fmt.Sprintf(":%d", cfg.RaftPort)
	transport := tcp.New(raftListenAddr)

	node := raft.NewNode(raft.Config{
		ID:                cfg.NodeID,
		Peers:             cfg.PeerNodes,
		Mode:              raft.ClusterMode(cfg.ClusterMode),
		ElectionTimeoutMs: cfg.RaftElectionTimeoutMs,
		HeartbeatMs:       cfg.RaftHeartbeatMs,
	}, transport, logger)

	if err := node.Start(); err != nil {
		logger.Error("failed to start raft node", "err", err)
		os.Exit(1)
	}

	// dynamic mode: join an existing cluster as observer before doing anything else
	if cfg.ClusterMode == "dynamic" && cfg.JoinAddr != "" {
		if err := joinCluster(cfg, transport, logger); err != nil {
			logger.Error("failed to join cluster", "err", err)
			node.Stop()
			os.Exit(1)
		}
	}

	// start HTTP admin server (dynamic mode exposes membership ops; both modes expose status)
	adminAddr := fmt.Sprintf(":%d", cfg.HTTPPort)
	adminSrv := http.NewAdminServer(adminAddr, node)
	if err := adminSrv.Start(); err != nil {
		logger.Error("failed to start admin server", "err", err)
		node.Stop()
		os.Exit(1)
	}
	logger.Info("admin HTTP server started", "addr", adminAddr)

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig

	logger.Info("shutting down")
	_ = adminSrv.Stop()
	node.Stop()
}

// joinCluster sends an ObserverJoin request to the existing cluster.
// The node starts as observer and can later be promoted to voter via the admin API.
func joinCluster(cfg *config.Config, transport *tcp.Transport, logger *slog.Logger) error {
	selfRaftAddr := fmt.Sprintf("localhost:%d", cfg.RaftPort)
	req := raft.ObserverJoinRequest{
		NodeID: cfg.NodeID,
		Addr:   selfRaftAddr,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := transport.SendObserverJoin(ctx, cfg.JoinAddr, req)
	if err != nil {
		return fmt.Errorf("observer join RPC failed: %w", err)
	}
	if !resp.Success {
		if resp.LeaderAddr != "" {
			// redirected — try the actual leader
			ctx2, cancel2 := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel2()
			resp2, err2 := transport.SendObserverJoin(ctx2, resp.LeaderAddr, req)
			if err2 != nil {
				return fmt.Errorf("observer join redirect failed: %w", err2)
			}
			if !resp2.Success {
				return fmt.Errorf("observer join rejected: %s", resp2.Err)
			}
		} else {
			return fmt.Errorf("observer join rejected: %s", resp.Err)
		}
	}

	logger.Info("joined cluster as observer", "via", cfg.JoinAddr)
	return nil
}

// killPort frees any process listening on the given TCP port.
// macOS uses lsof; Linux uses fuser. Failures are logged but never fatal.
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
	default:
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
