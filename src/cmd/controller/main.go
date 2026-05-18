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

	selfRaftAddr := fmt.Sprintf("localhost:%d", cfg.RaftPort)

	node := raft.NewNode(raft.Config{
		ID:                      cfg.NodeID,
		Addr:                    selfRaftAddr,
		Peers:                   cfg.PeerNodes,
		Mode:                    raft.ClusterMode(cfg.ClusterMode),
		ElectionTimeoutMs:       cfg.RaftElectionTimeoutMs,
		HeartbeatMs:             cfg.RaftHeartbeatMs,
		AutoPromote:             cfg.AutoPromote,
		AutoPromoteLagThreshold: cfg.AutoPromoteLagThreshold,
	}, transport, logger)

	if err := node.Start(); err != nil {
		logger.Error("failed to start raft node", "err", err)
		os.Exit(1)
	}

	// dynamic mode: join an existing cluster as observer before doing anything else
	if cfg.ClusterMode == "dynamic" && len(cfg.BootstrapServers) > 0 {
		if err := joinCluster(cfg, selfRaftAddr, transport, logger); err != nil {
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

// joinCluster registers this node as an observer with the existing cluster.
//
// Flow per attempt:
//  1. Try ObserverJoin on each bootstrap seed in order
//  2. If the seed is not the leader it returns LeaderAddr — retry on that address immediately
//  3. On network error or no leader known yet — log and move to next seed
//  4. After exhausting all seeds, wait JoinRetryIntervalMs and start over
//  5. Give up after JoinMaxRetries full passes
//
// This avoids the separate ClusterInfo round-trip: ObserverJoin already
// returns the leader address in its response so any node is a valid entry point.
func joinCluster(cfg *config.Config, selfRaftAddr string, transport *tcp.Transport, logger *slog.Logger) error {
	req := raft.ObserverJoinRequest{
		NodeID: cfg.NodeID,
		Addr:   selfRaftAddr,
	}
	retryInterval := time.Duration(cfg.JoinRetryIntervalMs) * time.Millisecond

	for attempt := 1; attempt <= cfg.JoinMaxRetries; attempt++ {
		logger.Info("join attempt", "attempt", attempt, "of", cfg.JoinMaxRetries, "seeds", cfg.BootstrapServers)

		for _, seed := range cfg.BootstrapServers {
			target := seed
			for {
				// single RPC attempt to target
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				resp, err := transport.SendObserverJoin(ctx, target, req)
				cancel()

				if err != nil {
					logger.Warn("seed unreachable, trying next",
						"target", target, "attempt", attempt, "err", err)
					break // move to next seed
				}

				if resp.Success {
					logger.Info("joined cluster as observer", "via", target)
					return nil
				}

				// not the leader but knows who is — follow redirect
				if resp.LeaderAddr != "" && resp.LeaderAddr != target {
					logger.Info("not leader, redirecting",
						"from", target, "to_leader", resp.LeaderAddr, "leader_id", resp.LeaderID)
					target = resp.LeaderAddr
					continue // retry immediately on the actual leader
				}

				// leader unknown yet or join rejected — log actual reason
				logger.Warn("join rejected, will retry",
					"seed", target, "attempt", attempt, "reason", resp.Err)
				break // wait and retry full pass
			}
		}

		if attempt < cfg.JoinMaxRetries {
			logger.Info("all seeds tried, waiting before next attempt",
				"wait", retryInterval, "attempt", attempt)
			time.Sleep(retryInterval)
		}
	}

	return fmt.Errorf("failed to join cluster after %d attempts via %v", cfg.JoinMaxRetries, cfg.BootstrapServers)
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
