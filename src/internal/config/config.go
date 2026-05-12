package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/joho/godotenv"
)

type NodeRole string

const (
	RoleController NodeRole = "controller"
	RoleBroker     NodeRole = "broker"
)

type Config struct {
	NodeRole              NodeRole
	ControllerHost        string
	ControllerPort        int
	PeerNodes             []string // addresses of all peer nodes e.g. ["localhost:9092", "localhost:9093"]
	NodeID                string
	HTTPPort              int
	GRPCPort              int
	RaftPort              int
	RaftElectionTimeoutMs int
	RaftHeartbeatMs       int
	LogLevel              string
	KillPortOnStart       bool // dev convenience: free the port before binding (default true)
}

// Load reads .env file (if present) then overlays actual environment variables.
// Real env vars always win over .env values.
func Load(envFile string) (*Config, error) {
	// Load .env without overwriting vars already set in the environment
	if envFile == "" {
		envFile = ".env"
	}
	_ = godotenv.Load(envFile) // ignore error — file is optional

	cfg := &Config{}

	role := getEnv("NODE_ROLE", "broker")
	switch NodeRole(role) {
	case RoleController, RoleBroker:
		cfg.NodeRole = NodeRole(role)
	default:
		return nil, fmt.Errorf("NODE_ROLE must be 'controller' or 'broker', got %q", role)
	}

	cfg.ControllerHost = getEnv("CONTROLLER_HOST", "localhost")

	var err error
	cfg.ControllerPort, err = getEnvInt("CONTROLLER_PORT", 8080)
	if err != nil {
		return nil, fmt.Errorf("CONTROLLER_PORT: %w", err)
	}

	cfg.PeerNodes = parsePeerNodes(getEnv("PEER_NODES", ""))

	cfg.NodeID = getEnv("NODE_ID", "node-1")

	cfg.HTTPPort, err = getEnvInt("HTTP_PORT", 8080)
	if err != nil {
		return nil, fmt.Errorf("HTTP_PORT: %w", err)
	}

	cfg.GRPCPort, err = getEnvInt("GRPC_PORT", 8082)
	if err != nil {
		return nil, fmt.Errorf("GRPC_PORT: %w", err)
	}

	cfg.RaftPort, err = getEnvInt("RAFT_PORT", 8081)
	if err != nil {
		return nil, fmt.Errorf("RAFT_PORT: %w", err)
	}

	cfg.RaftElectionTimeoutMs, err = getEnvInt("RAFT_ELECTION_TIMEOUT_MS", 1000)
	if err != nil {
		return nil, fmt.Errorf("RAFT_ELECTION_TIMEOUT_MS: %w", err)
	}

	cfg.RaftHeartbeatMs, err = getEnvInt("RAFT_HEARTBEAT_INTERVAL_MS", 100)
	if err != nil {
		return nil, fmt.Errorf("RAFT_HEARTBEAT_INTERVAL_MS: %w", err)
	}

	cfg.LogLevel = getEnv("LOG_LEVEL", "info")
	cfg.KillPortOnStart = getEnvBool("KILL_PORT_ON_START", true)

	return cfg, nil
}

// parsePeerNodes splits a comma-separated list of addresses and trims whitespace.
// Empty string returns nil slice.
func parsePeerNodes(raw string) []string {
	if raw == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	nodes := make([]string, 0, len(parts))
	for _, p := range parts {
		if addr := strings.TrimSpace(p); addr != "" {
			nodes = append(nodes, addr)
		}
	}
	return nodes
}

func getEnv(key, fallback string) string {
	if v, ok := os.LookupEnv(key); ok {
		return v
	}
	return fallback
}

func getEnvBool(key string, fallback bool) bool {
	v, ok := os.LookupEnv(key)
	if !ok {
		return fallback
	}
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "false", "0", "no":
		return false
	default:
		return true
	}
}

func getEnvInt(key string, fallback int) (int, error) {
	v, ok := os.LookupEnv(key)
	if !ok {
		return fallback, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return 0, fmt.Errorf("expected integer, got %q", v)
	}
	return n, nil
}
