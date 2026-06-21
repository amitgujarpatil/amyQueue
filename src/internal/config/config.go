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
	LogFormat             string // "text" (default, human-readable) or "json" (machine-parseable)
	KillPortOnStart       bool   // dev convenience: free the port before binding (default true)
	ClusterMode          string   // "static" (default) or "dynamic"
	BootstrapServers     []string // dynamic mode: 1+ seed raft addresses, tried in order until leader found
	JoinMaxRetries           int      // how many full passes over bootstrap servers before giving up
	JoinRetryIntervalMs      int      // wait between retry passes (ms)
	AutoPromote              bool     // auto-promote observers to voters when caught up
	AutoPromoteLagThreshold  int      // max entries behind leader to still be considered caught up
	MetricsPort              int      // port for the Prometheus /metrics endpoint

	// Persistence (Phase 4)
	DataDir string // root directory for all durable state (default: "./data")

	// Broker heartbeat (Phase 5)
	BrokerHeartbeatMs      int // how often the broker sends a heartbeat (default 3000)
	BrokerSessionTimeoutMs int // controller marks broker dead after this many ms without a heartbeat (default 30000)

	// Cluster identity and authentication (Phase 2)
	ClusterID             string // UUID identifying this cluster; required on controller and broker
	ClusterToken          string // shared secret for broker↔controller auth (AMYQUEUE_CLUSTER_TOKEN)
	ClusterTokenSecondary string // secondary token for zero-downtime rotation (AMYQUEUE_CLUSTER_TOKEN_SECONDARY)

	// Broker identity (Phase 2; used when NodeRole == RoleBroker)
	BrokerID   string // must be set explicitly — no auto-assignment (AMYQUEUE_BROKER_ID)
	BrokerHost string // host/IP advertised to the controller (AMYQUEUE_BROKER_HOST)
	BrokerPort int    // client-facing port advertised to the controller (AMYQUEUE_BROKER_PORT)
	RackID     string // optional rack/availability-zone label (AMYQUEUE_RACK_ID)

	// Controlled shutdown (Phase 2)
	ShutdownTimeoutMs      int // max wait for controller to migrate all leaders (default 30000)
	ShutdownDrainTimeoutMs int // max wait for in-flight requests to finish (default 5000)
	ShutdownMaxRetries     int // retries if controller is unreachable (default 3)
	ShutdownRetryBackoffMs int // backoff between retries (default 5000)
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

	cfg.RaftHeartbeatMs, err = getEnvInt("CONTROLLER_HEART_BEAT_INTERVAL", 1000)
	if err != nil {
		return nil, fmt.Errorf("CONTROLLER_HEART_BEAT_INTERVAL: %w", err)
	}

	cfg.LogLevel = getEnv("LOG_LEVEL", "info")
	cfg.LogFormat = getEnv("LOG_FORMAT", "text")
	if cfg.LogFormat != "text" && cfg.LogFormat != "json" {
		return nil, fmt.Errorf("LOG_FORMAT must be 'text' or 'json', got %q", cfg.LogFormat)
	}
	cfg.KillPortOnStart = getEnvBool("KILL_PORT_ON_START", true)

	cfg.ClusterMode = getEnv("CLUSTER_MODE", "static")
	if cfg.ClusterMode != "static" && cfg.ClusterMode != "dynamic" {
		return nil, fmt.Errorf("CLUSTER_MODE must be 'static' or 'dynamic', got %q", cfg.ClusterMode)
	}
	// BOOTSTRAP_SERVERS accepts the same comma-separated format as PEER_NODES
	cfg.BootstrapServers = parsePeerNodes(getEnv("BOOTSTRAP_SERVERS", ""))

	cfg.JoinMaxRetries, err = getEnvInt("JOIN_MAX_RETRIES", 10)
	if err != nil {
		return nil, fmt.Errorf("JOIN_MAX_RETRIES: %w", err)
	}
	cfg.JoinRetryIntervalMs, err = getEnvInt("JOIN_RETRY_INTERVAL_MS", 2000)
	if err != nil {
		return nil, fmt.Errorf("JOIN_RETRY_INTERVAL_MS: %w", err)
	}

	cfg.AutoPromote = getEnvBool("AUTO_PROMOTE", false)
	cfg.AutoPromoteLagThreshold, err = getEnvInt("AUTO_PROMOTE_LAG_THRESHOLD", 10)
	if err != nil {
		return nil, fmt.Errorf("AUTO_PROMOTE_LAG_THRESHOLD: %w", err)
	}

	cfg.MetricsPort, err = getEnvInt("METRICS_PORT", 9090)
	if err != nil {
		return nil, fmt.Errorf("METRICS_PORT: %w", err)
	}

	// Persistence (Phase 4)
	cfg.DataDir = getEnv("AMYQUEUE_DATA_DIR", "./data")

	// Broker heartbeat (Phase 5)
	cfg.BrokerHeartbeatMs, err = getEnvInt("AMYQUEUE_BROKER_HEARTBEAT_MS", 3000)
	if err != nil {
		return nil, fmt.Errorf("AMYQUEUE_BROKER_HEARTBEAT_MS: %w", err)
	}
	cfg.BrokerSessionTimeoutMs, err = getEnvInt("AMYQUEUE_BROKER_SESSION_TIMEOUT_MS", 30000)
	if err != nil {
		return nil, fmt.Errorf("AMYQUEUE_BROKER_SESSION_TIMEOUT_MS: %w", err)
	}

	// Cluster auth (Phase 2)
	cfg.ClusterID = getEnv("AMYQUEUE_CLUSTER_ID", "")
	cfg.ClusterToken = getEnv("AMYQUEUE_CLUSTER_TOKEN", "")
	cfg.ClusterTokenSecondary = getEnv("AMYQUEUE_CLUSTER_TOKEN_SECONDARY", "")

	// Broker identity (Phase 2)
	cfg.BrokerID = getEnv("AMYQUEUE_BROKER_ID", "")
	cfg.BrokerHost = getEnv("AMYQUEUE_BROKER_HOST", "localhost")
	cfg.BrokerPort, err = getEnvInt("AMYQUEUE_BROKER_PORT", cfg.GRPCPort)
	if err != nil {
		return nil, fmt.Errorf("AMYQUEUE_BROKER_PORT: %w", err)
	}
	cfg.RackID = getEnv("AMYQUEUE_RACK_ID", "")

	// Controlled shutdown (Phase 2)
	cfg.ShutdownTimeoutMs, err = getEnvInt("AMYQUEUE_SHUTDOWN_TIMEOUT_MS", 30000)
	if err != nil {
		return nil, fmt.Errorf("AMYQUEUE_SHUTDOWN_TIMEOUT_MS: %w", err)
	}
	cfg.ShutdownDrainTimeoutMs, err = getEnvInt("AMYQUEUE_SHUTDOWN_DRAIN_TIMEOUT_MS", 5000)
	if err != nil {
		return nil, fmt.Errorf("AMYQUEUE_SHUTDOWN_DRAIN_TIMEOUT_MS: %w", err)
	}
	cfg.ShutdownMaxRetries, err = getEnvInt("AMYQUEUE_SHUTDOWN_MAX_RETRIES", 3)
	if err != nil {
		return nil, fmt.Errorf("AMYQUEUE_SHUTDOWN_MAX_RETRIES: %w", err)
	}
	cfg.ShutdownRetryBackoffMs, err = getEnvInt("AMYQUEUE_SHUTDOWN_RETRY_BACKOFF_MS", 5000)
	if err != nil {
		return nil, fmt.Errorf("AMYQUEUE_SHUTDOWN_RETRY_BACKOFF_MS: %w", err)
	}

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
