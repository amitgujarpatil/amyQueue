# Logging

Every controller node writes structured logs to **stdout**. This page covers
log formats, levels, how log collection works end-to-end, how it compares to
Kafka's approach, and the full implementation detail.

---

## Log formats

AmyQueue supports two formats controlled by `LOG_FORMAT`:

**Text (default — human-readable in dev):**
```
time=2026-05-18T14:32:01.123Z level=INFO msg="became leader" node=ctrl-1 term=3 addr=localhost:7001
time=2026-05-18T14:32:01.456Z level=INFO msg="committed log index" node=ctrl-1 index=47
time=2026-05-18T14:32:05.001Z level=WARN msg="heartbeat failed" node=ctrl-1 to=localhost:7002 err="dial tcp: connection refused"
```

**JSON (`LOG_FORMAT=json` — machine-parseable, use in production):**
```json
{"time":"2026-05-18T14:32:01.123Z","level":"INFO","msg":"became leader","node":"ctrl-1","term":3,"addr":"localhost:7001"}
{"time":"2026-05-18T14:32:01.456Z","level":"INFO","msg":"committed log index","node":"ctrl-1","index":47}
{"time":"2026-05-18T14:32:05.001Z","level":"WARN","msg":"heartbeat failed","node":"ctrl-1","to":"localhost:7002","err":"dial tcp: connection refused"}
```

JSON format makes every field trivially parseable by any log shipper — no
regex, no grok patterns, no configuration needed on the Promtail/Filebeat side.

---

## Log levels

| Level | When used | Set with |
|---|---|---|
| `debug` | Per-heartbeat send/receive detail, RPC trace | `LOG_LEVEL=debug` |
| `info` | State transitions, membership changes, startup | `LOG_LEVEL=info` (default) |
| `warn` | Transient failures, retries, unreachable peers | `LOG_LEVEL=warn` |
| `error` | Fatal startup failures only | `LOG_LEVEL=error` |

!!! warning
    `LOG_LEVEL=debug` logs every heartbeat send and receive — multiple lines
    per 100ms per peer. Only use in dev for short sessions; it generates
    significant volume in a 3-node cluster.

---

## Key log messages

These are the messages worth watching for in production:

| Message | Level | What it means |
|---|---|---|
| `became leader` | INFO | This node won the election |
| `entering follower state` | INFO | Node is now following — includes term and timeout |
| `starting election` | INFO | Follower timed out, incrementing term |
| `granted vote` | INFO | This node voted for another candidate |
| `committed log index` | INFO | A quorum has replicated up to this index |
| `observer joined` | INFO | New node registered as observer |
| `observer caught up, safe to promote to voter` | INFO | Observer lag within threshold — includes curl hint |
| `auto-promoting observer to voter` | INFO | AUTO_PROMOTE=true triggered promotion |
| `membership: voter added` | INFO | AddVoter log entry applied |
| `membership: voter removed` | INFO | RemoveVoter log entry applied |
| `heartbeat failed` | DEBUG | AppendEntries RPC failed to a peer |
| `seed unreachable, trying next` | WARN | Bootstrap seed not reachable during join |
| `join rejected, will retry` | WARN | ObserverJoin failed — leader not known yet |
| `election timeout, becoming candidate` | INFO | No heartbeat received within timeout |

---

## How log collection works

Logs are push-based — the process writes, a collector picks up. Unlike
Prometheus (which pulls `/metrics`), the log pipeline flows outward:

```
AmyQueue controller
    │  slog → stdout (text or JSON)
    ▼
Container runtime / systemd
    │  captures stdout per process
    ▼
Log shipper  (Promtail / Filebeat / Fluentd)
    │  tails stdout, attaches labels, ships
    ▼
Log aggregator  (Loki / Elasticsearch)
    │  stores and indexes
    ▼
Query UI  (Grafana / Kibana)
    │  search, correlate with metrics
```

The process itself does nothing beyond writing to stdout. The rest is
deployment infrastructure.

---

## How Kafka does it vs AmyQueue

| | Kafka | AmyQueue |
|---|---|---|
| **Language** | Java | Go |
| **Logging framework** | Log4j 2 | Go stdlib `slog` |
| **Output** | Files in `/var/log/kafka/` | stdout |
| **Format** | Configurable (text/JSON via Log4j pattern) | `text` or `json` via `LOG_FORMAT` |
| **Shipper** | Filebeat / Fluentd tailing log files | Promtail / Filebeat reading stdout |
| **Aggregator** | Elasticsearch (typical) or Loki | Loki (natural fit with Prometheus) |

Kafka writing to files adds a layer — the file must be rotated, the shipper
must tail it, and disk space must be managed. Go writing to stdout is simpler:
the container runtime captures it, the shipper reads it from there, no file
management needed.

---

## The Loki + Promtail stack (recommended)

Loki is the natural complement to Prometheus — same Grafana UI, same label
model. You already have Prometheus for metrics; adding Loki adds log
correlation without a new tool.

```
Promtail config (promtail.yml):

scrape_configs:
  - job_name: amyqueue
    static_configs:
      - targets: [localhost]
        labels:
          job: amyqueue
          __path__: /proc/*/fd/1   # stdout for all processes
    pipeline_stages:
      - json:                      # parse JSON log lines
          expressions:
            level: level
            node_id: node
            msg: msg
      - labels:
            level:
            node_id:
```

Loki indexes only the labels (`job`, `level`, `node_id`). The full log line is
stored raw and searchable by content after filtering by label. Much cheaper
than Elasticsearch which full-text indexes everything.

### LogQL queries

**All WARN and ERROR logs from ctrl-1:**
```logql
{job="amyqueue", node_id="ctrl-1"} |= `level=WARN` or `level=ERROR`
```

**All election events across the cluster:**
```logql
{job="amyqueue"} |= `starting election`
```

**Observer promotion readiness:**
```logql
{job="amyqueue"} |= `safe to promote`
```

**Correlate with metrics in Grafana:**
- Left panel: `rate(amyqueue_raft_elections_total[1m])` spiking
- Right panel: Loki query for `starting election` — exact log lines at the same timestamp

---

## Running locally (dev)

### Loki

```bash
docker run -p 3100:3100 grafana/loki
```

### Promtail

Write `promtail-config.yml`:
```yaml
server:
  http_listen_port: 9080

clients:
  - url: http://localhost:3100/loki/api/v1/push

scrape_configs:
  - job_name: amyqueue
    pipeline_stages:
      - json:
          expressions:
            level: level
            node_id: node
    static_configs:
      - labels:
          job: amyqueue
          __path__: /tmp/amyqueue-*.log
```

Then redirect controller stdout to a file Promtail can tail:

```bash
LOG_FORMAT=json NODE_ID=ctrl-1 ... go run ./src/cmd/controller > /tmp/amyqueue-ctrl-1.log
```

### Grafana

Add Loki as a data source: `http://localhost:3100`

Now you can query logs and metrics side by side in the same dashboard.

---

## Implementation

### File map

```
src/internal/config/config.go       — LogFormat field, LOG_FORMAT env var
src/cmd/controller/main.go          — buildLogger() selects handler by format
```

### How buildLogger works

`buildLogger` is the only place in the codebase that knows about log format.
It returns a `*slog.Logger` — everything else just calls `.Info()`, `.Warn()`
etc. with key-value pairs and never knows or cares what format they end up in.

```go
func buildLogger(level, format string) *slog.Logger {
    var l slog.Level
    switch level {
    case "debug": l = slog.LevelDebug
    case "warn":  l = slog.LevelWarn
    case "error": l = slog.LevelError
    default:      l = slog.LevelInfo
    }
    opts := &slog.HandlerOptions{Level: l}
    if format == "json" {
        return slog.New(slog.NewJSONHandler(os.Stdout, opts))
    }
    return slog.New(slog.NewTextHandler(os.Stdout, opts))
}
```

`slog.NewJSONHandler` and `slog.NewTextHandler` are both stdlib — no external
dependency. The handler is swapped by format; the logger interface is identical
in both cases.

### How the logger flows through the codebase

The logger is constructed once in `main()` and passed down explicitly:

```
main()
  └── buildLogger(cfg.LogLevel, cfg.LogFormat)  → *slog.Logger
        ├── passed to raft.NewNode(cfg, transport, logger)
        │     └── stored as n.logger
        │           └── n.logger.With("node", cfg.ID)  — all raft logs carry node= field
        └── used directly in main() for startup/shutdown logs
```

`raft.Node` calls `logger.With("node", cfg.ID)` at construction time — this
attaches `node=ctrl-1` to every log line the Raft engine emits without
any call site needing to pass the node ID explicitly.

### Why stdout, not files

- Container runtimes (Docker, Kubernetes) capture stdout automatically — no
  file path configuration needed in the shipper.
- No log rotation needed — the runtime handles it.
- `systemd` captures stdout into journald — `journalctl -u amyqueue` works
  out of the box.
- Follows the 12-factor app principle: treat logs as event streams.

### Why slog (stdlib) not zerolog/zap

- Zero external dependency — `log/slog` is in the Go standard library since 1.21.
- Structured key-value API — `logger.Info("msg", "key", value)` — same as
  zerolog/zap but no import needed.
- Both `NewTextHandler` and `NewJSONHandler` are built in — format switching
  costs nothing.
- If performance profiling ever shows logging as a bottleneck (very unlikely
  for a consensus system), swapping to zerolog means replacing only
  `buildLogger` — no call sites change because the rest of the code uses
  `*slog.Logger`.

### Adding a new log call

Call the logger with key-value pairs. Use the node's logger (`n.logger`) inside
`raft.Node` — it already has `node=ctrl-1` attached:

```go
// inside raft.Node — node= is already attached
n.logger.Info("some event", "key1", val1, "key2", val2)

// inside main.go — attach node_id manually if needed
logger.Info("some event", "node_id", cfg.NodeID, "key", val)
```

Keys to always include on important events:
- `term` — current Raft term (helps correlate with metrics)
- `node_id` — which node (auto-attached inside raft.Node via logger.With)
- `err` — error value on WARN/ERROR lines

### LOG_FORMAT in config

`config.go` validates the value at load time — returns an error on unknown
values so a misconfigured node fails fast at startup rather than silently
writing unparseable output:

```go
cfg.LogFormat = getEnv("LOG_FORMAT", "text")
if cfg.LogFormat != "text" && cfg.LogFormat != "json" {
    return nil, fmt.Errorf("LOG_FORMAT must be 'text' or 'json', got %q", cfg.LogFormat)
}
```
