# Metrics Design

How AmyQueue exposes runtime observability data — design rationale, comparison
with how Kafka and other systems do it, metric type reference, and future plans.

---

## Why metrics matter

Logs tell you what happened at a point in time. Metrics tell you the shape of
the system over time — trends, rates, thresholds. You need both:

| Signal | Answers | Tool |
|---|---|---|
| Logs | "what happened at 14:32?" | slog, ELK |
| Metrics | "is the leader stable over the last hour?" | Prometheus, Grafana |
| Traces | "which RPC call caused this latency spike?" | OpenTelemetry (future) |

For a distributed consensus system like AmyQueue, metrics are especially
important because the problems that matter — leader instability, observer lag,
quorum loss — only become visible when you look at trends, not individual log
lines.

---

## Metric types

Prometheus defines four types. Knowing which to use prevents misuse.

### Gauge

A value that can go up or down. Represents the **current state** of something.

```
amyqueue_raft_current_term{node_id="ctrl-1"} 3
amyqueue_raft_commit_index{node_id="ctrl-1"} 47
amyqueue_raft_is_leader{node_id="ctrl-1"} 1
```

Use for: term, commit index, observer lag, member count, "is leader" flag.

**Never use a gauge for cumulative counts** — if the process restarts, it
resets to 0 and looks like a drop in Grafana.

### Counter

A value that only ever increases. Prometheus handles resets (process restart)
automatically by tracking the delta.

```
amyqueue_raft_elections_total{node_id="ctrl-1"} 5
amyqueue_raft_heartbeat_failures_total{node_id="ctrl-1",peer="localhost:7002"} 2
```

Use for: total elections, total RPCs sent, total failed heartbeats, total
membership changes.

**Never use a counter for something that can decrease** — use a gauge instead.

### Histogram

Records the distribution of observed values across configurable buckets.
Gives you percentiles (p50, p95, p99).

```
raft_rpc_duration_seconds_bucket{le="0.001"} 120
raft_rpc_duration_seconds_bucket{le="0.005"} 180
raft_rpc_duration_seconds_bucket{le="0.01"} 190
raft_rpc_duration_seconds_sum 0.823
raft_rpc_duration_seconds_count 190
```

Use for: RPC latency, heartbeat round-trip time, log append duration.

### Summary

Like a histogram but calculates quantiles client-side instead of server-side.
Less useful in practice because you cannot aggregate summaries across multiple
instances. **Prefer histograms** for everything except single-node percentile
tracking.

---

## How other systems do it

### Kafka + Prometheus

Kafka is Java — it uses **JMX** (Java Management Extensions) as its native
metrics layer. Every JVM exposes a tree of `MBeans` (managed beans) at a JMX
port. The problem: Prometheus does not speak JMX.

Two adapters bridge the gap:

**JMX Exporter (Java agent)**

```
Kafka broker JVM
  ├── internal JMX MBeans (kafka.server:type=BrokerTopicMetrics, etc.)
  └── JMX Exporter .jar loaded as -javaagent at startup
          └── translates MBeans → Prometheus text format
                  └── exposes :9404/metrics
                          ↑ Prometheus scrapes
```

The exporter runs inside the Kafka JVM. It reads JMX beans on each scrape and
converts them. Config is a YAML file that maps JMX bean names to Prometheus
metric names.

Downside: tight JVM coupling, config is verbose, JMX beans change between
Kafka versions.

**kafka_exporter (external Go binary)**

```
Kafka cluster
  └── Kafka Admin API / consumer group protocol

kafka_exporter (separate process)
  ├── connects to Kafka as an admin client
  ├── queries: consumer lag, topic offsets, broker health, partition leaders
  └── exposes :9308/metrics
          ↑ Prometheus scrapes
```

No JMX — reads Kafka's own client protocol. Better for high-level cluster
health (consumer lag, partition balance) than JVM internals.

Kafka KRaft (new controller mode) also exposes metrics directly via the same
JMX + exporter path, but controller-specific metrics were added: active
controller count, metadata log offset, voter set size.

### etcd

etcd is Go — it embeds `prometheus/client_golang` directly. No exporter
needed. `/metrics` is built into the etcd HTTP server.

```
etcd process
  └── /metrics endpoint (built in, :2379)
          ↑ Prometheus scrapes
```

Metrics include: `etcd_server_leader_changes_seen_total`,
`etcd_server_proposals_committed_total`, `etcd_disk_wal_fsync_duration_seconds`.

### CockroachDB

Same as etcd — Go process, prometheus client embedded, `/metrics` on the same
HTTP port as the admin UI. They expose ~1000 metrics but group them by prefix
(sql.*, raft.*, storage.*).

### What these share

Every modern distributed system separates the **metrics surface** from the
**internal protocol**:

| System | Internal protocol port | Metrics port |
|---|---|---|
| Kafka | 9092 (broker), 9093 (controller) | 9404 (JMX exporter) |
| etcd | 2380 (peer), 2379 (client) | 2379 (same, /metrics path) |
| CockroachDB | 26257 (SQL+Raft) | 8080 (admin, /metrics path) |
| Consul | 8300 (server RPC) | 8500 (HTTP, /v1/agent/metrics) |
| AmyQueue | RAFT_PORT (Raft TCP) | METRICS_PORT (Prometheus, /metrics) |

---

## AmyQueue design

### Separate, configurable port

The metrics endpoint runs on its own port (`METRICS_PORT`, default `9090`),
separate from:

- `RAFT_PORT` — internal Raft TCP (vote, append entries, observer join)
- `HTTP_PORT` — admin API (cluster status, voter management)

Reasons for a dedicated port:

1. **Independent firewalling.** The admin API (`HTTP_PORT`) should be reachable
   only by operators and tooling. The metrics port needs to be reachable by the
   Prometheus server, which may be in a different network segment. They have
   different access control requirements.

2. **No interference.** A scrape from Prometheus (every 15s) cannot accidentally
   hit an admin route or slow down an admin operation.

3. **Clear responsibility.** Anyone looking at the codebase immediately knows
   which port does what.

### External collector — keeping Raft core clean

The metrics are not collected inside `raft.Node`. Instead:

```
raft.Node
  └── exposes MetricsSnapshot() — read-only snapshot of internal state

metrics.Collector  (implements prometheus.Collector)
  ├── holds a reference to MetricsSource interface
  ├── on Collect(): calls MetricsSource.MetricsSnapshot()
  └── translates snapshot fields → prometheus Desc/Metric objects

metrics.Server
  ├── owns an http.Server on METRICS_PORT
  └── /metrics route → promhttp.Handler()
```

The `raft` package never imports `prometheus`. The `metrics` package imports
both `raft` (for the interface) and `prometheus` (for the client). The
dependency arrow is:

```
metrics → raft (read-only interface)
metrics → prometheus/client_golang
raft    → nothing observability-related
```

This matters because:
- Raft is the most critical package — keeping it free of external dependencies
  reduces surface area for bugs.
- The metrics package can be replaced or disabled without touching Raft.
- Testing Raft does not require a Prometheus registry.

### MetricsSource interface

`raft.Node` implements this interface. Any other component that wants to expose
metrics implements the same pattern.

```go
// src/internal/raft/metrics.go

type MetricsSource interface {
    MetricsSnapshot() MetricsSnapshot
}

type MetricsSnapshot struct {
    NodeID      string
    State       State    // Follower / Candidate / Leader
    Term        uint64
    CommitIndex uint64
    LastApplied uint64
    LeaderID    string
    Members     []Member

    // populated only on leader; empty on followers
    ObserverLag map[string]int64 // observer NodeID → lag entries

    // cumulative counters — only increase over process lifetime
    ElectionsStarted  uint64
    LeaderChanges     uint64
    HeartbeatFailures map[string]uint64 // peer addr → failure count
}
```

`ClusterStatus()` already exists on `AdminService` and returns most of this.
`MetricsSnapshot()` is a dedicated read path for metrics — same lock, lighter
copy, no HTTP concerns mixed in.

### Metrics server lifecycle

```
main.go
  │
  ├── node.Start()          — Raft TCP transport
  ├── adminSrv.Start()      — HTTP_PORT admin API
  └── metricsSrv.Start()    — METRICS_PORT /metrics
          │
          on SIGINT/SIGTERM:
          ├── metricsSrv.Stop()
          ├── adminSrv.Stop()
          └── node.Stop()
```

All three servers start in the same process, each on their own port, each shut
down in reverse order on signal.

---

## Metrics we expose (Phase 1 — controller)

### Raft state (gauges)

| Metric | Labels | Description |
|---|---|---|
| `amyqueue_raft_current_term` | `node_id` | Current Raft term |
| `amyqueue_raft_is_leader` | `node_id` | 1 if this node is the leader, 0 otherwise |
| `amyqueue_raft_commit_index` | `node_id` | Highest log index committed by quorum |
| `amyqueue_raft_last_applied` | `node_id` | Highest log index applied to state machine |

### Membership (gauges)

| Metric | Labels | Description |
|---|---|---|
| `amyqueue_raft_member_count` | `node_id` | Total members (voters + observers) |
| `amyqueue_raft_voter_count` | `node_id` | Voters only |
| `amyqueue_raft_observer_count` | `node_id` | Observers only |
| `amyqueue_raft_observer_lag` | `node_id`, `observer_id` | Log entries the observer is behind — populated only on the leader |

### Stability (counters)

| Metric | Labels | Description |
|---|---|---|
| `amyqueue_raft_elections_total` | `node_id` | Total elections started by this node — rising = instability |
| `amyqueue_raft_leader_changes_total` | `node_id` | Times this node observed a new leader |
| `amyqueue_raft_heartbeat_failures_total` | `node_id`, `peer` | Failed AppendEntries RPCs per peer |

---

## Future plans

### Phase 2 — Broker metrics

When brokers are implemented they will expose their own `MetricsSource`. The
`metrics.Collector` pattern is the same — broker implements the interface, a
separate collector translates it, same `/metrics` endpoint on `METRICS_PORT`.

| Metric | Type | Description |
|---|---|---|
| `amyqueue_broker_partition_count` | gauge | Partitions assigned to this broker |
| `amyqueue_broker_leader_partition_count` | gauge | Partitions this broker leads |
| `amyqueue_broker_log_end_offset` | gauge | Per-partition log end offset |
| `amyqueue_broker_consumer_group_lag` | gauge | Per-group per-partition lag |
| `amyqueue_broker_produce_requests_total` | counter | Total produce requests received |
| `amyqueue_broker_fetch_requests_total` | counter | Total fetch requests received |
| `amyqueue_broker_bytes_in_total` | counter | Total bytes received from producers |
| `amyqueue_broker_bytes_out_total` | counter | Total bytes sent to consumers |

### Phase 3 — RPC latency histograms

Once the Raft RPC layer is stable, add latency tracking:

| Metric | Type | Description |
|---|---|---|
| `amyqueue_raft_rpc_duration_seconds` | histogram | Per RPC type (vote, append_entries, observer_join) |
| `amyqueue_raft_heartbeat_rtt_seconds` | histogram | Round-trip time per peer |
| `amyqueue_broker_produce_duration_seconds` | histogram | End-to-end produce latency |

### Phase 4 — Alerting rules

Prometheus alerting rules (in `deploy/prometheus/alerts.yml`):

```yaml
# Leader instability
- alert: RaftLeaderUnstable
  expr: rate(amyqueue_raft_leader_changes_total[5m]) > 0.1
  for: 2m
  annotations:
    summary: "Leader changing more than once every 10 minutes"

# Observer falling behind
- alert: ObserverLagHigh
  expr: amyqueue_raft_observer_lag > 100
  for: 1m
  annotations:
    summary: "Observer {{ $labels.observer_id }} is {{ $value }} entries behind"

# Quorum at risk
- alert: VoterCountLow
  expr: amyqueue_raft_voter_count < 3
  annotations:
    summary: "Cluster has fewer than 3 voters — cannot tolerate any failure"
```

### Phase 5 — OpenTelemetry traces

Distributed tracing for cross-node RPC flows. A produce request that spans
producer → broker → replication → consumer acknowledgement. This requires
trace context propagation in the wire protocol — a larger change, deferred
until the broker protocol is stable.

---

## Scrape configuration (Prometheus)

```yaml
# prometheus.yml
scrape_configs:
  - job_name: amyqueue-controllers
    static_configs:
      - targets:
          - localhost:9090   # ctrl-1
          - localhost:9091   # ctrl-2
          - localhost:9092   # ctrl-3
    scrape_interval: 15s
    metrics_path: /metrics
```

Each controller runs its own metrics server. Prometheus scrapes all three
independently. Labels (`node_id`) distinguish them in queries and dashboards.

---

## Grafana dashboard (planned)

Key panels for an AmyQueue controller dashboard:

- **Leader timeline** — `amyqueue_raft_is_leader` per node over time; shows
  when leadership changed and to whom
- **Term progression** — `amyqueue_raft_current_term`; a fast-rising term
  means repeated elections (network partition or misconfiguration)
- **Commit index rate** — `rate(amyqueue_raft_commit_index[1m])`; throughput
  of the consensus log
- **Observer lag** — `amyqueue_raft_observer_lag` per observer; should trend
  toward 0 after joining
- **Election rate** — `rate(amyqueue_raft_elections_total[5m])`; should be
  near zero in a healthy cluster
