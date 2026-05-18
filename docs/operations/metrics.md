# Metrics

Every controller node exposes a Prometheus-compatible `/metrics` endpoint on
`METRICS_PORT` (default `9090`). This page covers how to scrape it, what the
output looks like, useful queries, and how to run a local Prometheus + Grafana
stack against a development cluster.

---

## Endpoint

```
GET http://<node>:<METRICS_PORT>/metrics
```

Safe to call at any time — it takes a single read lock on the node, copies
state, and returns. No side effects.

```bash
curl http://localhost:9090/metrics
```

Example output:

```
# HELP amyqueue_raft_current_term Current Raft term.
# TYPE amyqueue_raft_current_term gauge
amyqueue_raft_current_term{node_id="ctrl-1"} 3

# HELP amyqueue_raft_is_leader 1 if this node is the current leader, 0 otherwise.
# TYPE amyqueue_raft_is_leader gauge
amyqueue_raft_is_leader{node_id="ctrl-1"} 1

# HELP amyqueue_raft_commit_index Highest log index committed by a quorum.
# TYPE amyqueue_raft_commit_index gauge
amyqueue_raft_commit_index{node_id="ctrl-1"} 47

# HELP amyqueue_raft_last_applied Highest log index applied to the state machine.
# TYPE amyqueue_raft_last_applied gauge
amyqueue_raft_last_applied{node_id="ctrl-1"} 47

# HELP amyqueue_raft_member_count Total cluster members (voters + observers).
# TYPE amyqueue_raft_member_count gauge
amyqueue_raft_member_count{node_id="ctrl-1"} 3

# HELP amyqueue_raft_voter_count Number of voting members.
# TYPE amyqueue_raft_voter_count gauge
amyqueue_raft_voter_count{node_id="ctrl-1"} 3

# HELP amyqueue_raft_observer_count Number of observer (non-voting) members.
# TYPE amyqueue_raft_observer_count gauge
amyqueue_raft_observer_count{node_id="ctrl-1"} 0

# HELP amyqueue_raft_observer_lag Log entries the observer is behind the leader (leader only).
# TYPE amyqueue_raft_observer_lag gauge
amyqueue_raft_observer_lag{node_id="ctrl-1",observer_id="ctrl-4"} 2

# HELP amyqueue_raft_elections_total Total elections started by this node.
# TYPE amyqueue_raft_elections_total counter
amyqueue_raft_elections_total{node_id="ctrl-1"} 1

# HELP amyqueue_raft_leader_changes_total Total times this node observed a new leader.
# TYPE amyqueue_raft_leader_changes_total counter
amyqueue_raft_leader_changes_total{node_id="ctrl-1"} 1

# HELP amyqueue_raft_heartbeat_failures_total Total failed AppendEntries RPCs per peer.
# TYPE amyqueue_raft_heartbeat_failures_total counter
amyqueue_raft_heartbeat_failures_total{node_id="ctrl-1",peer="localhost:7002"} 0
amyqueue_raft_heartbeat_failures_total{node_id="ctrl-1",peer="localhost:7003"} 0
```

---

## Port layout per controller node

| Port (default) | Variable | Purpose |
|---|---|---|
| `7001` | `RAFT_PORT` | Internal Raft TCP — peers only |
| `8080` | `HTTP_PORT` | Admin API — operators |
| `9090` | `METRICS_PORT` | Prometheus scrape endpoint |

Run three controllers on one machine (dev):

| Node | RAFT_PORT | HTTP_PORT | METRICS_PORT |
|---|---|---|---|
| ctrl-1 | 7001 | 8080 | 9090 |
| ctrl-2 | 7002 | 8081 | 9091 |
| ctrl-3 | 7003 | 8082 | 9092 |

---

## Prometheus scrape config

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

---

## Useful PromQL queries

**Which node is the current leader?**
```promql
amyqueue_raft_is_leader == 1
```

**Current term across all nodes** (should all agree):
```promql
amyqueue_raft_current_term
```

**Election rate over 5 minutes** (should be near zero in a healthy cluster):
```promql
rate(amyqueue_raft_elections_total[5m])
```

**Observer lag** (should trend toward 0 after a node joins):
```promql
amyqueue_raft_observer_lag
```

**Heartbeat failure rate per peer**:
```promql
rate(amyqueue_raft_heartbeat_failures_total[1m])
```

**Commit index advancing** (throughput of the consensus log):
```promql
rate(amyqueue_raft_commit_index[1m])
```

---

## What healthy looks like

| Metric | Healthy value |
|---|---|
| `amyqueue_raft_is_leader` | Exactly one node shows `1` at all times |
| `amyqueue_raft_current_term` | Same value across all nodes; grows slowly |
| `rate(amyqueue_raft_elections_total[5m])` | Near zero |
| `amyqueue_raft_voter_count` | ≥ 3 |
| `amyqueue_raft_observer_lag` | Trending toward 0 after join; stable at 0 |
| `amyqueue_raft_heartbeat_failures_total` | Not rising continuously |

---

## What to watch for

**Fast-rising term:**
`amyqueue_raft_current_term` climbing quickly means repeated elections. Usually
caused by network timeouts between nodes — check `RAFT_ELECTION_TIMEOUT_MS` vs
`CONTROLLER_HEART_BEAT_INTERVAL` ratio (should be at least 1:10).

**No leader (`is_leader` = 0 on all nodes):**
Cluster has lost quorum. Check `amyqueue_raft_voter_count` — if it has dropped
below 3, the cluster cannot elect a leader until a voter comes back.

**Observer lag stuck high:**
`amyqueue_raft_observer_lag` not decreasing means the observer is not receiving
replication. Check network connectivity between the observer and the leader.

**Heartbeat failures rising on one peer:**
One peer is unreachable. If it is a voter and stays unreachable, the cluster
will keep trying elections once the follower times out.

---

## Running Prometheus locally (dev)

```bash
# write prometheus.yml then run:
docker run -p 9999:9090 \
  -v $(pwd)/prometheus.yml:/etc/prometheus/prometheus.yml \
  prom/prometheus
```

Open `http://localhost:9999` to query metrics. Use port 9999 so it does not
clash with AmyQueue's default `METRICS_PORT=9090`.

---

## Running Grafana locally (dev)

```bash
docker run -p 3000:3000 grafana/grafana
```

1. Add Prometheus as a data source: `http://host.docker.internal:9999`
2. Create a dashboard with panels from the PromQL queries above

Planned panels:

| Panel | Query |
|---|---|
| Leader timeline | `amyqueue_raft_is_leader` |
| Term progression | `amyqueue_raft_current_term` |
| Commit index rate | `rate(amyqueue_raft_commit_index[1m])` |
| Observer lag | `amyqueue_raft_observer_lag` |
| Election rate | `rate(amyqueue_raft_elections_total[5m])` |
| Heartbeat failures | `rate(amyqueue_raft_heartbeat_failures_total[1m])` |

---

## Implementation

This section documents how the metrics system is built — what files own what,
how data flows from the Raft node to Prometheus, and what to touch when adding
a new metric.

### File map

```
src/internal/raft/metrics.go          — MetricsSource interface + MetricsSnapshot struct
src/internal/raft/node.go             — MetricsSnapshot() method + counter fields
src/internal/metrics/collector.go     — prometheus.Collector implementation
src/internal/metrics/server.go        — HTTP server wrapping /metrics
src/internal/config/config.go         — MetricsPort field, METRICS_PORT env var
src/cmd/controller/main.go            — wires everything together at startup
```

### Data flow on every Prometheus scrape

```
Prometheus server
  │
  │  GET /metrics  (every 15s)
  ▼
metrics.Server  (HTTP server on METRICS_PORT)
  │
  │  promhttp.HandlerFor(registry)
  ▼
metrics.Collector.Collect(ch)
  │
  │  c.src.MetricsSnapshot()   ← single read lock on node.mu, returns by value
  ▼
raft.Node.MetricsSnapshot()
  │
  └── copies: state, term, commitIndex, lastApplied, leaderID,
              members slice, observerLag map, counter values
  │
  ▼
Collector translates snapshot fields → prometheus.Metric values
  │
  └── sends each metric into ch
  │
  ▼
promhttp serialises all metrics into Prometheus text format
  │
  ▼
HTTP response body returned to Prometheus
```

The critical design point: `raft.Node` is locked for only one read during the
entire scrape. The snapshot is a value copy — the collector then works on it
with no locks held.

### The interface seam — why raft never imports prometheus

`src/internal/raft/metrics.go` defines two things:

```go
type MetricsSource interface {
    MetricsSnapshot() MetricsSnapshot
}

type MetricsSnapshot struct {
    NodeID            string
    State             State
    Term              uint64
    CommitIndex       uint64
    LastApplied       uint64
    LeaderID          string
    Members           []Member
    ObserverLag       map[string]int64   // leader only; empty on followers
    ElectionsStarted  uint64
    LeaderChanges     uint64
    HeartbeatFailures map[string]uint64  // peer addr → count
}
```

`raft.Node` implements `MetricsSource` — it knows nothing about Prometheus.
`metrics.Collector` holds a `raft.MetricsSource` — it knows nothing about Raft
internals. The only coupling is the `MetricsSnapshot` struct, which is pure Go
with no external dependencies.

Consequence: you can unit-test the collector by passing a fake `MetricsSource`
that returns a hardcoded snapshot. You can test Raft without a Prometheus
registry. You can swap the metrics backend (e.g. OpenTelemetry) by replacing
`metrics.Collector` without touching anything in `raft/`.

### Where counters live and when they increment

All counters are fields on `raft.Node` under `n.mu`:

| Field | Type | Incremented in | When |
|---|---|---|---|
| `electionsStarted` | `uint64` | `runCandidate()` | At the top, before sending vote requests |
| `leaderChanges` | `uint64` | `handleAppendEntries()` | When `req.LeaderID != n.leaderID` (new leader seen) |
| `heartbeatFailures[addr]` | `map[string]uint64` | `broadcastHeartbeat()` | When `SendAppendEntries` returns an error |

They are never reset — counters must only increase. Prometheus calculates the
rate (delta between scrapes) in PromQL using `rate()`.

### How MetricsSnapshot() is implemented

`node.go` — takes `n.mu` once, copies everything, returns by value:

```go
func (n *Node) MetricsSnapshot() MetricsSnapshot {
    n.mu.Lock()
    defer n.mu.Unlock()

    members := n.voters.Members()  // Members() takes VoterSet.mu internally

    // observer lag — only computed on leader; matchIndex only meaningful there
    observerLag := make(map[string]int64)
    if n.state == Leader {
        lastIdx := n.log.lastIndex()
        for _, m := range members {
            if m.State == NodeObserver {
                match := n.matchIndex[m.Addr]
                lag := int64(lastIdx) - int64(match)
                if lag < 0 { lag = 0 }
                observerLag[m.ID] = lag
            }
        }
    }

    // defensive copy of heartbeat failures map
    failures := make(map[string]uint64, len(n.heartbeatFailures))
    for addr, count := range n.heartbeatFailures {
        failures[addr] = count
    }

    return MetricsSnapshot{
        NodeID:            n.cfg.ID,
        State:             n.state,
        Term:              n.currentTerm,
        CommitIndex:       n.commitIndex,
        LastApplied:       n.lastApplied,
        LeaderID:          n.leaderID,
        Members:           members,
        ObserverLag:       observerLag,
        ElectionsStarted:  n.electionsStarted,
        LeaderChanges:     n.leaderChanges,
        HeartbeatFailures: failures,
    }
}
```

### How the Collector is wired — Describe vs Collect

Prometheus requires every custom collector to implement two methods:

**`Describe(ch chan<- *prometheus.Desc)`** — called once at registration time.
Sends the static descriptor for every metric this collector will ever produce.
A `prometheus.Desc` is just the metric name, help string, and label names —
no values yet.

**`Collect(ch chan<- prometheus.Metric)`** — called on every scrape. Reads the
snapshot and sends actual metric values. Uses `prometheus.MustNewConstMetric`
to pair a descriptor with a value and label values.

```go
// registration (in metrics.Server.NewServer)
reg := prometheus.NewRegistry()
reg.MustRegister(NewCollector(src))  // Describe() called here

// on each GET /metrics
collector.Collect(ch)  // called by promhttp handler
    s := c.src.MetricsSnapshot()
    ch <- prometheus.MustNewConstMetric(c.currentTerm, prometheus.GaugeValue, float64(s.Term), s.NodeID)
    // ... rest of metrics
```

A dedicated `prometheus.NewRegistry()` is used instead of the default global
registry. This means Go runtime metrics (memory, GC, goroutines) are not
included — only AmyQueue metrics are exposed. Cleaner output, no noise.

### How the server starts and shuts down

`metrics.Server` is a thin wrapper around `http.Server`:

```go
// startup (main.go)
metricsSrv := metrics.NewServer(cfg.MetricsPort, node)
metricsSrv.Start()   // non-blocking: ListenAndServe in a goroutine

// shutdown (main.go, on SIGINT/SIGTERM — reverse order)
metricsSrv.Stop()    // srv.Close() — closes listener, in-flight requests finish
adminSrv.Stop()
node.Stop()
```

Shutdown order matters: metrics and admin servers stop first so no new
requests come in, then the Raft node stops cleanly.

### Adding a new metric — step by step

1. **Add the field to `MetricsSnapshot`** (`raft/metrics.go`) if it is a new
   data point. If it is a counter, add the corresponding field to `raft.Node`
   and increment it at the right place in `node.go`.

2. **Expose it in `MetricsSnapshot()`** (`node.go`) — copy the value under
   `n.mu` into the snapshot struct.

3. **Add a `*prometheus.Desc` field to `Collector`** (`metrics/collector.go`)
   — one descriptor per metric, created in `NewCollector`.

4. **Send it in `Describe`** — add `ch <- c.yourNewDesc`.

5. **Emit it in `Collect`** — read from the snapshot and call
   `prometheus.MustNewConstMetric`.

No changes needed anywhere else. The server, config, and `main.go` are
unaffected by adding metrics.
