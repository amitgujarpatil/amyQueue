# Phase 2 — Broker Registration: Final Design

> This document is the authoritative design record for Phase 2.
> It captures finalized decisions, the data model, open questions, and the reasoning behind every choice.
> The working discussion trail lives in `phase2-broker-design.md`.

---

## What Phase 2 Is

Broker registration and identity on the controller. No partition assignment or replication yet — that is Phase 3+. Every subsequent phase assumes the broker identity and epoch model defined here.

The controller needs to know — for every broker — who is registered, where it lives, what epoch it is on, and whether it is active or shutting down. Phase 2 defines those records, the registration flow, the epoch fencing mechanism, and the controlled shutdown path.

---

## Finalized Data Model

### BrokerStatus

```go
type BrokerStatus string

const (
    BrokerStatusActive       BrokerStatus = "active"
    BrokerStatusShuttingDown BrokerStatus = "shutting_down"
)
```

`Dead` is NOT a `BrokerStatus` value. Death is ephemeral, detected by `LivenessTracker` (Phase 5) from heartbeat timeouts. It is never written to the Raft log. A broker can be `status=active` and `alive=false` simultaneously — these are separate concerns.

---

### BrokerInfo

```go
type BrokerInfo struct {
    BrokerID  BrokerID
    Host      string
    Port      int32
    RackID    string
    Epoch     int64
    Status    BrokerStatus
}
```

`BrokerID` is a typed string — same alias defined in Phase 1. Operator-configured. No auto-assignment.

`Epoch` is a controller-assigned int64. Increments on every registration event. The only fencing key that prevents zombie brokers. Nothing outside `applyRegisterBroker` may generate or increment it.

`RackID` is stored now even though the assignment algorithm ignores it. Changing `BrokerInfo` later is a Raft log schema change. Cheaper to carry an empty string today.

`Status` flows through the Raft log — it is durable. When a broker announces controlled shutdown, `applyShutdownBroker` sets this to `ShuttingDown`. The operator API and assignment algorithm both read it.

---

### Raft Payloads

```go
type RegisterBrokerPayload struct {
    BrokerID  BrokerID
    Host      string
    Port      int32
    RackID    string
    ClusterID string   // validated against store.ClusterID
    Token     string   // validated by ClusterAuth middleware before Apply
}

type ShutdownBrokerPayload struct {
    BrokerID      BrokerID
    ExpectedEpoch int64   // CAS guard — same pattern as D5 in Phase 1
}
```

Token validation happens in HTTP middleware before the payload ever reaches the state machine. `ClusterID` is re-validated inside `applyRegisterBroker` as defence-in-depth.

---

### Store Additions

`Store` from Phase 1 gains a broker map:

```go
type Store struct {
    mu           sync.RWMutex
    topics       map[TopicID]*Topic
    topicsByName map[string]TopicID
    partitions   map[PartitionKey]*PartitionState
    brokers      map[BrokerID]*BrokerInfo     // Phase 2
    ClusterID    string
    version      int64
}
```

---

### Store Broker Methods

```go
// RegisterBroker stores BrokerInfo and returns the assigned epoch.
// Same BrokerID + same host:port  -> returns current epoch (idempotent).
// Same BrokerID + different host:port -> epoch++, update address.
// New BrokerID -> epoch = 1.
RegisterBroker(info BrokerInfo) (epoch int64, err error)

// GetBroker returns the registered BrokerInfo or ErrNotFound.
GetBroker(id BrokerID) (*BrokerInfo, error)

// ListBrokers returns all registered brokers regardless of status.
// Used for operator visibility (GET /brokers).
ListBrokers() []*BrokerInfo

// ListActiveBrokers returns only brokers with Status == BrokerStatusActive.
// Used by D1 assignment algorithm — shutting_down brokers excluded from new assignments.
ListActiveBrokers() []*BrokerInfo

// SetBrokerStatus updates the broker status in the store.
// Used by applyShutdownBroker to mark Status = ShuttingDown.
SetBrokerStatus(id BrokerID, status BrokerStatus, expectedEpoch int64) error
```

`ListActiveBrokers` vs `ListBrokers` separation is load-bearing: the assignment algorithm (D1 from Phase 1) calls `ListActiveBrokers` so a broker announcing shutdown is immediately excluded from new partition assignments. `ListBrokers` is for operator APIs only.

---

## Design Decisions

### D1 — BrokerEpoch: State Machine Assigns, CAS Fencing

**Problem:** The zombie broker problem. A broker loses network at t=1, the controller elects a new leader at t=3, the original broker's network recovers at t=5 and it still believes it is leader. Without a fencing token both brokers accept writes and data diverges.

**Decision:** Every registration event — whether first-time or restart — causes `applyRegisterBroker` to increment `BrokerEpoch`. This is the only place it changes.

```
t=1  B3 registered Epoch=5
t=2  B3 loses network
t=3  Controller detects B3 dead, elects B1
t=5  B3 recovers, re-registers -> Epoch=6
     Any request B3 makes with Epoch=5 is REJECTED: stale epoch
     B3 must re-register and get Epoch=6 before it can act
     By then it learns it lost leadership
```

**Epoch assignment rules:**
- New `BrokerID`: `epoch = 1`
- Re-registration (restart): `epoch = current + 1`
- Idempotent retry (same `BrokerID` + same `host:port`): return current epoch unchanged

**Invariant:** `BrokerEpoch` is assigned in exactly one place — inside `applyRegisterBroker` under the state machine mutex. Nothing else may generate or increment it.

---

### D2 — Registration Flow

Non-leader returns `503 + { leaderID, leaderAddr }` — same redirect pattern already in the Raft HTTP admin.

```
BROKER                              CONTROLLER LEADER
  |                                       |
  |-- POST /brokers/register ------------>|
  |   { brokerID, host, port, rackID,    |
  |     clusterID, token }               |
  |                                    validate (middleware)
  |                                    propose RegisterBrokerRecord to Raft
  |                                    applyRegisterBroker:
  |                                      new broker   -> epoch = 1
  |                                      re-register  -> epoch++
  |                                      store BrokerInfo
  |<-- 200 { brokerEpoch, clusterID, ------|
  |          metadataVersion }           |
  |                                      |
  store epoch, begin heartbeating        |
  |-- POST /brokers/{id}/heartbeat ------>|
  |   { brokerID, epoch }               |
```

**Registration response:**

```json
{
  "brokerEpoch":     6,
  "clusterID":       "550e8400-e29b-41d4-a716-446655440000",
  "metadataVersion": 42
}
```

`metadataVersion = Store.version` at apply time. If the broker has a local metadata cache from a previous run with an older version, it discards it and waits for a fresh LeaderAndISR push (Phase 9). Including `clusterID` lets the broker detect if it accidentally connected to the wrong cluster.

---

### D3 — Registration Edge Cases

**Normal restart (same BrokerID):** `applyRegisterBroker` sees BrokerID exists, increments epoch, updates host:port. Old epoch immediately invalid.

**Broker moves to different IP:** Same BrokerID, different `host:port`. Address overwritten. Controller uses new address for future LeaderAndISR pushes.

**Controller failover mid-registration:** Broker sends POST, leader crashes. Raft entry may or may not have committed. New leader elected. Broker gets connection dropped. Broker retries to new leader via 503 redirect. Idempotency in `applyRegisterBroker` handles this correctly — if the entry committed, same `host:port` returns current epoch (no-op). If it did not commit, registration runs fresh.

**Two brokers with same BrokerID (operator error):** Second registration overwrites first. First broker fails next heartbeat with stale epoch. They ping-pong epochs until operator fixes config. Log a clear WARN on any registration from a different address for an existing BrokerID.

**Dead broker stays in store:** `BrokerInfo` is kept after a broker dies. Config (`host:port`, `rackID`) is still needed if it comes back. Liveness is tracked separately in `LivenessTracker` (Phase 5), never in `BrokerInfo`. Same as Kafka.

---

### D4 — BrokerID: Operator-Configured, Required, No Auto-Assignment

**Decision:** `BrokerID` must be set via config file or environment variable. Broker fails to start immediately if not set — no silent default, no auto-assignment.

```
AMYQUEUE_BROKER_ID=broker-us-east-1a
# or config:
broker_id: "broker-us-east-1a"
```

**Why no auto-assign:** Auto-assignment requires a distributed counter or UUID, which couples identity generation to registration. Kafka KRaft requires `node.id` to be explicitly configured — auto-generation is not supported in KRaft mode. A duplicate `BrokerID` is an operator error with clear detection (WARN log on re-registration from different address). A human-readable name (`broker-us-east-1a`) makes logs and operator API output self-explanatory.

---

### D5 — Cluster Authentication

See full design → `cluster-auth-design.md`

Every broker request carries `ClusterID` and `ClusterToken` validated by HTTP middleware before the payload reaches the state machine.

```go
type RegisterBrokerPayload struct {
    BrokerID  BrokerID
    Host      string
    Port      int32
    RackID    string
    ClusterID string   // validated against store.ClusterID
    Token     string   // validated by ClusterAuth middleware
}
```

Heartbeat and all subsequent broker calls carry the same ClusterID + Token headers. Middleware validates first. Stale epoch is a separate 403 after auth passes.

---

### D6 — Write Durability Guarantees: acks + MinISR

`MinISR` is a write refusal floor on the broker side: if `len(ISR) < MinISR`, all writes are refused even if a leader exists. It does not control partition online/offline status. Reads always work as long as a leader exists.

**The producer's acks setting:**

| acks | Write considered successful when |
|---|---|
| 0 | Never waits — fire and forget |
| 1 | Leader wrote it to its local log |
| all (-1) | Every current ISR member wrote it to their log |

`MinISR` only has teeth when `acks=all`. With `acks=1` the producer does not wait for any ISR member beyond the leader.

**The guarantee:**

```
acks=all + MinISR=2:
  Any write returned SUCCESS is on >= 2 brokers.
  You can lose any 1 broker without losing that data.

acks=1:
  The leader received it. No further guarantee.
```

**When ISR drops below MinISR:**

```
ISR = [B1]  len=1 < MinISR=2

Partition ONLINE  (B1 is still leader, reads work)
Writes REFUSED    (NOT_ENOUGH_REPLICAS error to producer)

Writes resume only when another ISR member comes back
```

**Produce request validation order (implemented in future phases):**

1. Check this broker is the leader — reject `NOT_LEADER` if not
2. Check `len(ISR) >= topic.Config.MinISR` — reject `NOT_ENOUGH_REPLICAS` if not
3. Write to local log
4. If `acks=all`: wait for all current ISR members to acknowledge
5. If `acks=1`: return success immediately after step 3
6. If `acks=0`: return success before step 3

MinISR check happens before writing. A write is never partially accepted.

---

### D7 — Controlled Shutdown (SIGTERM Path)

**Why it matters:**

```
CRASH (no controlled shutdown):
  t=0    Broker dies
  t=30s  Controller detects death via heartbeat timeout
  t=30s  Leader election triggered
  = 30 seconds of partition unavailability per broker

CONTROLLED SHUTDOWN:
  t=0    SIGTERM received
  t=~1s  Controller migrates leaders, commits to Raft
  t=~1s  Broker drains and exits
  = ~1 second of metadata refresh, no actual unavailability
```

In a 10-broker rolling restart: without controlled shutdown = 5 minutes of disruption. With it = 10 seconds.

**Shutdown sequence:**

```
BROKER                              CONTROLLER LEADER
  |                                      |
  | OS sends SIGTERM/SIGINT              |
  | stop accepting new connections       |
  |                                      |
  |-- POST /brokers/{id}/shutdown ------>|
  |   { epoch, clusterID, token }        |
  |                                   Propose ShutdownBroker -> Raft
  |                                   applyShutdownBroker: Status=ShuttingDown
  |                                   For each leader partition:
  |                                     ElectLeader (exclude shutting-down broker)
  |                                     Propose UpdatePartition -> Raft
  |                                   For each follower partition:
  |                                     Propose ISR shrink -> Raft
  |                                   Wait for all to commit
  |<-- 200 { success: true,         ----|
  |    migratedPartitions: 12,          |
  |    failedPartitions: [] }           |
  |                                     |
  | drain in-flight requests            |
  | flush log segments to disk          |
  | exit 0                              |
```

**Failure handling:**

No other ISR member for a partition: partition goes Offline after the broker exits. `failedPartitions` returned in response. Broker logs WARN and exits anyway — operator must investigate.

Controller unreachable: retry N times with backoff. After max retries: dirty shutdown. `LivenessTracker` detects death via heartbeat timeout (~30s).

SIGKILL: cannot be caught — dirty shutdown, 30s recovery via heartbeat timeout.

**Signal handling:**

| Signal | Behaviour |
|---|---|
| SIGTERM | Controlled shutdown — migrate leaders, drain, flush, exit |
| SIGINT | Same as SIGTERM |
| SIGKILL | Cannot catch — dirty shutdown, 30s recovery window |

**Configuration:**

```yaml
shutdown_timeout_ms: 30000          # max wait for controller to migrate all leaders
shutdown_drain_timeout_ms: 5000     # max wait for in-flight requests to finish
shutdown_max_retries: 3             # retries if controller is unreachable
shutdown_retry_backoff_ms: 5000
```

---

### D8 — Broker Startup Sequence

Five ordered steps. Each must succeed before the next begins.

```
Step 1 — Load and validate config
  Required: BrokerID, Host, Port, ClusterID, ClusterToken
  Fail fast with clear error if any missing — do not start with defaults

Step 2 — Register with controller
  POST /brokers/register with retry + exponential backoff
  Follow 503 redirects to find the leader
  On success: store BrokerEpoch, ClusterID, MetadataVersion locally

Step 3 — Start heartbeat goroutine (Phase 5)
  Send POST /brokers/{id}/heartbeat every BrokerHeartbeatMs
  Include epoch + metadataVersion in every heartbeat
  On stale epoch response: re-register (back to Step 2)

Step 4 — Wait for LeaderAndISR push (Phase 9)
  Controller pushes partition assignments after registration
  Broker builds PartitionStateCache from the push
  If no partitions assigned: proceed immediately

Step 5 — Start accepting client connections
  Only after PartitionStateCache is populated (or empty push received)
  Producers and consumers can now connect
```

Fail fast in Step 1 prevents silent misconfiguration. Steps 3–5 are implemented in later phases but the contract is defined here so each phase knows exactly what it owns.

---

### D9 — Operator HTTP API

Both endpoints require `ClusterToken` authentication — not public.

```
GET /brokers
  Returns all registered brokers with live status.

  Response 200:
  {
    "brokers": [
      {
        "brokerID":        "broker-us-east-1a",
        "host":            "10.0.0.1",
        "port":            9092,
        "rackID":          "us-east-1a",
        "epoch":           5,
        "status":          "active",
        "alive":           true,
        "lastHeartbeatMs": 1200
      }
    ]
  }

GET /brokers/{id}
  Returns a single broker. 404 if not registered.
  Same fields as above for a single broker object.
```

`status`: durable — from `BrokerInfo` in Store (`active` or `shutting_down`).
`alive`: ephemeral — from `LivenessTracker` (`true` if last heartbeat within `BrokerSessionTimeoutMs`).

These are separate concerns deliberately. A broker can be `status=active` and `alive=false` (crashed), or `status=shutting_down` and `alive=true` (mid-controlled-shutdown). Operators need both signals to act correctly.

---

## Still Open

None. All design questions for Phase 2 are resolved.

---

## Deferred to Later Phases

| Topic | Phase |
|---|---|
| LivenessTracker (heartbeat expiry, alive field) | Phase 5 |
| Broker death detection and leader election trigger | Phase 5–6 |
| LeaderAndISR push to brokers | Phase 9 |
| ISR shrink and expand | Phase 7 |
| LEO reporting from brokers | Phase 7 |
| Rack-aware assignment logic | Future |
| Multiple listeners per broker | Future |
| IncarnationID | Future |
| mTLS / certificate-based auth | Future |
| Per-broker feature/capability negotiation | Future |

---

## Design Challenges

**C1 — BrokerEpoch assigned in Apply only.** `applyRegisterBroker` is the only place that assigns and increments `BrokerEpoch`. Hard invariant — nothing else may generate an epoch.

**C2 — Lock ordering.** `applyCommitted` runs with `n.mu` held. `sm.Apply` must not acquire any lock that another goroutine holds while holding `n.mu`. `MetadataStateMachine`'s own `sync.RWMutex` is always acquired after `n.mu` — never the reverse.

**C3 — Bootstrapping: first broker, no partitions yet.** If `CreateTopic` was called before any broker registered, partitions were born Offline (D4 edge case 2 from Phase 1). When the first broker registers, Phase 3 does not auto-trigger reassignment — this needs a decision at Phase 3.

**C4 — RackID in future assignment.** When rack-aware assignment is implemented, add a third penalty term to D1's scoring function: putting a replica on the same rack as an existing replica for this partition. `gamma >> beta >> alpha` — rack diversity beats topic spread beats load balance.

---

## Metrics

**Controller-side (extends existing MetricsSource pattern):**

| Metric | Type | Description |
|---|---|---|
| registered_broker_count | Gauge | Total brokers in store |
| alive_broker_count | Gauge | Brokers with active heartbeat (Phase 5) |
| broker_registrations_total | Counter | Cumulative registration events |
| offline_partition_count | Gauge | Partitions with Status == Offline |
| under_replicated_partition_count | Gauge | Partitions where len(ISR) < RF |

**Broker-side (new Prometheus server on broker):**

| Metric | Type | Description |
|---|---|---|
| broker_partition_count | Gauge | Total partitions hosted |
| broker_leader_partition_count | Gauge | Partitions where this broker is leader |
| broker_follower_partition_count | Gauge | Partitions where this broker is follower |
| broker_under_replicated_partitions | Gauge | Hosted partitions where len(ISR) < RF |
| broker_isr_shrinks_total | Counter | ISR shrink events (Phase 7) |
| broker_isr_expands_total | Counter | ISR expand events (Phase 7) |
| broker_bytes_in_total | Counter | Bytes received from producers (future) |
| broker_bytes_out_total | Counter | Bytes sent to consumers (future) |
| broker_messages_in_total | Counter | Messages received from producers (future) |

---

## Files This Design Produces

| File | Contents |
|---|---|
| `src/internal/metadata/model.go` | `BrokerStatus`, `BrokerInfo` (extends Phase 1 model) |
| `src/internal/metadata/store.go` | `brokers map[BrokerID]*BrokerInfo`; `RegisterBroker`, `GetBroker`, `ListBrokers`, `ListActiveBrokers`, `SetBrokerStatus` |
| `src/internal/controller/broker_handler.go` | HTTP handlers: `POST /brokers/register`, `POST /brokers/{id}/heartbeat`, `POST /brokers/{id}/shutdown`, `GET /brokers`, `GET /brokers/{id}` |
| `src/internal/controller/state_machine.go` | `applyRegisterBroker`, `applyShutdownBroker` |
| `src/internal/broker/broker.go` | Startup sequence, SIGTERM handler, controlled shutdown client |
