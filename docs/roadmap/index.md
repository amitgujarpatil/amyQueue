# Roadmap

## Where we are

The Raft controller cluster is fully working: leader election, log replication, dynamic membership, HTTP admin API, Prometheus metrics, structured logging. The broker is a complete stub.

The next milestone is **metadata management** — everything the system needs before a producer can produce and a consumer can consume. This covers 9 phases in dependency order. Each phase is independently shippable.

---

## Phase dependency order

```
Phase 1 (Topic & Partition Model)
  └── Phase 2 (Broker Registration)  ← CmdMetadata + StateMachine hook
        └── Phase 3 (Partition Assignment)
              ├── Phase 4 (Persistence)       ← can run parallel with Phase 5
              └── Phase 5 (Broker Heartbeat)  ← can run parallel with Phase 4
                    └── Phase 6 (Partition Leadership / ISR)
                          ├── Phase 7 (Offset Tracking / ISR shrink+expand)
                          └── Phase 8 (Consumer Group Coordinator)
                                └── Phase 9 (Controller-to-Broker Push)
```

---

## Phase 1 — Topic & Partition Metadata Model

**Status:** Not started

**Goal:** In-memory data model on the controller. No network, no persistence yet. Foundation every other phase builds on.

**Kafka parallel:** KRaft stores `TopicRecord` + `PartitionRecord` in the metadata log. UUID is the canonical key, name is a secondary index.

### Key decisions

- `TopicID` is a UUID string — canonical key; name is a secondary index. Decouples rename from all references.
- `Replicas []BrokerID` is ordered — index 0 is the preferred leader (Kafka semantics).
- `PartitionState` lives in a flat `map[PartitionKey]*PartitionState` separate from `Topic` — avoids nested lock contention.
- `TopicConfig` (retention, min-ISR, segment size) travels inside the log entry so followers reconstruct identical state without a separate config lookup.
- Empty `Leader` field is valid on creation — assignment happens in Phase 3.
- `Store.version` increments on every successful Apply — used by brokers to detect stale metadata (Phase 5+).

### New files

| File | Contents |
|---|---|
| `src/internal/metadata/model.go` | `TopicID`, `BrokerID`, `PartitionKey`, `Topic`, `TopicConfig`, `PartitionState`, `BrokerInfo` |
| `src/internal/metadata/store.go` | `Store` with `sync.RWMutex`; `CreateTopic`, `GetTopic`, `GetTopicByName`, `ListTopics`, `DeleteTopic`, `GetPartition`, `UpdatePartition`, `Version` |

---

## Phase 2 — Broker Registration

**Status:** Not started

**Goal:** Brokers announce themselves to the controller leader on startup. Registration is persisted through the Raft log so the controller cluster survives restarts.

**Kafka parallel:** KRaft brokers send `BrokerRegistrationRequest` → controller writes `RegisterBrokerRecord` into metadata log → replies with `BrokerEpoch`. Stale-epoch requests are rejected.

### Key decisions

- New `CmdMetadata CommandType = 2` in `messages.go`. Two-level dispatch: outer byte selects the state machine, inner `Type string` selects the operation (`"register_broker"`, `"create_topic"`, …).
- `StateMachine interface { Apply(entry LogEntry) error }` defined in `src/internal/raft/statemachine.go`. `Node` holds `sm StateMachine`, set via `WithStateMachine(sm)` before `Start()`.
- `BrokerEpoch` increments on each re-registration. Controller rejects commands from stale epoch.
- Registration uses the existing HTTP admin server — new `POST /brokers/register` endpoint. No gRPC yet.
- Non-leader controller returns `503 + LeaderAddr` redirect (same pattern as `ObserverJoinResponse`).

!!! warning "Lock ordering"
    `applyCommitted` runs with `n.mu` held. `sm.Apply` must not acquire any lock that another goroutine holds while also holding `n.mu`. `MetadataStateMachine`'s own `sync.RWMutex` is fine — it is always acquired after `n.mu`, never the reverse.

### New files

| File | Contents |
|---|---|
| `src/internal/raft/statemachine.go` | `StateMachine interface` |
| `src/internal/metadata/commands.go` | `MetadataCommandType` constants, `MetadataCommand`, `RegisterBrokerPayload`, encode/decode helpers |
| `src/internal/metadata/statemachine.go` | `MetadataStateMachine` implements `raft.StateMachine`; dispatches on `MetadataCommand.Type` |

### Modified files

- `src/internal/raft/messages.go` — add `CmdMetadata CommandType = 2`
- `src/internal/raft/node.go` — add `sm StateMachine` field; call `sm.Apply` in `applyCommitted`; add `WithStateMachine`
- `src/internal/api/metadata/http/admin.go` — add `POST /brokers/register`
- `src/cmd/controller/main.go` — construct and wire `MetadataStateMachine`
- `src/cmd/broker/main.go` — HTTP call to register on startup with retry

---

## Phase 3 — Partition Assignment

**Status:** Not started

**Goal:** When a topic is created, the controller assigns each partition to a set of brokers deterministically. Embedded in the `CreateTopic` log entry so all controllers reconstruct identical assignments on replay.

**Kafka parallel:** Rack-aware round-robin algorithm run at partition creation time. Replica list is `[preferredLeader, follower1, follower2]`. Deterministic given an ordered broker list.

### Key decisions

- Assignment runs inside `applyCreateTopic` in the state machine, **not** at propose time. All controllers must reach identical assignments using the same committed broker list.
- `CreateTopicPayload` carries `{ TopicID, Name, NumPartitions, ReplicationFactor, Config }` — does **not** carry pre-computed replica lists. The state machine computes them at apply time.
- `TopicID` (UUID) is generated by the HTTP handler before encoding the log entry — never inside `Apply`.
- Algorithm: striped round-robin. Sort brokers by `BrokerID` for determinism. Partition `i` gets replicas starting at `(i * spread) % numBrokers`.
- If not enough brokers at apply time: mark partitions leaderless, do not panic. Topic is created in under-replicated state.
- `Apply` must be idempotent: if topic name already exists, log warn and return nil.

### New files

| File | Contents |
|---|---|
| `src/internal/metadata/assignment.go` | `AssignReplicas(brokers []BrokerID, numPartitions int32, replicationFactor int32) [][]BrokerID` — pure function, fully testable |

### Modified files

- `src/internal/metadata/statemachine.go` — implement `applyCreateTopic`
- `src/internal/metadata/store.go` — add `CreateTopic(topic, []PartitionState)`
- `src/internal/api/metadata/http/admin.go` — add `POST /topics`

---

## Phase 4 — Metadata Persistence (Raft Log Durability)

**Status:** Not started

**Goal:** Controller cluster survives full restart. Both Raft hard state (term, votedFor) and the metadata state machine (reconstructed by log replay) must be durable.

**Kafka parallel:** KRaft writes log entries to segment files. State machine is reconstructed by replaying the log from the latest snapshot on startup.

### Key decisions

- Two separate concerns: (1) Raft hard state — must be flushed before responding to `VoteRequest` (safety invariant). (2) Metadata state — reconstructed by replaying committed log entries, no separate file needed.
- `raft.Storage interface` in `src/internal/raft/storage.go`. `Node` holds `storage Storage` (nil = in-memory, backward compatible).
- `votedFor` and `currentTerm` must be written to stable storage **before** returning `VoteGranted = true`. Crash before persistence + restart = two votes in same term = safety violation.
- Log truncation on conflict: `storage.TruncateLogFrom(prevIndex+1)` before writing new entries, otherwise truncated-in-memory entries reappear after restart.
- Recovery order in `Node.Start()`: load hard state → load log → `commitIndex = lastLogIndex` → `lastApplied = 0` → apply entries via state machine → begin normal Raft.
- File format: JSON newline-delimited append-only segments. One segment = one index range. Rotation at configurable size.

### New files

| File | Contents |
|---|---|
| `src/internal/raft/storage.go` | `HardState`, `Storage interface` |
| `src/internal/raft/storage/json/storage.go` | JSON file-based implementation. Paths: `data/<node-id>/raft/state.json`, `data/<node-id>/raft/log/` |
| `src/internal/metadata/snapshot.go` | `Snapshot(sm) ([]byte, error)`, `RestoreSnapshot(data) (*MetadataStateMachine, error)` |

### Modified files

- `src/internal/raft/node.go` — add `storage Storage`; call `SaveHardState` on term/votedFor change; `AppendLogEntries` in `appendOne`; load on `Start()`
- `src/internal/config/config.go` — add `DataDir string` (default `"./data"`)
- `src/cmd/controller/main.go` — construct storage, pass to node

---

## Phase 5 — Broker Heartbeat to Controller

**Status:** Not started

**Goal:** Controller knows which registered brokers are currently alive. Dead brokers trigger partition leader failover.

**Kafka parallel:** Brokers send `HeartbeatRequest` every 3 s. Controller marks broker dead after `broker.session.timeout.ms`. Triggers ISR changes and leader election.

### Key decisions

- Heartbeats are **not** written to the Raft log. Liveness is ephemeral — only the controller leader tracks it. On leader failover, the new leader starts fresh; brokers re-establish liveness within one heartbeat interval.
- `LivenessTracker` is separate from `MetadataStore` — leader-only in-memory state.
- When a broker times out: `LivenessTracker.StartSweep` fires `onDead(BrokerID)` → controller proposes `CmdDeregisterBroker` Raft entry → triggers Phase 6 partition election.
- Controller uses its own `time.Now()` when recording heartbeats — never trusts broker timestamp.
- Heartbeat payload: `{ BrokerID, Epoch, MetadataVersion int64 }`. Response includes `CurrentMetadataVersion` so broker can detect stale local state.

### New files

| File | Contents |
|---|---|
| `src/internal/metadata/liveness.go` | `LivenessTracker` with `RecordHeartbeat`, `AliveBrokers`, `StartSweep` |

### Modified files

- `src/internal/api/metadata/http/admin.go` — `POST /brokers/{id}/heartbeat`, `GET /brokers`
- `src/internal/config/config.go` — add `BrokerHeartbeatMs int` (default 3000), `BrokerSessionTimeoutMs int` (default 30000)
- `src/cmd/broker/main.go` — start heartbeat goroutine

---

## Phase 6 — Partition Leadership (ISR + Leader Election per Partition)

**Status:** Not started

**Goal:** For each partition, exactly one broker is the leader. Controller tracks ISR. When a broker dies, controller elects a new leader from ISR and persists the change via Raft.

**Kafka parallel:** Controller elects partition leader from ISR (first alive ISR member). `LeaderEpoch` increments per election. Unclean election is off by default.

### Key decisions

- ISR management is controller-owned in AmyQueue (not broker-owned like Kafka). Simpler, slightly slower ISR decisions — acceptable for this stage.
- `ElectLeader` is a pure function: `(partition, aliveBrokers, uncleanAllowed) → (newLeader, newISR, err)`. Deterministic, unit-testable.
- `PartitionEpoch` checked in `applyUpdatePartition`: if `incoming.PartitionEpoch != current + 1` → no-op (handles race between two concurrent leader proposals for the same partition).
- `UncleanLeaderElectionEnabled bool` default: false. If false and all ISR dead → partition offline.

### New files

| File | Contents |
|---|---|
| `src/internal/metadata/election.go` | `ElectLeader(...)` pure function |

### Modified files

- `src/internal/metadata/commands.go` — add `UpdatePartitionPayload{ ..., Reason string }`
- `src/internal/metadata/statemachine.go` — implement `applyUpdatePartition`
- `src/internal/metadata/liveness.go` — `onDead` callback triggers election
- `src/internal/metadata/store.go` — add `UpdatePartition`
- `src/internal/config/config.go` — add `UncleanLeaderElectionEnabled bool`

---

## Phase 7 — Offset Tracking (LogEndOffset per Partition)

**Status:** Not started

**Goal:** Controller knows the `LogEndOffset` (LEO) of each partition replica. Used to determine ISR membership.

### Key decisions

- Extend heartbeat payload with `PartitionOffsets []{ TopicID, PartitionID, LEO }`. Broker reports all hosted partitions' current LEO.
- `ReplicaLEO` lives in `LivenessTracker`, not `MetadataStore`. Ephemeral — resets on controller restart.
- ISR shrink/expand decisions still require Raft commitment even though the underlying data is ephemeral.
- ISR check: `leaderLEO - replicaLEO > ReplicaLagMaxOffset` → propose ISR shrink. ISR expand: replica alive AND caught up AND debounce one full heartbeat interval.
- `HighWatermark = min(LEO across ISR)` — tracked by the partition leader broker locally, not the controller.

### Modified files

- `src/internal/metadata/liveness.go` — extend `RecordHeartbeat` with offsets; add `ReplicaLEO` lookup; extend sweep for ISR shrink/expand
- `src/internal/api/metadata/http/admin.go` — extend heartbeat handler
- `src/internal/config/config.go` — add `ReplicaLagMaxOffset int` (default 4)
- `src/cmd/broker/main.go` — include per-partition LEO in heartbeat

---

## Phase 8 — Consumer Group Coordinator

**Status:** Not started

**Goal:** Controller tracks `__consumer_offsets` internal topic. Exposes `FindCoordinator` so consumers and producers can discover the broker that coordinates their group.

**Kafka parallel:** `hash(groupId) % numPartitions(__consumer_offsets)` → that partition's leader broker is the group coordinator.

### Key decisions

- `__consumer_offsets` is created as an internal topic via the full `CreateTopic` flow from Phase 3. Marked `Topic.Internal = true` — cannot be deleted by users.
- Auto-created at first cluster init via a `CmdClusterInit` log entry. Only fires once.
- `FindCoordinator`: `abs(fnv32(groupId)) % numPartitions` → look up that partition's leader broker → return `{ BrokerID, Host, Port }`.
- Consumer group metadata (`JoinGroup`/`SyncGroup`/`OffsetCommit`) is entirely broker-side. Controller only routes.

### New files

| File | Contents |
|---|---|
| `src/internal/metadata/coordinator.go` | `FindCoordinator(groupID, store) (*BrokerInfo, error)` — pure function using `hash/fnv` stdlib |

### Modified files

- `src/internal/metadata/model.go` — add `Topic.Internal bool`
- `src/internal/api/metadata/http/admin.go` — add `GET /groups/{id}/coordinator`
- `src/cmd/controller/main.go` — auto-create `__consumer_offsets` on fresh cluster start

---

## Phase 9 — Controller-to-Broker Push (LeaderAndISR)

**Status:** Not started

**Goal:** Controller pushes partition assignment changes to brokers proactively. Broker learns it is partition leader/follower and what its current `LeaderEpoch` is.

**Kafka parallel:** Controller sends `LeaderAndIsrRequest` to affected brokers whenever partition leadership changes. Broker updates local partition state and enforces epoch fencing.

### Key decisions

- Same pattern as Raft transport: `BrokerChannel interface` (domain side) + TCP adapter (transport side).
- New `BrokerAdminPort` TCP server on each broker. TCP tag namespace starts at 100 to avoid future Raft tag collisions.
- State machine fires `onPartitionUpdate` callback **after** `applyUpdatePartition`. Callback posts to a buffered channel; a separate goroutine drains and sends — prevents deadlock if TCP response handler acquires `n.mu`.
- Per-partition serial push queue (channel per partition key) — guarantees ordering: epoch E is sent before E+1 to the same partition.
- Broker maintains `PartitionStateCache map[PartitionKey]PartitionAssignment` (`sync.RWMutex`). Producer/consumer handlers (future) read it for epoch fencing.

### New files

| File | Contents |
|---|---|
| `src/internal/broker/admin.go` | `AdminService interface { HandleLeaderAndISR(...) }` |
| `src/internal/broker/messages.go` | `LeaderAndISRRequest`, `PartitionAssignment`, `LeaderAndISRResponse` |
| `src/internal/broker/tcp/admin.go` | TCP transport adapter |
| `src/internal/metadata/brokerchannel.go` | `BrokerChannel interface` + TCP client implementation |

### Modified files

- `src/internal/metadata/statemachine.go` — fire `onPartitionUpdate` callback
- `src/cmd/controller/main.go` — construct `BrokerChannel`, wire callback
- `src/cmd/broker/main.go` — start admin TCP server; implement `HandleLeaderAndISR`; maintain `PartitionStateCache`
- `src/internal/config/config.go` — add `BrokerAdminPort int` (default `GRPCPort + 1`)

---

## Cross-phase concerns

**Redirect handling (all phases):** Every RPC on a non-leader controller returns `{ LeaderID, LeaderAddr }`. HTTP: return `503` with body. Same pattern as `ObserverJoinResponse`. Define `NotLeaderError` in `metadata` package.

**Idempotency (Phases 1–3 state machine Apply):** Every `Apply` checks `entry.Index <= sm.lastApplied → return nil`. Create operations also check for existing name/ID and silently ignore duplicates.

**MetadataVersion:** `Store.version` increments on every `Apply`. Returned in heartbeat responses so brokers detect stale local state and re-fetch.

**Metrics:** Follow `raft.MetricsSource` pattern. Define `metadata.MetricsSource interface` with `MetricsSnapshot()`. Extend the existing metrics server. `metadata` package never imports `prometheus`. Key metrics: topic count, broker count, offline partition count, under-replicated partition count, ISR shrink rate.

**Keystone change:** Every phase from 2 onwards flows through a single line in `src/internal/raft/node.go` — the `applyCommitted` function where the comment currently reads *"CmdData entries: application state machine hook goes here in the future"*. Phase 2 replaces that comment with the `CmdMetadata` dispatch.