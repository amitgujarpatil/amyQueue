# Phase 1 — Topic & Partition Metadata Model: Final Design

> This document is the authoritative design record for Phase 1.
> It captures finalized decisions, the data model, open questions, and the reasoning behind every choice.
> The working discussion trail lives in `phase1-topic-partition-design.md`.

---

## What Phase 1 Is

In-memory metadata model on the controller. No network, no persistence yet. Every other phase builds on this foundation.

The controller needs to know — for every topic — how many partitions it has, which brokers hold replicas, who the current leader is, and which replicas are in sync. Phase 1 defines those records and the store that holds them.

---

## Finalized Data Model

### Types

```go
type TopicID     string   // UUID — canonical topic key
type BrokerID    string   // broker identifier
type PartitionStatus string

const (
    PartitionOnline  PartitionStatus = "online"   // leader elected, accepting writes
    PartitionOffline PartitionStatus = "offline"  // all ISR brokers dead, writes blocked
)

type PartitionKey struct {
    TopicID     TopicID
    PartitionID int32
}
```

`TopicID` and `BrokerID` are typed strings — not raw `string`. This prevents accidentally passing a broker ID where a topic ID is expected and vice versa.

`PartitionKey` is a comparable Go struct — safe as a map key, no string allocation per lookup.

---

### Topic

```go
type Topic struct {
    TopicID           TopicID
    Name              string
    NumPartitions     int32
    ReplicationFactor int32     // desired RF — actual may be lower at birth
    Internal          bool      // true for __consumer_offsets; user cannot delete
    Config            TopicConfig
}
```

`TopicID` is the canonical reference used everywhere internally. `Name` is the human-readable secondary index — renaming a topic only touches the name index, not all references to the topic.

`ReplicationFactor` records the desired number of replicas. Actual replica count may be lower if fewer brokers were available at creation time. The gap is detectable by comparing `len(partition.Replicas)` against this value — no extra field needed.

`Config` is embedded directly in the `Topic` struct and must travel inside the `CreateTopicPayload` log entry. If config were stored separately, followers replaying the log could reconstruct different config if defaults changed between versions — guaranteed divergence.

---

### TopicConfig

```go
type TopicConfig struct {
    RetentionMs     int64   // -1 = unlimited
    RetentionBytes  int64   // -1 = unlimited
    SegmentBytes    int64
    MinISR          int32   // minimum ISR size required to accept writes
    MaxMessageBytes int32
    CleanupPolicy   string  // "delete" | "compact"
}
```

`MinISR` must be validated at the HTTP layer: reject any `CreateTopic` request where `MinISR > ReplicationFactor` before it reaches the state machine. If it somehow reaches `applyCreateTopic`, the partition is created but writes will always be blocked — operator error, not a system bug.

---

### PartitionState

```go
type PartitionState struct {
    TopicID        TopicID
    PartitionID    int32
    Status         PartitionStatus  // Online or Offline — authoritative state
    Replicas       []BrokerID       // ordered; Replicas[0] = preferred leader
    ISR            []BrokerID       // in-sync replicas; subset of Replicas
    Leader         BrokerID         // empty string when Status == Offline
    LeaderEpoch    int32            // increments on every leader change
    PartitionEpoch int32            // increments on every state update
}
```

**Key invariants:**
- `Status` and `Leader` are always written together inside a single `Apply` call — never independently
- When `Status == Online`: `Leader` holds a valid BrokerID, `ISR` is non-empty
- When `Status == Offline`: `Leader == ""`, `ISR` may be empty
- Never infer state from `Leader == ""` alone — always read `Status`
- `Replicas[0]` is the preferred leader. If leadership changes, `Replicas[0]` does not change — it remains the preferred leader for future elections. Only `Leader` and `LeaderEpoch` change.

---

### Store

```go
type Store struct {
    mu           sync.RWMutex
    topics       map[TopicID]*Topic         // canonical index
    topicsByName map[string]TopicID         // secondary index for name lookups
    partitions   map[PartitionKey]*PartitionState  // flat map — see D2
    version      int64                      // increments on every Apply
}
```

Topic lock and partition map lock are the same `mu` — but topic reads and partition reads are both under `RLock`, so they never block each other. The flat partition map means no nested locking on topic → partition access.

`version` increments on every successful `Apply`. Returned in heartbeat responses so brokers detect stale local metadata and re-fetch (Phase 5+).

---

## Design Decisions

### D1 — Partition Assignment: Globally Aware Greedy

**Problem:** Simple round-robin assigns each topic's partitions independently. With enough topics, every broker accumulates leaders from every topic. A single broker failure takes down leaders from all topics simultaneously — maximum blast radius.

**Why we discussed it:** We wanted a guarantee that a single broker failure impacts the minimum number of topics. Round-robin cannot provide this — it has no awareness of what other topics have already placed on a given broker.

**Decision:** Use a globally aware greedy algorithm with a scoring function:

```
score(broker) = (total_leaders_on_broker × α)
              + (leaders_from_THIS_topic_on_broker × β)

α = 1           — load balance weight
β = numBrokers  — failure domain isolation weight
```

For each partition being assigned, pick the broker with the lowest score. Exclude brokers already holding any replica for this partition.

`β >> α` means failure domain isolation always wins over pure load balance — the algorithm forces each topic's leaders onto different brokers first, then load-balances the remainder.

**Example — 5 brokers, 3 topics, 1 partition each, RF=2:**

| Partition | Leader | Follower |
|---|---|---|
| Topic A, P0 | B0 | B3 |
| Topic B, P0 | B1 | B4 |
| Topic C, P0 | B2 | B3 |

```
B0 fails → Topic A loses 1 leader    (1 topic impacted)
B1 fails → Topic B loses 1 leader    (1 topic impacted)
B2 fails → Topic C loses 1 leader    (1 topic impacted)
B3 fails → A and C lose a follower   (0 leaders lost)
B4 fails → B loses a follower        (0 leaders lost)
```

Any single broker failure impacts at most 1 topic losing a leader.

**Function signature — extended from the roadmap's original:**

```go
// original (roadmap)
AssignReplicas(brokers []BrokerID, numPartitions, replicationFactor int32) [][]BrokerID

// final — takes current global placement for blast-radius awareness
AssignReplicas(
    brokers           []BrokerID,
    numPartitions     int32,
    replicationFactor int32,
    leaderCounts      map[BrokerID]int,  // current leader count per broker across ALL topics
) [][]BrokerID
```

The state machine reads `leaderCounts` from the store immediately before calling this function. The function remains pure — same inputs always produce same outputs — and deterministic across all controller replicas.

**User guidance:** Set `numPartitions` to a multiple of `numBrokers` for even leader distribution. With fewer partitions than brokers, some brokers hold 0 leaders for that topic — unavoidable, not a bug.

---

### D2 — Partition Storage: Flat Map

**Problem:** If partitions are stored as `Topic.Partitions []PartitionState`, reading a single partition requires locking the entire topic. With high-throughput ISR updates and heartbeat processing in Phases 5–7, this is a read bottleneck.

**Decision:** Partition state lives in a flat `map[PartitionKey]*PartitionState` on the store, entirely separate from the `Topic` struct.

**Properties:**
- Topic reads and partition reads are independent — no nested locking
- `PartitionKey` is a comparable Go struct, usable directly as a map key with no allocation
- Deletion cascade: `DeleteTopic` iterates `0..NumPartitions-1` and removes each `PartitionKey` explicitly. The entire operation happens inside a single `Apply` call under the state machine's mutex — no partial delete is ever visible to readers.

---

### D3 — Leader State: Explicit `PartitionStatus` Enum

**Problem:** `Leader == ""` is ambiguous. Before we resolved D4, it could mean either "leader not yet assigned" (temporary, harmless) or "leader died, no ISR member available" (alarming, writes blocked). Conflating these makes the "offline partition count" metric wrong and gives producers the wrong error.

**How D4 changed this:** Once we decided the leader is set at creation time (same as Kafka), there is no "leader not yet assigned" window. A partition is born Online with a leader, or born Offline if no brokers were available. The `Creating` state we initially considered is unnecessary.

**Final enum:**
```go
type PartitionStatus string

const (
    PartitionOnline  PartitionStatus = "online"
    PartitionOffline PartitionStatus = "offline"
)
```

**State transitions:**
```
(birth)  → Online    brokers available at creation — Leader=Replicas[0], ISR=Replicas
(birth)  → Offline   zero brokers at creation time
Online   → Offline   leader dies AND no alive ISR member to elect (Phase 6)
Offline  → Online    leader election succeeds — clean or unclean (Phase 6)
```

---

### D4 — ISR at Creation: ISR = Replicas

**Problem:** At creation, replicas are assigned but no replication is happening — brokers haven't been told about the partition yet (LeaderAndISR push is Phase 9). Is ISR = Replicas correct or dangerously optimistic?

**Options we considered:**

| Option | ISR value | Problem |
|---|---|---|
| A | `Replicas` | Concern: ISR may include brokers not yet replicating |
| B | `[Replicas[0]]` leader only | Writes blocked until follower joins ISR — requires Phase 7 to be live first |
| C | `[]` empty | Partition born Offline; extra state transition before first write |

**Decision: Option A — ISR = Replicas.**

This is not an optimistic assumption — it is mathematically correct. At creation, LEO = 0 for every replica. ISR membership means "replica LEO is within acceptable lag of leader LEO." Lag = 0 for all replicas at creation. They are provably in sync.

ISR shrink (Phase 7) takes over from the first write: any replica that fails to start replicating within `replica.lag.time.max.ms` is removed. That is the designed correction path.

**Kafka alignment:** Kafka KRaft sets `ISR = Replicas`, `Leader = Replicas[0]`, `LeaderEpoch = 0` at creation. AmyQueue does the same. Leader is not deferred to a separate phase — the state machine has the live broker list at apply time and sets the leader immediately.

**Creation state for every partition:**
```
Leader         = Replicas[0]
ISR            = Replicas
LeaderEpoch    = 0
PartitionEpoch = 0
Status         = Online         (or Offline if Replicas is empty)
```

---

### D4 — Edge Cases

**1. A replica in ISR goes down before the first write**
ISR contains a dead broker. ISR shrink (Phase 7) removes it within `replica.lag.time.max.ms`. Until then: if MinISR is still satisfied by live ISR members, writes proceed. If not, writes are blocked — correct, the cluster is genuinely under-replicated. This window also exists in Kafka; it is accepted and bounded.

**2. Zero brokers at creation time**
Assignment returns empty replica list. `applyCreateTopic` detects `len(Replicas) == 0`. Partition born Offline: `Status=Offline`, `Leader=""`, `ISR=[]`, `Replicas=[]`. Topic record is still created. When brokers register and partition is re-assigned, it transitions to Online.

**3. Fewer brokers than ReplicationFactor (under-replication at birth)**
Example: RF=3, only 2 brokers alive. Assignment assigns 2 replicas: `Replicas=[B0, B1]`, `ISR=[B0, B1]`. `ReplicationFactor=3` is preserved in Topic — the desired RF is on record. Under-replication is detectable by `len(partition.Replicas) < topic.ReplicationFactor` — no extra field needed.

**4. Leader dies before Phase 9 (LeaderAndISR push) is live**
Controller detects death via heartbeat timeout (Phase 5). Elects new leader from ISR (Phase 6) — `LeaderEpoch` increments. Brokers learn via the next push (Phase 9). Until then they serve stale metadata — same bounded gap Kafka tolerates.

**5. MinISR > ReplicationFactor**
Validation at HTTP layer rejects this before the log entry is written. If it reaches `applyCreateTopic`, partition is created but writes are always blocked — operator misconfiguration.

**6. Duplicate CreateTopic (Raft replay or client retry)**
`applyCreateTopic` checks if topic name already exists. If yes: log warning, return nil (no-op). Handles both Raft log replay on restart and client retries transparently.

---

## Decisions (continued)

### D5 — Epoch Fencing: State Machine Owns the Epoch ✓

**Decision:** The proposer carries `ExpectedEpoch` (the current epoch at propose time). The state machine is the only place that increments it. Transport is Raft, not HTTP.

See full discussion and diagram → `phase1-epoch-fencing.md`

**Updated payload:**
```go
type UpdatePartitionPayload struct {
    Key           PartitionKey
    NewLeader     BrokerID
    NewISR        []BrokerID
    ExpectedEpoch int32    // CAS guard — current PartitionEpoch at propose time
    Reason        string   // "leader_death" | "isr_shrink" | "isr_expand" | "unclean_election"
}
```

**`applyUpdatePartition` logic:**
```
if incoming.ExpectedEpoch != current.PartitionEpoch → log warning, return nil (no-op)

else:
    current.Leader          = incoming.NewLeader
    current.ISR             = incoming.NewISR
    current.Status          = Online | Offline
    current.LeaderEpoch    += 1  (only if leader changed)
    current.PartitionEpoch += 1  (always, on any successful apply)
```

**Why state machine owns it:** epoch increments in exactly one place — inside `Apply` under the state machine mutex. The proposer never computes a new epoch value, it only states what it observed. Pure compare-and-swap semantics.

**Why Raft not HTTP:** all controllers must apply updates in the same order. HTTP calls are lost on crash. Only Raft provides the ordering and durability guarantees needed for consistent state across the controller cluster.

## Still Open

### Open 1 — Deletion Cascade Atomicity

**Problem:** `DeleteTopic` must remove the Topic record plus all N partition entries from the flat map. Under the current design this happens inside one `Apply` call under the state machine mutex — appears atomic to readers. But:
- What if the process crashes mid-delete? On restart, Raft log replay re-runs `applyDeleteTopic` from the committed entry — the operation replays fully. This is safe as long as `applyDeleteTopic` is idempotent (deleting an already-missing key is a no-op in Go maps).
- Needs explicit confirmation that idempotent delete is the agreed contract before implementation.

---

## Deferred to Later Phases

| Topic | Phase |
|---|---|
| Partition reassignment (adding/removing replicas) | Future |
| Log end offset (LEO) tracking | Phase 7 |
| ISR shrink and expand | Phase 7 |
| HighWatermark | Broker-local, not controller |
| LeaderAndISR push to brokers | Phase 9 |

---

## Files This Design Produces

| File | Contents |
|---|---|
| `src/internal/metadata/model.go` | `TopicID`, `BrokerID`, `PartitionStatus`, `PartitionKey`, `Topic`, `TopicConfig`, `PartitionState` |
| `src/internal/metadata/store.go` | `Store` with `sync.RWMutex`; `CreateTopic`, `GetTopic`, `GetTopicByName`, `ListTopics`, `DeleteTopic`, `GetPartition`, `UpdatePartition`, `Version` |
| `src/internal/metadata/assignment.go` | `AssignReplicas(brokers, numPartitions, replicationFactor, leaderCounts)` — pure function, fully testable |
