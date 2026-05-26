# Phase 1 — Topic & Partition Model: Design Discussion

> Living doc. Updated throughout the design session before implementation begins.

---

## Questions Being Discussed

- [ ] What fields belong in a Topic record?
- [ ] What fields belong in a Partition record?
- [ ] What are the design challenges?
- [x] How do we handle ISR at creation time?
- [ ] Epoch fencing strategy?
- [ ] How do we represent "no leader" cleanly?
- [x] Deletion cascade — topic delete + partition cleanup?
- [ ] Under-replication — desired vs actual replicas?
- [x] Partition assignment algorithm — how to minimize blast radius when a broker goes down?
- [x] Flat partition map vs nested inside Topic?
- [x] How do we represent "no leader" cleanly?

---

## Topic Record — Candidate Fields

| Field | Type | Notes |
|---|---|---|
| `TopicID` | `string` (UUID) | Canonical key. Decouples all references from name. |
| `Name` | `string` | Human-readable. Secondary index. Rename only touches the index. |
| `NumPartitions` | `int32` | Fixed at creation time for now. |
| `ReplicationFactor` | `int32` | Desired replication — actual may be lower if brokers unavailable. |
| `Internal` | `bool` | True for `__consumer_offsets`. User cannot delete internal topics. |
| `Config` | `TopicConfig` | Embedded — must travel in log entry for deterministic replay. |

### TopicConfig — Candidate Fields

| Field | Type | Notes |
|---|---|---|
| `RetentionMs` | `int64` | -1 = unlimited |
| `RetentionBytes` | `int64` | -1 = unlimited |
| `SegmentBytes` | `int64` | Segment rollover size |
| `MinISR` | `int32` | Minimum in-sync replicas required to accept writes |
| `MaxMessageBytes` | `int32` | Max single message size |
| `CleanupPolicy` | `string` | `"delete"` or `"compact"` |

---

## Partition Record — Candidate Fields

| Field | Type | Notes |
|---|---|---|
| `TopicID` | `string` (UUID) | Parent topic reference |
| `PartitionID` | `int32` | 0-indexed within topic |
| `Replicas` | `[]BrokerID` | Ordered. Index 0 = preferred leader (Kafka semantics). |
| `ISR` | `[]BrokerID` | In-sync replicas. Subset of Replicas. |
| `Status` | `PartitionStatus` | `Online` or `Offline`. Authoritative state — never derive status from `Leader == ""`. See D3. |
| `Leader` | `BrokerID` | Current leader BrokerID. Set to `Replicas[0]` at creation. Empty only when `Status == Offline`. |
| `LeaderEpoch` | `int32` | Starts at 0. Increments on every leader change. Used for epoch fencing by brokers. |
| `PartitionEpoch` | `int32` | Starts at 0. Increments on every state update. Guards against stale concurrent proposals. |
| `ISR` | `[]BrokerID` | In-sync replicas. Set to `Replicas` at creation (mathematically correct at LEO=0). See D4. |

---

## Design Challenges & Open Questions

### 1. Flat Partition Map vs Nested Inside Topic
**Problem:** If partitions are stored as `Topic.Partitions []PartitionState`, a reader of any single partition must lock the whole topic.
**Proposed solution:** Keep a separate flat `map[PartitionKey]*PartitionState`. Topic lock and partition map lock are independent.
**Open question:** Does the flat map make deletion harder? (Must scan/delete all keys with a given TopicID.)

### 2. ISR at Creation Time
**Problem:** At creation, no replica has started replicating yet. Should `ISR = Replicas` (optimistic) or `ISR = []` (pessimistic)?
- Optimistic (`ISR = Replicas`): writes can proceed immediately, but ISR may include brokers that haven't actually caught up.
- Pessimistic (`ISR = []`): partition is offline until brokers report in. Safer but adds latency to first write.
**Open question:** Which do we adopt for now?

### 3. Epoch Fencing — Race Between Concurrent Proposals
**Problem:** Two concurrent events (e.g., two broker deaths reported slightly differently) can each trigger a `UpdatePartition` proposal for the same partition. Both get committed. The second one may be stale.
**Proposed solution:** `PartitionEpoch` checked in `applyUpdatePartition`: if `incoming.PartitionEpoch != current + 1` → no-op.
**Open question:** Who generates the new epoch — the proposer (HTTP handler) or the state machine?

### 4. Empty Leader Representation
**Problem:** Leader is empty at creation and also when partition goes offline. These are two different states (creating vs offline).
**Open question:** Should we have an explicit `PartitionStatus` enum (`Creating`, `Online`, `Offline`) or just rely on `Leader == ""`?

### 5. Config in the Log Entry
**Problem:** If `TopicConfig` is stored separately from the log entry, followers can reconstruct a different topic config on replay (e.g., if defaults changed between versions).
**Proposed solution:** `TopicConfig` is always embedded inside `CreateTopicPayload` — no separate config lookup ever.

### 6. Under-Replication Representation
**Problem:** At apply time there may be fewer live brokers than `ReplicationFactor`. Topic is still created, but in under-replicated state.
**Open question:** How do we expose this? `len(Replicas) < ReplicationFactor`? A separate flag? An `OfflineReplicas []BrokerID`?

### 7. Deletion Cascade
**Problem:** When a topic is deleted, all N partition records in the flat map must also be deleted. Must be atomic from the state machine's perspective.
**Open question:** Does `DeleteTopic` in the store iterate and delete all partition keys? What if it's interrupted?

### 8. PartitionKey Type
**Problem:** The key for the flat partition map must be a Go comparable type.
**Candidate:** `struct { TopicID string; PartitionID int32 }` — works as Go map key, clean, no string allocations.
**Open question:** Should `TopicID` be a type alias (`type TopicID string`) or a raw string?

---

## Decisions Made

### D1 — Partition Assignment: Globally Aware Greedy (blast-radius minimizing)

**Decision:** Use a globally aware greedy algorithm instead of simple round-robin.

**Problem with round-robin:** With basic round-robin, all topics independently start their leader assignment from the same or sequential broker. With enough topics and partitions, every broker accumulates leaders from every topic. When a broker goes down, leaders from all topics fail simultaneously — maximum blast radius.

**Algorithm: scored greedy with failure domain isolation**

For each partition being assigned a leader, score every candidate broker:

```
score(broker) = (total_leaders_on_broker × α)
              + (leaders_from_THIS_topic_on_broker × β)
```

Pick the broker with the **lowest score**. Exclude brokers already holding any replica for this partition.

- `α = 1` — load balance: prefer brokers with fewer total leaders
- `β = numBrokers` — blast radius: heavily penalise putting two leaders from the same topic on the same broker

`β >> α` ensures failure domain isolation wins over pure load balance.

For followers (replica[1..n]): same scoring, additionally exclude the already-chosen leader broker.

**Why β = numBrokers:** It makes one same-topic co-location as "expensive" as filling an entire broker's worth of leaders from other topics. In practice this forces the algorithm to spread each topic's leaders across different brokers first, then load-balance the remainder.

**Example — 5 brokers, 3 topics, 1 partition each, RF=2:**

Assign leaders greedily (all scores tied initially → pick lowest broker index):

| Step | Partition | Chosen Leader | Reason |
|---|---|---|---|
| 1 | Topic A, P0 | B0 | all empty, pick first |
| 2 | Topic B, P0 | B1 | B0 already has 1 leader |
| 3 | Topic C, P0 | B2 | B0, B1 already have 1 leader |

Assign followers (spread to least loaded, not same broker as leader):
- A-P0 follower → B3
- B-P0 follower → B4
- C-P0 follower → B3 (ties, B4 also fine)

**Blast radius result:**
```
B0 fails → Topic A loses 1 leader   (1 topic impacted)
B1 fails → Topic B loses 1 leader   (1 topic impacted)
B2 fails → Topic C loses 1 leader   (1 topic impacted)
B3 fails → A and C lose a follower  (0 leaders lost — ISR still has replicas)
B4 fails → B loses a follower       (0 leaders lost)
```

Any single broker failure impacts **at most 1 topic losing a leader**. Optimal for this configuration.

**Impact on function signature:**

The roadmap's original pure function signature:
```go
AssignReplicas(brokers []BrokerID, numPartitions, replicationFactor int32) [][]BrokerID
```

Must be extended to accept current global placement:
```go
AssignReplicas(
    brokers           []BrokerID,
    numPartitions     int32,
    replicationFactor int32,
    leaderCounts      map[BrokerID]int,  // current leaders per broker across ALL topics
) [][]BrokerID
```

The state machine reads `leaderCounts` from the store at apply time before calling this function. Still a pure function — same inputs always produce same output. Still deterministic across all controller replicas.

**Tradeoffs vs. simple round-robin:**

| | Round-robin | Greedy aware |
|---|---|---|
| Blast radius (5 brokers, 3 topics) | Up to 3 topics per failure | 1 topic per failure |
| Load balance | Perfect when partitions % brokers == 0 | Near-perfect, ±1 |
| Complexity | O(P) | O(P × B) where B = num brokers |
| Deterministic for AmyQueue? | Yes | Yes — store state is identical on all controllers at apply time |

**Note on numPartitions guidance:** Algorithm works best when `numPartitions` is a multiple of `numBrokers`. Users should be advised to set `numPartitions ≥ numBrokers` for even leader distribution. With fewer partitions than brokers, some brokers hold 0 leaders for that topic — unavoidable, not a bug.

---

### D2 — Partition Storage: Flat Map

**Decision:** Partition state lives in a flat `map[PartitionKey]*PartitionState` on the store, separate from the `Topic` struct.

**Rejected alternative:** `Topic.Partitions []PartitionState` — any read of a single partition requires locking the entire topic. With high-throughput ISR updates and heartbeat processing (Phases 5–7), this becomes a bottleneck.

**Flat map properties:**
- Topic lock and partition map lock are independent `sync.RWMutex` — readers of different partitions never block each other
- `PartitionKey` is a comparable Go struct: `struct { TopicID string; PartitionID int32 }` — no string allocations, safe as map key
- `TopicID` is `type TopicID string` (type alias) — prevents raw strings being passed where a TopicID is expected
- Deletion cascade: `DeleteTopic` iterates `0..NumPartitions-1` and deletes each key explicitly. Atomic within a single `Apply` call — state machine holds its own mutex for the duration, so no partial delete is visible to readers.

---

### D3 — Leader Representation: Explicit `PartitionStatus` Enum

**Decision:** Add a `Status PartitionStatus` field to `PartitionState`. Do not rely on `Leader == ""` alone.

**Original problem we discussed:** `Leader == ""` is ambiguous — it could mean either:
- Partition just created, leader not yet assigned — temporary, harmless
- Partition lost its leader, no ISR member available — alarming, writes blocked

Conflating these breaks the "offline partition count" metric (would incorrectly count partitions still initialising) and gives producers the wrong error.

**How D4 changed this:** Once we decided (D4) that the leader is set at creation time — same as Kafka — the "leader not yet assigned" state disappears entirely. A partition is born with a leader or born Offline. There is no in-between `Creating` state. This simplifies the enum.

**Final enum:**
```go
type PartitionStatus string

const (
    PartitionOnline  PartitionStatus = "online"   // leader elected, ISR valid, accepting writes
    PartitionOffline PartitionStatus = "offline"  // all ISR brokers dead, writes blocked
)
```

**Transition rules (enforced in state machine):**
```
(birth) → Online    created with live brokers available — leader = Replicas[0], ISR = Replicas
(birth) → Offline   created but zero brokers available at apply time (edge case — see D4)
Online  → Offline   leader broker dies AND no alive ISR member to take over (Phase 6)
Offline → Online    clean or unclean leader election succeeds (Phase 6)
```

**Invariant:** `Status` and `Leader` are always set together in a single `Apply` call — never independently. When `Status == Online`, `Leader` holds a valid BrokerID. When `Status == Offline`, `Leader` is empty string. Readers must check `Status` first, never infer state from `Leader == ""` alone.

---

### D4 — ISR at Creation Time: ISR = Replicas (Kafka-aligned)

**Decision:** At creation, set `ISR = Replicas`, `Leader = Replicas[0]`, `LeaderEpoch = 0`, `PartitionEpoch = 0`, `Status = Online`. Identical to Kafka KRaft behaviour.

---

#### The problem we discussed

When `applyCreateTopic` runs, replicas are assigned on paper but no replication is happening yet — brokers haven't been told about the partition (LeaderAndISR push is a later phase). So the question was: what is the correct value for `ISR` at the moment of creation?

---

#### Options we considered

**Option A — ISR = Replicas (optimistic label, but actually correct)**
All assigned replicas go into ISR immediately.
- Writes unblocked as soon as leader is elected
- Kafka does this
- Concern raised: ISR may contain brokers not yet replicating

**Option B — ISR = [Replicas[0]] (leader only)**
Only the preferred leader starts in ISR. Followers join after Phase 7 confirms they've caught up.
- Safer guarantee for `acks=all`
- Problem: with `MinISR=2` and `RF=2`, writes are blocked until the follower joins ISR — could take multiple heartbeat intervals
- Problem: leader is set at creation time (D3), so ISR = [leader] is valid, but followers can never join ISR until Phase 7 tracks LEO — a long wait

**Option C — ISR = [] (fully pessimistic)**
ISR starts empty, partition born Offline.
- Most conservative
- Rejected immediately: even after leader is elected, writes remain blocked until ISR fills. Creates an unnecessary extra state transition.

---

#### Why ISR = Replicas is mathematically correct, not a shortcut

The key insight: **at creation, LEO = 0 for every replica. There is nothing to be out of sync about.**

ISR membership means "this replica's LEO is within acceptable lag of the leader's LEO." At LEO=0, lag = 0 for every replica. They are all in sync by definition — not optimistically assumed, but provably true.

"Optimistic" would imply we're assuming something that might be false. At creation it is literally true.

The ISR shrink mechanism (Phase 7) then takes over: any replica that fails to start replicating within `replica.lag.time.max.ms` is removed from ISR. That is the designed correction path, not a workaround.

---

#### Leader is set at creation — same as Kafka

Kafka does not defer leader assignment. The moment `PartitionRecord` is written to the metadata log:
```
Leader        = Replicas[0]    ← preferred leader, immediately
ISR           = Replicas       ← all replicas, correct at LEO=0
LeaderEpoch   = 0
PartitionEpoch = 0
Status        = Online
```

AmyQueue does the same. Leader assignment is not deferred to a separate phase. The state machine has the live broker list at apply time — `Replicas[0]` is guaranteed alive because the assignment algorithm (D1) only assigns live brokers.

---

#### Edge cases and how each is handled

**Edge case 1 — A replica in ISR goes down before the first write**
- ISR contains a dead broker at this point
- ISR shrink (Phase 7) detects it within `replica.lag.time.max.ms` and removes it
- Until shrink runs: if `MinISR` is satisfied by remaining live ISR members, writes proceed. If not, writes are blocked — correct behaviour, the cluster is genuinely under-replicated.
- This window also exists in Kafka. It is an accepted and bounded gap.

**Edge case 2 — Zero live brokers at creation time**
- Assignment algorithm (D1) returns an empty replica list
- `applyCreateTopic` detects `len(Replicas) == 0`
- Partition is born `Offline`: `Status = Offline`, `Leader = ""`, `ISR = []`, `Replicas = []`
- Topic record is still created — the partition exists but cannot serve traffic
- When brokers register (Phase 2) and assignment is retried or partition is re-assigned, it transitions to Online

**Edge case 3 — Fewer live brokers than ReplicationFactor (under-replication at birth)**
- Example: RF=3 but only 2 brokers alive at apply time
- Assignment algorithm assigns only 2 replicas: `Replicas = [B0, B1]`
- `ISR = [B0, B1]` — correct for the replicas that exist
- `ReplicationFactor = 3` is preserved in the Topic record — records the *desired* RF
- `len(Replicas) < topic.ReplicationFactor` is the under-replication signal — computable on read, no extra field needed
- When a third broker registers, Phase 3 re-assignment can bring it up to full RF (future work)

**Edge case 4 — Leader (Replicas[0]) goes down before Phase 9 (LeaderAndISR push) is live**
- Controller detects broker death via heartbeat timeout (Phase 5)
- Controller elects new leader from ISR (Phase 6): picks first alive ISR member
- `LeaderEpoch` increments, new leader is written to metadata
- Brokers learn via the next LeaderAndISR push (Phase 9) — until then they serve stale state
- This is the same gap Kafka has between leader election and broker notification. Bounded by push latency.

**Edge case 5 — MinISR > len(ISR) at creation**
- Example: topic configured with `MinISR=3`, RF=2, so ISR starts with 2 members
- Writes with `acks=all` are blocked immediately — correct, the topic was misconfigured
- Controller should validate `MinISR <= ReplicationFactor` at the HTTP layer before writing to the log and reject the request with a clear error
- If it somehow reaches `applyCreateTopic`, the partition is created but writes will always be blocked until MinISR is reconfigured

**Edge case 6 — Duplicate CreateTopic for same topic name (replay / retried request)**
- `applyCreateTopic` checks if topic name already exists in store
- If yes: log a warning, return nil (no-op). Do not create a second topic with the same name.
- This handles Raft log replay on restart and client retries transparently.

---

#### The one invariant to document operationally

> ISR is accurate at creation (LEO=0 is provably in-sync for all replicas). ISR shrink (Phase 7) maintains accuracy from the first write onwards. Between creation and Phase 7 being live, a dead replica may remain in ISR beyond `replica.lag.time.max.ms` — this is a known bounded gap, the same window Kafka tolerates.

---

## Deferred to Later Phases

- Partition reassignment (adding/removing replicas)
- Log end offset (LEO) tracking — Phase 7
- HighWatermark — broker-local, not controller
