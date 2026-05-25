# Phase 1 — Topic & Partition Model: Design Discussion

> Living doc. Updated throughout the design session before implementation begins.

---

## Questions Being Discussed

- [ ] What fields belong in a Topic record?
- [ ] What fields belong in a Partition record?
- [ ] What are the design challenges?
- [ ] How do we handle ISR at creation time?
- [ ] Epoch fencing strategy?
- [ ] How do we represent "no leader" cleanly?
- [ ] Deletion cascade — topic delete + partition cleanup?
- [ ] Under-replication — desired vs actual replicas?

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
| `Leader` | `BrokerID` | Current leader. Empty string = offline / no leader yet. |
| `LeaderEpoch` | `int32` | Increments on every leader change. Used for epoch fencing by brokers. |
| `PartitionEpoch` | `int32` | Increments on every update (not just leader change). Guards against stale concurrent proposals. |

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

_(none yet — filling in as we discuss)_

---

## Deferred to Later Phases

- Partition reassignment (adding/removing replicas)
- Log end offset (LEO) tracking — Phase 7
- HighWatermark — broker-local, not controller
