# Phase 1 — Deletion Cascade Atomicity: Discussion & Decision

> Covers: the deletion cascade problem, how Kafka KRaft solves it, and what
> AmyQueue commits to for Phase 1 and Phase 4.

---

## The Problem

When `applyDeleteTopic` runs it must remove three categories of records from the store:

```
1. store.topics[topicID]              ← the Topic record
2. store.topicsByName[name]           ← the name secondary index
3. store.partitions[{topicID, 0}]     ← partition 0
   store.partitions[{topicID, 1}]     ← partition 1
   ...
   store.partitions[{topicID, N-1}]   ← partition N-1
```

All of these must disappear atomically — no reader should ever see a half-deleted topic.

---

## Why the Mutex Already Solves the Reader Problem

The state machine holds `mu.Lock()` for the entire `Apply` call. Readers cannot
acquire `mu.RLock()` until Apply finishes. From any reader's perspective, either
all deletes have happened or none have. **The reader problem is already solved by
the mutex.**

---

## The Real Problem — Crash Mid-Delete

```
t=1   DeleteTopic committed to Raft log at index 42
t=2   applyDeleteTopic starts under mu.Lock():
        ✓ delete store.topics[topicID]
        ✓ delete store.topicsByName["orders"]
        ✓ delete store.partitions[{topicID, 0}]
        ✓ delete store.partitions[{topicID, 1}]
        ✗ CRASH — partitions 2..N-1 still exist in memory
```

For **Phase 1 (in-memory store):** crash = full memory wipe. On restart, Raft
log replay begins from index 1. When it reaches index 42, `applyDeleteTopic`
runs again from scratch and deletes everything cleanly. Not a real problem.

For **Phase 4 (persistence):** crash leaves orphaned partition records on disk.
On restart, log replay re-runs `applyDeleteTopic` at index 42. It tries to
delete partitions that may already be gone. If delete of a missing key is not a
no-op, this panics or corrupts state.

---

## The Subtle Problem — TopicID Must Be the Delete Key, Not Name

Consider this sequence in the Raft log:

```
index 42: DeleteTopic { topicID: UUID-A, name: "orders" }
index 43: CreateTopic { topicID: UUID-B, name: "orders" }   ← same name, new UUID
```

If `applyDeleteTopic` deletes by **name**: re-running index 42 after index 43
has applied would delete UUID-B's records. Silent data loss.

If `applyDeleteTopic` deletes by **TopicID (UUID)**: re-running index 42 only
touches UUID-A's records. UUID-B is untouched.

This is why TopicID as UUID is non-negotiable. Deletes are always scoped to a
specific UUID, never to a name.

---

## How Kafka KRaft Solves This

We looked at Kafka's approach before deciding, because AmyQueue should stay
aligned with Kafka's semantics wherever possible.

### Append-Only Metadata Log With Remove Record Types

Kafka never mutates existing log entries. Deletion is expressed as new records
appended at the end:

```
index 40:  TopicRecord            { topicID: UUID-A, name: "orders" }   ← create
index 41:  PartitionRecord        { topicID: UUID-A, partitionID: 0 }
index 42:  PartitionRecord        { topicID: UUID-A, partitionID: 1 }
...
index 98:  RemovePartitionRecord  { topicID: UUID-A, partitionID: 0 }   ← delete
index 99:  RemovePartitionRecord  { topicID: UUID-A, partitionID: 1 }
index 100: RemoveTopicRecord      { topicID: UUID-A }
```

No existing entry is ever changed. The log is always forward-only. There is no
such thing as "partial write" to an existing record.

### Copy-on-Write MetadataImage + Atomic Pointer Swap

Kafka maintains a `MetadataImage` — an immutable snapshot of current state. When
records are applied, Kafka constructs a **new image** and atomically swaps the
pointer:

```
Current image (readers holding a reference to this):
  topics:     { UUID-A: "orders" }
  partitions: { {UUID-A,0}: ..., {UUID-A,1}: ... }

Apply removal records → build NEW image:
  topics:     {}
  partitions: {}

Atomic pointer swap → new image becomes current
Old image stays valid until last reader releases it (GC)
```

Readers always hold a reference to a complete, consistent image. No mutex blocks
readers during apply. Old and new images coexist briefly — readers are never
interrupted.

### Crash Recovery via Snapshots, Not Full Replay

```
Metadata log:
  [ 1 .. 5000 ][ 5001 .. 9000 ][ 9001 .. current ]
       ↑               ↑
   snapshot A      snapshot B  (latest, serialised MetadataImage at offset 9000)

On restart:
  1. Load snapshot B  ← full MetadataImage, instant
  2. Replay only entries 9001..current  ← small delta
  3. Done — no replay from index 1
```

Snapshots are periodic serialisations of the full MetadataImage to disk. Crash
recovery is bounded — at worst, replay the delta since the last snapshot.
Remove records in the delta replay idempotently.

### Log Compaction Cleans Up Over Time

```
Before compaction:
  index 40:  TopicRecord(UUID-A)           ← create
  index 100: RemoveTopicRecord(UUID-A)     ← delete

After compaction:
  both entries eliminated — they cancel each other out
```

A node joining fresh replays a compacted log and never sees a topic that was
created and deleted before the compaction point.

### Summary of Kafka's Three Interlocking Mechanisms

| Mechanism | What It Solves |
|---|---|
| Append-only log + Remove record types | No in-place mutation — no partial write possible |
| Copy-on-write image + atomic pointer swap | Readers always see a consistent full image — no mutex blocking |
| Snapshots | Crash recovery replays only a small delta — removal records are idempotent on replay |

---

## Decision for AmyQueue

### Phase 1 (in-memory)

The mutex approach is correct and sufficient. Crash = memory wipe = full log
replay = clean state. No partial delete is ever persisted. Nothing to fix.

### Phase 4 (persistence) — Contract Established Now

We adopt the same idempotency contract Kafka uses:

**`applyDeleteTopic` is idempotent. Deleting a non-existent key is always a
no-op, never an error.**

In Go, `delete(map, key)` on a missing key is already a no-op — the in-memory
store satisfies this for free. The Phase 4 storage layer must honour the same
contract for disk operations.

### Deletion Order — Partitions Before Topic Record

Partitions are deleted first, then the topic record and name index.

Reason: if a crash happens after partitions are deleted but before the topic
record is removed, log replay re-runs the full delete starting from the topic
record. No orphaned partition records exist. If the topic record were deleted
first and the process crashed before all partitions were removed, orphaned
partition records with no parent topic would exist on disk — requiring a
separate GC scan on startup. Partitions-first avoids that entirely.

### Delete Key Is Always TopicID (UUID), Never Name

`applyDeleteTopic` iterates `0..topic.NumPartitions-1` and deletes by
`PartitionKey{TopicID, i}`. The topic record and name index are removed by
`TopicID`. Name is never used as a delete key. This makes the operation safe
to replay even if the same name was reused for a new topic after the original
was deleted.

### Phase 4 Direction — Snapshots

When Phase 4 persistence is implemented, the right direction is to adopt
Kafka's snapshot pattern: periodic serialisation of the full MetadataImage,
so restart replay is bounded to a small delta. This eliminates the need to
replay the full log on every restart and makes the idempotent-delete contract
less critical in practice (most deletions will be captured in snapshots before
any crash).

---

## Summary

| Question | Decision |
|---|---|
| Is deletion atomic to readers? | Yes — mutex holds for entire Apply call |
| What if process crashes mid-delete? | Phase 1: no issue (memory wipe + full replay). Phase 4: idempotent delete contract covers it |
| What key does delete use? | Always TopicID (UUID) — never name |
| What order are records deleted? | Partitions first, then topic record and name index |
| Is deleting a missing key an error? | Never — always a no-op (Go map semantics + storage layer contract) |
| Long-term crash recovery strategy? | Snapshots (Phase 4) — same as Kafka, bounds replay to delta only |
