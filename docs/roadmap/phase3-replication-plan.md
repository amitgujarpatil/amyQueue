# Phase 3 — Data Replication: Plan & Concepts

> Pre-discussion planning doc. Covers what Phase 3 must address, how Kafka handles
> each area, the challenges, and the recommended scope boundary.
> Working discussion will be tracked in phase3-replication-design.md once we begin.

---

## What Phase 3 Is

Phase 1 gave us metadata. Phase 2 gave us broker identity. Phase 3 is where actual
data moves — producers write messages to leaders, leaders persist them to disk,
followers pull and replicate, and the system decides what is safe to expose to
consumers. This is the most complex phase and the core of the durability guarantees
designed in Phase 2.

---

## The Six Core Areas

### 1. Log Structure on Disk

A partition is a directory. Inside it are segment files. Kafka stores three files
per segment:

```
topic-name-0/
  00000000000000000000.log        ← raw message bytes, append-only
  00000000000000000000.index      ← sparse offset → file position map
  00000000000000000000.timeindex  ← timestamp → offset map
  00000000000000000042.log        ← next segment (rolled when previous reached segment.bytes)
  00000000000000000042.index
  00000000000000000042.timeindex
```

The filename prefix is the base offset of that segment. The active segment is open
for writes. All others are read-only.

**Challenge:** Concurrent reads (consumers) and writes (producer appends). Solved
by the append-only property — readers and the writer never touch the same byte
offset. No locking needed between reader and writer on the file itself.

---

### 2. Message / RecordBatch Format

Kafka stores messages in batches (RecordBatch), not individually. The batch is the
unit of compression, CRC, and replication.

```
RecordBatch:
  baseOffset        int64   ← offset of first record in batch
  batchLength       int32
  magic             int8    ← format version
  crc               int32   ← CRC32C over everything after this field
  attributes        int16   ← compression codec, timestamp type
  lastOffsetDelta   int32   ← relative offset of last record
  baseTimestamp     int64
  maxTimestamp      int64
  producerID        int64   ← for idempotent produce (deferred)
  producerEpoch     int16
  baseSequence      int32
  records[]         Record
    offsetDelta     int32   ← relative to baseOffset
    timestampDelta  int64   ← relative to baseTimestamp
    key             bytes
    value           bytes
    headers[]       {key string, value bytes}
```

Compression is applied at the batch level — the `records[]` payload is compressed
as a unit. Supported codecs: none, gzip, snappy, lz4, zstd.

---

### 3. Producer → Leader Write Path

```
Producer                    Leader Broker               Follower Brokers
   |                              |                           |
   |-- ProduceRequest ----------->|                           |
   |   { partition, acks,         |                           |
   |     RecordBatch }            |                           |
   |                           validate epoch                 |
   |                           append to active segment       |
   |                           advance own LEO                |
   |                           (acks=0) return immediately    |
   |                           (acks=1) return SUCCESS ------>|
   |                           (acks=all) wait...             |
   |                                            fetch loop -->|
   |                                            leader sees   |
   |                                            follower LEOs |
   |                                            advance HWM   |
   |<-- SUCCESS (acks=all) ---------|           when HWM >=   |
   |                                            written offset|
```

With `acks=all`: leader blocks the response until HWM advances past the written
offset — meaning all current ISR members have fetched and acknowledged the data.

---

### 4. Follower Replication — Pull-Based Fetch Loop

Followers pull from the leader. The leader never pushes. Each follower runs a
continuous fetch loop:

```
loop:
  send FetchRequest(partitionID, fetchOffset=myLEO, maxBytes, leaderEpoch)
  receive FetchResponse(records, highWatermark, leaderEpoch)
  validate leaderEpoch — reject if mismatch (stale leader)
  append records to local log
  advance myLEO
  update local copy of HWM from response
  repeat
```

**Why pull, not push:**
- Follower controls its own fetch rate
- Leader has no backpressure problem — it just responds to requests
- A slow follower never blocks the leader
- Replication is the same protocol as consumer fetch — no separate replication path

**Leader tracks follower LEOs:** The leader reads the `fetchOffset` field of each
incoming FetchRequest to know how far that follower has gotten. There is no separate
"replica status" message — the fetch itself is the status report.

---

### 5. High Watermark (HWM) — The Commit Line

HWM = the highest offset that has been written to ALL current ISR members.

Consumers can only read up to HWM. Data above HWM is uncommitted.

```
Leader LEO:    offset 100
Follower1 LEO: offset 98   (tracked by leader from fetch requests)
Follower2 LEO: offset 99

HWM = min(100, 98, 99) = 98

Consumer reads: offsets 0 .. 97  (exclusive HWM)
Uncommitted:    offsets 98, 99, 100  (not yet safe to expose)
```

**HWM advancement:** The leader advances HWM lazily — each time it receives a fetch
request from a follower, it updates that follower's tracked LEO and recomputes
`HWM = min(all ISR member LEOs)`. This means HWM can lag LEO by one fetch cycle.
That lag is bounded by `replica.fetch.wait.max.ms` (default 500ms).

**Leader also sends HWM to followers in FetchResponse.** Followers maintain their
own local copy of HWM. This is needed so followers can serve consumer reads
correctly if they are promoted to leader.

---

### 6. ISR Shrink and Expand

**ISR shrink — follower falls behind:**

```
Follower hasn't sent a fetch request within replica.lag.time.max.ms (default 30s)
  OR
Follower LEO is more than replica.lag.max.messages behind leader LEO
  (message count lag is deprecated in Kafka in favour of time-based only)

Leader detects this locally
Leader sends UpdatePartition (AlterPartition in Kafka KRaft) to controller
Controller commits new ISR to Raft log
Partition continues with smaller ISR
```

**ISR expand — follower catches up:**

```
Follower's LEO == leader's LEO  (fully caught up)
Leader detects this locally
Leader sends UpdatePartition to controller with follower added back to ISR
Controller commits to Raft
```

**Key design principle:** The **leader broker** detects ISR changes. The controller
only learns the outcome via UpdatePartition. The controller does not run its own
lag clock and does not receive raw LEO values. This keeps the controller clean —
it manages metadata, the broker manages data-plane state.

---

### 7. Leader Epoch Fencing and Log Truncation on Follower Recovery

Every leadership change increments `LeaderEpoch` (already in PartitionState from
Phase 1). Followers include the current leader epoch in every fetch request.

**The recovery problem:**

```
t=1  Leader LEO=100, HWM=97
     Follower LEO=100 (has offsets 98, 99, 100 — uncommitted)
t=2  Follower dies
t=3  New leader elected, starts fresh at its own LEO
     New leader's HWM may start at a lower point
t=4  Follower recovers — it has LEO=100 but offsets 98-100 were never committed
     If follower keeps those bytes it has DIVERGED from the new leader
```

**The fix — OffsetsForLeaderEpoch then truncate:**

```
Follower sends: OffsetsForLeaderEpoch(partitionID, leaderEpoch=5)
Leader responds: endOffset=97  (last committed offset for epoch 5)
Follower truncates log to offset 97
Follower sends normal FetchRequest from offset 97
Follower rejoins ISR once caught up
```

This is the correctness boundary for the entire replication design. Without it
a recovered follower can silently diverge from the cluster.

---

### 8. Log Retention

Two retention modes (from TopicConfig, already designed in Phase 1):

```
Time-based:  delete segments where all messages are older than retention.ms
Size-based:  delete oldest segments when total log size > retention.bytes
```

Retention runs as a background goroutine on each broker, checking periodically
(default: log.retention.check.interval.ms = 5 minutes in Kafka).

Only complete segments (not the active segment) are eligible for deletion.
The active segment is always kept.

Deletion advances the **Log Start Offset (LSO)** — the earliest offset available.
Consumers requesting offsets below LSO get an OFFSET_OUT_OF_RANGE error.

---

## Kafka Alignment Summary

| Concept | Kafka | AmyQueue Phase 3 |
|---|---|---|
| Log structure | Segment .log + .index + .timeindex | Same |
| Message unit | RecordBatch (batch-level compression + CRC) | Same structure |
| Replication direction | Pull — follower fetches from leader | Same |
| LEO tracking | Leader reads from follower's fetchOffset | Same |
| HWM | min(ISR LEOs), advanced per fetch cycle | Same |
| HWM in FetchResponse | Yes — leader sends HWM to follower | Same |
| ISR shrink | Leader detects lag, proposes to controller | Same |
| ISR expand | Leader detects catch-up, proposes to controller | Same |
| Leader epoch in fetch | Yes — fencing | Same |
| Log truncation on recovery | OffsetsForLeaderEpoch + truncate | Same |
| Retention | Time + size based, background goroutine | Same |
| Segment rolling | Roll when active segment >= segment.bytes | Same |

---

## Topics to Address vs Defer

### Must Address in Phase 3

1. Log segment structure — what files, naming convention, active vs sealed
2. RecordBatch format — offset, timestamp, key, value, headers, CRC, compression
3. Producer write path on leader — append to log, advance LEO, acks logic
4. Follower fetch loop — pull design, FetchRequest / FetchResponse protocol
5. LEO tracking per follower on the leader
6. HWM computation and advancement
7. Consumer read fence — reads stop at HWM
8. ISR shrink — lag detection by leader, UpdatePartition to controller
9. ISR expand — catch-up detection by leader, UpdatePartition to controller
10. Leader epoch fencing in fetch
11. Log truncation on follower recovery (OffsetsForLeaderEpoch)
12. Segment rolling (when active segment exceeds segment.bytes)
13. Log retention — time-based and size-based

### Defer

| Feature | Why |
|---|---|
| Log compaction (compact cleanup policy) | Separate complex GC mechanism |
| Idempotent produce (producer ID + sequence) | Deduplication concern, Phase 5+ |
| Follower reads (read from non-leader replica) | Kafka 2.4+ feature, complexity vs gain |
| Transactions | Separate phase entirely |
| Tiered storage | Out of scope |
| Remote log management | Out of scope |

---

## The Hardest Problem: HWM Gap on Leader Failover

Old leader had HWM=97, LEO=100. New leader is elected — it only knows its own log,
not the old leader's HWM. If the new leader starts at a higher HWM, followers with
LEO=97 will think they are behind and fetch from 97, but the new leader's HWM might
already be wrong.

Kafka's fix: leader epoch end offsets. Each epoch records the highest committed
offset for that epoch. `OffsetsForLeaderEpoch` response includes this. This is the
single most subtle correctness problem in the replication layer and the reason
`LeaderEpoch` exists in `PartitionState` at all.

---

## Recommended Phase 3 Scope Boundary

```
Phase 3 = "Data Replication" — broker to broker
  Produce → persist → replicate → commit via HWM
  ISR management (shrink + expand)
  Log structure + retention

Phase 4 = "Consumer Read Path"
  Consumer offset management
  Consumer groups
  __consumer_offsets topic
```

Keeping Phase 3 focused on the produce and replicate path — no consumer offset
tracking. Consumers can fetch up to HWM, but group management and committed offsets
belong in Phase 4.
