# Log Structure on Disk

## What a Partition Log Is

Each partition is stored as a directory on the broker's disk. Inside that
directory are one or more segment files. A segment is a fixed-size chunk
of the partition's append-only message stream.

```
/data/amyqueue/
  orders-0/                     ← topic "orders", partition 0
    00000000000000000000.log
    00000000000000000000.index
    00000000000000000000.timeindex
    00000000000000001000.log     ← next segment, base offset = 1000
    00000000000000001000.index
    00000000000000001000.timeindex
    00000000000000002500.log     ← active segment (open for writes)
    00000000000000002500.index
    00000000000000002500.timeindex
  orders-1/                     ← partition 1
    ...
  payments-0/
    ...
```

---

## Three Files Per Segment

### `.log` — The Data File

Raw message bytes. Append-only. The only file that grows.
Each entry is a `RecordBatch` — a batch of one or more messages with
a shared header (CRC, compression, base offset, timestamps).

```
[RecordBatch][RecordBatch][RecordBatch]...
```

### `.index` — Offset Index

A sparse map from logical offset → byte position in the `.log` file.
Not every offset has an entry — only one entry per `index.interval.bytes`
of data (default 4096 bytes in Kafka).

Used to seek to a specific offset without scanning the full `.log` file.

```
Entry format: (relative offset: int32, position: int32)

relative offset = absolute offset - base offset of this segment
```

### `.timeindex` — Time Index

A sparse map from timestamp → offset. Used for time-based consumer seeks
(`seekToTimestamp`) and for time-based retention decisions.

```
Entry format: (timestamp: int64, relative offset: int32)
```

---

## Segment Naming Convention

The filename prefix is the **base offset** of that segment — the offset of
the first message in the segment, zero-padded to 20 digits.

```
00000000000000002500.log  →  base offset = 2500
                              contains messages from offset 2500 onwards
```

---

## Active vs Sealed Segments

| | Active | Sealed |
|---|---|---|
| State | Open for writes | Read-only |
| Count | Exactly one per partition | Zero or more |
| Can be deleted by retention | No | Yes |
| Index file | Written incrementally | Finalized on seal |

When the active segment reaches `segment.bytes` in size (or `segment.ms`
in age), it is sealed and a new active segment is created starting at
the next offset.

---

## How Reads Work

To read from offset N:

```
1. Find the segment with the largest base offset <= N
   (binary search on sealed segments + check if active segment applies)

2. Binary search the .index file of that segment
   Find the largest indexed offset <= N
   Get its byte position P in the .log file

3. Scan forward through .log starting at position P
   Skip RecordBatches until base offset + lastOffsetDelta >= N
   Return records starting from offset N
```

The sparse index means at most a few KB of `.log` scanning per read.

---

## How Writes Work

```
1. Append RecordBatch to the end of the active .log file
2. Update .index if enough bytes have accumulated since last index entry
3. Update .timeindex similarly
4. Advance LEO = base offset + lastOffsetDelta + 1
5. fsync() — depends on flush config (flush.messages, flush.ms)
```

The `.log` file is append-only — no existing bytes are ever modified.
This is what makes concurrent reads safe without file-level locking.

---

## fsync and Durability

Kafka (and AmyQueue) do not fsync on every write by default. The OS
page cache holds writes in memory and flushes to disk periodically.

```yaml
flush.messages: 9223372036854775807   # effectively never (rely on OS flush)
flush.ms:       9223372036854775807   # effectively never
```

Durability comes from **replication**, not fsync. With RF=3 and acks=all,
data is on 3 brokers' page caches. For a write to be lost, all three
brokers must die before the OS flushes to disk simultaneously.

For environments that require fsync: set `flush.messages=1` (every write)
or `flush.ms=1000` (every second). This reduces throughput significantly.

---

## Log Start Offset (LSO) and Log End Offset (LEO)

```
┌─────────────────────────────────────────────────────────┐
│  segment-0    │  segment-1    │  segment-2 (active)     │
│  offset 0-999 │ offset 1000-2499 │ offset 2500-...     │
└─────────────────────────────────────────────────────────┘
  ^                                                        ^
  LSO=0                                                  LEO=2847
  (earliest                                         (next write here)
   readable)
```

After retention deletes segment-0:

```
LSO = 1000   (segment-0 is gone, earliest is now offset 1000)
LEO = 2847   (unchanged)
```

---

## RecordBatch Format (Summary)

Each entry in the `.log` file is a `RecordBatch`:

```
RecordBatch:
  baseOffset        int64   ← absolute offset of first record
  batchLength       int32   ← length of everything after this field
  magic             int8    ← format version
  crc               int32   ← CRC32C
  attributes        int16   ← compression codec (bits 0-2), timestamp type (bit 3)
  lastOffsetDelta   int32   ← offset of last record relative to baseOffset
  baseTimestamp     int64
  maxTimestamp      int64
  records[]:
    offsetDelta     varint  ← relative to baseOffset
    timestampDelta  varint  ← relative to baseTimestamp
    key             bytes
    value           bytes
    headers[]       {key string, value bytes}
```

Compression is applied to the `records[]` payload as a whole unit.
