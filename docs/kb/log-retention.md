# Log Retention

## What It Is

Retention controls how long or how much data a partition keeps on disk.
Old data is deleted automatically once the limit is exceeded. This is how
a message queue avoids filling the disk indefinitely.

---

## Two Retention Modes

Both policies run simultaneously. A segment is eligible for deletion if
**either** condition is met — whichever fires first wins.

### Size-Based — `retention.bytes`

```yaml
retention.bytes: 1073741824   # 1 GB
```

When the total log size for a partition exceeds this value, the broker
deletes the oldest sealed segments until the total is back within the limit.

`-1` means unlimited (no size-based retention).

### Time-Based — `retention.ms`

```yaml
retention.ms: 604800000   # 7 days
```

A segment is eligible for deletion when all messages inside it are older
than `retention.ms`. The segment's max timestamp is used, not the min —
so a segment is only deleted once every single message in it has expired.

`-1` means unlimited (no time-based retention).

---

## Segment Granularity — The Key Detail

Retention deletes **entire segment files**, not individual messages.
The broker cannot surgically remove a single message from the middle of a segment.

```
partition log (total = 1.1 GB, retention.bytes = 1 GB):

  segment-000.log  →  400 MB  (sealed, oldest)
  segment-400.log  →  400 MB  (sealed)
  segment-800.log  →  300 MB  (ACTIVE — being written to)

  Retention fires:
    delete segment-000.log  →  total = 700 MB  ✓ within limit
```

---

## The Active Segment Is Never Deleted

The active segment (the one currently being written to) is always kept,
even if it alone exceeds `retention.bytes`. Only sealed segments are
eligible for deletion.

This means total on-disk size can temporarily exceed `retention.bytes` —
the limit is only enforced after a segment is sealed and the retention
check fires.

---

## retention.bytes Is Per Partition, Not Per Topic

If a topic has 10 partitions and `retention.bytes = 1 GB`, each partition
can hold up to 1 GB. The topic's total storage can reach 10 GB.

---

## Retention Runs on a Background Interval

Deletion is not immediate. The broker checks retention periodically
(Kafka default: every 5 minutes via `log.retention.check.interval.ms`).
There is always a lag between exceeding the limit and the actual cleanup.

---

## Log Start Offset (LSO)

When a segment is deleted, the **Log Start Offset** advances past all
messages in that segment. LSO is the earliest readable offset.

```
Before deletion:  LSO = 0,     readable: offset 0 → HWM
After deletion:   LSO = 5000,  readable: offset 5000 → HWM
```

Any consumer requesting an offset below LSO gets `OFFSET_OUT_OF_RANGE`.
The consumer must seek to `earliest` (the current LSO) and accept that
those messages are gone.

---

## Segment Rolling

The active segment is sealed (rolled) and a new active segment starts when:
- The active segment size reaches `segment.bytes` (default 1 GB in Kafka), OR
- The active segment has been open longer than `segment.ms` (default 7 days)

Only after rolling does a segment become eligible for retention deletion.

---

## AmyQueue Config

```yaml
# per-topic config (set in TopicConfig at CreateTopic time)
retention.ms:    604800000   # -1 = unlimited
retention.bytes: -1           # -1 = unlimited

# per-segment rolling
segment.bytes:   1073741824   # 1 GB
```

These map directly to the `TopicConfig` struct designed in Phase 1:

```go
type TopicConfig struct {
    RetentionMs    int64   // -1 = unlimited
    RetentionBytes int64   // -1 = unlimited
    SegmentBytes   int64
    ...
}
```

---

## What Happens to In-Flight Consumers

A consumer that was reading from offset 1200 and then the segment containing
offset 1200 gets deleted:

```
1. Consumer sends FetchRequest(offset=1200)
2. Broker sees 1200 < LSO (now 5000)
3. Broker returns OFFSET_OUT_OF_RANGE error
4. Consumer must decide: seek to earliest, seek to latest, or fail
```

Consumer groups (Phase 4) handle this by detecting `OFFSET_OUT_OF_RANGE`
and resetting to `auto.offset.reset` policy (earliest or latest).
