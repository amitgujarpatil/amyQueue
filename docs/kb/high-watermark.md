# High Watermark (HWM)

## What It Is

The High Watermark is the highest offset that has been written to **all
current ISR members**. It is the commit line — data at or below HWM is
safe to expose to consumers. Data above HWM exists on the leader but has
not yet been replicated to all ISR followers.

---

## The Three Offsets

```
Leader log:

  offset:  0    1    2    3    4    5    6    7    8    9    10
           [----committed (on all ISR)----][---uncommitted---]

           ^                               ^                  ^
           LSO                            HWM                LEO
           (Log Start Offset)        (High Watermark)  (Log End Offset)
           earliest readable         commit line        next write here

Consumer can read: LSO → HWM-1  (inclusive)
Consumer cannot read: HWM and above (uncommitted)
```

- **LSO** — earliest available offset (advances as old segments are deleted by retention)
- **HWM** — highest committed offset (all ISR members have this data)
- **LEO** — Log End Offset — where the next write will go (leader's frontier)

---

## How HWM Is Computed

```
HWM = min(LEO of all current ISR members)
```

The leader tracks each follower's LEO from the `fetchOffset` field of their
incoming FetchRequests. Every time a follower fetches, the leader updates its
record of that follower's LEO and recomputes HWM.

```
Leader LEO:    100
Follower1 LEO: 98    (learned from its last FetchRequest)
Follower2 LEO: 99

HWM = min(100, 98, 99) = 98

Consumer reads up to offset 97 (exclusive)
Offsets 98, 99, 100 are uncommitted
```

---

## HWM Advancement Is Lazy

The leader does not advance HWM immediately when it writes a message.
HWM advances only when a follower sends a FetchRequest proving it has
caught up:

```
t=1  Leader writes offset 100. LEO=101. HWM still=97.
t=2  Follower1 sends FetchRequest(fetchOffset=99)
       Leader learns: Follower1.LEO = 99
       HWM = min(101, 99, 99) = 99
t=3  Follower2 sends FetchRequest(fetchOffset=100)
       Leader learns: Follower2.LEO = 100
       HWM = min(101, 100, 99) = 99  (still bottlenecked by Follower1)
t=4  Follower1 sends FetchRequest(fetchOffset=101)
       Leader learns: Follower1.LEO = 101
       HWM = min(101, 101, 100) = 100
```

The lag between a write landing on the leader and HWM advancing is
bounded by the follower's fetch interval (`replica.fetch.wait.max.ms`,
default 500ms in Kafka).

---

## Leader Sends HWM to Followers

Every FetchResponse from the leader includes the current HWM. Followers
maintain a local copy of HWM. This matters for two reasons:

1. If a follower is promoted to leader, it already knows what is committed
2. Followers use HWM to answer consumer reads correctly

---

## acks=all Blocks Until HWM Advances

With `acks=all`, the producer's write is not acknowledged until HWM has
advanced past the written offset — meaning all ISR members confirmed.

```
Producer writes batch ending at offset 100
acks=all: leader holds response open
Followers fetch, leader sees their LEOs advance
HWM reaches 101 (past offset 100)
Leader returns SUCCESS to producer
```

This is what makes `acks=all + MinISR=2` safe: any acknowledged write
is guaranteed to be on at least `MinISR` brokers.

---

## HWM on Leader Failover

When a new leader is elected, it does not know the old leader's HWM.
It only knows its own log. This is the HWM gap problem.

Kafka's fix: **leader epoch end offsets**. Each epoch records the highest
committed offset for that epoch. Followers use `OffsetsForLeaderEpoch`
to find where to truncate.

See → `epoch-fencing.md` for the full log truncation protocol.

---

## AmyQueue Implementation Notes

- HWM lives on the leader broker — it is NOT stored in the controller's metadata
- Controller's `PartitionState` stores `Leader`, `ISR`, `LeaderEpoch`, `PartitionEpoch` — not HWM
- HWM is a runtime data-plane value, not a metadata-plane value
- Consumers must read from the leader to get accurate HWM-fenced responses
