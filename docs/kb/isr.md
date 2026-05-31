# In-Sync Replicas (ISR)

## What It Is

ISR (In-Sync Replicas) is the set of replicas that are currently caught up
with the leader. Only ISR members participate in the durability guarantee.
A write with `acks=all` is only acknowledged when all current ISR members
have persisted it.

ISR is a subset of the full replica set (Replicas). A replica can fall out
of ISR if it falls behind, and rejoin ISR when it catches up.

```
Replicas = [B0, B1, B2]   ← all assigned replicas for this partition
ISR      = [B0, B1]       ← B2 fell behind and was removed
```

---

## ISR at Partition Creation

At creation time, ISR = Replicas. This is not optimistic — it is
mathematically correct. At LEO=0, lag = 0 for every replica. They are
all in sync by definition.

ISR shrink takes over from the first write: any replica that fails to
start replicating within the lag window is removed.

---

## ISR Shrink — Follower Falls Behind

A follower is removed from ISR when it has not fetched from the leader
within `replica.lag.time.max.ms` (default 30 seconds in Kafka).

```
Detection: the leader broker checks, for each follower in ISR,
           when it last sent a FetchRequest.
           
If now - lastFetch > replica.lag.time.max.ms:
  Remove follower from ISR
  Send UpdatePartition to controller (new ISR without this follower)
  Controller commits new ISR to Raft
```

**The leader detects ISR changes — not the controller.** The controller
only learns the outcome via `UpdatePartition`. It does not run its own
lag clock.

### What Triggers a Follower to Fall Behind

- Broker is dead (no heartbeats, no fetches)
- Broker is alive but overwhelmed (disk I/O, GC pause, network congestion)
- Broker is alive but slow (under-powered hardware)

### Effect of ISR Shrink on Writes

```
Before:  ISR=[B0, B1, B2], MinISR=2
         Writes flow normally with acks=all

B2 falls behind → ISR shrinks to [B0, B1]
After:   ISR=[B0, B1], MinISR=2
         len(ISR)=2 >= MinISR=2 → writes still flow ✓

B1 also falls behind → ISR shrinks to [B0]
After:   ISR=[B0], MinISR=2
         len(ISR)=1 < MinISR=2 → writes REFUSED (NOT_ENOUGH_REPLICAS)
         Reads still work (leader B0 is alive)
```

---

## ISR Expand — Follower Catches Up

A follower that was removed from ISR can rejoin when it has fully caught
up to the leader LEO.

```
Detection: the leader sees a FetchRequest where fetchOffset == leader LEO
           (follower has consumed all available data)

If follower.LEO == leader.LEO AND follower not in ISR:
  Add follower back to ISR
  Send UpdatePartition to controller (new ISR with follower added)
  Controller commits new ISR to Raft
```

---

## MinISR — The Write Floor

`MinISR` is a per-topic config (set in `TopicConfig.MinISR`). It is a
write refusal floor:

```
If len(ISR) < MinISR:
  Refuse all produce requests with acks=all
  Return NOT_ENOUGH_REPLICAS to producer
```

MinISR does NOT take a partition offline. The partition stays Online.
Reads still work. Only writes are refused.

### MinISR Interaction With acks

| acks | MinISR check applies? |
|---|---|
| 0 | No — fire and forget |
| 1 | No — leader-only ack |
| all (-1) | Yes — refused if len(ISR) < MinISR |

MinISR only has teeth with `acks=all`.

### Recommended Values

| Setup | MinISR | Guarantee |
|---|---|---|
| RF=3, high durability | 2 | Survives 1 broker loss without data loss |
| RF=3, high availability | 1 | Never blocks writes, but can lose data |
| RF=1 (dev/test) | 1 | No replication at all |

---

## ISR and the Raft Log

ISR is part of `PartitionState` stored in the controller's Raft log.
Every ISR change (shrink or expand) goes through the Raft state machine
via `UpdatePartitionPayload`. This ensures all controller replicas agree
on the current ISR.

```go
type PartitionState struct {
    ...
    ISR            []BrokerID       // current in-sync replicas
    ...
}
```

The `PartitionEpoch` increments on every `UpdatePartition` apply —
including ISR changes. This is the CAS guard that prevents stale proposals.

---

## Unclean Leader Election (Deferred)

If all ISR members die and only an out-of-sync replica remains:
- Clean election: no candidate → partition goes Offline
- Unclean election: elect the out-of-sync replica (possible data loss)

Configurable via `unclean.leader.election.enable` (default: false).
AmyQueue defers this — Phase 6 decision.
