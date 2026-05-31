# Epoch Fencing

Two independent fencing mechanisms in AmyQueue. Both follow the same pattern:
a monotonically increasing integer assigned by the state machine, used as a
compare-and-swap guard to reject stale operations.

---

## BrokerEpoch — Zombie Broker Prevention

### The Problem

```
t=1  B3 is leader for P0, serving writes
t=2  B3 loses network — B3 thinks it's alive, others see it as dead
t=3  Controller detects B3 dead via heartbeat timeout
t=4  Controller elects B1 as new leader for P0
t=5  B3 network recovers — still thinks it is leader for P0
t=6  Producer sends write to B3 (stale metadata)
     B3 accepts it → DATA DIVERGENCE: two brokers think they are leader
```

This is the zombie broker problem. A broker that was declared dead but
recovered still acts as if it has authority.

### The Fix

Every re-registration produces a higher epoch. Any request from B3 using
the old epoch is rejected.

```
t=1  B3 registered, Epoch=5
t=2  B3 loses network
t=3  Controller detects B3 dead
t=4  B1 elected new leader
t=5  B3 recovers → re-registers → controller assigns Epoch=6
     Any request B3 sends with Epoch=5 is REJECTED: stale epoch
     B3 must re-register and get Epoch=6 before it can act
```

### Rules

- `BrokerEpoch` is assigned in exactly one place: `applyRegisterBroker`
- First registration: `epoch = 1`
- Every restart (re-registration): `epoch = current + 1`
- Idempotent retry (same host:port): return current epoch unchanged
- Nothing outside the state machine may generate or increment a BrokerEpoch

### Where Epoch Is Validated

Every broker request to the controller (heartbeat, shutdown, LEO report)
includes the broker's current epoch. Controller validates it before processing.
Stale epoch → `403 stale broker epoch, re-register`.

---

## PartitionEpoch — CAS Fencing on Partition Updates

### The Problem

The controller leader proposes `UpdatePartition` to Raft. There is a gap
between when the proposal is sent and when it commits. Two proposals can
be in flight simultaneously even from the same leader (one from a dead
broker detection, one from an ISR expand):

```
Time →

Leader detects B2 dead:  [Propose P1: ISR=[B0,B1]] ----commit----> Apply P1
Leader gets ISR expand:  [Propose P2: ISR=[B0,B1,B3]] ----commit----> Apply P2

If P2 commits first, then P1 applies: P1's ISR=[B0,B1] would OVERWRITE
the expanded ISR [B0,B1,B3] — losing B3 silently.
```

### The Fix

Each proposal carries `ExpectedEpoch` — the `PartitionEpoch` the proposer
observed when it made the decision. The state machine rejects any proposal
where `ExpectedEpoch != current PartitionEpoch`.

```
P1 reads PartitionEpoch=5, carries ExpectedEpoch=5
P2 reads PartitionEpoch=5, carries ExpectedEpoch=5

P1 commits first → Apply: epoch matches (5==5) → apply, PartitionEpoch becomes 6
P2 commits second → Apply: epoch mismatch (5 != 6) → reject, log warning, no-op

Result: only the first committed proposal wins. The second is safely ignored.
The proposer retries with fresh state.
```

### Rules

- State machine is the ONLY place that increments `PartitionEpoch`
- Increment happens on every successful `applyUpdatePartition`
- Proposer reads the current epoch BEFORE proposing — never computes a new one
- On mismatch: log warning, return nil (no-op). Never error.

### UpdatePartitionPayload

```go
type UpdatePartitionPayload struct {
    Key           PartitionKey
    NewLeader     BrokerID
    NewISR        []BrokerID
    ExpectedEpoch int32    // CAS guard — current PartitionEpoch at propose time
    Reason        string   // "leader_death" | "isr_shrink" | "isr_expand" | "unclean_election"
}
```

### applyUpdatePartition Logic

```
if incoming.ExpectedEpoch != current.PartitionEpoch:
  log warning "stale epoch, ignoring"
  return nil  ← no-op, safe

else:
  current.ISR            = incoming.NewISR
  current.Leader         = incoming.NewLeader
  current.Status         = Online or Offline
  current.LeaderEpoch   += 1  (only if leader changed)
  current.PartitionEpoch += 1  (always, on every successful apply)
```

---

## LeaderEpoch — Fencing on Follower Fetch

`LeaderEpoch` is a third, related concept. It increments every time
leadership for a partition changes (a subset of `PartitionEpoch` increments).

Followers include `LeaderEpoch` in every FetchRequest. The leader rejects
fetches with a mismatched epoch — this prevents a follower from fetching
from a stale leader that was already superseded.

### Log Truncation on Recovery

When a follower restarts after being dead, it may have uncommitted data
above the old HWM. It must truncate before rejoining ISR.

```
1. Follower sends: OffsetsForLeaderEpoch(myLastKnownLeaderEpoch)
2. Leader responds: endOffset=97  (last committed offset for that epoch)
3. Follower truncates log to offset 97
4. Follower begins normal FetchRequest from offset 97
5. Follower rejoins ISR once caught up to leader LEO
```

Without this truncation, the follower would have divergent data that was
never committed — and if it became leader, it would serve data no producer
ever received a SUCCESS for.

---

## Summary

| Epoch | Protects Against | Who Assigns | Where Validated |
|---|---|---|---|
| BrokerEpoch | Zombie brokers (stale authority) | `applyRegisterBroker` | Every broker request to controller |
| PartitionEpoch | Stale concurrent proposals | `applyUpdatePartition` | Inside `applyUpdatePartition` (CAS) |
| LeaderEpoch | Follower fetching from stale leader | `applyUpdatePartition` (subset) | Fetch handler on leader broker |
