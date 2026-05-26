# Phase 1 — Epoch Fencing: Discussion & Decision

> Covers: who detects broker death, who triggers UpdatePartition, why two proposals
> can exist in the Raft log simultaneously from a single leader, and how epoch
> fencing guards against stale proposals.

---

## Context

`PartitionEpoch` exists to guard `applyUpdatePartition` against stale proposals.
Two concurrent `UpdatePartition` proposals for the same partition can both be
committed to the Raft log. Without a fencing mechanism, the second one silently
overwrites the first — incorrect state.

Before deciding *how* epoch fencing works, we needed to understand *why* the race
exists in the first place.

---

## Who Detects a Broker Death

Only the **controller leader** knows when a broker dies:

```
Broker ──── heartbeat every 3s ───────────────► Controller Leader
                                                  LivenessTracker
                                                  records timestamp

LivenessTracker.StartSweep() runs periodically
  for each registered broker:
    if now - lastHeartbeat > BrokerSessionTimeoutMs (30s)
      → broker declared dead
      → fires onDead(BrokerID)
```

Non-leader controllers have no liveness information. They learn about broker
deaths only through committed Raft log entries — never directly.

---

## Who Triggers UpdatePartition

`UpdatePartition` is an internal controller state machine operation.
It never goes to a broker directly.

```
Broker B3 dies
      │
      ▼
Controller Leader
  LivenessTracker.onDead(B3)
  ElectLeader(partition, aliveBrokers) → newLeader, newISR
  propose CmdMetadata[UpdatePartitionPayload] ──► Raft log
                                                      │
                                          committed across all controllers
                                                      │
                                    applyUpdatePartition() on ALL controllers
                                    controller Store updated
                                                      │
                                          (separately — Phase 9)
                                    LeaderAndISRRequest pushed to brokers
                                                      │
                                                      ▼
                                              Broker B1 (new leader)
                                              updates its local cache
```

Brokers learn the new leader and ISR after the fact via Phase 9 push.

---

## Why Two Proposals Can Exist From a Single Leader

There is always exactly one controller leader. The race does not come from two
leaders. It comes from the gap between **propose** and **commit**.

```
  PROPOSE  = leader writes entry to its log + sends AppendEntries to followers
             → instant, no waiting
  COMMIT   = quorum of followers acknowledged
             → requires network round trip, takes time
  APPLY    = state machine executes the committed entry
             → only happens after commit
```

The leader does not wait for P1 to commit before proposing P2. Between the
moment it proposes P1 and the moment P1 actually commits (and the store
updates), the leader may have already fired off P2 for the same partition.

---

## Timeline Diagram

```
╔══════════════════════════════════════════════════════════════════════════════════╗
║                         CONTROLLER CLUSTER                                       ║
║                                                                                  ║
║   ┌─────────────────────┐      ┌──────────────────┐    ┌──────────────────┐     ║
║   │  CONTROLLER LEADER  │      │  CONTROLLER C2   │    │  CONTROLLER C3   │     ║
║   │                     │      │   (follower)     │    │   (follower)     │     ║
║   │  LivenessTracker    │      │                  │    │                  │     ║
║   │  Store              │      │  Store           │    │  Store           │     ║
║   └─────────────────────┘      └──────────────────┘    └──────────────────┘     ║
╚══════════════════════════════════════════════════════════════════════════════════╝

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 TIMELINE — Broker B3 and B4 both die close together
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

 BROKERS          B1 ────────────────── heartbeat ──────────────────────────────►
 (sending         B2 ────────────────── heartbeat ──────────────────────────────►
  heartbeats)     B3 ──── heartbeat ──── ✗ DEAD (no more heartbeats)
                  B4 ──── heartbeat ──────────────── ✗ DEAD


 CONTROLLER    LivenessTracker
 LEADER        sweep runs...
                  │
                  ├─ t=1 ─ B3 timeout detected
                  │         onDead(B3) fires
                  │         reads PartitionEpoch = 0
                  │         ElectLeader() → newLeader=B1
                  │         PROPOSE P1 ─────────────────────────────────────────► Raft log
                  │                                                                [ P1 ]
                  │                                                                (not committed yet,
                  │                                                                 waiting for quorum)
                  │
                  ├─ t=2 ─ B4 timeout detected        ← happens BEFORE P1 commits
                  │         onDead(B4) fires
                  │         reads PartitionEpoch = 0  ← STILL 0! P1 not applied yet
                  │         ElectLeader() → newLeader=B2
                  │         PROPOSE P2 ─────────────────────────────────────────► Raft log
                  │                                                                [ P1 ][ P2 ]
                  │                                                                (both waiting)
                  │
                  ├─ t=3 ─ C2 and C3 acknowledge P1
                  │         P1 now has quorum → COMMITTED
                  │         applyUpdatePartition(P1) runs on ALL controllers
                  │
                  │         ┌──────────────────────────────────────────────────┐
                  │         │  P1 payload:  ExpectedEpoch = 0                  │
                  │         │  Store now:   PartitionEpoch = 0  ✓ match        │
                  │         │  → APPLY: Leader=B1, PartitionEpoch becomes 1    │
                  │         └──────────────────────────────────────────────────┘
                  │
                  └─ t=4 ─ C2 and C3 acknowledge P2
                            P2 now has quorum → COMMITTED
                            applyUpdatePartition(P2) runs on ALL controllers

                            ┌──────────────────────────────────────────────────┐
                            │  P2 payload:  ExpectedEpoch = 0                  │
                            │  Store now:   PartitionEpoch = 1  ✗ MISMATCH     │
                            │  → REJECT: no-op, log warning                    │
                            └──────────────────────────────────────────────────┘


━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 RAFT LOG (replicated across all controllers)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

  index:  [ 1 ][ 2 ][ 3 ][ 4 ][ 5  P1  ][ 6  P2  ]
                                    ↑           ↑
                              committed     committed
                              t=3           t=4
```

---

## The Two Options for Epoch Generation

### Option A — Proposer Generates the New Epoch

The `onDead` callback reads current epoch from the store, adds 1, encodes
`NewEpoch = current + 1` in the payload.

```
applyUpdatePartition check:
  if incoming.NewEpoch != current.PartitionEpoch + 1 → reject
```

Problem: two concurrent proposers read the same `current = 0` and both encode
`NewEpoch = 1`. The check sees `1 != 0+1` as true for the first (apply) and
`1 != 1+1` as false for the second (reject). This actually works for the simple
case — but epoch generation is now spread across two places: the proposer and
the store. Nothing prevents a bug where the proposer miscalculates.

### Option B — State Machine Generates the Epoch (Chosen)

The proposer carries `ExpectedEpoch` — "apply this only if current epoch is
still X." The state machine itself increments the epoch on apply.

```
applyUpdatePartition check:
  if incoming.ExpectedEpoch != current.PartitionEpoch → reject
  else: apply update, current.PartitionEpoch++
```

The epoch is incremented in exactly one place: inside `Apply`, under the state
machine mutex. The proposer never computes a "new" epoch — it only states what
it saw. Compare-and-swap semantics.

---

## Decision — Option B: State Machine Owns the Epoch

**Epoch is only ever incremented inside `applyUpdatePartition`. Never outside it.**

Reasons:
- Single place of truth: no risk of proposer miscalculating the new value
- Clean CAS semantics: proposal says "if current is X, apply this" — not "set epoch to Y"
- Resilient to concurrent proposals: both carry `ExpectedEpoch=0`, first wins, second is cleanly rejected regardless of ordering
- Proposer never needs to read-then-encode-epoch — it reads once at proposal time to know what to expect, not to compute a new value

**Updated payload:**

```go
type UpdatePartitionPayload struct {
    Key           PartitionKey
    NewLeader     BrokerID
    NewISR        []BrokerID
    ExpectedEpoch int32    // current PartitionEpoch at propose time — CAS guard
    Reason        string   // "leader_death" | "isr_shrink" | "isr_expand" | "unclean_election"
}
```

**`applyUpdatePartition` logic:**

```
if incoming.ExpectedEpoch != current.PartitionEpoch:
    log warning (stale proposal, reason, key)
    return nil   ← idempotent no-op

apply update:
    current.Leader         = incoming.NewLeader
    current.ISR            = incoming.NewISR
    current.Status         = Online (or Offline if NewLeader == "")
    current.LeaderEpoch   += 1   (if leader changed)
    current.PartitionEpoch += 1  ← always incremented on any successful apply
```

---

## Decision — Raft, Not HTTP

**UpdatePartition proposals go through Raft, not via direct HTTP calls.**

The question was whether the controller leader could short-circuit by calling
an HTTP endpoint to update partition state on all controllers directly.

**Why Raft:**
- All controllers must apply updates in the same order — only Raft guarantees this
- On leader failover, the new leader replays the log and reconstructs identical
  state — HTTP calls would be lost on crash
- HTTP would require the leader to track which controllers have applied which
  updates — Raft already solves this
- Consistency is a Raft invariant, not something we re-implement over HTTP

HTTP is used only for external-facing operations (client requests to create
topics, register brokers, etc.). Internal state machine mutations always go
through Raft.

---

## Summary

| Question | Decision |
|---|---|
| Who detects broker death? | Controller leader — `LivenessTracker.StartSweep` |
| Who proposes UpdatePartition? | Controller leader — `onDead` callback |
| Who applies UpdatePartition? | All controllers — `applyUpdatePartition` in state machine |
| Why can two proposals exist? | Propose ≠ Commit — leader fires P2 before P1 commits |
| Who generates the epoch? | State machine only — proposer carries `ExpectedEpoch` (CAS) |
| Transport for UpdatePartition? | Raft log — never HTTP |
