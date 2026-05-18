# Cluster Membership

AmyQueue supports two cluster modes: **static** and **dynamic**.

---

## Static mode

Voter set is fixed at startup via `PEER_NODES`. Nothing changes at runtime.
Suitable for development and simple deployments.

```bash
CLUSTER_MODE=static
PEER_NODES="localhost:7002,localhost:7003"
```

---

## Dynamic mode

Voter set is managed through committed Raft log entries. Nodes can be added
or removed without restarting the cluster.

```bash
CLUSTER_MODE=dynamic
BOOTSTRAP_SERVERS="localhost:7001,localhost:7002,localhost:7003"
```

---

## Observer pattern

Every new node starts as an **observer**:

- Receives log replication from the leader
- Does **not** vote in elections
- Does **not** count toward quorum
- Cannot block log commits

Once caught up, an observer can be promoted to a **voter**.

```
New node starts
    ↓
Joins as Observer  ← replicates log, catches up
    ↓
Admin promotes to Voter  ← POST /cluster/voters
    ↓
Full Voter  ← participates in elections, counts toward quorum
```

---

## How log replication works (and how "caught up" is measured)

`AppendEntries` is both the heartbeat and the replication RPC — same call,
same code path. When the leader has new log entries since the last send, they
are included in the request. When there is nothing new, it is a pure heartbeat.
Observers receive `AppendEntries` just like voters do.

The leader tracks two indices per peer (voter or observer):

| Index | Meaning |
|---|---|
| `nextIndex[addr]` | Next log entry to send to this peer |
| `matchIndex[addr]` | Highest log entry confirmed replicated on this peer |

Every successful `AppendEntries` response updates `matchIndex[addr]`.
Lag is simply:

```
lag = leader.lastIndex - matchIndex[observer.addr]
```

When `lag <= AUTO_PROMOTE_LAG_THRESHOLD` (default 10), the observer has
replicated everything within the threshold and is considered caught up.

---

## Observer promotion — manual vs auto

The leader checks every observer's lag after each heartbeat tick in
`maybePromoteObservers()`.

**Manual promotion (`AUTO_PROMOTE=false`, the default):**

When an observer crosses the lag threshold the leader logs:

```
observer caught up, safe to promote to voter
  node_id=ctrl-4  addr=localhost:7004  lag=0
  hint=POST /cluster/voters {"node_id":"ctrl-4","addr":"localhost:7004"}
```

This message fires once per leadership term per observer (suppressed by
`pendingPromotion` map to avoid spam every heartbeat). A newly elected leader
re-evaluates all observers and logs it again if they are still caught up.

The operator then calls the hint manually:

```bash
curl -X POST http://localhost:8080/cluster/voters \
  -H "Content-Type: application/json" \
  -d '{"node_id":"ctrl-4","addr":"localhost:7004"}'
```

**Auto-promotion (`AUTO_PROMOTE=true`):**

When the lag threshold is met the leader automatically calls `AddVoter` through
the Raft log — same path as the manual `POST /cluster/voters`. The leader logs:

```
auto-promoting observer to voter  node_id=ctrl-4  addr=localhost:7004  lag=0
```

`pendingPromotion` prevents duplicate log entries if the promotion takes more
than one heartbeat cycle to commit. The map is cleared on `becomeLeader` so a
freshly elected leader re-evaluates all observers from scratch.

!!! warning "Auto-promote grows quorum silently"
    Promoting from 3 voters to 4 raises the quorum from 2 to 3 — the cluster
    now needs 3 votes to commit anything. A fourth node that crashes shortly
    after promotion can stall the cluster. Use `AUTO_PROMOTE=false` (the
    default) in production and promote deliberately.

---

## Why observers can't call elections

An observer that called an election would increment the term and disrupt the
current leader's heartbeat flow — even though no one would vote for it.
`runFollower()` checks `voters.IsVoter(nodeID)` before starting an election.
Observers reset the timer and wait indefinitely.

---

## Membership change safety

Membership changes go through the Raft log as `CmdMembership` entries.
**The old quorum commits the change before it takes effect.**

```
Admin: POST /cluster/voters {"node_id":"ctrl-4"}
    ↓
Leader appends MembershipChange{ADD_VOTER, ctrl-4} to log
    ↓
Old quorum (ctrl-1, ctrl-2, ctrl-3) commits the entry
    ↓
applyMembershipEntry() fires → ctrl-4 added to VoterSet as Voter
    ↓
Quorum size increases from 2 → 3
```

This prevents split-brain: there is never a window where two quorum
definitions are active at the same time.

---

## Removing a voter

```bash
curl -X DELETE http://localhost:8080/cluster/voters/ctrl-2
```

The node being removed participates in committing its own removal.
After commit, if the removed node is itself, it steps down to Follower.

!!! warning "Minimum cluster size"
    Never remove a voter if it would drop the cluster below 3 nodes (or 5 for
    higher fault tolerance). A 2-node cluster cannot tolerate any failure.
