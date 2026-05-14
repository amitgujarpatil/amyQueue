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
