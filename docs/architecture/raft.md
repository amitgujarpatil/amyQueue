# Raft Consensus

AmyQueue controllers use the **Raft consensus algorithm** to agree on cluster
state. Only one node is the leader at any time. All writes go through the leader.

---

## Node states

```
            timeout
Follower ──────────────► Candidate ──── quorum votes ──► Leader
   ▲                          │                              │
   │◄─── higher term ─────────┘                             │
   │◄─── higher term ─────────────────────────────────────── │
   │◄─── heartbeat received ───────────────────────────────── │
```

| State | Role |
|---|---|
| **Follower** | Passive. Resets election timer on each heartbeat. Votes in elections. |
| **Candidate** | Actively soliciting votes. Increments term, votes for self. |
| **Leader** | Accepts all writes. Sends heartbeats. Replicates log to followers. |

---

## Leader election

1. Follower's election timer fires (no heartbeat received within `RAFT_ELECTION_TIMEOUT_MS`)
2. Becomes Candidate — increments `currentTerm`, votes for self
3. Sends `VoteRequest{term, lastLogIndex, lastLogTerm}` to all voters
4. Voter grants vote if: not yet voted this term **and** candidate's log is at least as up-to-date
5. Candidate collects majority → becomes Leader → broadcasts heartbeat immediately

**Split-brain protection:** Any node receiving a message with `term > currentTerm`
steps down to Follower immediately. Two leaders in the same term is impossible
because quorum requires majority overlap — two majorities always share at least one node.

**Randomised timeout:** Election timeout is randomised between `base` and `base × 1.5`
to prevent all nodes timing out simultaneously and causing repeated split votes.

---

## Log replication

The leader appends entries to its log and replicates them via `AppendEntries` RPCs.
An entry is **committed** once a majority of voters have it in their log.

```
Leader:    [1][2][3][4]  ← commitIndex=4
Follower1: [1][2][3][4]  ← matchIndex=4 ✓
Follower2: [1][2][3]     ← matchIndex=3 (still catching up)

Quorum = 3 nodes, majority = 2
Entry 4 is committed: leader + follower1 = 2 ✓
```

If a follower's log conflicts with the leader's, the leader backs off `nextIndex`
and retries until it finds the common point, then overwrites the follower's
conflicting suffix.

---

## Heartbeats

The leader sends `AppendEntries` (empty entries) every `CONTROLLER_HEART_BEAT_INTERVAL` ms.
This serves two purposes:

1. Resets follower election timers — keeps followers from calling elections
2. Carries `LeaderAddr` — followers always know the current leader's TCP address
   for redirect purposes

---

## Log entry types

| Type | Purpose |
|---|---|
| `CmdData` | Normal application data (future: topic/partition commands) |
| `CmdMembership` | Membership change — add voter, remove voter |

Membership entries are applied to `VoterSet` only after they are **committed**.
This is the key safety property: the old quorum commits the change before the
new quorum takes effect.
