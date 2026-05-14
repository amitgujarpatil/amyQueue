# AmyQueue — System Flows

Key flows in the system, updated as we build. Each flow describes the
sequence of steps, which components are involved, and edge cases handled.

---

## 1. Controller startup (static mode)

```
ENV loaded (CLUSTER_MODE=static, PEER_NODES=..., RAFT_PORT=...)
  ↓
KillPortOnStart → free RAFT_PORT if occupied
  ↓
TCP transport starts listening on :RAFT_PORT
  ↓
Raft node starts → seeds VoterSet from PEER_NODES
  ↓
All nodes enter Follower state
  ↓
Election timeout fires on one node → becomes Candidate
  ↓
Candidate sends VoteRequest to all voters
  ↓
Majority grants vote → Candidate becomes Leader
  ↓
Leader sends heartbeat (AppendEntries, empty entries) every CONTROLLER_HEART_BEAT_INTERVAL ms
  ↓
Followers reset election timer on each heartbeat
  ↓
HTTP admin server starts on :HTTP_PORT (serves /cluster/status)
```

**Key env vars:** `NODE_ID`, `NODE_ROLE`, `RAFT_PORT`, `HTTP_PORT`, `PEER_NODES`,
`CLUSTER_MODE=static`, `RAFT_ELECTION_TIMEOUT_MS`, `CONTROLLER_HEART_BEAT_INTERVAL`

---

## 2. Controller startup (dynamic mode) — first 3 nodes forming the cluster

Same as static startup except `PEER_NODES` still seeds the initial voter set.
`CLUSTER_MODE=dynamic` enables the observer join and voter promotion APIs.
No `BOOTSTRAP_SERVERS` needed for the founding nodes.

---

## 3. New node joining an existing cluster (dynamic mode)

```
New node starts with CLUSTER_MODE=dynamic, BOOTSTRAP_SERVERS=seed1,seed2,...
  ↓
joinCluster() called before admin server starts
  ↓
For each seed in BOOTSTRAP_SERVERS (attempt loop, max JOIN_MAX_RETRIES):
  ↓
  SendObserverJoin(seed) ──► seed is not leader?
                              └─ response contains LeaderAddr
                              └─ immediately retry ObserverJoin on LeaderAddr
  ↓
  seed unreachable? → log network error → try next seed
  ↓
  no leader yet / rejected? → log reason → try next seed
  ↓
  all seeds tried → wait JOIN_RETRY_INTERVAL_MS → next attempt
  ↓
Leader receives ObserverJoin
  ↓
Leader registers new node in VoterSet as Observer
Leader sets nextIndex[newNodeAddr] and starts sending heartbeats to it
  ↓
New node starts receiving log entries, catches up
  ↓
Admin promotes observer to voter:
  POST /cluster/voters {"node_id":"ctrl-4","addr":"localhost:7004"}
  ↓
Leader appends MembershipChange{ADD_VOTER} to Raft log
  ↓
Old quorum (existing voters) commits the entry
  ↓
applyMembershipEntry() fires on commit → node added to VoterSet as Voter
  ↓
New node now participates in elections and counts toward quorum
```

**Key env vars:** `CLUSTER_MODE=dynamic`, `BOOTSTRAP_SERVERS`, `JOIN_MAX_RETRIES`,
`JOIN_RETRY_INTERVAL_MS`

**Edge cases handled:**
- Dead seed → network error logged, next seed tried
- Seed is not leader → redirect followed immediately (no extra round-trip)
- Leader changes mid-join → redirect followed once on next attempt
- All seeds exhausted → wait + retry up to JOIN_MAX_RETRIES passes

---

## 4. Leader heartbeat and follower tracking

```
Leader ticker fires every CONTROLLER_HEART_BEAT_INTERVAL ms
  ↓
broadcastHeartbeat() called
  ↓
For each peer (voters + observers):
  AppendEntriesRequest{
    LeaderID,
    LeaderAddr,    ← leader's own raft address, stored by followers for redirects
    PrevLogIndex,
    Entries,       ← empty if pure heartbeat, non-empty if catching up
    LeaderCommit,
  }
  ↓
Follower receives AppendEntries:
  - stores leaderID + leaderAddr
  - resets election timer
  - applies any new log entries
  - advances commitIndex if LeaderCommit moved
```

**Why LeaderAddr is in every heartbeat:** Followers need it to answer redirect
requests from joining nodes — they know the leader address without a separate lookup.

---

## 5. Leader election

```
Follower election timer expires (no heartbeat received)
  ↓
Node checks: am I a voter? (observers skip this)
  ↓
Increment currentTerm, vote for self, become Candidate
  ↓
Send VoteRequest{Term, CandidateID, LastLogIndex, LastLogTerm} to all voters
  ↓
Each voter:
  - rejects if req.Term < own term
  - grants if: not voted yet this term AND candidate log is at least as up-to-date
  ↓
Candidate collects votes — needs quorum (majority of VoterSet)
  ↓
Quorum reached → become Leader
  ↓
Immediately broadcast heartbeat (suppress follower election timers)
```

**Split-brain protection:** Any node receiving a message with Term > currentTerm
immediately steps down to Follower and updates its term. Two leaders in the same
term is impossible because quorum requires majority overlap.

---

## 6. Dynamic voter removal

```
Admin: DELETE /cluster/voters/{node_id}
  ↓
Leader appends MembershipChange{REMOVE_VOTER, nodeID} to log
  ↓
Current quorum (INCLUDING the node being removed) commits the entry
  ↓
applyMembershipEntry() fires → node removed from VoterSet
  ↓
If removed node == self → step down to Follower
  ↓
Quorum size recalculates from remaining voters
```

**Why the removed node commits its own removal:** Prevents a situation where
the remaining nodes don't have quorum to commit the change.

---

## 7. Bootstrap discovery (ClusterInfo)

Any node can be queried for current cluster state via:
- **TCP RPC:** `ClusterInfoRequest` → `ClusterInfoResponse{LeaderID, LeaderAddr, Members}`
- **HTTP:** `GET /cluster/status`

Used by external clients and monitoring tools. Not used in the node join flow
(ObserverJoin handles redirects itself).
