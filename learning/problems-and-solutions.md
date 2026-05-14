# AmyQueue — Problems and Solutions

Every non-obvious problem we ran into, how we debugged it, and what we
decided. Useful reference for future similar decisions.

---

## 1. Split-brain during dynamic membership change

**Problem:**
If you naively swap the cluster config live (e.g. going from 3 nodes to 5),
there is a window where two different quorum definitions are active at the
same time. The old 3-node quorum can elect a leader independently of the new
5-node quorum — two leaders, two truths, data corruption.

**Solution:**
One membership change at a time, always committed through the OLD quorum first.
We encode the change as a `CmdMembership` log entry. The current voters must
commit it before it takes effect. Only after commit does `applyMembershipEntry()`
update the VoterSet. There is never a window where two quorum definitions coexist.

**Reference:** Raft paper §6, Kafka KRaft membership change design.

---

## 2. Observer nodes calling elections

**Problem:**
In dynamic mode new nodes join as observers before being promoted to voters.
If observers ran the normal election timeout logic they would start elections
they cannot win (no one votes for them) and disrupt the cluster.

**Solution:**
`runFollower()` checks `voters.IsVoter(n.cfg.ID)` before starting an election.
Observers reset the timer and wait indefinitely until they are promoted.

---

## 3. Followers could not answer leader redirect requests

**Problem:**
When a non-leader node receives an `ObserverJoin` request it should redirect
the caller to the leader. We stored `leaderID` (a name like `ctrl-2`) but not
`leaderAddr` (the TCP address). The redirect was useless — the joining node
cannot dial a name, only an address.

**Solution:**
Added `LeaderAddr string` to `AppendEntriesRequest`. The leader puts its own
raft listen address in every heartbeat. Followers store it as `n.leaderAddr`
on every heartbeat received. All redirect responses now return this address.
Every follower is always up-to-date because heartbeats arrive every
`CONTROLLER_HEART_BEAT_INTERVAL` ms.

---

## 4. Separate ClusterInfo round-trip was redundant

**Problem:**
The original join flow was:
```
ObserverJoin sends ClusterInfo to seed → get LeaderAddr → ObserverJoin to leader
```
This was two round-trips. If the seed happened to be the leader, the first
call was completely wasted.

**Solution:**
`ObserverJoin` already returns `LeaderAddr` in its response when the node is
not the leader. So we send `ObserverJoin` directly to each seed. If the seed
is not the leader, we get the leader address back and redirect immediately —
no separate `ClusterInfo` call needed.

`ClusterInfo` is kept as a handler for external monitoring and HTTP status
but removed from the join bootstrap path.

---

## 5. JOIN_ADDR was a single point of failure for bootstrap

**Problem:**
`JOIN_ADDR` accepted only one address. If that node was down the joining node
had no fallback and failed immediately.

**Solution:**
Renamed to `BOOTSTRAP_SERVERS` (comma-separated, same format as `PEER_NODES`).
The join loop tries each seed in order. Dead seeds are skipped with a logged
error. After all seeds are tried, it waits `JOIN_RETRY_INTERVAL_MS` and tries
again. Gives up after `JOIN_MAX_RETRIES` full passes.

**New env vars:** `BOOTSTRAP_SERVERS`, `JOIN_MAX_RETRIES` (default 10),
`JOIN_RETRY_INTERVAL_MS` (default 2000ms)

---

## 6. Port conflict on restart during development

**Problem:**
Restarting a controller during development always failed with
`bind: address already in use` because the previous process was still holding
the port (Go takes a moment to release it, or the process was killed hard).

**Solution:**
`KILL_PORT_ON_START=true` (default). Before binding, the controller finds any
process on `RAFT_PORT` via `lsof` (macOS) or `fuser` (Linux) and sends
`kill -9`. If nothing is on the port it is a no-op. Set to `false` in
production where you never want this behaviour.

---

## 7. Quorum counted observers as voters

**Problem:**
In early dynamic mode implementation, `maybeAdvanceCommitIndex` counted all
peers in `matchIndex` toward quorum — including observers. An observer that
was far behind could block log commits.

**Solution:**
Quorum is calculated from `VoterSet.QuorumSize()` which counts only members
with `State == NodeVoter`. The leader iterates `voters.Members()` and skips
any `NodeObserver` when checking `matchIndex`. Observers receive heartbeats
and replicate the log, but they never block commits.
