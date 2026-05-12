# Dynamic Cluster Membership in Raft

**Question:** We have static controllers today. How do we safely add/remove controllers
at runtime without restarting, without downtime, and without a split-brain situation?
How does Kafka handle this?

---

## How Kafka (KRaft mode) handles it

Kafka moved from ZooKeeper to its own Raft-based controller quorum called KRaft
(Kafka Raft). Here is exactly what it does for membership changes:

### 1. Voter set is stored in the Raft log itself

Kafka does not use an external config file for the controller list. The set of
voters (which nodes participate in elections and quorum counting) is itself a
record in the Raft log. This means membership changes go through the same
consensus path as any other data change — they are only "applied" once a quorum
commits them. No split-brain possible because the log is the single source of truth.

### 2. Adding a controller (observer → voter promotion)

Kafka calls new nodes **observers** before they become voters.

Steps:
1. New node starts and connects to the cluster as an **observer** — it receives
   log entries and catches up but does NOT vote and is NOT counted in quorum.
2. Once the leader sees the observer is caught up (its log index is close to the
   leader's), the leader writes a **membership change record** into the Raft log
   proposing the observer be promoted to voter.
3. The old quorum (existing voters) commits that record.
4. Only after commit does the new node start voting and count toward quorum.

Why this is safe: at no point are there two different quorum definitions active at
the same time. The old quorum approves the change before the new quorum takes effect.

### 3. Removing a controller (voter → removed)

Steps:
1. Admin issues a remove-voter command to the leader.
2. Leader writes a membership change record removing that node from the voter set.
3. Old quorum (which still includes the node being removed) commits it.
4. After commit the removed node steps down and stops voting.

Why this is safe: the node being removed still participates in committing its own
removal. Quorum is maintained throughout.

### 4. The "joint consensus" problem Kafka avoids

The naive approach — just swap the config — is dangerous. Imagine you have 3 nodes
and you want to move to 5. During the transition:
- Old quorum: majority of {A, B, C} = 2 nodes
- New quorum: majority of {A, B, C, D, E} = 3 nodes

If A+B think old config is active and A+B+C think new config is active, two
different leaders could win elections simultaneously — classic split brain.

Kafka avoids this by never having two quorum definitions active at the same time.
There is always exactly one committed voter set at any point.

---

## The two standard approaches in Raft literature

### Approach 1 — Single-server changes (what Kafka does, what we should do)

Only add or remove **one node at a time**. Mathematically proven safe:
- Adding one node to an odd cluster keeps the old majority disjoint from new majority
- Old quorum commits the change before new quorum is active
- Zero window where two leaders can coexist

Limitation: to go from 3 → 5 nodes you do two separate operations (3→4, 4→5)
with a commit between each.

### Approach 2 — Joint consensus (Raft paper original)

Two-phase commit across old and new config simultaneously. More complex,
allows bulk changes, but much harder to implement correctly.

**We should use Approach 1 (single-server changes).** Same as Kafka.

---

## What we need to build for dynamic mode

### New concepts needed

| Concept | What it is |
|---|---|
| `VoterSet` | The committed list of nodes that vote and count toward quorum |
| Observer | A node that replicates the log but does not vote |
| Membership log entry | A special `LogEntry.Command` type that changes the VoterSet |
| Admin API | HTTP/gRPC endpoint to trigger add-voter / remove-voter |

### Safe add-voter flow (our implementation plan)

```
Admin → POST /cluster/voters {addr: "localhost:7004"}
         ↓
Leader checks: is the new node an observer and is it caught up?
         ↓
Leader appends MembershipChange{action: ADD, addr: "..."}  to log
         ↓
Current quorum commits it (majority of OLD voter set)
         ↓
Node is now a voter — quorum size increases
```

### Safe remove-voter flow

```
Admin → DELETE /cluster/voters/ctrl-2
         ↓
Leader appends MembershipChange{action: REMOVE, nodeID: "ctrl-2"} to log
         ↓
Current quorum (INCLUDING ctrl-2) commits it
         ↓
ctrl-2 steps down, quorum size decreases
```

### Split-brain protection

- Never allow quorum to be calculated from two different voter sets simultaneously
- VoterSet only changes after the membership entry is **committed** (not just appended)
- If the leader dies mid-change, the new leader either commits or rolls back — never
  applies a partial membership change

---

## Static vs Dynamic mode in our system

| | Static mode | Dynamic mode |
|---|---|---|
| Voter set source | `PEER_NODES` env var at startup | Committed Raft log entries |
| Add controller | Requires restart of all nodes | Online, zero downtime |
| Remove controller | Requires restart of all nodes | Online, zero downtime |
| Complexity | Simple, good for dev/local | Required for production |
| Risk | None | Must guard against partial changes |

**Decision:** Keep static mode as default (it is what we have now). Add dynamic mode
as an opt-in once the Raft log is being persisted to disk (because membership state
must survive restarts).

---

## Decisions made

1. **In-memory log for now** — dynamic membership state lives in-memory. A restart
   loses observer registrations. Acceptable until we add disk persistence later.
2. **Admin API: HTTP first, transport-agnostic** — `raft.AdminService` interface is the
   seam. HTTP is the first implementation in `api/metadata/http/`. gRPC can be added
   in `api/metadata/grpc/` without touching Raft or HTTP code.
3. **Observer phase always required** — every new node starts as observer, catches up,
   then an admin explicitly promotes it to voter via `POST /cluster/voters`.

## What was built

| File | Purpose |
|---|---|
| `raft/membership.go` | `VoterSet`, `Member`, `MembershipChange`, observer/voter states |
| `raft/admin.go` | `AdminService` interface — transport-agnostic admin operations |
| `raft/messages.go` | Added `ObserverJoinRequest/Response`, `AddVoterRequest/Response`, `RemoveVoterRequest/Response`, `CommandType` |
| `raft/log.go` | `LogEntry` now carries `CommandType` (data vs membership) |
| `raft/node.go` | VoterSet replaces static peers; quorum from voters only; membership entries applied on commit; observers don't call elections |
| `raft/tcp/transport.go` | Added `tagObserverJoin` + `SendObserverJoin` |
| `api/metadata/http/admin.go` | HTTP server calling `AdminService` |
| `config/config.go` | Added `CLUSTER_MODE`, `JOIN_ADDR` |
| `cmd/controller/main.go` | Wires HTTP admin, dynamic join on startup |

## How to run

### Static mode (default — what we had before)
```bash
# Terminal 1
NODE_ROLE=controller NODE_ID=ctrl-1 RAFT_PORT=7001 HTTP_PORT=8080 \
  PEER_NODES="localhost:7002,localhost:7003" CLUSTER_MODE=static \
  go run ./src/cmd/controller

# Terminal 2
NODE_ROLE=controller NODE_ID=ctrl-2 RAFT_PORT=7002 HTTP_PORT=8081 \
  PEER_NODES="localhost:7001,localhost:7003" CLUSTER_MODE=static \
  go run ./src/cmd/controller

# Terminal 3
NODE_ROLE=controller NODE_ID=ctrl-3 RAFT_PORT=7003 HTTP_PORT=8082 \
  PEER_NODES="localhost:7001,localhost:7002" CLUSTER_MODE=static \
  go run ./src/cmd/controller
```

### Dynamic mode — adding a 4th node at runtime
```bash
# Start initial 3 nodes with CLUSTER_MODE=dynamic and no PEER_NODES
NODE_ROLE=controller NODE_ID=ctrl-1 RAFT_PORT=7001 HTTP_PORT=8080 \
  PEER_NODES="localhost:7002,localhost:7003" CLUSTER_MODE=dynamic \
  go run ./src/cmd/controller

# ... start ctrl-2 and ctrl-3 similarly ...

# Start new node as observer (joins via any existing node)
NODE_ROLE=controller NODE_ID=ctrl-4 RAFT_PORT=7004 HTTP_PORT=8083 \
  CLUSTER_MODE=dynamic JOIN_ADDR=localhost:7001 \
  go run ./src/cmd/controller

# Check status — ctrl-4 appears as observer
curl http://localhost:8080/cluster/status

# Promote ctrl-4 to voter
curl -X POST http://localhost:8080/cluster/voters \
  -H "Content-Type: application/json" \
  -d '{"node_id":"ctrl-4","addr":"localhost:7004"}'

# Remove a voter
curl -X DELETE http://localhost:8080/cluster/voters/ctrl-2
```

### Admin API reference
| Method | Path | Purpose |
|---|---|---|
| GET | `/cluster/status` | Leader, term, all members + their state |
| POST | `/cluster/observers/join` | New node registers itself as observer |
| POST | `/cluster/voters` | Promote observer → voter |
| DELETE | `/cluster/voters/{id}` | Remove a voter |
