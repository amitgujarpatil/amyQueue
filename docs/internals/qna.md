# Q&A

Unique questions asked during development — with full answers. These capture
understanding that isn't obvious from reading the code or architecture docs.

---

## 2026-05-18 — What does the HTTP admin server do and why was it added?

**Q:** In controller nodes we have an admin server — why did we add it and what does it do?

**A:**

The Raft TCP transport (`RAFT_PORT`) speaks only the internal Raft wire
protocol. There is no way to query cluster state or trigger admin operations
over that channel without writing a custom binary client.

The admin server adds an HTTP layer on top of the same `raft.Node` so operators
can inspect and manage the cluster with plain `curl`. It implements the
`raft.AdminService` interface — four methods the node already provides:

| Route | What it does |
|---|---|
| `GET /cluster/status` | Returns leader ID, leader addr, term, full member list. Safe on any node. |
| `POST /cluster/observers/join` | Registers a new node as an observer — receives log replication but does not vote. |
| `POST /cluster/voters` | Promotes an observer to a full voter via a committed Raft log entry. |
| `DELETE /cluster/voters/{id}` | Removes a voter via a committed Raft log entry. |

The server takes a `raft.AdminService` interface, not a concrete `*raft.Node`.
Replacing HTTP with gRPC later = new adapter in `api/metadata/grpc/`, zero
changes to Raft core.

This is the same split Kafka makes: broker port 9092 speaks Kafka wire
protocol only; a separate JMX/admin port exposes management. KRaft controller
nodes do the same — internal controller port for replication, separate admin
surface for operators.

---

## 2026-05-18 — How does the leader know an observer is safe to promote? Is AppendEntries already log replication?

**Q:** You said "when the new node catches up to the leader log" — but do we
actually have log replication right now, or is AppendEntries just a heartbeat?
And how do we print a message saying it's safe to promote?

**A:**

`AppendEntries` is both the heartbeat and the replication RPC — same call,
same code path. When the leader calls `n.log.entriesFrom(nextIdx)` and entries
exist, they are included in the request. When there is nothing new, the entries
field is empty and it is a pure heartbeat. Observers receive `AppendEntries`
exactly like voters do — the comment in `broadcastHeartbeat` says
`// observers need replication to catch up`.

**How "caught up" is measured:**

Every successful `AppendEntries` response updates `matchIndex[addr]` — the
highest log index confirmed replicated on that peer. Lag is:

```
lag = leader.lastIndex - matchIndex[observer.addr]
```

`maybePromoteObservers()` runs after every heartbeat tick and computes this
for every observer. When `lag <= AUTO_PROMOTE_LAG_THRESHOLD` (default 10),
the observer is caught up.

**The new log message (manual mode):**

When `AUTO_PROMOTE=false` (the default) and an observer crosses the threshold,
the leader now logs:

```
observer caught up, safe to promote to voter
  node_id=ctrl-4  addr=localhost:7004  lag=0
  hint=POST /cluster/voters {"node_id":"ctrl-4","addr":"localhost:7004"}
```

`pendingPromotion` suppresses repeats every heartbeat — the message fires once
per leadership term per observer. A newly elected leader re-evaluates all
observers and logs it again if they are still caught up.

**Auto-promote mode:**

With `AUTO_PROMOTE=true` the leader calls `AddVoter` automatically through the
Raft log (same path as the manual `POST /cluster/voters`) and logs:

```
auto-promoting observer to voter  node_id=ctrl-4  addr=localhost:7004  lag=0
```

---

## 2026-05-18 — Should the admin server run on every controller node or only the leader?

**Q:** On each controller node we need to run this HTTP admin server, right?

**A:**

Yes, every controller runs it. Three reasons:

1. **The leader changes.** If only one node ran the admin server and lost
   leadership or crashed, there would be no admin surface until manual
   intervention.

2. **Non-leader routes redirect gracefully.** Write operations (`JoinAsObserver`,
   `AddVoter`, `RemoveVoter`) check `n.state == Leader`. A follower returns
   `LeaderAddr` in the response so the caller can follow the redirect. No
   requests are silently dropped.

3. **`GET /cluster/status` is useful on any node.** Current term, leader
   address, full member list — useful for health checks, monitoring, debugging.

Brokers do not run this server. They have no Raft state — membership
management is a controller concern. Brokers will get their own HTTP surface
in a later phase (health probes, metrics) but backed by a different interface.
