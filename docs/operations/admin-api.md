# Admin API

## Why it exists

The Raft TCP transport (`RAFT_PORT`) speaks only the internal Raft wire
protocol — vote requests, append entries, observer join RPCs. There is no way
to query cluster state or trigger administrative operations over that channel
from outside the cluster.

The HTTP admin server adds an **operator-facing layer** on top of the same
`raft.Node`, so you can inspect and manage the cluster without touching the
internal Raft framing. It implements the `raft.AdminService` interface — the
same four methods the Raft node already provides. Replacing HTTP with gRPC
later means writing a new adapter that calls the same interface; the Raft core
does not change.

This is the same split Kafka makes: the broker binary port (9092) speaks only
the Kafka wire protocol; a separate JMX/admin port exposes management
operations. Controller nodes in KRaft mode do the same — internal controller
port for replication, separate admin surface for operators.

## Why every controller node runs it

The admin server starts unconditionally on every controller, not only on the
leader. Three reasons:

1. **The leader changes.** If only one node ran the admin server and lost
   leadership or crashed, there would be no admin surface until manual
   intervention.

2. **Non-leader routes redirect gracefully.** `JoinAsObserver`, `AddVoter`,
   and `RemoveVoter` all check `n.state == Leader`. A follower returns
   `LeaderAddr` in the response body so the caller can follow the redirect to
   the actual leader. No requests are silently dropped.

3. **`GET /cluster/status` is useful on any node.** It returns whatever that
   node currently knows — current term, leader address, full member list. Safe
   for health checks, monitoring, and debugging.

---

The HTTP admin server starts on `HTTP_PORT` (default `8080`) alongside every
controller node. All membership operations must be directed to the **leader** —
non-leaders return a redirect hint with the current leader address.

---

## GET /cluster/status

Returns current cluster state. Safe to call on any node.

```bash
curl http://localhost:8080/cluster/status
```

```json
{
  "LeaderID": "ctrl-2",
  "LeaderAddr": "localhost:7002",
  "Term": 3,
  "Members": [
    {"ID": "ctrl-1", "Addr": "localhost:7001", "State": "voter"},
    {"ID": "ctrl-2", "Addr": "localhost:7002", "State": "voter"},
    {"ID": "ctrl-3", "Addr": "localhost:7003", "State": "voter"},
    {"ID": "ctrl-4", "Addr": "localhost:7004", "State": "observer"}
  ]
}
```

---

## POST /cluster/observers/join

Called automatically by a new node on startup in dynamic mode. Can also be
called manually to register a node as an observer.

```bash
curl -X POST http://localhost:8080/cluster/observers/join \
  -H "Content-Type: application/json" \
  -d '{"node_id": "ctrl-4", "addr": "localhost:7004"}'
```

**Response (success):**
```json
{"Success": true}
```

**Response (not leader — follow LeaderAddr):**
```json
{
  "Success": false,
  "LeaderID": "ctrl-2",
  "LeaderAddr": "localhost:7002",
  "Err": "not the leader"
}
```

---

## POST /cluster/voters

Promotes an observer to a full voter. Must be sent to the leader.
The change goes through the Raft log — the current quorum commits it first.

```bash
curl -X POST http://localhost:8080/cluster/voters \
  -H "Content-Type: application/json" \
  -d '{"node_id": "ctrl-4", "addr": "localhost:7004"}'
```

**Response (success):**
```json
{"Success": true}
```

**Response (not leader — follow LeaderAddr):**
```json
{
  "Success": false,
  "Err": "not the leader, leader is ctrl-2"
}
```

---

## DELETE /cluster/voters/{id}

Removes a voter from the cluster. The removed node commits its own removal
via a `CmdMembership` log entry. If the removed node is itself, it steps down
to follower immediately after the entry is applied.

```bash
curl -X DELETE http://localhost:8080/cluster/voters/ctrl-2
```

**Response (success):**
```json
{"Success": true}
```

**Response (not leader — follow LeaderAddr):**
```json
{
  "Success": false,
  "Err": "not the leader, leader is ctrl-1"
}
```

!!! warning
    Do not remove a voter if it would drop the cluster below 3 nodes.
