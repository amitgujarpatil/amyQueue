# Admin API

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

**Response:**
```json
{"Success": true}
```

---

## DELETE /cluster/voters/{id}

Removes a voter from the cluster. The removed node commits its own removal.

```bash
curl -X DELETE http://localhost:8080/cluster/voters/ctrl-2
```

!!! warning
    Do not remove a voter if it would drop the cluster below 3 nodes.
