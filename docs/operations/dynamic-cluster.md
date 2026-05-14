# Dynamic Cluster

In dynamic mode nodes can join and leave without restarting the cluster.

## Starting the founding 3 nodes

Same as static but with `CLUSTER_MODE=dynamic`:

```bash
NODE_ROLE=controller NODE_ID=ctrl-1 RAFT_PORT=7001 HTTP_PORT=8080 \
  PEER_NODES="localhost:7002,localhost:7003" CLUSTER_MODE=dynamic \
  go run ./src/cmd/controller
```

## Adding a new node

```bash
NODE_ROLE=controller NODE_ID=ctrl-4 RAFT_PORT=7004 HTTP_PORT=8083 \
  CLUSTER_MODE=dynamic \
  BOOTSTRAP_SERVERS="localhost:7001,localhost:7002,localhost:7003" \
  JOIN_MAX_RETRIES=10 JOIN_RETRY_INTERVAL_MS=2000 \
  go run ./src/cmd/controller
```

The node will:

1. Try each bootstrap server with `ObserverJoin`
2. Follow redirect to leader if needed
3. Register as observer — start receiving log replication
4. Catch up to leader's log

Promote it to voter once caught up:

```bash
curl -X POST http://localhost:8080/cluster/voters \
  -H "Content-Type: application/json" \
  -d '{"node_id":"ctrl-4","addr":"localhost:7004"}'
```

## Removing a node

```bash
curl -X DELETE http://localhost:8080/cluster/voters/ctrl-2
```

The removed node steps down gracefully. The quorum recalculates automatically.

## Bootstrap join flow

```
New node starts
  │
  ├─ try BOOTSTRAP_SERVERS[0] ──► unreachable? → try next seed
  │
  ├─ try BOOTSTRAP_SERVERS[1] ──► not leader? → get LeaderAddr → redirect
  │                                              │
  │                                              ▼
  │                                         ObserverJoin(leader)
  │                                              │
  │                                         Success → joined as observer
  │
  └─ all seeds failed → wait JOIN_RETRY_INTERVAL_MS → retry
                        (up to JOIN_MAX_RETRIES attempts)
```
