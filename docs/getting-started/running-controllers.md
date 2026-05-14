# Running Controllers

## Prerequisites

- Go 1.22+
- Clone the repo: `git clone git@github.com:amitgujarpatil/amyQueue.git`

---

## Static mode (3-node cluster)

Open three terminals and run one command in each.

=== "Terminal 1 — ctrl-1"
    ```bash
    NODE_ROLE=controller NODE_ID=ctrl-1 \
      RAFT_PORT=7001 HTTP_PORT=8080 \
      PEER_NODES="localhost:7002,localhost:7003" \
      CLUSTER_MODE=static \
      go run ./src/cmd/controller
    ```

=== "Terminal 2 — ctrl-2"
    ```bash
    NODE_ROLE=controller NODE_ID=ctrl-2 \
      RAFT_PORT=7002 HTTP_PORT=8081 \
      PEER_NODES="localhost:7001,localhost:7003" \
      CLUSTER_MODE=static \
      go run ./src/cmd/controller
    ```

=== "Terminal 3 — ctrl-3"
    ```bash
    NODE_ROLE=controller NODE_ID=ctrl-3 \
      RAFT_PORT=7003 HTTP_PORT=8082 \
      PEER_NODES="localhost:7001,localhost:7002" \
      CLUSTER_MODE=static \
      go run ./src/cmd/controller
    ```

Within ~1 second one node will win the election. Check who is leader:

```bash
curl http://localhost:8080/cluster/status
```

---

## Dynamic mode — adding a 4th controller at runtime

Start the initial 3 nodes with `CLUSTER_MODE=dynamic`:

```bash
NODE_ROLE=controller NODE_ID=ctrl-1 RAFT_PORT=7001 HTTP_PORT=8080 \
  PEER_NODES="localhost:7002,localhost:7003" CLUSTER_MODE=dynamic \
  go run ./src/cmd/controller
```

Start a new node as observer — it will find the leader automatically:

```bash
NODE_ROLE=controller NODE_ID=ctrl-4 RAFT_PORT=7004 HTTP_PORT=8083 \
  CLUSTER_MODE=dynamic \
  BOOTSTRAP_SERVERS="localhost:7001,localhost:7002,localhost:7003" \
  go run ./src/cmd/controller
```

Promote it to voter when it has caught up:

```bash
curl -X POST http://localhost:8080/cluster/voters \
  -H "Content-Type: application/json" \
  -d '{"node_id":"ctrl-4","addr":"localhost:7004"}'
```

---

## Verifying the cluster

```bash
# who is the leader, what is the term, who are the members
curl http://localhost:8080/cluster/status | jq

# kill the leader — watch re-election in the logs
kill <leader-pid>
```

!!! tip
    Set `LOG_LEVEL=debug` to see every heartbeat sent and received.
