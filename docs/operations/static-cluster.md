# Static Cluster

In static mode the voter set is fixed at startup. No nodes can be added or
removed without restarting the cluster. Best for development and simple setups.

## Starting a 3-node cluster

=== "Terminal 1"
    ```bash
    NODE_ROLE=controller NODE_ID=ctrl-1 \
      RAFT_PORT=7001 HTTP_PORT=8080 \
      PEER_NODES="localhost:7002,localhost:7003" \
      CLUSTER_MODE=static \
      go run ./src/cmd/controller
    ```

=== "Terminal 2"
    ```bash
    NODE_ROLE=controller NODE_ID=ctrl-2 \
      RAFT_PORT=7002 HTTP_PORT=8081 \
      PEER_NODES="localhost:7001,localhost:7003" \
      CLUSTER_MODE=static \
      go run ./src/cmd/controller
    ```

=== "Terminal 3"
    ```bash
    NODE_ROLE=controller NODE_ID=ctrl-3 \
      RAFT_PORT=7003 HTTP_PORT=8082 \
      PEER_NODES="localhost:7001,localhost:7002" \
      CLUSTER_MODE=static \
      go run ./src/cmd/controller
    ```

## Testing failover

Kill the leader process. Within `RAFT_ELECTION_TIMEOUT_MS` (default 1s) one of
the remaining nodes will call an election and become the new leader.

```bash
# find the leader
curl -s http://localhost:8080/cluster/status | grep LeaderID

# stop it — re-election happens automatically
```
