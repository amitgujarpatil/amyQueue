# Configuration Reference

All configuration is via environment variables. Copy `.env.example` to `.env`
for local development — the node loads it automatically at startup without
overwriting variables already set in the shell.

```bash
cp .env.example .env
```

---

## Node identity

| Variable | Default | Description |
|---|---|---|
| `NODE_ID` | `node-1` | Unique name for this node in the cluster |
| `NODE_ROLE` | `broker` | `controller` or `broker` |

---

## Ports

| Variable | Default | Description |
|---|---|---|
| `RAFT_PORT` | `8081` | Raft consensus TCP port |
| `HTTP_PORT` | `8080` | HTTP admin API port |
| `GRPC_PORT` | `8082` | gRPC port (future) |

---

## Cluster

| Variable | Default | Description |
|---|---|---|
| `CLUSTER_MODE` | `static` | `static` or `dynamic` |
| `PEER_NODES` | _(empty)_ | Comma-separated peer raft addresses for static mode |
| `BOOTSTRAP_SERVERS` | _(empty)_ | Comma-separated seed addresses for dynamic join |
| `CONTROLLER_HOST` | `localhost` | Controller host (used by brokers) |
| `CONTROLLER_PORT` | `8080` | Controller HTTP port (used by brokers) |

---

## Raft tuning

| Variable | Default | Description |
|---|---|---|
| `RAFT_ELECTION_TIMEOUT_MS` | `1000` | Base election timeout. Actual is randomised up to +50% to avoid split votes |
| `CONTROLLER_HEART_BEAT_INTERVAL` | `100` | How often leader sends heartbeats (ms). Keep at least 5-10× smaller than election timeout |

!!! warning "Ratio rule"
    Always keep `CONTROLLER_HEART_BEAT_INTERVAL` well below `RAFT_ELECTION_TIMEOUT_MS`.
    A good ratio is 1:10. If heartbeats are too slow, followers time out and call
    unnecessary elections.

---

## Dynamic join

| Variable | Default | Description |
|---|---|---|
| `JOIN_MAX_RETRIES` | `10` | Full passes over all bootstrap servers before giving up |
| `JOIN_RETRY_INTERVAL_MS` | `2000` | Wait between retry passes (ms) |

---

## Auto-promotion

| Variable | Default | Description |
|---|---|---|
| `AUTO_PROMOTE` | `false` | Leader auto-promotes observers to voters once they catch up |
| `AUTO_PROMOTE_LAG_THRESHOLD` | `10` | Max log entries behind the leader to be considered caught up |

!!! note
    Only meaningful when `CLUSTER_MODE=dynamic`. With `AUTO_PROMOTE=false` (default)
    an observer stays an observer until you call `POST /cluster/voters` manually.
    Set it to `true` only on nodes you intend to become full voting members.

---

## Developer conveniences

| Variable | Default | Description |
|---|---|---|
| `KILL_PORT_ON_START` | `true` | Free `RAFT_PORT` before binding. Useful in dev, disable in production |
| `LOG_LEVEL` | `info` | `debug`, `info`, `warn`, `error` |
