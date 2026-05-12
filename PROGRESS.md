# AmyQueue — Build Progress

A running log of what was built, why, and how. Updated as we go.

---

## 1. Project bootstrap
**What:** Set up the full project skeleton — directory layout, Go module, Makefile, scripts, stub binaries for broker/controller/cli, and docs.  
**Why:** Needed a clean starting point before writing any real logic. Having the structure in place means every future piece has an obvious home.  
**Key files:** `go.mod`, `Makefile`, `src/cmd/broker/main.go`, `src/cmd/controller/main.go`, `src/cmd/cli/main.go`, `scripts/`, `docs/ROADMAP.md`

---

## 2. Config package — env-based configuration
**What:** Created `src/internal/config` package that:
- Loads `.env` file on startup (via `godotenv`) without overwriting real env vars already set in the shell
- Maps env vars to a typed `Config` struct
- Validates `NODE_ROLE` (must be `controller` or `broker`)
- Parses `PEER_NODES` from a comma-separated string into `[]string`

**Why:** Both broker and controller need configuration. Using env vars (with `.env` as a local dev convenience) is the standard 12-factor approach — no YAML config files to maintain, easy to override in any environment.

**Key env vars introduced:**

| Variable | Default | Purpose |
|---|---|---|
| `NODE_ROLE` | `broker` | Whether this process is a controller or broker |
| `CONTROLLER_HOST` | `localhost` | Host of the controller this node talks to |
| `CONTROLLER_PORT` | `8080` | Port of the controller |
| `PEER_NODES` | _(empty)_ | Comma-separated addresses of all peer nodes |
| `NODE_ID` | `node-1` | Unique name for this node in the cluster |
| `HTTP_PORT` | `8080` | Port for the HTTP API |
| `GRPC_PORT` | `8082` | Port for the gRPC API |
| `RAFT_PORT` | `8081` | Port for Raft consensus communication |
| `LOG_LEVEL` | `info` | Logging verbosity |

**How to use:**
```bash
# Using .env file
cp .env.example .env
# edit .env with your values
go run ./src/cmd/broker

# Or pass inline
NODE_ROLE=broker CONTROLLER_HOST=localhost PEER_NODES="localhost:9092,localhost:9093" go run ./src/cmd/broker
```

**Key files:** `src/internal/config/config.go`, `.env.example`, `go.mod` (added `godotenv`)

---

---

## 3. Raft consensus engine — leader election + transport abstraction

**What:** Built a full Raft implementation for controller nodes:

- `src/internal/raft/messages.go` — wire message types (`VoteRequest`, `VoteResponse`, `AppendEntriesRequest`, `AppendEntriesResponse`). Transport-agnostic structs.
- `src/internal/raft/log.go` — append-only in-memory log with sentinel at index 0, thread-safe, supports append/truncate on conflict.
- `src/internal/raft/transport.go` — `Transport` interface (the swap-out seam) + `Handlers` callback struct. Core never touches the wire.
- `src/internal/raft/node.go` — full state machine: Follower → Candidate → Leader loops, randomised election timeout, vote collection with quorum check, heartbeat broadcast, log replication, commit index advancement, split-brain protection via term comparison.
- `src/internal/raft/tcp/transport.go` — TCP implementation of `Transport`. Each RPC is: dial → gob-encode request → gob-decode response → close. Simple and correct for a 3-5 node cluster.

**Why transport interface:** Replacing TCP with gRPC, HTTP, or a custom binary protocol only requires implementing `Transport`. The `node.go` business logic is never touched. This was the core design goal.

**Split-brain / network partition handling:**
- Any node that receives a message with `term > currentTerm` immediately steps down to follower. This prevents two nodes believing they are leader at the same time.
- Leader must receive responses from a quorum of peers before advancing `commitIndex`. A partitioned leader that can't reach a majority stops committing — it does not serve stale writes.
- Randomised election timeout (base to base×1.5) ensures split votes resolve quickly without coordination.

**Key env vars added:**

| Variable | Default | Purpose |
|---|---|---|
| `RAFT_ELECTION_TIMEOUT_MS` | `1000` | Base election timeout (actual is randomised up to +50%) |
| `RAFT_HEARTBEAT_INTERVAL_MS` | `100` | How often leader sends heartbeats |

**How to run a 3-node controller cluster (copy-paste ready):**

Terminal 1 — controller-1:
```bash
NODE_ROLE=controller NODE_ID=ctrl-1 RAFT_PORT=7001 \
  PEER_NODES="localhost:7002,localhost:7003" \
  RAFT_ELECTION_TIMEOUT_MS=1000 RAFT_HEARTBEAT_INTERVAL_MS=100 \
  LOG_LEVEL=info \
  go run ./src/cmd/controller
```

Terminal 2 — controller-2:
```bash
NODE_ROLE=controller NODE_ID=ctrl-2 RAFT_PORT=7002 \
  PEER_NODES="localhost:7001,localhost:7003" \
  RAFT_ELECTION_TIMEOUT_MS=1000 RAFT_HEARTBEAT_INTERVAL_MS=100 \
  LOG_LEVEL=info \
  go run ./src/cmd/controller
```

Terminal 3 — controller-3:
```bash
NODE_ROLE=controller NODE_ID=ctrl-3 RAFT_PORT=7003 \
  PEER_NODES="localhost:7001,localhost:7002" \
  RAFT_ELECTION_TIMEOUT_MS=1000 RAFT_HEARTBEAT_INTERVAL_MS=100 \
  LOG_LEVEL=info \
  go run ./src/cmd/controller
```

Kill any controller to watch re-election happen automatically.

**Key files:** `src/internal/raft/node.go`, `src/internal/raft/transport.go`, `src/internal/raft/tcp/transport.go`, `src/internal/raft/messages.go`, `src/internal/raft/log.go`

---

## What's next
- Add gRPC transport (implement `raft.Transport` interface, drop in as replacement for TCP)
- Broker registration: broker connects to controller leader on startup
- Controller tracks live brokers in-memory, detects failures via missed heartbeats
