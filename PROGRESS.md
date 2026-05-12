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

## What's next
- Phase 2: Broker registers itself with the controller on startup (HTTP or gRPC call)
- Phase 2: Controller tracks which brokers are alive (in-memory map to start)
- Phase 3: Raft election between controller nodes
