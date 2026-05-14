# Transport Layer

AmyQueue uses a **pluggable transport design** — the Raft core and all service
logic is completely decoupled from the wire protocol.

---

## The rule

> Dependency arrow always points **domain → transport**, never the reverse.

| Layer | Package | Knows about | Never imports |
|---|---|---|---|
| TCP primitives | `transport/tcp` | `net.Conn` only | raft, api, broker |
| gRPC primitives | `transport/grpc` | gRPC server/client | raft, api, broker |
| HTTP primitives | `transport/http` | HTTP server/middleware | raft, api, broker |
| Raft over TCP | `raft/tcp` | `raft.*` + `transport/tcp` | api, broker |
| Raft over gRPC | `raft/grpc` *(future)* | `raft.*` + `transport/grpc` | api, broker |
| Admin HTTP | `api/metadata/http` | `raft.AdminService` + HTTP | TCP internals |

---

## The seam: Transport interface

```go
type Transport interface {
    Start(handlers Handlers) error
    SendVoteRequest(ctx, addr, req) (VoteResponse, error)
    SendAppendEntries(ctx, addr, req) (AppendEntriesResponse, error)
    SendObserverJoin(ctx, addr, req) (ObserverJoinResponse, error)
    SendClusterInfo(ctx, addr, req) (ClusterInfoResponse, error)
    Close() error
}
```

The Raft node only talks to this interface. Swapping TCP for gRPC means
implementing this interface in `raft/grpc/transport.go` — zero changes to
`node.go`.

---

## The seam: ConnHandler

```go
type ConnHandler func(conn net.Conn)
```

`transport/tcp.Server` calls this per accepted connection. The domain layer
(e.g. `raft/tcp`) owns all framing and decode logic inside the handler.
This is the only point where raw TCP and domain code meet.

---

## Adding a new service (pattern)

To add a producer API over TCP:

1. `transport/tcp` — already done, no changes
2. Create `api/producer/tcp/handler.go`:
   - Implements a `ConnHandler` with your message framing
   - Uses `transport/tcp.NewServer` and `transport/tcp.Dial`
   - Never touches `raft` or other domains
3. Create `api/producer/service.go` — transport-agnostic interface
4. Wire into `cmd/broker/main.go`

To add gRPC for the same producer API:

1. Build `transport/grpc/server.go` — generic gRPC setup
2. Create `api/producer/grpc/handler.go` — implements the same `ProducerService` interface
3. Drop in as a replacement in `cmd/broker/main.go` — producer logic untouched
