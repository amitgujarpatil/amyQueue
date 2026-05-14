# Architecture Overview

AmyQueue is split into two types of nodes: **controllers** and **brokers**.

```
┌─────────────────────────────────────────┐
│     Controller Cluster (Raft)           │
│  ┌──────┐  ┌──────┐  ┌──────┐          │
│  │Ctrl-1│  │Ctrl-2│  │Ctrl-3│          │
│  │Leader│  │Follow│  │Follow│          │
│  └───┬──┘  └───┬──┘  └───┬──┘          │
└──────┼─────────┼─────────┼─────────────┘
       │  Raft consensus    │
       └────────┬───────────┘
                │ Metadata (topics, partitions, broker map)
       ┌────────┴────────┐
       ▼                 ▼
   ┌──────┐          ┌──────┐
   │Broker│          │Broker│
   │  -1  │          │  -2  │
   └──────┘          └──────┘
       ▲                 ▲
  Produce/Consume    Produce/Consume
       │                 │
   Producers          Consumers
```

---

## Controllers

- Form a **Raft cluster** (typically 3 or 5 nodes)
- Manage cluster metadata: topics, partition assignments, broker health
- Do **not** store messages — that is the broker's job
- One leader at a time; leader handles all metadata writes
- Followers replicate the log and can answer read requests

## Brokers

- Store partition data (the actual messages)
- Serve producer write requests and consumer read requests
- Register with the controller on startup
- Receive partition assignments from the controller leader
- Replicate data to peer brokers based on replication factor

## Code layout

```
src/
  cmd/
    controller/   ← controller entry point
    broker/       ← broker entry point
    cli/          ← admin CLI (planned)
  internal/
    config/       ← env-based configuration
    raft/         ← Raft state machine + log
      tcp/        ← Raft over TCP transport
    transport/
      tcp/        ← generic TCP server + client
      grpc/       ← generic gRPC (future)
      http/       ← generic HTTP (future)
    api/
      metadata/   ← controller metadata API
      broker/     ← broker registration API
      producer/   ← producer API (planned)
      consumer/   ← consumer API (planned)
    broker/
      partition/  ← partition management
      storage/    ← message log storage
```
