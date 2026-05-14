# AmyQueue

**AmyQueue** is a distributed message queue built from scratch — Amit's Kafka.

Built to deeply understand how systems like Apache Kafka work under the hood:
Raft consensus, leader election, partition replication, producer/consumer APIs,
and everything in between.

---

## What we're building

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
                │ Metadata distribution
       ┌────────┴────────┐
       ▼                 ▼
   ┌──────┐          ┌──────┐
   │Broker│          │Broker│
   │  -1  │          │  -2  │
   └──────┘          └──────┘
```

- **Controllers** — Raft cluster managing metadata, leader election, broker health
- **Brokers** — Store and serve messages, replicate partitions
- **Producers** — Write messages to topics
- **Consumers** — Read messages from topics

---

## Current status

| Component | Status |
|---|---|
| Raft consensus (leader election, heartbeat, log replication) | ✅ Done |
| Static cluster mode | ✅ Done |
| Dynamic cluster mode (observer → voter promotion) | ✅ Done |
| Bootstrap discovery | ✅ Done |
| Admin HTTP API | ✅ Done |
| Broker registration | 🔧 Next |
| Partition log storage | 🔧 Planned |
| Producer API | 🔧 Planned |
| Consumer API | 🔧 Planned |

---

## Quick start

```bash
git clone git@github.com:amitgujarpatil/amyQueue.git
cd amyQueue

# Terminal 1
NODE_ROLE=controller NODE_ID=ctrl-1 RAFT_PORT=7001 HTTP_PORT=8080 \
  PEER_NODES="localhost:7002,localhost:7003" CLUSTER_MODE=static \
  go run ./src/cmd/controller

# Terminal 2
NODE_ROLE=controller NODE_ID=ctrl-2 RAFT_PORT=7002 HTTP_PORT=8081 \
  PEER_NODES="localhost:7001,localhost:7003" CLUSTER_MODE=static \
  go run ./src/cmd/controller

# Terminal 3
NODE_ROLE=controller NODE_ID=ctrl-3 RAFT_PORT=7003 HTTP_PORT=8082 \
  PEER_NODES="localhost:7001,localhost:7002" CLUSTER_MODE=static \
  go run ./src/cmd/controller

# Check who is leader
curl http://localhost:8080/cluster/status
```
