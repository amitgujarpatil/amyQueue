# Phase 2 — Broker: Design Discussion

> Living doc. Updated throughout the design session before implementation begins.

---

## Questions Being Discussed

- [ ] What fields define a broker record?
- [ ] BrokerEpoch type, who assigns it, when does it increment?
- [ ] RackID — include now or defer?
- [ ] IncarnationID — needed or is BrokerEpoch enough?
- [ ] Multiple listeners — one host:port now or extensible from the start?
- [ ] Controller store vs broker self-state — what lives where?
- [ ] Re-registration — what happens when a broker restarts?
- [ ] Idempotency on retry?
- [ ] Controller failover during registration?
- [ ] Metrics aligned with Kafka?
- [ ] Key log events?
- [ ] What to pick from Kafka now vs defer?
- [x] Authentication — how do only trusted nodes join?

---

## 1. What Is a Broker

A broker does five things:

```
1. Stores partition data (actual messages on disk)
2. Serves PRODUCE requests from writers
3. Serves FETCH requests from consumers
4. Replicates data — followers pull from the leader broker
5. Reports state to the controller — heartbeats and LEO offsets
```

The controller never touches message data. It only tracks metadata and pushes
instructions to brokers via LeaderAndISR (Phase 9).

---

## 2. Broker Identity — Candidate Fields

Kafka KRaft stores per broker: BrokerID (int32), IncarnationID (UUID),
RackID (string), EndPoints (multiple listeners), BrokerEpoch (int64).

AmyQueue candidate:

```go
type BrokerInfo struct {
    BrokerID  BrokerID   // permanent — string alias from Phase 1
    Host      string
    Port      int32
    RackID    string     // store now, use in rack-aware assignment later
    Epoch     int64      // controller-assigned fencing key
}
```

BrokerID as string: Kafka uses int32. AmyQueue already defined BrokerID as a
typed string in Phase 1. Keep it — no auto-increment registry needed, operators
can name brokers meaningfully.

IncarnationID: Kafka uses a UUID that changes on every restart to let the
controller distinguish restarts from running sessions. BrokerEpoch covers the
same need in AmyQueue — every restart produces a higher epoch. Defer.

RackID: Add the field now even if the assignment algorithm ignores it. Changing
RegisterBrokerPayload and BrokerInfo later means a Raft log schema change.
Cheaper to carry an empty string now.

Multiple listeners: Single host:port for now. A []Endpoint slice adds complexity
with no current benefit. Defer.

BrokerEpoch int32 vs int64: Kafka uses int64. int32 overflows after ~2 billion
registrations. Use int64.

---

## 3. BrokerEpoch — The Fencing Mechanism

Prevents zombie brokers. Most important concept in Phase 2.

The zombie broker problem:

```
t=1  B3 is leader for P0, serving writes
t=2  B3 loses network — B3 thinks alive, others see it dead
t=3  Controller detects B3 dead via heartbeat timeout
t=4  Controller elects B1 as new leader for P0
t=5  B3 network recovers — still thinks it is leader for P0
t=6  Producer sends write to B3 (stale routing metadata)
     B3 accepts it — DATA DIVERGENCE: B3 and B1 both think they are leader
```

How BrokerEpoch fixes it:

```
t=1  B3 registered Epoch=5
t=2  B3 loses network
t=3  Controller detects B3 dead
t=4  B1 elected new leader
t=5  B3 recovers, re-registers -> controller assigns Epoch=6
t=6  Any request B3 makes with Epoch=5 is REJECTED: stale epoch
     B3 must re-register and get Epoch=6 first
     By then it learns it lost leadership and must fetch from B1
```

Who assigns BrokerEpoch: the state machine only, inside applyRegisterBroker.
Same principle as D5 — HTTP handler proposes, state machine assigns. Nothing
else may generate an epoch.

When does it increment: on every registration event. First registration:
epoch=1. Every restart: epoch++. A broker that never restarts keeps its epoch.

---

## 4. Registration Flow

```
        BROKER                              CONTROLLER LEADER
          |                                       |
  startup |                                       |
          |-- POST /brokers/register ------------>|
          |   { brokerID, host, port, rackID }    |
          |                                      validate
          |                                      propose RegisterBrokerRecord to Raft
          |                                      (committed across all controllers)
          |                                      applyRegisterBroker:
          |                                        new broker   -> epoch = 1
          |                                        re-register  -> epoch++
          |                                        store BrokerInfo
          |<-- 200 { brokerEpoch: 6 } ------------|
          |                                       |
  store epoch, begin heartbeating                 |
          |-- POST /brokers/{id}/heartbeat ------->|
          |   { brokerID, epoch: 6 }              |
```

Non-leader returns 503 + { leaderID, leaderAddr } — same redirect pattern
already in the Raft HTTP admin.

---

## 5. Edge Cases

Normal restart (same BrokerID): applyRegisterBroker sees BrokerID exists,
increments epoch, updates host:port. Old epoch immediately invalid.

Broker moves to different IP: same BrokerID, different host:port. Overwrites
stored address. Controller uses new address for future LeaderAndISR pushes.

Controller failover mid-registration:
Broker sends POST, leader crashes. Raft commits the entry (quorum still works).
New leader elected. Broker gets connection dropped. Broker retries to new leader
via 503 redirect. applyRegisterBroker must be idempotent:
  same brokerID + same host:port -> return current epoch (no-op)
  same brokerID + different host:port -> epoch++ (re-registration)

Two brokers with same BrokerID (operator error): second registration overwrites
first. First broker fails next heartbeat (stale epoch). They ping-pong epochs
until operator fixes config. Log a clear warning.

Dead broker stays in store: keep BrokerInfo after a broker dies. Config
(host:port, rack) is still needed if it comes back. Liveness tracked separately
in LivenessTracker (Phase 5), not in BrokerInfo. Same as Kafka.

---

## 6. Controller Store vs Broker Self-State

```
CONTROLLER STORE (durable via Raft)       BROKER (its own state only)
Store.brokers                             BrokerID, Host, Port, RackID
  map[BrokerID]*BrokerInfo               MyEpoch (received from controller)
  { BrokerID, Host, Port, RackID,        PartitionStateCache (Phase 9)
    Epoch }

LivenessTracker (Phase 5, ephemeral,
  leader-only, resets on failover)
  lastHeartbeat map[BrokerID]time.Time
```

---

## 7. Logs — Key Events

Broker-side:
```
INFO  registering with controller         addr=... brokerID=...
INFO  registration successful             brokerID=... epoch=6
WARN  registration failed retrying        attempt=2 err=...
WARN  stale epoch rejected by controller  oldEpoch=5
INFO  re-registered after stale epoch     newEpoch=6
DEBUG heartbeat acknowledged              metadataVersion=42
WARN  heartbeat rejected stale epoch      epoch=5
INFO  becoming leader for partition       topicID=... partitionID=0 leaderEpoch=3
INFO  becoming follower for partition     topicID=... partitionID=1 leaderID=...
```

Controller-side:
```
INFO  broker registered                   brokerID=... host=... port=... epoch=6
WARN  broker re-registered new address    brokerID=... oldAddr=... newAddr=...
WARN  possible duplicate brokerID         brokerID=...
INFO  broker declared dead                brokerID=... epoch=6 reason=heartbeat_timeout
```

---

## 8. Metrics — Kafka-Aligned

Controller-side (extends existing MetricsSource pattern):

| Metric | Type | Description |
|---|---|---|
| registered_broker_count | Gauge | Total brokers in store |
| alive_broker_count | Gauge | Brokers with active heartbeat (Phase 5) |
| broker_registrations_total | Counter | Cumulative registration events |
| offline_partition_count | Gauge | Partitions with Status == Offline |
| under_replicated_partition_count | Gauge | Partitions where len(ISR) < RF |

Broker-side (new Prometheus server on broker):

| Metric | Type | Description |
|---|---|---|
| broker_partition_count | Gauge | Total partitions hosted |
| broker_leader_partition_count | Gauge | Partitions where this broker is leader |
| broker_follower_partition_count | Gauge | Partitions where this broker is follower |
| broker_under_replicated_partitions | Gauge | Hosted partitions where len(ISR) < RF |
| broker_isr_shrinks_total | Counter | ISR shrink events (Phase 7) |
| broker_isr_expands_total | Counter | ISR expand events (Phase 7) |
| broker_bytes_in_total | Counter | Bytes received from producers (future) |
| broker_bytes_out_total | Counter | Bytes sent to consumers (future) |
| broker_messages_in_total | Counter | Messages received from producers (future) |

---

## 9. Design Challenges

C1 — BrokerEpoch assigned in Apply only. applyRegisterBroker is the only place
that assigns and increments BrokerEpoch. Hard invariant — nothing else may
generate an epoch.

C2 — Lock ordering. applyCommitted runs with n.mu held. sm.Apply must not
acquire any lock that another goroutine holds while holding n.mu.
MetadataStateMachine's own sync.RWMutex is always acquired after n.mu — never
the reverse.

C3 — Bootstrapping: first broker, no partitions yet. If CreateTopic was called
before any broker registered, partitions were born Offline (D4 edge case 2).
When the first broker registers, Phase 3 does not auto-trigger — this needs
a decision.

C4 — RackID in assignment algorithm. D1 scores brokers on total leaders and
same-topic leaders. With RackID, add a third penalty term: putting a replica
on the same rack as an existing replica for this partition. gamma >> beta >>
alpha — rack diversity beats topic spread beats load balance.

C5 — Broker never gets a registration response. Retry with exponential backoff.
Idempotency in applyRegisterBroker ensures retry is safe — broker gets correct
current epoch whether or not the first attempt committed.

---

## 10. What We Pick From Kafka vs What We Defer

| Feature | Decision |
|---|---|
| BrokerID as permanent string identifier | Pick |
| Single host:port listener | Pick |
| BrokerEpoch int64 controller-assigned | Pick |
| RackID field stored but unused | Pick |
| Registration via HTTP POST to leader | Pick |
| 503 redirect on non-leader | Pick |
| Idempotent re-registration | Pick |
| BrokerInfo persisted via Raft log | Pick |
| Dead broker stays in store | Pick |
| IncarnationID | Defer |
| Multiple listeners (PLAINTEXT SSL SASL) | Defer |
| Feature and capability negotiation | Defer |
| Rack-aware assignment logic | Defer |
| Explicit deregistration command | Defer |

---

## Decisions Made

### D-Auth — Cluster Authentication: ClusterID + Shared Token

See full design → `cluster-auth-design.md`

Registration request must carry ClusterID and Token validated by HTTP middleware
before the payload reaches the state machine. Heartbeat and all subsequent broker
calls carry the same headers.

RegisterBrokerPayload:
```go
type RegisterBrokerPayload struct {
    BrokerID  BrokerID
    Host      string
    Port      int32
    RackID    string
    ClusterID string   // validated against store.ClusterID
    Token     string   // validated by ClusterAuth middleware
}
```

Token validation is middleware — happens before routing to any handler.
State machine re-validates ClusterID as defence-in-depth.
Stale epoch is a separate 403 after auth passes.
