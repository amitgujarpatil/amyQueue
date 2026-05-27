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
| Controlled shutdown (SIGTERM -> leader migration -> drain -> exit) | Pick |
| Explicit deregistration command | Defer |

---

## Write Durability Guarantees — acks + MinISR

This is a foundational concept that directly shapes how the broker validates
produce requests in future phases. Documented here so the contract is clear
before implementation begins.

### The Setup

With 1 partition, RF=3, MinISR=2, 3 brokers:

```
B0 = leader replica    (accepts writes, serves reads)
B1 = follower replica  (replicates from B0)
B2 = follower replica  (replicates from B0)

ISR = [B0, B1, B2]
```

### What MinISR Controls

MinISR is a write refusal floor on the broker side. It says:

  If len(ISR) < MinISR, REFUSE all writes — even if a leader exists.

It does not control when a partition goes offline. It controls when writes
are accepted. Reads always work as long as a leader exists.

### What acks Controls

The producer chooses its durability guarantee per request:

| acks | Write considered successful when |
|---|---|
| 0 | Never waits — fire and forget |
| 1 | Leader wrote it to its local log |
| all (-1) | Every current ISR member wrote it to their log |

MinISR only has teeth when acks=all. With acks=1 the producer does not
wait for any ISR member beyond the leader.

### What Happens When the Leader Dies

```
Before: ISR=[B0, B1, B2]  len=3 >= MinISR=2  writes flowing to B0

B0 dies:
  Controller detects death via heartbeat timeout (Phase 5)
  ElectLeader picks B1 — first alive ISR member (Phase 6)
  ISR shrinks to [B1, B2]  len=2 >= MinISR=2

After: B1 is new leader, B2 is follower
  Partition ONLINE — writes continue to B1
  Producers with stale metadata pointing to B0 get "not leader"
  error, refresh metadata, retry to B1 — takes milliseconds
```

The system continues. No write outage as long as ISR stays >= MinISR.

### Data Loss Depends on acks

With acks=all + MinISR=2:
```
Producer writes W1
  B0 waits for B1 and B2 to confirm they wrote it
  All ISR confirmed -> SUCCESS returned to producer
  W1 is on B1 and B2

B0 dies:
  B1 becomes leader — W1 is still there
  ZERO DATA LOSS for any acknowledged write
```

With acks=1:
```
Producer writes W1
  B0 writes it locally, returns SUCCESS immediately
  B1 and B2 have NOT replicated W1 yet

B0 dies before B1/B2 pull W1:
  W1 exists only on dead B0
  B1 becomes leader — W1 is GONE
  DATA LOSS even though producer received SUCCESS
```

### When ISR Drops Below MinISR

If B2 also dies after B0 already died:
```
ISR = [B1]  len=1 < MinISR=2

Partition ONLINE  (B1 is still leader, reads work)
Writes REFUSED    (NOT_ENOUGH_REPLICAS error to producer)

Writes resume only when B2 comes back and rejoins ISR
```

This prevents writing into a dangerously under-replicated state where the
next broker death would cause unavoidable data loss.

### The Guarantee in One Line

```
acks=all + MinISR=2:
  Any write returned SUCCESS is on >= 2 brokers.
  You can lose any 1 broker without losing that data.

acks=1:
  The leader received it. No further guarantee.
```

### Impact on Broker Implementation (Future Phases)

When the broker receives a produce request it must:

1. Check it is the leader for this partition — reject with NOT_LEADER if not
2. Check len(ISR) >= topic.Config.MinISR — reject with NOT_ENOUGH_REPLICAS if not
3. Write to local log
4. If acks=all: wait for all current ISR followers to fetch and acknowledge
5. If acks=1: return success immediately after step 3
6. If acks=0: return success before step 3

MinISR check (step 2) happens BEFORE writing (step 3). A write is never
partially accepted — either the full durability contract is met or the
request is rejected cleanly.

---

## Graceful Shutdown (Controlled Shutdown)

### Why It Matters

Without controlled shutdown every broker restart causes a 30-second outage
window per broker — the controller must wait for the heartbeat timeout before
declaring the broker dead and triggering leader election.

```
CRASH (no controlled shutdown):
  t=0    Broker dies
  t=30s  Controller detects death via heartbeat timeout
  t=30s  Leader election triggered
  = 30 seconds of partition unavailability

CONTROLLED SHUTDOWN:
  t=0    SIGTERM received
  t=~1s  Controller migrates leaders, commits to Raft
  t=~1s  Broker drains and exits
  = ~1 second of metadata refresh, no actual unavailability
```

In a 10-broker rolling restart (upgrade, config change, cert rotation):
  Without controlled shutdown: 10 x 30s = 5 minutes of rolling disruption
  With controlled shutdown:    10 x 1s  = 10 seconds

### How Kafka Does It

Kafka calls it Controlled Shutdown. The broker sends a ControlledShutdownRequest
to the controller. The controller elects new leaders for all partitions the broker
leads (excluding the shutting-down broker from candidates), removes it from ISR of
follower partitions, and responds once all Raft entries commit. The broker then
drains, flushes, and exits. If some partitions cannot be migrated (no other ISR
member), Kafka retries up to controlled.shutdown.max.retries times then exits dirty.

### AmyQueue Design

New BrokerStatus field in BrokerInfo (durable — goes through Raft):

```go
type BrokerStatus string

const (
    BrokerStatusActive       BrokerStatus = "active"
    BrokerStatusShuttingDown BrokerStatus = "shutting_down"
)
```

Dead is NOT in this enum. Death is ephemeral, detected by LivenessTracker,
never written to the Raft log.

New Raft command ShutdownBrokerPayload:

```go
type ShutdownBrokerPayload struct {
    BrokerID      BrokerID
    ExpectedEpoch int64   // CAS guard — same pattern as D5
}
```

applyShutdownBroker: validate BrokerID + epoch match, set Status = ShuttingDown.

Full shutdown sequence:

```
BROKER                              CONTROLLER LEADER
  |                                      |
  | OS sends SIGTERM                     |
  | stop accepting new connections       |
  |                                      |
  |-- POST /brokers/{id}/shutdown ------>|
  |   { epoch, clusterID, token }        |
  |                                   Propose ShutdownBroker -> Raft
  |                                   applyShutdownBroker: Status=ShuttingDown
  |                                   For each leader partition:
  |                                     ElectLeader (exclude this broker)
  |                                     Propose UpdatePartition -> Raft
  |                                   For each follower partition:
  |                                     Propose ISR shrink -> Raft
  |                                   Wait for all to commit
  |<-- 200 { success: true,         ----|
  |    migratedPartitions: 12,          |
  |    failedPartitions: [] }           |
  |                                     |
  | drain in-flight requests            |
  | flush log segments to disk          |
  | exit 0                              |
```

### Failure Handling

No other ISR member for a partition:
```
ISR=[B3], B3 is shutting down
ElectLeader finds no candidates
Partition goes Offline after B3 exits
failedPartitions returned to broker in response
Broker logs WARN and exits anyway — operator must investigate
```

Controller unreachable during shutdown:
```
POST /shutdown times out
Broker retries N times with backoff
After max retries: dirty shutdown — same path as crash
LivenessTracker detects death via heartbeat timeout
```

Shutdown timeout exceeded:
```
shutdown_timeout_ms exceeded before all partitions migrated
Broker aborts, dirty shutdown for remaining partitions
```

SIGKILL:
```
Cannot be caught — dirty shutdown, no controlled path possible
LivenessTracker detects death after heartbeat timeout (~30s)
```

### Configuration

```yaml
shutdown_timeout_ms: 30000          # max wait for controller to migrate leaders
shutdown_drain_timeout_ms: 5000     # max wait for in-flight requests
shutdown_max_retries: 3             # retries if controller unreachable
shutdown_retry_backoff_ms: 5000
```

### Logs During Shutdown

```
INFO  received shutdown signal initiating controlled shutdown
INFO  requesting leader migration   leader_partitions=12 follower_partitions=8
INFO  leader migration complete     migrated=12 failed=0
INFO  removed from ISR              follower_partitions=8
INFO  draining in-flight requests   count=3
INFO  log segments flushed          partitions=20
INFO  broker shutdown complete      epoch=6 uptime=72h

WARN  leader migration incomplete   failed=2 partitions=[topic-A/0 topic-B/3]
WARN  those partitions will go offline after exit
WARN  shutdown timeout exceeded     remaining=5 proceeding dirty
```

### Signal Handling

| Signal | Behaviour |
|---|---|
| SIGTERM | Controlled shutdown — migrate leaders, drain, flush, exit |
| SIGINT | Same as SIGTERM — controlled shutdown |
| SIGKILL | Cannot catch — dirty shutdown, 30s recovery via heartbeat timeout |

### Differences From Kafka

| | Kafka | AmyQueue |
|---|---|---|
| Shutdown request | Dedicated Kafka protocol RPC | HTTP POST on admin server |
| Retry loop | Per-partition retries | Single round, retry full request |
| ISR removal for followers | Part of controlled shutdown | Same — propose ISR shrink |

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
