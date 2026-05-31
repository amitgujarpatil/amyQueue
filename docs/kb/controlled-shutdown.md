# Controlled Shutdown (Graceful Broker Exit)

## The Problem — The 30 Second Window

Without controlled shutdown, every broker restart causes unavailability:

```
CRASH (no controlled shutdown):
  t=0    Broker dies
  t=30s  Controller detects death via heartbeat timeout
  t=30s  Leader election triggered for all partitions this broker led
  = 30 seconds of unavailability per broker

10-broker rolling restart (upgrade, config change):
  10 × 30s = 5 minutes of rolling disruption
```

The 30-second window is the heartbeat timeout — how long the controller
waits before declaring a broker dead and acting on it.

---

## The Fix — Controlled Shutdown

The broker tells the controller it is about to exit. The controller
migrates leadership before the broker stops:

```
CONTROLLED SHUTDOWN:
  t=0    SIGTERM received by broker
  t=~1s  Controller migrates all leaders away from this broker
  t=~1s  Broker drains in-flight requests, flushes disk, exits
  = ~1 second of metadata refresh, no actual unavailability

10-broker rolling restart:
  10 × 1s = 10 seconds
```

---

## Shutdown Sequence

```
BROKER                              CONTROLLER LEADER
  |                                      |
  | OS sends SIGTERM / SIGINT            |
  | stop accepting new producer connects |
  |                                      |
  |-- POST /brokers/{id}/shutdown ------>|
  |   { epoch, clusterID, token }        |
  |                                   Propose ShutdownBroker → Raft
  |                                   applyShutdownBroker:
  |                                     BrokerStatus = ShuttingDown
  |                                   For each leader partition:
  |                                     ElectLeader (exclude this broker)
  |                                     Propose UpdatePartition → Raft
  |                                   For each ISR follower partition:
  |                                     Propose ISR shrink → Raft
  |                                   Wait for all to commit
  |<-- 200 { migratedPartitions: 12,    |
  |          failedPartitions: [] } -----|
  |                                      |
  | drain in-flight requests             |
  | flush active log segments to disk    |
  | exit 0                               |
```

---

## BrokerStatus in the Raft Log

`ShuttingDown` status is durable — written to the Raft log so all
controllers know this broker is exiting. The assignment algorithm
(`ListActiveBrokers`) excludes `ShuttingDown` brokers immediately —
no new partitions are assigned to it.

```go
type BrokerStatus string

const (
    BrokerStatusActive       BrokerStatus = "active"
    BrokerStatusShuttingDown BrokerStatus = "shutting_down"
)
```

`Dead` is NOT a `BrokerStatus` value. Death is ephemeral — detected by
`LivenessTracker` from heartbeat timeout. It is never written to Raft.

---

## Failure Cases

### No Other ISR Member for a Partition

```
ISR=[B3], B3 is shutting down
ElectLeader finds no other candidate
Partition goes Offline after B3 exits
failedPartitions returned to broker in shutdown response
Broker logs WARN and exits anyway — operator must investigate
```

### Controller Unreachable

```
POST /shutdown times out
Broker retries N times with backoff (shutdown_max_retries=3)
After max retries: dirty shutdown
LivenessTracker detects death via heartbeat timeout (~30s)
```

### Shutdown Timeout Exceeded

```
shutdown_timeout_ms exceeded before all partitions migrated
Broker aborts, dirty shutdown for remaining partitions
```

---

## Signal Handling

| Signal | Behaviour |
|---|---|
| SIGTERM | Controlled shutdown — migrate, drain, flush, exit |
| SIGINT | Same as SIGTERM |
| SIGKILL | Cannot catch — dirty shutdown, 30s recovery window |

---

## Configuration

```yaml
shutdown_timeout_ms:       30000   # max wait for controller to migrate all leaders
shutdown_drain_timeout_ms:  5000   # max wait for in-flight requests
shutdown_max_retries:           3   # retries if controller unreachable
shutdown_retry_backoff_ms:   5000
```

---

## How Kafka Does It

Kafka calls this Controlled Shutdown. The broker sends a
`ControlledShutdownRequest` to the controller. The controller elects new
leaders for all partitions the broker leads (excluding the shutting-down
broker), removes it from ISR of follower partitions, and responds once
all Raft entries commit. The broker then drains, flushes, and exits.

AmyQueue follows the same pattern. Difference: the shutdown request is
an HTTP POST on the admin server rather than a dedicated Kafka protocol RPC.
