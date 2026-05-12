# Partition Leadership and Replication

## Key Concept: Leadership is Per-Partition, Not Per-Topic

**Critical Understanding:**
- ❌ **NOT**: "A topic has a leader"
- ✅ **CORRECT**: "Each partition has a leader"

A topic with 10 partitions has **10 leaders** (distributed across brokers), not 1 leader!

---

## Example Scenario

### Setup: Topic with 3 Partitions, Replication Factor 3

```
Topic: "orders"
Partitions: 3 (partition 0, 1, 2)
Replication Factor: 3 (each partition has 3 copies)
Brokers: 4 (broker-1, broker-2, broker-3, broker-4)
```

### Partition Distribution

```
┌─────────────────────────────────────────────────────────────────┐
│                         TOPIC: orders                           │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  Partition 0:                                                   │
│    Leader: broker-1    Followers: [broker-2, broker-3]         │
│                                                                  │
│  Partition 1:                                                   │
│    Leader: broker-2    Followers: [broker-3, broker-4]         │
│                                                                  │
│  Partition 2:                                                   │
│    Leader: broker-3    Followers: [broker-4, broker-1]         │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

### Visual: Physical Layout on Brokers

```
┌──────────────────┐  ┌──────────────────┐  ┌──────────────────┐  ┌──────────────────┐
│    Broker-1      │  │    Broker-2      │  │    Broker-3      │  │    Broker-4      │
├──────────────────┤  ├──────────────────┤  ├──────────────────┤  ├──────────────────┤
│                  │  │                  │  │                  │  │                  │
│ orders-0 [L]     │  │ orders-0 [F]     │  │ orders-0 [F]     │  │                  │
│   offset: 1000   │  │   offset: 1000   │  │   offset: 1000   │  │                  │
│                  │  │                  │  │                  │  │                  │
│ orders-2 [F]     │  │ orders-1 [L]     │  │ orders-1 [F]     │  │ orders-1 [F]     │
│   offset: 500    │  │   offset: 800    │  │   offset: 800    │  │   offset: 800    │
│                  │  │                  │  │                  │  │                  │
│                  │  │                  │  │ orders-2 [L]     │  │ orders-2 [F]     │
│                  │  │                  │  │   offset: 500    │  │   offset: 500    │
│                  │  │                  │  │                  │  │                  │
└──────────────────┘  └──────────────────┘  └──────────────────┘  └──────────────────┘

[L] = Leader    [F] = Follower
```

**Key Observations:**
1. Each partition exists on 3 brokers (replicas)
2. Only ONE broker is the leader for each partition
3. Leaders are distributed across brokers (load balancing)
4. Broker-1 is the leader for partition 0 but follower for partition 2
5. Broker-4 doesn't have partition 0 at all (replication factor = 3, not 4)

---

## How Writes are Handled

### Rule: ALL Writes Go to the Partition Leader

```
Producer → Determines partition (by key or round-robin)
        → Finds partition leader (from metadata)
        → Sends write request to LEADER ONLY
        → Leader appends to log
        → Leader replicates to followers
        → Leader responds to producer when replicated
```

### Detailed Write Flow

#### Step 1: Producer Determines Partition

```go
// Producer logic
message := {key: "user123", value: "order data"}

// Hash key to determine partition
partition := hash(message.key) % numPartitions
// hash("user123") % 3 = partition 1
```

#### Step 2: Producer Finds Partition Leader

```
Producer has metadata cache:
{
  "orders": {
    "partition_0": {"leader": "broker-1", "replicas": ["broker-1", "broker-2", "broker-3"]},
    "partition_1": {"leader": "broker-2", "replicas": ["broker-2", "broker-3", "broker-4"]},
    "partition_2": {"leader": "broker-3", "replicas": ["broker-3", "broker-4", "broker-1"]}
  }
}

For partition 1, leader is broker-2
Producer sends write request to broker-2
```

#### Step 3: Leader Receives Write

```
┌──────────────────────────────────────────────────────────┐
│                      Broker-2                            │
│                  (Leader for partition 1)                │
├──────────────────────────────────────────────────────────┤
│                                                          │
│  1. Receive produce request                             │
│     - Message: {key: "user123", value: "order data"}    │
│                                                          │
│  2. Validate:                                            │
│     - Is this broker the leader? YES ✓                  │
│     - Is partition healthy? YES ✓                       │
│                                                          │
│  3. Append to local log:                                │
│     - Assign offset: 801                                │
│     - Write to orders-1/segment.log                     │
│     - Flush to disk                                     │
│                                                          │
│  4. Send to followers (async):                          │
│     - Send to broker-3 (follower)                       │
│     - Send to broker-4 (follower)                       │
│                                                          │
└──────────────────────────────────────────────────────────┘
```

#### Step 4: Followers Receive Replication

```
┌────────────────────┐                    ┌────────────────────┐
│     Broker-3       │                    │     Broker-4       │
│  (Follower for     │                    │  (Follower for     │
│   partition 1)     │                    │   partition 1)     │
├────────────────────┤                    ├────────────────────┤
│                    │                    │                    │
│ 1. Receive msg     │                    │ 1. Receive msg     │
│    from broker-2   │                    │    from broker-2   │
│                    │                    │                    │
│ 2. Append to log   │                    │ 2. Append to log   │
│    offset: 801     │                    │    offset: 801     │
│                    │                    │                    │
│ 3. ACK to leader   │                    │ 3. ACK to leader   │
│                    │                    │                    │
└────────────────────┘                    └────────────────────┘
         │                                          │
         │         ACK (replicated)                 │
         └──────────────┬───────────────────────────┘
                        ▼
              ┌────────────────────┐
              │     Broker-2       │
              │     (Leader)       │
              ├────────────────────┤
              │                    │
              │ 4. Receive ACKs    │
              │    from followers  │
              │                    │
              │ 5. Update ISR      │
              │    (In-Sync        │
              │     Replicas)      │
              │                    │
              │ 6. Respond to      │
              │    producer:       │
              │    "Success,       │
              │     offset=801"    │
              │                    │
              └────────────────────┘
```

#### Step 5: Leader Responds to Producer

```
Broker-2 (leader) → Producer

{
  "success": true,
  "partition": 1,
  "offset": 801,
  "timestamp": "2026-05-12T10:00:00Z"
}
```

---

## ISR (In-Sync Replicas)

### What is ISR?

**ISR** = Set of replicas that are "caught up" with the leader

```
Partition 1:
  Leader: broker-2 (offset: 801)
  Replicas: [broker-2, broker-3, broker-4]
  ISR: [broker-2, broker-3, broker-4]  ← All caught up

If broker-4 falls behind:
  Leader: broker-2 (offset: 801)
  Replicas: [broker-2, broker-3, broker-4]
  ISR: [broker-2, broker-3]  ← broker-4 removed from ISR
  
  broker-4 current offset: 750 (51 messages behind!)
```

### Why ISR Matters

**Write Acknowledgment Strategy:**

```
Producer acks configuration:

acks=0 (Fire and forget)
  - Leader doesn't wait for any replication
  - Fastest, but data loss possible
  - Response: Immediate

acks=1 (Leader acknowledgment)
  - Leader appends to log
  - Doesn't wait for followers
  - Fast, some data loss risk
  - Response: After leader writes

acks=all (All ISR acknowledgment)
  - Leader waits for ALL ISR replicas to acknowledge
  - Slowest, but no data loss
  - Response: After all ISR replicas write
```

**Example with acks=all:**

```
Partition 1:
  Leader: broker-2
  ISR: [broker-2, broker-3, broker-4]
  min.insync.replicas: 2

Write flow:
1. Producer sends message to broker-2
2. Broker-2 appends to log
3. Broker-2 sends to broker-3 and broker-4
4. Broker-3 ACKs (1/3 ISR responded)
5. Broker-4 ACKs (2/3 ISR responded)
6. ISR count (2) >= min.insync.replicas (2) ✓
7. Broker-2 responds to producer: SUCCESS

If only broker-3 ACKed and broker-4 was down:
- ISR: [broker-2, broker-3] (size=2)
- 2 >= min.insync.replicas (2) ✓
- Still succeeds!

If ISR becomes [broker-2] only:
- ISR size (1) < min.insync.replicas (2) ✗
- Write FAILS with "NOT_ENOUGH_REPLICAS"
```

---

## What If Producer Writes to a Follower?

### Scenario: Producer Sends Write to Wrong Broker

```
Producer tries to write to broker-3 for partition 1
But broker-3 is a FOLLOWER, not the leader!

┌────────────────────┐
│     Broker-3       │
│  (Follower for     │
│   partition 1)     │
├────────────────────┤
│                    │
│ Receives produce   │
│ request            │
│                    │
│ Checks: Am I the   │
│ leader? NO         │
│                    │
│ Response:          │
│   HTTP 307         │
│   Redirect         │
│   Location:        │
│   broker-2:9092    │
│                    │
└────────────────────┘
```

**HTTP Response:**
```http
HTTP/1.1 307 Temporary Redirect
Location: http://broker-2:9092/api/v1/produce
Content-Type: application/json

{
  "success": false,
  "error": "NOT_LEADER_FOR_PARTITION",
  "leader": "broker-2",
  "leader_address": "http://broker-2:9092"
}
```

**Producer Action:**
1. Receives 307 redirect
2. Updates metadata cache (partition 1 leader = broker-2)
3. Retries request to broker-2 (the leader)
4. Future requests go directly to broker-2

---

## Reads: Can Read from Followers?

### Current Implementation: Leader-Only Reads

```
Traditional Kafka (and our design):
  - All reads go to partition leader
  - Ensures strong consistency
  - Followers only used for replication

┌─────────────┐
│  Consumer   │
└──────┬──────┘
       │ Fetch request (topic=orders, partition=1, offset=800)
       ▼
┌────────────────────┐
│     Broker-2       │ ← Leader for partition 1
│     (Leader)       │
├────────────────────┤
│ Returns messages   │
│ offset 800-899     │
└────────────────────┘

Broker-3 and Broker-4 (followers) are NOT used for reads
```

### Future Enhancement: Follower Reads (Optional)

```
Newer Kafka allows reading from followers for:
  - Reduced leader load
  - Lower latency (read from nearby replica)
  - Better geographic distribution

Trade-off: Eventual consistency (follower might be slightly behind)

┌─────────────┐
│  Consumer   │
└──────┬──────┘
       │ Fetch request (topic=orders, partition=1, offset=800)
       │ Preference: "any replica"
       ▼
┌────────────────────┐
│     Broker-3       │ ← Follower for partition 1
│    (Follower)      │
├────────────────────┤
│ Returns messages   │
│ offset 800-895     │
│ (might be 4 msgs   │
│  behind leader)    │
└────────────────────┘
```

**For now, keep it simple: Leader-only reads**

---

## Leader Election (Partition Level)

### When Does Leader Election Happen?

1. **Broker Failure**: Current leader crashes
2. **Network Partition**: Leader isolated from cluster
3. **Controlled Shutdown**: Leader being decommissioned
4. **Partition Reassignment**: Manual rebalancing

### Leader Election Process

```
Initial State:
  Partition 1:
    Leader: broker-2
    ISR: [broker-2, broker-3, broker-4]

Event: broker-2 crashes!

Step 1: Controller detects failure
  - broker-2 stops sending heartbeats
  - Controller marks broker-2 as DEAD

Step 2: Controller triggers leader election
  - For each partition where broker-2 was leader
  - Select new leader from ISR

Step 3: Select new leader
  - ISR: [broker-2, broker-3, broker-4]
  - broker-2 is dead, remove from ISR
  - Remaining ISR: [broker-3, broker-4]
  - Pick first from ISR: broker-3

Step 4: Update metadata
  Partition 1:
    Leader: broker-3 (changed!)
    ISR: [broker-3, broker-4]
    Offline: [broker-2]

Step 5: Notify brokers
  - broker-3 promoted to leader
  - broker-4 now replicates from broker-3
  - Producers/consumers update metadata

New State:
  Partition 1:
    Leader: broker-3 ✓
    ISR: [broker-3, broker-4]
```

**Visual:**

```
BEFORE (broker-2 is leader):
┌──────────┐   writes    ┌──────────┐   replicates   ┌──────────┐
│ Producer │────────────►│ Broker-2 │───────────────►│ Broker-3 │
└──────────┘             │ [Leader] │        │       │[Follower]│
                         └──────────┘        │       └──────────┘
                                             │
                                             │       ┌──────────┐
                                             └──────►│ Broker-4 │
                                                     │[Follower]│
                                                     └──────────┘

broker-2 CRASHES! ⚡

AFTER (broker-3 is new leader):
┌──────────┐   writes    ┌──────────┐   replicates   ┌──────────┐
│ Producer │────────────►│ Broker-3 │───────────────►│ Broker-4 │
└──────────┘             │ [Leader] │                │[Follower]│
                         └──────────┘                └──────────┘

                         ┌──────────┐
                         │ Broker-2 │
                         │  [DEAD]  │
                         └──────────┘
```

---

## Implementation: Checking Leadership

### In Worker Node

```go
type PartitionInfo struct {
    Topic       string
    PartitionID int32
    Leader      string   // broker-id of leader
    Replicas    []string // all replicas
    ISR         []string // in-sync replicas
}

type Worker struct {
    brokerID         string
    metadataCache    *MetadataCache
    partitionManager *PartitionManager
}

func (w *Worker) handleProduceRequest(req *ProduceRequest) (*ProduceResponse, error) {
    // Get partition info from metadata
    partitionInfo := w.metadataCache.GetPartition(req.Topic, req.Partition)
    
    // Check if this broker is the leader
    if partitionInfo.Leader != w.brokerID {
        return &ProduceResponse{
            Success:       false,
            Error:         "NOT_LEADER_FOR_PARTITION",
            Leader:        partitionInfo.Leader,
            LeaderAddress: w.getBrokerAddress(partitionInfo.Leader),
        }, nil
    }
    
    // This broker is the leader, proceed with write
    partitionLog := w.partitionManager.GetPartition(req.Topic, req.Partition)
    
    // Append to log
    offsets, err := partitionLog.Append(req.Messages)
    if err != nil {
        return nil, err
    }
    
    // Replicate to ISR followers
    err = w.replicateToFollowers(req.Topic, req.Partition, req.Messages, partitionInfo.ISR)
    if err != nil {
        return nil, err
    }
    
    return &ProduceResponse{
        Success:   true,
        Partition: req.Partition,
        Offsets:   offsets,
    }, nil
}

func (w *Worker) replicateToFollowers(topic string, partition int32, messages []*Message, isr []string) error {
    // Send to each follower in ISR (except self)
    acks := 0
    required := len(isr) - 1 // All ISR except leader (self)
    
    for _, followerID := range isr {
        if followerID == w.brokerID {
            continue // Skip self
        }
        
        followerAddr := w.getBrokerAddress(followerID)
        
        // Send replication request
        err := w.sendReplicationRequest(followerAddr, topic, partition, messages)
        if err == nil {
            acks++
        }
    }
    
    // Check if we have enough acks
    if acks < required {
        return fmt.Errorf("insufficient replicas: got %d, need %d", acks, required)
    }
    
    return nil
}

func (w *Worker) handleReplicationRequest(req *ReplicationRequest) error {
    // This broker is a follower, receiving data from leader
    
    partitionLog := w.partitionManager.GetPartition(req.Topic, req.Partition)
    
    // Append to log (as follower)
    _, err := partitionLog.Append(req.Messages)
    if err != nil {
        return err
    }
    
    return nil
}
```

---

## Metadata Cache Updates

### How Workers Stay Updated

```go
type MetadataCache struct {
    mu              sync.RWMutex
    topics          map[string]*TopicMetadata
    brokers         map[string]*BrokerInfo
    version         int64
    controllerLeader string
}

func (mc *MetadataCache) RefreshPeriodically() {
    ticker := time.NewTicker(5 * time.Second)
    
    for range ticker.C {
        // Fetch latest metadata from controller
        metadata, err := mc.fetchFromController()
        if err != nil {
            log.Printf("Error fetching metadata: %v", err)
            continue
        }
        
        // Check if metadata changed
        if metadata.Version > mc.version {
            mc.mu.Lock()
            mc.topics = metadata.Topics
            mc.brokers = metadata.Brokers
            mc.version = metadata.Version
            mc.mu.Unlock()
            
            log.Printf("Metadata updated to version %d", metadata.Version)
        }
    }
}

func (mc *MetadataCache) GetPartitionLeader(topic string, partition int32) string {
    mc.mu.RLock()
    defer mc.mu.RUnlock()
    
    topicMeta, exists := mc.topics[topic]
    if !exists {
        return ""
    }
    
    if partition >= int32(len(topicMeta.Partitions)) {
        return ""
    }
    
    return topicMeta.Partitions[partition].Leader
}
```

---

## Summary: The Complete Picture

### Key Points

1. **Leadership is per-partition**, not per-topic
2. **All writes go to the partition leader**
3. **Leader replicates to followers** (in ISR)
4. **Followers only replicate, don't serve writes**
5. **Reads also go to leader** (for strong consistency)
6. **If producer sends to wrong broker**, gets 307 redirect
7. **Leader election happens** when current leader fails
8. **ISR tracks which replicas are caught up**
9. **min.insync.replicas** controls durability vs availability

### Write Path Summary

```
1. Producer determines partition (by key hash or round-robin)
2. Producer looks up partition leader from metadata
3. Producer sends write to leader broker
4. Leader validates (am I really the leader?)
5. Leader appends to local log
6. Leader replicates to ISR followers in parallel
7. Leader waits for ISR acknowledgments (based on acks config)
8. Leader responds to producer with offset
9. Followers continue replicating asynchronously
```

### Failure Scenarios

| Scenario | What Happens |
|----------|--------------|
| Leader crashes | Controller elects new leader from ISR |
| Follower crashes | Removed from ISR, leader continues |
| Network partition | Minority partition can't accept writes |
| All ISR down | Partition offline until replica recovers |
| Leader slow | Followers might fall behind, removed from ISR |

### Configuration Trade-offs

| Setting | Durability | Availability | Latency |
|---------|-----------|--------------|---------|
| `acks=0` | ⚠️ Low | ✅ High | ✅ Fast |
| `acks=1` | ⚠️ Medium | ✅ High | ✅ Fast |
| `acks=all` | ✅ High | ⚠️ Medium | ⚠️ Slow |
| `min.insync.replicas=1` | ⚠️ Low | ✅ High | ✅ Fast |
| `min.insync.replicas=2` | ✅ High | ⚠️ Medium | ⚠️ Slow |

---

## Next Steps for Implementation

1. ✅ Understand partition leadership model
2. ✅ Implement leader check in produce handler
3. ⏭️ Implement replication to followers
4. ⏭️ Implement ISR tracking
5. ⏭️ Implement leader election in controller
6. ⏭️ Add metadata refresh in workers
7. ⏭️ Add acks configuration support

This is the heart of Kafka's distributed architecture - partition-level leadership with replication!
