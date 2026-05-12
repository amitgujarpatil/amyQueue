# Raft with Node Roles: Controller and Worker Architecture

## Overview

In Kafka KRaft (and similar distributed systems), nodes have **different roles** while still participating in the same Raft consensus cluster. This document explains how this works and how to implement it in your Raft system.

---

## Kafka KRaft Architecture

### The Two Node Types

**1. Controller Nodes** (Control Plane)
- Participate in Raft consensus (voting members)
- Store and manage cluster metadata
- Run leader election
- Replicate metadata log via Raft
- Handle cluster coordination

**2. Broker Nodes** (Data Plane)  
- Do NOT participate in Raft consensus (non-voting)
- Serve client requests (produce/consume messages)
- Store actual data (topic partitions)
- Read metadata from controllers
- Report status to controller leader

### Visual Architecture

```
┌─────────────────────────────────────────────────────────┐
│                    CONTROL PLANE                        │
│           (Raft Consensus Cluster)                      │
│                                                          │
│  ┌────────────┐  ┌────────────┐  ┌────────────┐       │
│  │Controller 1│◄─┤Controller 2│─►│Controller 3│       │
│  │  (Leader)  │  │ (Follower) │  │ (Follower) │       │
│  └────────────┘  └────────────┘  └────────────┘       │
│       │                │                 │              │
│       │ Metadata       │ Metadata        │ Metadata     │
│       │ Replication    │ Replication     │ Replication  │
│       │ (Raft)         │ (Raft)          │ (Raft)       │
└───────┼────────────────┼─────────────────┼──────────────┘
        │                │                 │
        │ Metadata       │ Metadata        │ Metadata
        │ Fetch          │ Fetch           │ Fetch
        ▼                ▼                 ▼
┌─────────────────────────────────────────────────────────┐
│                     DATA PLANE                          │
│              (Non-Voting Workers)                       │
│                                                          │
│  ┌─────────┐    ┌─────────┐    ┌─────────┐            │
│  │Broker 1 │    │Broker 2 │    │Broker 3 │            │
│  │ (Worker)│    │ (Worker)│    │ (Worker)│            │
│  └─────────┘    └─────────┘    └─────────┘            │
│       ▲              ▲              ▲                    │
│       │              │              │                    │
│       └──────────────┴──────────────┘                   │
│           Client Requests (Produce/Consume)             │
└─────────────────────────────────────────────────────────┘
```

---

## Roles and Responsibilities

### Controller Nodes (Raft Participants)

#### Responsibilities:
1. **Leader Election**
   - Participate in Raft leader election
   - Vote for candidates
   - Can become leader

2. **Metadata Management**
   - Store cluster metadata (topics, partitions, configs)
   - Replicate metadata changes via Raft log
   - Ensure consistency across all controllers

3. **Coordination** (Leader Only)
   - Accept metadata changes from brokers
   - Assign partitions to brokers
   - Track broker health (liveness)
   - Make administrative decisions

4. **Metadata Distribution**
   - Serve metadata requests from brokers
   - Push metadata updates to brokers

#### What They DON'T Do:
- ❌ Serve client data requests
- ❌ Store topic data/messages
- ❌ Handle produce/consume operations

### Broker Nodes (Non-Voting Workers)

#### Responsibilities:
1. **Client Operations**
   - Accept produce requests from clients
   - Serve consume requests from clients
   - Store actual message data

2. **Metadata Consumption**
   - Fetch metadata from controller leader
   - Stay updated on cluster state
   - Apply configuration changes

3. **Status Reporting**
   - Send heartbeats to controller leader
   - Report partition status
   - Report resource usage

4. **Data Replication** (Broker-to-Broker)
   - Replicate partition data to other brokers
   - ISR (In-Sync Replica) management
   - Note: This is separate from Raft metadata replication!

#### What They DON'T Do:
- ❌ Participate in Raft consensus
- ❌ Vote in leader elections
- ❌ Store/replicate metadata (they consume it)
- ❌ Make cluster-wide decisions

---

## How Controllers Communicate

### Among Themselves (Raft Consensus)

```
Controller Communication (Raft Protocol):

┌──────────────┐                    ┌──────────────┐
│ Controller 1 │  AppendEntries     │ Controller 2 │
│   (Leader)   │───────────────────►│  (Follower)  │
│              │  RequestVote        │              │
│              │◄───────────────────│              │
└──────────────┘                    └──────────────┘
        │                                    │
        │         ┌──────────────┐          │
        │         │ Controller 3 │          │
        └────────►│  (Follower)  │◄─────────┘
                  └──────────────┘

Protocol: Standard Raft (AppendEntries, RequestVote)
Purpose: Metadata replication and consistency
Content: Metadata log entries (topic creation, config changes, etc.)
```

### With Brokers (Metadata Distribution)

```
Controller-Broker Communication:

┌──────────────┐
│ Controller   │
│   (Leader)   │
└──────────────┘
        │
        │ Metadata API
        │ (NOT Raft protocol)
        │
        ├─────────────────┬─────────────────┐
        ▼                 ▼                 ▼
   ┌─────────┐       ┌─────────┐       ┌─────────┐
   │Broker 1 │       │Broker 2 │       │Broker 3 │
   └─────────┘       └─────────┘       └─────────┘

Protocol: Custom metadata RPC (NOT Raft)
Purpose: Distribute metadata to workers
Content: Cluster state, partition assignments, configs
```

---

## Key Design Patterns

### Pattern 1: Separation of Control and Data Planes

**Why separate?**

**Before (All-in-One):**
```
Every node does everything:
- Raft consensus
- Client requests
- Data storage

Problems:
- Heavy client load affects consensus
- Scaling is all-or-nothing
- Resource contention
```

**After (Separated):**
```
Controllers: Lightweight, stable, focus on consensus
Brokers: Heavy workload, easily scaled, isolated failures

Benefits:
- Scale data plane independently
- Stable control plane
- Clear failure domains
```

### Pattern 2: Non-Voting Members (Observers)

Brokers are **observers** or **learners** in Raft terminology:

```
Raft Cluster Membership:

Voting Members (Controllers):
- Participate in elections
- Required for quorum
- Count: Typically 3 or 5 (small, odd number)

Non-Voting Members (Brokers):
- Receive metadata updates
- Do NOT vote
- Do NOT count toward quorum
- Count: Can be 100s or 1000s (scales freely)
```

**Example with 3 Controllers, 10 Brokers:**
```
Quorum needed: 2 out of 3 controllers (majority of voters only)
Brokers are invisible to quorum calculation

If 5 brokers fail: Cluster still healthy (controllers intact)
If 2 controllers fail: Cluster loses quorum (only 1/3 remains)
```

### Pattern 3: Metadata vs Data Replication

**Two separate replication systems:**

```
1. Metadata Replication (Raft, among controllers):
   Entry: "Create topic 'orders' with 10 partitions"
   Replicated: Controller 1 → Controller 2 → Controller 3
   Purpose: Ensure consistent cluster state

2. Data Replication (Broker-to-Broker, NOT Raft):
   Entry: Actual message "order_id=123, amount=$50"
   Replicated: Broker 1 → Broker 2 → Broker 3
   Purpose: Ensure message durability
```

**Visual:**
```
Metadata Log (Raft, on Controllers):
┌──────────────────────────────────────┐
│ idx=1: Create topic "orders"         │
│ idx=2: Add partition to "orders"     │
│ idx=3: Broker 5 joined               │
│ idx=4: Config change: retention=7d   │
└──────────────────────────────────────┘

Data Log (on Brokers, NOT Raft):
┌──────────────────────────────────────┐
│ offset=1: {"order_id": 123, ...}     │
│ offset=2: {"order_id": 124, ...}     │
│ offset=3: {"order_id": 125, ...}     │
└──────────────────────────────────────┘
```

---

## Extending Raft with Node Roles

### Step 1: Define Node Types

```go
type NodeRole int

const (
    RoleController NodeRole = iota  // Voting member
    RoleWorker                       // Non-voting member
)

type Node struct {
    id          string
    role        NodeRole
    state       State        // Leader, Follower, Candidate
    currentTerm int
    votedFor    string
    log         []LogEntry
    
    // Role-specific fields
    isVoter     bool         // Controllers: true, Workers: false
    
    // For workers only
    controllerLeader string  // Which controller to follow
}
```

### Step 2: Modify Leader Election

**Controllers participate, Workers don't:**

```go
func (n *Node) startElection() {
    // Only controllers can become candidates
    if n.role != RoleController {
        return // Workers never start elections
    }
    
    n.state = Candidate
    n.currentTerm++
    n.votedFor = n.id
    
    // Request votes from OTHER CONTROLLERS only
    votesReceived := 1 // Vote for self
    
    for _, peer := range n.peers {
        if peer.role == RoleController {  // Only ask controllers
            response := peer.RequestVote(RequestVoteArgs{
                Term:         n.currentTerm,
                CandidateId:  n.id,
                LastLogIndex: len(n.log),
                LastLogTerm:  n.getLastLogTerm(),
            })
            
            if response.VoteGranted {
                votesReceived++
            }
        }
    }
    
    // Majority of CONTROLLERS only (not all nodes!)
    controllerCount := n.countControllers()
    if votesReceived > controllerCount/2 {
        n.becomeLeader()
    }
}

func (n *Node) countControllers() int {
    count := 0
    for _, peer := range n.peers {
        if peer.role == RoleController {
            count++
        }
    }
    return count + 1 // +1 for self
}
```

### Step 3: Modify RequestVote Handler

```go
func (n *Node) handleRequestVote(req RequestVoteArgs) RequestVoteReply {
    // Workers never vote!
    if n.role != RoleWorker {
        return RequestVoteReply{
            Term:        n.currentTerm,
            VoteGranted: false,
        }
    }
    
    // Standard Raft voting logic for controllers
    if req.Term < n.currentTerm {
        return RequestVoteReply{Term: n.currentTerm, VoteGranted: false}
    }
    
    if req.Term > n.currentTerm {
        n.currentTerm = req.Term
        n.votedFor = ""
        n.state = Follower
    }
    
    // Already voted for someone else?
    if n.votedFor != "" && n.votedFor != req.CandidateId {
        return RequestVoteReply{Term: n.currentTerm, VoteGranted: false}
    }
    
    // Check log up-to-date
    if n.isLogUpToDate(req.LastLogIndex, req.LastLogTerm) {
        n.votedFor = req.CandidateId
        return RequestVoteReply{Term: n.currentTerm, VoteGranted: true}
    }
    
    return RequestVoteReply{Term: n.currentTerm, VoteGranted: false}
}
```

### Step 4: AppendEntries for Both Types

**Controllers:** Replicate among themselves via Raft

**Workers:** Receive metadata updates (one-way)

```go
func (n *Node) sendAppendEntries() {
    if n.state != Leader {
        return
    }
    
    // Send to controllers (full Raft protocol)
    for _, peer := range n.peers {
        if peer.role == RoleController {
            go n.replicateToController(peer)
        }
    }
    
    // Send to workers (simplified metadata push)
    for _, peer := range n.peers {
        if peer.role == RoleWorker {
            go n.sendMetadataToWorker(peer)
        }
    }
}

func (n *Node) replicateToController(peer *Node) {
    // Standard Raft AppendEntries
    prevLogIndex := n.nextIndex[peer.id] - 1
    prevLogTerm := n.log[prevLogIndex].term
    
    entries := n.log[n.nextIndex[peer.id]:]
    
    reply := peer.AppendEntries(AppendEntriesArgs{
        Term:         n.currentTerm,
        LeaderId:     n.id,
        PrevLogIndex: prevLogIndex,
        PrevLogTerm:  prevLogTerm,
        Entries:      entries,
        LeaderCommit: n.commitIndex,
    })
    
    // Handle reply, update nextIndex, etc.
}

func (n *Node) sendMetadataToWorker(peer *Node) {
    // Simplified: Just send committed entries
    // Workers don't send acknowledgments for quorum
    
    peer.ReceiveMetadata(MetadataArgs{
        Term:         n.currentTerm,
        LeaderId:     n.id,
        Entries:      n.getCommittedEntries(),
        CommitIndex:  n.commitIndex,
    })
}
```

### Step 5: Worker Behavior

```go
type WorkerNode struct {
    Node
    metadataCache []LogEntry
    lastMetadataIndex int
}

func (w *WorkerNode) run() {
    // Workers don't participate in elections
    // They just follow the controller leader
    
    for {
        // Periodically fetch metadata from controller leader
        w.fetchMetadata()
        
        // Handle client requests using current metadata
        w.handleClientRequests()
        
        time.Sleep(100 * time.Millisecond)
    }
}

func (w *WorkerNode) fetchMetadata() {
    if w.controllerLeader == "" {
        w.discoverControllerLeader()
        return
    }
    
    // Request metadata from leader
    response := w.requestMetadataUpdate()
    
    if response.Success {
        w.metadataCache = response.Entries
        w.lastMetadataIndex = response.CommitIndex
    } else if response.NotLeader {
        // Controller leader changed, discover new one
        w.controllerLeader = response.NewLeader
    }
}

func (w *WorkerNode) discoverControllerLeader() {
    // Try all controllers to find current leader
    for _, controller := range w.getControllers() {
        response := controller.GetStatus()
        if response.IsLeader {
            w.controllerLeader = controller.id
            return
        }
    }
}
```

---

## Implementation Roadmap

### Phase 1: Basic Raft (Already Done!)
- ✅ Leader election
- ✅ Log replication
- ✅ All nodes are equal (voters)

### Phase 2: Add Node Roles
```
1. Add NodeRole field to Node struct
2. Modify election logic:
   - Only controllers start elections
   - Only controllers vote
   - Quorum based on controller count only
3. Test: 3 controllers + 0 workers (should work like before)
```

### Phase 3: Add Workers (Observers)
```
1. Implement worker node that:
   - Doesn't vote
   - Doesn't start elections
   - Receives metadata from leader
2. Add metadata fetch API
3. Test: 3 controllers + 3 workers
   - Kill controller leader, workers should discover new leader
```

### Phase 4: Separate Control and Data Planes
```
1. Controllers handle:
   - POST /admin/create-topic
   - POST /admin/config-change
   - GET /metadata

2. Workers handle:
   - POST /data/write
   - GET /data/read
   - Fetch metadata from controllers

3. Test: Client writes to worker, worker uses metadata from controller
```

### Phase 5: Worker Registration
```
1. Workers register with controller leader
2. Controller tracks worker health
3. Controller assigns work to workers
4. Test: Add/remove workers dynamically
```

### Phase 6: Failure Scenarios
```
1. Controller leader fails → New election, workers discover new leader
2. Worker fails → Controllers detect, reassign work
3. Network partition → Controllers maintain consistency
4. Test all combinations
```

---

## HTTP API Design

### Controller APIs (Port 8080-8082)

```
# Admin operations (write to Raft log)
POST /admin/create-resource
  Body: {"name": "resource1", "config": {...}}
  Response: {"success": true}

POST /admin/delete-resource
  Body: {"name": "resource1"}
  Response: {"success": true}

# Metadata distribution
GET /metadata
  Response: {
    "term": 5,
    "leader": "controller-1",
    "entries": [...]
  }

# Cluster status
GET /raft/status
  Response: {
    "node_id": "controller-1",
    "role": "controller",
    "state": "leader",
    "term": 5,
    "voters": 3,
    "workers": 10
  }

# Internal Raft RPCs
POST /raft/request-vote
POST /raft/append-entries
```

### Worker APIs (Port 9090-9099)

```
# Client operations (use metadata from controllers)
POST /write
  Body: {"key": "user123", "value": "data"}
  Response: {"success": true}

GET /read/{key}
  Response: {"value": "data"}

# Status
GET /status
  Response: {
    "node_id": "worker-1",
    "role": "worker",
    "controller_leader": "controller-1",
    "metadata_version": 1523,
    "healthy": true
  }

# Internal
GET /internal/metadata-version
  Response: {"version": 1523}
```

---

## Configuration Example

### Controller Config (controller-1.json)
```json
{
  "node_id": "controller-1",
  "role": "controller",
  "http_port": 8080,
  "raft_port": 8081,
  "peers": [
    {"id": "controller-2", "address": "localhost:8083"},
    {"id": "controller-3", "address": "localhost:8085"}
  ],
  "workers": [
    {"id": "worker-1", "address": "localhost:9090"},
    {"id": "worker-2", "address": "localhost:9091"}
  ]
}
```

### Worker Config (worker-1.json)
```json
{
  "node_id": "worker-1",
  "role": "worker",
  "http_port": 9090,
  "controllers": [
    {"id": "controller-1", "address": "localhost:8080"},
    {"id": "controller-2", "address": "localhost:8082"},
    {"id": "controller-3", "address": "localhost:8084"}
  ],
  "metadata_fetch_interval": "1s"
}
```

---

## Testing Scenarios

### Test 1: Basic Election with Roles
```
Setup: 3 controllers, 2 workers
Action: Start all nodes
Expected: 
- One controller becomes leader
- Workers discover leader
- Workers fetch metadata
```

### Test 2: Controller Leader Failure
```
Setup: 3 controllers, 2 workers, controller-1 is leader
Action: Kill controller-1
Expected:
- Controller-2 or controller-3 becomes new leader
- Workers detect old leader is down
- Workers discover new leader
- System continues working
```

### Test 3: Worker Failure
```
Setup: 3 controllers, 3 workers
Action: Kill worker-1
Expected:
- Controllers detect worker-1 is down
- Other workers continue normally
- No leader election triggered
- Cluster remains healthy
```

### Test 4: Metadata Propagation
```
Setup: 3 controllers, 2 workers
Action: POST /admin/create-resource to leader
Expected:
- Entry added to Raft log
- Replicated to controller followers
- Committed when majority ack
- Workers fetch updated metadata
- Workers see new resource
```

### Test 5: Network Partition
```
Setup: 3 controllers, 4 workers
Action: Partition into [C1, C2, W1, W2] and [C3, W3, W4]
Expected:
- Majority partition (C1, C2) continues
- C1 or C2 remains/becomes leader
- W1, W2 continue working
- Minority partition (C3) cannot elect leader
- W3, W4 eventually detect leader is unreachable
```

---

## Key Differences from Basic Raft

| Aspect | Basic Raft | Raft with Roles |
|--------|-----------|-----------------|
| **All nodes vote?** | Yes | Only controllers vote |
| **All nodes in quorum?** | Yes | Only controllers count |
| **All nodes can be leader?** | Yes | Only controllers can lead |
| **All nodes replicate log?** | Yes | Controllers replicate, workers consume |
| **Cluster size affects quorum?** | Yes | Only controller count matters |
| **Scaling** | Hard (quorum grows) | Easy (add workers freely) |

---

## Benefits of This Architecture

### 1. Independent Scaling
```
Need more capacity?
- Add workers (no quorum impact)
- Controllers stay stable

Need more fault tolerance?
- Add controllers (quorum grows, but data plane unaffected)
```

### 2. Stability
```
Heavy client load?
- Affects workers only
- Controllers remain responsive
- Consensus stays healthy
```

### 3. Clear Failure Domains
```
Worker crash: Data plane impact, control plane fine
Controller crash: Control plane impact, data plane continues (with stale metadata)
```

### 4. Resource Optimization
```
Controllers: Small, cheap instances (CPU/memory for consensus)
Workers: Large, powerful instances (storage/network for data)
```

---

## Common Pitfalls

### Pitfall 1: Workers in Quorum Calculation
```
❌ Wrong: quorum = (3 controllers + 10 workers) / 2 + 1 = 7
✅ Right: quorum = 3 controllers / 2 + 1 = 2
```

### Pitfall 2: Workers Voting
```
❌ Wrong: Worker receives RequestVote, sends VoteGranted
✅ Right: Worker receives RequestVote, ignores it or rejects
```

### Pitfall 3: Metadata Staleness
```
❌ Wrong: Worker uses cached metadata forever
✅ Right: Worker periodically fetches updates, has expiry
```

### Pitfall 4: Split Brain
```
❌ Wrong: Worker talks to old leader after partition
✅ Right: Worker detects leader change, switches to new leader
```

---

## Next Steps for Your Implementation

1. **Start with existing Raft** (leader election working)
2. **Add role field** to nodes
3. **Modify election** to exclude workers
4. **Implement metadata fetch** API
5. **Test with mixed cluster** (3 controllers + 2 workers)
6. **Add failure handling** (leader discovery for workers)
7. **Implement actual use case** (e.g., distributed key-value store)

---

## Summary

**Controllers (Voting Members):**
- Run Raft consensus
- Small cluster (3-5 nodes)
- Lightweight, stable
- Store metadata

**Workers (Non-Voting Members):**
- Don't participate in Raft
- Large cluster (unlimited scaling)
- Heavy workload
- Consume metadata from controllers

**Key Insight:** Separate **what needs consensus** (metadata/coordination) from **what doesn't** (data processing). Use Raft only for the control plane, let workers scale freely!

This is how modern distributed systems (Kafka, etcd, Consul) achieve both **strong consistency** (Raft) and **horizontal scalability** (workers).
