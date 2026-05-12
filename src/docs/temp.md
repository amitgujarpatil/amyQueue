# Raft Consensus Algorithm - Complete Learning Guide

## What is Raft?

Raft is a **consensus algorithm** designed to be easy to understand. It allows a cluster of machines to work together as a coherent group that can survive failures of some members. Raft is used in distributed systems like etcd, Consul, and CockroachDB.

**Key Goal**: Keep a replicated log consistent across multiple servers, even when some fail.

---

## Why Raft Matters

- **Distributed consensus** is hard - nodes must agree on state despite network failures, delays, and crashes
- Raft solves this in an understandable way (unlike Paxos)
- Essential for building reliable distributed systems (databases, key-value stores, configuration management)

---

## Core Concepts You Must Understand First

### 1. **Replicated State Machine**
- Multiple servers maintain identical copies of state
- All servers execute the same commands in the same order
- If they start with the same state, they stay synchronized
- The **log** is the ordered sequence of commands

### 2. **Strong Leadership**
Raft uses a strong leader approach:
- Only ONE leader at a time
- Leader accepts client requests
- Leader replicates log entries to followers
- Simpler than symmetric protocols (like Paxos)

### 3. **Server States**
Every server is always in one of three states:

```
┌─────────┐  times out, starts election  ┌───────────┐
│ Follower│────────────────────────────>│ Candidate │
└─────────┘                               └───────────┘
     ^                                          │
     │                                          │ receives votes
     │                                          │ from majority
     │         ┌────────┐                       │
     └─────────│ Leader │<──────────────────────┘
               └────────┘
               discovers server with
               higher term
```

- **Follower**: Passive, responds to requests from leader/candidates
- **Candidate**: Tries to become leader during elections
- **Leader**: Handles all client requests, replicates log

---

## Raft's Key Components

### 1. **Leader Election**

**When does it happen?**
- When cluster starts (no leader)
- When leader fails (followers stop receiving heartbeats)

**Process:**
1. Follower times out waiting for heartbeat (150-300ms typical)
2. Becomes **Candidate**, increments term, votes for itself
3. Sends `RequestVote` RPCs to all other servers
4. Receives votes if candidate's log is at least as up-to-date
5. Wins election if it gets majority of votes
6. Becomes Leader and sends heartbeats to establish authority

**Key Rules:**
- Each server votes for at most ONE candidate per term
- Randomized election timeouts prevent split votes (150-300ms range)
- Candidate with most up-to-date log wins

### 2. **Log Replication**

**How it works:**
1. Client sends command to leader
2. Leader appends entry to its local log
3. Leader sends `AppendEntries` RPC to all followers in parallel
4. Leader waits for majority to replicate the entry
5. Leader applies entry to state machine (commits it)
6. Leader returns result to client
7. Leader notifies followers about committed entries

**Log Structure:**
```
Index:  1    2    3    4    5
Term:   1    1    1    2    3
Cmd:   [x=1][y=2][x=3][y=4][x=5]
```

Each log entry contains:
- **Command** for state machine
- **Term** when entry was created by leader
- **Index** position in log

**Key Property: Log Matching**
- If two entries in different logs have same index and term, they contain the same command
- If two entries in different logs have same index and term, all preceding entries are identical

### 3. **Safety Rules**

**Election Safety**: At most one leader per term

**Leader Append-Only**: Leader never overwrites or deletes entries

**Log Matching**: If two logs contain an entry with same index and term, all previous entries are identical

**Leader Completeness**: If an entry is committed in a term, it will be present in all future leaders

**State Machine Safety**: If a server has applied a log entry at a given index, no other server will apply a different entry for that index

---

## The Algorithm Step-by-Step

### Phase 1: Leader Election

```
1. All servers start as Followers
2. If follower doesn't hear from leader within election timeout:
   a. Increment currentTerm
   b. Transition to Candidate
   c. Vote for self
   d. Send RequestVote RPC to all other servers
3. If candidate receives votes from majority: become Leader
4. If candidate receives AppendEntries from valid leader: become Follower
5. If election timeout elapses: start new election
```

### Phase 2: Log Replication (Normal Operation)

```
1. Leader receives command from client
2. Leader appends entry to its log
3. Leader sends AppendEntries RPC to each follower (in parallel)
4. Follower checks:
   - Is prevLogIndex/prevLogTerm matching?
   - If not, return failure (leader will retry with earlier entries)
   - If yes, append entries and return success
5. When majority of followers acknowledge:
   - Leader marks entry as committed
   - Leader applies entry to state machine
   - Leader returns result to client
6. In subsequent AppendEntries, leader includes commitIndex
7. Followers apply committed entries to their state machines
```

### Phase 3: Handling Failures

**Leader Failure:**
- Followers detect missing heartbeats
- New election begins
- New leader elected
- New leader has all committed entries (election restriction)

**Follower Failure:**
- Leader keeps retrying AppendEntries indefinitely
- When follower recovers, it catches up

**Network Partition:**
- Minority partition cannot commit (no majority)
- Majority partition continues operating
- When partition heals, minority's uncommitted entries are overwritten

---

## Essential Data Structures

### Persistent State (on all servers)
Must be saved to disk before responding to RPCs:
```
currentTerm   - latest term server has seen
votedFor      - candidateId that received vote in current term
log[]         - log entries; each entry contains command and term
```

### Volatile State (on all servers)
```
commitIndex   - index of highest log entry known to be committed
lastApplied   - index of highest log entry applied to state machine
```

### Volatile State (on leaders only)
Reinitialized after election:
```
nextIndex[]   - for each server, index of next log entry to send
matchIndex[]  - for each server, index of highest log entry known to be replicated
```

---

## The Two RPC Calls

### 1. RequestVote RPC
Invoked by candidates to gather votes.

**Arguments:**
- `term` - candidate's term
- `candidateId` - candidate requesting vote
- `lastLogIndex` - index of candidate's last log entry
- `lastLogTerm` - term of candidate's last log entry

**Results:**
- `term` - currentTerm, for candidate to update itself
- `voteGranted` - true means candidate received vote

### 2. AppendEntries RPC
Invoked by leader to replicate log entries; also used as heartbeat.

**Arguments:**
- `term` - leader's term
- `leaderId` - so follower can redirect clients
- `prevLogIndex` - index of log entry immediately preceding new ones
- `prevLogTerm` - term of prevLogIndex entry
- `entries[]` - log entries to store (empty for heartbeat)
- `leaderCommit` - leader's commitIndex

**Results:**
- `term` - currentTerm, for leader to update itself
- `success` - true if follower contained entry matching prevLogIndex and prevLogTerm

---

## Prerequisites for Implementation

### 1. **Distributed Systems Concepts**
- [ ] Network communication (RPCs, HTTP requests)
- [ ] Handling timeouts and retries
- [ ] Concurrent programming (goroutines/threads)
- [ ] Clock skew and time-based decisions

### 2. **HTTP Server Knowledge**
- [ ] Request/response handling
- [ ] JSON serialization
- [ ] REST API design
- [ ] HTTP status codes
- [ ] Handling concurrent requests

### 3. **State Management**
- [ ] Persistent storage (files or embedded DB)
- [ ] In-memory data structures
- [ ] Thread-safe access to shared state
- [ ] State machine pattern

### 4. **Testing Strategies**
- [ ] Network failure simulation
- [ ] Partition testing
- [ ] Crash recovery testing
- [ ] Consistency verification

---

## Building Raft with HTTP Server - Step by Step

### Step 1: Basic HTTP Server (Single Node)
```
Goal: Create HTTP server that can store and retrieve key-value pairs
- POST /set - set a key-value pair
- GET /get/{key} - retrieve a value
- In-memory storage only
```

### Step 2: Add Multiple Servers (No Consensus Yet)
```
Goal: Run 3 separate server instances
- Each server runs on different port
- Each server maintains its own state
- No communication between servers yet
- Observe: updates to one server don't affect others (inconsistency!)
```

### Step 3: Implement Leader Election
```
Goal: Servers can elect a leader
- Add RequestVote RPC endpoint
- Implement election timeout timer
- Add candidate state and voting logic
- Test: Kill leader, observe new election
```

### Step 4: Implement Heartbeat
```
Goal: Leader maintains authority
- Add AppendEntries RPC endpoint (empty entries)
- Leader sends periodic heartbeats
- Followers reset election timer on heartbeat
- Test: Leader stays stable when heartbeating
```

### Step 5: Implement Log Replication
```
Goal: Leader replicates commands to followers
- Client requests go to leader only
- Leader appends to log and replicates
- Implement log matching logic
- Test: All servers have same log after replication
```

### Step 6: Implement Commit and Apply
```
Goal: Apply committed entries to state machine
- Track commitIndex and lastApplied
- Apply committed entries in order
- Return results to clients only after commit
- Test: State machines stay consistent
```

### Step 7: Add Persistence
```
Goal: Servers survive crashes
- Save currentTerm, votedFor, log to disk
- Restore state on startup
- Test: Crash and restart servers
```

### Step 8: Handle Network Failures
```
Goal: System continues operating despite failures
- Implement retry logic
- Handle follower lag (nextIndex adjustment)
- Test: Network partitions, delayed messages
```

---

## HTTP API Design for Raft

### Client API (External)
```
POST /write
  Body: {"command": "set x=5"}
  Response: {"success": true, "result": "OK"}

GET /read/{key}
  Response: {"value": "5"}
```

### Raft Internal API (Between Servers)
```
POST /raft/request-vote
  Body: {term, candidateId, lastLogIndex, lastLogTerm}
  Response: {term, voteGranted}

POST /raft/append-entries
  Body: {term, leaderId, prevLogIndex, prevLogTerm, entries, leaderCommit}
  Response: {term, success}

GET /raft/status
  Response: {state, term, leader, logLength, commitIndex}
```

---

## Common Pitfalls to Avoid

1. **Forgetting to persist state** - Always save term, votedFor, log to disk
2. **Not randomizing election timeouts** - Leads to split votes
3. **Applying uncommitted entries** - Only apply after majority replication
4. **Incorrect log matching** - Must check both prevLogIndex AND prevLogTerm
5. **Ignoring term numbers** - Always update to higher term, step down if outdated
6. **Blocking on RPCs** - Use timeouts and handle failures gracefully
7. **Not handling leader redirect** - Followers should tell clients who the leader is

---

## Recommended Learning Path

1. **Read the Paper**: "In Search of an Understandable Consensus Algorithm" by Ongaro & Ousterhout
2. **Watch Visualization**: Check out "Raft Consensus Algorithm" visualization at thesecretlivesofdata.com/raft
3. **Implement Basic HTTP Server**: Get comfortable with HTTP and state management
4. **Add One Raft Feature at a Time**: Don't try to build everything at once
5. **Test Thoroughly**: Each component before moving to next
6. **Study Existing Implementations**: Look at etcd/raft or hashicorp/raft for reference

---

## Testing Checklist

- [ ] Leader election completes successfully
- [ ] New election happens when leader fails
- [ ] Split vote triggers new election with randomized timeouts
- [ ] Log entries replicate to all followers
- [ ] Committed entries never lost
- [ ] Follower with stale log catches up
- [ ] Network partition doesn't cause inconsistency
- [ ] Servers recover after crash (persistence works)
- [ ] Clients redirected from followers to leader
- [ ] High throughput under normal operation

---

## Resources

- **Original Paper**: https://raft.github.io/raft.pdf
- **Interactive Visualization**: https://thesecretlivesofdata.com/raft
- **Raft Website**: https://raft.github.io
- **Reference Implementations**:
  - Go: github.com/hashicorp/raft
  - Go: github.com/etcd-io/raft
  - Rust: github.com/tikv/raft-rs

---

## Next Steps

1. Start with the interactive visualization to build intuition
2. Read the original paper (focus on Section 5)
3. Implement a basic HTTP key-value server (no Raft yet)
4. Add Raft incrementally, one feature at a time
5. Test each feature thoroughly before moving on
6. Celebrate when your first 3-node cluster survives a leader failure! 🎉

Good luck building your Raft implementation!


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


1/ raft is concensues algorithm
2/ we need to raft to keep same information in all the availabel nodes.

3/ leadership is raft.

- only leader accepts the request and process it.
- leader is reponsible for replecating data to followers.
- at any point in time only one will be a leader

4/ Server states 

- 1 Leader 
- many followers 
- many candidates

majority wins become leader.

Note: we always start a cluster with odd number of nodes.
where each node is an indpendent server.

5/ Voting process triggers.

when leader dies voting process start.

- how do we know leader is die, by missing the heart beat from 
  leader.

[

 quorum - minimum number of peoples before taking the offical desion.

]

desions regarding number of nodes in cluster.

1/ static_mode start cluster with static number (ideally odd numbers)
   - every node have information about the peer nodes, 
     how they or if they are running on same host then on which port 
     they needs to talk.

2/ dyanmic_mode 
   - nodes can join cluster without restarting the cluster.
   - when node is up and running with the information about 
     all peer nodes.
   - rest of nodes also needs to update their peer nodes 
     list with this new node. (still need to figure out how ?)

3/ auto_mode 
   - nodes can join cluster automatically and all peers nodes 
     also update their list auto.     
   - and new node can also participate in voting.

Two types of nodes 




everthing is going to be a perfect.
i dont keep a track of what he knows or not.
now its upon you.
no its ok, i got it.
take a moment.


kraft for election prcess.
what we called it is concensues algorithm.

3 controller nodes. -<> manage  
3 workers nodes 






