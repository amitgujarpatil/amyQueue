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
