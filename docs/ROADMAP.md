# AmyQueue Implementation Roadmap

## Overview

This document outlines the step-by-step implementation plan for AmyQueue, a distributed message queue system built from scratch.

---

## Phase 1: Foundation ✅ (Completed)

**Goal**: Project setup and comprehensive design documentation

### Tasks
- [x] Project initialization
- [x] Architecture design
- [x] Design documentation
  - [x] Raft consensus algorithm
  - [x] Controller/Broker architecture
  - [x] Metadata management
  - [x] Partition storage
  - [x] Consumer groups
  - [x] Replication strategy
  - [x] Protocol comparison (TCP vs gRPC)
- [x] Repository structure
- [x] Documentation (README, ROADMAP)

**Duration**: 2 weeks  
**Status**: ✅ Complete

---

## Phase 2: Core Infrastructure (Weeks 3-4)

**Goal**: Build foundational components needed by all services

### 2.1 Configuration Management
- [ ] Environment variable loading (.env support)
- [ ] Configuration validation
- [ ] Configuration hot-reload support
- [ ] Node-specific config (controller vs broker)

**Files**: `src/internal/config/`

### 2.2 Logging and Observability
- [ ] Structured logging (JSON format)
- [ ] Log levels (debug, info, warn, error)
- [ ] Contextual logging
- [ ] Basic metrics collection

**Files**: `src/pkg/logger/`, `src/pkg/metrics/`

### 2.3 Storage Layer
- [ ] File-based storage abstraction
- [ ] JSON storage implementation
- [ ] Segment file management
- [ ] Directory structure handling

**Files**: `src/internal/storage/`

**Deliverables**:
- Configuration system working
- Logging to console and file
- Basic storage read/write operations

---

## Phase 3: Raft Implementation (Weeks 5-8)

**Goal**: Implement Raft consensus algorithm for controller cluster

### 3.1 Basic Raft Node
- [ ] Node state management (Follower, Candidate, Leader)
- [ ] Term management
- [ ] Persistent state (currentTerm, votedFor, log)
- [ ] Election timeout handling

**Files**: `src/internal/raft/node.go`, `src/internal/raft/state.go`

### 3.2 Leader Election
- [ ] RequestVote RPC
- [ ] Vote granting logic
- [ ] Election timeout with randomization
- [ ] Term updates
- [ ] Become leader/follower logic

**Files**: `src/internal/raft/election.go`

### 3.3 Log Replication
- [ ] AppendEntries RPC
- [ ] Log entry structure
- [ ] Log append logic
- [ ] Consistency checks (prevLogIndex, prevLogTerm)
- [ ] Commit index management

**Files**: `src/internal/raft/replication.go`, `src/internal/raft/log.go`

### 3.4 Safety and Persistence
- [ ] Log persistence to disk
- [ ] State recovery on restart
- [ ] Snapshot creation
- [ ] Snapshot installation
- [ ] Log compaction

**Files**: `src/internal/raft/snapshot.go`, `src/internal/raft/persistence.go`

### 3.5 Testing
- [ ] Unit tests for each component
- [ ] Leader election tests
- [ ] Log replication tests
- [ ] Network partition simulation
- [ ] Crash recovery tests

**Files**: `src/internal/raft/*_test.go`

**Deliverables**:
- 3-node Raft cluster running
- Leader election working
- Log replication functional
- Survives node failures

---

## Phase 4: Metadata Management (Weeks 9-10)

**Goal**: Implement metadata state machine and management

### 4.1 State Machine
- [ ] Metadata state machine interface
- [ ] Topic operations (create, delete, update)
- [ ] Broker operations (register, deregister, heartbeat)
- [ ] Partition operations (leader change, ISR update)
- [ ] Apply log entries to state

**Files**: `src/internal/metadata/state_machine.go`

### 4.2 Metadata Structures
- [ ] Topic metadata
- [ ] Partition metadata
- [ ] Broker metadata
- [ ] Consumer group metadata
- [ ] In-memory indexes

**Files**: `src/internal/metadata/types.go`

### 4.3 Partition Assignment
- [ ] Round-robin assignment algorithm
- [ ] Replica placement strategy
- [ ] Leader election (partition level)
- [ ] ISR management

**Files**: `src/internal/metadata/assignment.go`

**Deliverables**:
- Metadata state machine integrated with Raft
- Topics can be created/deleted
- Brokers can register/heartbeat
- Partition assignment working

---

## Phase 5: gRPC Services (Weeks 11-12)

**Goal**: Implement gRPC communication layer

### 5.1 Protocol Buffers
- [ ] Define proto files
  - [ ] Metadata service (controller)
  - [ ] Broker service (data plane)
  - [ ] Replication service (broker-to-broker)
  - [ ] Raft service (controller-to-controller)
- [ ] Generate Go code
- [ ] Setup proto build automation

**Files**: `proto/*.proto`, `Makefile`

### 5.2 Metadata Service (Controller)
- [ ] CreateTopic RPC
- [ ] DeleteTopic RPC
- [ ] ListTopics RPC
- [ ] DescribeTopic RPC
- [ ] RegisterBroker RPC
- [ ] Heartbeat RPC
- [ ] GetMetadata RPC

**Files**: `src/internal/grpc/metadata_server.go`

### 5.3 Raft Service (Controller)
- [ ] RequestVote RPC
- [ ] AppendEntries RPC
- [ ] InstallSnapshot RPC

**Files**: `src/internal/grpc/raft_server.go`

### 5.4 gRPC Server Setup
- [ ] Server initialization
- [ ] Connection management
- [ ] Error handling
- [ ] Interceptors (logging, metrics)

**Files**: `src/internal/grpc/server.go`

**Deliverables**:
- gRPC services defined and generated
- Controller gRPC server running
- Can create topics via gRPC

---

## Phase 6: HTTP API (Week 13)

**Goal**: Implement HTTP/REST API for human-friendly management

### 6.1 HTTP Server
- [ ] Router setup (gorilla/mux)
- [ ] Middleware (logging, CORS, leader redirect)
- [ ] Error response formatting
- [ ] Health check endpoint

**Files**: `src/internal/http/server.go`, `src/internal/http/middleware.go`

### 6.2 Topic Endpoints
- [ ] POST /api/v1/topics (create)
- [ ] GET /api/v1/topics (list)
- [ ] GET /api/v1/topics/{name} (describe)
- [ ] PUT /api/v1/topics/{name} (update)
- [ ] DELETE /api/v1/topics/{name} (delete)

**Files**: `src/internal/http/topic_handlers.go`

### 6.3 Cluster Endpoints
- [ ] GET /api/v1/cluster/info
- [ ] GET /api/v1/cluster/health
- [ ] GET /api/v1/raft/status
- [ ] GET /api/v1/brokers

**Files**: `src/internal/http/cluster_handlers.go`

**Deliverables**:
- HTTP API running on port 8080
- Can manage topics via curl
- Swagger/OpenAPI documentation

---

## Phase 7: Controller Application (Week 14)

**Goal**: Complete controller node application

### 7.1 Controller Main
- [ ] Command-line flags
- [ ] Configuration loading
- [ ] Component initialization
- [ ] Graceful shutdown
- [ ] Signal handling

**Files**: `src/cmd/controller/main.go`

### 7.2 Integration
- [ ] Wire Raft + Metadata + gRPC + HTTP
- [ ] Metadata cache for brokers
- [ ] Periodic cleanup tasks
- [ ] Health monitoring

**Files**: `src/cmd/controller/server.go`

**Deliverables**:
- Standalone controller binary
- Can run 3-controller cluster
- Topics managed across cluster

---

## Phase 8: Partition Storage (Weeks 15-16)

**Goal**: Implement partition log storage on brokers

### 8.1 Partition Log
- [ ] Segment file management
- [ ] Log append operation
- [ ] Log read operation
- [ ] Offset tracking
- [ ] High watermark management

**Files**: `src/internal/partition/log.go`

### 8.2 Segment Management
- [ ] Segment creation
- [ ] Segment rolling
- [ ] Index file creation
- [ ] Time index support

**Files**: `src/internal/partition/segment.go`

### 8.3 Message Format
- [ ] Message structure
- [ ] Serialization (JSON initially)
- [ ] Batch support
- [ ] Header support

**Files**: `src/internal/partition/message.go`

### 8.4 Retention and Cleanup
- [ ] Time-based retention
- [ ] Size-based retention
- [ ] Segment deletion
- [ ] Periodic cleanup task

**Files**: `src/internal/partition/retention.go`

**Deliverables**:
- Partition log can store/retrieve messages
- Segments roll correctly
- Retention policies work

---

## Phase 9: Broker Data Plane (Weeks 17-18)

**Goal**: Implement broker produce/consume APIs

### 9.1 Broker Service (gRPC)
- [ ] Produce RPC
- [ ] Fetch RPC
- [ ] FetchStream RPC (long-polling)
- [ ] Leader validation
- [ ] Partition routing

**Files**: `src/internal/grpc/broker_server.go`

### 9.2 Partition Manager
- [ ] Manage local partitions
- [ ] Partition creation/deletion
- [ ] Leader/follower state
- [ ] Metadata cache updates

**Files**: `src/internal/partition/manager.go`

### 9.3 Producer Logic
- [ ] Validate produce request
- [ ] Route to partition leader
- [ ] Append to log
- [ ] Return offsets

**Files**: `src/internal/grpc/produce.go`

### 9.4 Consumer Logic
- [ ] Validate fetch request
- [ ] Read from partition log
- [ ] Respect offsets
- [ ] Batch fetching

**Files**: `src/internal/grpc/fetch.go`

**Deliverables**:
- Can produce messages to broker
- Can consume messages from broker
- Leader redirect working

---

## Phase 10: Replication (Weeks 19-20)

**Goal**: Implement partition replication and ISR

### 10.1 Replication Service
- [ ] ReplicateLog RPC
- [ ] Follower fetch logic
- [ ] Leader replication logic
- [ ] ISR tracking

**Files**: `src/internal/grpc/replication_server.go`

### 10.2 Leader Replication
- [ ] Send to all replicas
- [ ] Track acknowledgments
- [ ] Update high watermark
- [ ] Handle follower failures

**Files**: `src/internal/partition/replication.go`

### 10.3 Follower Replication
- [ ] Fetch from leader
- [ ] Apply to local log
- [ ] Acknowledge to leader
- [ ] Handle leader changes

**Files**: `src/internal/partition/follower.go`

### 10.4 ISR Management
- [ ] Track replica lag
- [ ] Add/remove from ISR
- [ ] Report ISR to controller
- [ ] Handle min.insync.replicas

**Files**: `src/internal/partition/isr.go`

**Deliverables**:
- Messages replicate to followers
- ISR tracked correctly
- Can survive broker failures

---

## Phase 11: Consumer Groups (Weeks 21-22)

**Goal**: Implement consumer group management

### 11.1 Consumer Group Manager
- [ ] Group creation
- [ ] Member join/leave
- [ ] Heartbeat tracking
- [ ] Rebalancing trigger

**Files**: `src/internal/consumergroup/manager.go`

### 11.2 Partition Assignment
- [ ] Round-robin strategy
- [ ] Range strategy
- [ ] Sticky strategy
- [ ] Assignment distribution

**Files**: `src/internal/consumergroup/assignment.go`

### 11.3 Offset Management
- [ ] Commit offset
- [ ] Fetch offset
- [ ] Offset persistence
- [ ] Auto-commit support

**Files**: `src/internal/consumergroup/offset.go`

### 11.4 Consumer Group RPCs
- [ ] JoinGroup RPC
- [ ] LeaveGroup RPC
- [ ] Heartbeat RPC
- [ ] CommitOffset RPC
- [ ] FetchOffsets RPC

**Files**: `src/internal/grpc/consumergroup_server.go`

**Deliverables**:
- Consumer groups functional
- Partition assignment working
- Offsets committed and recovered
- Rebalancing on member changes

---

## Phase 12: Broker Application (Week 23)

**Goal**: Complete broker node application

### 12.1 Broker Main
- [ ] Command-line flags
- [ ] Configuration loading
- [ ] Component initialization
- [ ] Graceful shutdown

**Files**: `src/cmd/broker/main.go`

### 12.2 Integration
- [ ] Wire all broker components
- [ ] Register with controller
- [ ] Periodic heartbeat
- [ ] Metadata refresh
- [ ] Partition cleanup tasks

**Files**: `src/cmd/broker/server.go`

**Deliverables**:
- Standalone broker binary
- Registers with controller
- Handles produce/consume
- Replicates data

---

## Phase 13: CLI Tool (Week 24)

**Goal**: Build command-line interface for users

### 13.1 CLI Framework
- [ ] Command structure (cobra)
- [ ] Configuration file support
- [ ] Output formatting (table, JSON)

**Files**: `src/cmd/cli/main.go`

### 13.2 Topic Commands
- [ ] topic create
- [ ] topic list
- [ ] topic describe
- [ ] topic delete

**Files**: `src/cmd/cli/topic.go`

### 13.3 Producer/Consumer Commands
- [ ] produce (single message)
- [ ] produce (from file)
- [ ] consume (with group)
- [ ] consume (specific partition/offset)

**Files**: `src/cmd/cli/produce.go`, `src/cmd/cli/consume.go`

### 13.4 Cluster Commands
- [ ] cluster info
- [ ] broker list
- [ ] health check

**Files**: `src/cmd/cli/cluster.go`

**Deliverables**:
- CLI tool working
- Can manage cluster via CLI
- Can produce/consume messages

---

## Phase 14: Testing & Validation (Weeks 25-26)

**Goal**: Comprehensive testing and bug fixes

### 14.1 Integration Tests
- [ ] End-to-end produce/consume
- [ ] Consumer group rebalancing
- [ ] Partition leader election
- [ ] Broker failure recovery
- [ ] Controller leader election

**Files**: `test/integration/`

### 14.2 Chaos Testing
- [ ] Random broker kills
- [ ] Network partitions
- [ ] Slow networks
- [ ] Disk failures
- [ ] Clock skew

**Files**: `test/chaos/`

### 14.3 Performance Testing
- [ ] Throughput benchmarks
- [ ] Latency measurements
- [ ] Memory profiling
- [ ] CPU profiling

**Files**: `test/benchmark/`

**Deliverables**:
- All tests passing
- Performance benchmarks documented
- Known limitations documented

---

## Phase 15: Production Features (Weeks 27-30)

**Goal**: Add production-ready features

### 15.1 Security
- [ ] TLS support
- [ ] Authentication
- [ ] Authorization (ACLs)
- [ ] SASL support

### 15.2 Compression
- [ ] Gzip compression
- [ ] Snappy compression
- [ ] LZ4 compression

### 15.3 Advanced Features
- [ ] Transactions
- [ ] Idempotent producer
- [ ] Exactly-once semantics
- [ ] Quotas

### 15.4 Monitoring
- [ ] Prometheus metrics
- [ ] Grafana dashboards
- [ ] Health checks
- [ ] Alerting

**Deliverables**:
- TLS enabled
- Compression working
- Metrics exported
- Production-ready

---

## Phase 16: Documentation & Polish (Week 31-32)

**Goal**: Finalize documentation and examples

### 16.1 Documentation
- [ ] API documentation
- [ ] Configuration reference
- [ ] Operations guide
- [ ] Troubleshooting guide

### 16.2 Examples
- [ ] Producer examples (Go, Python)
- [ ] Consumer examples
- [ ] Docker Compose setup
- [ ] Kubernetes manifests

### 16.3 Polish
- [ ] Code cleanup
- [ ] Error message improvement
- [ ] Logging improvements
- [ ] Performance tuning

**Deliverables**:
- Complete documentation
- Working examples
- Production deployment guides

---

## Timeline Summary

| Phase | Duration | Status |
|-------|----------|--------|
| 1. Foundation | 2 weeks | ✅ Complete |
| 2. Core Infrastructure | 2 weeks | 📋 Planned |
| 3. Raft Implementation | 4 weeks | 📋 Planned |
| 4. Metadata Management | 2 weeks | 📋 Planned |
| 5. gRPC Services | 2 weeks | 📋 Planned |
| 6. HTTP API | 1 week | 📋 Planned |
| 7. Controller Application | 1 week | 📋 Planned |
| 8. Partition Storage | 2 weeks | 📋 Planned |
| 9. Broker Data Plane | 2 weeks | 📋 Planned |
| 10. Replication | 2 weeks | 📋 Planned |
| 11. Consumer Groups | 2 weeks | 📋 Planned |
| 12. Broker Application | 1 week | 📋 Planned |
| 13. CLI Tool | 1 week | 📋 Planned |
| 14. Testing & Validation | 2 weeks | 📋 Planned |
| 15. Production Features | 4 weeks | 📋 Planned |
| 16. Documentation & Polish | 2 weeks | 📋 Planned |
| **Total** | **32 weeks** (~8 months) | |

---

## Success Criteria

✅ **Milestone 1** (Week 8): Raft cluster running with leader election and log replication

✅ **Milestone 2** (Week 14): Controller cluster managing topics and brokers

✅ **Milestone 3** (Week 20): Brokers can produce and consume messages with replication

✅ **Milestone 4** (Week 24): Complete system with consumer groups and CLI

✅ **Milestone 5** (Week 32): Production-ready with security, monitoring, and documentation

---

## Notes

- Each phase builds on previous phases
- Testing happens continuously, not just in Phase 14
- Documentation updates happen throughout
- Timeline assumes part-time effort (20-30 hours/week)
- Adjust based on actual progress and learnings
