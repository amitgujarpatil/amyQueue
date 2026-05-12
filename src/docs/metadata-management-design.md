# Metadata Management Design: Topics, Partitions, and Storage

## Overview

This document details how to design and implement metadata management for a Kafka-like distributed system with Raft consensus, including storage, replication, and gRPC communication.

---

## Kafka Metadata: What Needs to be Stored

### 1. Topic Metadata

```json
{
  "topic_id": "topic-uuid-123",
  "name": "orders",
  "num_partitions": 10,
  "replication_factor": 3,
  "config": {
    "retention_ms": 604800000,
    "retention_bytes": 1073741824,
    "segment_ms": 86400000,
    "segment_bytes": 1073741824,
    "cleanup_policy": "delete",
    "compression_type": "snappy",
    "max_message_bytes": 1048576,
    "min_insync_replicas": 2
  },
  "partitions": [
    {
      "partition_id": 0,
      "leader": "broker-1",
      "replicas": ["broker-1", "broker-2", "broker-3"],
      "isr": ["broker-1", "broker-2"],
      "offline_replicas": []
    }
  ],
  "created_at": "2026-05-12T10:00:00Z",
  "updated_at": "2026-05-12T10:00:00Z"
}
```

### 2. Broker (Worker) Metadata

```json
{
  "broker_id": "broker-1",
  "host": "192.168.1.100",
  "port": 9092,
  "rack": "rack-1",
  "endpoints": {
    "plaintext": "192.168.1.100:9092",
    "grpc": "192.168.1.100:9093"
  },
  "status": "alive",
  "last_heartbeat": "2026-05-12T10:05:30Z",
  "registered_at": "2026-05-12T10:00:00Z",
  "resources": {
    "disk_total_bytes": 1099511627776,
    "disk_used_bytes": 549755813888,
    "cpu_usage_percent": 45.5,
    "network_throughput_mbps": 125.3
  }
}
```

### 3. Partition Assignment Metadata

```json
{
  "topic_name": "orders",
  "partition_id": 0,
  "leader": "broker-1",
  "replicas": ["broker-1", "broker-2", "broker-3"],
  "isr": ["broker-1", "broker-2"],
  "leader_epoch": 5,
  "partition_epoch": 12,
  "status": "online"
}
```

### 4. Controller Metadata

```json
{
  "controller_id": "controller-1",
  "host": "192.168.1.10",
  "raft_port": 8081,
  "grpc_port": 8082,
  "raft_state": "leader",
  "raft_term": 5,
  "commit_index": 1523,
  "last_applied": 1523,
  "cluster_id": "cluster-uuid-abc",
  "metadata_version": 1523
}
```

### 5. Configuration Metadata

```json
{
  "cluster_config": {
    "cluster_id": "cluster-uuid-abc",
    "default_replication_factor": 3,
    "min_insync_replicas": 2,
    "default_retention_ms": 604800000,
    "leader_imbalance_check_interval_ms": 300000,
    "auto_leader_rebalance_enable": true
  },
  "version": 15,
  "updated_at": "2026-05-12T10:00:00Z"
}
```

---

## Metadata Storage Architecture

### Storage Layers

```
┌─────────────────────────────────────────────────────────┐
│              APPLICATION LAYER                          │
│  (Topic creation, partition assignment, etc.)           │
└─────────────────┬───────────────────────────────────────┘
                  │
                  ▼
┌─────────────────────────────────────────────────────────┐
│           RAFT LAYER (Consensus)                        │
│  - Log replication                                      │
│  - Leader election                                      │
│  - Commit index management                              │
└─────────────────┬───────────────────────────────────────┘
                  │
                  ▼
┌─────────────────────────────────────────────────────────┐
│         STATE MACHINE LAYER                             │
│  - Apply committed entries                              │
│  - In-memory state (fast reads)                         │
│  - Snapshots for compaction                             │
└─────────────────┬───────────────────────────────────────┘
                  │
                  ▼
┌─────────────────────────────────────────────────────────┐
│           STORAGE LAYER                                 │
│  - Raft log (persistent)                                │
│  - Snapshots (periodic)                                 │
│  - State machine data (optional cache)                  │
└─────────────────────────────────────────────────────────┘
```

### File Structure

```
data/
├── controller-1/
│   ├── raft/
│   │   ├── state.json              # currentTerm, votedFor
│   │   ├── log/
│   │   │   ├── 00000000000000000001.log
│   │   │   ├── 00000000000000001001.log
│   │   │   └── 00000000000000002001.log
│   │   └── snapshots/
│   │       ├── snapshot-1000.json
│   │       └── snapshot-2000.json
│   └── metadata/
│       ├── topics/
│       │   ├── orders.json
│       │   ├── payments.json
│       │   └── users.json
│       ├── brokers/
│       │   ├── broker-1.json
│       │   ├── broker-2.json
│       │   └── broker-3.json
│       ├── partitions/
│       │   └── assignments.json
│       └── config/
│           └── cluster.json
```

---

## Raft Log Entry Structure

### Entry Types

```go
type EntryType string

const (
    EntryTypeCreateTopic     EntryType = "CREATE_TOPIC"
    EntryTypeDeleteTopic     EntryType = "DELETE_TOPIC"
    EntryTypeUpdateTopic     EntryType = "UPDATE_TOPIC"
    EntryTypeRegisterBroker  EntryType = "REGISTER_BROKER"
    EntryTypeDeregisterBroker EntryType = "DEREGISTER_BROKER"
    EntryTypeUpdatePartition EntryType = "UPDATE_PARTITION"
    EntryTypeUpdateConfig    EntryType = "UPDATE_CONFIG"
)

type LogEntry struct {
    Index     uint64    `json:"index"`
    Term      uint64    `json:"term"`
    Type      EntryType `json:"type"`
    Timestamp string    `json:"timestamp"`
    Data      []byte    `json:"data"` // JSON encoded operation
}
```

### Example Log Entries

#### 1. Create Topic Entry

```json
{
  "index": 1,
  "term": 1,
  "type": "CREATE_TOPIC",
  "timestamp": "2026-05-12T10:00:00Z",
  "data": {
    "topic_id": "topic-uuid-123",
    "name": "orders",
    "num_partitions": 10,
    "replication_factor": 3,
    "config": {
      "retention_ms": 604800000,
      "cleanup_policy": "delete"
    }
  }
}
```

#### 2. Register Broker Entry

```json
{
  "index": 2,
  "term": 1,
  "type": "REGISTER_BROKER",
  "timestamp": "2026-05-12T10:01:00Z",
  "data": {
    "broker_id": "broker-1",
    "host": "192.168.1.100",
    "port": 9092,
    "rack": "rack-1"
  }
}
```

#### 3. Update Partition Entry

```json
{
  "index": 3,
  "term": 1,
  "type": "UPDATE_PARTITION",
  "timestamp": "2026-05-12T10:02:00Z",
  "data": {
    "topic_name": "orders",
    "partition_id": 0,
    "new_leader": "broker-2",
    "new_isr": ["broker-2", "broker-3"],
    "reason": "leader_failover"
  }
}
```

---

## State Machine Design

### In-Memory State

```go
type MetadataStateMachine struct {
    mu sync.RWMutex
    
    // In-memory indexes for fast lookups
    topics       map[string]*Topic           // name -> topic
    topicsByID   map[string]*Topic           // id -> topic
    brokers      map[string]*Broker          // broker_id -> broker
    partitions   map[string]*PartitionState  // "topic:partition" -> state
    config       *ClusterConfig
    
    // Raft state
    lastApplied  uint64
    version      uint64  // Metadata version (increments on every change)
}

type Topic struct {
    ID               string                 `json:"topic_id"`
    Name             string                 `json:"name"`
    NumPartitions    int                    `json:"num_partitions"`
    ReplicationFactor int                   `json:"replication_factor"`
    Config           *TopicConfig           `json:"config"`
    Partitions       []*PartitionMetadata   `json:"partitions"`
    CreatedAt        time.Time              `json:"created_at"`
    UpdatedAt        time.Time              `json:"updated_at"`
}

type TopicConfig struct {
    RetentionMs        int64  `json:"retention_ms"`
    RetentionBytes     int64  `json:"retention_bytes"`
    SegmentMs          int64  `json:"segment_ms"`
    SegmentBytes       int64  `json:"segment_bytes"`
    CleanupPolicy      string `json:"cleanup_policy"`      // "delete" or "compact"
    CompressionType    string `json:"compression_type"`    // "none", "gzip", "snappy", "lz4"
    MaxMessageBytes    int    `json:"max_message_bytes"`
    MinInSyncReplicas  int    `json:"min_insync_replicas"`
}

type PartitionMetadata struct {
    PartitionID     int      `json:"partition_id"`
    Leader          string   `json:"leader"`
    Replicas        []string `json:"replicas"`
    ISR             []string `json:"isr"`              // In-Sync Replicas
    OfflineReplicas []string `json:"offline_replicas"`
    LeaderEpoch     int64    `json:"leader_epoch"`
}

type Broker struct {
    BrokerID      string            `json:"broker_id"`
    Host          string            `json:"host"`
    Port          int               `json:"port"`
    Rack          string            `json:"rack"`
    Endpoints     map[string]string `json:"endpoints"`
    Status        string            `json:"status"` // "alive", "dead", "decommissioning"
    LastHeartbeat time.Time         `json:"last_heartbeat"`
    RegisteredAt  time.Time         `json:"registered_at"`
    Resources     *BrokerResources  `json:"resources"`
}

type BrokerResources struct {
    DiskTotalBytes       int64   `json:"disk_total_bytes"`
    DiskUsedBytes        int64   `json:"disk_used_bytes"`
    CPUUsagePercent      float64 `json:"cpu_usage_percent"`
    NetworkThroughputMbps float64 `json:"network_throughput_mbps"`
}

type ClusterConfig struct {
    ClusterID                    string `json:"cluster_id"`
    DefaultReplicationFactor     int    `json:"default_replication_factor"`
    MinInSyncReplicas            int    `json:"min_insync_replicas"`
    DefaultRetentionMs           int64  `json:"default_retention_ms"`
    LeaderImbalanceCheckIntervalMs int64 `json:"leader_imbalance_check_interval_ms"`
    AutoLeaderRebalanceEnable    bool   `json:"auto_leader_rebalance_enable"`
}
```

### Apply Function (Core State Machine Logic)

```go
func (sm *MetadataStateMachine) Apply(entry *LogEntry) error {
    sm.mu.Lock()
    defer sm.mu.Unlock()
    
    // Idempotency check
    if entry.Index <= sm.lastApplied {
        return nil // Already applied
    }
    
    switch entry.Type {
    case EntryTypeCreateTopic:
        return sm.applyCreateTopic(entry)
    case EntryTypeDeleteTopic:
        return sm.applyDeleteTopic(entry)
    case EntryTypeUpdateTopic:
        return sm.applyUpdateTopic(entry)
    case EntryTypeRegisterBroker:
        return sm.applyRegisterBroker(entry)
    case EntryTypeDeregisterBroker:
        return sm.applyDeregisterBroker(entry)
    case EntryTypeUpdatePartition:
        return sm.applyUpdatePartition(entry)
    case EntryTypeUpdateConfig:
        return sm.applyUpdateConfig(entry)
    default:
        return fmt.Errorf("unknown entry type: %s", entry.Type)
    }
    
    sm.lastApplied = entry.Index
    sm.version++
    
    return nil
}

func (sm *MetadataStateMachine) applyCreateTopic(entry *LogEntry) error {
    var cmd CreateTopicCommand
    if err := json.Unmarshal(entry.Data, &cmd); err != nil {
        return err
    }
    
    // Check if topic already exists
    if _, exists := sm.topics[cmd.Name]; exists {
        return fmt.Errorf("topic %s already exists", cmd.Name)
    }
    
    // Create topic
    topic := &Topic{
        ID:                cmd.TopicID,
        Name:              cmd.Name,
        NumPartitions:     cmd.NumPartitions,
        ReplicationFactor: cmd.ReplicationFactor,
        Config:            cmd.Config,
        CreatedAt:         time.Now(),
        UpdatedAt:         time.Now(),
        Partitions:        make([]*PartitionMetadata, cmd.NumPartitions),
    }
    
    // Assign partitions to brokers
    brokerList := sm.getAliveBrokers()
    if len(brokerList) < cmd.ReplicationFactor {
        return fmt.Errorf("not enough brokers for replication factor %d", cmd.ReplicationFactor)
    }
    
    for i := 0; i < cmd.NumPartitions; i++ {
        replicas := sm.assignReplicas(brokerList, cmd.ReplicationFactor, i)
        leader := replicas[0] // First replica is initial leader
        
        partition := &PartitionMetadata{
            PartitionID:     i,
            Leader:          leader,
            Replicas:        replicas,
            ISR:             []string{leader}, // Initially only leader is in ISR
            OfflineReplicas: []string{},
            LeaderEpoch:     0,
        }
        
        topic.Partitions[i] = partition
        
        // Index by "topic:partition"
        key := fmt.Sprintf("%s:%d", cmd.Name, i)
        sm.partitions[key] = &PartitionState{
            TopicName:   cmd.Name,
            PartitionID: i,
            Metadata:    partition,
        }
    }
    
    // Store in indexes
    sm.topics[topic.Name] = topic
    sm.topicsByID[topic.ID] = topic
    
    return nil
}

func (sm *MetadataStateMachine) applyRegisterBroker(entry *LogEntry) error {
    var cmd RegisterBrokerCommand
    if err := json.Unmarshal(entry.Data, &cmd); err != nil {
        return err
    }
    
    broker := &Broker{
        BrokerID:      cmd.BrokerID,
        Host:          cmd.Host,
        Port:          cmd.Port,
        Rack:          cmd.Rack,
        Endpoints:     cmd.Endpoints,
        Status:        "alive",
        LastHeartbeat: time.Now(),
        RegisteredAt:  time.Now(),
        Resources:     &BrokerResources{},
    }
    
    sm.brokers[broker.BrokerID] = broker
    
    return nil
}

func (sm *MetadataStateMachine) applyUpdatePartition(entry *LogEntry) error {
    var cmd UpdatePartitionCommand
    if err := json.Unmarshal(entry.Data, &cmd); err != nil {
        return err
    }
    
    key := fmt.Sprintf("%s:%d", cmd.TopicName, cmd.PartitionID)
    partitionState, exists := sm.partitions[key]
    if !exists {
        return fmt.Errorf("partition not found: %s", key)
    }
    
    // Update partition metadata
    if cmd.NewLeader != "" {
        partitionState.Metadata.Leader = cmd.NewLeader
        partitionState.Metadata.LeaderEpoch++
    }
    
    if cmd.NewISR != nil {
        partitionState.Metadata.ISR = cmd.NewISR
    }
    
    // Update in topic as well
    topic := sm.topics[cmd.TopicName]
    topic.Partitions[cmd.PartitionID] = partitionState.Metadata
    topic.UpdatedAt = time.Now()
    
    return nil
}

// Helper: Assign replicas using round-robin
func (sm *MetadataStateMachine) assignReplicas(brokers []string, replicationFactor, partitionID int) []string {
    replicas := make([]string, replicationFactor)
    for i := 0; i < replicationFactor; i++ {
        idx := (partitionID + i) % len(brokers)
        replicas[i] = brokers[idx]
    }
    return replicas
}

func (sm *MetadataStateMachine) getAliveBrokers() []string {
    var brokers []string
    for _, broker := range sm.brokers {
        if broker.Status == "alive" {
            brokers = append(brokers, broker.BrokerID)
        }
    }
    // Sort for deterministic assignment
    sort.Strings(brokers)
    return brokers
}
```

---

## Storage Utility Design

### Interface

```go
type Storage interface {
    // Raft log
    SaveLogEntry(entry *LogEntry) error
    LoadLogEntries(startIndex, endIndex uint64) ([]*LogEntry, error)
    GetLastLogIndex() (uint64, error)
    TruncateLog(fromIndex uint64) error
    
    // Raft state
    SaveState(term uint64, votedFor string) error
    LoadState() (term uint64, votedFor string, err error)
    
    // Snapshots
    SaveSnapshot(index uint64, data []byte) error
    LoadSnapshot() (index uint64, data []byte, err error)
    GetLatestSnapshotIndex() (uint64, error)
    
    // Metadata (for debugging/backup)
    SaveMetadata(sm *MetadataStateMachine) error
    LoadMetadata() (*MetadataStateMachine, error)
    
    Close() error
}
```

### JSON File Implementation

```go
package storage

import (
    "encoding/json"
    "fmt"
    "os"
    "path/filepath"
    "sort"
    "strconv"
    "strings"
    "sync"
)

type JSONStorage struct {
    mu          sync.RWMutex
    baseDir     string
    raftDir     string
    metadataDir string
    logDir      string
    snapshotDir string
    
    // In-memory cache for recent log entries
    logCache []*LogEntry
}

func NewJSONStorage(baseDir string) (*JSONStorage, error) {
    s := &JSONStorage{
        baseDir:     baseDir,
        raftDir:     filepath.Join(baseDir, "raft"),
        metadataDir: filepath.Join(baseDir, "metadata"),
        logDir:      filepath.Join(baseDir, "raft", "log"),
        snapshotDir: filepath.Join(baseDir, "raft", "snapshots"),
        logCache:    make([]*LogEntry, 0),
    }
    
    // Create directories
    dirs := []string{s.raftDir, s.metadataDir, s.logDir, s.snapshotDir}
    for _, dir := range dirs {
        if err := os.MkdirAll(dir, 0755); err != nil {
            return nil, err
        }
    }
    
    return s, nil
}

// Save log entry to file
func (s *JSONStorage) SaveLogEntry(entry *LogEntry) error {
    s.mu.Lock()
    defer s.mu.Unlock()
    
    // Determine which log file (segment every 1000 entries)
    segmentStart := (entry.Index / 1000) * 1000
    if segmentStart == 0 {
        segmentStart = 1
    }
    
    filename := fmt.Sprintf("%020d.log", segmentStart)
    filepath := filepath.Join(s.logDir, filename)
    
    // Read existing entries in segment
    var entries []*LogEntry
    if data, err := os.ReadFile(filepath); err == nil {
        json.Unmarshal(data, &entries)
    }
    
    // Append new entry
    entries = append(entries, entry)
    
    // Write back
    data, err := json.MarshalIndent(entries, "", "  ")
    if err != nil {
        return err
    }
    
    if err := os.WriteFile(filepath, data, 0644); err != nil {
        return err
    }
    
    // Update cache
    s.logCache = append(s.logCache, entry)
    if len(s.logCache) > 100 { // Keep last 100 in cache
        s.logCache = s.logCache[1:]
    }
    
    return nil
}

// Load log entries in range
func (s *JSONStorage) LoadLogEntries(startIndex, endIndex uint64) ([]*LogEntry, error) {
    s.mu.RLock()
    defer s.mu.RUnlock()
    
    var result []*LogEntry
    
    // Determine which segments to read
    segments := s.getSegmentsInRange(startIndex, endIndex)
    
    for _, segment := range segments {
        filepath := filepath.Join(s.logDir, segment)
        data, err := os.ReadFile(filepath)
        if err != nil {
            continue
        }
        
        var entries []*LogEntry
        if err := json.Unmarshal(data, &entries); err != nil {
            return nil, err
        }
        
        // Filter entries in range
        for _, entry := range entries {
            if entry.Index >= startIndex && entry.Index <= endIndex {
                result = append(result, entry)
            }
        }
    }
    
    return result, nil
}

func (s *JSONStorage) getSegmentsInRange(startIndex, endIndex uint64) []string {
    files, _ := os.ReadDir(s.logDir)
    
    var segments []string
    for _, file := range files {
        if !strings.HasSuffix(file.Name(), ".log") {
            continue
        }
        
        // Parse segment start index from filename
        name := strings.TrimSuffix(file.Name(), ".log")
        segmentStart, err := strconv.ParseUint(name, 10, 64)
        if err != nil {
            continue
        }
        
        segmentEnd := segmentStart + 999
        
        // Check if segment overlaps with range
        if segmentEnd >= startIndex && segmentStart <= endIndex {
            segments = append(segments, file.Name())
        }
    }
    
    sort.Strings(segments)
    return segments
}

// Get last log index
func (s *JSONStorage) GetLastLogIndex() (uint64, error) {
    s.mu.RLock()
    defer s.mu.RUnlock()
    
    // Check cache first
    if len(s.logCache) > 0 {
        return s.logCache[len(s.logCache)-1].Index, nil
    }
    
    // Read from disk
    files, err := os.ReadDir(s.logDir)
    if err != nil || len(files) == 0 {
        return 0, nil
    }
    
    // Get last segment
    lastSegment := files[len(files)-1]
    filepath := filepath.Join(s.logDir, lastSegment.Name())
    
    data, err := os.ReadFile(filepath)
    if err != nil {
        return 0, err
    }
    
    var entries []*LogEntry
    if err := json.Unmarshal(data, &entries); err != nil {
        return 0, err
    }
    
    if len(entries) == 0 {
        return 0, nil
    }
    
    return entries[len(entries)-1].Index, nil
}

// Save Raft state (term, votedFor)
func (s *JSONStorage) SaveState(term uint64, votedFor string) error {
    s.mu.Lock()
    defer s.mu.Unlock()
    
    state := map[string]interface{}{
        "term":      term,
        "voted_for": votedFor,
    }
    
    data, err := json.MarshalIndent(state, "", "  ")
    if err != nil {
        return err
    }
    
    filepath := filepath.Join(s.raftDir, "state.json")
    return os.WriteFile(filepath, data, 0644)
}

// Load Raft state
func (s *JSONStorage) LoadState() (uint64, string, error) {
    s.mu.RLock()
    defer s.mu.RUnlock()
    
    filepath := filepath.Join(s.raftDir, "state.json")
    data, err := os.ReadFile(filepath)
    if err != nil {
        if os.IsNotExist(err) {
            return 0, "", nil // No state yet
        }
        return 0, "", err
    }
    
    var state map[string]interface{}
    if err := json.Unmarshal(data, &state); err != nil {
        return 0, "", err
    }
    
    term := uint64(state["term"].(float64))
    votedFor := state["voted_for"].(string)
    
    return term, votedFor, nil
}

// Save snapshot
func (s *JSONStorage) SaveSnapshot(index uint64, data []byte) error {
    s.mu.Lock()
    defer s.mu.Unlock()
    
    filename := fmt.Sprintf("snapshot-%020d.json", index)
    filepath := filepath.Join(s.snapshotDir, filename)
    
    return os.WriteFile(filepath, data, 0644)
}

// Load latest snapshot
func (s *JSONStorage) LoadSnapshot() (uint64, []byte, error) {
    s.mu.RLock()
    defer s.mu.RUnlock()
    
    files, err := os.ReadDir(s.snapshotDir)
    if err != nil || len(files) == 0 {
        return 0, nil, nil
    }
    
    // Get latest snapshot (last file)
    sort.Slice(files, func(i, j int) bool {
        return files[i].Name() < files[j].Name()
    })
    
    lastSnapshot := files[len(files)-1]
    filepath := filepath.Join(s.snapshotDir, lastSnapshot.Name())
    
    data, err := os.ReadFile(filepath)
    if err != nil {
        return 0, nil, err
    }
    
    // Parse index from filename
    name := strings.TrimPrefix(lastSnapshot.Name(), "snapshot-")
    name = strings.TrimSuffix(name, ".json")
    index, _ := strconv.ParseUint(name, 10, 64)
    
    return index, data, nil
}

// Save metadata (for backup/debugging)
func (s *JSONStorage) SaveMetadata(sm *MetadataStateMachine) error {
    s.mu.Lock()
    defer s.mu.Unlock()
    
    // Save topics
    topicsDir := filepath.Join(s.metadataDir, "topics")
    os.MkdirAll(topicsDir, 0755)
    
    for _, topic := range sm.topics {
        data, _ := json.MarshalIndent(topic, "", "  ")
        filepath := filepath.Join(topicsDir, topic.Name+".json")
        os.WriteFile(filepath, data, 0644)
    }
    
    // Save brokers
    brokersDir := filepath.Join(s.metadataDir, "brokers")
    os.MkdirAll(brokersDir, 0755)
    
    for _, broker := range sm.brokers {
        data, _ := json.MarshalIndent(broker, "", "  ")
        filepath := filepath.Join(brokersDir, broker.BrokerID+".json")
        os.WriteFile(filepath, data, 0644)
    }
    
    return nil
}

func (s *JSONStorage) Close() error {
    // Flush any cached data if needed
    return nil
}
```

---

## gRPC Service Definitions

### Proto File: `metadata.proto`

```protobuf
syntax = "proto3";

package metadata;

option go_package = "github.com/yourorg/yourproject/proto/metadata";

// Controller service (for admin operations and metadata management)
service MetadataService {
  // Topic operations
  rpc CreateTopic(CreateTopicRequest) returns (CreateTopicResponse);
  rpc DeleteTopic(DeleteTopicRequest) returns (DeleteTopicResponse);
  rpc ListTopics(ListTopicsRequest) returns (ListTopicsResponse);
  rpc DescribeTopic(DescribeTopicRequest) returns (DescribeTopicResponse);
  rpc UpdateTopicConfig(UpdateTopicConfigRequest) returns (UpdateTopicConfigResponse);
  
  // Broker operations
  rpc RegisterBroker(RegisterBrokerRequest) returns (RegisterBrokerResponse);
  rpc Heartbeat(HeartbeatRequest) returns (HeartbeatResponse);
  rpc GetMetadata(GetMetadataRequest) returns (GetMetadataResponse);
  
  // Partition operations
  rpc UpdatePartitionState(UpdatePartitionStateRequest) returns (UpdatePartitionStateResponse);
  
  // Cluster operations
  rpc GetClusterInfo(GetClusterInfoRequest) returns (GetClusterInfoResponse);
}

// Raft service (for controller-to-controller communication)
service RaftService {
  rpc RequestVote(RequestVoteRequest) returns (RequestVoteResponse);
  rpc AppendEntries(AppendEntriesRequest) returns (AppendEntriesResponse);
  rpc InstallSnapshot(stream InstallSnapshotRequest) returns (InstallSnapshotResponse);
}

// Messages

message CreateTopicRequest {
  string name = 1;
  int32 num_partitions = 2;
  int32 replication_factor = 3;
  map<string, string> config = 4;
}

message CreateTopicResponse {
  bool success = 1;
  string error = 2;
  string topic_id = 3;
}

message DeleteTopicRequest {
  string name = 1;
}

message DeleteTopicResponse {
  bool success = 1;
  string error = 2;
}

message ListTopicsRequest {
  // Empty for now
}

message ListTopicsResponse {
  repeated TopicInfo topics = 1;
}

message TopicInfo {
  string topic_id = 1;
  string name = 2;
  int32 num_partitions = 3;
  int32 replication_factor = 4;
  map<string, string> config = 5;
}

message DescribeTopicRequest {
  string name = 1;
}

message DescribeTopicResponse {
  bool success = 1;
  string error = 2;
  TopicDetail topic = 3;
}

message TopicDetail {
  string topic_id = 1;
  string name = 2;
  int32 num_partitions = 3;
  int32 replication_factor = 4;
  map<string, string> config = 5;
  repeated PartitionDetail partitions = 6;
  string created_at = 7;
  string updated_at = 8;
}

message PartitionDetail {
  int32 partition_id = 1;
  string leader = 2;
  repeated string replicas = 3;
  repeated string isr = 4;
  repeated string offline_replicas = 5;
  int64 leader_epoch = 6;
}

message RegisterBrokerRequest {
  string broker_id = 1;
  string host = 2;
  int32 port = 3;
  string rack = 4;
  map<string, string> endpoints = 5;
}

message RegisterBrokerResponse {
  bool success = 1;
  string error = 2;
}

message HeartbeatRequest {
  string broker_id = 1;
  BrokerResources resources = 2;
}

message BrokerResources {
  int64 disk_total_bytes = 1;
  int64 disk_used_bytes = 2;
  double cpu_usage_percent = 3;
  double network_throughput_mbps = 4;
}

message HeartbeatResponse {
  bool success = 1;
  int64 metadata_version = 2;
}

message GetMetadataRequest {
  int64 last_known_version = 1;
}

message GetMetadataResponse {
  int64 version = 1;
  repeated TopicDetail topics = 2;
  repeated BrokerInfo brokers = 3;
  ClusterConfig config = 4;
}

message BrokerInfo {
  string broker_id = 1;
  string host = 2;
  int32 port = 3;
  string status = 4;
  map<string, string> endpoints = 5;
}

message ClusterConfig {
  string cluster_id = 1;
  int32 default_replication_factor = 2;
  int32 min_insync_replicas = 3;
  int64 default_retention_ms = 4;
}

message UpdatePartitionStateRequest {
  string topic_name = 1;
  int32 partition_id = 2;
  string new_leader = 3;
  repeated string new_isr = 4;
  string reason = 5;
}

message UpdatePartitionStateResponse {
  bool success = 1;
  string error = 2;
}

message GetClusterInfoRequest {
  // Empty
}

message GetClusterInfoResponse {
  string cluster_id = 1;
  string controller_leader = 2;
  int32 num_controllers = 3;
  int32 num_brokers = 4;
  int32 num_topics = 5;
  int64 metadata_version = 6;
}

// Raft messages

message RequestVoteRequest {
  uint64 term = 1;
  string candidate_id = 2;
  uint64 last_log_index = 3;
  uint64 last_log_term = 4;
}

message RequestVoteResponse {
  uint64 term = 1;
  bool vote_granted = 2;
}

message AppendEntriesRequest {
  uint64 term = 1;
  string leader_id = 2;
  uint64 prev_log_index = 3;
  uint64 prev_log_term = 4;
  repeated LogEntry entries = 5;
  uint64 leader_commit = 6;
}

message LogEntry {
  uint64 index = 1;
  uint64 term = 2;
  string type = 3;
  string timestamp = 4;
  bytes data = 5;
}

message AppendEntriesResponse {
  uint64 term = 1;
  bool success = 2;
  uint64 conflict_index = 3;
  uint64 conflict_term = 4;
}

message InstallSnapshotRequest {
  uint64 term = 1;
  string leader_id = 2;
  uint64 last_included_index = 3;
  uint64 last_included_term = 4;
  bytes data = 5;
  bool done = 6;
}

message InstallSnapshotResponse {
  uint64 term = 1;
  bool success = 2;
}
```

---

## Metadata Replication Flow

### Flow 1: Create Topic

```
Client → Controller Leader:
  CreateTopic(name="orders", partitions=10, replicas=3)

Controller Leader:
  1. Validate request (enough brokers, valid config)
  2. Create Raft log entry
  3. Replicate to follower controllers via AppendEntries
  4. Wait for majority ACK
  5. Commit entry
  6. Apply to state machine
  7. Respond to client

State machine applies entry:
  - Create topic in memory
  - Assign partitions to brokers
  - Update version

Workers fetch metadata:
  - Periodic GetMetadata RPC to leader
  - Discover new topic
  - Start accepting produce/consume requests
```

### Flow 2: Broker Registration

```
Broker starts:
  1. Load config (controller addresses)
  2. Call RegisterBroker(broker_id, host, port) to any controller
  3. If not leader, get redirected to leader
  4. Leader creates Raft log entry
  5. Entry replicated and committed
  6. Leader responds with success

Broker ongoing:
  - Send Heartbeat every 5 seconds to leader
  - Fetch metadata updates
  - Report resource usage
```

### Flow 3: Partition Leader Failover

```
Scenario: Broker-1 (leader of partition 0) crashes

Controller leader detects:
  1. Heartbeat timeout for broker-1
  2. Mark broker-1 as "dead"
  3. For each partition where broker-1 is leader:
     - Select new leader from ISR
     - Create UpdatePartition log entry
     - Replicate and commit
     - Apply to state machine

Other brokers fetch metadata:
  - Discover partition 0 has new leader (broker-2)
  - Update local routing
  - Send requests to new leader
```

---

## Complete Example: Topic Creation

### 1. Client Request

```bash
grpcurl -plaintext -d '{
  "name": "orders",
  "num_partitions": 10,
  "replication_factor": 3,
  "config": {
    "retention_ms": "604800000",
    "cleanup_policy": "delete"
  }
}' localhost:8082 metadata.MetadataService/CreateTopic
```

### 2. Controller Receives Request

```go
func (s *MetadataServer) CreateTopic(ctx context.Context, req *pb.CreateTopicRequest) (*pb.CreateTopicResponse, error) {
    // Validate
    if req.NumPartitions <= 0 {
        return &pb.CreateTopicResponse{
            Success: false,
            Error:   "num_partitions must be positive",
        }, nil
    }
    
    // Check if we're the leader
    if !s.raft.IsLeader() {
        leader := s.raft.GetLeader()
        return &pb.CreateTopicResponse{
            Success: false,
            Error:   fmt.Sprintf("not leader, try %s", leader),
        }, nil
    }
    
    // Create command
    cmd := CreateTopicCommand{
        TopicID:           generateUUID(),
        Name:              req.Name,
        NumPartitions:     int(req.NumPartitions),
        ReplicationFactor: int(req.ReplicationFactor),
        Config:            parseConfig(req.Config),
    }
    
    data, _ := json.Marshal(cmd)
    
    // Create log entry
    entry := &LogEntry{
        Type:      EntryTypeCreateTopic,
        Timestamp: time.Now().Format(time.RFC3339),
        Data:      data,
    }
    
    // Propose to Raft (this will replicate and commit)
    if err := s.raft.Propose(entry); err != nil {
        return &pb.CreateTopicResponse{
            Success: false,
            Error:   err.Error(),
        }, nil
    }
    
    // Wait for commit (with timeout)
    if err := s.waitForApply(entry, 5*time.Second); err != nil {
        return &pb.CreateTopicResponse{
            Success: false,
            Error:   "timeout waiting for commit",
        }, nil
    }
    
    return &pb.CreateTopicResponse{
        Success: true,
        TopicId: cmd.TopicID,
    }, nil
}
```

### 3. Raft Replication

```
Controller-1 (Leader):
  - Creates log entry at index 15, term 3
  - Sends AppendEntries to Controller-2 and Controller-3

Controller-2 (Follower):
  - Receives AppendEntries
  - Checks prevLogIndex/prevLogTerm match
  - Appends entry to log
  - Saves to disk
  - Responds success

Controller-3 (Follower):
  - Same as Controller-2

Controller-1 (Leader):
  - Receives 2/3 ACKs (majority!)
  - Commits entry (commitIndex = 15)
  - Applies to state machine
  - Responds to client
```

### 4. State Machine Apply

```go
func (sm *MetadataStateMachine) applyCreateTopic(entry *LogEntry) error {
    // Parse command
    var cmd CreateTopicCommand
    json.Unmarshal(entry.Data, &cmd)
    
    // Create topic
    topic := &Topic{
        ID:                cmd.TopicID,
        Name:              cmd.Name,
        NumPartitions:     cmd.NumPartitions,
        ReplicationFactor: cmd.ReplicationFactor,
        Config:            cmd.Config,
        CreatedAt:         time.Now(),
    }
    
    // Assign partitions
    brokers := sm.getAliveBrokers() // ["broker-1", "broker-2", "broker-3", "broker-4"]
    
    for i := 0; i < cmd.NumPartitions; i++ {
        replicas := []string{
            brokers[(i+0)%len(brokers)],
            brokers[(i+1)%len(brokers)],
            brokers[(i+2)%len(brokers)],
        }
        
        partition := &PartitionMetadata{
            PartitionID: i,
            Leader:      replicas[0],
            Replicas:    replicas,
            ISR:         []string{replicas[0]},
        }
        
        topic.Partitions = append(topic.Partitions, partition)
    }
    
    // Store
    sm.topics[topic.Name] = topic
    sm.version++
    
    log.Printf("Topic created: %s with %d partitions", topic.Name, topic.NumPartitions)
    
    return nil
}
```

### 5. Workers Discover Topic

```go
// Worker periodically fetches metadata
func (w *Worker) fetchMetadata() {
    resp, err := w.controllerClient.GetMetadata(context.Background(), &pb.GetMetadataRequest{
        LastKnownVersion: w.metadataVersion,
    })
    
    if err != nil {
        log.Printf("Error fetching metadata: %v", err)
        return
    }
    
    if resp.Version > w.metadataVersion {
        // New metadata available
        w.updateMetadata(resp)
        w.metadataVersion = resp.Version
        
        log.Printf("Metadata updated to version %d, topics: %d", resp.Version, len(resp.Topics))
    }
}
```

---

## Project Structure

```
project/
├── cmd/
│   ├── controller/
│   │   └── main.go
│   └── broker/
│       └── main.go
├── internal/
│   ├── raft/
│   │   ├── node.go
│   │   ├── election.go
│   │   ├── replication.go
│   │   └── log.go
│   ├── metadata/
│   │   ├── state_machine.go
│   │   ├── topic.go
│   │   ├── broker.go
│   │   └── partition.go
│   ├── storage/
│   │   ├── interface.go
│   │   ├── json_storage.go
│   │   └── snapshot.go
│   └── grpc/
│       ├── metadata_server.go
│       ├── raft_server.go
│       └── client.go
├── proto/
│   ├── metadata.proto
│   └── raft.proto
├── pkg/
│   └── utils/
│       ├── uuid.go
│       └── time.go
├── configs/
│   ├── controller-1.json
│   ├── controller-2.json
│   └── broker-1.json
└── data/
    ├── controller-1/
    ├── controller-2/
    └── broker-1/
```

---

## Next Steps

### Phase 1: Storage Layer ✅
- Implement JSON storage utility
- Test save/load log entries
- Test snapshots

### Phase 2: Metadata State Machine ✅
- Implement topic creation
- Implement broker registration
- Test apply function

### Phase 3: gRPC Services
- Define proto files
- Generate Go code
- Implement MetadataService
- Implement RaftService

### Phase 4: Integration
- Connect Raft with state machine
- Implement propose/commit flow
- Test end-to-end topic creation

### Phase 5: Workers
- Implement metadata fetch
- Implement heartbeat
- Test leader discovery

### Phase 6: Advanced Features
- Partition reassignment
- Leader election (partition level)
- Retention/cleanup

---

## Summary

**Metadata to Store:**
- Topics (name, partitions, replicas, config, retention)
- Brokers (id, host, port, status, resources)
- Partitions (leader, ISR, replicas)
- Cluster config

**Storage Strategy:**
- Raft log for all changes (append-only)
- State machine for current state (in-memory)
- JSON files for persistence (segments + snapshots)
- Periodic snapshots for compaction

**Replication:**
- Metadata changes → Raft log entries
- Replicated via AppendEntries
- Committed when majority ACK
- Applied to state machine on all controllers
- Workers fetch from leader

**gRPC Services:**
- MetadataService (admin operations, metadata fetch)
- RaftService (controller-to-controller consensus)
- Separate from data operations (produce/consume)

This design gives you strong consistency (Raft), durability (persistent log), and scalability (workers fetch metadata)!
