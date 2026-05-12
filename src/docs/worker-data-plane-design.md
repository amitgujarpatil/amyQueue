# Worker Data Plane: Message Storage and Consumer Groups

## Overview

This document covers the implementation of the data plane on worker nodes: storing messages, handling produce/consume operations, managing consumer groups, offsets, and persistence.

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────┐
│                   PRODUCER CLIENTS                      │
└─────────────┬───────────────────────────────────────────┘
              │ Produce Request
              ▼
┌─────────────────────────────────────────────────────────┐
│                    WORKER NODE                          │
│                                                          │
│  ┌────────────────────────────────────────────────┐    │
│  │          Producer Handler                       │    │
│  │  - Route to partition                           │    │
│  │  - Validate message                             │    │
│  │  - Assign offset                                │    │
│  └────────────┬───────────────────────────────────┘    │
│               │                                          │
│               ▼                                          │
│  ┌────────────────────────────────────────────────┐    │
│  │        Partition Manager                        │    │
│  │  - Manage partition logs                        │    │
│  │  - Handle replication                           │    │
│  │  - Track high watermark                         │    │
│  └────────────┬───────────────────────────────────┘    │
│               │                                          │
│               ▼                                          │
│  ┌────────────────────────────────────────────────┐    │
│  │          Log Storage                            │    │
│  │  - Segment files                                │    │
│  │  - Index files                                  │    │
│  │  - Write messages                               │    │
│  └─────────────────────────────────────────────────┘   │
│                                                          │
│  ┌────────────────────────────────────────────────┐    │
│  │        Consumer Handler                         │    │
│  │  - Fetch messages                               │    │
│  │  - Track consumer groups                        │    │
│  │  - Commit offsets                               │    │
│  └────────────┬───────────────────────────────────┘    │
│               │                                          │
│               ▼                                          │
│  ┌────────────────────────────────────────────────┐    │
│  │      Consumer Group Manager                     │    │
│  │  - Track group members                          │    │
│  │  - Partition assignment                         │    │
│  │  - Offset storage                               │    │
│  └─────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────┘
              │
              ▼ Consume Request
┌─────────────────────────────────────────────────────────┐
│                   CONSUMER CLIENTS                      │
└─────────────────────────────────────────────────────────┘
```

---

## File Storage Structure

### Directory Layout

```
data/
├── broker-1/
│   ├── partitions/
│   │   ├── orders-0/              # Topic: orders, Partition: 0
│   │   │   ├── 00000000000000000000.log    # Segment file
│   │   │   ├── 00000000000000000000.index  # Offset index
│   │   │   ├── 00000000000000000000.timeindex # Time index
│   │   │   ├── 00000000000001000000.log
│   │   │   ├── 00000000000001000000.index
│   │   │   ├── 00000000000001000000.timeindex
│   │   │   └── metadata.json       # Partition metadata
│   │   ├── orders-1/
│   │   │   ├── 00000000000000000000.log
│   │   │   ├── 00000000000000000000.index
│   │   │   └── metadata.json
│   │   └── payments-0/
│   │       └── ...
│   ├── consumer-groups/
│   │   ├── checkout-service/      # Consumer group name
│   │   │   ├── offsets.json       # Committed offsets
│   │   │   └── members.json       # Group members
│   │   └── analytics-service/
│   │       └── offsets.json
│   └── replication/
│       └── follower-state.json    # Tracking for follower partitions
```

### Segment Files Explained

**Why Segments?**
- Can't have infinite single file
- Enables efficient cleanup (delete old segments)
- Enables fast lookups with indexes
- Bounded memory usage

**Segment Naming:**
```
Filename: 00000000000000123456.log
          └─────────┬────────────┘
              Base offset (20 digits)

Example:
00000000000000000000.log  → Messages starting from offset 0
00000000000001000000.log  → Messages starting from offset 1,000,000
```

---

## Message Format

### Wire Format (Binary)

```
Message Structure:
┌─────────────────────────────────────────────────────────┐
│ Offset (8 bytes)                                        │
├─────────────────────────────────────────────────────────┤
│ MessageSize (4 bytes)                                   │
├─────────────────────────────────────────────────────────┤
│ CRC (4 bytes) - checksum                                │
├─────────────────────────────────────────────────────────┤
│ Magic (1 byte) - version                                │
├─────────────────────────────────────────────────────────┤
│ Attributes (1 byte) - compression, etc.                 │
├─────────────────────────────────────────────────────────┤
│ Timestamp (8 bytes)                                     │
├─────────────────────────────────────────────────────────┤
│ KeyLength (4 bytes)                                     │
├─────────────────────────────────────────────────────────┤
│ Key (variable bytes)                                    │
├─────────────────────────────────────────────────────────┤
│ ValueLength (4 bytes)                                   │
├─────────────────────────────────────────────────────────┤
│ Value (variable bytes)                                  │
├─────────────────────────────────────────────────────────┤
│ HeadersCount (4 bytes)                                  │
├─────────────────────────────────────────────────────────┤
│ Headers (variable)                                      │
└─────────────────────────────────────────────────────────┘

Total: 34 + KeyLength + ValueLength + HeadersLength bytes
```

### JSON Format (For Simplicity)

```json
{
  "offset": 12345,
  "timestamp": "2026-05-12T10:00:00.123Z",
  "key": "user123",
  "value": {
    "order_id": "order-789",
    "amount": 99.99,
    "items": ["item1", "item2"]
  },
  "headers": {
    "source": "checkout-service",
    "trace-id": "abc-123-def"
  }
}
```

### Go Struct

```go
type Message struct {
    Offset    int64             `json:"offset"`
    Timestamp time.Time         `json:"timestamp"`
    Key       string            `json:"key"`
    Value     []byte            `json:"value"`     // Raw JSON or bytes
    Headers   map[string]string `json:"headers"`
    
    // Internal fields (not serialized to file)
    partition int32
    topic     string
}

type MessageBatch struct {
    Messages []Message `json:"messages"`
}
```

---

## Partition Log Implementation

### Partition Log Structure

```go
package partition

import (
    "encoding/json"
    "fmt"
    "io"
    "os"
    "path/filepath"
    "sync"
    "time"
)

type PartitionLog struct {
    mu sync.RWMutex
    
    topic       string
    partitionID int32
    dataDir     string
    
    // Active segment (currently being written to)
    activeSegment *Segment
    
    // All segments (including active)
    segments []*Segment
    
    // Config
    segmentSize      int64  // Max size per segment (e.g., 1GB)
    retentionMs      int64  // Retention period
    retentionBytes   int64  // Retention size
    
    // State
    nextOffset       int64  // Next offset to assign
    highWatermark    int64  // Highest committed offset
    logStartOffset   int64  // Oldest offset (after cleanup)
}

type Segment struct {
    baseOffset  int64
    logFile     *os.File
    indexFile   *os.File
    currentSize int64
    lastModified time.Time
}

func NewPartitionLog(topic string, partitionID int32, dataDir string, config *PartitionConfig) (*PartitionLog, error) {
    partitionDir := filepath.Join(dataDir, fmt.Sprintf("%s-%d", topic, partitionID))
    
    if err := os.MkdirAll(partitionDir, 0755); err != nil {
        return nil, err
    }
    
    pl := &PartitionLog{
        topic:          topic,
        partitionID:    partitionID,
        dataDir:        partitionDir,
        segmentSize:    config.SegmentBytes,
        retentionMs:    config.RetentionMs,
        retentionBytes: config.RetentionBytes,
        segments:       make([]*Segment, 0),
    }
    
    // Load existing segments or create first one
    if err := pl.loadSegments(); err != nil {
        return nil, err
    }
    
    if len(pl.segments) == 0 {
        if err := pl.createNewSegment(0); err != nil {
            return nil, err
        }
    }
    
    pl.activeSegment = pl.segments[len(pl.segments)-1]
    
    // Recover nextOffset from last message
    pl.recoverState()
    
    return pl, nil
}

func (pl *PartitionLog) Append(messages []*Message) ([]int64, error) {
    pl.mu.Lock()
    defer pl.mu.Unlock()
    
    offsets := make([]int64, len(messages))
    
    for i, msg := range messages {
        // Assign offset
        msg.Offset = pl.nextOffset
        msg.Timestamp = time.Now()
        offsets[i] = msg.Offset
        
        // Serialize message
        data, err := json.Marshal(msg)
        if err != nil {
            return nil, err
        }
        
        // Add newline delimiter
        data = append(data, '\n')
        
        // Check if we need to roll segment
        if pl.activeSegment.currentSize+int64(len(data)) > pl.segmentSize {
            if err := pl.rollSegment(); err != nil {
                return nil, err
            }
        }
        
        // Write to active segment
        if err := pl.writeToSegment(data); err != nil {
            return nil, err
        }
        
        pl.nextOffset++
    }
    
    // Sync to disk (can be async for better performance)
    if err := pl.activeSegment.logFile.Sync(); err != nil {
        return nil, err
    }
    
    return offsets, nil
}

func (pl *PartitionLog) Read(startOffset int64, maxMessages int, maxBytes int64) ([]*Message, error) {
    pl.mu.RLock()
    defer pl.mu.RUnlock()
    
    // Validate offset
    if startOffset < pl.logStartOffset {
        return nil, fmt.Errorf("offset %d is before log start offset %d", startOffset, pl.logStartOffset)
    }
    
    if startOffset >= pl.nextOffset {
        return []*Message{}, nil // No messages available
    }
    
    // Find segment containing startOffset
    segment := pl.findSegment(startOffset)
    if segment == nil {
        return nil, fmt.Errorf("segment not found for offset %d", startOffset)
    }
    
    // Read messages from segment(s)
    messages := make([]*Message, 0, maxMessages)
    totalBytes := int64(0)
    currentSegmentIdx := pl.getSegmentIndex(segment)
    
    for currentSegmentIdx < len(pl.segments) {
        seg := pl.segments[currentSegmentIdx]
        
        // Read from this segment
        segMessages, err := pl.readFromSegment(seg, startOffset, maxMessages-len(messages), maxBytes-totalBytes)
        if err != nil {
            return nil, err
        }
        
        for _, msg := range segMessages {
            if msg.Offset >= startOffset {
                messages = append(messages, msg)
                totalBytes += int64(len(msg.Value))
                
                if len(messages) >= maxMessages || totalBytes >= maxBytes {
                    return messages, nil
                }
            }
        }
        
        // Move to next segment if we need more messages
        currentSegmentIdx++
        if currentSegmentIdx < len(pl.segments) {
            startOffset = pl.segments[currentSegmentIdx].baseOffset
        }
    }
    
    return messages, nil
}

func (pl *PartitionLog) writeToSegment(data []byte) error {
    n, err := pl.activeSegment.logFile.Write(data)
    if err != nil {
        return err
    }
    
    pl.activeSegment.currentSize += int64(n)
    pl.activeSegment.lastModified = time.Now()
    
    return nil
}

func (pl *PartitionLog) rollSegment() error {
    // Close current active segment
    if err := pl.activeSegment.logFile.Close(); err != nil {
        return err
    }
    
    // Create new segment starting at nextOffset
    if err := pl.createNewSegment(pl.nextOffset); err != nil {
        return err
    }
    
    pl.activeSegment = pl.segments[len(pl.segments)-1]
    
    return nil
}

func (pl *PartitionLog) createNewSegment(baseOffset int64) error {
    segmentName := fmt.Sprintf("%020d", baseOffset)
    logPath := filepath.Join(pl.dataDir, segmentName+".log")
    indexPath := filepath.Join(pl.dataDir, segmentName+".index")
    
    logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
    if err != nil {
        return err
    }
    
    indexFile, err := os.OpenFile(indexPath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
    if err != nil {
        logFile.Close()
        return err
    }
    
    segment := &Segment{
        baseOffset:   baseOffset,
        logFile:      logFile,
        indexFile:    indexFile,
        currentSize:  0,
        lastModified: time.Now(),
    }
    
    pl.segments = append(pl.segments, segment)
    
    return nil
}

func (pl *PartitionLog) findSegment(offset int64) *Segment {
    // Binary search for segment
    for i := len(pl.segments) - 1; i >= 0; i-- {
        if pl.segments[i].baseOffset <= offset {
            return pl.segments[i]
        }
    }
    return nil
}

func (pl *PartitionLog) readFromSegment(seg *Segment, startOffset int64, maxMessages int, maxBytes int64) ([]*Message, error) {
    // Seek to beginning of file
    seg.logFile.Seek(0, io.SeekStart)
    
    messages := make([]*Message, 0)
    decoder := json.NewDecoder(seg.logFile)
    totalBytes := int64(0)
    
    for {
        var msg Message
        if err := decoder.Decode(&msg); err != nil {
            if err == io.EOF {
                break
            }
            return nil, err
        }
        
        if msg.Offset >= startOffset {
            messages = append(messages, &msg)
            totalBytes += int64(len(msg.Value))
            
            if len(messages) >= maxMessages || totalBytes >= maxBytes {
                break
            }
        }
    }
    
    return messages, nil
}

func (pl *PartitionLog) loadSegments() error {
    files, err := os.ReadDir(pl.dataDir)
    if err != nil {
        return err
    }
    
    for _, file := range files {
        if filepath.Ext(file.Name()) == ".log" {
            // Parse base offset from filename
            var baseOffset int64
            fmt.Sscanf(file.Name(), "%020d.log", &baseOffset)
            
            logPath := filepath.Join(pl.dataDir, file.Name())
            indexPath := filepath.Join(pl.dataDir, fmt.Sprintf("%020d.index", baseOffset))
            
            logFile, err := os.OpenFile(logPath, os.O_RDWR|os.O_APPEND, 0644)
            if err != nil {
                return err
            }
            
            indexFile, err := os.OpenFile(indexPath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
            if err != nil {
                logFile.Close()
                return err
            }
            
            stat, _ := logFile.Stat()
            
            segment := &Segment{
                baseOffset:   baseOffset,
                logFile:      logFile,
                indexFile:    indexFile,
                currentSize:  stat.Size(),
                lastModified: stat.ModTime(),
            }
            
            pl.segments = append(pl.segments, segment)
        }
    }
    
    return nil
}

func (pl *PartitionLog) recoverState() {
    // Find highest offset from segments
    if len(pl.segments) > 0 {
        lastSegment := pl.segments[len(pl.segments)-1]
        
        // Read last segment to find highest offset
        lastSegment.logFile.Seek(0, io.SeekStart)
        decoder := json.NewDecoder(lastSegment.logFile)
        
        lastOffset := lastSegment.baseOffset - 1
        
        for {
            var msg Message
            if err := decoder.Decode(&msg); err != nil {
                break
            }
            lastOffset = msg.Offset
        }
        
        pl.nextOffset = lastOffset + 1
        pl.highWatermark = lastOffset
    }
}

func (pl *PartitionLog) GetHighWatermark() int64 {
    pl.mu.RLock()
    defer pl.mu.RUnlock()
    return pl.highWatermark
}

func (pl *PartitionLog) GetLogStartOffset() int64 {
    pl.mu.RLock()
    defer pl.mu.RUnlock()
    return pl.logStartOffset
}

func (pl *PartitionLog) GetLogEndOffset() int64 {
    pl.mu.RLock()
    defer pl.mu.RUnlock()
    return pl.nextOffset
}

// Cleanup old segments based on retention policy
func (pl *PartitionLog) Cleanup() error {
    pl.mu.Lock()
    defer pl.mu.Unlock()
    
    now := time.Now()
    deletableSegments := make([]*Segment, 0)
    
    // Find segments to delete
    for i, seg := range pl.segments {
        // Don't delete active segment
        if i == len(pl.segments)-1 {
            break
        }
        
        // Check time-based retention
        if pl.retentionMs > 0 {
            age := now.Sub(seg.lastModified).Milliseconds()
            if age > pl.retentionMs {
                deletableSegments = append(deletableSegments, seg)
                continue
            }
        }
        
        // Check size-based retention
        if pl.retentionBytes > 0 {
            totalSize := int64(0)
            for j := i; j < len(pl.segments); j++ {
                totalSize += pl.segments[j].currentSize
            }
            if totalSize > pl.retentionBytes {
                deletableSegments = append(deletableSegments, seg)
            }
        }
    }
    
    // Delete segments
    for _, seg := range deletableSegments {
        if err := pl.deleteSegment(seg); err != nil {
            return err
        }
    }
    
    return nil
}

func (pl *PartitionLog) deleteSegment(seg *Segment) error {
    seg.logFile.Close()
    seg.indexFile.Close()
    
    logPath := filepath.Join(pl.dataDir, fmt.Sprintf("%020d.log", seg.baseOffset))
    indexPath := filepath.Join(pl.dataDir, fmt.Sprintf("%020d.index", seg.baseOffset))
    
    os.Remove(logPath)
    os.Remove(indexPath)
    
    // Remove from segments list
    for i, s := range pl.segments {
        if s.baseOffset == seg.baseOffset {
            pl.segments = append(pl.segments[:i], pl.segments[i+1:]...)
            break
        }
    }
    
    // Update log start offset
    if len(pl.segments) > 0 {
        pl.logStartOffset = pl.segments[0].baseOffset
    }
    
    return nil
}

func (pl *PartitionLog) Close() error {
    pl.mu.Lock()
    defer pl.mu.Unlock()
    
    for _, seg := range pl.segments {
        seg.logFile.Close()
        seg.indexFile.Close()
    }
    
    return nil
}

func (pl *PartitionLog) getSegmentIndex(segment *Segment) int {
    for i, seg := range pl.segments {
        if seg.baseOffset == segment.baseOffset {
            return i
        }
    }
    return -1
}
```

---

## Consumer Group Management

### Consumer Group Structure

```go
package consumergroup

import (
    "encoding/json"
    "fmt"
    "os"
    "path/filepath"
    "sync"
    "time"
)

type ConsumerGroup struct {
    GroupID   string
    Members   map[string]*Member  // consumerID -> Member
    Offsets   map[string]int64    // "topic:partition" -> offset
    State     string              // "Empty", "Stable", "Rebalancing"
    Leader    string              // Leader member ID
    Protocol  string              // Assignment strategy
    Generation int                // Increments on rebalance
    
    mu      sync.RWMutex
    dataDir string
}

type Member struct {
    MemberID       string
    ClientID       string
    Host           string
    JoinedAt       time.Time
    LastHeartbeat  time.Time
    Assignments    []TopicPartition  // Assigned partitions
}

type TopicPartition struct {
    Topic       string `json:"topic"`
    PartitionID int32  `json:"partition_id"`
}

func NewConsumerGroup(groupID string, dataDir string) (*ConsumerGroup, error) {
    groupDir := filepath.Join(dataDir, "consumer-groups", groupID)
    if err := os.MkdirAll(groupDir, 0755); err != nil {
        return nil, err
    }
    
    cg := &ConsumerGroup{
        GroupID:    groupID,
        Members:    make(map[string]*Member),
        Offsets:    make(map[string]int64),
        State:      "Empty",
        Generation: 0,
        dataDir:    groupDir,
    }
    
    // Load existing state
    cg.loadState()
    
    return cg, nil
}

func (cg *ConsumerGroup) JoinGroup(memberID, clientID, host string) error {
    cg.mu.Lock()
    defer cg.mu.Unlock()
    
    member := &Member{
        MemberID:      memberID,
        ClientID:      clientID,
        Host:          host,
        JoinedAt:      time.Now(),
        LastHeartbeat: time.Now(),
        Assignments:   make([]TopicPartition, 0),
    }
    
    cg.Members[memberID] = member
    
    // First member becomes leader
    if len(cg.Members) == 1 {
        cg.Leader = memberID
    }
    
    // Trigger rebalance
    cg.State = "Rebalancing"
    
    // Save state
    cg.saveState()
    
    return nil
}

func (cg *ConsumerGroup) LeaveGroup(memberID string) error {
    cg.mu.Lock()
    defer cg.mu.Unlock()
    
    delete(cg.Members, memberID)
    
    // If leader left, elect new leader
    if cg.Leader == memberID && len(cg.Members) > 0 {
        for id := range cg.Members {
            cg.Leader = id
            break
        }
    }
    
    // Trigger rebalance
    if len(cg.Members) > 0 {
        cg.State = "Rebalancing"
    } else {
        cg.State = "Empty"
    }
    
    cg.saveState()
    
    return nil
}

func (cg *ConsumerGroup) Heartbeat(memberID string) error {
    cg.mu.Lock()
    defer cg.mu.Unlock()
    
    member, exists := cg.Members[memberID]
    if !exists {
        return fmt.Errorf("member %s not in group", memberID)
    }
    
    member.LastHeartbeat = time.Now()
    
    return nil
}

func (cg *ConsumerGroup) CommitOffset(topic string, partitionID int32, offset int64) error {
    cg.mu.Lock()
    defer cg.mu.Unlock()
    
    key := fmt.Sprintf("%s:%d", topic, partitionID)
    cg.Offsets[key] = offset
    
    // Save offsets to disk
    return cg.saveOffsets()
}

func (cg *ConsumerGroup) GetOffset(topic string, partitionID int32) int64 {
    cg.mu.RLock()
    defer cg.mu.RUnlock()
    
    key := fmt.Sprintf("%s:%d", topic, partitionID)
    offset, exists := cg.Offsets[key]
    if !exists {
        return -1 // Start from beginning
    }
    
    return offset
}

func (cg *ConsumerGroup) AssignPartitions(partitions []TopicPartition) error {
    cg.mu.Lock()
    defer cg.mu.Unlock()
    
    // Simple round-robin assignment
    members := make([]*Member, 0, len(cg.Members))
    for _, member := range cg.Members {
        members = append(members, member)
    }
    
    if len(members) == 0 {
        return fmt.Errorf("no members in group")
    }
    
    // Clear existing assignments
    for _, member := range members {
        member.Assignments = make([]TopicPartition, 0)
    }
    
    // Assign partitions
    for i, partition := range partitions {
        memberIdx := i % len(members)
        members[memberIdx].Assignments = append(members[memberIdx].Assignments, partition)
    }
    
    cg.State = "Stable"
    cg.Generation++
    
    cg.saveState()
    
    return nil
}

func (cg *ConsumerGroup) GetAssignments(memberID string) ([]TopicPartition, error) {
    cg.mu.RLock()
    defer cg.mu.RUnlock()
    
    member, exists := cg.Members[memberID]
    if !exists {
        return nil, fmt.Errorf("member %s not in group", memberID)
    }
    
    return member.Assignments, nil
}

func (cg *ConsumerGroup) CheckHeartbeats(timeout time.Duration) {
    cg.mu.Lock()
    defer cg.mu.Unlock()
    
    now := time.Now()
    deadMembers := make([]string, 0)
    
    for memberID, member := range cg.Members {
        if now.Sub(member.LastHeartbeat) > timeout {
            deadMembers = append(deadMembers, memberID)
        }
    }
    
    // Remove dead members
    for _, memberID := range deadMembers {
        delete(cg.Members, memberID)
    }
    
    // Trigger rebalance if members died
    if len(deadMembers) > 0 && len(cg.Members) > 0 {
        cg.State = "Rebalancing"
    }
}

func (cg *ConsumerGroup) saveState() error {
    data, err := json.MarshalIndent(cg, "", "  ")
    if err != nil {
        return err
    }
    
    path := filepath.Join(cg.dataDir, "members.json")
    return os.WriteFile(path, data, 0644)
}

func (cg *ConsumerGroup) loadState() error {
    path := filepath.Join(cg.dataDir, "members.json")
    data, err := os.ReadFile(path)
    if err != nil {
        if os.IsNotExist(err) {
            return nil // No state yet
        }
        return err
    }
    
    return json.Unmarshal(data, cg)
}

func (cg *ConsumerGroup) saveOffsets() error {
    data, err := json.MarshalIndent(cg.Offsets, "", "  ")
    if err != nil {
        return err
    }
    
    path := filepath.Join(cg.dataDir, "offsets.json")
    return os.WriteFile(path, data, 0644)
}

func (cg *ConsumerGroup) loadOffsets() error {
    path := filepath.Join(cg.dataDir, "offsets.json")
    data, err := os.ReadFile(path)
    if err != nil {
        if os.IsNotExist(err) {
            return nil
        }
        return err
    }
    
    return json.Unmarshal(data, &cg.Offsets)
}
```

---

## Producer API

### HTTP/gRPC Endpoints

**Produce Messages:**

```
POST /api/v1/produce
```

**Request:**
```json
{
  "topic": "orders",
  "partition": 0,
  "messages": [
    {
      "key": "user123",
      "value": {
        "order_id": "order-789",
        "amount": 99.99
      },
      "headers": {
        "source": "checkout-service"
      }
    },
    {
      "key": "user456",
      "value": {
        "order_id": "order-790",
        "amount": 149.99
      }
    }
  ]
}
```

**Response:**
```json
{
  "success": true,
  "partition": 0,
  "offsets": [12345, 12346],
  "timestamp": "2026-05-12T10:00:00Z"
}
```

### Handler Implementation

```go
func (h *WorkerHandler) handleProduce(w http.ResponseWriter, r *http.Request) {
    var req struct {
        Topic     string    `json:"topic"`
        Partition int32     `json:"partition"`
        Messages  []struct {
            Key     string            `json:"key"`
            Value   json.RawMessage   `json:"value"`
            Headers map[string]string `json:"headers"`
        } `json:"messages"`
    }
    
    if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
        h.respondError(w, http.StatusBadRequest, "invalid_json", err.Error())
        return
    }
    
    // Get partition log
    partitionLog, err := h.partitionManager.GetPartition(req.Topic, req.Partition)
    if err != nil {
        h.respondError(w, http.StatusNotFound, "partition_not_found", err.Error())
        return
    }
    
    // Check if this broker is the leader for this partition
    if !h.isPartitionLeader(req.Topic, req.Partition) {
        leader := h.getPartitionLeader(req.Topic, req.Partition)
        h.respondError(w, http.StatusTemporaryRedirect, "not_partition_leader", 
            fmt.Sprintf("Leader is %s", leader))
        return
    }
    
    // Convert to internal messages
    messages := make([]*partition.Message, len(req.Messages))
    for i, msg := range req.Messages {
        messages[i] = &partition.Message{
            Key:     msg.Key,
            Value:   []byte(msg.Value),
            Headers: msg.Headers,
        }
    }
    
    // Append to partition log
    offsets, err := partitionLog.Append(messages)
    if err != nil {
        h.respondError(w, http.StatusInternalServerError, "append_failed", err.Error())
        return
    }
    
    // TODO: Replicate to followers (ISR)
    
    h.respondJSON(w, http.StatusOK, map[string]interface{}{
        "success":   true,
        "partition": req.Partition,
        "offsets":   offsets,
        "timestamp": time.Now(),
    })
}
```

---

## Consumer API

### HTTP/gRPC Endpoints

**Fetch Messages:**

```
GET /api/v1/consume?topic=orders&partition=0&offset=12345&max_messages=100
```

**Response:**
```json
{
  "topic": "orders",
  "partition": 0,
  "messages": [
    {
      "offset": 12345,
      "timestamp": "2026-05-12T10:00:00Z",
      "key": "user123",
      "value": {
        "order_id": "order-789",
        "amount": 99.99
      },
      "headers": {
        "source": "checkout-service"
      }
    }
  ],
  "high_watermark": 12500
}
```

**Consumer Group Operations:**

```
POST /api/v1/consumer-groups/{group_id}/join
POST /api/v1/consumer-groups/{group_id}/leave
POST /api/v1/consumer-groups/{group_id}/heartbeat
POST /api/v1/consumer-groups/{group_id}/commit
GET  /api/v1/consumer-groups/{group_id}/offsets
```

### Consumer Group Join

**Request:**
```json
POST /api/v1/consumer-groups/checkout-service/join

{
  "member_id": "consumer-1",
  "client_id": "checkout-app",
  "host": "10.0.1.100",
  "topics": ["orders", "payments"]
}
```

**Response:**
```json
{
  "success": true,
  "member_id": "consumer-1",
  "generation": 5,
  "leader": "consumer-1",
  "assignments": [
    {"topic": "orders", "partition_id": 0},
    {"topic": "orders", "partition_id": 1},
    {"topic": "payments", "partition_id": 0}
  ]
}
```

### Commit Offset

**Request:**
```json
POST /api/v1/consumer-groups/checkout-service/commit

{
  "member_id": "consumer-1",
  "offsets": [
    {"topic": "orders", "partition_id": 0, "offset": 12500},
    {"topic": "orders", "partition_id": 1, "offset": 8900}
  ]
}
```

**Response:**
```json
{
  "success": true
}
```

### Handler Implementation

```go
func (h *WorkerHandler) handleConsume(w http.ResponseWriter, r *http.Request) {
    topic := r.URL.Query().Get("topic")
    partition := int32(0)
    fmt.Sscanf(r.URL.Query().Get("partition"), "%d", &partition)
    
    offset := int64(-1)
    fmt.Sscanf(r.URL.Query().Get("offset"), "%d", &offset)
    
    maxMessages := 100
    fmt.Sscanf(r.URL.Query().Get("max_messages"), "%d", &maxMessages)
    
    maxBytes := int64(1048576) // 1MB
    fmt.Sscanf(r.URL.Query().Get("max_bytes"), "%d", &maxBytes)
    
    // Get partition log
    partitionLog, err := h.partitionManager.GetPartition(topic, partition)
    if err != nil {
        h.respondError(w, http.StatusNotFound, "partition_not_found", err.Error())
        return
    }
    
    // If offset is -1, start from beginning
    if offset == -1 {
        offset = partitionLog.GetLogStartOffset()
    }
    
    // Read messages
    messages, err := partitionLog.Read(offset, maxMessages, maxBytes)
    if err != nil {
        h.respondError(w, http.StatusInternalServerError, "read_failed", err.Error())
        return
    }
    
    // Convert to response format
    responseMessages := make([]map[string]interface{}, len(messages))
    for i, msg := range messages {
        var value interface{}
        json.Unmarshal(msg.Value, &value)
        
        responseMessages[i] = map[string]interface{}{
            "offset":    msg.Offset,
            "timestamp": msg.Timestamp,
            "key":       msg.Key,
            "value":     value,
            "headers":   msg.Headers,
        }
    }
    
    h.respondJSON(w, http.StatusOK, map[string]interface{}{
        "topic":          topic,
        "partition":      partition,
        "messages":       responseMessages,
        "high_watermark": partitionLog.GetHighWatermark(),
    })
}

func (h *WorkerHandler) handleConsumerGroupJoin(w http.ResponseWriter, r *http.Request) {
    vars := mux.Vars(r)
    groupID := vars["group_id"]
    
    var req struct {
        MemberID string   `json:"member_id"`
        ClientID string   `json:"client_id"`
        Host     string   `json:"host"`
        Topics   []string `json:"topics"`
    }
    
    if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
        h.respondError(w, http.StatusBadRequest, "invalid_json", err.Error())
        return
    }
    
    // Get or create consumer group
    group, err := h.consumerGroupManager.GetOrCreateGroup(groupID)
    if err != nil {
        h.respondError(w, http.StatusInternalServerError, "group_error", err.Error())
        return
    }
    
    // Join group
    if err := group.JoinGroup(req.MemberID, req.ClientID, req.Host); err != nil {
        h.respondError(w, http.StatusInternalServerError, "join_failed", err.Error())
        return
    }
    
    // Trigger partition assignment
    partitions := h.getPartitionsForTopics(req.Topics)
    if err := group.AssignPartitions(partitions); err != nil {
        h.respondError(w, http.StatusInternalServerError, "assignment_failed", err.Error())
        return
    }
    
    // Get assignments for this member
    assignments, _ := group.GetAssignments(req.MemberID)
    
    h.respondJSON(w, http.StatusOK, map[string]interface{}{
        "success":     true,
        "member_id":   req.MemberID,
        "generation":  group.Generation,
        "leader":      group.Leader,
        "assignments": assignments,
    })
}

func (h *WorkerHandler) handleCommitOffset(w http.ResponseWriter, r *http.Request) {
    vars := mux.Vars(r)
    groupID := vars["group_id"]
    
    var req struct {
        MemberID string `json:"member_id"`
        Offsets  []struct {
            Topic       string `json:"topic"`
            PartitionID int32  `json:"partition_id"`
            Offset      int64  `json:"offset"`
        } `json:"offsets"`
    }
    
    if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
        h.respondError(w, http.StatusBadRequest, "invalid_json", err.Error())
        return
    }
    
    // Get consumer group
    group, err := h.consumerGroupManager.GetGroup(groupID)
    if err != nil {
        h.respondError(w, http.StatusNotFound, "group_not_found", err.Error())
        return
    }
    
    // Commit offsets
    for _, offset := range req.Offsets {
        if err := group.CommitOffset(offset.Topic, offset.PartitionID, offset.Offset); err != nil {
            h.respondError(w, http.StatusInternalServerError, "commit_failed", err.Error())
            return
        }
    }
    
    h.respondJSON(w, http.StatusOK, map[string]interface{}{
        "success": true,
    })
}
```

---

## Partition Manager

Manages all partitions on a worker node:

```go
package worker

type PartitionManager struct {
    mu         sync.RWMutex
    partitions map[string]*partition.PartitionLog // "topic:partition" -> log
    dataDir    string
    metadata   *MetadataCache
}

func NewPartitionManager(dataDir string, metadata *MetadataCache) *PartitionManager {
    return &PartitionManager{
        partitions: make(map[string]*partition.PartitionLog),
        dataDir:    dataDir,
        metadata:   metadata,
    }
}

func (pm *PartitionManager) GetPartition(topic string, partitionID int32) (*partition.PartitionLog, error) {
    key := fmt.Sprintf("%s:%d", topic, partitionID)
    
    pm.mu.RLock()
    pl, exists := pm.partitions[key]
    pm.mu.RUnlock()
    
    if exists {
        return pl, nil
    }
    
    // Create partition if assigned to this broker
    pm.mu.Lock()
    defer pm.mu.Unlock()
    
    // Double-check after acquiring write lock
    if pl, exists := pm.partitions[key]; exists {
        return pl, nil
    }
    
    // Get partition config from metadata
    config := pm.metadata.GetPartitionConfig(topic, partitionID)
    
    // Create new partition log
    pl, err := partition.NewPartitionLog(topic, partitionID, pm.dataDir, config)
    if err != nil {
        return nil, err
    }
    
    pm.partitions[key] = pl
    
    return pl, nil
}

func (pm *PartitionManager) Close() error {
    pm.mu.Lock()
    defer pm.mu.Unlock()
    
    for _, pl := range pm.partitions {
        pl.Close()
    }
    
    return nil
}

func (pm *PartitionManager) RunCleanup() {
    ticker := time.NewTicker(5 * time.Minute)
    defer ticker.Stop()
    
    for range ticker.C {
        pm.mu.RLock()
        partitions := make([]*partition.PartitionLog, 0, len(pm.partitions))
        for _, pl := range pm.partitions {
            partitions = append(partitions, pl)
        }
        pm.mu.RUnlock()
        
        for _, pl := range partitions {
            if err := pl.Cleanup(); err != nil {
                log.Printf("Error cleaning up partition: %v", err)
            }
        }
    }
}
```

---

## Complete Example Flow

### Producer Flow

```
1. Client sends produce request to any broker
   POST /api/v1/produce
   {
     "topic": "orders",
     "partition": 0,
     "messages": [...]
   }

2. Broker checks if it's the partition leader
   - If not, return 307 redirect to leader
   - If yes, proceed

3. Get partition log
   - PartitionManager.GetPartition("orders", 0)
   - Returns PartitionLog instance

4. Append messages to log
   - Assigns sequential offsets
   - Writes to active segment file
   - Syncs to disk

5. Replicate to followers (ISR)
   - Send messages to replica brokers
   - Wait for acknowledgment
   - Update high watermark

6. Return offsets to client
   {
     "success": true,
     "offsets": [12345, 12346]
   }
```

### Consumer Flow

```
1. Client joins consumer group (first time)
   POST /api/v1/consumer-groups/my-group/join
   {
     "member_id": "consumer-1",
     "topics": ["orders"]
   }

2. Broker assigns partitions
   - Round-robin across group members
   - Returns assignments

3. Client fetches messages
   GET /api/v1/consume?topic=orders&partition=0&offset=12345

4. Broker reads from partition log
   - PartitionLog.Read(12345, 100, 1MB)
   - Returns messages

5. Client processes messages

6. Client commits offset
   POST /api/v1/consumer-groups/my-group/commit
   {
     "offsets": [
       {"topic": "orders", "partition_id": 0, "offset": 12400}
     ]
   }

7. Broker saves offset
   - ConsumerGroup.CommitOffset("orders", 0, 12400)
   - Persists to disk
```

---

## Configuration

### Partition Config

```json
{
  "segment_bytes": 1073741824,
  "retention_ms": 604800000,
  "retention_bytes": 10737418240,
  "index_interval_bytes": 4096,
  "flush_messages": 10000,
  "flush_ms": 1000
}
```

### Worker Config

```json
{
  "broker_id": "broker-1",
  "host": "192.168.1.101",
  "port": 9092,
  "data_dir": "./data/broker-1",
  "log_dirs": ["./data/broker-1/partitions"],
  "num_io_threads": 8,
  "num_network_threads": 3,
  "socket_send_buffer_bytes": 102400,
  "socket_receive_buffer_bytes": 102400,
  "socket_request_max_bytes": 104857600
}
```

---

## Summary

**Key Components:**

1. **Partition Log**: Segment-based storage with indexes
2. **Consumer Groups**: Offset tracking and partition assignment
3. **Producer API**: Write messages to partition leader
4. **Consumer API**: Read messages, commit offsets
5. **Partition Manager**: Manages all local partitions

**File Structure:**
- Segment files (.log) for messages
- Index files (.index) for fast lookups
- Consumer group offsets (JSON)
- Partition metadata

**Operations:**
- Produce: Append to log, replicate, return offset
- Consume: Read from log, respect consumer group offsets
- Commit: Save consumer progress
- Cleanup: Delete old segments based on retention

**Next Steps:**
1. Implement partition log with segments
2. Add consumer group manager
3. Create producer/consumer APIs
4. Add replication (ISR)
5. Implement retention cleanup
6. Add monitoring/metrics

This gives you a working Kafka-like message queue with persistence, consumer groups, and offset management!
