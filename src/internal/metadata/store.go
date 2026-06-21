package metadata

import (
	"log"
	"sync"
)

// Store is the metadata state machine interface.
// Apply* methods are called by the Raft FSM under the Raft commit lock.
// Read methods are safe to call concurrently from any goroutine.
//
// Phase 4 will replace InMemoryStore with a disk-backed implementation
// by swapping the concrete type behind this interface — no call sites change.
type Store interface {
	// Write — called by Raft FSM; each is idempotent and safe to replay
	ApplyClusterInit(payload ClusterInitPayload) error
	ApplyCreateTopic(payload CreateTopicPayload) error
	ApplyDeleteTopic(payload DeleteTopicPayload) error
	ApplyUpdatePartition(payload UpdatePartitionPayload) error
	ApplyRegisterBroker(payload RegisterBrokerPayload) (epoch int64, err error)
	ApplyShutdownBroker(payload ShutdownBrokerPayload) error

	// Read
	GetTopic(id TopicID) (*Topic, bool)
	GetTopicByName(name string) (*Topic, bool)
	ListTopics() []*Topic
	GetPartition(key PartitionKey) (*PartitionState, bool)
	ListPartitions(topicID TopicID) []*PartitionState
	LeaderCounts() map[BrokerID]int
	Version() int64
	ClusterID() string

	// Broker reads
	GetBroker(id BrokerID) (*BrokerInfo, bool)
	ListBrokers() []*BrokerInfo
	ListActiveBrokers() []*BrokerInfo
}

// InMemoryStore is the in-memory implementation of Store.
// All state lives in Go maps under a single RWMutex.
type InMemoryStore struct {
	mu           sync.RWMutex
	clusterID    string
	topics       map[TopicID]*Topic
	topicsByName map[string]TopicID
	partitions   map[PartitionKey]*PartitionState
	brokers      map[BrokerID]*BrokerInfo
	version      int64
}

func NewInMemoryStore() *InMemoryStore {
	return &InMemoryStore{
		topics:       make(map[TopicID]*Topic),
		topicsByName: make(map[string]TopicID),
		partitions:   make(map[PartitionKey]*PartitionState),
		brokers:      make(map[BrokerID]*BrokerInfo),
	}
}

// --- Write operations ---

func (s *InMemoryStore) ApplyClusterInit(payload ClusterInitPayload) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.clusterID != "" {
		return nil // idempotent: already initialised
	}
	s.clusterID = payload.ClusterID
	s.version++
	return nil
}

func (s *InMemoryStore) ApplyCreateTopic(payload CreateTopicPayload) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.topicsByName[payload.Name]; exists {
		log.Printf("metadata: ApplyCreateTopic: topic %q already exists, skipping (idempotent replay)", payload.Name)
		return nil
	}

	leaderCounts := s.leaderCountsLocked()
	assignments := AssignReplicas(payload.Brokers, payload.NumPartitions, payload.ReplicationFactor, leaderCounts)

	topic := &Topic{
		TopicID:           payload.TopicID,
		Name:              payload.Name,
		NumPartitions:     payload.NumPartitions,
		ReplicationFactor: payload.ReplicationFactor,
		Internal:          payload.Internal,
		Config:            payload.Config,
	}
	s.topics[payload.TopicID] = topic
	s.topicsByName[payload.Name] = payload.TopicID

	for i := int32(0); i < payload.NumPartitions; i++ {
		key := PartitionKey{TopicID: payload.TopicID, PartitionID: i}
		var replicas []BrokerID
		if assignments != nil && int(i) < len(assignments) {
			replicas = assignments[i]
		}

		state := &PartitionState{
			TopicID:     payload.TopicID,
			PartitionID: i,
			Replicas:    replicas,
		}

		if len(replicas) == 0 {
			state.Status = PartitionOffline
			state.Leader = ""
			state.ISR = nil
		} else {
			state.Status = PartitionOnline
			state.Leader = replicas[0]
			state.ISR = copyBrokerIDs(replicas)
		}

		s.partitions[key] = state
	}

	s.version++
	return nil
}

// ApplyDeleteTopic removes partitions first (0..NumPartitions-1), then the topic record,
// then the name index — so a crash mid-delete can never leave orphaned partitions.
func (s *InMemoryStore) ApplyDeleteTopic(payload DeleteTopicPayload) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	topic, exists := s.topics[payload.TopicID]
	if !exists {
		return nil // idempotent: already gone
	}

	for i := int32(0); i < topic.NumPartitions; i++ {
		delete(s.partitions, PartitionKey{TopicID: payload.TopicID, PartitionID: i})
	}
	delete(s.topics, payload.TopicID)
	delete(s.topicsByName, topic.Name)

	s.version++
	return nil
}

// ApplyUpdatePartition is a compare-and-swap on PartitionEpoch.
// A stale ExpectedEpoch is a no-op (Raft replay safety).
func (s *InMemoryStore) ApplyUpdatePartition(payload UpdatePartitionPayload) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	state, exists := s.partitions[payload.Key]
	if !exists {
		log.Printf("metadata: ApplyUpdatePartition: partition %v not found, skipping", payload.Key)
		return nil
	}

	if payload.ExpectedEpoch != state.PartitionEpoch {
		log.Printf("metadata: ApplyUpdatePartition: stale epoch for %v (expected %d, current %d), skipping",
			payload.Key, payload.ExpectedEpoch, state.PartitionEpoch)
		return nil
	}

	prevLeader := state.Leader
	state.Leader = payload.NewLeader
	state.ISR = copyBrokerIDs(payload.NewISR)

	if payload.NewLeader == "" {
		state.Status = PartitionOffline
	} else {
		state.Status = PartitionOnline
	}

	if state.Leader != prevLeader {
		state.LeaderEpoch++
	}
	state.PartitionEpoch++

	s.version++
	return nil
}

// ApplyRegisterBroker upserts a broker record and returns the assigned epoch.
// Idempotency rules:
//   - New BrokerID:                epoch = 1
//   - Same BrokerID + same host:port: return current epoch (retry safety)
//   - Same BrokerID + diff host:port: epoch++, update address
func (s *InMemoryStore) ApplyRegisterBroker(payload RegisterBrokerPayload) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	existing, ok := s.brokers[payload.BrokerID]
	var epoch int64
	if !ok {
		epoch = 1
	} else if existing.Host == payload.Host && existing.Port == payload.Port {
		epoch = existing.Epoch // idempotent retry
	} else {
		log.Printf("metadata: broker %q re-registering from new address %s:%d (was %s:%d), epoch++",
			payload.BrokerID, payload.Host, payload.Port, existing.Host, existing.Port)
		epoch = existing.Epoch + 1
	}

	s.brokers[payload.BrokerID] = &BrokerInfo{
		BrokerID: payload.BrokerID,
		Host:     payload.Host,
		Port:     payload.Port,
		RackID:   payload.RackID,
		Epoch:    epoch,
		Status:   BrokerStatusActive,
	}
	s.version++
	return epoch, nil
}

// ApplyShutdownBroker marks a broker as shutting_down using a CAS on epoch.
// A stale ExpectedEpoch is a no-op (Raft replay safety).
func (s *InMemoryStore) ApplyShutdownBroker(payload ShutdownBrokerPayload) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	b, ok := s.brokers[payload.BrokerID]
	if !ok {
		log.Printf("metadata: ApplyShutdownBroker: broker %q not found, skipping", payload.BrokerID)
		return nil
	}
	if b.Epoch != payload.ExpectedEpoch {
		log.Printf("metadata: ApplyShutdownBroker: stale epoch for %q (expected %d, current %d), skipping",
			payload.BrokerID, payload.ExpectedEpoch, b.Epoch)
		return nil
	}
	b.Status = BrokerStatusShuttingDown
	s.version++
	return nil
}

// --- Read operations ---

func (s *InMemoryStore) GetTopic(id TopicID) (*Topic, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	t, ok := s.topics[id]
	if !ok {
		return nil, false
	}
	return copyTopic(t), true
}

func (s *InMemoryStore) GetTopicByName(name string) (*Topic, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	id, ok := s.topicsByName[name]
	if !ok {
		return nil, false
	}
	t := s.topics[id]
	return copyTopic(t), true
}

func (s *InMemoryStore) ListTopics() []*Topic {
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := make([]*Topic, 0, len(s.topics))
	for _, t := range s.topics {
		result = append(result, copyTopic(t))
	}
	return result
}

func (s *InMemoryStore) GetPartition(key PartitionKey) (*PartitionState, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	p, ok := s.partitions[key]
	if !ok {
		return nil, false
	}
	return copyPartitionState(p), true
}

func (s *InMemoryStore) ListPartitions(topicID TopicID) []*PartitionState {
	s.mu.RLock()
	defer s.mu.RUnlock()

	topic, ok := s.topics[topicID]
	if !ok {
		return nil
	}

	result := make([]*PartitionState, 0, topic.NumPartitions)
	for i := int32(0); i < topic.NumPartitions; i++ {
		if p, ok := s.partitions[PartitionKey{TopicID: topicID, PartitionID: i}]; ok {
			result = append(result, copyPartitionState(p))
		}
	}
	return result
}

func (s *InMemoryStore) LeaderCounts() map[BrokerID]int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.leaderCountsLocked()
}

func (s *InMemoryStore) Version() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.version
}

func (s *InMemoryStore) ClusterID() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.clusterID
}

func (s *InMemoryStore) GetBroker(id BrokerID) (*BrokerInfo, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	b, ok := s.brokers[id]
	if !ok {
		return nil, false
	}
	cp := *b
	return &cp, true
}

func (s *InMemoryStore) ListBrokers() []*BrokerInfo {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]*BrokerInfo, 0, len(s.brokers))
	for _, b := range s.brokers {
		cp := *b
		result = append(result, &cp)
	}
	return result
}

func (s *InMemoryStore) ListActiveBrokers() []*BrokerInfo {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var result []*BrokerInfo
	for _, b := range s.brokers {
		if b.Status == BrokerStatusActive {
			cp := *b
			result = append(result, &cp)
		}
	}
	return result
}

// leaderCountsLocked computes current leader counts from partitions.
// Caller must hold at least a read lock.
func (s *InMemoryStore) leaderCountsLocked() map[BrokerID]int {
	counts := make(map[BrokerID]int)
	for _, p := range s.partitions {
		if p.Status == PartitionOnline && p.Leader != "" {
			counts[p.Leader]++
		}
	}
	return counts
}

// --- Copy helpers (prevent callers from mutating internal state) ---

func copyTopic(t *Topic) *Topic {
	cp := *t
	return &cp
}

func copyPartitionState(p *PartitionState) *PartitionState {
	cp := *p
	cp.Replicas = copyBrokerIDs(p.Replicas)
	cp.ISR = copyBrokerIDs(p.ISR)
	return &cp
}

func copyBrokerIDs(src []BrokerID) []BrokerID {
	if src == nil {
		return nil
	}
	dst := make([]BrokerID, len(src))
	copy(dst, src)
	return dst
}
