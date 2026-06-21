package metadata

import (
	"encoding/json"
	"fmt"
)

// snapshotData is the serialised form of the full metadata state.
// Used to checkpoint the state machine so restart does not require replaying
// the entire Raft log from index 0.
type snapshotData struct {
	ClusterID  string                      `json:"cluster_id"`
	Topics     map[TopicID]*Topic          `json:"topics"`
	TopicNames map[string]TopicID          `json:"topic_names"`
	Partitions map[string]*PartitionState  `json:"partitions"` // key = "topicID/partitionID"
	Brokers    map[BrokerID]*BrokerInfo    `json:"brokers"`
	Version    int64                       `json:"version"`
}

// Snapshot serialises the state machine to a portable byte slice.
func Snapshot(store Store) ([]byte, error) {
	s, ok := store.(*InMemoryStore)
	if !ok {
		return nil, nil // unsupported store type — caller can skip snapshotting
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	snap := snapshotData{
		ClusterID:  s.clusterID,
		Topics:     make(map[TopicID]*Topic, len(s.topics)),
		TopicNames: make(map[string]TopicID, len(s.topicsByName)),
		Partitions: make(map[string]*PartitionState, len(s.partitions)),
		Brokers:    make(map[BrokerID]*BrokerInfo, len(s.brokers)),
		Version:    s.version,
	}

	for id, t := range s.topics {
		cp := *t
		snap.Topics[id] = &cp
	}
	for name, id := range s.topicsByName {
		snap.TopicNames[name] = id
	}
	for key, p := range s.partitions {
		cp := *p
		cp.Replicas = copyBrokerIDs(p.Replicas)
		cp.ISR = copyBrokerIDs(p.ISR)
		k := partitionKeyString(key)
		snap.Partitions[k] = &cp
	}
	for id, b := range s.brokers {
		cp := *b
		snap.Brokers[id] = &cp
	}

	return json.Marshal(snap)
}

// RestoreSnapshot deserialises a snapshot into a fresh InMemoryStore.
func RestoreSnapshot(data []byte) (*InMemoryStore, error) {
	var snap snapshotData
	if err := json.Unmarshal(data, &snap); err != nil {
		return nil, err
	}

	s := NewInMemoryStore()
	s.clusterID = snap.ClusterID
	s.version = snap.Version

	for id, t := range snap.Topics {
		s.topics[id] = t
	}
	for name, id := range snap.TopicNames {
		s.topicsByName[name] = id
	}
	for keyStr, p := range snap.Partitions {
		var key PartitionKey
		if _, err := partitionKeyFromString(keyStr, &key); err == nil {
			s.partitions[key] = p
		}
	}
	for id, b := range snap.Brokers {
		s.brokers[id] = b
	}

	return s, nil
}

func partitionKeyString(k PartitionKey) string {
	return string(k.TopicID) + "/" + int32Str(k.PartitionID)
}

func partitionKeyFromString(s string, key *PartitionKey) (int, error) {
	var topicID string
	var pid int32
	n, err := scanPartitionKey(s, &topicID, &pid)
	if err != nil {
		return 0, err
	}
	key.TopicID = TopicID(topicID)
	key.PartitionID = pid
	return n, nil
}

func int32Str(n int32) string {
	if n == 0 {
		return "0"
	}
	buf := make([]byte, 0, 12)
	if n < 0 {
		buf = append(buf, '-')
		n = -n
	}
	for n > 0 {
		buf = append([]byte{byte('0' + n%10)}, buf...)
		n /= 10
	}
	return string(buf)
}

func scanPartitionKey(s string, topicID *string, pid *int32) (int, error) {
	for i := len(s) - 1; i >= 0; i-- {
		if s[i] == '/' {
			*topicID = s[:i]
			_, err := fmt.Sscanf(s[i+1:], "%d", pid)
			return i, err
		}
	}
	return 0, fmt.Errorf("invalid partition key: %q", s)
}
