package metadata

import (
	"errors"
	"hash/fnv"
)

// ErrNoCoordinator is returned when no partition has a live leader for the
// computed coordinator partition (e.g. __consumer_offsets not yet created).
var ErrNoCoordinator = errors.New("no coordinator available for group")

// ConsumerOffsetsTopicName is the internal topic used to anchor group coordinators.
const ConsumerOffsetsTopicName = "__consumer_offsets"

// FindCoordinator returns the BrokerInfo of the broker that should coordinate
// the given consumer group. The algorithm matches Kafka KRaft:
//
//	abs(fnv32(groupID)) % numPartitions(__consumer_offsets) → partition leader
//
// The __consumer_offsets topic must exist (created via ClusterInit) and have
// at least one partition with a live leader for this function to succeed.
func FindCoordinator(groupID string, store Store) (*BrokerInfo, error) {
	topic, ok := store.GetTopicByName(ConsumerOffsetsTopicName)
	if !ok {
		return nil, ErrNoCoordinator
	}
	if topic.NumPartitions <= 0 {
		return nil, ErrNoCoordinator
	}

	h := fnv.New32a()
	_, _ = h.Write([]byte(groupID))
	hash := int64(h.Sum32())
	if hash < 0 {
		hash = -hash
	}
	targetPartition := int32(hash % int64(topic.NumPartitions))

	p, ok := store.GetPartition(PartitionKey{TopicID: topic.TopicID, PartitionID: targetPartition})
	if !ok || p.Leader == "" {
		return nil, ErrNoCoordinator
	}

	b, ok := store.GetBroker(p.Leader)
	if !ok {
		return nil, ErrNoCoordinator
	}
	return b, nil
}

// ConsumerOffsetsConfig holds the configuration for auto-creating __consumer_offsets.
type ConsumerOffsetsConfig struct {
	NumPartitions     int32
	ReplicationFactor int32
}

// DefaultConsumerOffsetsConfig returns sensible defaults matching Kafka's defaults.
func DefaultConsumerOffsetsConfig() ConsumerOffsetsConfig {
	return ConsumerOffsetsConfig{
		NumPartitions:     50,
		ReplicationFactor: 3,
	}
}
