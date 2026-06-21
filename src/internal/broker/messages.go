package broker

import "github.com/yourusername/amyqueue/src/internal/metadata"

// LeaderAndISRRequest is sent by the controller to a broker whenever partition
// leadership or ISR membership changes. The broker updates its local
// PartitionStateCache and starts/stops replication accordingly.
type LeaderAndISRRequest struct {
	ControllerEpoch int64                `json:"controller_epoch"`
	Partitions      []PartitionUpdate    `json:"partitions"`
}

// PartitionUpdate carries the full partition state for one partition.
type PartitionUpdate struct {
	TopicID         metadata.TopicID      `json:"topic_id"`
	TopicName       string                `json:"topic_name"`
	PartitionID     int32                 `json:"partition_id"`
	Leader          metadata.BrokerID     `json:"leader"`
	LeaderEpoch     int32                 `json:"leader_epoch"`
	ISR             []metadata.BrokerID   `json:"isr"`
	Replicas        []metadata.BrokerID   `json:"replicas"`
	PartitionEpoch  int32                 `json:"partition_epoch"`
}

// LeaderAndISRResponse is the broker's reply to a LeaderAndISRRequest.
type LeaderAndISRResponse struct {
	Results []PartitionResult `json:"results"`
}

// PartitionResult reports the outcome of applying one PartitionUpdate.
type PartitionResult struct {
	TopicID     metadata.TopicID `json:"topic_id"`
	PartitionID int32            `json:"partition_id"`
	Err         string           `json:"err,omitempty"` // empty = success
}
