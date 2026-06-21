package metadata

// BrokerStatus is the durable state written to the Raft log.
// Liveness (alive/dead) is tracked separately in LivenessTracker (Phase 5) —
// a broker can be status=active and alive=false (crashed) simultaneously.
type BrokerStatus string

const (
	BrokerStatusActive       BrokerStatus = "active"
	BrokerStatusShuttingDown BrokerStatus = "shutting_down"
)

// BrokerInfo is the durable record for a registered broker.
// Epoch is the fencing token: only commands carrying the current epoch are accepted.
// It is assigned and incremented exclusively inside applyRegisterBroker.
type BrokerInfo struct {
	BrokerID BrokerID
	Host     string
	Port     int32
	RackID   string
	Epoch    int64
	Status   BrokerStatus
}

// TopicID is the canonical UUID-based key for a topic.
// Distinct from Name so renaming a topic only touches the name index.
type TopicID string

// BrokerID identifies a broker in the cluster.
type BrokerID string

// PartitionStatus is the authoritative state of a partition.
// Never infer state from Leader == "" alone — always read Status.
type PartitionStatus string

const (
	PartitionOnline  PartitionStatus = "online"
	PartitionOffline PartitionStatus = "offline"
)

// PartitionKey is a comparable struct used directly as a map key with no allocation.
type PartitionKey struct {
	TopicID     TopicID
	PartitionID int32
}

type Topic struct {
	TopicID           TopicID
	Name              string
	NumPartitions     int32
	ReplicationFactor int32 // desired RF; actual may be lower if fewer brokers were available at birth
	Internal          bool  // true for system topics; user cannot delete
	Config            TopicConfig
}

type TopicConfig struct {
	RetentionMs     int64  // -1 = unlimited
	RetentionBytes  int64  // -1 = unlimited
	SegmentBytes    int64
	MinISR          int32  // minimum ISR size to accept writes with acks=all
	MaxMessageBytes int32
	CleanupPolicy   string // "delete" | "compact"
}

type PartitionState struct {
	TopicID        TopicID
	PartitionID    int32
	Status         PartitionStatus
	Replicas       []BrokerID // ordered; Replicas[0] = preferred leader (never changes)
	ISR            []BrokerID // in-sync replicas; subset of Replicas
	Leader         BrokerID   // empty string when Status == Offline
	LeaderEpoch    int32      // increments on every leader change
	PartitionEpoch int32      // increments on every state update
}

// --- Command payloads ---
// These travel through the Raft log and are applied by the state machine.

type ClusterInitPayload struct {
	ClusterID string // UUID; generated at propose time, never inside Apply
}

type CreateTopicPayload struct {
	TopicID           TopicID
	Name              string
	NumPartitions     int32
	ReplicationFactor int32
	Internal          bool
	Config            TopicConfig
	Brokers           []BrokerID // live brokers available at proposal time
}

type DeleteTopicPayload struct {
	TopicID TopicID // always UUID; safe to replay even after name reuse
}

type UpdatePartitionPayload struct {
	Key           PartitionKey
	NewLeader     BrokerID
	NewISR        []BrokerID
	ExpectedEpoch int32  // CAS guard — current PartitionEpoch at propose time
	Reason        string // "leader_death" | "isr_shrink" | "isr_expand" | "unclean_election"
}
