package metadata

import (
	"sync"
	"time"
)

// replicaKey identifies a specific replica's offset record.
type replicaKey struct {
	broker      BrokerID
	topicID     TopicID
	partitionID int32
}

// LivenessTracker tracks which registered brokers are currently alive based on
// heartbeat timestamps. This state is purely ephemeral — it is never written
// to the Raft log. On leader failover the new leader starts fresh; brokers
// re-establish liveness within one heartbeat interval.
//
// Only the controller leader tracks liveness. Followers maintain the struct
// but never act on it.
type LivenessTracker struct {
	mu          sync.RWMutex
	lastSeen    map[BrokerID]time.Time
	replicaLEOs map[replicaKey]int64 // per-replica LogEndOffset, updated from heartbeats
	sessionMs   int                  // broker is considered dead after sessionMs without a heartbeat
}

// PartitionOffset carries a single replica's LogEndOffset for one partition.
// Reported by the broker in each heartbeat payload.
type PartitionOffset struct {
	TopicID     TopicID `json:"topic_id"`
	PartitionID int32   `json:"partition_id"`
	LEO         int64   `json:"leo"` // LogEndOffset — next offset to be written
}

// BrokerHeartbeat is the payload sent by a broker on each heartbeat.
type BrokerHeartbeat struct {
	BrokerID        BrokerID          `json:"broker_id"`
	Epoch           int64             `json:"epoch"`
	MetadataVersion int64             `json:"metadata_version"`
	Offsets         []PartitionOffset `json:"offsets,omitempty"` // Phase 7: per-partition LEO
}

// HeartbeatResponse is returned to the broker with the controller's current
// metadata version so the broker can detect stale local state.
type HeartbeatResponse struct {
	CurrentMetadataVersion int64 `json:"current_metadata_version"`
	// StaleEpoch is true when the broker's epoch doesn't match the stored epoch.
	// The broker should re-register to obtain a fresh epoch.
	StaleEpoch bool `json:"stale_epoch,omitempty"`
}

func NewLivenessTracker(sessionTimeoutMs int) *LivenessTracker {
	return &LivenessTracker{
		lastSeen:    make(map[BrokerID]time.Time),
		replicaLEOs: make(map[replicaKey]int64),
		sessionMs:   sessionTimeoutMs,
	}
}

// RecordHeartbeat records that brokerID is alive as of now and updates its
// per-partition LogEndOffsets. The controller always uses its own clock.
func (t *LivenessTracker) RecordHeartbeat(id BrokerID, offsets []PartitionOffset) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.lastSeen[id] = time.Now()
	for _, o := range offsets {
		t.replicaLEOs[replicaKey{broker: id, topicID: o.TopicID, partitionID: o.PartitionID}] = o.LEO
	}
}

// ReplicaLEO returns the last known LogEndOffset for a specific broker+partition.
// Returns 0 if no offset has been reported yet.
func (t *LivenessTracker) ReplicaLEO(brokerID BrokerID, topicID TopicID, partitionID int32) int64 {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.replicaLEOs[replicaKey{broker: brokerID, topicID: topicID, partitionID: partitionID}]
}

// LaggingReplicas returns the set of replicas whose LEO is more than maxLag
// behind the leader's LEO for the given partition. Used for ISR shrink decisions.
func (t *LivenessTracker) LaggingReplicas(
	topicID TopicID,
	partitionID int32,
	leaderID BrokerID,
	allReplicas []BrokerID,
	maxLag int64,
) []BrokerID {
	t.mu.RLock()
	defer t.mu.RUnlock()

	leaderLEO := t.replicaLEOs[replicaKey{broker: leaderID, topicID: topicID, partitionID: partitionID}]

	var lagging []BrokerID
	for _, replica := range allReplicas {
		if replica == leaderID {
			continue
		}
		replicaLEO := t.replicaLEOs[replicaKey{broker: replica, topicID: topicID, partitionID: partitionID}]
		if leaderLEO-replicaLEO > maxLag {
			lagging = append(lagging, replica)
		}
	}
	return lagging
}

// IsAlive returns true if brokerID sent a heartbeat within the session timeout.
func (t *LivenessTracker) IsAlive(id BrokerID) bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	ts, ok := t.lastSeen[id]
	if !ok {
		return false
	}
	return time.Since(ts) < time.Duration(t.sessionMs)*time.Millisecond
}

// AliveBrokers returns the IDs of all brokers that have heartbeated within the
// session timeout window.
func (t *LivenessTracker) AliveBrokers() []BrokerID {
	t.mu.RLock()
	defer t.mu.RUnlock()
	cutoff := time.Now().Add(-time.Duration(t.sessionMs) * time.Millisecond)
	var alive []BrokerID
	for id, ts := range t.lastSeen {
		if ts.After(cutoff) {
			alive = append(alive, id)
		}
	}
	return alive
}

// LastHeartbeatMs returns how many milliseconds ago brokerID last heartbeated.
// Returns -1 if the broker has never heartbeated.
func (t *LivenessTracker) LastHeartbeatMs(id BrokerID) int64 {
	t.mu.RLock()
	defer t.mu.RUnlock()
	ts, ok := t.lastSeen[id]
	if !ok {
		return -1
	}
	return time.Since(ts).Milliseconds()
}

// StartSweep runs a background goroutine that periodically checks for dead
// brokers and calls onDead for each one. The goroutine stops when stopC is closed.
func (t *LivenessTracker) StartSweep(sweepIntervalMs int, stopC <-chan struct{}, onDead func(BrokerID)) {
	go func() {
		ticker := time.NewTicker(time.Duration(sweepIntervalMs) * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stopC:
				return
			case <-ticker.C:
				for _, id := range t.deadBrokers() {
					onDead(id)
				}
			}
		}
	}()
}

// deadBrokers returns IDs of brokers that have missed the session timeout but
// were previously seen (so we don't fire for brokers that never heartbeated).
func (t *LivenessTracker) deadBrokers() []BrokerID {
	t.mu.RLock()
	defer t.mu.RUnlock()
	cutoff := time.Now().Add(-time.Duration(t.sessionMs) * time.Millisecond)
	var dead []BrokerID
	for id, ts := range t.lastSeen {
		if ts.Before(cutoff) {
			dead = append(dead, id)
		}
	}
	return dead
}

// Remove deletes the liveness record for a broker (e.g. after controlled shutdown).
func (t *LivenessTracker) Remove(id BrokerID) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.lastSeen, id)
}
