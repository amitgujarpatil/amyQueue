// Package admin implements the broker-side admin TCP server that receives
// LeaderAndISR pushes from the controller.
package admin

import (
	"encoding/json"
	"log/slog"
	"sync"

	"github.com/yourusername/amyqueue/src/internal/broker"
	"github.com/yourusername/amyqueue/src/internal/metadata"
)

// AdminService handles controller-pushed partition assignments on the broker.
// It maintains PartitionStateCache — the source of truth for which partitions
// this broker leads or follows, used for epoch fencing in future produce/consume.
type AdminService struct {
	mu     sync.RWMutex
	cache  map[metadata.PartitionKey]broker.PartitionAssignment
	logger *slog.Logger
}

func New(logger *slog.Logger) *AdminService {
	return &AdminService{
		cache:  make(map[metadata.PartitionKey]broker.PartitionAssignment),
		logger: logger,
	}
}

// HandleLeaderAndISR applies a LeaderAndISR push from the controller.
// Each PartitionUpdate is processed in order; stale PartitionEpoch entries
// are no-ops (same CAS guard as the metadata state machine).
func (s *AdminService) HandleLeaderAndISR(req broker.LeaderAndISRRequest) broker.LeaderAndISRResponse {
	var results []broker.PartitionResult

	s.mu.Lock()
	defer s.mu.Unlock()

	for _, update := range req.Partitions {
		key := metadata.PartitionKey{TopicID: update.TopicID, PartitionID: update.PartitionID}

		existing, exists := s.cache[key]
		if exists && existing.LeaderEpoch > update.LeaderEpoch {
			// Stale push — a newer epoch is already applied.
			s.logger.Debug("stale LeaderAndISR update, skipping",
				"topic_id", update.TopicID, "partition_id", update.PartitionID,
				"stored_epoch", existing.LeaderEpoch, "incoming_epoch", update.LeaderEpoch)
			results = append(results, broker.PartitionResult{
				TopicID:     update.TopicID,
				PartitionID: update.PartitionID,
			})
			continue
		}

		s.cache[key] = broker.PartitionAssignment{
			Leader:      update.Leader,
			ISR:         update.ISR,
			LeaderEpoch: update.LeaderEpoch,
		}
		s.logger.Info("partition assignment updated",
			"topic_id", update.TopicID,
			"partition_id", update.PartitionID,
			"leader", update.Leader,
			"leader_epoch", update.LeaderEpoch,
			"isr", update.ISR,
		)
		results = append(results, broker.PartitionResult{
			TopicID:     update.TopicID,
			PartitionID: update.PartitionID,
		})
	}

	return broker.LeaderAndISRResponse{Results: results}
}

// GetAssignment returns the current assignment for a partition.
func (s *AdminService) GetAssignment(key metadata.PartitionKey) (broker.PartitionAssignment, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	a, ok := s.cache[key]
	return a, ok
}

// AllAssignments returns a snapshot of the full PartitionStateCache.
func (s *AdminService) AllAssignments() map[metadata.PartitionKey]broker.PartitionAssignment {
	s.mu.RLock()
	defer s.mu.RUnlock()
	cp := make(map[metadata.PartitionKey]broker.PartitionAssignment, len(s.cache))
	for k, v := range s.cache {
		cp[k] = v
	}
	return cp
}

// MarshalJSON is used only for diagnostic endpoints — not a hot path.
func (s *AdminService) MarshalJSON() ([]byte, error) {
	return json.Marshal(s.AllAssignments())
}
