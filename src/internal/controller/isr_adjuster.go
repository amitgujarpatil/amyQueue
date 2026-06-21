package controller

import (
	"errors"
	"log/slog"

	"github.com/yourusername/amyqueue/src/internal/metadata"
	"github.com/yourusername/amyqueue/src/internal/raft"
)

// ISRAdjuster checks all online partitions for ISR shrink/expand conditions
// on each liveness sweep tick and proposes UpdatePartition commands as needed.
//
// ISR shrink: a replica's LEO falls more than maxLag behind the leader's LEO.
// ISR expand: a replica is alive AND caught up (within maxLag) AND currently not in ISR.
type ISRAdjuster struct {
	node        *raft.Node
	store       metadata.Store
	liveness    *metadata.LivenessTracker
	maxLag      int64
	logger      *slog.Logger
}

func NewISRAdjuster(
	node *raft.Node,
	store metadata.Store,
	liveness *metadata.LivenessTracker,
	maxLag int64,
	logger *slog.Logger,
) *ISRAdjuster {
	return &ISRAdjuster{
		node:     node,
		store:    store,
		liveness: liveness,
		maxLag:   maxLag,
		logger:   logger,
	}
}

// AdjustISR runs one sweep over all online partitions, proposing ISR changes
// where needed. Called on each liveness sweep tick.
func (a *ISRAdjuster) AdjustISR() {
	aliveBrokers := metadata.AliveBrokerSet(a.liveness.AliveBrokers())

	for _, topic := range a.store.ListTopics() {
		for _, p := range a.store.ListPartitions(topic.TopicID) {
			if p.Status != metadata.PartitionOnline || p.Leader == "" {
				continue
			}
			a.maybeAdjust(p, aliveBrokers)
		}
	}
}

func (a *ISRAdjuster) maybeAdjust(p *metadata.PartitionState, aliveBrokers map[metadata.BrokerID]bool) {
	// Build new ISR:
	// - Keep existing ISR members that are alive and not lagging.
	// - Add replicas that are alive and caught up but not currently in ISR (expand).
	isrSet := make(map[metadata.BrokerID]bool, len(p.ISR))
	for _, id := range p.ISR {
		isrSet[id] = true
	}

	lagging := a.liveness.LaggingReplicas(p.TopicID, p.PartitionID, p.Leader, p.Replicas, a.maxLag)
	laggingSet := make(map[metadata.BrokerID]bool, len(lagging))
	for _, id := range lagging {
		laggingSet[id] = true
	}

	var newISR []metadata.BrokerID
	// Start from leader (always first in ISR if alive).
	if aliveBrokers[p.Leader] {
		newISR = append(newISR, p.Leader)
	}

	// Add current ISR members that are alive and not lagging.
	for _, id := range p.ISR {
		if id == p.Leader {
			continue
		}
		if aliveBrokers[id] && !laggingSet[id] {
			newISR = append(newISR, id)
		}
	}

	// Expand: add replicas not in ISR that are now alive and caught up.
	for _, id := range p.Replicas {
		if id == p.Leader || isrSet[id] {
			continue
		}
		if aliveBrokers[id] && !laggingSet[id] {
			newISR = append(newISR, id)
		}
	}

	// No change needed.
	if isrEqual(p.ISR, newISR) {
		return
	}

	reason := "isr_shrink"
	if len(newISR) > len(p.ISR) {
		reason = "isr_expand"
	}

	payload := metadata.UpdatePartitionPayload{
		Key:           metadata.PartitionKey{TopicID: p.TopicID, PartitionID: p.PartitionID},
		NewLeader:     p.Leader,
		NewISR:        newISR,
		ExpectedEpoch: p.PartitionEpoch,
		Reason:        reason,
	}
	cmd, err := metadata.EncodeMetadataCommand(metadata.CmdTypeUpdatePartition, payload)
	if err != nil {
		a.logger.Error("encode ISR adjust command", "err", err)
		return
	}

	if err := a.node.Propose(raft.CmdMetadata, cmd); err != nil {
		var nle *raft.NotLeaderError
		if !errors.As(err, &nle) {
			a.logger.Error("propose ISR adjust failed", "err", err)
		}
		return
	}

	a.logger.Info("ISR adjusted",
		"topic_id", p.TopicID, "partition_id", p.PartitionID,
		"old_isr", p.ISR, "new_isr", newISR, "reason", reason)
}

func isrEqual(a, b []metadata.BrokerID) bool {
	if len(a) != len(b) {
		return false
	}
	set := make(map[metadata.BrokerID]bool, len(a))
	for _, id := range a {
		set[id] = true
	}
	for _, id := range b {
		if !set[id] {
			return false
		}
	}
	return true
}
