package controller

import (
	"errors"
	"log/slog"

	"github.com/yourusername/amyqueue/src/internal/metadata"
	"github.com/yourusername/amyqueue/src/internal/raft"
)

// PartitionElector proposes leader election entries to the Raft log when the
// liveness sweep detects a dead broker. It is called from the onDead callback
// in the controller's main loop.
type PartitionElector struct {
	node     *raft.Node
	store    metadata.Store
	liveness *metadata.LivenessTracker
	unclean  bool
	logger   *slog.Logger
}

func NewPartitionElector(
	node *raft.Node,
	store metadata.Store,
	liveness *metadata.LivenessTracker,
	uncleanAllowed bool,
	logger *slog.Logger,
) *PartitionElector {
	return &PartitionElector{
		node:     node,
		store:    store,
		liveness: liveness,
		unclean:  uncleanAllowed,
		logger:   logger,
	}
}

// OnBrokerDead is the onDead callback for LivenessTracker.StartSweep.
// For every partition where the dead broker is leader, it elects a replacement
// and proposes an UpdatePartition entry to the Raft log.
func (e *PartitionElector) OnBrokerDead(deadID metadata.BrokerID) {
	e.logger.Warn("broker dead — triggering partition leader election", "broker_id", deadID)

	aliveBrokers := metadata.AliveBrokerSet(e.liveness.AliveBrokers())

	topics := e.store.ListTopics()
	for _, t := range topics {
		partitions := e.store.ListPartitions(t.TopicID)
		for _, p := range partitions {
			if p.Leader != deadID {
				continue
			}
			e.electAndPropose(p, aliveBrokers)
		}
	}
}

func (e *PartitionElector) electAndPropose(p *metadata.PartitionState, aliveBrokers map[metadata.BrokerID]bool) {
	newLeader, newISR, err := metadata.ElectLeader(p, aliveBrokers, e.unclean)
	if errors.Is(err, metadata.ErrNoISRAlive) {
		e.logger.Warn("partition going offline — no alive ISR member",
			"topic_id", p.TopicID, "partition_id", p.PartitionID)
		newLeader = ""
		newISR = nil
	} else if err != nil {
		e.logger.Error("election error", "topic_id", p.TopicID, "partition_id", p.PartitionID, "err", err)
		return
	}

	reason := "leader_death"
	if newLeader == "" {
		reason = "leader_death_no_isr"
	}

	payload := metadata.UpdatePartitionPayload{
		Key:           metadata.PartitionKey{TopicID: p.TopicID, PartitionID: p.PartitionID},
		NewLeader:     newLeader,
		NewISR:        newISR,
		ExpectedEpoch: p.PartitionEpoch,
		Reason:        reason,
	}

	cmd, err := metadata.EncodeMetadataCommand(metadata.CmdTypeUpdatePartition, payload)
	if err != nil {
		e.logger.Error("encode election command", "err", err)
		return
	}

	if err := e.node.Propose(raft.CmdMetadata, cmd); err != nil {
		var nle *raft.NotLeaderError
		if !errors.As(err, &nle) {
			e.logger.Error("propose election failed", "topic_id", p.TopicID, "partition_id", p.PartitionID, "err", err)
		}
		return
	}

	e.logger.Info("partition leader elected",
		"topic_id", p.TopicID,
		"partition_id", p.PartitionID,
		"new_leader", newLeader,
		"new_isr", newISR,
		"reason", reason,
	)
}
