package controller

import (
	"log/slog"

	"github.com/yourusername/amyqueue/src/internal/metadata"
)

// PushCoordinator receives partition update notifications from the state machine
// and pushes LeaderAndISR updates to all affected brokers via BrokerChannel.
//
// Each broker gets exactly the partitions it hosts (as leader or replica).
// Pushes are fire-and-forget with error logging — the liveness sweep provides
// the recovery path when brokers are unreachable.
type PushCoordinator struct {
	store   metadata.Store
	channel metadata.BrokerChannel
	logger  *slog.Logger
}

func NewPushCoordinator(
	store metadata.Store,
	channel metadata.BrokerChannel,
	logger *slog.Logger,
) *PushCoordinator {
	return &PushCoordinator{store: store, channel: channel, logger: logger}
}

// OnPartitionUpdated is called by the state machine after a successful
// ApplyUpdatePartition. It fans out the new state to all replica brokers.
func (p *PushCoordinator) OnPartitionUpdated(key metadata.PartitionKey) {
	partition, ok := p.store.GetPartition(key)
	if !ok {
		return
	}
	topic, ok := p.store.GetTopic(key.TopicID)
	if !ok {
		return
	}

	push := metadata.PartitionPush{
		TopicID:        partition.TopicID,
		TopicName:      topic.Name,
		PartitionID:    partition.PartitionID,
		Leader:         partition.Leader,
		LeaderEpoch:    partition.LeaderEpoch,
		ISR:            partition.ISR,
		Replicas:       partition.Replicas,
		PartitionEpoch: partition.PartitionEpoch,
	}

	// Push to every replica broker (each needs to know its role).
	sent := make(map[metadata.BrokerID]bool)
	for _, brokerID := range partition.Replicas {
		if sent[brokerID] {
			continue
		}
		sent[brokerID] = true

		b, ok := p.store.GetBroker(brokerID)
		if !ok {
			continue
		}

		addr := b.Host + ":" + int32ToStr(b.Port+1) // BrokerAdminPort = GRPCPort + 1

		req := metadata.LeaderAndISRPush{
			ControllerEpoch: p.store.Version(),
			Partitions:      []metadata.PartitionPush{push},
		}

		go func(brokerAddr string, r metadata.LeaderAndISRPush) {
			if err := p.channel.SendLeaderAndISR(brokerAddr, r); err != nil {
				p.logger.Warn("LeaderAndISR push failed",
					"broker", brokerAddr, "topic_id", key.TopicID, "partition_id", key.PartitionID, "err", err)
			}
		}(addr, req)
	}
}

func int32ToStr(n int32) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	buf := make([]byte, 0, 12)
	for n > 0 {
		buf = append([]byte{byte('0' + n%10)}, buf...)
		n /= 10
	}
	if neg {
		buf = append([]byte{'-'}, buf...)
	}
	return string(buf)
}
