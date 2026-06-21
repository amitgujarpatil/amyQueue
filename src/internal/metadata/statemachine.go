package metadata

import (
	"encoding/json"
	"fmt"
	"log"

	"github.com/yourusername/amyqueue/src/internal/raft"
)

// MetadataStateMachine implements raft.StateMachine and routes CmdMetadata log
// entries to the appropriate Store Apply method.
//
// Lock ordering: Apply is called from applyCommitted with n.mu held.
// The Store's own mutex is acquired after n.mu — never before.
//
// onPartitionUpdate fires AFTER a successful ApplyUpdatePartition. It is called
// from within Apply (i.e. with n.mu held via applyCommitted), so it must not
// block or acquire n.mu. The callback should post to a channel; a separate
// goroutine reads from it and sends LeaderAndISR pushes.
type MetadataStateMachine struct {
	store             Store
	onPartitionUpdate func(key PartitionKey) // optional; set via WithPartitionUpdateHook
}

func NewMetadataStateMachine(store Store) *MetadataStateMachine {
	return &MetadataStateMachine{store: store}
}

// WithPartitionUpdateHook registers a callback invoked after each successful
// ApplyUpdatePartition. Must be called before the node starts.
func (sm *MetadataStateMachine) WithPartitionUpdateHook(fn func(key PartitionKey)) *MetadataStateMachine {
	sm.onPartitionUpdate = fn
	return sm
}

func (sm *MetadataStateMachine) Apply(entry raft.LogEntry) error {
	cmd, err := DecodeMetadataCommand(entry.Command)
	if err != nil {
		return fmt.Errorf("metadata: index %d: decode: %w", entry.Index, err)
	}

	switch cmd.Type {
	case CmdTypeClusterInit:
		var p ClusterInitPayload
		if err := json.Unmarshal(cmd.Payload, &p); err != nil {
			return fmt.Errorf("metadata: cluster_init decode: %w", err)
		}
		return sm.store.ApplyClusterInit(p)

	case CmdTypeCreateTopic:
		var p CreateTopicPayload
		if err := json.Unmarshal(cmd.Payload, &p); err != nil {
			return fmt.Errorf("metadata: create_topic decode: %w", err)
		}
		return sm.store.ApplyCreateTopic(p)

	case CmdTypeDeleteTopic:
		var p DeleteTopicPayload
		if err := json.Unmarshal(cmd.Payload, &p); err != nil {
			return fmt.Errorf("metadata: delete_topic decode: %w", err)
		}
		return sm.store.ApplyDeleteTopic(p)

	case CmdTypeUpdatePartition:
		var p UpdatePartitionPayload
		if err := json.Unmarshal(cmd.Payload, &p); err != nil {
			return fmt.Errorf("metadata: update_partition decode: %w", err)
		}
		if err := sm.store.ApplyUpdatePartition(p); err != nil {
			return err
		}
		if sm.onPartitionUpdate != nil {
			sm.onPartitionUpdate(p.Key)
		}
		return nil

	case CmdTypeRegisterBroker:
		var p RegisterBrokerPayload
		if err := json.Unmarshal(cmd.Payload, &p); err != nil {
			return fmt.Errorf("metadata: register_broker decode: %w", err)
		}
		_, err := sm.store.ApplyRegisterBroker(p)
		return err

	case CmdTypeShutdownBroker:
		var p ShutdownBrokerPayload
		if err := json.Unmarshal(cmd.Payload, &p); err != nil {
			return fmt.Errorf("metadata: shutdown_broker decode: %w", err)
		}
		return sm.store.ApplyShutdownBroker(p)

	default:
		log.Printf("metadata: unknown command type %q at index %d, skipping", cmd.Type, entry.Index)
		return nil
	}
}
