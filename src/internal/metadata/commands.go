package metadata

import "encoding/json"

// MetadataCommandType identifies the operation within a CmdMetadata log entry.
type MetadataCommandType string

const (
	CmdTypeClusterInit     MetadataCommandType = "cluster_init"
	CmdTypeCreateTopic     MetadataCommandType = "create_topic"
	CmdTypeDeleteTopic     MetadataCommandType = "delete_topic"
	CmdTypeUpdatePartition MetadataCommandType = "update_partition"
	CmdTypeRegisterBroker  MetadataCommandType = "register_broker"
	CmdTypeShutdownBroker  MetadataCommandType = "shutdown_broker"
)

// MetadataCommand is the envelope written into the Raft log for every metadata
// operation. The outer Type selects the handler; Payload carries the operation data.
type MetadataCommand struct {
	Type    MetadataCommandType `json:"type"`
	Payload json.RawMessage     `json:"payload"`
}

// RegisterBrokerPayload is the data carried in a CmdTypeRegisterBroker entry.
// ClusterID is re-validated inside applyRegisterBroker as defence-in-depth even
// though the HTTP middleware already checked it.
type RegisterBrokerPayload struct {
	BrokerID  BrokerID `json:"broker_id"`
	Host      string   `json:"host"`
	Port      int32    `json:"port"`
	RackID    string   `json:"rack_id"`
	ClusterID string   `json:"cluster_id"`
}

// ShutdownBrokerPayload carries the CAS guard so a stale shutdown request is a no-op.
type ShutdownBrokerPayload struct {
	BrokerID      BrokerID `json:"broker_id"`
	ExpectedEpoch int64    `json:"expected_epoch"`
}

// EncodeMetadataCommand marshals a typed payload into the CmdMetadata wire format.
func EncodeMetadataCommand(cmdType MetadataCommandType, payload any) ([]byte, error) {
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	return json.Marshal(MetadataCommand{Type: cmdType, Payload: json.RawMessage(payloadBytes)})
}

// DecodeMetadataCommand parses the envelope from a Raft log entry's Command bytes.
func DecodeMetadataCommand(data []byte) (*MetadataCommand, error) {
	var cmd MetadataCommand
	if err := json.Unmarshal(data, &cmd); err != nil {
		return nil, err
	}
	return &cmd, nil
}
