package metadata

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
)

// BrokerChannel is the controller's abstraction for pushing LeaderAndISR updates
// to a broker. Implemented as an HTTP client talking to the broker's admin port.
// The interface allows swapping transport implementations without changing callers.
type BrokerChannel interface {
	// SendLeaderAndISR delivers a partition assignment update to the broker at addr.
	// Returns an error if the broker is unreachable or returns a non-2xx status.
	SendLeaderAndISR(addr string, req LeaderAndISRPush) error
}

// LeaderAndISRPush is the controller's view of the push payload.
type LeaderAndISRPush struct {
	ControllerEpoch int64           `json:"controller_epoch"`
	Partitions      []PartitionPush `json:"partitions"`
}

// PartitionPush carries one partition's updated state for a LeaderAndISR push.
type PartitionPush struct {
	TopicID        TopicID   `json:"topic_id"`
	TopicName      string    `json:"topic_name"`
	PartitionID    int32     `json:"partition_id"`
	Leader         BrokerID  `json:"leader"`
	LeaderEpoch    int32     `json:"leader_epoch"`
	ISR            []BrokerID `json:"isr"`
	Replicas       []BrokerID `json:"replicas"`
	PartitionEpoch int32     `json:"partition_epoch"`
}

// HTTPBrokerChannel implements BrokerChannel over plain HTTP.
// The broker's admin endpoint is POST /admin/leader-and-isr on its HTTP port.
type HTTPBrokerChannel struct {
	client *http.Client
}

func NewHTTPBrokerChannel() *HTTPBrokerChannel {
	return &HTTPBrokerChannel{client: &http.Client{}}
}

func (c *HTTPBrokerChannel) SendLeaderAndISR(addr string, req LeaderAndISRPush) error {
	data, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("marshal LeaderAndISR: %w", err)
	}

	url := "http://" + addr + "/admin/leader-and-isr"
	resp, err := c.client.Post(url, "application/json", bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("send LeaderAndISR to %s: %w", addr, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("LeaderAndISR to %s: status %d", addr, resp.StatusCode)
	}
	return nil
}
