package broker

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/yourusername/amyqueue/src/internal/config"
	"github.com/yourusername/amyqueue/src/internal/metadata"
)

// Broker holds the runtime state for a broker node.
// Epoch is the fencing token assigned by the controller on registration.
type Broker struct {
	cfg   *config.Config
	Epoch int64
}

func New(cfg *config.Config) *Broker {
	return &Broker{cfg: cfg}
}

// Register calls POST /brokers/register on the controller with exponential
// backoff until it succeeds or maxRetries full passes are exhausted.
// On 503 with a leader hint the request is redirected to the actual leader.
func (b *Broker) Register(ctx context.Context) error {
	if b.cfg.BrokerID == "" {
		return fmt.Errorf("AMYQUEUE_BROKER_ID must be set — broker cannot start without an explicit ID")
	}
	if b.cfg.ClusterToken == "" {
		return fmt.Errorf("AMYQUEUE_CLUSTER_TOKEN must be set")
	}

	controllerAddr := fmt.Sprintf("%s:%d", b.cfg.ControllerHost, b.cfg.HTTPPort)
	target := controllerAddr

	body := map[string]any{
		"broker_id":  b.cfg.BrokerID,
		"host":       b.cfg.BrokerHost,
		"port":       int32(b.cfg.BrokerPort),
		"rack_id":    b.cfg.RackID,
		"cluster_id": b.cfg.ClusterID,
		"token":      b.cfg.ClusterToken,
	}

	const maxRetries = 10
	backoff := 500 * time.Millisecond

	for attempt := 1; attempt <= maxRetries; attempt++ {
		epoch, newTarget, err := b.tryRegister(ctx, target, body)
		if err == nil {
			b.Epoch = epoch
			return nil
		}
		if newTarget != "" && newTarget != target {
			target = newTarget
			continue // immediate redirect to leader — no backoff
		}
		if attempt < maxRetries {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(backoff):
			}
			if backoff < 30*time.Second {
				backoff *= 2
			}
		}
	}
	return fmt.Errorf("broker registration failed after %d attempts", maxRetries)
}

// Shutdown sends POST /brokers/{id}/shutdown to the controller for a controlled exit.
func (b *Broker) Shutdown(ctx context.Context) error {
	url := fmt.Sprintf("http://%s:%d/brokers/%s/shutdown",
		b.cfg.ControllerHost, b.cfg.HTTPPort, b.cfg.BrokerID)

	body := map[string]any{
		"epoch":      b.Epoch,
		"cluster_id": b.cfg.ClusterID,
		"token":      b.cfg.ClusterToken,
	}
	data, err := json.Marshal(body)
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(data))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("shutdown request returned %d", resp.StatusCode)
	}
	return nil
}

func (b *Broker) tryRegister(ctx context.Context, target string, body map[string]any) (epoch int64, redirectTo string, err error) {
	data, err := json.Marshal(body)
	if err != nil {
		return 0, "", err
	}

	url := "http://" + target + "/brokers/register"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(data))
	if err != nil {
		return 0, "", err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, "", fmt.Errorf("connect to %s: %w", target, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusServiceUnavailable {
		var redirect struct {
			LeaderAddr string `json:"leader_addr"`
		}
		_ = json.NewDecoder(resp.Body).Decode(&redirect)
		if redirect.LeaderAddr != "" {
			return 0, redirect.LeaderAddr, fmt.Errorf("not leader, redirecting to %s", redirect.LeaderAddr)
		}
		return 0, "", fmt.Errorf("controller returned 503 with no leader hint")
	}

	if resp.StatusCode != http.StatusOK {
		return 0, "", fmt.Errorf("registration returned %d", resp.StatusCode)
	}

	var result struct {
		BrokerEpoch int64 `json:"broker_epoch"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return 0, "", fmt.Errorf("decode registration response: %w", err)
	}

	return result.BrokerEpoch, "", nil
}

// StartHeartbeat starts a goroutine that sends POST /brokers/{id}/heartbeat to
// the controller every heartbeatMs milliseconds. Stops when ctx is cancelled.
// On StaleEpoch response the broker must re-register; this method returns so
// the caller can restart the full startup sequence.
func (b *Broker) StartHeartbeat(ctx context.Context, heartbeatMs int) {
	go func() {
		ticker := time.NewTicker(time.Duration(heartbeatMs) * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				stale, err := b.sendHeartbeat(ctx)
				if err != nil {
					// transient error — log and continue
					_ = err
				}
				if stale {
					// Epoch is stale — broker must re-register. Stop this heartbeat loop.
					return
				}
			}
		}
	}()
}

func (b *Broker) sendHeartbeat(ctx context.Context) (staleEpoch bool, err error) {
	url := fmt.Sprintf("http://%s:%d/brokers/%s/heartbeat",
		b.cfg.ControllerHost, b.cfg.HTTPPort, b.cfg.BrokerID)

	body := map[string]any{
		"broker_id":        b.cfg.BrokerID,
		"epoch":            b.Epoch,
		"metadata_version": 0, // placeholder until Phase 7 LEO tracking
		"cluster_id":       b.cfg.ClusterID,
		"token":            b.cfg.ClusterToken,
	}
	data, marshalErr := json.Marshal(body)
	if marshalErr != nil {
		return false, marshalErr
	}

	req, reqErr := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(data))
	if reqErr != nil {
		return false, reqErr
	}
	req.Header.Set("Content-Type", "application/json")

	resp, doErr := http.DefaultClient.Do(req)
	if doErr != nil {
		return false, doErr
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return false, fmt.Errorf("heartbeat returned %d", resp.StatusCode)
	}

	var hbResp struct {
		StaleEpoch bool `json:"stale_epoch"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&hbResp)
	return hbResp.StaleEpoch, nil
}

// PartitionAssignment is a placeholder type used by Phase 9 partition state cache.
type PartitionAssignment struct {
	Leader      metadata.BrokerID
	ISR         []metadata.BrokerID
	LeaderEpoch int32
}
