package controller

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"github.com/yourusername/amyqueue/src/internal/metadata"
	"github.com/yourusername/amyqueue/src/internal/raft"
)

// MetadataService implements api/metadata/http.MetadataService.
// It validates auth, proposes Raft commands, and reads from the store.
type MetadataService struct {
	node  *raft.Node
	store metadata.Store
	auth  metadata.ClusterAuth
}

func NewMetadataService(node *raft.Node, store metadata.Store, auth metadata.ClusterAuth) *MetadataService {
	return &MetadataService{node: node, store: store, auth: auth}
}

// --- Broker handlers ---

type registerBrokerRequest struct {
	BrokerID  metadata.BrokerID `json:"broker_id"`
	Host      string            `json:"host"`
	Port      int32             `json:"port"`
	RackID    string            `json:"rack_id"`
	ClusterID string            `json:"cluster_id"`
	Token     string            `json:"token"`
}

type registerBrokerResponse struct {
	BrokerEpoch     int64  `json:"broker_epoch"`
	ClusterID       string `json:"cluster_id"`
	MetadataVersion int64  `json:"metadata_version"`
}

func (s *MetadataService) HandleRegisterBroker(w http.ResponseWriter, r *http.Request) {
	var req registerBrokerRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}

	if err := s.auth.ValidateRequest(req.ClusterID, req.Token); err != nil {
		writeErr(w, http.StatusUnauthorized, err.Error())
		return
	}

	if req.BrokerID == "" {
		writeErr(w, http.StatusBadRequest, "broker_id is required")
		return
	}
	if req.Host == "" {
		writeErr(w, http.StatusBadRequest, "host is required")
		return
	}

	payload := metadata.RegisterBrokerPayload{
		BrokerID:  req.BrokerID,
		Host:      req.Host,
		Port:      req.Port,
		RackID:    req.RackID,
		ClusterID: req.ClusterID,
	}
	cmd, err := metadata.EncodeMetadataCommand(metadata.CmdTypeRegisterBroker, payload)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "encode command: "+err.Error())
		return
	}

	if err := s.node.Propose(raft.CmdMetadata, cmd); err != nil {
		var nle *raft.NotLeaderError
		if errors.As(err, &nle) {
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{
				"error":       "not the leader",
				"leader_id":   nle.LeaderID,
				"leader_addr": nle.LeaderAddr,
			})
			return
		}
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}

	b, ok := s.store.GetBroker(req.BrokerID)
	if !ok {
		writeErr(w, http.StatusInternalServerError, "broker not found after registration")
		return
	}
	writeJSON(w, http.StatusOK, registerBrokerResponse{
		BrokerEpoch:     b.Epoch,
		ClusterID:       s.store.ClusterID(),
		MetadataVersion: s.store.Version(),
	})
}

type shutdownBrokerRequest struct {
	Epoch     int64  `json:"epoch"`
	ClusterID string `json:"cluster_id"`
	Token     string `json:"token"`
}

type shutdownBrokerResponse struct {
	Success bool `json:"success"`
}

func (s *MetadataService) HandleShutdownBroker(w http.ResponseWriter, r *http.Request) {
	id := metadata.BrokerID(r.PathValue("id"))
	if id == "" {
		writeErr(w, http.StatusBadRequest, "missing broker id")
		return
	}

	var req shutdownBrokerRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}

	if err := s.auth.ValidateRequest(req.ClusterID, req.Token); err != nil {
		writeErr(w, http.StatusUnauthorized, err.Error())
		return
	}

	payload := metadata.ShutdownBrokerPayload{
		BrokerID:      id,
		ExpectedEpoch: req.Epoch,
	}
	cmd, err := metadata.EncodeMetadataCommand(metadata.CmdTypeShutdownBroker, payload)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "encode command: "+err.Error())
		return
	}

	if err := s.node.Propose(raft.CmdMetadata, cmd); err != nil {
		var nle *raft.NotLeaderError
		if errors.As(err, &nle) {
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{
				"error":       "not the leader",
				"leader_id":   nle.LeaderID,
				"leader_addr": nle.LeaderAddr,
			})
			return
		}
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, shutdownBrokerResponse{Success: true})
}

func (s *MetadataService) HandleListBrokers(w http.ResponseWriter, r *http.Request) {
	if err := s.authFromHeader(r); err != nil {
		writeErr(w, http.StatusUnauthorized, err.Error())
		return
	}
	brokers := s.store.ListBrokers()
	writeJSON(w, http.StatusOK, map[string]any{"brokers": brokers})
}

func (s *MetadataService) HandleGetBroker(w http.ResponseWriter, r *http.Request) {
	if err := s.authFromHeader(r); err != nil {
		writeErr(w, http.StatusUnauthorized, err.Error())
		return
	}
	id := metadata.BrokerID(r.PathValue("id"))
	b, ok := s.store.GetBroker(id)
	if !ok {
		writeErr(w, http.StatusNotFound, "broker not found")
		return
	}
	writeJSON(w, http.StatusOK, b)
}

// --- Topic handlers (Phase 3 wires these fully; stubs now) ---

type createTopicRequest struct {
	Name              string              `json:"name"`
	NumPartitions     int32               `json:"num_partitions"`
	ReplicationFactor int32               `json:"replication_factor"`
	Config            metadata.TopicConfig `json:"config"`
	ClusterID         string              `json:"cluster_id"`
	Token             string              `json:"token"`
}

func (s *MetadataService) HandleCreateTopic(w http.ResponseWriter, r *http.Request) {
	var req createTopicRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := s.auth.ValidateRequest(req.ClusterID, req.Token); err != nil {
		writeErr(w, http.StatusUnauthorized, err.Error())
		return
	}
	if req.Name == "" {
		writeErr(w, http.StatusBadRequest, "name is required")
		return
	}
	if req.NumPartitions <= 0 {
		writeErr(w, http.StatusBadRequest, "num_partitions must be > 0")
		return
	}
	if req.ReplicationFactor <= 0 {
		writeErr(w, http.StatusBadRequest, "replication_factor must be > 0")
		return
	}

	topicID := metadata.TopicID(newUUID())
	activeBrokers := s.store.ListActiveBrokers()
	brokerIDs := make([]metadata.BrokerID, len(activeBrokers))
	for i, b := range activeBrokers {
		brokerIDs[i] = b.BrokerID
	}

	payload := metadata.CreateTopicPayload{
		TopicID:           topicID,
		Name:              req.Name,
		NumPartitions:     req.NumPartitions,
		ReplicationFactor: req.ReplicationFactor,
		Config:            req.Config,
		Brokers:           brokerIDs,
	}
	cmd, err := metadata.EncodeMetadataCommand(metadata.CmdTypeCreateTopic, payload)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "encode command: "+err.Error())
		return
	}

	if err := s.node.Propose(raft.CmdMetadata, cmd); err != nil {
		var nle *raft.NotLeaderError
		if errors.As(err, &nle) {
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{
				"error":       "not the leader",
				"leader_id":   nle.LeaderID,
				"leader_addr": nle.LeaderAddr,
			})
			return
		}
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}

	t, _ := s.store.GetTopic(topicID)
	writeJSON(w, http.StatusCreated, t)
}

func (s *MetadataService) HandleListTopics(w http.ResponseWriter, r *http.Request) {
	if err := s.authFromHeader(r); err != nil {
		writeErr(w, http.StatusUnauthorized, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"topics": s.store.ListTopics()})
}

func (s *MetadataService) HandleGetTopic(w http.ResponseWriter, r *http.Request) {
	if err := s.authFromHeader(r); err != nil {
		writeErr(w, http.StatusUnauthorized, err.Error())
		return
	}
	id := metadata.TopicID(r.PathValue("id"))
	t, ok := s.store.GetTopic(id)
	if !ok {
		// try by name as fallback
		t, ok = s.store.GetTopicByName(string(id))
	}
	if !ok {
		writeErr(w, http.StatusNotFound, "topic not found")
		return
	}
	writeJSON(w, http.StatusOK, t)
}

func (s *MetadataService) HandleDeleteTopic(w http.ResponseWriter, r *http.Request) {
	if err := s.authFromHeader(r); err != nil {
		writeErr(w, http.StatusUnauthorized, err.Error())
		return
	}
	id := metadata.TopicID(r.PathValue("id"))
	t, ok := s.store.GetTopic(id)
	if !ok {
		writeErr(w, http.StatusNotFound, "topic not found")
		return
	}
	if t.Internal {
		writeErr(w, http.StatusForbidden, "cannot delete internal topic")
		return
	}

	payload := metadata.DeleteTopicPayload{TopicID: id}
	cmd, err := metadata.EncodeMetadataCommand(metadata.CmdTypeDeleteTopic, payload)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "encode command: "+err.Error())
		return
	}

	if err := s.node.Propose(raft.CmdMetadata, cmd); err != nil {
		var nle *raft.NotLeaderError
		if errors.As(err, &nle) {
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{
				"error":       "not the leader",
				"leader_id":   nle.LeaderID,
				"leader_addr": nle.LeaderAddr,
			})
			return
		}
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// --- Partition handlers ---

func (s *MetadataService) HandleListPartitions(w http.ResponseWriter, r *http.Request) {
	if err := s.authFromHeader(r); err != nil {
		writeErr(w, http.StatusUnauthorized, err.Error())
		return
	}
	id := metadata.TopicID(r.PathValue("id"))
	if _, ok := s.store.GetTopic(id); !ok {
		writeErr(w, http.StatusNotFound, "topic not found")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"partitions": s.store.ListPartitions(id)})
}

func (s *MetadataService) HandleGetPartition(w http.ResponseWriter, r *http.Request) {
	if err := s.authFromHeader(r); err != nil {
		writeErr(w, http.StatusUnauthorized, err.Error())
		return
	}
	id := metadata.TopicID(r.PathValue("id"))
	pidStr := r.PathValue("pid")
	var pid int32
	if _, err := fmt.Sscanf(pidStr, "%d", &pid); err != nil {
		writeErr(w, http.StatusBadRequest, "partition id must be an integer")
		return
	}
	p, ok := s.store.GetPartition(metadata.PartitionKey{TopicID: id, PartitionID: pid})
	if !ok {
		writeErr(w, http.StatusNotFound, "partition not found")
		return
	}
	writeJSON(w, http.StatusOK, p)
}

// --- Cluster init handler ---

type clusterInitRequest struct {
	ClusterID string `json:"cluster_id"`
	Token     string `json:"token"`
}

func (s *MetadataService) HandleClusterInit(w http.ResponseWriter, r *http.Request) {
	var req clusterInitRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := s.auth.ValidateRequest(req.ClusterID, req.Token); err != nil {
		writeErr(w, http.StatusUnauthorized, err.Error())
		return
	}

	if s.store.ClusterID() != "" {
		writeJSON(w, http.StatusOK, map[string]any{
			"cluster_id":       s.store.ClusterID(),
			"metadata_version": s.store.Version(),
			"already_init":     true,
		})
		return
	}

	payload := metadata.ClusterInitPayload{ClusterID: req.ClusterID}
	cmd, err := metadata.EncodeMetadataCommand(metadata.CmdTypeClusterInit, payload)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "encode command: "+err.Error())
		return
	}

	if err := s.node.Propose(raft.CmdMetadata, cmd); err != nil {
		var nle *raft.NotLeaderError
		if errors.As(err, &nle) {
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{
				"error":       "not the leader",
				"leader_id":   nle.LeaderID,
				"leader_addr": nle.LeaderAddr,
			})
			return
		}
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"cluster_id":       s.store.ClusterID(),
		"metadata_version": s.store.Version(),
	})
}

// --- helpers ---

func (s *MetadataService) authFromHeader(r *http.Request) error {
	clusterID := r.Header.Get("X-Cluster-ID")
	token := r.Header.Get("X-Cluster-Token")
	return s.auth.ValidateRequest(clusterID, token)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}
