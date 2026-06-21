package http

import (
	"encoding/json"
	"net/http"

	"github.com/yourusername/amyqueue/src/internal/raft"
)

// AdminServer exposes cluster membership operations and metadata operations over HTTP.
//
// Raft admin routes:
//   GET  /cluster/status          — current leader, term, member list
//   POST /cluster/observers/join  — new node registers itself as observer
//   POST /cluster/voters          — promote observer to voter (admin op)
//   DELETE /cluster/voters/{id}   — remove a voter (admin op)
//
// Metadata routes (registered via RegisterMetadataRoutes):
//   POST   /brokers/register       — broker self-registration
//   POST   /brokers/{id}/shutdown  — controlled broker shutdown
//   GET    /brokers                — list all registered brokers
//   GET    /brokers/{id}           — get single broker
//
// To replace HTTP with gRPC: implement the same operations in
// api/metadata/grpc/admin.go calling the same service interfaces.
type AdminServer struct {
	svc  raft.AdminService
	addr string
	mux  *http.ServeMux
	srv  *http.Server
}

func NewAdminServer(addr string, svc raft.AdminService) *AdminServer {
	s := &AdminServer{svc: svc, addr: addr, mux: http.NewServeMux()}
	s.mux.HandleFunc("GET /cluster/status", s.handleStatus)
	s.mux.HandleFunc("POST /cluster/observers/join", s.handleJoin)
	s.mux.HandleFunc("POST /cluster/voters", s.handleAddVoter)
	s.mux.HandleFunc("DELETE /cluster/voters/{id}", s.handleRemoveVoter)
	s.srv = &http.Server{Addr: addr, Handler: s.mux}
	return s
}

// RegisterMetadataRoutes adds broker, topic, and partition HTTP handlers.
// Must be called before Start.
func (s *AdminServer) RegisterMetadataRoutes(meta MetadataService) {
	// Broker routes
	s.mux.HandleFunc("POST /brokers/register", meta.HandleRegisterBroker)
	s.mux.HandleFunc("POST /brokers/{id}/heartbeat", meta.HandleBrokerHeartbeat)
	s.mux.HandleFunc("POST /brokers/{id}/shutdown", meta.HandleShutdownBroker)
	s.mux.HandleFunc("GET /brokers", meta.HandleListBrokers)
	s.mux.HandleFunc("GET /brokers/{id}", meta.HandleGetBroker)

	// Topic routes
	s.mux.HandleFunc("POST /topics", meta.HandleCreateTopic)
	s.mux.HandleFunc("GET /topics", meta.HandleListTopics)
	s.mux.HandleFunc("GET /topics/{id}", meta.HandleGetTopic)
	s.mux.HandleFunc("DELETE /topics/{id}", meta.HandleDeleteTopic)

	// Partition routes
	s.mux.HandleFunc("GET /topics/{id}/partitions", meta.HandleListPartitions)
	s.mux.HandleFunc("GET /topics/{id}/partitions/{pid}", meta.HandleGetPartition)

	// Cluster init
	s.mux.HandleFunc("POST /cluster/init", meta.HandleClusterInit)

	// Consumer group coordinator
	s.mux.HandleFunc("GET /groups/{id}/coordinator", meta.HandleFindCoordinator)
}

// MetadataService is the interface the AdminServer calls for metadata operations.
// The controller package implements this by proposing entries to Raft.
type MetadataService interface {
	// Broker
	HandleRegisterBroker(w http.ResponseWriter, r *http.Request)
	HandleBrokerHeartbeat(w http.ResponseWriter, r *http.Request)
	HandleShutdownBroker(w http.ResponseWriter, r *http.Request)
	HandleListBrokers(w http.ResponseWriter, r *http.Request)
	HandleGetBroker(w http.ResponseWriter, r *http.Request)

	// Topic
	HandleCreateTopic(w http.ResponseWriter, r *http.Request)
	HandleListTopics(w http.ResponseWriter, r *http.Request)
	HandleGetTopic(w http.ResponseWriter, r *http.Request)
	HandleDeleteTopic(w http.ResponseWriter, r *http.Request)

	// Partition
	HandleListPartitions(w http.ResponseWriter, r *http.Request)
	HandleGetPartition(w http.ResponseWriter, r *http.Request)

	// Cluster
	HandleClusterInit(w http.ResponseWriter, r *http.Request)

	// Consumer group
	HandleFindCoordinator(w http.ResponseWriter, r *http.Request)
}

func (s *AdminServer) Start() error {
	go s.srv.ListenAndServe()
	return nil
}

func (s *AdminServer) Stop() error {
	return s.srv.Close()
}

// GET /cluster/status
func (s *AdminServer) handleStatus(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, s.svc.ClusterStatus())
}

// POST /cluster/observers/join
// Body: {"node_id": "ctrl-4", "addr": "localhost:7004"}
func (s *AdminServer) handleJoin(w http.ResponseWriter, r *http.Request) {
	var req raft.ObserverJoinRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	resp := s.svc.JoinAsObserver(req)
	if !resp.Success {
		writeJSON(w, http.StatusBadRequest, resp)
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

// POST /cluster/voters
// Body: {"node_id": "ctrl-4", "addr": "localhost:7004"}
func (s *AdminServer) handleAddVoter(w http.ResponseWriter, r *http.Request) {
	var req raft.AddVoterRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	resp := s.svc.AddVoter(req)
	if !resp.Success {
		writeJSON(w, http.StatusBadRequest, resp)
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

// DELETE /cluster/voters/{id}
func (s *AdminServer) handleRemoveVoter(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if id == "" {
		writeErr(w, http.StatusBadRequest, "missing voter id in path")
		return
	}
	resp := s.svc.RemoveVoter(raft.RemoveVoterRequest{NodeID: id})
	if !resp.Success {
		writeJSON(w, http.StatusBadRequest, resp)
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}
