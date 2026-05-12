package http

import (
	"encoding/json"
	"net/http"

	"github.com/yourusername/amyqueue/src/internal/raft"
)

// AdminServer exposes cluster membership operations over HTTP.
// It calls raft.AdminService — it knows nothing about TCP or gRPC.
//
// Routes:
//   GET  /cluster/status          — current leader, term, member list
//   POST /cluster/observers/join  — new node registers itself as observer
//   POST /cluster/voters          — promote observer to voter (admin op)
//   DELETE /cluster/voters/{id}   — remove a voter (admin op)
//
// To replace HTTP with gRPC: implement the same operations in
// api/metadata/grpc/admin.go calling the same raft.AdminService interface.
type AdminServer struct {
	svc  raft.AdminService
	addr string
	srv  *http.Server
}

func NewAdminServer(addr string, svc raft.AdminService) *AdminServer {
	s := &AdminServer{svc: svc, addr: addr}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /cluster/status", s.handleStatus)
	mux.HandleFunc("POST /cluster/observers/join", s.handleJoin)
	mux.HandleFunc("POST /cluster/voters", s.handleAddVoter)
	mux.HandleFunc("DELETE /cluster/voters/{id}", s.handleRemoveVoter)
	s.srv = &http.Server{Addr: addr, Handler: mux}
	return s
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
