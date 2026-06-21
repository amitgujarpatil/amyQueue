package admin

import (
	"encoding/json"
	"net/http"

	"github.com/yourusername/amyqueue/src/internal/broker"
)

// Server is the broker's admin HTTP server that receives LeaderAndISR pushes
// from the controller. Listens on BrokerPort+1.
type Server struct {
	svc *AdminService
	srv *http.Server
}

func NewServer(addr string, svc *AdminService) *Server {
	s := &Server{svc: svc}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /admin/leader-and-isr", s.handleLeaderAndISR)
	mux.HandleFunc("GET /admin/partitions", s.handleListPartitions)
	s.srv = &http.Server{Addr: addr, Handler: mux}
	return s
}

func (s *Server) Start() error {
	go s.srv.ListenAndServe()
	return nil
}

func (s *Server) Stop() error {
	return s.srv.Close()
}

func (s *Server) handleLeaderAndISR(w http.ResponseWriter, r *http.Request) {
	var req broker.LeaderAndISRRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	resp := s.svc.HandleLeaderAndISR(req)
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

func (s *Server) handleListPartitions(w http.ResponseWriter, r *http.Request) {
	assignments := s.svc.AllAssignments()
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"partitions": assignments})
}
