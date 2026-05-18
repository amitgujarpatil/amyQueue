package metrics

import (
	"fmt"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/yourusername/amyqueue/src/internal/raft"
)

// Server exposes a /metrics endpoint on a dedicated port for Prometheus scraping.
// It is intentionally separate from the admin HTTP server (HTTP_PORT) so that
// metrics access and admin access can be firewalled independently.
type Server struct {
	srv *http.Server
}

func NewServer(port int, src raft.MetricsSource) *Server {
	reg := prometheus.NewRegistry()
	reg.MustRegister(NewCollector(src))

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))

	return &Server{
		srv: &http.Server{
			Addr:    fmt.Sprintf(":%d", port),
			Handler: mux,
		},
	}
}

func (s *Server) Start() error {
	go s.srv.ListenAndServe()
	return nil
}

func (s *Server) Stop() error {
	return s.srv.Close()
}
