package tcp

import (
	"fmt"
	"net"
)

// ConnHandler is called in its own goroutine for every accepted connection.
// The implementation is responsible for closing the conn when done.
type ConnHandler func(conn net.Conn)

// Server is a generic TCP listener. It knows nothing about the protocol running
// on top — callers supply a ConnHandler that owns all framing and dispatch.
//
// Pattern for adding a new service over TCP:
//   1. Create a Server with the listen address.
//   2. Implement a ConnHandler that reads/writes your message format.
//   3. Call Start(handler) — the server calls your handler for every connection.
//   4. Call Close() on shutdown.
type Server struct {
	addr     string
	listener net.Listener
}

func NewServer(addr string) *Server {
	return &Server{addr: addr}
}

func (s *Server) Start(handler ConnHandler) error {
	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return fmt.Errorf("tcp server listen %s: %w", s.addr, err)
	}
	s.listener = ln
	go s.acceptLoop(handler)
	return nil
}

func (s *Server) Close() error {
	if s.listener != nil {
		return s.listener.Close()
	}
	return nil
}

func (s *Server) acceptLoop(handler ConnHandler) {
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			return // listener was closed
		}
		go handler(conn)
	}
}
