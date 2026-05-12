package tcp

import (
	"context"
	"encoding/gob"
	"net"
	"time"

	tcputil "github.com/yourusername/amyqueue/src/internal/transport/tcp"
	"github.com/yourusername/amyqueue/src/internal/raft"
)

// Raft message type tags — one byte prefix so the receiver knows what to decode.
const (
	tagVoteRequest   byte = 1
	tagAppendEntries byte = 3
)

// Transport implements raft.Transport using the generic TCP server/client from
// transport/tcp. This layer owns only the Raft message framing (tag byte + gob
// encode/decode). All TCP mechanics live in transport/tcp.
//
// To add a new transport for Raft (e.g. gRPC), create raft/grpc/transport.go
// that implements the same raft.Transport interface using transport/grpc.
type Transport struct {
	server *tcputil.Server
}

func New(listenAddr string) *Transport {
	return &Transport{
		server: tcputil.NewServer(listenAddr),
	}
}

func (t *Transport) Start(handlers raft.Handlers) error {
	return t.server.Start(connHandler(handlers))
}

func (t *Transport) Close() error {
	return t.server.Close()
}

// ─── client side ─────────────────────────────────────────────────────────────

func (t *Transport) SendVoteRequest(ctx context.Context, addr string, req raft.VoteRequest) (raft.VoteResponse, error) {
	conn, err := tcputil.Dial(ctx, addr)
	if err != nil {
		return raft.VoteResponse{}, err
	}
	defer conn.Close()

	enc, dec := gob.NewEncoder(conn), gob.NewDecoder(conn)
	if err := enc.Encode(tagVoteRequest); err != nil {
		return raft.VoteResponse{}, err
	}
	if err := enc.Encode(req); err != nil {
		return raft.VoteResponse{}, err
	}
	var resp raft.VoteResponse
	return resp, dec.Decode(&resp)
}

func (t *Transport) SendAppendEntries(ctx context.Context, addr string, req raft.AppendEntriesRequest) (raft.AppendEntriesResponse, error) {
	conn, err := tcputil.Dial(ctx, addr)
	if err != nil {
		return raft.AppendEntriesResponse{}, err
	}
	defer conn.Close()

	enc, dec := gob.NewEncoder(conn), gob.NewDecoder(conn)
	if err := enc.Encode(tagAppendEntries); err != nil {
		return raft.AppendEntriesResponse{}, err
	}
	if err := enc.Encode(req); err != nil {
		return raft.AppendEntriesResponse{}, err
	}
	var resp raft.AppendEntriesResponse
	return resp, dec.Decode(&resp)
}

// ─── server side ─────────────────────────────────────────────────────────────

// connHandler returns a transport/tcp.ConnHandler that speaks the Raft wire
// protocol. This is the only place that knows both net.Conn and raft.Handlers.
func connHandler(h raft.Handlers) tcputil.ConnHandler {
	return func(conn net.Conn) {
		defer conn.Close()
		_ = conn.SetDeadline(time.Now().Add(5 * time.Second))

		enc, dec := gob.NewEncoder(conn), gob.NewDecoder(conn)

		var tag byte
		if err := dec.Decode(&tag); err != nil {
			return
		}

		switch tag {
		case tagVoteRequest:
			var req raft.VoteRequest
			if err := dec.Decode(&req); err != nil {
				return
			}
			_ = enc.Encode(h.HandleVoteRequest(req))

		case tagAppendEntries:
			var req raft.AppendEntriesRequest
			if err := dec.Decode(&req); err != nil {
				return
			}
			_ = enc.Encode(h.HandleAppendEntries(req))
		}
	}
}
