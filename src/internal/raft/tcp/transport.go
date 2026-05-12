package tcp

import (
	"context"
	"encoding/gob"
	"fmt"
	"net"
	"time"

	"github.com/yourusername/amyqueue/src/internal/raft"
)

// message type tags so the receiver knows what to decode
const (
	tagVoteRequest      byte = 1
	tagVoteResponse     byte = 2
	tagAppendEntries    byte = 3
	tagAppendResponse   byte = 4
)

// Transport implements raft.Transport over a raw TCP connection.
// Each RPC is a single dial → encode request → decode response → close.
// This is simple and works fine for a Raft cluster with a handful of nodes.
type Transport struct {
	listenAddr string
	listener   net.Listener
}

func New(listenAddr string) *Transport {
	return &Transport{listenAddr: listenAddr}
}

func (t *Transport) Start(handlers raft.Handlers) error {
	ln, err := net.Listen("tcp", t.listenAddr)
	if err != nil {
		return fmt.Errorf("tcp transport listen %s: %w", t.listenAddr, err)
	}
	t.listener = ln
	go t.accept(handlers)
	return nil
}

func (t *Transport) Close() error {
	if t.listener != nil {
		return t.listener.Close()
	}
	return nil
}

func (t *Transport) SendVoteRequest(ctx context.Context, addr string, req raft.VoteRequest) (raft.VoteResponse, error) {
	conn, err := dialCtx(ctx, addr)
	if err != nil {
		return raft.VoteResponse{}, err
	}
	defer conn.Close()

	enc := gob.NewEncoder(conn)
	dec := gob.NewDecoder(conn)

	if err := enc.Encode(tagVoteRequest); err != nil {
		return raft.VoteResponse{}, err
	}
	if err := enc.Encode(req); err != nil {
		return raft.VoteResponse{}, err
	}

	var resp raft.VoteResponse
	if err := dec.Decode(&resp); err != nil {
		return raft.VoteResponse{}, err
	}
	return resp, nil
}

func (t *Transport) SendAppendEntries(ctx context.Context, addr string, req raft.AppendEntriesRequest) (raft.AppendEntriesResponse, error) {
	conn, err := dialCtx(ctx, addr)
	if err != nil {
		return raft.AppendEntriesResponse{}, err
	}
	defer conn.Close()

	enc := gob.NewEncoder(conn)
	dec := gob.NewDecoder(conn)

	if err := enc.Encode(tagAppendEntries); err != nil {
		return raft.AppendEntriesResponse{}, err
	}
	if err := enc.Encode(req); err != nil {
		return raft.AppendEntriesResponse{}, err
	}

	var resp raft.AppendEntriesResponse
	if err := dec.Decode(&resp); err != nil {
		return raft.AppendEntriesResponse{}, err
	}
	return resp, nil
}

// ─── server side ─────────────────────────────────────────────────────────────

func (t *Transport) accept(h raft.Handlers) {
	for {
		conn, err := t.listener.Accept()
		if err != nil {
			return // listener closed
		}
		go t.handle(conn, h)
	}
}

func (t *Transport) handle(conn net.Conn, h raft.Handlers) {
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))

	enc := gob.NewEncoder(conn)
	dec := gob.NewDecoder(conn)

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
		resp := h.HandleVoteRequest(req)
		_ = enc.Encode(resp)

	case tagAppendEntries:
		var req raft.AppendEntriesRequest
		if err := dec.Decode(&req); err != nil {
			return
		}
		resp := h.HandleAppendEntries(req)
		_ = enc.Encode(resp)
	}
}

func dialCtx(ctx context.Context, addr string) (net.Conn, error) {
	d := net.Dialer{}
	return d.DialContext(ctx, "tcp", addr)
}
