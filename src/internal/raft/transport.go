package raft

import "context"

// Transport is the only network seam in the Raft core. Swap TCP for gRPC, HTTP,
// or any custom protocol by providing a different implementation.
type Transport interface {
	// Start begins listening for inbound RPCs and routes them to the node via
	// the provided handlers.
	Start(handlers Handlers) error

	// SendVoteRequest sends a RequestVote RPC to the peer at addr.
	SendVoteRequest(ctx context.Context, addr string, req VoteRequest) (VoteResponse, error)

	// SendAppendEntries sends an AppendEntries RPC (also used as heartbeat when
	// Entries is empty) to the peer at addr.
	SendAppendEntries(ctx context.Context, addr string, req AppendEntriesRequest) (AppendEntriesResponse, error)

	// SendObserverJoin is called by a new node to register itself with the leader.
	SendObserverJoin(ctx context.Context, addr string, req ObserverJoinRequest) (ObserverJoinResponse, error)

	// SendClusterInfo asks any node for the current leader address and member list.
	// Used during bootstrap — the caller doesn't know the leader yet.
	SendClusterInfo(ctx context.Context, addr string, req ClusterInfoRequest) (ClusterInfoResponse, error)

	// Close shuts down the transport.
	Close() error
}

// Handlers is the callback surface the transport calls when an inbound RPC arrives.
// The node registers these so business logic stays in node.go.
type Handlers struct {
	HandleVoteRequest   func(req VoteRequest) VoteResponse
	HandleAppendEntries func(req AppendEntriesRequest) AppendEntriesResponse
	HandleObserverJoin  func(req ObserverJoinRequest) ObserverJoinResponse
	HandleClusterInfo   func(req ClusterInfoRequest) ClusterInfoResponse
}
