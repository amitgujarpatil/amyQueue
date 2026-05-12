package raft

// AdminService is the transport-agnostic interface for cluster membership
// operations. The Raft node implements this. The transport layer (HTTP today,
// gRPC tomorrow) calls it — it never knows how the wire works.
//
// Pattern: same as Transport. To add a new admin protocol, implement a new
// server in api/metadata/<proto>/ that calls AdminService methods.
type AdminService interface {
	// AddVoter promotes an observer to a full voter. The change goes through
	// the Raft log so the current quorum commits it before it takes effect.
	// Only the leader can process this; followers return ErrNotLeader.
	AddVoter(req AddVoterRequest) AddVoterResponse

	// RemoveVoter removes a voter from the cluster. The current quorum
	// (including the node being removed) commits the change first.
	RemoveVoter(req RemoveVoterRequest) RemoveVoterResponse

	// JoinAsObserver is called by a new node to register itself with the
	// leader. The leader adds it to the VoterSet as an observer and starts
	// sending it AppendEntries so it can catch up.
	JoinAsObserver(req ObserverJoinRequest) ObserverJoinResponse

	// ClusterStatus returns the current membership snapshot.
	ClusterStatus() ClusterStatusResponse
}

type ClusterStatusResponse struct {
	LeaderID string
	Term     uint64
	Members  []Member
}
