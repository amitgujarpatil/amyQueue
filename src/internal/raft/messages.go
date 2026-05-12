package raft

// All wire message types. Transport layer encodes/decodes these — Raft core only
// sees these structs.

// CommandType tags a LogEntry so the state machine knows how to apply it.
type CommandType byte

const (
	CmdData       CommandType = 0 // normal application data
	CmdMembership CommandType = 1 // membership change (add/remove voter)
)

// ObserverJoinRequest is sent by a new node to the leader when it wants to
// join the cluster as an observer. The leader registers it in the VoterSet
// so it starts receiving AppendEntries, but it does not vote yet.
type ObserverJoinRequest struct {
	NodeID string
	Addr   string // raft listen address of the joining node
}

type ObserverJoinResponse struct {
	Success    bool
	LeaderID   string // redirect hint if we are not the leader
	LeaderAddr string
	Err        string
}

// AddVoterRequest is the admin operation to promote an observer to voter.
// Goes through the Raft log so the old quorum commits it first.
type AddVoterRequest struct {
	NodeID string
	Addr   string
}

type AddVoterResponse struct {
	Success bool
	Err     string
}

// RemoveVoterRequest is the admin operation to remove a voter from the set.
type RemoveVoterRequest struct {
	NodeID string
}

type RemoveVoterResponse struct {
	Success bool
	Err     string
}

type VoteRequest struct {
	Term         uint64
	CandidateID  string
	LastLogIndex uint64
	LastLogTerm  uint64
}

type VoteResponse struct {
	Term        uint64
	VoteGranted bool
}

type AppendEntriesRequest struct {
	Term         uint64
	LeaderID     string
	LeaderAddr   string // raft listen address of the leader — lets followers answer redirects
	PrevLogIndex uint64
	PrevLogTerm  uint64
	Entries      []LogEntry
	LeaderCommit uint64
}

// ClusterInfoRequest can be sent to ANY node. It returns the current leader
// address and the full member list. Used by new nodes for bootstrap discovery —
// they don't know who the leader is yet, so they ask any seed and get directed.
type ClusterInfoRequest struct{}

type ClusterInfoResponse struct {
	LeaderID   string
	LeaderAddr string // raft address of the current leader (empty if no leader known yet)
	Members    []Member
}

type AppendEntriesResponse struct {
	Term    uint64
	Success bool
	// NextIndex is the next index the follower expects (used by leader to back off quickly)
	NextIndex uint64
}
