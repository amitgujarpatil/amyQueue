package raft

// All wire message types. Transport layer encodes/decodes these — Raft core only
// sees these structs.

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
	PrevLogIndex uint64
	PrevLogTerm  uint64
	Entries      []LogEntry
	LeaderCommit uint64
}

type AppendEntriesResponse struct {
	Term    uint64
	Success bool
	// NextIndex is the next index the follower expects (used by leader to back off quickly)
	NextIndex uint64
}
