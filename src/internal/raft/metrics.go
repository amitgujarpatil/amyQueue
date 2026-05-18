package raft

// MetricsSource is a read-only view into a Raft node's runtime state.
// Implemented by Node; consumed by the metrics package.
// The raft package never imports prometheus — this interface is the seam.
type MetricsSource interface {
	MetricsSnapshot() MetricsSnapshot
}

// MetricsSnapshot is a point-in-time copy of node state for metrics collection.
// All fields are safe to read without holding any lock — the snapshot is taken
// under the node's mutex and then returned by value.
type MetricsSnapshot struct {
	NodeID      string
	State       State
	Term        uint64
	CommitIndex uint64
	LastApplied uint64
	LeaderID    string
	Members     []Member

	// ObserverLag is populated only on the leader: maps observer NodeID → lag
	// (leader.lastIndex - matchIndex[observer.addr]). Empty on followers.
	ObserverLag map[string]int64

	// Cumulative counters — only increase over the lifetime of the process.
	ElectionsStarted uint64
	LeaderChanges    uint64
	HeartbeatFailures map[string]uint64 // peer addr → failure count
}
