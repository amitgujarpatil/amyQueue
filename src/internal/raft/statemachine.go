package raft

// StateMachine processes committed log entries for the application layer.
// Apply is called from applyCommitted with the Raft node's mutex held.
// Implementations must not acquire any lock that could be held while also
// acquiring n.mu — always acquire the state machine's own lock after n.mu.
type StateMachine interface {
	Apply(entry LogEntry) error
}
