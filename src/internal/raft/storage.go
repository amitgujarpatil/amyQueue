package raft

// HardState is the Raft safety invariant that must reach stable storage
// before this node returns VoteGranted=true. A crash between in-memory update
// and flush, followed by restart, would allow two votes in the same term —
// a safety violation.
type HardState struct {
	CurrentTerm uint64 `json:"current_term"`
	VotedFor    string `json:"voted_for"`
	CommitIndex uint64 `json:"commit_index"`
}

// Storage persists Raft hard state and the log across restarts.
// A nil Storage is in-memory only — compatible with existing tests.
//
// Implementors must guarantee that SaveHardState is durable (fsync or
// equivalent) before returning. A write that returns nil but is lost on
// restart is a safety violation.
type Storage interface {
	// SaveHardState atomically writes term, votedFor, and commitIndex.
	// Called before returning VoteGranted=true and on any term change.
	SaveHardState(hs HardState) error

	// LoadHardState returns the last persisted hard state, or a zero-value
	// HardState if no state has been saved yet.
	LoadHardState() (HardState, error)

	// AppendLogEntries appends entries to the persisted log.
	// If entries[0].Index is less than the last stored index the storage
	// must truncate all entries at that index and beyond before appending
	// (mirrors the in-memory Log.append conflict-resolution rule).
	AppendLogEntries(entries []LogEntry) error

	// LoadLogEntries returns all stored log entries in ascending index order.
	// Returns nil (not an error) when no entries have been persisted.
	LoadLogEntries() ([]LogEntry, error)
}
