package raft

import "sync"

type LogEntry struct {
	Index       uint64
	Term        uint64
	Type        CommandType // CmdData or CmdMembership
	Command     []byte
}

type Log struct {
	mu      sync.RWMutex
	entries []LogEntry
}

func newLog() *Log {
	// index 0 is a sentinel (term 0, empty command) so real entries start at 1
	return &Log{entries: []LogEntry{{Index: 0, Term: 0}}}
}

func (l *Log) lastIndex() uint64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return uint64(len(l.entries) - 1)
}

func (l *Log) lastTerm() uint64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.entries[len(l.entries)-1].Term
}

func (l *Log) entry(index uint64) (LogEntry, bool) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if index >= uint64(len(l.entries)) {
		return LogEntry{}, false
	}
	return l.entries[index], true
}

// entriesFrom returns a copy of entries starting at index (inclusive).
func (l *Log) entriesFrom(index uint64) []LogEntry {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if index >= uint64(len(l.entries)) {
		return nil
	}
	cp := make([]LogEntry, len(l.entries)-int(index))
	copy(cp, l.entries[index:])
	return cp
}

// append adds entries, truncating any conflicting suffix first.
func (l *Log) append(prevIndex uint64, entries []LogEntry) {
	l.mu.Lock()
	defer l.mu.Unlock()
	// truncate any entries after prevIndex that may conflict
	l.entries = l.entries[:prevIndex+1]
	l.entries = append(l.entries, entries...)
}

func (l *Log) appendOne(entry LogEntry) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.entries = append(l.entries, entry)
}
