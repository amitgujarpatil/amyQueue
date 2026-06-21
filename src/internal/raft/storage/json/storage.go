// Package json provides a JSON file-based implementation of raft.Storage.
//
// Layout under dataDir/nodeID/raft/:
//
//	state.json       — hard state (term, votedFor, commitIndex); atomic replace
//	log/entries.ndjson — log entries, one JSON object per line; rewritten on truncation
package json

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"github.com/yourusername/amyqueue/src/internal/raft"
)

// Storage is the JSON file-backed implementation of raft.Storage.
type Storage struct {
	stateFile string // path to state.json
	logFile   string // path to log/entries.ndjson
}

// New creates a Storage rooted at dataDir/nodeID/raft/.
// The directory tree is created on first use.
func New(dataDir, nodeID string) (*Storage, error) {
	base := filepath.Join(dataDir, nodeID, "raft")
	logDir := filepath.Join(base, "log")
	if err := os.MkdirAll(logDir, 0o755); err != nil {
		return nil, fmt.Errorf("raft/storage: create dirs: %w", err)
	}
	return &Storage{
		stateFile: filepath.Join(base, "state.json"),
		logFile:   filepath.Join(logDir, "entries.ndjson"),
	}, nil
}

// SaveHardState atomically replaces state.json with the new hard state.
// Atomicity is achieved by writing to a temp file and renaming.
func (s *Storage) SaveHardState(hs raft.HardState) error {
	data, err := json.Marshal(hs)
	if err != nil {
		return err
	}
	tmp := s.stateFile + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return fmt.Errorf("raft/storage: write hard state: %w", err)
	}
	if err := os.Rename(tmp, s.stateFile); err != nil {
		return fmt.Errorf("raft/storage: rename hard state: %w", err)
	}
	return nil
}

// LoadHardState reads state.json. Returns a zero HardState if the file doesn't exist yet.
func (s *Storage) LoadHardState() (raft.HardState, error) {
	data, err := os.ReadFile(s.stateFile)
	if errors.Is(err, os.ErrNotExist) {
		return raft.HardState{}, nil
	}
	if err != nil {
		return raft.HardState{}, fmt.Errorf("raft/storage: read hard state: %w", err)
	}
	var hs raft.HardState
	if err := json.Unmarshal(data, &hs); err != nil {
		return raft.HardState{}, fmt.Errorf("raft/storage: decode hard state: %w", err)
	}
	return hs, nil
}

// AppendLogEntries adds entries to the NDJSON log file.
// If the first entry's index is ≤ the last stored index, the stored log is
// truncated to prevIndex before appending (same semantics as raft.Log.append).
func (s *Storage) AppendLogEntries(entries []raft.LogEntry) error {
	if len(entries) == 0 {
		return nil
	}

	existing, err := s.LoadLogEntries()
	if err != nil {
		return err
	}

	// Truncate conflicting suffix.
	firstNew := entries[0].Index
	truncated := make([]raft.LogEntry, 0, len(existing))
	for _, e := range existing {
		if e.Index < firstNew {
			truncated = append(truncated, e)
		}
	}
	merged := append(truncated, entries...)

	return s.writeAll(merged)
}

// LoadLogEntries reads and parses entries.ndjson, returning entries sorted by index.
func (s *Storage) LoadLogEntries() ([]raft.LogEntry, error) {
	f, err := os.Open(s.logFile)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("raft/storage: open log: %w", err)
	}
	defer f.Close()

	var entries []raft.LogEntry
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 1<<20), 1<<20) // 1 MiB per line
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		var entry raft.LogEntry
		if err := json.Unmarshal(line, &entry); err != nil {
			return nil, fmt.Errorf("raft/storage: decode log entry: %w", err)
		}
		entries = append(entries, entry)
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("raft/storage: scan log: %w", err)
	}

	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Index < entries[j].Index
	})
	return entries, nil
}

// writeAll rewrites entries.ndjson from scratch with the given entries.
func (s *Storage) writeAll(entries []raft.LogEntry) error {
	tmp := s.logFile + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return fmt.Errorf("raft/storage: create log tmp: %w", err)
	}
	w := bufio.NewWriter(f)
	for _, e := range entries {
		line, err := json.Marshal(e)
		if err != nil {
			_ = f.Close()
			return fmt.Errorf("raft/storage: marshal log entry %d: %w", e.Index, err)
		}
		_, _ = w.Write(line)
		_ = w.WriteByte('\n')
	}
	if err := w.Flush(); err != nil {
		_ = f.Close()
		return fmt.Errorf("raft/storage: flush log: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("raft/storage: close log: %w", err)
	}
	if err := os.Rename(tmp, s.logFile); err != nil {
		return fmt.Errorf("raft/storage: rename log: %w", err)
	}
	return nil
}
