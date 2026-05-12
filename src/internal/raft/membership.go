package raft

import (
	"encoding/json"
	"sync"
)

// ClusterMode controls how the voter set is managed.
type ClusterMode string

const (
	ModeStatic  ClusterMode = "static"  // voter set fixed at startup via PEER_NODES
	ModeDynamic ClusterMode = "dynamic" // voter set managed through Raft log entries
)

// MembershipAction is the operation carried in a membership log entry.
type MembershipAction string

const (
	ActionAddVoter    MembershipAction = "add_voter"
	ActionRemoveVoter MembershipAction = "remove_voter"
)

// MembershipChange is serialised into LogEntry.Command when CommandType == CmdMembership.
type MembershipChange struct {
	Action MembershipAction
	NodeID string
	Addr   string // raft listen address of the node being added/removed
}

// NodeState tracks a single node in the cluster view.
type NodeState string

const (
	NodeObserver NodeState = "observer" // replicates log, does not vote
	NodeVoter    NodeState = "voter"    // participates in elections and quorum
)

// Member represents one node in the cluster.
type Member struct {
	ID    string
	Addr  string
	State NodeState
}

// VoterSet is the authoritative list of who votes and who is an observer.
// In static mode it is populated at startup and never changes.
// In dynamic mode it is mutated only when a membership log entry is committed.
type VoterSet struct {
	mu      sync.RWMutex
	members map[string]*Member // keyed by NodeID
}

func newVoterSet() *VoterSet {
	return &VoterSet{members: make(map[string]*Member)}
}

// AddVoter promotes a node to voter (or inserts it as voter if new).
func (v *VoterSet) AddVoter(id, addr string) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.members[id] = &Member{ID: id, Addr: addr, State: NodeVoter}
}

// AddObserver registers a node as observer. Safe to call multiple times.
func (v *VoterSet) AddObserver(id, addr string) {
	v.mu.Lock()
	defer v.mu.Unlock()
	if _, exists := v.members[id]; !exists {
		v.members[id] = &Member{ID: id, Addr: addr, State: NodeObserver}
	}
}

// Remove removes a node entirely from the set.
func (v *VoterSet) Remove(id string) {
	v.mu.Lock()
	defer v.mu.Unlock()
	delete(v.members, id)
}

// Voters returns addresses of all current voters except the given self ID.
func (v *VoterSet) Voters(selfID string) []string {
	v.mu.RLock()
	defer v.mu.RUnlock()
	out := make([]string, 0, len(v.members))
	for id, m := range v.members {
		if id != selfID && m.State == NodeVoter {
			out = append(out, m.Addr)
		}
	}
	return out
}

// QuorumSize returns the minimum votes needed for a majority (including self).
func (v *VoterSet) QuorumSize() int {
	v.mu.RLock()
	defer v.mu.RUnlock()
	total := 0
	for _, m := range v.members {
		if m.State == NodeVoter {
			total++
		}
	}
	return total/2 + 1
}

// Members returns a snapshot of all members (voters + observers).
func (v *VoterSet) Members() []Member {
	v.mu.RLock()
	defer v.mu.RUnlock()
	out := make([]Member, 0, len(v.members))
	for _, m := range v.members {
		out = append(out, *m)
	}
	return out
}

// IsVoter reports whether id is a current voter.
func (v *VoterSet) IsVoter(id string) bool {
	v.mu.RLock()
	defer v.mu.RUnlock()
	m, ok := v.members[id]
	return ok && m.State == NodeVoter
}

// allPeerAddrs returns addresses of every member except selfID (voters + observers).
// The leader uses this to replicate to everyone, but only voters count for quorum.
func (v *VoterSet) allPeerAddrs(selfID string) []string {
	v.mu.RLock()
	defer v.mu.RUnlock()
	out := make([]string, 0, len(v.members))
	for id, m := range v.members {
		if id != selfID && m.Addr != "" {
			out = append(out, m.Addr)
		}
	}
	return out
}

// addrOf returns the Raft listen address for the given nodeID.
func (v *VoterSet) addrOf(nodeID string) string {
	v.mu.RLock()
	defer v.mu.RUnlock()
	if m, ok := v.members[nodeID]; ok {
		return m.Addr
	}
	return ""
}

// encodeMembershipChange serialises a MembershipChange into bytes for LogEntry.Command.
func encodeMembershipChange(mc MembershipChange) ([]byte, error) {
	return json.Marshal(mc)
}

// decodeMembershipChange deserialises bytes from LogEntry.Command.
func decodeMembershipChange(data []byte) (MembershipChange, error) {
	var mc MembershipChange
	return mc, json.Unmarshal(data, &mc)
}
