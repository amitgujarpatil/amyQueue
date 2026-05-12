package raft

import (
	"context"
	"errors"
	"log/slog"
	"math/rand"
	"sync"
	"time"
)

var ErrNotLeader = errors.New("not the leader")

type State int

const (
	Follower  State = iota
	Candidate State = iota
	Leader    State = iota
)

func (s State) String() string {
	switch s {
	case Follower:
		return "follower"
	case Candidate:
		return "candidate"
	case Leader:
		return "leader"
	}
	return "unknown"
}

// Config holds the tunable knobs for a Raft node.
type Config struct {
	ID                string
	Peers             []string    // initial voter addresses (static mode); ignored in dynamic mode after bootstrap
	Mode              ClusterMode // static or dynamic
	ElectionTimeoutMs int
	HeartbeatMs       int
}

// Node is a single Raft participant. It owns the state machine, the log, and
// drives the election and heartbeat loops. It knows nothing about the wire
// format — that is entirely the Transport's responsibility.
//
// In static mode: VoterSet is seeded from Config.Peers and never changes.
// In dynamic mode: VoterSet is mutated only when a CmdMembership log entry
// is committed — the only safe time to change it.
type Node struct {
	cfg       Config
	transport Transport
	log       *Log
	voters    *VoterSet
	logger    *slog.Logger

	mu          sync.Mutex
	state       State
	currentTerm uint64
	votedFor    string
	commitIndex uint64
	lastApplied uint64
	leaderID    string

	// leader only: next log index to send to each peer addr
	nextIndex  map[string]uint64
	matchIndex map[string]uint64

	heartbeatC chan struct{}
	stopC      chan struct{}
	doneC      chan struct{}
}

func NewNode(cfg Config, transport Transport, logger *slog.Logger) *Node {
	if logger == nil {
		logger = slog.Default()
	}
	if cfg.Mode == "" {
		cfg.Mode = ModeStatic
	}

	voters := newVoterSet()

	// seed voter set — self is always a voter
	voters.AddVoter(cfg.ID, "") // addr for self is not needed for outbound RPCs
	for _, addr := range cfg.Peers {
		voters.AddVoter(addr, addr) // in static mode ID == addr for simplicity
	}

	return &Node{
		cfg:        cfg,
		transport:  transport,
		log:        newLog(),
		voters:     voters,
		logger:     logger.With("node", cfg.ID),
		state:      Follower,
		heartbeatC: make(chan struct{}, 1),
		stopC:      make(chan struct{}),
		doneC:      make(chan struct{}),
		nextIndex:  make(map[string]uint64),
		matchIndex: make(map[string]uint64),
	}
}

// Start wires up the transport handlers and begins the Raft event loop.
func (n *Node) Start() error {
	handlers := Handlers{
		HandleVoteRequest:   n.handleVoteRequest,
		HandleAppendEntries: n.handleAppendEntries,
		HandleObserverJoin:  n.JoinAsObserver,
	}
	if err := n.transport.Start(handlers); err != nil {
		return err
	}
	go n.run()
	n.logger.Info("raft node started", "mode", n.cfg.Mode, "peers", n.cfg.Peers)
	return nil
}

// Stop shuts the node down gracefully.
func (n *Node) Stop() {
	close(n.stopC)
	<-n.doneC
	_ = n.transport.Close()
	n.logger.Info("raft node stopped")
}

func (n *Node) CurrentState() State {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.state
}

func (n *Node) Term() uint64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.currentTerm
}

// ─── AdminService implementation ─────────────────────────────────────────────

func (n *Node) ClusterStatus() ClusterStatusResponse {
	n.mu.Lock()
	defer n.mu.Unlock()
	return ClusterStatusResponse{
		LeaderID: n.leaderID,
		Term:     n.currentTerm,
		Members:  n.voters.Members(),
	}
}

// JoinAsObserver registers a new node as an observer so the leader starts
// replicating to it. Only the leader can do this; others return a redirect hint.
func (n *Node) JoinAsObserver(req ObserverJoinRequest) ObserverJoinResponse {
	n.mu.Lock()
	state := n.state
	leaderID := n.leaderID
	n.mu.Unlock()

	if state != Leader {
		leaderAddr := n.voters.addrOf(leaderID)
		return ObserverJoinResponse{
			LeaderID:   leaderID,
			LeaderAddr: leaderAddr,
			Err:        ErrNotLeader.Error(),
		}
	}

	if n.cfg.Mode == ModeStatic {
		return ObserverJoinResponse{Err: "cluster is in static mode, dynamic join not allowed"}
	}

	n.voters.AddObserver(req.NodeID, req.Addr)
	n.mu.Lock()
	n.nextIndex[req.Addr] = n.log.lastIndex() + 1
	n.matchIndex[req.Addr] = 0
	n.mu.Unlock()

	n.logger.Info("observer joined", "node_id", req.NodeID, "addr", req.Addr)
	return ObserverJoinResponse{Success: true}
}

// AddVoter promotes an observer to voter via a Raft log entry.
// Only the leader can call this; it blocks until the entry is committed.
func (n *Node) AddVoter(req AddVoterRequest) AddVoterResponse {
	n.mu.Lock()
	state := n.state
	leaderID := n.leaderID
	n.mu.Unlock()

	if state != Leader {
		return AddVoterResponse{Err: ErrNotLeader.Error() + ", leader is " + leaderID}
	}
	if n.cfg.Mode == ModeStatic {
		return AddVoterResponse{Err: "cluster is in static mode"}
	}

	cmd, err := encodeMembershipChange(MembershipChange{
		Action: ActionAddVoter,
		NodeID: req.NodeID,
		Addr:   req.Addr,
	})
	if err != nil {
		return AddVoterResponse{Err: err.Error()}
	}

	if err := n.appendAndWaitCommit(CmdMembership, cmd); err != nil {
		return AddVoterResponse{Err: err.Error()}
	}
	return AddVoterResponse{Success: true}
}

// RemoveVoter removes a voter via a Raft log entry.
func (n *Node) RemoveVoter(req RemoveVoterRequest) RemoveVoterResponse {
	n.mu.Lock()
	state := n.state
	leaderID := n.leaderID
	n.mu.Unlock()

	if state != Leader {
		return RemoveVoterResponse{Err: ErrNotLeader.Error() + ", leader is " + leaderID}
	}
	if n.cfg.Mode == ModeStatic {
		return RemoveVoterResponse{Err: "cluster is in static mode"}
	}

	cmd, err := encodeMembershipChange(MembershipChange{
		Action: ActionRemoveVoter,
		NodeID: req.NodeID,
	})
	if err != nil {
		return RemoveVoterResponse{Err: err.Error()}
	}

	if err := n.appendAndWaitCommit(CmdMembership, cmd); err != nil {
		return RemoveVoterResponse{Err: err.Error()}
	}
	return RemoveVoterResponse{Success: true}
}

// appendAndWaitCommit appends a log entry as leader and blocks until it is
// committed by a quorum. Simple poll — good enough for admin operations.
func (n *Node) appendAndWaitCommit(cmdType CommandType, cmd []byte) error {
	n.mu.Lock()
	idx := n.log.lastIndex() + 1
	entry := LogEntry{
		Index:   idx,
		Term:    n.currentTerm,
		Type:    cmdType,
		Command: cmd,
	}
	n.log.appendOne(entry)
	n.mu.Unlock()

	// wait for commit with a timeout
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		n.mu.Lock()
		committed := n.commitIndex >= idx
		n.mu.Unlock()
		if committed {
			return nil
		}
		time.Sleep(10 * time.Millisecond)
	}
	return errors.New("timeout waiting for log entry to commit")
}

// applyMembershipEntry applies a committed membership change to the VoterSet.
// Must be called with n.mu held.
func (n *Node) applyMembershipEntry(entry LogEntry) {
	mc, err := decodeMembershipChange(entry.Command)
	if err != nil {
		n.logger.Error("failed to decode membership change", "err", err)
		return
	}

	switch mc.Action {
	case ActionAddVoter:
		n.voters.AddVoter(mc.NodeID, mc.Addr)
		n.logger.Info("membership: voter added", "node_id", mc.NodeID, "addr", mc.Addr)
	case ActionRemoveVoter:
		n.voters.Remove(mc.NodeID)
		n.logger.Info("membership: voter removed", "node_id", mc.NodeID)
		// if we just removed ourselves, step down
		if mc.NodeID == n.cfg.ID {
			n.state = Follower
		}
	}
}

// ─── main event loop ──────────────────────────────────────────────────────────

func (n *Node) run() {
	defer close(n.doneC)
	for {
		n.mu.Lock()
		state := n.state
		n.mu.Unlock()

		switch state {
		case Follower:
			n.runFollower()
		case Candidate:
			n.runCandidate()
		case Leader:
			n.runLeader()
		}

		select {
		case <-n.stopC:
			return
		default:
		}
	}
}

// ─── follower ─────────────────────────────────────────────────────────────────

func (n *Node) runFollower() {
	timeout := n.electionTimeout()
	n.logger.Info("entering follower state", "term", n.Term(), "timeout", timeout)
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	for {
		select {
		case <-n.stopC:
			return
		case <-n.heartbeatC:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timer.Reset(n.electionTimeout())
		case <-timer.C:
			// only voters call elections — observers wait indefinitely
			n.mu.Lock()
			isVoter := n.voters.IsVoter(n.cfg.ID)
			n.mu.Unlock()
			if !isVoter {
				timer.Reset(n.electionTimeout())
				continue
			}
			n.logger.Info("election timeout, becoming candidate")
			n.mu.Lock()
			n.state = Candidate
			n.mu.Unlock()
			return
		}
	}
}

// ─── candidate ───────────────────────────────────────────────────────────────

func (n *Node) runCandidate() {
	n.mu.Lock()
	n.currentTerm++
	n.votedFor = n.cfg.ID
	term := n.currentTerm
	n.mu.Unlock()

	n.logger.Info("starting election", "term", term)

	votes := 1
	needed := n.voters.QuorumSize()
	peers := n.voters.Voters(n.cfg.ID)
	voteCh := make(chan bool, len(peers))

	req := VoteRequest{
		Term:         term,
		CandidateID:  n.cfg.ID,
		LastLogIndex: n.log.lastIndex(),
		LastLogTerm:  n.log.lastTerm(),
	}

	for _, peer := range peers {
		go func(addr string) {
			ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
			defer cancel()
			resp, err := n.transport.SendVoteRequest(ctx, addr, req)
			if err != nil {
				voteCh <- false
				return
			}
			n.mu.Lock()
			if resp.Term > n.currentTerm {
				n.currentTerm = resp.Term
				n.state = Follower
				n.votedFor = ""
			}
			n.mu.Unlock()
			voteCh <- resp.VoteGranted
		}(peer)
	}

	timeout := time.NewTimer(n.electionTimeout())
	defer timeout.Stop()

	for {
		select {
		case <-n.stopC:
			return
		case <-timeout.C:
			n.logger.Info("election timed out, restarting", "term", term)
			return
		case <-n.heartbeatC:
			n.mu.Lock()
			n.state = Follower
			n.mu.Unlock()
			return
		case granted := <-voteCh:
			if granted {
				votes++
				if votes >= needed {
					n.becomeLeader()
					return
				}
			}
		}
	}
}

func (n *Node) becomeLeader() {
	n.mu.Lock()
	n.state = Leader
	n.leaderID = n.cfg.ID
	nextIdx := n.log.lastIndex() + 1
	for _, addr := range n.voters.Voters(n.cfg.ID) {
		n.nextIndex[addr] = nextIdx
		n.matchIndex[addr] = 0
	}
	term := n.currentTerm
	n.mu.Unlock()
	n.logger.Info("became leader", "term", term)
}

// ─── leader ───────────────────────────────────────────────────────────────────

func (n *Node) runLeader() {
	heartbeat := time.NewTicker(time.Duration(n.cfg.HeartbeatMs) * time.Millisecond)
	defer heartbeat.Stop()
	n.broadcastHeartbeat()

	for {
		select {
		case <-n.stopC:
			return
		case <-heartbeat.C:
			n.broadcastHeartbeat()
			n.mu.Lock()
			state := n.state
			n.mu.Unlock()
			if state != Leader {
				return
			}
		}
	}
}

func (n *Node) broadcastHeartbeat() {
	n.mu.Lock()
	term := n.currentTerm
	leaderID := n.cfg.ID
	commitIndex := n.commitIndex
	n.mu.Unlock()

	// send to all voters + observers (observers need replication to catch up)
	allPeers := n.voters.allPeerAddrs(n.cfg.ID)

	for _, peer := range allPeers {
		go func(addr string) {
			n.mu.Lock()
			nextIdx := n.nextIndex[addr]
			if nextIdx == 0 {
				nextIdx = n.log.lastIndex() + 1
				n.nextIndex[addr] = nextIdx
			}
			n.mu.Unlock()

			prevLogIndex := nextIdx - 1
			prevLogEntry, _ := n.log.entry(prevLogIndex)

			req := AppendEntriesRequest{
				Term:         term,
				LeaderID:     leaderID,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogEntry.Term,
				Entries:      n.log.entriesFrom(nextIdx),
				LeaderCommit: commitIndex,
			}

			isHeartbeat := len(req.Entries) == 0
			n.logger.Debug("sending heartbeat",
				"to", addr,
				"term", req.Term,
				"commit_index", req.LeaderCommit,
				"prev_log_index", req.PrevLogIndex,
				"entries", len(req.Entries),
				"heartbeat_only", isHeartbeat,
			)

			ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
			defer cancel()
			resp, err := n.transport.SendAppendEntries(ctx, addr, req)
			if err != nil {
				n.logger.Debug("heartbeat failed", "to", addr, "err", err)
				return
			}

			n.mu.Lock()
			defer n.mu.Unlock()

			if resp.Term > n.currentTerm {
				n.currentTerm = resp.Term
				n.state = Follower
				n.votedFor = ""
				return
			}
			if resp.Success {
				n.matchIndex[addr] = req.PrevLogIndex + uint64(len(req.Entries))
				n.nextIndex[addr] = n.matchIndex[addr] + 1
				n.maybeAdvanceCommitIndex()
			} else {
				if resp.NextIndex > 0 && resp.NextIndex < n.nextIndex[addr] {
					n.nextIndex[addr] = resp.NextIndex
				} else if n.nextIndex[addr] > 1 {
					n.nextIndex[addr]--
				}
			}
		}(peer)
	}
}

// maybeAdvanceCommitIndex checks if a quorum of VOTERS (not observers) have
// replicated an entry. Must be called with n.mu held.
func (n *Node) maybeAdvanceCommitIndex() {
	quorum := n.voters.QuorumSize()
	lastIdx := n.log.lastIndex()
	for idx := lastIdx; idx > n.commitIndex; idx-- {
		entry, ok := n.log.entry(idx)
		if !ok || entry.Term != n.currentTerm {
			continue
		}
		// count only voters (observers don't count toward quorum)
		count := 1 // leader self
		for _, m := range n.voters.Members() {
			if m.ID == n.cfg.ID || m.State != NodeVoter {
				continue
			}
			if n.matchIndex[m.Addr] >= idx {
				count++
			}
		}
		if count >= quorum {
			n.commitIndex = idx
			n.logger.Info("committed log index", "index", idx)
			// apply all newly committed entries
			n.applyCommitted()
			break
		}
	}
}

// applyCommitted applies all log entries between lastApplied and commitIndex.
// Must be called with n.mu held.
func (n *Node) applyCommitted() {
	for n.lastApplied < n.commitIndex {
		n.lastApplied++
		entry, ok := n.log.entry(n.lastApplied)
		if !ok {
			continue
		}
		if entry.Type == CmdMembership {
			n.applyMembershipEntry(entry)
		}
		// CmdData entries: application state machine hook goes here in the future
	}
}

// ─── RPC handlers (called by transport) ─────────────────────────────────────

func (n *Node) handleVoteRequest(req VoteRequest) VoteResponse {
	n.mu.Lock()
	defer n.mu.Unlock()

	resp := VoteResponse{Term: n.currentTerm}

	if req.Term < n.currentTerm {
		return resp
	}
	if req.Term > n.currentTerm {
		n.currentTerm = req.Term
		n.state = Follower
		n.votedFor = ""
	}

	alreadyVoted := n.votedFor != "" && n.votedFor != req.CandidateID
	logOK := req.LastLogTerm > n.log.lastTerm() ||
		(req.LastLogTerm == n.log.lastTerm() && req.LastLogIndex >= n.log.lastIndex())

	if !alreadyVoted && logOK {
		n.votedFor = req.CandidateID
		resp.VoteGranted = true
		resp.Term = n.currentTerm
		n.logger.Info("granted vote", "to", req.CandidateID, "term", req.Term)
	}
	return resp
}

func (n *Node) handleAppendEntries(req AppendEntriesRequest) AppendEntriesResponse {
	n.mu.Lock()
	defer n.mu.Unlock()

	resp := AppendEntriesResponse{Term: n.currentTerm}

	if req.Term < n.currentTerm {
		return resp
	}
	if req.Term > n.currentTerm {
		n.currentTerm = req.Term
		n.votedFor = ""
	}
	n.state = Follower
	n.leaderID = req.LeaderID

	select {
	case n.heartbeatC <- struct{}{}:
	default:
	}

	n.logger.Debug("received heartbeat",
		"from_leader", req.LeaderID,
		"term", req.Term,
		"leader_commit", req.LeaderCommit,
		"prev_log_index", req.PrevLogIndex,
		"entries", len(req.Entries),
		"our_log_index", n.log.lastIndex(),
	)

	if req.PrevLogIndex > 0 {
		prev, ok := n.log.entry(req.PrevLogIndex)
		if !ok || prev.Term != req.PrevLogTerm {
			resp.NextIndex = n.log.lastIndex() + 1
			return resp
		}
	}

	if len(req.Entries) > 0 {
		n.log.append(req.PrevLogIndex, req.Entries)
	}

	if req.LeaderCommit > n.commitIndex {
		last := n.log.lastIndex()
		if req.LeaderCommit < last {
			n.commitIndex = req.LeaderCommit
		} else {
			n.commitIndex = last
		}
		n.applyCommitted()
	}

	resp.Success = true
	resp.Term = n.currentTerm
	return resp
}

// ─── helpers ─────────────────────────────────────────────────────────────────

func (n *Node) quorum() int {
	return n.voters.QuorumSize()
}

func (n *Node) electionTimeout() time.Duration {
	base := n.cfg.ElectionTimeoutMs
	if base <= 0 {
		base = 1000
	}
	jitter := rand.Intn(base / 2)
	return time.Duration(base+jitter) * time.Millisecond
}
