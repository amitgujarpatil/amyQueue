package raft

import (
	"context"
	"log/slog"
	"math/rand"
	"sync"
	"time"
)

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
	Peers             []string // addresses of all other nodes in the cluster
	ElectionTimeoutMs int      // base election timeout; actual is randomised +50%
	HeartbeatMs       int
}

// Node is a single Raft participant. It owns the state machine, the log, and
// drives the election and heartbeat loops. It knows nothing about the wire
// format — that is entirely the Transport's responsibility.
type Node struct {
	cfg       Config
	transport Transport
	log       *Log
	logger    *slog.Logger

	mu          sync.Mutex
	state       State
	currentTerm uint64
	votedFor    string
	commitIndex uint64
	lastApplied uint64

	// leader only: next log index to send to each peer
	nextIndex  map[string]uint64
	matchIndex map[string]uint64

	// channels that drive the main loop
	heartbeatC chan struct{} // reset election timer when a valid heartbeat arrives
	stopC      chan struct{}
	doneC      chan struct{}
}

func NewNode(cfg Config, transport Transport, logger *slog.Logger) *Node {
	if logger == nil {
		logger = slog.Default()
	}
	return &Node{
		cfg:        cfg,
		transport:  transport,
		log:        newLog(),
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
	}
	if err := n.transport.Start(handlers); err != nil {
		return err
	}
	go n.run()
	n.logger.Info("raft node started", "peers", n.cfg.Peers)
	return nil
}

// Stop shuts the node down gracefully.
func (n *Node) Stop() {
	close(n.stopC)
	<-n.doneC
	_ = n.transport.Close()
	n.logger.Info("raft node stopped")
}

// State returns the current role of the node (safe to call from any goroutine).
func (n *Node) CurrentState() State {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.state
}

// Term returns the current term.
func (n *Node) Term() uint64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.currentTerm
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
			// received valid leader contact — reset timer
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timer.Reset(n.electionTimeout())
		case <-timer.C:
			// leader silent for too long → become candidate
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
	n.votedFor = n.cfg.ID // vote for self
	term := n.currentTerm
	n.mu.Unlock()

	n.logger.Info("starting election", "term", term)

	votes := 1 // self
	needed := n.quorum()
	voteCh := make(chan bool, len(n.cfg.Peers))

	req := VoteRequest{
		Term:         term,
		CandidateID:  n.cfg.ID,
		LastLogIndex: n.log.lastIndex(),
		LastLogTerm:  n.log.lastTerm(),
	}

	for _, peer := range n.cfg.Peers {
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
			// split vote or no quorum — start a new election
			n.logger.Info("election timed out, restarting", "term", term)
			return
		case <-n.heartbeatC:
			// a leader appeared while we were campaigning
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
	// initialise leader tracking indexes
	nextIdx := n.log.lastIndex() + 1
	for _, peer := range n.cfg.Peers {
		n.nextIndex[peer] = nextIdx
		n.matchIndex[peer] = 0
	}
	term := n.currentTerm
	n.mu.Unlock()
	n.logger.Info("became leader", "term", term)
}

// ─── leader ───────────────────────────────────────────────────────────────────

func (n *Node) runLeader() {
	heartbeat := time.NewTicker(time.Duration(n.cfg.HeartbeatMs) * time.Millisecond)
	defer heartbeat.Stop()

	// send an immediate heartbeat so followers reset their timers right away
	n.broadcastHeartbeat()

	for {
		select {
		case <-n.stopC:
			return
		case <-heartbeat.C:
			n.broadcastHeartbeat()
			// after broadcasting, check if we still hold leadership
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

	for _, peer := range n.cfg.Peers {
		go func(addr string) {
			n.mu.Lock()
			nextIdx := n.nextIndex[addr]
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

			ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
			defer cancel()
			resp, err := n.transport.SendAppendEntries(ctx, addr, req)
			if err != nil {
				return
			}

			n.mu.Lock()
			defer n.mu.Unlock()

			if resp.Term > n.currentTerm {
				// discovered a higher term — step down
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
				// follower rejected — back off next index
				if resp.NextIndex > 0 && resp.NextIndex < n.nextIndex[addr] {
					n.nextIndex[addr] = resp.NextIndex
				} else if n.nextIndex[addr] > 1 {
					n.nextIndex[addr]--
				}
			}
		}(peer)
	}
}

// maybeAdvanceCommitIndex checks if a new log index can be committed (quorum
// of matchIndex values). Must be called with n.mu held.
func (n *Node) maybeAdvanceCommitIndex() {
	quorum := n.quorum()
	lastIdx := n.log.lastIndex()
	for idx := lastIdx; idx > n.commitIndex; idx-- {
		entry, ok := n.log.entry(idx)
		if !ok || entry.Term != n.currentTerm {
			continue
		}
		count := 1 // leader itself
		for _, match := range n.matchIndex {
			if match >= idx {
				count++
			}
		}
		if count >= quorum {
			n.commitIndex = idx
			n.logger.Info("committed log index", "index", idx)
			break
		}
	}
}

// ─── RPC handlers (called by transport) ─────────────────────────────────────

func (n *Node) handleVoteRequest(req VoteRequest) VoteResponse {
	n.mu.Lock()
	defer n.mu.Unlock()

	resp := VoteResponse{Term: n.currentTerm}

	if req.Term < n.currentTerm {
		return resp // stale term — deny
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
		return resp // stale leader
	}

	// valid leader contact — reset election timer
	if req.Term > n.currentTerm {
		n.currentTerm = req.Term
		n.votedFor = ""
	}
	n.state = Follower

	// non-blocking send to heartbeat channel
	select {
	case n.heartbeatC <- struct{}{}:
	default:
	}

	// consistency check: does our log contain prevLogIndex with prevLogTerm?
	if req.PrevLogIndex > 0 {
		prev, ok := n.log.entry(req.PrevLogIndex)
		if !ok || prev.Term != req.PrevLogTerm {
			resp.NextIndex = n.log.lastIndex() + 1
			return resp // log inconsistency
		}
	}

	// append any new entries (truncates conflicting suffix)
	if len(req.Entries) > 0 {
		n.log.append(req.PrevLogIndex, req.Entries)
	}

	// advance commit index
	if req.LeaderCommit > n.commitIndex {
		last := n.log.lastIndex()
		if req.LeaderCommit < last {
			n.commitIndex = req.LeaderCommit
		} else {
			n.commitIndex = last
		}
	}

	resp.Success = true
	resp.Term = n.currentTerm
	return resp
}

// ─── helpers ─────────────────────────────────────────────────────────────────

func (n *Node) quorum() int {
	total := len(n.cfg.Peers) + 1 // peers + self
	return total/2 + 1
}

func (n *Node) electionTimeout() time.Duration {
	base := n.cfg.ElectionTimeoutMs
	if base <= 0 {
		base = 1000
	}
	// randomise between base and base*1.5 to avoid split votes
	jitter := rand.Intn(base / 2)
	return time.Duration(base+jitter) * time.Millisecond
}
