package raft

import (
	"log"
	"math/rand"
	"sync"
	"time"
)

// NodeState represents the current role of a Raft node.
type NodeState int

const (
	Follower NodeState = iota
	Candidate
	Leader
)

func (s NodeState) String() string {
	switch s {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	default:
		return "Unknown"
	}
}

const (
	minElectionTimeout = 150 * time.Millisecond
	maxElectionTimeout = 300 * time.Millisecond
	heartbeatInterval  = 50 * time.Millisecond
)

// ApplyMsg is sent on the apply channel when a log entry is committed.
type ApplyMsg struct {
	Command Command
	Index   int
	Term    int
}

// Node implements the Raft consensus algorithm.
type Node struct {
	mu sync.Mutex

	// Identity
	id    string
	peers []string

	// Persistent state
	currentTerm int
	votedFor    string
	log         []LogEntry

	// Volatile state
	commitIndex int
	lastApplied int
	state       NodeState
	leaderID    string

	// Leader volatile state
	nextIndex  map[string]int
	matchIndex map[string]int

	// Channels
	applyCh chan ApplyMsg
	stopCh  chan struct{}

	// Components
	transport Transport

	// Timers
	electionTimer *time.Timer
	rand          *rand.Rand
}

// NewNode creates a new Raft node.
func NewNode(id string, peers []string, transport Transport, applyCh chan ApplyMsg) *Node {
	r := rand.New(rand.NewSource(time.Now().UnixNano() + int64(len(id))))
	n := &Node{
		id:            id,
		peers:         peers,
		currentTerm:   0,
		votedFor:      "",
		log:           make([]LogEntry, 0),
		commitIndex:   0,
		lastApplied:   0,
		state:         Follower,
		nextIndex:     make(map[string]int),
		matchIndex:    make(map[string]int),
		applyCh:       applyCh,
		stopCh:        make(chan struct{}),
		transport:     transport,
		rand:          r,
		electionTimer: time.NewTimer(minElectionTimeout + time.Duration(r.Int63n(int64(maxElectionTimeout-minElectionTimeout)))),
	}
	return n
}

// SetTransport sets the transport for the node. Used when the transport
// needs a reference to the node (circular dependency at construction time).
func (n *Node) SetTransport(t Transport) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.transport = t
}

// Start begins the Raft node's operation.
func (n *Node) Start() {
	n.mu.Lock()
	n.electionTimer.Reset(n.randomElectionTimeout())
	n.mu.Unlock()

	go n.run()
}

// Stop shuts down the Raft node.
func (n *Node) Stop() {
	close(n.stopCh)
}

// Propose submits a command to be replicated. Returns false if this node is not the leader.
func (n *Node) Propose(cmd Command) bool {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.state != Leader {
		return false
	}

	entry := LogEntry{
		Term:    n.currentTerm,
		Index:   n.lastLogIndex() + 1,
		Command: cmd,
	}
	n.log = append(n.log, entry)
	log.Printf("[%s] Leader appended entry at index %d", n.id, entry.Index)

	// Immediately trigger replication
	go n.replicateToAll()

	return true
}

// State returns the current node state.
func (n *Node) State() NodeState {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.state
}

// LeaderID returns the current known leader ID.
func (n *Node) LeaderID() string {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.leaderID
}

// run is the main event loop.
func (n *Node) run() {
	for {
		select {
		case <-n.stopCh:
			return
		case <-n.electionTimer.C:
			n.handleElectionTimeout()
		}
	}
}

func (n *Node) handleElectionTimeout() {
	n.mu.Lock()
	if n.state == Leader {
		// Leaders don't time out; reset and ignore
		n.electionTimer.Reset(n.randomElectionTimeout())
		n.mu.Unlock()
		return
	}

	// Convert to candidate
	n.state = Candidate
	n.currentTerm++
	n.votedFor = n.id
	term := n.currentTerm
	lastLogIndex := n.lastLogIndex()
	lastLogTerm := n.lastLogTerm()
	n.electionTimer.Reset(n.randomElectionTimeout())
	n.mu.Unlock()

	log.Printf("[%s] Starting election for term %d", n.id, term)

	votes := 1 // vote for self
	var votesMu sync.Mutex
	finished := make(chan struct{})

	for _, peer := range n.peers {
		go func(peer string) {
			defer func() { finished <- struct{}{} }()

			resp, err := n.transport.RequestVote(peer, &RequestVoteRequest{
				Term:         term,
				CandidateID:  n.id,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			})
			if err != nil {
				return
			}

			n.mu.Lock()
			defer n.mu.Unlock()

			if resp.Term > n.currentTerm {
				n.stepDown(resp.Term)
				return
			}

			if n.state != Candidate || n.currentTerm != term {
				return
			}

			if resp.VoteGranted {
				votesMu.Lock()
				votes++
				won := votes > (len(n.peers)+1)/2
				votesMu.Unlock()

				if won {
					n.becomeLeader()
				}
			}
		}(peer)
	}

	// Wait for all RPCs to complete (or timeout via election timer)
	go func() {
		for i := 0; i < len(n.peers); i++ {
			<-finished
		}
	}()
}

func (n *Node) becomeLeader() {
	if n.state != Candidate {
		return
	}
	log.Printf("[%s] Became leader for term %d", n.id, n.currentTerm)
	n.state = Leader
	n.leaderID = n.id

	// Initialize leader state
	nextIdx := n.lastLogIndex() + 1
	for _, peer := range n.peers {
		n.nextIndex[peer] = nextIdx
		n.matchIndex[peer] = 0
	}

	// Start heartbeats
	go n.heartbeatLoop()
}

func (n *Node) heartbeatLoop() {
	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()

	// Send initial heartbeat immediately
	n.replicateToAll()

	for {
		select {
		case <-n.stopCh:
			return
		case <-ticker.C:
			n.mu.Lock()
			isLeader := n.state == Leader
			n.mu.Unlock()
			if !isLeader {
				return
			}
			n.replicateToAll()
		}
	}
}

func (n *Node) replicateToAll() {
	n.mu.Lock()
	if n.state != Leader {
		n.mu.Unlock()
		return
	}
	term := n.currentTerm
	n.mu.Unlock()

	for _, peer := range n.peers {
		go n.replicateTo(peer, term)
	}
}

func (n *Node) replicateTo(peer string, term int) {
	n.mu.Lock()
	if n.state != Leader || n.currentTerm != term {
		n.mu.Unlock()
		return
	}

	nextIdx := n.nextIndex[peer]
	prevLogIndex := nextIdx - 1
	prevLogTerm := 0
	if prevLogIndex > 0 && prevLogIndex <= len(n.log) {
		prevLogTerm = n.log[prevLogIndex-1].Term
	}

	var entries []LogEntry
	if nextIdx <= len(n.log) {
		entries = make([]LogEntry, len(n.log)-nextIdx+1)
		copy(entries, n.log[nextIdx-1:])
	}

	req := &AppendEntriesRequest{
		Term:         n.currentTerm,
		LeaderID:     n.id,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: n.commitIndex,
	}
	n.mu.Unlock()

	resp, err := n.transport.AppendEntries(peer, req)
	if err != nil {
		return
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	if resp.Term > n.currentTerm {
		n.stepDown(resp.Term)
		return
	}

	if n.state != Leader || n.currentTerm != term {
		return
	}

	if resp.Success {
		if len(entries) > 0 {
			n.nextIndex[peer] = entries[len(entries)-1].Index + 1
			n.matchIndex[peer] = entries[len(entries)-1].Index
			n.advanceCommitIndex()
		}
	} else {
		// Decrement nextIndex and retry
		if n.nextIndex[peer] > 1 {
			n.nextIndex[peer]--
		}
	}
}

func (n *Node) advanceCommitIndex() {
	for idx := n.commitIndex + 1; idx <= n.lastLogIndex(); idx++ {
		if n.log[idx-1].Term != n.currentTerm {
			continue
		}
		matches := 1 // leader has it
		for _, peer := range n.peers {
			if n.matchIndex[peer] >= idx {
				matches++
			}
		}
		if matches > (len(n.peers)+1)/2 {
			n.commitIndex = idx
		}
	}
	n.applyCommitted()
}

func (n *Node) applyCommitted() {
	for n.lastApplied < n.commitIndex {
		n.lastApplied++
		entry := n.log[n.lastApplied-1]
		msg := ApplyMsg{
			Command: entry.Command,
			Index:   entry.Index,
			Term:    entry.Term,
		}
		// Non-blocking send
		select {
		case n.applyCh <- msg:
		default:
			go func() { n.applyCh <- msg }()
		}
	}
}

// HandleRequestVote processes an incoming RequestVote RPC.
func (n *Node) HandleRequestVote(req *RequestVoteRequest) *RequestVoteResponse {
	n.mu.Lock()
	defer n.mu.Unlock()

	resp := &RequestVoteResponse{Term: n.currentTerm}

	if req.Term < n.currentTerm {
		return resp
	}

	if req.Term > n.currentTerm {
		n.stepDown(req.Term)
		resp.Term = n.currentTerm
	}

	// Grant vote if we haven't voted yet (or voted for this candidate)
	// and candidate's log is at least as up-to-date as ours
	if (n.votedFor == "" || n.votedFor == req.CandidateID) && n.isLogUpToDate(req.LastLogIndex, req.LastLogTerm) {
		n.votedFor = req.CandidateID
		resp.VoteGranted = true
		n.electionTimer.Reset(n.randomElectionTimeout())
	}

	return resp
}

// HandleAppendEntries processes an incoming AppendEntries RPC.
func (n *Node) HandleAppendEntries(req *AppendEntriesRequest) *AppendEntriesResponse {
	n.mu.Lock()
	defer n.mu.Unlock()

	resp := &AppendEntriesResponse{Term: n.currentTerm}

	if req.Term < n.currentTerm {
		return resp
	}

	if req.Term > n.currentTerm {
		n.stepDown(req.Term)
		resp.Term = n.currentTerm
	}

	// Valid leader heartbeat - reset election timer
	n.state = Follower
	n.leaderID = req.LeaderID
	n.electionTimer.Reset(n.randomElectionTimeout())

	// Check if log contains entry at prevLogIndex with prevLogTerm
	if req.PrevLogIndex > 0 {
		if req.PrevLogIndex > len(n.log) {
			return resp
		}
		if n.log[req.PrevLogIndex-1].Term != req.PrevLogTerm {
			// Conflict: delete this entry and all that follow
			n.log = n.log[:req.PrevLogIndex-1]
			return resp
		}
	}

	// Append new entries
	for _, entry := range req.Entries {
		idx := entry.Index
		if idx <= len(n.log) {
			if n.log[idx-1].Term != entry.Term {
				n.log = n.log[:idx-1]
				n.log = append(n.log, entry)
			}
		} else {
			n.log = append(n.log, entry)
		}
	}

	// Update commit index
	if req.LeaderCommit > n.commitIndex {
		lastNewIdx := n.lastLogIndex()
		if req.LeaderCommit < lastNewIdx {
			n.commitIndex = req.LeaderCommit
		} else {
			n.commitIndex = lastNewIdx
		}
		n.applyCommitted()
	}

	resp.Success = true
	return resp
}

// stepDown converts the node to a follower with the given term.
// Must be called with n.mu held.
func (n *Node) stepDown(term int) {
	n.currentTerm = term
	n.state = Follower
	n.votedFor = ""
	n.electionTimer.Reset(n.randomElectionTimeout())
}

func (n *Node) lastLogIndex() int {
	return len(n.log)
}

func (n *Node) lastLogTerm() int {
	if len(n.log) == 0 {
		return 0
	}
	return n.log[len(n.log)-1].Term
}

func (n *Node) isLogUpToDate(lastIndex, lastTerm int) bool {
	myLastTerm := n.lastLogTerm()
	if lastTerm != myLastTerm {
		return lastTerm > myLastTerm
	}
	return lastIndex >= n.lastLogIndex()
}

func (n *Node) randomElectionTimeout() time.Duration {
	d := minElectionTimeout + time.Duration(n.rand.Int63n(int64(maxElectionTimeout-minElectionTimeout)))
	return d
}
