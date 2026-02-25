package raft

// RequestVoteRequest is sent by candidates to gather votes.
type RequestVoteRequest struct {
	Term         int    `json:"term"`
	CandidateID  string `json:"candidate_id"`
	LastLogIndex int    `json:"last_log_index"`
	LastLogTerm  int    `json:"last_log_term"`
}

// RequestVoteResponse is the reply to a RequestVote RPC.
type RequestVoteResponse struct {
	Term        int  `json:"term"`
	VoteGranted bool `json:"vote_granted"`
}

// AppendEntriesRequest is sent by leaders to replicate log entries and as heartbeats.
type AppendEntriesRequest struct {
	Term         int        `json:"term"`
	LeaderID     string     `json:"leader_id"`
	PrevLogIndex int        `json:"prev_log_index"`
	PrevLogTerm  int        `json:"prev_log_term"`
	Entries      []LogEntry `json:"entries"`
	LeaderCommit int        `json:"leader_commit"`
}

// AppendEntriesResponse is the reply to an AppendEntries RPC.
type AppendEntriesResponse struct {
	Term    int  `json:"term"`
	Success bool `json:"success"`
}

// Transport defines the interface for Raft RPC communication between nodes.
type Transport interface {
	// RequestVote sends a RequestVote RPC to the target node.
	RequestVote(target string, req *RequestVoteRequest) (*RequestVoteResponse, error)

	// AppendEntries sends an AppendEntries RPC to the target node.
	AppendEntries(target string, req *AppendEntriesRequest) (*AppendEntriesResponse, error)
}
