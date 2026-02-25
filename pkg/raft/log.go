package raft

// Command represents a state machine command.
type Command struct {
	Op    string `json:"op"`    // "set" or "delete"
	Key   string `json:"key"`
	Value string `json:"value,omitempty"`
}

// LogEntry represents a single entry in the Raft log.
type LogEntry struct {
	Term    int     `json:"term"`
	Index   int     `json:"index"`
	Command Command `json:"command"`
}
