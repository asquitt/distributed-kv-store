package raft

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

// HTTPTransport implements Transport using HTTP/JSON RPCs.
type HTTPTransport struct {
	client *http.Client
	node   *Node
	mux    *http.ServeMux
}

// NewHTTPTransport creates a new HTTP-based transport.
func NewHTTPTransport(node *Node) *HTTPTransport {
	t := &HTTPTransport{
		client: &http.Client{Timeout: 2 * time.Second},
		node:   node,
		mux:    http.NewServeMux(),
	}
	t.mux.HandleFunc("/raft/vote", t.handleRequestVote)
	t.mux.HandleFunc("/raft/append", t.handleAppendEntries)
	return t
}

// Handler returns the HTTP handler for Raft RPCs.
func (t *HTTPTransport) Handler() http.Handler {
	return t.mux
}

// RequestVote sends a RequestVote RPC via HTTP.
func (t *HTTPTransport) RequestVote(target string, req *RequestVoteRequest) (*RequestVoteResponse, error) {
	body, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}

	resp, err := t.client.Post(fmt.Sprintf("http://%s/raft/vote", target), "application/json", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var result RequestVoteResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}
	return &result, nil
}

// AppendEntries sends an AppendEntries RPC via HTTP.
func (t *HTTPTransport) AppendEntries(target string, req *AppendEntriesRequest) (*AppendEntriesResponse, error) {
	body, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}

	resp, err := t.client.Post(fmt.Sprintf("http://%s/raft/append", target), "application/json", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var result AppendEntriesResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}
	return &result, nil
}

func (t *HTTPTransport) handleRequestVote(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req RequestVoteRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	resp := t.node.HandleRequestVote(&req)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

func (t *HTTPTransport) handleAppendEntries(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req AppendEntriesRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	resp := t.node.HandleAppendEntries(&req)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}
