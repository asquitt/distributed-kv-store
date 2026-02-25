package raft

import (
	"sync"
	"testing"
	"time"
)

// mockTransport implements Transport for testing without network I/O.
type mockTransport struct {
	mu    sync.Mutex
	nodes map[string]*Node
}

func newMockTransport() *mockTransport {
	return &mockTransport{nodes: make(map[string]*Node)}
}

func (t *mockTransport) register(id string, node *Node) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.nodes[id] = node
}

func (t *mockTransport) RequestVote(target string, req *RequestVoteRequest) (*RequestVoteResponse, error) {
	t.mu.Lock()
	node, ok := t.nodes[target]
	t.mu.Unlock()
	if !ok {
		return nil, &netError{}
	}
	return node.HandleRequestVote(req), nil
}

func (t *mockTransport) AppendEntries(target string, req *AppendEntriesRequest) (*AppendEntriesResponse, error) {
	t.mu.Lock()
	node, ok := t.nodes[target]
	t.mu.Unlock()
	if !ok {
		return nil, &netError{}
	}
	return node.HandleAppendEntries(req), nil
}

type netError struct{}

func (e *netError) Error() string { return "node unreachable" }

func createCluster(t *testing.T, n int) ([]*Node, []chan ApplyMsg, *mockTransport) {
	t.Helper()
	transport := newMockTransport()
	nodes := make([]*Node, n)
	channels := make([]chan ApplyMsg, n)

	ids := make([]string, n)
	for i := 0; i < n; i++ {
		ids[i] = string(rune('A' + i))
	}

	for i := 0; i < n; i++ {
		var peers []string
		for j := 0; j < n; j++ {
			if i != j {
				peers = append(peers, ids[j])
			}
		}
		channels[i] = make(chan ApplyMsg, 100)
		nodes[i] = NewNode(ids[i], peers, transport, channels[i])
		transport.register(ids[i], nodes[i])
	}

	return nodes, channels, transport
}

func waitForLeader(t *testing.T, nodes []*Node, timeout time.Duration) *Node {
	t.Helper()
	deadline := time.After(timeout)
	for {
		select {
		case <-deadline:
			t.Fatal("timed out waiting for leader election")
			return nil
		default:
			for _, n := range nodes {
				if n.State() == Leader {
					return n
				}
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
}

func TestLeaderElection(t *testing.T) {
	nodes, _, _ := createCluster(t, 3)
	defer func() {
		for _, n := range nodes {
			n.Stop()
		}
	}()

	for _, n := range nodes {
		n.Start()
	}

	leader := waitForLeader(t, nodes, 3*time.Second)
	if leader == nil {
		t.Fatal("no leader elected")
	}

	// Verify exactly one leader
	leaderCount := 0
	for _, n := range nodes {
		if n.State() == Leader {
			leaderCount++
		}
	}
	if leaderCount != 1 {
		t.Fatalf("expected exactly 1 leader, got %d", leaderCount)
	}
}

func TestLogReplication(t *testing.T) {
	nodes, channels, _ := createCluster(t, 3)
	defer func() {
		for _, n := range nodes {
			n.Stop()
		}
	}()

	for _, n := range nodes {
		n.Start()
	}

	leader := waitForLeader(t, nodes, 3*time.Second)

	// Propose a command
	cmd := Command{Op: "set", Key: "foo", Value: "bar"}
	ok := leader.Propose(cmd)
	if !ok {
		t.Fatal("leader rejected proposal")
	}

	// Wait for the command to be applied on at least 2 nodes (majority)
	applied := 0
	deadline := time.After(3 * time.Second)
	for applied < 2 {
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for replication, only %d nodes applied", applied)
		default:
			for _, ch := range channels {
				select {
				case msg := <-ch:
					if msg.Command.Key == "foo" && msg.Command.Value == "bar" {
						applied++
					}
				default:
				}
			}
			if applied < 2 {
				time.Sleep(10 * time.Millisecond)
			}
		}
	}
}

func TestRejectStaleLeader(t *testing.T) {
	node := NewNode("A", []string{"B"}, newMockTransport(), make(chan ApplyMsg, 10))

	// Simulate a stale AppendEntries from an old leader
	resp := node.HandleAppendEntries(&AppendEntriesRequest{
		Term:     0,
		LeaderID: "B",
	})
	// Node is at term 0 too, so this should succeed (same term)
	if !resp.Success {
		t.Error("expected success for same-term AppendEntries")
	}

	// Now bump our term
	node.HandleRequestVote(&RequestVoteRequest{
		Term:        5,
		CandidateID: "C",
	})

	// Stale AppendEntries should be rejected
	resp = node.HandleAppendEntries(&AppendEntriesRequest{
		Term:     3,
		LeaderID: "B",
	})
	if resp.Success {
		t.Error("expected rejection for stale term")
	}
}

func TestVoteGranting(t *testing.T) {
	transport := newMockTransport()
	node := NewNode("A", []string{"B", "C"}, transport, make(chan ApplyMsg, 10))
	node.electionTimer.Reset(time.Hour) // prevent elections

	// Node should grant vote to first requester
	resp := node.HandleRequestVote(&RequestVoteRequest{
		Term:        1,
		CandidateID: "B",
	})
	if !resp.VoteGranted {
		t.Error("expected vote to be granted to B")
	}

	// Node should not grant vote to second requester in same term
	resp = node.HandleRequestVote(&RequestVoteRequest{
		Term:        1,
		CandidateID: "C",
	})
	if resp.VoteGranted {
		t.Error("expected vote to be denied to C (already voted for B)")
	}
}

func TestNonLeaderRejectsProposal(t *testing.T) {
	node := NewNode("A", []string{"B"}, newMockTransport(), make(chan ApplyMsg, 10))
	// Node starts as follower
	ok := node.Propose(Command{Op: "set", Key: "k", Value: "v"})
	if ok {
		t.Error("follower should reject proposals")
	}
}
