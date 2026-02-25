package api

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/asquitt/distributed-kv-store/pkg/raft"
	"github.com/asquitt/distributed-kv-store/pkg/storage"
)

// stubTransport satisfies raft.Transport for unit tests that don't need networking.
type stubTransport struct{}

func (s *stubTransport) RequestVote(string, *raft.RequestVoteRequest) (*raft.RequestVoteResponse, error) {
	return &raft.RequestVoteResponse{}, nil
}
func (s *stubTransport) AppendEntries(string, *raft.AppendEntriesRequest) (*raft.AppendEntriesResponse, error) {
	return &raft.AppendEntriesResponse{}, nil
}

func setupTestServer(t *testing.T) (*Server, *storage.Store) {
	t.Helper()
	store := storage.NewStore()
	applyCh := make(chan raft.ApplyMsg, 100)
	node := raft.NewNode("test", nil, &stubTransport{}, applyCh)
	srv := NewServer(store, node)
	return srv, store
}

func TestGetExistingKey(t *testing.T) {
	srv, store := setupTestServer(t)
	store.Set("hello", "world")

	req := httptest.NewRequest(http.MethodGet, "/kv/hello", nil)
	w := httptest.NewRecorder()
	srv.Handler().ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	if !strings.Contains(w.Body.String(), "world") {
		t.Errorf("expected body to contain 'world', got %s", w.Body.String())
	}
}

func TestGetMissingKey(t *testing.T) {
	srv, _ := setupTestServer(t)

	req := httptest.NewRequest(http.MethodGet, "/kv/missing", nil)
	w := httptest.NewRecorder()
	srv.Handler().ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", w.Code)
	}
}

func TestPutNotLeader(t *testing.T) {
	srv, _ := setupTestServer(t)

	req := httptest.NewRequest(http.MethodPut, "/kv/key1", strings.NewReader("value1"))
	w := httptest.NewRecorder()
	srv.Handler().ServeHTTP(w, req)

	// Node is a follower, so writes should be rejected
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d", w.Code)
	}
}

func TestHealthEndpoint(t *testing.T) {
	srv, _ := setupTestServer(t)

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	w := httptest.NewRecorder()
	srv.Handler().ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	if !strings.Contains(w.Body.String(), "ok") {
		t.Errorf("expected 'ok' in health response, got %s", w.Body.String())
	}
}

func TestMissingKey(t *testing.T) {
	srv, _ := setupTestServer(t)

	req := httptest.NewRequest(http.MethodGet, "/kv/", nil)
	w := httptest.NewRecorder()
	srv.Handler().ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}

