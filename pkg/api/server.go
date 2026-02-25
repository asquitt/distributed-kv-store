package api

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"

	"github.com/asquitt/distributed-kv-store/pkg/raft"
	"github.com/asquitt/distributed-kv-store/pkg/storage"
)

// Server provides an HTTP API for the distributed key-value store.
type Server struct {
	store *storage.Store
	node  *raft.Node
	mux   *http.ServeMux
}

// NewServer creates a new API server.
func NewServer(store *storage.Store, node *raft.Node) *Server {
	s := &Server{store: store, node: node, mux: http.NewServeMux()}
	s.mux.HandleFunc("/kv/", s.handleKV)
	s.mux.HandleFunc("/health", s.handleHealth)
	return s
}

// Handler returns the HTTP handler.
func (s *Server) Handler() http.Handler {
	return s.mux
}

type kvResponse struct {
	Key      string `json:"key,omitempty"`
	Value    string `json:"value,omitempty"`
	Error    string `json:"error,omitempty"`
	LeaderID string `json:"leader_id,omitempty"`
}

func (s *Server) handleKV(w http.ResponseWriter, r *http.Request) {
	key := strings.TrimPrefix(r.URL.Path, "/kv/")
	if key == "" {
		writeJSON(w, http.StatusBadRequest, kvResponse{Error: "key required"})
		return
	}

	switch r.Method {
	case http.MethodGet:
		s.handleGet(w, key)
	case http.MethodPut:
		s.handlePut(w, r, key)
	case http.MethodDelete:
		s.handleDelete(w, key)
	default:
		writeJSON(w, http.StatusMethodNotAllowed, kvResponse{Error: "method not allowed"})
	}
}

func (s *Server) handleGet(w http.ResponseWriter, key string) {
	val, err := s.store.Get(key)
	if err != nil {
		if err == storage.ErrKeyNotFound {
			writeJSON(w, http.StatusNotFound, kvResponse{Error: "key not found"})
			return
		}
		writeJSON(w, http.StatusInternalServerError, kvResponse{Error: err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, kvResponse{Key: key, Value: val})
}

func (s *Server) handlePut(w http.ResponseWriter, r *http.Request, key string) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, kvResponse{Error: "failed to read body"})
		return
	}
	value := string(body)

	cmd := raft.Command{Op: "set", Key: key, Value: value}
	if !s.node.Propose(cmd) {
		writeJSON(w, http.StatusServiceUnavailable, kvResponse{
			Error:    "not leader",
			LeaderID: s.node.LeaderID(),
		})
		return
	}

	writeJSON(w, http.StatusOK, kvResponse{Key: key, Value: value})
}

func (s *Server) handleDelete(w http.ResponseWriter, key string) {
	cmd := raft.Command{Op: "delete", Key: key}
	if !s.node.Propose(cmd) {
		writeJSON(w, http.StatusServiceUnavailable, kvResponse{
			Error:    "not leader",
			LeaderID: s.node.LeaderID(),
		})
		return
	}

	writeJSON(w, http.StatusOK, kvResponse{Key: key})
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	state := s.node.State()
	resp := map[string]string{
		"status": "ok",
		"role":   state.String(),
		"leader": s.node.LeaderID(),
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}
