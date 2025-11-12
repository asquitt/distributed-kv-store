package storage

import (
	"errors"
	"sync"
)

var (
	ErrKeyNotFound = errors.New("key not found")
	ErrEmptyKey    = errors.New("key cannot be empty")
)

// Store represents an in-memory key-value store with thread-safety
type Store struct {
	mu   sync.RWMutex
	data map[string]string
}

// NewStore creates a new Store instance
func NewStore() *Store {
	return &Store{
		data: make(map[string]string),
	}
}

// Get retrieves a value by key
func (s *Store) Get(key string) (string, error) {
	if key == "" {
		return "", ErrEmptyKey
	}
	
	s.mu.RLock()
	defer s.mu.RUnlock()
	
	val, exists := s.data[key]
	if !exists {
		return "", ErrKeyNotFound
	}
	return val, nil
}

// Set stores a key-value pair
func (s *Store) Set(key, value string) error {
	if key == "" {
		return ErrEmptyKey
	}
	
	s.mu.Lock()
	defer s.mu.Unlock()
	
	s.data[key] = value
	return nil
}

// Delete removes a key-value pair
func (s *Store) Delete(key string) error {
	if key == "" {
		return ErrEmptyKey
	}
	
	s.mu.Lock()
	defer s.mu.Unlock()
	
	if _, exists := s.data[key]; !exists {
		return ErrKeyNotFound
	}
	
	delete(s.data, key)
	return nil
}