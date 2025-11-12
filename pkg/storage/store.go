package storage

import (
	"errors"
	"sync"
)

var (
	ErrKeyNotFound = errors.New("key not found")
)

// Store represents an in-memory key-value store
type Store struct {
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
	val, exists := s.data[key]
	if !exists {
		return "", ErrKeyNotFound
	}
	return val, nil
}

// Set stores a key-value pair
func (s *Store) Set(key, value string) error {
	s.data[key] = value
	return nil
}