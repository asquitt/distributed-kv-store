package storage

import (
	"testing"
)

func TestStoreOperations(t *testing.T) {
	store := NewStore()
	
	// Test Set and Get
	err := store.Set("key1", "value1")
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}
	
	val, err := store.Get("key1")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if val != "value1" {
		t.Errorf("Expected 'value1', got '%s'", val)
	}
	
	// Test Get non-existent key
	_, err = store.Get("nonexistent")
	if err != ErrKeyNotFound {
		t.Errorf("Expected ErrKeyNotFound, got %v", err)
	}
}