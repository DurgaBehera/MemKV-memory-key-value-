package store

import (
	"fmt"
	"errors"
	"sync"
	"time"
)

// Store represents the in-memory key-value store with thread safety
type Store struct {
	data map[string]*Entry
	mu   sync.RWMutex
}

// NewStore creates a new store instance
func NewStore() *Store {
	return &Store{
		data: make(map[string]*Entry),
	}
}

// Set stores a key-value pair
func (s *Store) Set(key, value string) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.data[key] = NewEntry(value, 0)
	return 1 // Always returns 1 for SET (key was set)
}

// Get retrieves a value by key
func (s *Store) Get(key string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	entry, exists := s.data[key]
	if !exists || entry.IsExpired() {
		if exists && entry.IsExpired() {
			// Clean up expired entry
			s.mu.RUnlock()
			s.mu.Lock()
			delete(s.data, key)
			s.mu.Unlock()
			s.mu.RLock()
		}
		return "", false
	}
	return entry.Value, true
}

// Delete removes a key and returns 1 if key existed, 0 otherwise
func (s *Store) Delete(key string) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.data[key]; exists {
		delete(s.data, key)
		return 1
	}
	return 0
}

// Exists checks if a key exists and is not expired
func (s *Store) Exists(key string) int {
	s.mu.RLock()
	defer s.mu.RUnlock()

	entry, exists := s.data[key]
	if !exists || entry.IsExpired() {
		if exists && entry.IsExpired() {
			// Clean up expired entry
			s.mu.RUnlock()
			s.mu.Lock()
			delete(s.data, key)
			s.mu.Unlock()
			s.mu.RLock()
		}
		return 0
	}
	return 1
}

// Increment increments a numeric value by 1 and returns the new value
func (s *Store) Increment(key string) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	entry, exists := s.data[key]
	if !exists {
		// Key doesn't exist, treat as 0
		s.data[key] = NewEntry("1", 0)
		return 1, nil
	}

	if entry.IsExpired() {
		delete(s.data, key)
		s.data[key] = NewEntry("1", 0)
		return 1, nil
	}

	// Try to parse as integer
	var current int64
	var err error
	if _, err = fmt.Sscanf(entry.Value, "%d", &current); err != nil {
		return 0, errors.New("value is not an integer")
	}

	newValue := current + 1
	s.data[key] = NewEntry(fmt.Sprintf("%d", newValue), getTTL(entry))
	return newValue, nil
}

// Expire sets a TTL on a key and returns 1 if key existed, 0 otherwise
func (s *Store) Expire(key string, ttl time.Duration) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	entry, exists := s.data[key]
	if !exists {
		return 0
	}

	if ttl <= 0 {
		delete(s.data, key)
		return 1
	}

	expiresAt := time.Now().Add(ttl)
	s.data[key] = &Entry{
		Value:     entry.Value,
		ExpiresAt: &expiresAt,
	}
	return 1
}

// TTL returns the time to live for a key in seconds
func (s *Store) TTL(key string) int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()

	entry, exists := s.data[key]
	if !exists {
		return -2 // Key does not exist
	}

	if entry.IsExpired() {
		// Clean up expired entry
		s.mu.RUnlock()
		s.mu.Lock()
		delete(s.data, key)
		s.mu.Unlock()
		s.mu.RLock()
		return -2 // Key does not exist after cleanup
	}

	if entry.ExpiresAt == nil {
		return -1 // Key exists but has no expiration
	}

	remaining := entry.ExpiresAt.Sub(time.Now())
	if remaining <= 0 {
		// Key expired
		s.mu.RUnlock()
		s.mu.Lock()
		delete(s.data, key)
		s.mu.Unlock()
		s.mu.RLock()
		return -2 // Key does not exist after cleanup
	}
	return int64(remaining.Seconds())
}

// Keys returns all non-expired keys (for debugging/persistence)
func (s *Store) Keys() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	keys := []string{}
	for k, v := range s.data {
		if !v.IsExpired() {
			keys = append(keys, k)
		}
	}
	return keys
}

// Len returns the number of non-expired keys
func (s *Store) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()

	count := 0
	for _, v := range s.data {
		if !v.IsExpired() {
			count++
		}
	}
	return count
}

// getTTL returns the TTL duration from an entry (0 if no expiration)
func getTTL(e *Entry) time.Duration {
	if e.ExpiresAt == nil {
		return 0
	}
	return time.Until(*e.ExpiresAt)
}