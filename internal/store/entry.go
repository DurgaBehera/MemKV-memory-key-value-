package store

import "time"

// Entry represents a key-value pair with optional expiration
type Entry struct {
	Value     string
	ExpiresAt *time.Time // nil means no expiration
}

// IsExpired checks if the entry has expired
func (e *Entry) IsExpired() bool {
	if e.ExpiresAt == nil {
		return false
	}
	return time.Now().After(*e.ExpiresAt)
}

// NewEntry creates a new entry with optional expiration
func NewEntry(value string, ttl time.Duration) *Entry {
	var expiresAt *time.Time
	if ttl > 0 {
		t := time.Now().Add(ttl)
		expiresAt = &t
	}
	return &Entry{
		Value:     value,
		ExpiresAt: expiresAt,
	}
}