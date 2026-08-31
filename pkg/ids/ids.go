// Package ids provides UUIDv7 generation and parsing helpers. UUIDv7 is the
// only UUID version used by this service.
package ids

import (
	"github.com/google/uuid"
)

// New returns a fresh UUIDv7.
func New() (uuid.UUID, error) {
	return uuid.NewV7()
}

// NewString returns a fresh UUIDv7 in canonical string form.
func NewString() (string, error) {
	id, err := uuid.NewV7()
	if err != nil {
		return "", err
	}
	return id.String(), nil
}

// Parse parses a canonical UUID string.
func Parse(s string) (uuid.UUID, error) {
	return uuid.Parse(s)
}

// Valid reports whether s is a canonical UUID string.
func Valid(s string) bool {
	id, err := uuid.Parse(s)
	return err == nil && id.String() == s
}
