package ids_test

import (
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/simpwf/workflow-engine/pkg/ids"
)

func TestNewReturnsUUIDv7(t *testing.T) {
	id, err := ids.New()
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if id.Version() != 7 {
		t.Errorf("Version() = %d, want 7", id.Version())
	}
}

func TestNewStringReturnsParseableUUIDv7(t *testing.T) {
	s, err := ids.NewString()
	if err != nil {
		t.Fatalf("NewString() error = %v", err)
	}
	id, err := uuid.Parse(s)
	if err != nil {
		t.Fatalf("uuid.Parse(%q) error = %v", s, err)
	}
	if id.Version() != 7 {
		t.Errorf("Version() = %d, want 7", id.Version())
	}
}

func TestNewStringUnique(t *testing.T) {
	a, err := ids.NewString()
	if err != nil {
		t.Fatal(err)
	}
	b, err := ids.NewString()
	if err != nil {
		t.Fatal(err)
	}
	if a == b {
		t.Fatalf("two generated ids are equal: %s", a)
	}
}

func TestParse(t *testing.T) {
	valid, err := ids.NewString()
	if err != nil {
		t.Fatal(err)
	}
	if got, err := ids.Parse(valid); err != nil {
		t.Errorf("Parse(%q) error = %v", valid, err)
	} else if got.String() != valid {
		t.Errorf("Parse(%q) = %s, want round-trip", valid, got)
	}

	if _, err := ids.Parse("not-a-uuid"); err == nil {
		t.Error("Parse(invalid) error = nil, want error")
	}
}

func TestValid(t *testing.T) {
	valid, err := ids.NewString()
	if err != nil {
		t.Fatal(err)
	}
	if !ids.Valid(valid) {
		t.Errorf("Valid(%q) = false, want true", valid)
	}
	if ids.Valid(strings.ToUpper(valid)) {
		t.Errorf("Valid(upper %q) = true, want false (must match canonical form)", valid)
	}
	if ids.Valid("") {
		t.Error("Valid(\"\") = true, want false")
	}
}
