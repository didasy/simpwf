package repository

import (
	"errors"
	"testing"
)

func TestHistoryGapPreservesUnderlyingCause(t *testing.T) {
	cause := errors.New("contextdiff failure")
	err := historyGap(cause)
	if !errors.Is(err, ErrHistoryGap) {
		t.Fatalf("historyGap() error = %v, want ErrHistoryGap", err)
	}
	if !errors.Is(err, cause) {
		t.Fatalf("historyGap() error = %v, want underlying cause", err)
	}
}
