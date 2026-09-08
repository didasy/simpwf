package model

import (
	"encoding/json"
	"fmt"
	"strings"
)

const (
	ContextModeFull = "full"
	ContextModeLean = "lean"
)

// ParseContextMode extracts and validates an optional top-level context_mode.
func ParseContextMode(content json.RawMessage) (*string, error) {
	if len(content) == 0 {
		return nil, nil
	}
	var raw struct {
		ContextMode *string `json:"context_mode"`
	}
	if err := json.Unmarshal(content, &raw); err != nil {
		return nil, err
	}
	if raw.ContextMode == nil {
		return nil, nil
	}
	mode := strings.ToLower(strings.TrimSpace(*raw.ContextMode))
	if mode != ContextModeFull && mode != ContextModeLean {
		return nil, fmt.Errorf("context_mode %q must be full or lean", *raw.ContextMode)
	}
	return &mode, nil
}
