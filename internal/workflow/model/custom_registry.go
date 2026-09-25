// Package model defines the workflow domain types, validation, and parsing.
package model

import (
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"sync"
)

var customTypeNameRe = regexp.MustCompile(`^[a-z][a-z0-9_]*$`)

// maxCustomTypeNameLen caps custom node type names.
const maxCustomTypeNameLen = 64

// CustomValidator validates a custom node's raw config object and returns
// its parsed form. It is owned by the node: the core stores the returned
// value on NodeContent.Custom and never inspects it.
type CustomValidator func(json.RawMessage) (any, error)

var customTypeMu sync.RWMutex

// customValidators maps custom node type names to their validators.
var customValidators = map[string]CustomValidator{}

// IsBuiltinNodeType reports whether t is one of the core node types.
func IsBuiltinNodeType(t string) bool {
	switch NodeType(t) {
	case NodeTypeScript, NodeTypeConditions, NodeTypeInput, NodeTypeGroup,
		NodeTypeExternalCall, NodeTypeOutput, NodeTypePoller:
		return true
	default:
		return false
	}
}

// ValidateCustomTypeName rejects empty names, names that are not lowercase
// identifiers, overlong names, and builtin type collisions.
func ValidateCustomTypeName(t string) error {
	if t == "" {
		return fmt.Errorf("custom node type must be non-empty")
	}
	if len(t) > maxCustomTypeNameLen {
		return fmt.Errorf("custom node type %q exceeds %d characters", t, maxCustomTypeNameLen)
	}
	if !customTypeNameRe.MatchString(t) {
		return fmt.Errorf("custom node type %q must match ^[a-z][a-z0-9_]*$", t)
	}
	if IsBuiltinNodeType(t) {
		return fmt.Errorf("custom node type %q collides with a builtin node type", t)
	}
	return nil
}

// RegisterCustomType adds a custom node type with its config validator.
// Duplicate registrations, builtin collisions, invalid names, and nil
// validators are rejected.
func RegisterCustomType(nodeType string, validate CustomValidator) error {
	if validate == nil {
		return fmt.Errorf("custom node type %q requires a validator", nodeType)
	}
	if err := ValidateCustomTypeName(nodeType); err != nil {
		return err
	}
	customTypeMu.Lock()
	defer customTypeMu.Unlock()
	if _, exists := customValidators[nodeType]; exists {
		return fmt.Errorf("custom node type %q is already registered", nodeType)
	}
	customValidators[nodeType] = validate
	return nil
}

// UnregisterCustomType removes a custom node type. It exists for facade
// rollback and tests; production code never unregisters.
func UnregisterCustomType(nodeType string) {
	customTypeMu.Lock()
	defer customTypeMu.Unlock()
	delete(customValidators, nodeType)
}

// LookupCustomType returns the validator for a registered custom node type.
func LookupCustomType(nodeType string) (CustomValidator, bool) {
	customTypeMu.RLock()
	defer customTypeMu.RUnlock()
	v, ok := customValidators[nodeType]
	return v, ok
}

// CustomTypes lists registered custom node type names in sorted order.
func CustomTypes() []string {
	customTypeMu.RLock()
	defer customTypeMu.RUnlock()
	out := make([]string, 0, len(customValidators))
	for t := range customValidators {
		out = append(out, t)
	}
	sort.Strings(out)
	return out
}
