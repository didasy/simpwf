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

// customSchemas maps custom node type names to their compiled schema entry.
var customSchemas = map[string]*customSchemaEntry{}

// customSchemaEntry is one registered custom node's config sub-schema plus
// the full node envelope wrapped around it. Both are built once at
// registration so the read path serves cached bytes and never compiles.
type customSchemaEntry struct {
	// config is the author-supplied sub-schema describing the config
	// object alone.
	config json.RawMessage
	// full is that sub-schema wrapped into the full node envelope.
	full json.RawMessage
}

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

// UnregisterCustomType removes a custom node type and its schema. It exists
// for facade rollback and tests; production code never unregisters.
func UnregisterCustomType(nodeType string) {
	customTypeMu.Lock()
	defer customTypeMu.Unlock()
	delete(customValidators, nodeType)
	delete(customSchemas, nodeType)
}

// RegisterCustomSchema adds the config sub-schema of a custom node type.
// The schema is mandatory, must be a JSON object, and must compile as a
// draft 2020-12 schema: a custom node that cannot describe its config is
// rejected at startup rather than served as null on every read.
func RegisterCustomSchema(nodeType string, schema json.RawMessage) error {
	if len(schema) == 0 {
		return fmt.Errorf("custom node type %q requires a Schema", nodeType)
	}
	if err := ValidateCustomTypeName(nodeType); err != nil {
		return err
	}
	// The author schema is checked on its own terms first, so an author
	// writing a self-contained sub-schema (with its own $defs) is not
	// judged by the envelope it will be nested in.
	if err := compileJSONSchema(schema); err != nil {
		return fmt.Errorf("custom node type %q: invalid Schema: %w", nodeType, err)
	}
	full, err := buildCustomNodeSchema(nodeType, schema)
	if err != nil {
		return fmt.Errorf("custom node type %q: invalid Schema: %w", nodeType, err)
	}
	customTypeMu.Lock()
	defer customTypeMu.Unlock()
	if _, exists := customSchemas[nodeType]; exists {
		return fmt.Errorf("custom node type %q is already registered", nodeType)
	}
	customSchemas[nodeType] = &customSchemaEntry{
		config: append(json.RawMessage(nil), schema...),
		full:   full,
	}
	return nil
}

// UnregisterCustomSchema removes a custom node type's schema. It exists for
// facade rollback and tests; production code never unregisters.
func UnregisterCustomSchema(nodeType string) {
	customTypeMu.Lock()
	defer customTypeMu.Unlock()
	delete(customSchemas, nodeType)
}

// LookupCustomSchema returns the cached full node-envelope schema of a
// registered custom node type.
func LookupCustomSchema(nodeType string) (json.RawMessage, bool) {
	entry, ok := lookupCustomSchemaEntry(nodeType)
	if !ok {
		return nil, false
	}
	return entry.full, true
}

// LookupCustomConfigSchema returns the author-supplied config sub-schema of
// a registered custom node type, the value the author passed to
// RegisterCustomSchema.
func LookupCustomConfigSchema(nodeType string) json.RawMessage {
	entry, ok := lookupCustomSchemaEntry(nodeType)
	if !ok {
		return nil
	}
	return entry.config
}

// lookupCustomSchemaEntry returns the cached schema entry for a type.
func lookupCustomSchemaEntry(nodeType string) (*customSchemaEntry, bool) {
	customTypeMu.RLock()
	defer customTypeMu.RUnlock()
	entry, ok := customSchemas[nodeType]
	return entry, ok
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
