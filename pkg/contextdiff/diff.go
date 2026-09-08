// Package contextdiff computes and applies compact context changes.
package contextdiff

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/simpwf/workflow-engine/pkg/contextpath"
)

// Diff stores changed values and removed paths. Unset is always encoded
// as an array (never null) so history replay can distinguish a valid
// empty diff from a missing payload.
type Diff struct {
	Set   map[string]json.RawMessage `json:"set"`
	Unset []string                   `json:"unset"`
}

// DiffMaps computes a path-level diff between two JSON-compatible contexts.
// Maps are flattened; arrays and scalar values are replaced at their path.
func DiffMaps(before, after map[string]any) (Diff, error) {
	d := Diff{Set: make(map[string]json.RawMessage)}
	keys := make(map[string]struct{}, len(before)+len(after))
	for key := range before {
		keys[key] = struct{}{}
	}
	for key := range after {
		keys[key] = struct{}{}
	}

	sortedKeys := make([]string, 0, len(keys))
	for key := range keys {
		sortedKeys = append(sortedKeys, key)
	}
	sort.Strings(sortedKeys)
	for _, key := range sortedKeys {
		if !plainTopLevelKey(key) {
			// A top-level key containing path syntax is ambiguous with a
			// nested path, so callers must use a path-safe key there.
			return Diff{}, fmt.Errorf("contextdiff: ambiguous top-level context key %q", key)
		}
		beforeValue, beforeOK := before[key]
		afterValue, afterOK := after[key]
		if err := diffValue(&d, key, beforeValue, beforeOK, afterValue, afterOK); err != nil {
			return Diff{}, err
		}
	}
	if d.Unset == nil {
		d.Unset = []string{}
	}
	sort.Strings(d.Unset)
	return d, nil
}

func diffValue(d *Diff, path string, before any, beforeOK bool, after any, afterOK bool) error {
	switch {
	case !afterOK:
		d.Unset = append(d.Unset, path)
		return nil
	case beforeOK && maps(before) && maps(after):
		beforeMap := before.(map[string]any)
		afterMap := after.(map[string]any)
		keys := make(map[string]struct{}, len(beforeMap)+len(afterMap))
		for key := range beforeMap {
			keys[key] = struct{}{}
		}
		for key := range afterMap {
			keys[key] = struct{}{}
		}
		sortedKeys := make([]string, 0, len(keys))
		for key := range keys {
			sortedKeys = append(sortedKeys, key)
		}
		sort.Strings(sortedKeys)
		if len(sortedKeys) == 0 {
			return nil
		}
		for _, key := range sortedKeys {
			if !contextpath.ValidKey(key) {
				return fmt.Errorf("contextdiff: invalid context key %q", key)
			}
			beforeValue, beforeChildOK := beforeMap[key]
			afterValue, afterChildOK := afterMap[key]
			if err := diffValue(d, path+"."+key, beforeValue, beforeChildOK, afterValue, afterChildOK); err != nil {
				return err
			}
		}
		return nil
	case beforeOK && jsonEqual(before, after):
		return nil
	default:
		raw, err := json.Marshal(after)
		if err != nil {
			return fmt.Errorf("contextdiff: marshal %q: %w", path, err)
		}
		d.Set[path] = raw
		return nil
	}
}

func maps(value any) bool {
	_, ok := value.(map[string]any)
	return ok
}

func jsonEqual(left, right any) bool {
	leftRaw, leftErr := json.Marshal(left)
	rightRaw, rightErr := json.Marshal(right)
	return leftErr == nil && rightErr == nil && bytes.Equal(leftRaw, rightRaw)
}

// Apply clones base and applies unset paths followed by set paths.
func Apply(base map[string]any, d Diff) (map[string]any, error) {
	out, err := cloneMap(base)
	if err != nil {
		return nil, err
	}
	for _, path := range d.Unset {
		if err := deletePath(out, path); err != nil {
			return nil, err
		}
	}

	paths := make([]string, 0, len(d.Set))
	for path := range d.Set {
		paths = append(paths, path)
	}
	sort.Strings(paths)
	for _, path := range paths {
		var value any
		if err := json.Unmarshal(d.Set[path], &value); err != nil {
			return nil, fmt.Errorf("contextdiff: decode set %q: %w", path, err)
		}
		if plainTopLevelKey(path) {
			out[path] = value
			continue
		}
		if err := setNested(out, path, value); err != nil {
			return nil, err
		}
	}
	return out, nil
}

func cloneMap(base map[string]any) (map[string]any, error) {
	if base == nil {
		return map[string]any{}, nil
	}
	raw, err := json.Marshal(base)
	if err != nil {
		return nil, fmt.Errorf("contextdiff: clone context: %w", err)
	}
	var out map[string]any
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, fmt.Errorf("contextdiff: decode cloned context: %w", err)
	}
	if out == nil {
		out = map[string]any{}
	}
	return out, nil
}

func deletePath(ctx map[string]any, path string) error {
	if plainTopLevelKey(path) {
		if _, ok := ctx[path]; !ok {
			return fmt.Errorf("contextdiff: unset %q: %w", path, contextpath.ErrPathNotFound)
		}
		delete(ctx, path)
		return nil
	}
	parsed, err := contextpath.Parse(path)
	if err != nil {
		return err
	}
	if len(parsed) == 0 {
		return errors.New("contextdiff: empty unset path")
	}
	return deleteParsed(ctx, parsed)
}

func plainTopLevelKey(key string) bool {
	return !strings.ContainsAny(key, ".[]")
}

func setNested(ctx map[string]any, path string, value any) error {
	parsed, err := contextpath.Parse(path)
	if err != nil {
		return fmt.Errorf("contextdiff: set %q: %w", path, err)
	}
	if len(parsed) > 0 && parsed[0].Index != nil {
		return fmt.Errorf("contextdiff: set %q: top-level path is not an object key", path)
	}
	if err := contextpath.Set(ctx, path, value); err != nil {
		return fmt.Errorf("contextdiff: set %q: %w", path, err)
	}
	return nil
}

func deleteParsed(value any, path contextpath.Path) error {
	segment := path[0]
	switch current := value.(type) {
	case map[string]any:
		if segment.Index != nil {
			return fmt.Errorf("contextdiff: %q is not an array", segment.Key)
		}
		next, ok := current[segment.Key]
		if !ok {
			return fmt.Errorf("contextdiff: unset %q: %w", segment.Key, contextpath.ErrPathNotFound)
		}
		if len(path) == 1 {
			delete(current, segment.Key)
			return nil
		}
		return deleteParsed(next, path[1:])
	case []any:
		if segment.Index == nil {
			return fmt.Errorf("contextdiff: %q is not a map", segment.Key)
		}
		if *segment.Index < 0 || *segment.Index >= len(current) {
			return fmt.Errorf("contextdiff: index %d out of range: %w", *segment.Index, contextpath.ErrPathNotFound)
		}
		if len(path) == 1 {
			return errors.New("contextdiff: cannot unset array element")
		}
		return deleteParsed(current[*segment.Index], path[1:])
	default:
		return fmt.Errorf("contextdiff: cannot descend into %T: %w", value, contextpath.ErrPathNotFound)
	}
}

// Empty reports whether diff has no changes.
func (d Diff) Empty() bool {
	return len(d.Set) == 0 && len(d.Unset) == 0
}

// JSON returns the JSON representation of diff.
func (d Diff) JSON() json.RawMessage {
	raw, err := json.Marshal(d)
	if err != nil {
		return json.RawMessage(`{"set":{},"unset":[]}`)
	}
	return raw
}

// ParseDiff parses a JSON diff.
func ParseDiff(raw json.RawMessage) (Diff, error) {
	if len(bytes.TrimSpace(raw)) == 0 || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return Diff{}, errors.New("contextdiff: null or empty diff")
	}
	var d Diff
	if err := json.Unmarshal(raw, &d); err != nil {
		return Diff{}, fmt.Errorf("contextdiff: parse diff: %w", err)
	}
	if d.Set == nil {
		d.Set = make(map[string]json.RawMessage)
	}
	if d.Unset == nil {
		d.Unset = []string{}
	}
	return d, nil
}
