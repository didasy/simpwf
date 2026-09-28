// Package envsnapshot captures allowlisted process env for workflow templates.
//
// A deny list guards the snapshot: credentials that must never reach a
// workflow template are dropped on the way in, and {{ env.* }} references to
// them resolve as missing. Two independent layers apply — the snapshot itself
// excludes denied names, and callers strip denied names out of a base map —
// because a snapshot is only trustworthy if the merge cannot be bypassed.
package envsnapshot

import (
	"os"
	"path"
	"strings"
)

// Prefix is the only env namespace snapshotted into workflow contexts.
// Workflows address values as {{ env.SIMPWF_X }}.
const Prefix = "SIMPWF_"

// Denied names are patterns matched against the full env var name. Globs are
// supported so an operator can add patterns without code changes; a pattern
// without a wildcard is an exact match.
var defaultDenied = []string{
	"SIMPWF_API_TOKEN",
	"SIMPWF_AUTH_*",
	"SIMPWF_SYSTEM_*",
	"SIMPWF_*_DSN",
}

// override holds the operator-supplied patterns installed by SetOverride.
var override struct {
	extra []string
	allow []string
}

// IsDeniedDefault reports whether name matches a hardcoded deny pattern.
func IsDeniedDefault(name string) bool {
	return matchAny(defaultDenied, name)
}

// IsDenied reports whether name is denied: a default or extra pattern matches
// and no allow pattern matches.
func IsDenied(name string) bool {
	denied := IsDeniedDefault(name) || matchAny(override.extra, name)
	if !denied {
		return false
	}
	return !matchAny(override.allow, name)
}

// SetOverride installs the operator deny additions and allow exceptions. It is
// called once at startup from configuration; the empty lists mean defaults
// only.
func SetOverride(extra, allow []string) {
	override.extra = append([]string(nil), extra...)
	override.allow = append([]string(nil), allow...)
}

// matchAny reports whether any glob pattern matches name. An empty pattern
// list never matches.
func matchAny(patterns []string, name string) bool {
	for _, p := range patterns {
		if p == "" {
			continue
		}
		if ok, err := path.Match(p, name); err == nil && ok {
			return true
		}
	}
	return false
}

// Snapshot returns {"NAME": "value"} for env vars starting with prefix and
// matching no deny pattern.
func Snapshot(prefix string) map[string]any {
	out := map[string]any{}
	for _, kv := range os.Environ() {
		if name, val, ok := strings.Cut(kv, "="); ok && strings.HasPrefix(name, prefix) && !IsDenied(name) {
			out[name] = val
		}
	}
	return out
}

// StripDenied copies base without denied keys (base must be map or nil). A
// caller-supplied env map therefore cannot smuggle a denied name past the
// snapshot.
func StripDenied(base any) map[string]any {
	out := map[string]any{}
	m, ok := base.(map[string]any)
	if !ok {
		return out
	}
	for k, v := range m {
		if IsDenied(k) {
			continue
		}
		out[k] = v
	}
	return out
}

// Merge overlays snapshot onto base (base must be map or nil); snapshot wins per key.
func Merge(base any, snapshot map[string]any) map[string]any {
	out := map[string]any{}
	if m, ok := base.(map[string]any); ok {
		for k, v := range m {
			out[k] = v
		}
	}
	for k, v := range snapshot {
		out[k] = v
	}
	return out
}
