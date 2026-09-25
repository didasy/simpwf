// Package envsnapshot captures allowlisted process env for workflow templates.
package envsnapshot

import (
	"os"
	"strings"
)

// Prefix is the only env namespace snapshotted into workflow contexts.
// Workflows address values as {{ env.SIMPWF_X }}.
const Prefix = "SIMPWF_"

// Snapshot returns {"NAME": "value"} for env vars starting with prefix.
func Snapshot(prefix string) map[string]any {
	out := map[string]any{}
	for _, kv := range os.Environ() {
		if name, val, ok := strings.Cut(kv, "="); ok && strings.HasPrefix(name, prefix) {
			out[name] = val
		}
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
