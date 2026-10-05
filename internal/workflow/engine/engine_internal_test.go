package engine

import (
	"encoding/json"
	"testing"
)

// TestUnmarshalNullContext proves stored "null"/empty contexts load as a
// writable empty map instead of a nil map (CORR-1 defense in depth).
func TestUnmarshalNullContext(t *testing.T) {
	for _, raw := range []string{`null`, ``, `{}`} {
		m, err := unmarshalContext(json.RawMessage(raw))
		if err != nil {
			t.Fatalf("unmarshalContext(%q) error = %v", raw, err)
		}
		if m == nil {
			t.Fatalf("unmarshalContext(%q) = nil map, want empty map", raw)
		}
		m["probe"] = true // must not panic
		if m["probe"] != true {
			t.Fatalf("unmarshalContext(%q): write did not persist", raw)
		}
	}
}
