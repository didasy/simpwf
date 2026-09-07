package contextdiff_test

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/simpwf/workflow-engine/pkg/contextdiff"
)

func TestDiffApplyRoundtrip(t *testing.T) {
	before := map[string]any{
		"a":    1.0,
		"gone": true,
		"arr":  []any{1.0},
	}
	after := map[string]any{
		"a":   2.0,
		"arr": []any{1.0, 2.0},
		"n":   map[string]any{"x": 1.0},
	}

	d, err := contextdiff.DiffMaps(before, after)
	if err != nil {
		t.Fatal(err)
	}
	got, err := contextdiff.Apply(before, d)
	if err != nil {
		t.Fatal(err)
	}
	assertJSONEqual(t, after, got)
}

func TestDiffMapsEqualIsEmpty(t *testing.T) {
	d, err := contextdiff.DiffMaps(
		map[string]any{"nested": map[string]any{"value": "same"}},
		map[string]any{"nested": map[string]any{"value": "same"}},
	)
	if err != nil {
		t.Fatal(err)
	}
	if !d.Empty() {
		t.Fatalf("DiffMaps() = %#v, want empty diff", d)
	}
}

func TestDiffApplyUnsetsRemovedKey(t *testing.T) {
	before := map[string]any{"keep": true, "remove": map[string]any{"value": 1.0}}
	after := map[string]any{"keep": true}

	d, err := contextdiff.DiffMaps(before, after)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(d.Unset, []string{"remove"}) {
		t.Fatalf("Unset = %#v, want [remove]", d.Unset)
	}
	got, err := contextdiff.Apply(before, d)
	if err != nil {
		t.Fatal(err)
	}
	assertJSONEqual(t, after, got)
}

func TestDiffApplyReplacesArraysAtArrayPath(t *testing.T) {
	before := map[string]any{"items": []any{map[string]any{"id": 1.0}}}
	after := map[string]any{"items": []any{map[string]any{"id": 2.0}, map[string]any{"id": 3.0}}}

	d, err := contextdiff.DiffMaps(before, after)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := d.Set["items"]; !ok {
		t.Fatalf("Set = %#v, want wholesale items replacement", d.Set)
	}
	for path := range d.Set {
		if path != "items" {
			t.Fatalf("Set contains path %q, want only array path", path)
		}
	}
	got, err := contextdiff.Apply(before, d)
	if err != nil {
		t.Fatal(err)
	}
	assertJSONEqual(t, after, got)
}

func TestDiffApplySupportsHyphenatedTopLevelKey(t *testing.T) {
	key := "my-output-key"
	before := map[string]any{"keep": true}
	after := map[string]any{"keep": true, key: "value"}

	d, err := contextdiff.DiffMaps(before, after)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := d.Set[key]; !ok {
		t.Fatalf("Set = %#v, want top-level key %q", d.Set, key)
	}
	got, err := contextdiff.Apply(before, d)
	if err != nil {
		t.Fatal(err)
	}
	assertJSONEqual(t, after, got)

	removed, err := contextdiff.DiffMaps(after, before)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(removed.Unset, []string{key}) {
		t.Fatalf("Unset = %#v, want [%q]", removed.Unset, key)
	}
	got, err = contextdiff.Apply(after, removed)
	if err != nil {
		t.Fatal(err)
	}
	assertJSONEqual(t, before, got)
}

func TestDiffMapsRejectsAmbiguousTopLevelPathKey(t *testing.T) {
	_, err := contextdiff.DiffMaps(
		map[string]any{},
		map[string]any{"a.b": "value"},
	)
	if err == nil {
		t.Fatal("DiffMaps() error = nil, want ambiguous top-level path key error")
	}
}

func assertJSONEqual(t *testing.T, want, got any) {
	t.Helper()
	wantRaw, err := json.Marshal(want)
	if err != nil {
		t.Fatal(err)
	}
	gotRaw, err := json.Marshal(got)
	if err != nil {
		t.Fatal(err)
	}
	var wantNorm, gotNorm any
	if err := json.Unmarshal(wantRaw, &wantNorm); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(gotRaw, &gotNorm); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(wantNorm, gotNorm) {
		t.Fatalf("want %#v, got %#v", wantNorm, gotNorm)
	}
}
