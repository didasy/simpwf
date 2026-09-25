package envsnapshot

import "testing"

func TestSnapshotFiltersPrefix(t *testing.T) {
	t.Setenv("SIMPWF_S3_ENDPOINT", "play.min.io:9000")
	t.Setenv("NOT_MINE", "x")
	got := Snapshot("SIMPWF_")
	if got["SIMPWF_S3_ENDPOINT"] != "play.min.io:9000" {
		t.Fatal("want endpoint")
	}
	if _, ok := got["NOT_MINE"]; ok {
		t.Fatal("want prefix filter")
	}
}

func TestMergeSnapshotWins(t *testing.T) {
	base := map[string]any{"A": "old", "B": "keep"}
	got := Merge(base, map[string]any{"A": "new"})
	if got["A"] != "new" || got["B"] != "keep" {
		t.Fatalf("want overlay, got %v", got)
	}
}

func TestMergeNilBase(t *testing.T) {
	got := Merge(nil, map[string]any{"A": "1"})
	if got["A"] != "1" {
		t.Fatalf("want snapshot on nil base, got %v", got)
	}
}
