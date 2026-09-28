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

func TestSnapshotExcludesDenied(t *testing.T) {
	t.Setenv("SIMPWF_INFRA_POSTGRESQL_DSN", "postgres://x")
	t.Setenv("SIMPWF_API_TOKEN", "tok")
	t.Setenv("SIMPWF_AUTH_ENABLED", "true")
	t.Setenv("SIMPWF_SYSTEM_USER_ID", "u1")
	t.Setenv("SIMPWF_S3_ENDPOINT", "play.min.io:9000")
	got := Snapshot(Prefix)
	for _, k := range []string{
		"SIMPWF_INFRA_POSTGRESQL_DSN",
		"SIMPWF_API_TOKEN",
		"SIMPWF_AUTH_ENABLED",
		"SIMPWF_SYSTEM_USER_ID",
	} {
		if _, ok := got[k]; ok {
			t.Fatalf("denied key %s snapshotted", k)
		}
	}
	if got["SIMPWF_S3_ENDPOINT"] != "play.min.io:9000" {
		t.Fatal("allowed key missing")
	}
}

func TestMergeCannotSmuggleDenied(t *testing.T) {
	base := map[string]any{"SIMPWF_API_TOKEN": "evil", "SIMPWF_S3_ENDPOINT": "ok"}
	got := Merge(StripDenied(base), Snapshot("TEST_PREFIX_THAT_MATCHES_NOTHING_"))
	if _, ok := got["SIMPWF_API_TOKEN"]; ok {
		t.Fatal("denied base key survived")
	}
	if got["SIMPWF_S3_ENDPOINT"] != "ok" {
		t.Fatal("allowed base key dropped")
	}
}

func TestStripDeniedNonMap(t *testing.T) {
	if got := StripDenied("nope"); len(got) != 0 {
		t.Fatalf("want empty map, got %v", got)
	}
}

func TestIsDeniedOverride(t *testing.T) {
	SetOverride([]string{"SIMPWF_OPENROUTER*"}, []string{"SIMPWF_OPENROUTER_MODEL"})
	defer SetOverride(nil, nil)

	if !IsDenied("SIMPWF_OPENROUTER_KEY") {
		t.Fatal("extra pattern must deny")
	}
	if IsDenied("SIMPWF_OPENROUTER_MODEL") {
		t.Fatal("allow exception must win")
	}
	if IsDenied("SIMPWF_S3_ENDPOINT") {
		t.Fatal("unrelated key must stay allowed")
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
