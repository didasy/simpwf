package repository_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestMigrationIndexes is a DB-free guard that the PERF-3 hot claim/poll/list
// indexes exist in the applied Atlas migrations. It globs the version files
// (filename-agnostic) and asserts each expected CREATE INDEX statement —
// with its columns in order and, for partials, its WHERE predicate — plus
// the DROP of the standalone status index the claim composite subsumes.
//
// If this fails after a model-tag change, regenerate with `task migrate-diff`
// (raw `atlas migrate diff ...` from Taskfile.yaml when task is unavailable).
func TestMigrationIndexes(t *testing.T) {
	files, err := filepath.Glob("../../../migrations/versions/*.sql")
	if err != nil {
		t.Fatalf("glob versions: %v", err)
	}
	if len(files) == 0 {
		t.Fatal("no migration versions found; run `task migrate-diff` to generate them")
	}
	var b strings.Builder
	for _, f := range files {
		data, err := os.ReadFile(f)
		if err != nil {
			t.Fatalf("read %s: %v", f, err)
		}
		b.Write(data)
		b.WriteByte('\n')
	}
	ddl := b.String()

	// Tokens per index must appear in order, starting at the index name, so
	// composite column order and partial predicates are covered, not just the
	// index name.
	want := []struct {
		name   string
		tokens []string
	}{
		{"idx_workflow_instances_claim", []string{
			`"idx_workflow_instances_claim"`,
			`"status"`, `"waiting_reason"`, `"lease_expiry"`, `"updated_at"`,
		}},
		{"idx_workflow_instances_termination_pending", []string{
			`"idx_workflow_instances_termination_pending"`,
			`"termination_pending"`, `WHERE`, `termination_pending = true`,
		}},
		{"idx_status_update_outbox_ready", []string{
			`"idx_status_update_outbox_ready"`,
			`"next_attempt_at"`, `WHERE`, `delivered_at IS NULL`, `dead_at IS NULL`,
		}},
		{"idx_cron_schedules_enabled", []string{
			`"idx_cron_schedules_enabled"`, `"enabled"`,
		}},
		{"idx_workflow_instances_created_by", []string{
			`"idx_workflow_instances_created_by"`,
			`"created_by"`, `"created_at"`,
		}},
	}
	for _, w := range want {
		pos := 0
		for _, tok := range w.tokens {
			i := strings.Index(ddl[pos:], tok)
			if i < 0 {
				t.Errorf("migrations lack %s (missing %q after %d bytes); regenerate with `task migrate-diff`", w.name, tok, pos)
				break
			}
			pos += i + len(tok)
		}
	}

	// The claim composite's status prefix covers status-only predicates, so
	// the old standalone index must be dropped, not kept alongside.
	if !strings.Contains(ddl, `DROP INDEX "public"."idx_workflow_instances_status";`) {
		t.Error(`migrations lack DROP INDEX "public"."idx_workflow_instances_status"; regenerate with ` + "`task migrate-diff`")
	}
}
