package repository_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/simpwf/workflow-engine/internal/workflow/model"
	"github.com/simpwf/workflow-engine/internal/workflow/repository"
)

const testScheduleSystemActor = "11111111-1111-7111-8111-111111111111"

func newCronSchedule(id, defID string) model.CronSchedule {
	now := time.Now().UTC()
	return model.CronSchedule{
		ID:                   id,
		WorkflowDefinitionID: defID,
		Crontab:              "*/5 * * * *",
		Timezone:             "UTC",
		Context:              json.RawMessage(`{"k":"v"}`),
		Enabled:              true,
		CreatedBy:            testScheduleSystemActor,
		UpdatedBy:            testScheduleSystemActor,
		CreatedAt:            now,
		UpdatedAt:            now,
	}
}

func TestCronScheduleMapperRoundTrip(t *testing.T) {
	in := newCronSchedule(
		"11111111-1111-7111-8111-111111111111",
		"22222222-2222-7222-8222-222222222222",
	)
	got := repository.CronScheduleFromModel(repository.CronScheduleToModel(in))
	if got.ID != in.ID || got.WorkflowDefinitionID != in.WorkflowDefinitionID {
		t.Errorf("identity mismatch: %+v", got)
	}
	if got.Crontab != in.Crontab || got.Timezone != in.Timezone || !got.Enabled {
		t.Errorf("schedule fields mismatch: %+v", got)
	}
	if string(got.Context) != string(in.Context) {
		t.Errorf("context = %s, want %s", got.Context, in.Context)
	}
	if got.CreatedBy != in.CreatedBy || got.UpdatedBy != in.UpdatedBy {
		t.Errorf("audit actors mismatch: %+v", got)
	}
	if !got.CreatedAt.Equal(in.CreatedAt) || !got.UpdatedAt.Equal(in.UpdatedAt) {
		t.Errorf("timestamps mismatch: %+v", got)
	}
}

func TestScheduleCreateAndGet(t *testing.T) {
	db := setupTestDB(t)
	ctx := context.Background()
	repo := repository.NewScheduleRepository(db)

	sched := newCronSchedule(
		"11111111-1111-7111-8111-111111111111",
		"22222222-2222-7222-8222-222222222222",
	)
	if err := repo.Create(ctx, sched); err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	got, err := repo.GetByID(ctx, sched.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.WorkflowDefinitionID != sched.WorkflowDefinitionID || got.Crontab != sched.Crontab ||
		got.Timezone != sched.Timezone || !got.Enabled {
		t.Errorf("mismatch: %+v", got)
	}
	if !jsonEqual(t, got.Context, sched.Context) {
		t.Errorf("context mismatch: %s", got.Context)
	}
}

func TestScheduleCreateDuplicateConflict(t *testing.T) {
	db := setupTestDB(t)
	ctx := context.Background()
	repo := repository.NewScheduleRepository(db)

	sched := newCronSchedule(
		"11111111-1111-7111-8111-111111111111",
		"22222222-2222-7222-8222-222222222222",
	)
	if err := repo.Create(ctx, sched); err != nil {
		t.Fatal(err)
	}
	if err := repo.Create(ctx, sched); !errors.Is(err, model.ErrConflict) {
		t.Errorf("duplicate create error = %v, want ErrConflict", err)
	}
}

func TestScheduleGetNotFound(t *testing.T) {
	db := setupTestDB(t)
	ctx := context.Background()
	repo := repository.NewScheduleRepository(db)

	if _, err := repo.GetByID(ctx, "99999999-9999-7999-8999-999999999999"); !errors.Is(err, model.ErrNotFound) {
		t.Errorf("GetByID missing error = %v, want ErrNotFound", err)
	}
}

func TestScheduleListPagination(t *testing.T) {
	db := setupTestDB(t)
	ctx := context.Background()
	repo := repository.NewScheduleRepository(db)

	ids := []string{
		"11111111-1111-7111-8111-111111111111",
		"22222222-2222-7222-8222-222222222222",
		"33333333-3333-7333-8333-333333333333",
	}
	for _, id := range ids {
		if err := repo.Create(ctx, newCronSchedule(id, "44444444-4444-7444-8444-444444444444")); err != nil {
			t.Fatal(err)
		}
	}
	items, total, err := repo.List(ctx, repository.ScheduleListQuery{Page: 1, PerPage: 2})
	if err != nil {
		t.Fatal(err)
	}
	if total != 3 || len(items) != 2 {
		t.Fatalf("page 1: total = %d len = %d, want 3 and 2", total, len(items))
	}
	items, total, err = repo.List(ctx, repository.ScheduleListQuery{Page: 2, PerPage: 2})
	if err != nil {
		t.Fatal(err)
	}
	if total != 3 || len(items) != 1 {
		t.Fatalf("page 2: total = %d len = %d, want 3 and 1", total, len(items))
	}
}

func TestScheduleListEnabledOnly(t *testing.T) {
	db := setupTestDB(t)
	ctx := context.Background()
	repo := repository.NewScheduleRepository(db)

	on := newCronSchedule("11111111-1111-7111-8111-111111111111", "44444444-4444-7444-8444-444444444444")
	off := newCronSchedule("22222222-2222-7222-8222-222222222222", "44444444-4444-7444-8444-444444444444")
	off.Enabled = false
	if err := repo.Create(ctx, on); err != nil {
		t.Fatal(err)
	}
	if err := repo.Create(ctx, off); err != nil {
		t.Fatal(err)
	}
	enabledOnly := true
	items, total, err := repo.List(ctx, repository.ScheduleListQuery{Page: 1, PerPage: 50, Enabled: &enabledOnly})
	if err != nil {
		t.Fatal(err)
	}
	if total != 1 || len(items) != 1 || items[0].ID != on.ID {
		t.Fatalf("enabled-only list = %+v total %d, want only %s", items, total, on.ID)
	}
	items, total, err = repo.List(ctx, repository.ScheduleListQuery{Page: 1, PerPage: 50})
	if err != nil {
		t.Fatal(err)
	}
	if total != 2 || len(items) != 2 {
		t.Fatalf("unfiltered list total = %d len = %d, want 2 and 2", total, len(items))
	}
}

func TestScheduleDelete(t *testing.T) {
	db := setupTestDB(t)
	ctx := context.Background()
	repo := repository.NewScheduleRepository(db)

	sched := newCronSchedule(
		"11111111-1111-7111-8111-111111111111",
		"22222222-2222-7222-8222-222222222222",
	)
	if err := repo.Create(ctx, sched); err != nil {
		t.Fatal(err)
	}
	if err := repo.Delete(ctx, sched.ID); err != nil {
		t.Fatalf("Delete() error = %v", err)
	}
	if _, err := repo.GetByID(ctx, sched.ID); !errors.Is(err, model.ErrNotFound) {
		t.Errorf("GetByID after delete error = %v, want ErrNotFound", err)
	}
	if err := repo.Delete(ctx, sched.ID); !errors.Is(err, model.ErrNotFound) {
		t.Errorf("second Delete error = %v, want ErrNotFound", err)
	}
}

func TestScheduleSetEnabled(t *testing.T) {
	db := setupTestDB(t)
	ctx := context.Background()
	repo := repository.NewScheduleRepository(db)

	sched := newCronSchedule(
		"11111111-1111-7111-8111-111111111111",
		"22222222-2222-7222-8222-222222222222",
	)
	if err := repo.Create(ctx, sched); err != nil {
		t.Fatal(err)
	}
	updated, err := repo.SetEnabled(ctx, sched.ID, false, "55555555-5555-7555-8555-555555555555")
	if err != nil {
		t.Fatalf("SetEnabled() error = %v", err)
	}
	if updated.Enabled {
		t.Errorf("Enabled = true, want false")
	}
	if updated.UpdatedBy != "55555555-5555-7555-8555-555555555555" {
		t.Errorf("UpdatedBy = %s, want pausing actor", updated.UpdatedBy)
	}
	if _, err := repo.SetEnabled(ctx, "99999999-9999-7999-8999-999999999999", false, testScheduleSystemActor); !errors.Is(err, model.ErrNotFound) {
		t.Errorf("SetEnabled missing error = %v, want ErrNotFound", err)
	}
}

func TestWorkflowDefinitionDeleteReferencedByScheduleConflict(t *testing.T) {
	db := setupTestDB(t)
	ctx := context.Background()
	defs := repository.NewWorkflowDefinitionRepository(db)
	schedules := repository.NewScheduleRepository(db)

	def := newWorkflowDef("11111111-1111-7111-8111-111111111111", "flow", 1, "33333333-3333-7333-8333-333333333333")
	if err := defs.Create(ctx, def); err != nil {
		t.Fatal(err)
	}
	sched := newCronSchedule("22222222-2222-7222-8222-222222222222", def.ID)
	if err := schedules.Create(ctx, sched); err != nil {
		t.Fatal(err)
	}
	if err := defs.Delete(ctx, def.ID); !errors.Is(err, model.ErrConflict) {
		t.Errorf("delete referenced by schedule error = %v, want ErrConflict", err)
	}
	if err := schedules.Delete(ctx, sched.ID); err != nil {
		t.Fatal(err)
	}
	if err := defs.Delete(ctx, def.ID); err != nil {
		t.Errorf("delete after schedule removed error = %v, want nil", err)
	}
}
