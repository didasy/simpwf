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

func newWorkflowDef(id, name string, version int, lineage string) model.WorkflowDefinition {
	return model.WorkflowDefinition{
		ID:        id,
		Name:      name,
		Version:   version,
		LineageID: lineage,
		Content:   json.RawMessage(`{"start_node_id":"aaaaaaaa-aaaa-7aaa-8aaa-aaaaaaaaaaaa","nodes":[{"id":"aaaaaaaa-aaaa-7aaa-8aaa-aaaaaaaaaaaa","type":"script","script":"return 1;"}]}`),
		CreatedBy: "11111111-1111-7111-8111-111111111111",
		UpdatedBy: "11111111-1111-7111-8111-111111111111",
		CreatedAt: time.Now().UTC(),
		UpdatedAt: time.Now().UTC(),
	}
}

func TestWorkflowDefinitionCreateAndGet(t *testing.T) {
	db := setupTestDB(t)
	ctx := context.Background()
	repo := repository.NewWorkflowDefinitionRepository(db)

	def := newWorkflowDef("11111111-1111-7111-8111-111111111111", "flow", 1, "33333333-3333-7333-8333-333333333333")
	if err := repo.Create(ctx, def); err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	got, err := repo.GetByID(ctx, def.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Name != "flow" || got.Version != 1 || got.LineageID != def.LineageID {
		t.Errorf("mismatch: %+v", got)
	}
	if !jsonEqual(t, got.Content, def.Content) {
		t.Errorf("content mismatch")
	}
}

func TestWorkflowDefinitionVersionChain(t *testing.T) {
	db := setupTestDB(t)
	ctx := context.Background()
	repo := repository.NewWorkflowDefinitionRepository(db)

	v1 := newWorkflowDef("11111111-1111-7111-8111-111111111111", "flow", 1, "33333333-3333-7333-8333-333333333333")
	if err := repo.Create(ctx, v1); err != nil {
		t.Fatal(err)
	}
	v2 := newWorkflowDef("22222222-2222-7222-8222-222222222222", "flow", 2, v1.LineageID)
	prev := v1.ID
	v2.PreviousVersionID = &prev
	if err := repo.Create(ctx, v2); err != nil {
		t.Fatal(err)
	}
	got, err := repo.GetByID(ctx, v2.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Version != 2 || got.LineageID != v1.LineageID || got.PreviousVersionID == nil || *got.PreviousVersionID != v1.ID {
		t.Errorf("version chain mismatch: %+v", got)
	}
}

func TestWorkflowDefinitionDuplicatePreviousVersionConflict(t *testing.T) {
	db := setupTestDB(t)
	ctx := context.Background()
	repo := repository.NewWorkflowDefinitionRepository(db)

	v1 := newWorkflowDef("11111111-1111-7111-8111-111111111111", "flow", 1, "33333333-3333-7333-8333-333333333333")
	if err := repo.Create(ctx, v1); err != nil {
		t.Fatal(err)
	}
	prev := v1.ID
	v2 := newWorkflowDef("22222222-2222-7222-8222-222222222222", "flow", 2, v1.LineageID)
	v2.PreviousVersionID = &prev
	if err := repo.Create(ctx, v2); err != nil {
		t.Fatal(err)
	}
	other := newWorkflowDef("44444444-4444-7444-8444-444444444444", "other", 2, "55555555-5555-7555-8555-555555555555")
	other.PreviousVersionID = &prev
	if err := repo.Create(ctx, other); !errors.Is(err, model.ErrConflict) {
		t.Errorf("duplicate previous error = %v, want ErrConflict", err)
	}
}

func TestWorkflowDefinitionListLatestOnly(t *testing.T) {
	db := setupTestDB(t)
	ctx := context.Background()
	repo := repository.NewWorkflowDefinitionRepository(db)

	defs := []model.WorkflowDefinition{
		newWorkflowDef("11111111-1111-7111-8111-111111111111", "flow", 1, "33333333-3333-7333-8333-333333333333"),
		newWorkflowDef("22222222-2222-7222-8222-222222222222", "flow", 2, "33333333-3333-7333-8333-333333333333"),
		newWorkflowDef("33333333-3333-7333-8333-333333333333", "other", 1, "44444444-4444-7444-8444-444444444444"),
	}
	for _, d := range defs {
		if err := repo.Create(ctx, d); err != nil {
			t.Fatal(err)
		}
	}
	items, total, err := repo.List(ctx, repository.DefinitionListQuery{Page: 1, PerPage: 50, Order: "-created_at", LatestOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	if total != 2 {
		t.Errorf("latest_only total = %d, want 2", total)
	}
	names := map[string]bool{}
	for _, it := range items {
		names[it.Name] = true
	}
	if !names["other"] || !names["flow"] {
		t.Errorf("latest items = %+v", items)
	}
	for _, it := range items {
		if it.Name == "flow" && it.Version != 2 {
			t.Errorf("flow latest version = %d, want 2", it.Version)
		}
	}
}

func TestWorkflowDefinitionDeleteReferencedConflict(t *testing.T) {
	db := setupTestDB(t)
	ctx := context.Background()
	repo := repository.NewWorkflowDefinitionRepository(db)

	def := newWorkflowDef("11111111-1111-7111-8111-111111111111", "flow", 1, "33333333-3333-7333-8333-333333333333")
	if err := repo.Create(ctx, def); err != nil {
		t.Fatal(err)
	}

	// referenced by a workflow request
	req := repository.WorkflowRequestModel{
		ID:                   "22222222-2222-7222-8222-222222222222",
		WorkflowDefinitionID: def.ID,
		CreatedBy:            "11111111-1111-7111-8111-111111111111",
		CreatedAt:            time.Now().UTC(),
	}
	if err := db.Create(&req).Error; err != nil {
		t.Fatal(err)
	}
	if err := repo.Delete(ctx, def.ID); !errors.Is(err, model.ErrConflict) {
		t.Errorf("delete referenced by request error = %v, want ErrConflict", err)
	}
	if err := db.Delete(&req).Error; err != nil {
		t.Fatal(err)
	}

	// referenced by a workflow instance
	inst := repository.WorkflowInstanceModel{
		ID:                   "33333333-3333-7333-8333-333333333333",
		WorkflowDefinitionID: def.ID,
		Status:               string(model.WorkflowWaiting),
		CreatedBy:            "11111111-1111-7111-8111-111111111111",
		UpdatedBy:            "11111111-1111-7111-8111-111111111111",
		CreatedAt:            time.Now().UTC(),
		UpdatedAt:            time.Now().UTC(),
	}
	if err := db.Create(&inst).Error; err != nil {
		t.Fatal(err)
	}
	if err := repo.Delete(ctx, def.ID); !errors.Is(err, model.ErrConflict) {
		t.Errorf("delete referenced by instance error = %v, want ErrConflict", err)
	}
}

func TestWorkflowDefinitionDeleteOK(t *testing.T) {
	db := setupTestDB(t)
	ctx := context.Background()
	repo := repository.NewWorkflowDefinitionRepository(db)

	def := newWorkflowDef("11111111-1111-7111-8111-111111111111", "flow", 1, "33333333-3333-7333-8333-333333333333")
	if err := repo.Create(ctx, def); err != nil {
		t.Fatal(err)
	}
	if err := repo.Delete(ctx, def.ID); err != nil {
		t.Fatalf("Delete() error = %v", err)
	}
	if _, err := repo.GetByID(ctx, def.ID); !errors.Is(err, model.ErrNotFound) {
		t.Errorf("GetByID after delete error = %v, want ErrNotFound", err)
	}
}
