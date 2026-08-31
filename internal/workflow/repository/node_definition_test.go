package repository_test

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/simpwf/workflow-engine/internal/workflow/model"
	"github.com/simpwf/workflow-engine/internal/workflow/repository"
)

func newDef(id, name string, version int, lineage string, nodeType string) model.NodeDefinition {
	return model.NodeDefinition{
		ID:        id,
		Name:      name,
		Version:   version,
		LineageID: lineage,
		Type:      nodeType,
		Content:   json.RawMessage(`{"type":"script","script":"return 1;"}`),
		CreatedBy: "11111111-1111-7111-8111-111111111111",
		UpdatedBy: "11111111-1111-7111-8111-111111111111",
		CreatedAt: time.Now().UTC(),
		UpdatedAt: time.Now().UTC(),
	}
}

func jsonEqual(t *testing.T, a, b []byte) bool {
	t.Helper()
	var va, vb any
	if err := json.Unmarshal(a, &va); err != nil {
		t.Fatalf("unmarshal %s: %v", a, err)
	}
	if err := json.Unmarshal(b, &vb); err != nil {
		t.Fatalf("unmarshal %s: %v", b, err)
	}
	return reflect.DeepEqual(va, vb)
}

func TestNodeDefinitionCreateAndGet(t *testing.T) {
	db := setupTestDB(t)
	ctx := context.Background()
	repo := repository.NewNodeDefinitionRepository(db)

	def := newDef("11111111-1111-7111-8111-111111111111", "transform", 1, "33333333-3333-7333-8333-333333333333", "script")
	if err := repo.Create(ctx, def); err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	got, err := repo.GetByID(ctx, def.ID)
	if err != nil {
		t.Fatalf("GetByID() error = %v", err)
	}
	if got.Name != def.Name || got.Version != 1 || got.Type != "script" || got.LineageID != def.LineageID {
		t.Errorf("mismatch: %+v", got)
	}
	if !jsonEqual(t, got.Content, def.Content) {
		t.Errorf("content mismatch: %s vs %s", got.Content, def.Content)
	}
}

func TestNodeDefinitionVersionChain(t *testing.T) {
	db := setupTestDB(t)
	ctx := context.Background()
	repo := repository.NewNodeDefinitionRepository(db)

	v1 := newDef("11111111-1111-7111-8111-111111111111", "transform", 1, "33333333-3333-7333-8333-333333333333", "script")
	if err := repo.Create(ctx, v1); err != nil {
		t.Fatal(err)
	}
	// Service-computed next version: version 2, inherited lineage.
	v2 := newDef("22222222-2222-7222-8222-222222222222", "transform", 2, v1.LineageID, "script")
	prev := v1.ID
	v2.PreviousVersionID = &prev
	if err := repo.Create(ctx, v2); err != nil {
		t.Fatalf("Create(v2) error = %v", err)
	}

	got, err := repo.GetByID(ctx, v2.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Version != 2 {
		t.Errorf("version = %d, want 2", got.Version)
	}
	if got.LineageID != v1.LineageID {
		t.Errorf("lineage = %s, want inherited %s", got.LineageID, v1.LineageID)
	}
	if got.PreviousVersionID == nil || *got.PreviousVersionID != v1.ID {
		t.Errorf("previous = %v, want %s", got.PreviousVersionID, v1.ID)
	}
}

func TestNodeDefinitionDuplicatePreviousVersionConflict(t *testing.T) {
	db := setupTestDB(t)
	ctx := context.Background()
	repo := repository.NewNodeDefinitionRepository(db)

	v1 := newDef("11111111-1111-7111-8111-111111111111", "transform", 1, "33333333-3333-7333-8333-333333333333", "script")
	if err := repo.Create(ctx, v1); err != nil {
		t.Fatal(err)
	}
	prev := v1.ID

	v2 := newDef("22222222-2222-7222-8222-222222222222", "transform", 2, "33333333-3333-7333-8333-333333333333", "script")
	v2.PreviousVersionID = &prev
	if err := repo.Create(ctx, v2); err != nil {
		t.Fatal(err)
	}

	other := newDef("44444444-4444-7444-8444-444444444444", "transform-other", 2, "55555555-5555-7555-8555-555555555555", "script")
	other.PreviousVersionID = &prev
	if err := repo.Create(ctx, other); err == nil {
		t.Fatal("Create() with duplicate previous_version_id error = nil, want conflict")
	} else if !errors.Is(err, model.ErrConflict) {
		t.Errorf("error = %v, want ErrConflict", err)
	}
}

func TestNodeDefinitionListFilters(t *testing.T) {
	db := setupTestDB(t)
	ctx := context.Background()
	repo := repository.NewNodeDefinitionRepository(db)

	defs := []model.NodeDefinition{
		newDef("11111111-1111-7111-8111-111111111111", "alpha", 1, "33333333-3333-7333-8333-333333333333", "script"),
		newDef("22222222-2222-7222-8222-222222222222", "beta", 1, "44444444-4444-7444-8444-444444444444", "conditions"),
		newDef("33333333-3333-7333-8333-333333333333", "beta", 2, "44444444-4444-7444-8444-444444444444", "script"),
	}
	for _, d := range defs {
		if err := repo.Create(ctx, d); err != nil {
			t.Fatal(err)
		}
	}

	// name filter
	items, total, err := repo.List(ctx, repository.DefinitionListQuery{Page: 1, PerPage: 50, Order: "-created_at", Name: "beta"})
	if err != nil {
		t.Fatal(err)
	}
	if total != 2 || len(items) != 2 {
		t.Errorf("name filter: total %d items %d, want 2/2", total, len(items))
	}

	// type filter
	items, total, err = repo.List(ctx, repository.DefinitionListQuery{Page: 1, PerPage: 50, Order: "-created_at", Type: "conditions"})
	if err != nil {
		t.Fatal(err)
	}
	if total != 1 || len(items) != 1 || items[0].Name != "beta" {
		t.Errorf("type filter: total %d items %+v", total, items)
	}

	// id filter
	_, total, err = repo.List(ctx, repository.DefinitionListQuery{Page: 1, PerPage: 50, Order: "-created_at", IDs: []string{defs[0].ID, defs[2].ID}})
	if err != nil {
		t.Fatal(err)
	}
	if total != 2 {
		t.Errorf("id filter: total %d, want 2", total)
	}

	// latest_only returns only the newest version per lineage
	_, total, err = repo.List(ctx, repository.DefinitionListQuery{Page: 1, PerPage: 50, Order: "-created_at", LatestOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	if total != 2 {
		t.Errorf("latest_only: total %d, want 2 (alpha v1 + beta v2)", total)
	}

	// pagination
	items, total, err = repo.List(ctx, repository.DefinitionListQuery{Page: 2, PerPage: 1, Order: "-created_at"})
	if err != nil {
		t.Fatal(err)
	}
	if total != 3 || len(items) != 1 {
		t.Errorf("pagination: total %d items %d, want 3/1", total, len(items))
	}
}

func TestNodeDefinitionDeleteReferencedConflict(t *testing.T) {
	db := setupTestDB(t)
	ctx := context.Background()
	repo := repository.NewNodeDefinitionRepository(db)

	def := newDef("11111111-1111-7111-8111-111111111111", "transform", 1, "33333333-3333-7333-8333-333333333333", "script")
	if err := repo.Create(ctx, def); err != nil {
		t.Fatal(err)
	}

	// referenced by a workflow definition
	seedInstanceFixture(t, db)
	insertInstance(t, db, newTestInstance("55555555-5555-7555-8555-555555555555", model.WorkflowWaiting, ""))
	ref := repository.WorkflowDefinitionNodeRefModel{
		WorkflowDefinitionID: fixtureWorkflowDefID,
		NodeDefinitionID:     def.ID,
		CreatedAt:            time.Now().UTC(),
	}
	if err := db.Create(&ref).Error; err != nil {
		t.Fatal(err)
	}
	if err := repo.Delete(ctx, def.ID); !errors.Is(err, model.ErrConflict) {
		t.Errorf("Delete() referenced by definition error = %v, want ErrConflict", err)
	}
	if err := db.Delete(&ref).Error; err != nil {
		t.Fatal(err)
	}

	// referenced by a node instance
	inst := repository.NodeInstanceModel{
		ID:                 "44444444-4444-7444-8444-444444444444",
		WorkflowInstanceID: "55555555-5555-7555-8555-555555555555",
		NodeID:             "66666666-6666-7666-8666-666666666666",
		NodeDefinitionID:   &def.ID,
		Name:               "transform",
		Type:               "script",
		Status:             string(model.NodeWaiting),
		CreatedAt:          time.Now().UTC(),
		UpdatedAt:          time.Now().UTC(),
	}
	if err := db.Create(&inst).Error; err != nil {
		t.Fatal(err)
	}
	if err := repo.Delete(ctx, def.ID); !errors.Is(err, model.ErrConflict) {
		t.Errorf("Delete() referenced by instance error = %v, want ErrConflict", err)
	}
}

func TestNodeDefinitionDeleteOK(t *testing.T) {
	db := setupTestDB(t)
	ctx := context.Background()
	repo := repository.NewNodeDefinitionRepository(db)

	def := newDef("11111111-1111-7111-8111-111111111111", "transform", 1, "33333333-3333-7333-8333-333333333333", "script")
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

func TestWorkflowDefinitionNodeRefsRoundTrip(t *testing.T) {
	db := setupTestDB(t)
	ctx := context.Background()

	// FK prerequisites: two node definitions and one workflow definition.
	defA := newDef("11111111-1111-7111-8111-111111111111", "a", 1, "aaaaaaaa-aaaa-7aaa-8aaa-aaaaaaaaaaaa", "script")
	defB := newDef("22222222-2222-7222-8222-222222222222", "b", 1, "bbbbbbbb-bbbb-7bbb-8bbb-bbbbbbbbbbbb", "script")
	if err := repository.NewNodeDefinitionRepository(db).Create(ctx, defA); err != nil {
		t.Fatal(err)
	}
	if err := repository.NewNodeDefinitionRepository(db).Create(ctx, defB); err != nil {
		t.Fatal(err)
	}
	wf := model.WorkflowDefinition{
		ID: "33333333-3333-7333-8333-333333333333", Name: "flow", Version: 1,
		LineageID: "33333333-3333-7333-8333-333333333333",
		Content:   json.RawMessage(`{"start_node_id":"11111111-1111-7111-8111-111111111111"}`),
		CreatedBy: "11111111-1111-7111-8111-111111111111",
		UpdatedBy: "11111111-1111-7111-8111-111111111111",
		CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC(),
	}
	m := repository.WorkflowDefinitionToModel(wf)
	if err := db.Create(&m).Error; err != nil {
		t.Fatal(err)
	}

	refs := []string{defA.ID, defB.ID}
	wfID := wf.ID
	if err := repository.AddWorkflowDefinitionNodeRefs(ctx, db, wfID, refs); err != nil {
		t.Fatalf("AddWorkflowDefinitionNodeRefs() error = %v", err)
	}
	got, err := repository.WorkflowDefinitionNodeRefs(ctx, db, wfID)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("refs len = %d, want 2", len(got))
	}
}

func TestUpsertSystemUserAndFKs(t *testing.T) {
	// setupTestDB seeds the system user used by definition foreign keys.
	db := setupTestDB(t)
	var m repository.UserModel
	if err := db.First(&m).Error; err != nil {
		t.Fatalf("expected seeded system user, got %v", err)
	}
	if m.Name != "system" || m.Email != "system@localhost" {
		t.Errorf("user = %+v, want seeded system user", m)
	}
}
