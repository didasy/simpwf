package repository_test

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/simpwf/workflow-engine/internal/workflow/model"
	"github.com/simpwf/workflow-engine/internal/workflow/repository"
	"github.com/simpwf/workflow-engine/pkg/database"
	"gorm.io/gorm"
)

func setupTestDB(t *testing.T) *gorm.DB {
	t.Helper()
	ctx := context.Background()
	dsn := os.Getenv("TEST_DATABASE_DSN")
	if dsn == "" {
		t.Skip("TEST_DATABASE_DSN not set; skipping live database test")
	}
	opts := database.DefaultOptions()
	opts.DSN = dsn
	db, err := database.New(opts)
	if err != nil {
		t.Fatalf("database.New() error = %v", err)
	}
	t.Cleanup(func() {
		sqlDB, err := db.DB()
		if err == nil {
			_ = sqlDB.Close()
		}
	})

	// Test-only schema bootstrap. Production schema is owned exclusively by
	// Atlas migrations (cmd/atlas-loader); AutoMigrate never runs there.
	if err := db.AutoMigrate(
		&repository.UserModel{},
		&repository.NodeDefinitionModel{},
		&repository.WorkflowDefinitionModel{},
		&repository.WorkflowDefinitionNodeRefModel{},
		&repository.WorkflowRequestModel{},
		&repository.WorkflowInstanceModel{},
		&repository.NodeInstanceModel{},
		&repository.WorkflowInstanceEventModel{},
		&repository.InputDeliveryModel{},
		&repository.StatusUpdateOutboxModel{},
	); err != nil {
		t.Fatalf("AutoMigrate() error = %v", err)
	}

	// Truncate all tables in one statement so foreign-key ordering never
	// blocks the reset.
	if err := db.Exec(`TRUNCATE TABLE
		status_update_outbox, input_deliveries, workflow_instance_events, node_instances,
		workflow_instances, workflow_requests, workflow_definition_node_refs,
		workflow_definitions, node_definitions, users
		RESTART IDENTITY`).Error; err != nil {
		t.Fatalf("truncate tables: %v", err)
	}

	// Seed the system user; definition rows carry created_by foreign keys.
	system := model.User{ID: "11111111-1111-7111-8111-111111111111", Name: "system", Email: "system@localhost"}
	if err := repository.UpsertSystemUser(ctx, db, system); err != nil {
		t.Fatalf("seed system user: %v", err)
	}
	return db
}

func TestUpsertSystemUserCreatesThenUpdates(t *testing.T) {
	db := setupTestDB(t)
	ctx := context.Background()

	u := model.User{ID: "11111111-1111-7111-8111-111111111111", Name: "system", Email: "system@localhost"}
	if err := repository.UpsertSystemUser(ctx, db, u); err != nil {
		t.Fatalf("UpsertSystemUser() error = %v", err)
	}

	var count int64
	if err := db.Model(&repository.UserModel{}).Count(&count).Error; err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("users count = %d, want 1", count)
	}

	u.Name = "system-renamed"
	u.Email = "system+2@localhost"
	if err := repository.UpsertSystemUser(ctx, db, u); err != nil {
		t.Fatalf("second UpsertSystemUser() error = %v", err)
	}
	if err := db.Model(&repository.UserModel{}).Count(&count).Error; err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("users count after update = %d, want 1 (no duplicate)", count)
	}

	var stored repository.UserModel
	if err := db.Where("id = ?", u.ID).First(&stored).Error; err != nil {
		t.Fatal(err)
	}
	if stored.Name != "system-renamed" || stored.Email != "system+2@localhost" {
		t.Errorf("stored user = %+v, want updated name/email", stored)
	}
}

func TestUserMapperRoundTrip(t *testing.T) {
	in := model.User{
		ID:        "11111111-1111-7111-8111-111111111111",
		Name:      "system",
		Email:     "system@localhost",
		Metadata:  json.RawMessage(`{"role":"admin"}`),
		CreatedAt: time.Date(2026, 8, 5, 1, 2, 3, 0, time.UTC),
		UpdatedAt: time.Date(2026, 8, 5, 2, 3, 4, 0, time.UTC),
	}
	got := repository.UserFromModel(repository.UserToModel(in))
	if got.ID != in.ID || got.Name != in.Name || got.Email != in.Email {
		t.Errorf("round trip mismatch: %+v", got)
	}
	if string(got.Metadata) != string(in.Metadata) {
		t.Errorf("metadata = %s, want %s", got.Metadata, in.Metadata)
	}
	if !got.CreatedAt.Equal(in.CreatedAt) || !got.UpdatedAt.Equal(in.UpdatedAt) {
		t.Errorf("timestamps mismatch: %+v", got)
	}
}

func TestNodeDefinitionMapperRoundTrip(t *testing.T) {
	prev := "22222222-2222-7222-8222-222222222222"
	in := model.NodeDefinition{
		ID:                "11111111-1111-7111-8111-111111111111",
		Name:              "transform",
		Version:           2,
		PreviousVersionID: &prev,
		LineageID:         "33333333-3333-7333-8333-333333333333",
		Type:              "script",
		Content:           json.RawMessage(`{"script":"return 1;"}`),
		CreatedBy:         "user-1",
		UpdatedBy:         "user-2",
		CreatedAt:         time.Date(2026, 8, 5, 1, 0, 0, 0, time.UTC),
		UpdatedAt:         time.Date(2026, 8, 5, 2, 0, 0, 0, time.UTC),
	}
	got := repository.NodeDefinitionFromModel(repository.NodeDefinitionToModel(in))
	if got.ID != in.ID || got.Name != in.Name || got.Version != in.Version || got.Type != in.Type {
		t.Errorf("round trip mismatch: %+v", got)
	}
	if got.PreviousVersionID == nil || *got.PreviousVersionID != prev {
		t.Errorf("previous version id mismatch: %v", got.PreviousVersionID)
	}
	if got.LineageID != in.LineageID || got.CreatedBy != in.CreatedBy || got.UpdatedBy != in.UpdatedBy {
		t.Errorf("audit/lineage mismatch: %+v", got)
	}
	if string(got.Content) != string(in.Content) {
		t.Errorf("content mismatch")
	}
}

func TestWorkflowDefinitionMapperRoundTrip(t *testing.T) {
	in := model.WorkflowDefinition{
		ID:        "11111111-1111-7111-8111-111111111111",
		Name:      "order-flow",
		Version:   1,
		LineageID: "33333333-3333-7333-8333-333333333333",
		Content:   json.RawMessage(`{"start_node_id":"x"}`),
		CreatedBy: "user-1",
		UpdatedBy: "user-1",
	}
	got := repository.WorkflowDefinitionFromModel(repository.WorkflowDefinitionToModel(in))
	if got.ID != in.ID || got.Name != in.Name || got.Version != in.Version {
		t.Errorf("round trip mismatch: %+v", got)
	}
	if got.PreviousVersionID != nil {
		t.Errorf("previous version id = %v, want nil", *got.PreviousVersionID)
	}
	if string(got.Content) != string(in.Content) {
		t.Errorf("content mismatch")
	}
}

func TestWorkflowInstanceMapperRoundTrip(t *testing.T) {
	started := time.Date(2026, 8, 5, 1, 0, 0, 0, time.UTC)
	in := model.WorkflowInstance{
		ID:                   "11111111-1111-7111-8111-111111111111",
		WorkflowDefinitionID: "22222222-2222-7222-8222-222222222222",
		Status:               model.WorkflowRunning,
		WaitingReason:        model.WaitingReasonRunnable,
		PauseRequested:       true,
		TerminationPending:   false,
		CurrentGroupID:       "33333333-3333-7333-8333-333333333333",
		CurrentNodeID:        "44444444-4444-7444-8444-444444444444",
		CreatedBy:            "55555555-5555-7555-8555-555555555555",
		UpdatedBy:            "66666666-6666-7666-8666-666666666666",
		Frame:                json.RawMessage(`{"group":"main"}`),
		Context:              json.RawMessage(`{"data1":1}`),
		Counters:             json.RawMessage(`{"finished":3}`),
		Revision:             42,
		LeasedBy:             "worker-1",
		LeaseExpiry:          time.Date(2026, 8, 5, 3, 0, 0, 0, time.UTC),
		Error:                "boom",
		StartedAt:            &started,
	}
	got := repository.WorkflowInstanceFromModel(repository.WorkflowInstanceToModel(in))
	if got.ID != in.ID || got.Status != in.Status || got.WaitingReason != in.WaitingReason {
		t.Errorf("round trip mismatch: %+v", got)
	}
	if !got.PauseRequested || got.TerminationPending {
		t.Errorf("pause flags mismatch: %+v", got)
	}
	if got.CurrentGroupID != in.CurrentGroupID || got.CurrentNodeID != in.CurrentNodeID {
		t.Errorf("cursor mismatch: %+v", got)
	}
	if got.CreatedBy != in.CreatedBy || got.UpdatedBy != in.UpdatedBy {
		t.Errorf("audit actors mismatch: %+v", got)
	}
	if got.Revision != 42 || got.LeasedBy != "worker-1" || got.Error != "boom" {
		t.Errorf("lease/error mismatch: %+v", got)
	}
	if string(got.Frame) != string(in.Frame) || string(got.Context) != string(in.Context) || string(got.Counters) != string(in.Counters) {
		t.Errorf("jsonb fields mismatch: %+v", got)
	}
	if got.StartedAt == nil || !got.StartedAt.Equal(started) {
		t.Errorf("started_at mismatch: %v", got.StartedAt)
	}
}
