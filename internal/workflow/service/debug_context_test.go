package service_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/simpwf/workflow-engine/internal/workflow/auth"
	"github.com/simpwf/workflow-engine/internal/workflow/model"
	"github.com/simpwf/workflow-engine/internal/workflow/repository"
	"github.com/simpwf/workflow-engine/internal/workflow/service"
)

func TestDebugContextRequiresDebug(t *testing.T) {
	db := setupSvcDB(t)
	ctx := context.Background()
	svc := svcInstanceService(db)
	wfID := svcCreateWorkflow(t, db, "11111111-1111-7111-8111-111111111101",
		svcNodeJSON("11111111-1111-7111-8111-111111111101", "script", "a", "return 1;", "", nil),
	)
	inst, err := svc.Create(ctx, service.CreateInstance{WorkflowDefinitionID: wfID})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := svc.DebugContext(ctx, inst.ID, "", 0, auth.Principal{}); !errors.Is(err, model.ErrConflict) {
		t.Errorf("DebugContext(non-debug) error = %v, want ErrConflict", err)
	}
}

func TestDebugContextUnknownInstance(t *testing.T) {
	db := setupSvcDB(t)
	ctx := context.Background()
	svc := svcInstanceService(db)
	if _, err := svc.DebugContext(ctx, svcNewID(), "", 0, auth.Principal{}); !errors.Is(err, model.ErrNotFound) {
		t.Errorf("DebugContext(unknown) error = %v, want ErrNotFound", err)
	}
}

func TestDebugContextCursorAndExplicitNode(t *testing.T) {
	db := setupSvcDB(t)
	ctx := context.Background()
	svc := svcInstanceService(db)
	n1 := "11111111-1111-7111-8111-111111111101"
	n2 := "11111111-1111-7111-8111-111111111102"
	wfID := svcCreateWorkflow(t, db, n1,
		svcNodeJSON(n1, "script", "a", "context.x = 1; return 1;", n2, map[string]any{"output_property": "out1"}),
		svcNodeJSON(n2, "script", "b", "context.y = 2; return 2;", "", map[string]any{"output_property": "out2"}),
	)
	inst, err := svc.Create(ctx, service.CreateInstance{
		WorkflowDefinitionID: wfID, Debug: true, Context: json.RawMessage(`{"seed":7}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Resume(ctx, service.ControlRequest{InstanceID: inst.ID}); err != nil {
		t.Fatalf("Resume() error = %v", err)
	}
	repo := repository.NewInstanceRepository(db)
	eng := svcTestEngine(t, db)
	claimed, err := repo.ClaimNext(ctx, "dbg-worker", time.Minute, 10)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("claim = %d, err %v", len(claimed), err)
	}
	if err := eng.Process(ctx, claimed[0]); err != nil {
		t.Fatalf("Process(n1) error = %v", err)
	}
	occ1, err := repo.GetNodeInstanceByNode(ctx, inst.ID, n1)
	if err != nil {
		t.Fatalf("GetNodeInstanceByNode(n1) error = %v", err)
	}

	// Default cursor: parked on n2, which never ran, so the live redacted
	// instance context is the source.
	cur, err := svc.DebugContext(ctx, inst.ID, "", 0, auth.Principal{})
	if err != nil {
		t.Fatalf("DebugContext(cursor) error = %v", err)
	}
	if cur.NodeID != n2 {
		t.Errorf("NodeID = %q, want %q", cur.NodeID, n2)
	}
	if cur.OccurrenceID != nil || cur.Attempt != nil {
		t.Errorf("detail = %+v, want nil occurrence/attempt for not_started", cur)
	}
	if !cur.IsDebugPaused {
		t.Error("IsDebugPaused = false, want true")
	}
	if !strings.Contains(cur.TypeScript, "declare const context:") {
		t.Errorf("TypeScript = %q, want declaration", cur.TypeScript)
	}
	if !strings.Contains(cur.TypeScript, "out1") {
		t.Errorf("TypeScript = %q, want live context key out1", cur.TypeScript)
	}

	// Explicit node: n1's ContextBefore is the seed snapshot.
	d, err := svc.DebugContext(ctx, inst.ID, n1, 0, auth.Principal{})
	if err != nil {
		t.Fatalf("DebugContext(n1) error = %v", err)
	}
	if d.NodeID != n1 {
		t.Errorf("NodeID = %q, want %q", d.NodeID, n1)
	}
	if d.OccurrenceID == nil || *d.OccurrenceID != occ1.ID {
		t.Errorf("OccurrenceID = %v, want %s", d.OccurrenceID, occ1.ID)
	}
	if d.Attempt == nil || *d.Attempt != 1 {
		t.Errorf("Attempt = %v, want 1", d.Attempt)
	}
	if !strings.Contains(d.TypeScript, "seed: number;") {
		t.Errorf("TypeScript = %q, want seed from ContextBefore", d.TypeScript)
	}

	// Beyond-latest attempt and unknown node are 404.
	if _, err := svc.DebugContext(ctx, inst.ID, n1, 9, auth.Principal{}); !errors.Is(err, model.ErrNotFound) {
		t.Errorf("DebugContext(attempt=9) error = %v, want ErrNotFound", err)
	}
	if _, err := svc.DebugContext(ctx, inst.ID, "99999999-9999-7999-8999-999999999999", 0, auth.Principal{}); !errors.Is(err, model.ErrNotFound) {
		t.Errorf("DebugContext(unknown node) error = %v, want ErrNotFound", err)
	}
}

func TestDebugContextRedactsSecrets(t *testing.T) {
	db := setupSvcDB(t)
	ctx := context.Background()
	svc := svcInstanceService(db)
	n1 := "11111111-1111-7111-8111-111111111101"
	wfID := svcCreateWorkflow(t, db, n1,
		svcNodeJSON(n1, "script", "a", "return 1;", "", nil),
	)
	// Create replaces any request `secret` root with the stored snapshot,
	// so the secret must be seeded in the store to reach the context.
	secrets := repository.NewSecretRepository(db)
	if _, err := secrets.Create(ctx, "API_KEY", "plain-value"); err != nil {
		t.Fatal(err)
	}
	inst, err := svc.Create(ctx, service.CreateInstance{
		WorkflowDefinitionID: wfID, Debug: true,
		Context: json.RawMessage(`{"order":{"total":10}}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Resume(ctx, service.ControlRequest{InstanceID: inst.ID}); err != nil {
		t.Fatalf("Resume() error = %v", err)
	}
	repo := repository.NewInstanceRepository(db)
	eng := svcTestEngine(t, db)
	claimed, err := repo.ClaimNext(ctx, "dbg-worker", time.Minute, 10)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("claim = %d, err %v", len(claimed), err)
	}
	if err := eng.Process(ctx, claimed[0]); err != nil {
		t.Fatalf("Process(n1) error = %v", err)
	}
	d, err := svc.DebugContext(ctx, inst.ID, n1, 0, auth.Principal{})
	if err != nil {
		t.Fatalf("DebugContext() error = %v", err)
	}
	if strings.Contains(d.TypeScript, "plain-value") {
		t.Errorf("TypeScript leaked secret plaintext: %q", d.TypeScript)
	}
	if !strings.Contains(d.TypeScript, "order") {
		t.Errorf("TypeScript = %q, want order key", d.TypeScript)
	}
	// The secret root keeps only its key names: masked values render as
	// string, never the plaintext.
	if !strings.Contains(d.TypeScript, "secret: { API_KEY: string; };") {
		t.Errorf("TypeScript = %q, want masked secret key type", d.TypeScript)
	}
}

func TestDebugContextUntrustedSecretCollapses(t *testing.T) {
	db := setupSvcDB(t)
	ctx := context.Background()
	svc := svcInstanceService(db)
	n1 := "11111111-1111-7111-8111-111111111101"
	wfID := svcCreateWorkflow(t, db, n1,
		svcNodeJSON(n1, "script", "a", "return 1;", "", nil),
	)
	inst, err := svc.Create(ctx, service.CreateInstance{
		WorkflowDefinitionID: wfID, Debug: true,
		Context: json.RawMessage(`{"secret":"oops","order":{"total":1}}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	d, err := svc.DebugContext(ctx, inst.ID, "", 0, auth.Principal{})
	if err != nil {
		t.Fatalf("DebugContext() error = %v", err)
	}
	// The non-object secret root drops: inference degrades, and the
	// plaintext never leaks.
	if strings.Contains(d.TypeScript, "oops") {
		t.Errorf("TypeScript leaked untrusted secret plaintext: %q", d.TypeScript)
	}
	if strings.Contains(d.TypeScript, "secret") {
		t.Errorf("TypeScript = %q, want untrusted secret root dropped", d.TypeScript)
	}
	if !strings.Contains(d.TypeScript, "order") {
		t.Errorf("TypeScript = %q, want remaining order key", d.TypeScript)
	}
}

func TestDebugContextLeanReconstructs(t *testing.T) {
	db := setupSvcDB(t)
	ctx := context.Background()
	opts := model.LeanOptions{AnchorEvery: 20, ReplayMax: 500}
	svc := leanService(t, db, opts)
	n1 := "11111111-1111-7111-8111-111111111101"
	n2 := "11111111-1111-7111-8111-111111111102"
	wfID := svcCreateWorkflowWithMode(t, db, n1, model.ContextModeLean,
		svcNodeJSON(n1, "script", "a", "context.x = 1; return 1;", n2, map[string]any{"output_property": "out1"}),
		svcNodeJSON(n2, "script", "b", "context.y = 2; return 2;", "", map[string]any{"output_property": "out2"}),
	)
	inst, err := svc.Create(ctx, service.CreateInstance{
		WorkflowDefinitionID: wfID, Debug: true, Context: json.RawMessage(`{"seed":7}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Resume(ctx, service.ControlRequest{InstanceID: inst.ID}); err != nil {
		t.Fatalf("Resume() error = %v", err)
	}
	repo := repository.NewInstanceRepositoryWithOptions(db, opts)
	eng := svcTestEngine(t, db)
	claimed, err := repo.ClaimNext(ctx, "dbg-worker", time.Minute, 10)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("claim = %d, err %v", len(claimed), err)
	}
	if err := eng.Process(ctx, claimed[0]); err != nil {
		t.Fatalf("Process(n1) error = %v", err)
	}
	d, err := svc.DebugContext(ctx, inst.ID, n1, 0, auth.Principal{})
	if err != nil {
		t.Fatalf("DebugContext() error = %v", err)
	}
	if !strings.Contains(d.TypeScript, "seed: number;") {
		t.Errorf("TypeScript = %q, want lean-reconstructed seed", d.TypeScript)
	}
}
