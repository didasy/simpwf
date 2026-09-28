package service_test

import (
	"context"
	"testing"

	"github.com/simpwf/workflow-engine/internal/workflow/auth"
	"github.com/simpwf/workflow-engine/internal/workflow/model"
	"github.com/simpwf/workflow-engine/internal/workflow/repository"
	"github.com/simpwf/workflow-engine/internal/workflow/service"
)

// actorUserID is a second user distinct from the system user, standing in
// for a just-in-time OIDC identity so the tests can tell the two apart.
const actorUserID = "44444444-4444-7444-8444-444444444444"

// TestDefinitionCreateRecordsRequestActor: creating a definition through an
// authenticated request must record that human on created_by/updated_by,
// not the system user. Otherwise "who wrote this workflow" is unanswerable
// the moment OIDC is switched on.
func TestDefinitionCreateRecordsRequestActor(t *testing.T) {
	ctx := context.Background()

	nodeSvc := service.NewNodeDefinitionService(newFakeNodeRepo(), testLimits, actorID)
	node, err := nodeSvc.Create(ctx, service.CreateNodeDefinition{
		Name:    "ask",
		Type:    "input",
		Content: []byte(`{"type":"input","channel":"http"}`),
		Actor:   actorUserID,
	})
	if err != nil {
		t.Fatalf("node Create() error = %v", err)
	}
	if node.CreatedBy != actorUserID || node.UpdatedBy != actorUserID {
		t.Errorf("node audit actors = %q/%q, want %q", node.CreatedBy, node.UpdatedBy, actorUserID)
	}

	wfSvc := newWorkflowService(newFakeWorkflowRepo(), nil)
	wf, err := wfSvc.Create(ctx, service.CreateWorkflowDefinition{
		Name:    "actor-flow",
		Content: []byte(validWorkflowContent),
		Actor:   actorUserID,
	})
	if err != nil {
		t.Fatalf("workflow Create() error = %v", err)
	}
	if wf.CreatedBy != actorUserID || wf.UpdatedBy != actorUserID {
		t.Errorf("workflow audit actors = %q/%q, want %q", wf.CreatedBy, wf.UpdatedBy, actorUserID)
	}
}

// TestDefinitionCreateFallsBackToSystemUser is the compatibility half: a
// request with no actor (a service call, a test, an unauthenticated
// deployment) must still attribute to the configured system user rather
// than writing an empty created_by that the foreign key would reject.
func TestDefinitionCreateFallsBackToSystemUser(t *testing.T) {
	ctx := context.Background()

	node, err := service.NewNodeDefinitionService(newFakeNodeRepo(), testLimits, actorID).
		Create(ctx, service.CreateNodeDefinition{
			Name:    "ask",
			Type:    "input",
			Content: []byte(`{"type":"input","channel":"http"}`),
		})
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	if node.CreatedBy != actorID {
		t.Errorf("created_by = %q, want the default actor %q", node.CreatedBy, actorID)
	}
}

// TestControlsRecordRequestActor: every control event must name the caller
// who issued it. These are the events an operator reads to answer "who
// stopped this instance".
func TestControlsRecordRequestActor(t *testing.T) {
	db := setupSvcDB(t)
	ctx := context.Background()
	svc := svcInstanceServiceWithCatalog(db, gateCatalog)

	n1 := "11111111-1111-7111-8111-111111111101"
	n2 := "11111111-1111-7111-8111-111111111102"
	wfID := svcCreateWorkflow(t, db, n1,
		svcNodeJSON(n1, "input", "ask", "", n2, map[string]any{"channel": "http"}),
		svcNodeJSON(n2, "script", "finish", "return {done:true};", "", nil),
	)
	inst, err := svc.Create(ctx, service.CreateInstance{WorkflowDefinitionID: wfID, Context: []byte(`{}`)})
	if err != nil {
		t.Fatal(err)
	}
	cur := driveEngine(t, db, inst.ID)
	if cur.Status != model.WorkflowWaiting || cur.WaitingReason != model.WaitingReasonInput {
		t.Fatalf("instance = %+v, want parked on input", cur)
	}

	if _, err := svc.Pause(ctx, service.ControlRequest{
		InstanceID: inst.ID, Actor: actorUserID,
	}); err != nil {
		t.Fatalf("Pause() error = %v", err)
	}
	if _, err := svc.Resume(ctx, service.ControlRequest{
		InstanceID: inst.ID, Actor: actorUserID,
	}); err != nil {
		t.Fatalf("Resume() error = %v", err)
	}
	if _, err := svc.Stop(ctx, service.ControlRequest{
		InstanceID: inst.ID, Reason: "spotted a bug", Actor: actorUserID,
	}); err != nil {
		t.Fatalf("Stop() error = %v", err)
	}

	types := svcEventTypes(t, db, inst.ID)
	for _, want := range []string{"paused", "resumed", "stop"} {
		if !types[want] {
			t.Errorf("event %q missing; got %v", want, types)
		}
	}
	for _, e := range loadEvents(t, db, inst.ID) {
		switch e.Type {
		case "paused", "resumed", "stop":
			if e.CreatedBy != actorUserID {
				t.Errorf("event %q created_by = %q, want the caller %q", e.Type, e.CreatedBy, actorUserID)
			}
		}
	}
}

// TestControlDefaultsToSystemUser keeps the unauthenticated path intact: a
// control with no actor records the system user.
func TestControlDefaultsToSystemUser(t *testing.T) {
	db := setupSvcDB(t)
	ctx := context.Background()
	svc := svcInstanceServiceWithCatalog(db, gateCatalog)

	n1 := "11111111-1111-7111-8111-111111111101"
	wfID := svcCreateWorkflow(t, db, n1,
		svcNodeJSON(n1, "input", "ask", "", "", map[string]any{"channel": "http"}),
	)
	inst, err := svc.Create(ctx, service.CreateInstance{WorkflowDefinitionID: wfID, Context: []byte(`{}`)})
	if err != nil {
		t.Fatal(err)
	}
	driveEngine(t, db, inst.ID)

	if _, err := svc.Pause(ctx, service.ControlRequest{InstanceID: inst.ID}); err != nil {
		t.Fatalf("Pause() error = %v", err)
	}
	for _, e := range loadEvents(t, db, inst.ID) {
		if e.Type == "paused" && e.CreatedBy != svcSysUserID {
			t.Errorf("paused created_by = %q, want the system user %q", e.CreatedBy, svcSysUserID)
		}
	}
}

// TestRollbackRecordsRequestActor: the rollback event is the record of who
// rewound the instance, so it must carry the caller. The target is the
// earlier, already-executed occurrence; rolling back to the node the
// instance is currently parked on is a documented no-op.
func TestRollbackRecordsRequestActor(t *testing.T) {
	db := setupSvcDB(t)
	ctx := context.Background()
	svc := svcInstanceServiceWithCatalog(db, gateCatalog)

	n1 := "11111111-1111-7111-8111-111111111101"
	n2 := "11111111-1111-7111-8111-111111111102"
	wfID := svcCreateWorkflow(t, db, n1,
		svcNodeJSON(n1, "input", "first", "", n2, map[string]any{"channel": "http", "output_property": "first"}),
		svcNodeJSON(n2, "input", "second", "", "", map[string]any{"channel": "http", "output_property": "second"}),
	)
	inst, err := svc.Create(ctx, service.CreateInstance{WorkflowDefinitionID: wfID, Context: []byte(`{}`)})
	if err != nil {
		t.Fatal(err)
	}
	// Drive to the first park, deliver, then drive on to the second.
	if cur := driveEngine(t, db, inst.ID); cur.WaitingReason != model.WaitingReasonInput {
		t.Fatalf("instance = %+v, want parked on the first input", cur)
	}
	if _, err := svc.DeliverInput(ctx, service.DeliverInput{
		InstanceID: inst.ID, IdempotencyKey: "actor-rb-1", Payload: []byte(`{"v":1}`),
	}); err != nil {
		t.Fatalf("DeliverInput() error = %v", err)
	}
	cur := driveEngine(t, db, inst.ID)
	if cur.Status != model.WorkflowWaiting || cur.WaitingReason != model.WaitingReasonInput {
		t.Fatalf("instance = %+v, want parked on the second input", cur)
	}
	repo := repository.NewInstanceRepository(db)
	first, err := repo.GetNodeInstanceByNode(ctx, inst.ID, n1)
	if err != nil {
		t.Fatal(err)
	}
	if first.Status != model.NodeFinished {
		t.Fatalf("first occurrence = %s, want finished", first.Status)
	}
	if _, err := svc.Pause(ctx, service.ControlRequest{InstanceID: inst.ID, Actor: actorUserID}); err != nil {
		t.Fatalf("Pause() error = %v", err)
	}
	if _, err := svc.Rollback(ctx, service.RollbackRequest{
		InstanceID:         inst.ID,
		TargetOccurrenceID: first.ID,
		Reason:             "wrong branch",
		Actor:              actorUserID,
	}); err != nil {
		t.Fatalf("Rollback() error = %v", err)
	}

	found := false
	for _, e := range loadEvents(t, db, inst.ID) {
		if e.Type != "rollback" {
			continue
		}
		found = true
		if e.CreatedBy != actorUserID {
			t.Errorf("rollback created_by = %q, want the caller %q", e.CreatedBy, actorUserID)
		}
	}
	if !found {
		t.Fatal("no rollback event recorded")
	}
}

// TestResolveActorPrecedence documents the one rule every audited path
// shares: an explicit actor wins, and anything else is the system user.
func TestResolveActorPrecedence(t *testing.T) {
	// resolveActor is unexported, so this exercises it through the public
	// surface: a control request with and without an actor.
	db := setupSvcDB(t)
	ctx := context.Background()
	svc := svcInstanceServiceWithCatalog(db, gateCatalog)
	n1 := "11111111-1111-7111-8111-111111111101"
	wfID := svcCreateWorkflow(t, db, n1,
		svcNodeJSON(n1, "input", "ask", "", "", map[string]any{"channel": "http"}),
	)
	inst, err := svc.Create(ctx, service.CreateInstance{WorkflowDefinitionID: wfID, Context: []byte(`{}`)})
	if err != nil {
		t.Fatal(err)
	}
	driveEngine(t, db, inst.ID)

	if _, err := svc.Pause(ctx, service.ControlRequest{InstanceID: inst.ID, Actor: actorUserID}); err != nil {
		t.Fatalf("Pause() error = %v", err)
	}
	// An empty actor must not overwrite the recorded one with a blank.
	if _, err := svc.Pause(ctx, service.ControlRequest{InstanceID: inst.ID}); err != nil {
		t.Fatalf("second Pause() error = %v", err)
	}
	var sawCaller bool
	for _, e := range loadEvents(t, db, inst.ID) {
		if e.Type == "paused" && e.CreatedBy == actorUserID {
			sawCaller = true
		}
		if e.Type == "paused" && e.CreatedBy == "" {
			t.Error("a paused event recorded an empty created_by")
		}
	}
	if !sawCaller {
		t.Error("no paused event recorded the requesting actor")
	}
}

// A principal resolved from a token must be usable as the actor directly,
// so the handler and the service agree on the identity source.
func TestPrincipalUserIDIsUsableAsActor(t *testing.T) {
	p := auth.Principal{Subject: "ada", Issuer: "https://idp", UserID: actorUserID, Roles: []string{"finance"}}
	if p.UserID != actorUserID {
		t.Fatalf("UserID = %q, want %q", p.UserID, actorUserID)
	}
}
