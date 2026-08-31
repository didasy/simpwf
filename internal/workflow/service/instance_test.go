package service_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/simpwf/workflow-engine/internal/workflow/engine"
	"github.com/simpwf/workflow-engine/internal/workflow/executor"
	"github.com/simpwf/workflow-engine/internal/workflow/model"
	"github.com/simpwf/workflow-engine/internal/workflow/repository"
	"github.com/simpwf/workflow-engine/internal/workflow/service"
	"github.com/simpwf/workflow-engine/pkg/database"
	"github.com/simpwf/workflow-engine/pkg/ids"
	"gorm.io/gorm"
)

const svcSysUserID = "11111111-1111-7111-8111-111111111111"

var svcLimits = model.NodeLimits{DefaultTimeout: 5 * time.Second, MaxTimeout: 10 * time.Second, ConditionTimeout: 2 * time.Second}

func setupSvcDB(t *testing.T) *gorm.DB {
	t.Helper()
	dsn := os.Getenv("TEST_DATABASE_DSN_SERVICE")
	if dsn == "" {
		dsn = os.Getenv("TEST_DATABASE_DSN")
	}
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
	if err := db.AutoMigrate(
		&repository.UserModel{}, &repository.NodeDefinitionModel{},
		&repository.WorkflowDefinitionModel{}, &repository.WorkflowDefinitionNodeRefModel{},
		&repository.WorkflowRequestModel{}, &repository.WorkflowInstanceModel{},
		&repository.NodeInstanceModel{}, &repository.WorkflowInstanceEventModel{},
		&repository.InputDeliveryModel{}, &repository.StatusUpdateOutboxModel{},
	); err != nil {
		t.Fatalf("AutoMigrate() error = %v", err)
	}
	if err := db.Exec(`TRUNCATE TABLE
		status_update_outbox, input_deliveries, workflow_instance_events, node_instances,
		workflow_instances, workflow_requests, workflow_definition_node_refs,
		workflow_definitions, node_definitions, users RESTART IDENTITY`).Error; err != nil {
		t.Fatalf("truncate tables: %v", err)
	}
	ctx := context.Background()
	if err := repository.UpsertSystemUser(ctx, db, model.User{ID: svcSysUserID, Name: "system", Email: "system@localhost"}); err != nil {
		t.Fatalf("seed system user: %v", err)
	}
	return db
}

func svcNewID() string {
	id, err := ids.NewString()
	if err != nil {
		panic(err)
	}
	return id
}

func svcNodeJSON(id, typ, name, script, next string, extra map[string]any) string {
	m := map[string]any{"id": id, "type": typ, "name": name}
	if script != "" {
		m["script"] = script
	}
	if next != "" {
		m["next_node"] = next
	}
	for k, v := range extra {
		m[k] = v
	}
	b, _ := json.Marshal(m)
	return string(b)
}

func svcCreateWorkflow(t *testing.T, db *gorm.DB, start string, nodes ...string) string {
	t.Helper()
	raw := fmt.Sprintf(`{"start_node_id":%q,"nodes":[%s]}`, start, joinAll(nodes))
	wf := model.WorkflowDefinition{
		ID: svcNewID(), Name: "svc-flow", Version: 1, LineageID: svcNewID(),
		Content: json.RawMessage(raw), CreatedBy: svcSysUserID, UpdatedBy: svcSysUserID,
		CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC(),
	}
	if err := repository.NewWorkflowDefinitionRepository(db).Create(context.Background(), wf); err != nil {
		t.Fatalf("create workflow: %v", err)
	}
	return wf.ID
}

func joinAll(parts []string) string {
	out := ""
	for i, p := range parts {
		if i > 0 {
			out += ","
		}
		out += p
	}
	return out
}

// svcWorkflowService mirrors production wiring: the workflow definition
// service resolves node_definition_id references into the executable tree.
func svcWorkflowService(db *gorm.DB) service.WorkflowDefinitionService {
	return service.NewWorkflowDefinitionService(
		repository.NewWorkflowDefinitionRepository(db),
		repository.NewNodeDefinitionRepository(db),
		svcLimits,
		svcSysUserID,
	)
}

func svcInstanceService(db *gorm.DB) service.InstanceService {
	instances := repository.NewInstanceRepository(db)
	validator := &executor.InputExecutor{}
	return service.NewInstanceService(
		instances,
		repository.NewWorkflowDefinitionRepository(db),
		svcWorkflowService(db),
		validator,
		executor.NewHookRunner(nil),
		svcSysUserID,
		svcLimits,
		nil,
	)
}

// driveEngine runs claim -> process until the instance parks on input,
// finishes, or fails.
func driveEngine(t *testing.T, db *gorm.DB, instanceID string) model.WorkflowInstance {
	return driveEngineWithExecLimits(t, db, instanceID, executor.Limits{})
}

func driveEngineWithExecLimits(t *testing.T, db *gorm.DB, instanceID string, execLimits executor.Limits) model.WorkflowInstance {
	t.Helper()
	ctx := context.Background()
	instances := repository.NewInstanceRepository(db)
	wfSvc := svcWorkflowService(db)
	loader := func(ctx context.Context, id string) (*model.WorkflowContent, error) {
		inst, err := instances.GetByID(ctx, id)
		if err != nil {
			return nil, err
		}
		def, err := repository.NewWorkflowDefinitionRepository(db).GetByID(ctx, inst.WorkflowDefinitionID)
		if err != nil {
			return nil, err
		}
		wc, err := model.ParseWorkflowContent(def.Content, svcLimits)
		if err != nil {
			return nil, err
		}
		return wfSvc.Materialize(ctx, wc)
	}
	e := engine.NewEngine(instances, executor.NewExecutors(execLimits, nil, executor.Dependencies{}), executor.NewHookRunner(nil), model.DefaultLimits(), loader, svcSysUserID)
	for i := 0; i < 200; i++ {
		claimed, err := instances.ClaimNext(ctx, "svc-worker", time.Minute, 10)
		if err != nil {
			t.Fatal(err)
		}
		for _, w := range claimed {
			if err := e.Process(ctx, w); err != nil {
				t.Fatalf("Process() error = %v", err)
			}
		}
		cur, err := instances.GetByID(ctx, instanceID)
		if err != nil {
			t.Fatal(err)
		}
		if cur.Status == model.WorkflowWaiting && cur.WaitingReason == model.WaitingReasonInput {
			return *cur
		}
		if cur.Status == model.WorkflowFinished || cur.Status == model.WorkflowFailed || cur.Status == model.WorkflowStopped {
			return *cur
		}
	}
	t.Fatal("engine did not settle")
	return model.WorkflowInstance{}
}

func TestCreateInstance(t *testing.T) {
	db := setupSvcDB(t)
	ctx := context.Background()
	wfID := svcCreateWorkflow(t, db, "11111111-1111-7111-8111-111111111101",
		svcNodeJSON("11111111-1111-7111-8111-111111111101", "script", "a", "return 1;", "", nil),
	)
	svc := svcInstanceService(db)

	inst, err := svc.Create(ctx, service.CreateInstance{
		WorkflowDefinitionID: wfID,
		Context:              json.RawMessage(`{"x":1}`),
	})
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	if inst.Status != model.WorkflowWaiting || inst.WaitingReason != model.WaitingReasonRunnable {
		t.Errorf("instance = %+v", inst)
	}
	frame, err := model.ParseFrame(inst.Frame)
	if err != nil {
		t.Fatal(err)
	}
	if frame.CurrentNodeID != "11111111-1111-7111-8111-111111111101" {
		t.Errorf("frame = %+v, want start node", frame)
	}
	if string(inst.Context) != `{"x":1}` {
		t.Errorf("context = %s", inst.Context)
	}
	if inst.CreatedBy != svcSysUserID || inst.UpdatedBy != svcSysUserID {
		t.Errorf("audit actors = %q/%q, want %q", inst.CreatedBy, inst.UpdatedBy, svcSysUserID)
	}
	got, err := svc.GetStatus(ctx, inst.ID)
	if err != nil {
		t.Fatalf("GetStatus() error = %v", err)
	}
	if got.CreatedBy != svcSysUserID || got.UpdatedBy != svcSysUserID {
		t.Errorf("persisted audit actors = %q/%q, want %q", got.CreatedBy, got.UpdatedBy, svcSysUserID)
	}
}

func TestCreateInstanceDefaultsContextAndErrors(t *testing.T) {
	db := setupSvcDB(t)
	ctx := context.Background()
	wfID := svcCreateWorkflow(t, db, "11111111-1111-7111-8111-111111111101",
		svcNodeJSON("11111111-1111-7111-8111-111111111101", "script", "a", "return 1;", "", nil),
	)
	svc := svcInstanceService(db)

	inst, err := svc.Create(ctx, service.CreateInstance{WorkflowDefinitionID: wfID})
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	if string(inst.Context) != "{}" {
		t.Errorf("context = %s, want {}", inst.Context)
	}

	if _, err := svc.Create(ctx, service.CreateInstance{WorkflowDefinitionID: svcNewID()}); err == nil {
		t.Error("Create(unknown workflow) error = nil")
	}
	if _, err := svc.Create(ctx, service.CreateInstance{WorkflowDefinitionID: wfID, Context: json.RawMessage(`"not-an-object"`)}); err == nil {
		t.Error("Create(non-object context) error = nil")
	}
}

func TestDeliverInputValidResumes(t *testing.T) {
	db := setupSvcDB(t)
	ctx := context.Background()
	svc := svcInstanceService(db)

	wfID := svcCreateWorkflow(t, db, "11111111-1111-7111-8111-111111111101",
		svcNodeJSON("11111111-1111-7111-8111-111111111101", "input", "ask", "", "11111111-1111-7111-8111-111111111102",
			map[string]any{"channel": "http", "context_path": "webhook"}),
		svcNodeJSON("11111111-1111-7111-8111-111111111102", "script", "after", "return context.webhook.success ? 'ok' : 'no';", "", map[string]any{"output_property": "after"}),
	)
	inst, err := svc.Create(ctx, service.CreateInstance{WorkflowDefinitionID: wfID, Context: json.RawMessage(`{}`)})
	if err != nil {
		t.Fatal(err)
	}
	cur := driveEngine(t, db, inst.ID)
	if cur.Status != model.WorkflowWaiting || cur.WaitingReason != model.WaitingReasonInput {
		t.Fatalf("instance = %+v, want waiting on input", cur)
	}

	delivery, err := svc.DeliverInput(ctx, service.DeliverInput{
		InstanceID: inst.ID, IdempotencyKey: "key-1", Payload: []byte(`{"success":true}`),
	})
	if err != nil {
		t.Fatalf("DeliverInput() error = %v", err)
	}
	if !delivery.Accepted {
		t.Errorf("delivery = %+v, want accepted", delivery)
	}

	// the instance resumes and finishes
	cur = driveEngine(t, db, inst.ID)
	if cur.Status != model.WorkflowFinished {
		t.Fatalf("status = %s, want finished (error %q)", cur.Status, cur.Error)
	}
	got, _ := svc.GetContext(ctx, inst.ID)
	var ctxMap map[string]any
	_ = json.Unmarshal(got.Context, &ctxMap)
	wh, ok := ctxMap["webhook"].(map[string]any)
	if !ok || wh["success"] != true {
		t.Errorf("context = %v, want webhook payload at context_path", ctxMap)
	}
	if ctxMap["after"] != "ok" {
		t.Errorf("after = %v", ctxMap["after"])
	}
}

func TestDeliverInputMaterializesReferencedInputNode(t *testing.T) {
	db := setupSvcDB(t)
	ctx := context.Background()
	svc := svcInstanceService(db)

	ndID := "11111111-1111-7111-8111-111111111301"
	nodeDef := model.NodeDefinition{
		ID: ndID, Name: "ask", Version: 1, LineageID: svcNewID(),
		Type: "input", Content: json.RawMessage(`{"type":"input","channel":"http","context_path":"webhook"}`),
		CreatedBy: svcSysUserID, UpdatedBy: svcSysUserID,
		CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC(),
	}
	if err := repository.NewNodeDefinitionRepository(db).Create(ctx, nodeDef); err != nil {
		t.Fatalf("create node definition: %v", err)
	}

	// The workflow node carries only a node_definition_id reference, like the
	// production "new-post" node: no inline type.
	wfID := svcCreateWorkflow(t, db, "11111111-1111-7111-8111-111111111302",
		`{"id":"11111111-1111-7111-8111-111111111302","name":"ask","node_definition_id":"`+ndID+`"}`,
	)
	inst, err := svc.Create(ctx, service.CreateInstance{WorkflowDefinitionID: wfID, Context: json.RawMessage(`{}`)})
	if err != nil {
		t.Fatal(err)
	}
	cur := driveEngine(t, db, inst.ID)
	if cur.Status != model.WorkflowWaiting || cur.WaitingReason != model.WaitingReasonInput {
		t.Fatalf("instance = %+v, want waiting on input", cur)
	}

	delivery, err := svc.DeliverInput(ctx, service.DeliverInput{
		InstanceID: inst.ID, IdempotencyKey: "ref-1", Payload: []byte(`{"success":true}`),
	})
	if err != nil {
		t.Fatalf("DeliverInput() error = %v", err)
	}
	if !delivery.Accepted {
		t.Fatalf("delivery = %+v, want accepted", delivery)
	}

	curStatus, _ := svc.GetStatus(ctx, inst.ID)
	if curStatus.Status != model.WorkflowFinished {
		t.Fatalf("status = %s, want finished after delivery", curStatus.Status)
	}
	got, _ := svc.GetContext(ctx, inst.ID)
	var ctxMap map[string]any
	_ = json.Unmarshal(got.Context, &ctxMap)
	wh, ok := ctxMap["webhook"].(map[string]any)
	if !ok || wh["success"] != true {
		t.Errorf("context = %v, want payload written at context_path", ctxMap)
	}
}

func TestDeliverInputRejectsWithMessage(t *testing.T) {
	db := setupSvcDB(t)
	ctx := context.Background()
	svc := svcInstanceService(db)

	wfID := svcCreateWorkflow(t, db, "11111111-1111-7111-8111-111111111101",
		svcNodeJSON("11111111-1111-7111-8111-111111111101", "input", "ask", "", "", map[string]any{
			"channel": "http", "context_path": "webhook",
			"validation": map[string]any{
				"script": "input = JSON.parse(input); if (!input.success) { return 'Webhook failed!'; };",
			},
		}),
	)
	inst, err := svc.Create(ctx, service.CreateInstance{WorkflowDefinitionID: wfID})
	if err != nil {
		t.Fatal(err)
	}
	driveEngine(t, db, inst.ID)

	delivery, err := svc.DeliverInput(ctx, service.DeliverInput{
		InstanceID: inst.ID, IdempotencyKey: "bad-1", Payload: []byte(`{"success":false}`),
	})
	if err != nil {
		t.Fatalf("DeliverInput() error = %v", err)
	}
	if delivery.Accepted || delivery.Error != "Webhook failed!" {
		t.Errorf("delivery = %+v, want rejected with message", delivery)
	}
	// instance still waits on input
	cur, _ := svc.GetStatus(ctx, inst.ID)
	if cur.Status != model.WorkflowWaiting || cur.WaitingReason != model.WaitingReasonInput {
		t.Errorf("instance = %+v, want still waiting on input", cur)
	}
}

func TestDeliverInputIdempotentReplay(t *testing.T) {
	db := setupSvcDB(t)
	ctx := context.Background()
	svc := svcInstanceService(db)

	wfID := svcCreateWorkflow(t, db, "11111111-1111-7111-8111-111111111101",
		svcNodeJSON("11111111-1111-7111-8111-111111111101", "input", "ask", "", "11111111-1111-7111-8111-111111111102",
			map[string]any{"channel": "http", "context_path": "webhook"}),
		svcNodeJSON("11111111-1111-7111-8111-111111111102", "script", "after", "return 1;", "", map[string]any{"output_property": "after"}),
	)
	inst, err := svc.Create(ctx, service.CreateInstance{WorkflowDefinitionID: wfID})
	if err != nil {
		t.Fatal(err)
	}
	driveEngine(t, db, inst.ID)

	first, err := svc.DeliverInput(ctx, service.DeliverInput{
		InstanceID: inst.ID, IdempotencyKey: "key-1", Payload: []byte(`{"ok":1}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	driveEngine(t, db, inst.ID) // completes the workflow

	replay, err := svc.DeliverInput(ctx, service.DeliverInput{
		InstanceID: inst.ID, IdempotencyKey: "key-1", Payload: []byte(`{"ok":2}`),
	})
	if err != nil {
		t.Fatalf("replay error = %v", err)
	}
	if replay.Accepted != first.Accepted || replay.ID != first.ID {
		t.Errorf("replay = %+v, want the originally recorded delivery %+v", replay, first)
	}
}

func TestDeliverInputNotWaitingConflict(t *testing.T) {
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
	if _, err := svc.DeliverInput(ctx, service.DeliverInput{
		InstanceID: inst.ID, IdempotencyKey: "k", Payload: []byte(`{}`),
	}); err == nil {
		t.Error("DeliverInput() on runnable instance error = nil, want conflict")
	}
}

func TestDeliverInputSourceMustMatchChannel(t *testing.T) {
	db := setupSvcDB(t)
	ctx := context.Background()
	svc := svcInstanceService(db)
	wfID := svcCreateWorkflow(t, db, "11111111-1111-7111-8111-111111111101",
		svcNodeJSON("11111111-1111-7111-8111-111111111101", "input", "ask", "", "", map[string]any{
			"channel": "redis", "context_path": "webhook",
		}),
	)
	inst, err := svc.Create(ctx, service.CreateInstance{WorkflowDefinitionID: wfID})
	if err != nil {
		t.Fatal(err)
	}
	driveEngine(t, db, inst.ID)

	// A delivery on a transport that does not match the input node channel
	// is rejected as a conflict, even with a valid payload.
	if _, err := svc.DeliverInput(ctx, service.DeliverInput{
		InstanceID: inst.ID, IdempotencyKey: "http-key", Payload: []byte(`{"ok":1}`), Source: "http",
	}); err == nil {
		t.Error("DeliverInput(http) on redis channel error = nil, want conflict")
	}

	// The matching transport is accepted.
	delivery, err := svc.DeliverInput(ctx, service.DeliverInput{
		InstanceID: inst.ID, IdempotencyKey: "redis-key", Payload: []byte(`{"ok":1}`), Source: "redis",
	})
	if err != nil {
		t.Fatalf("DeliverInput(redis) error = %v", err)
	}
	if !delivery.Accepted {
		t.Errorf("delivery accepted = false, want true")
	}
}

func TestDeliverInputFinishesWorkflow(t *testing.T) {
	db := setupSvcDB(t)
	ctx := context.Background()
	svc := svcInstanceService(db)

	// Input node is the last node: delivery must finish the workflow.
	wfID := svcCreateWorkflow(t, db, "11111111-1111-7111-8111-111111111101",
		svcNodeJSON("11111111-1111-7111-8111-111111111101", "input", "ask", "", "", map[string]any{
			"channel": "http", "context_path": "webhook",
		}),
	)
	inst, err := svc.Create(ctx, service.CreateInstance{WorkflowDefinitionID: wfID})
	if err != nil {
		t.Fatal(err)
	}
	driveEngine(t, db, inst.ID)

	if _, err := svc.DeliverInput(ctx, service.DeliverInput{
		InstanceID: inst.ID, IdempotencyKey: "last-1", Payload: []byte(`{"done":true}`),
	}); err != nil {
		t.Fatalf("DeliverInput() error = %v", err)
	}
	cur, _ := svc.GetStatus(ctx, inst.ID)
	if cur.Status != model.WorkflowFinished {
		t.Errorf("status = %s, want finished", cur.Status)
	}
}

func TestNodeDebugNotStarted(t *testing.T) {
	db := setupSvcDB(t)
	ctx := context.Background()
	svc := svcInstanceService(db)

	// Two nodes; the instance never runs, so the second node has no
	// occurrence yet.
	wfID := svcCreateWorkflow(t, db, "11111111-1111-7111-8111-111111111101",
		svcNodeJSON("11111111-1111-7111-8111-111111111101", "script", "a", "return 1;", "11111111-1111-7111-8111-111111111102", nil),
		svcNodeJSON("11111111-1111-7111-8111-111111111102", "script", "b", "return 2;", "", nil),
	)
	inst, err := svc.Create(ctx, service.CreateInstance{WorkflowDefinitionID: wfID})
	if err != nil {
		t.Fatal(err)
	}

	d, err := svc.NodeDebug(ctx, inst.ID, "11111111-1111-7111-8111-111111111102", 0)
	if err != nil {
		t.Fatalf("NodeDebug() error = %v", err)
	}
	if d.Status != "not_started" || d.AttemptCount != 0 {
		t.Errorf("detail = %+v, want not_started with 0 attempts", d)
	}
	if d.SelectedAttempt != nil || d.LatestAttempt != nil {
		t.Errorf("detail = %+v, want nil attempts", d)
	}
	if d.OccurrenceID != "11111111-1111-7111-8111-111111111102" || d.Name != "b" || d.Type != "script" {
		t.Errorf("detail = %+v", d)
	}
	if len(d.ContextBefore) != 0 || len(d.ContextAfter) != 0 || len(d.Output) != 0 {
		t.Errorf("detail = %+v, want nil snapshots", d)
	}
}

func TestNodeDebugFinishedLatestAndExact(t *testing.T) {
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
	driveEngine(t, db, inst.ID)

	d, err := svc.NodeDebug(ctx, inst.ID, "11111111-1111-7111-8111-111111111101", 0)
	if err != nil {
		t.Fatalf("NodeDebug() error = %v", err)
	}
	if d.Status != "finished" || d.AttemptCount != 1 {
		t.Errorf("detail = %+v, want finished with 1 attempt", d)
	}
	if d.SelectedAttempt == nil || *d.SelectedAttempt != 1 || d.LatestAttempt == nil || *d.LatestAttempt != 1 {
		t.Errorf("detail = %+v, want selected/latest 1", d)
	}
	if len(d.ContextBefore) == 0 || len(d.ContextAfter) == 0 || len(d.Output) == 0 {
		t.Errorf("detail = %+v, want before/after/output snapshots", d)
	}
	if d.DurationMS == nil {
		t.Errorf("detail = %+v, want duration", d)
	}
	if d.FinishedAt == nil {
		t.Errorf("detail = %+v, want finished_at", d)
	}

	// Exact attempt 1 is selectable.
	d1, err := svc.NodeDebug(ctx, inst.ID, "11111111-1111-7111-8111-111111111101", 1)
	if err != nil {
		t.Fatalf("NodeDebug(attempt=1) error = %v", err)
	}
	if d1.SelectedAttempt == nil || *d1.SelectedAttempt != 1 {
		t.Errorf("detail = %+v, want selected 1", d1)
	}

	// Attempt 2 never ran.
	if _, err := svc.NodeDebug(ctx, inst.ID, "11111111-1111-7111-8111-111111111101", 2); !errors.Is(err, model.ErrNotFound) {
		t.Errorf("NodeDebug(attempt=2) error = %v, want ErrNotFound", err)
	}
}

func TestNodeDebugLoopAttemptsAndRunning(t *testing.T) {
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
	driveEngine(t, db, inst.ID)

	// Simulate a second attempt of the same occurrence (recovery retry)
	// that is still running.
	instances := repository.NewInstanceRepository(db)
	n, err := instances.GetNodeInstanceByNode(ctx, inst.ID, "11111111-1111-7111-8111-111111111101")
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	n.Attempt = 2
	n.Status = model.NodeRunning
	n.StartedAt = &now
	n.FinishedAt = nil
	if err := instances.UpdateNodeInstance(ctx, *n); err != nil {
		t.Fatal(err)
	}

	d, err := svc.NodeDebug(ctx, inst.ID, "11111111-1111-7111-8111-111111111101", 0)
	if err != nil {
		t.Fatalf("NodeDebug() error = %v", err)
	}
	if d.Status != "running" || d.AttemptCount != 2 {
		t.Errorf("detail = %+v, want running with 2 attempts", d)
	}
	if d.LatestAttempt == nil || *d.LatestAttempt != 2 {
		t.Errorf("detail = %+v, want latest 2", d)
	}
	if d.DurationMS != nil {
		t.Errorf("detail = %+v, want nil duration while running", d)
	}
	if d.FinishedAt != nil {
		t.Errorf("detail = %+v, want nil finished_at while running", d)
	}

	// Exact attempt 1 resolves with the occurrence's metadata.
	d1, err := svc.NodeDebug(ctx, inst.ID, "11111111-1111-7111-8111-111111111101", 1)
	if err != nil {
		t.Fatalf("NodeDebug(attempt=1) error = %v", err)
	}
	if d1.SelectedAttempt == nil || *d1.SelectedAttempt != 1 || d1.LatestAttempt == nil || *d1.LatestAttempt != 2 {
		t.Errorf("detail = %+v, want selected 1 latest 2", d1)
	}

	// Attempt 3 never ran.
	if _, err := svc.NodeDebug(ctx, inst.ID, "11111111-1111-7111-8111-111111111101", 3); !errors.Is(err, model.ErrNotFound) {
		t.Errorf("NodeDebug(attempt=3) error = %v, want ErrNotFound", err)
	}

	// The occurrence id resolves to the same occurrence.
	dOcc, err := svc.NodeDebug(ctx, inst.ID, n.ID, 0)
	if err != nil {
		t.Fatalf("NodeDebug(occurrence id) error = %v", err)
	}
	if dOcc.OccurrenceID != n.ID || dOcc.Name != "a" {
		t.Errorf("detail = %+v, want occurrence %s", dOcc, n.ID)
	}
}

func TestNodeDebugErrors(t *testing.T) {
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

	if _, err := svc.NodeDebug(ctx, svcNewID(), "11111111-1111-7111-8111-111111111101", 0); !errors.Is(err, model.ErrNotFound) {
		t.Errorf("NodeDebug(unknown instance) error = %v, want ErrNotFound", err)
	}
	if _, err := svc.NodeDebug(ctx, inst.ID, "99999999-9999-7999-8999-999999999999", 0); !errors.Is(err, model.ErrNotFound) {
		t.Errorf("NodeDebug(unknown node) error = %v, want ErrNotFound", err)
	}
	if _, err := svc.NodeDebug(ctx, inst.ID, "99999999-9999-7999-8999-999999999998", 1); !errors.Is(err, model.ErrNotFound) {
		t.Errorf("NodeDebug(unknown occurrence) error = %v, want ErrNotFound", err)
	}
}

type fakeCanceller struct {
	mu  sync.Mutex
	ids []string
}

func (f *fakeCanceller) Cancel(id string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.ids = append(f.ids, id)
}

func (f *fakeCanceller) cancelled() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := append([]string(nil), f.ids...)
	return out
}

func svcControlService(db *gorm.DB, c service.Canceller) service.InstanceService {
	instances := repository.NewInstanceRepository(db)
	return service.NewInstanceService(
		instances,
		repository.NewWorkflowDefinitionRepository(db),
		svcWorkflowService(db),
		&executor.InputExecutor{},
		executor.NewHookRunner(nil),
		svcSysUserID,
		svcLimits,
		c,
	)
}

func svcEventTypes(t *testing.T, db *gorm.DB, instanceID string) map[string]bool {
	t.Helper()
	events, err := repository.NewInstanceRepository(db).ListEvents(context.Background(), instanceID)
	if err != nil {
		t.Fatal(err)
	}
	types := map[string]bool{}
	for _, ev := range events {
		types[ev.Type] = true
	}
	return types
}

func TestControlPauseImmediateAndIdempotent(t *testing.T) {
	db := setupSvcDB(t)
	ctx := context.Background()
	svc := svcControlService(db, nil)

	wfID := svcCreateWorkflow(t, db, "11111111-1111-7111-8111-111111111101",
		svcNodeJSON("11111111-1111-7111-8111-111111111101", "script", "a", "return 1;", "", nil),
	)
	inst, err := svc.Create(ctx, service.CreateInstance{WorkflowDefinitionID: wfID})
	if err != nil {
		t.Fatal(err)
	}

	res, err := svc.Pause(ctx, inst.ID)
	if err != nil {
		t.Fatalf("Pause() error = %v", err)
	}
	if res.Status != model.WorkflowPaused || res.PauseRequested {
		t.Errorf("res = %+v, want paused immediately", res)
	}
	stored, _ := svc.GetStatus(ctx, inst.ID)
	if stored.Status != model.WorkflowPaused {
		t.Errorf("status = %s, want paused", stored.Status)
	}
	if !svcEventTypes(t, db, inst.ID)["paused"] {
		t.Error("event 'paused' missing")
	}

	res2, err := svc.Pause(ctx, inst.ID)
	if err != nil || res2.Status != model.WorkflowPaused {
		t.Errorf("second pause = %+v, err %v, want idempotent", res2, err)
	}
}

func TestControlPauseDeferred(t *testing.T) {
	db := setupSvcDB(t)
	ctx := context.Background()
	svc := svcControlService(db, nil)

	wfID := svcCreateWorkflow(t, db, "11111111-1111-7111-8111-111111111101",
		svcNodeJSON("11111111-1111-7111-8111-111111111101", "script", "a", "return 1;", "", nil),
	)
	inst, err := svc.Create(ctx, service.CreateInstance{WorkflowDefinitionID: wfID})
	if err != nil {
		t.Fatal(err)
	}
	instances := repository.NewInstanceRepository(db)
	claimed, err := instances.ClaimNext(ctx, "worker-1", time.Minute, 10)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("claim = %d, err %v", len(claimed), err)
	}

	res, err := svc.Pause(ctx, inst.ID)
	if err != nil {
		t.Fatalf("Pause() error = %v", err)
	}
	if res.Status != model.WorkflowRunning || !res.PauseRequested {
		t.Errorf("res = %+v, want deferred pause on running instance", res)
	}
	if !svcEventTypes(t, db, inst.ID)["pause_requested"] {
		t.Error("event 'pause_requested' missing")
	}
}

func TestControlPauseTerminalConflict(t *testing.T) {
	db := setupSvcDB(t)
	ctx := context.Background()
	svc := svcControlService(db, nil)

	wfID := svcCreateWorkflow(t, db, "11111111-1111-7111-8111-111111111101",
		svcNodeJSON("11111111-1111-7111-8111-111111111101", "script", "a", "return 1;", "", nil),
	)
	inst, err := svc.Create(ctx, service.CreateInstance{WorkflowDefinitionID: wfID})
	if err != nil {
		t.Fatal(err)
	}
	driveEngine(t, db, inst.ID) // finishes

	if _, err := svc.Pause(ctx, inst.ID); !errors.Is(err, model.ErrConflict) {
		t.Errorf("Pause(finished) error = %v, want ErrConflict", err)
	}
}

func TestControlResume(t *testing.T) {
	db := setupSvcDB(t)
	ctx := context.Background()
	svc := svcControlService(db, nil)

	wfID := svcCreateWorkflow(t, db, "11111111-1111-7111-8111-111111111101",
		svcNodeJSON("11111111-1111-7111-8111-111111111101", "script", "a", "return 1;", "", nil),
	)
	inst, err := svc.Create(ctx, service.CreateInstance{WorkflowDefinitionID: wfID})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Pause(ctx, inst.ID); err != nil {
		t.Fatal(err)
	}
	res, err := svc.Resume(ctx, inst.ID)
	if err != nil {
		t.Fatalf("Resume() error = %v", err)
	}
	if res.Status != model.WorkflowWaiting {
		t.Errorf("res = %+v, want waiting", res)
	}
	stored, _ := svc.GetStatus(ctx, inst.ID)
	if stored.Status != model.WorkflowWaiting {
		t.Errorf("status = %s, want waiting", stored.Status)
	}
	if !svcEventTypes(t, db, inst.ID)["resumed"] {
		t.Error("event 'resumed' missing")
	}

	// Resume of an active instance is idempotent.
	if _, err := svc.Resume(ctx, inst.ID); err != nil {
		t.Errorf("resume active error = %v, want nil", err)
	}
}

func TestControlResumeClearsPendingPause(t *testing.T) {
	db := setupSvcDB(t)
	ctx := context.Background()
	svc := svcControlService(db, nil)

	wfID := svcCreateWorkflow(t, db, "11111111-1111-7111-8111-111111111101",
		svcNodeJSON("11111111-1111-7111-8111-111111111101", "script", "a", "return 1;", "", nil),
	)
	inst, err := svc.Create(ctx, service.CreateInstance{WorkflowDefinitionID: wfID})
	if err != nil {
		t.Fatal(err)
	}
	instances := repository.NewInstanceRepository(db)
	if _, err := instances.ClaimNext(ctx, "worker-1", time.Minute, 10); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Pause(ctx, inst.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Resume(ctx, inst.ID); err != nil {
		t.Fatal(err)
	}
	stored, _ := svc.GetStatus(ctx, inst.ID)
	if stored.PauseRequested {
		t.Errorf("stored = %+v, want pause_requested cleared", stored)
	}
	if !svcEventTypes(t, db, inst.ID)["resume"] {
		t.Error("event 'resume' missing")
	}
}

func TestControlStop(t *testing.T) {
	db := setupSvcDB(t)
	ctx := context.Background()
	canceller := &fakeCanceller{}
	svc := svcControlService(db, canceller)

	wfID := svcCreateWorkflow(t, db, "11111111-1111-7111-8111-111111111101",
		svcNodeJSON("11111111-1111-7111-8111-111111111101", "script", "a", "return 1;", "", nil),
	)
	inst, err := svc.Create(ctx, service.CreateInstance{WorkflowDefinitionID: wfID})
	if err != nil {
		t.Fatal(err)
	}

	// Stop a waiting instance: immediate terminal, no cancellation.
	res, err := svc.Stop(ctx, inst.ID, "operator")
	if err != nil {
		t.Fatalf("Stop() error = %v", err)
	}
	if res.Status != model.WorkflowStopped || res.TerminationPending {
		t.Errorf("res = %+v, want stopped without pending", res)
	}
	stored, _ := svc.GetStatus(ctx, inst.ID)
	if stored.Status != model.WorkflowStopped {
		t.Errorf("status = %s, want stopped", stored.Status)
	}
	if len(canceller.cancelled()) != 0 {
		t.Errorf("canceller called %v, want none for waiting instance", canceller.cancelled())
	}
	if !svcEventTypes(t, db, inst.ID)["stop"] {
		t.Error("event 'stop' missing")
	}

	// Repeat stop is idempotent.
	if _, err := svc.Stop(ctx, inst.ID, "operator"); err != nil {
		t.Errorf("second stop error = %v, want nil", err)
	}

	// Stop a running instance: pending + local cancellation signal.
	inst2, err := svc.Create(ctx, service.CreateInstance{WorkflowDefinitionID: wfID})
	if err != nil {
		t.Fatal(err)
	}
	instances := repository.NewInstanceRepository(db)
	if _, err := instances.ClaimNext(ctx, "worker-1", time.Minute, 10); err != nil {
		t.Fatal(err)
	}
	res2, err := svc.Stop(ctx, inst2.ID, "operator")
	if err != nil {
		t.Fatalf("Stop(running) error = %v", err)
	}
	if res2.Status != model.WorkflowStopped || !res2.TerminationPending {
		t.Errorf("res2 = %+v, want stopped with pending", res2)
	}
	if got := canceller.cancelled(); len(got) != 1 || got[0] != inst2.ID {
		t.Errorf("canceller ids = %v, want [%s]", got, inst2.ID)
	}
}

func TestControlStopTerminalConflict(t *testing.T) {
	db := setupSvcDB(t)
	ctx := context.Background()
	svc := svcControlService(db, nil)

	wfID := svcCreateWorkflow(t, db, "11111111-1111-7111-8111-111111111101",
		svcNodeJSON("11111111-1111-7111-8111-111111111101", "script", "a", "return 1;", "", nil),
	)
	inst, err := svc.Create(ctx, service.CreateInstance{WorkflowDefinitionID: wfID})
	if err != nil {
		t.Fatal(err)
	}
	driveEngine(t, db, inst.ID) // finished

	if _, err := svc.Stop(ctx, inst.ID, "operator"); !errors.Is(err, model.ErrConflict) {
		t.Errorf("Stop(finished) error = %v, want ErrConflict", err)
	}
	if _, err := svc.Resume(ctx, inst.ID); !errors.Is(err, model.ErrConflict) {
		t.Errorf("Resume(finished) error = %v, want ErrConflict", err)
	}
}

func TestControlStopParksInputAttemptStopped(t *testing.T) {
	db := setupSvcDB(t)
	ctx := context.Background()
	svc := svcControlService(db, nil)

	wfID := svcCreateWorkflow(t, db, "11111111-1111-7111-8111-111111111101",
		svcNodeJSON("11111111-1111-7111-8111-111111111101", "input", "ask", "", "", map[string]any{
			"channel": "http", "context_path": "webhook",
		}),
	)
	inst, err := svc.Create(ctx, service.CreateInstance{WorkflowDefinitionID: wfID})
	if err != nil {
		t.Fatal(err)
	}
	driveEngine(t, db, inst.ID) // parks waiting on input

	if _, err := svc.Stop(ctx, inst.ID, "operator"); err != nil {
		t.Fatalf("Stop() error = %v", err)
	}
	attempts, _ := repository.NewInstanceRepository(db).ListNodeInstances(ctx, inst.ID)
	if len(attempts) != 1 || attempts[0].Status != model.NodeStopped {
		t.Errorf("attempts = %+v, want the parked input attempt stopped", attempts)
	}
}

func TestInstanceServiceListDelegatesAndFilters(t *testing.T) {
	db := setupSvcDB(t)
	ctx := context.Background()
	svc := svcInstanceService(db)

	wfID := svcCreateWorkflow(t, db, "11111111-1111-7111-8111-111111111101",
		svcNodeJSON("11111111-1111-7111-8111-111111111101", "script", "a", "return 1;", "", nil),
	)
	wfID2 := svcCreateWorkflow(t, db, "11111111-1111-7111-8111-111111111202",
		svcNodeJSON("11111111-1111-7111-8111-111111111202", "script", "b", "return 2;", "", nil),
	)

	inst1, err := svc.Create(ctx, service.CreateInstance{WorkflowDefinitionID: wfID})
	if err != nil {
		t.Fatalf("Create(wf1) error = %v", err)
	}
	inst2, err := svc.Create(ctx, service.CreateInstance{WorkflowDefinitionID: wfID2})
	if err != nil {
		t.Fatalf("Create(wf2) error = %v", err)
	}
	inst3, err := svc.Create(ctx, service.CreateInstance{WorkflowDefinitionID: wfID})
	if err != nil {
		t.Fatalf("Create(wf1 #2) error = %v", err)
	}

	items, total, err := svc.List(ctx, repository.InstanceListQuery{
		Page: 1, PerPage: 50, Order: "-created_at",
		WorkflowDefinitionID: wfID,
		Statuses:             []string{"waiting"},
	})
	if err != nil {
		t.Fatalf("List() error = %v", err)
	}
	if total != 2 {
		t.Errorf("total = %d, want 2", total)
	}
	if len(items) != 2 || items[0].ID != inst3.ID || items[1].ID != inst1.ID {
		t.Errorf("items = [%s], want [%s %s] (newest first, wf filtered)",
			itemIDs(items), inst3.ID, inst1.ID)
	}
	for _, it := range items {
		if it.WorkflowDefinitionID != wfID || it.Status != model.WorkflowWaiting {
			t.Errorf("item = %+v, want wf %s waiting", it, wfID)
		}
	}

	// No filters: all three, oldest first.
	items, total, err = svc.List(ctx, repository.InstanceListQuery{Page: 1, PerPage: 50, Order: "created_at"})
	if err != nil {
		t.Fatalf("List(all) error = %v", err)
	}
	if total != 3 || len(items) != 3 || items[0].ID != inst1.ID || items[2].ID != inst3.ID {
		t.Errorf("total = %d items = [%s], want 3 in creation order", total, itemIDs(items))
	}
	if inst2.ID == "" {
		t.Errorf("inst2 = %+v", inst2)
	}

	// Pagination is honored: page 2 of 2 per page yields the last item.
	items, total, err = svc.List(ctx, repository.InstanceListQuery{Page: 2, PerPage: 2, Order: "created_at"})
	if err != nil {
		t.Fatalf("List(page 2) error = %v", err)
	}
	if total != 3 || len(items) != 1 || items[0].ID != inst3.ID {
		t.Errorf("page 2: total = %d items = [%s], want [%s]", total, itemIDs(items), inst3.ID)
	}
}

func itemIDs(items []model.WorkflowInstance) string {
	out := ""
	for i, w := range items {
		if i > 0 {
			out += " "
		}
		out += w.ID
	}
	return out
}

func TestUpdateContextReplacesPausedContext(t *testing.T) {
	db := setupSvcDB(t)
	ctx := context.Background()
	svc := svcInstanceService(db)

	wfID := svcCreateWorkflow(t, db, "11111111-1111-7111-8111-111111111101",
		svcNodeJSON("11111111-1111-7111-8111-111111111101", "script", "a", "return context.x;", "", map[string]any{"output_property": "out"}),
	)
	inst, err := svc.Create(ctx, service.CreateInstance{WorkflowDefinitionID: wfID, Context: json.RawMessage(`{"x":1}`)})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Pause(ctx, inst.ID); err != nil {
		t.Fatalf("Pause() error = %v", err)
	}

	got, err := svc.UpdateContext(ctx, service.UpdateContext{
		InstanceID: inst.ID,
		Context:    json.RawMessage(`{"y":{"nested":2}}`),
		Reason:     "urgent fix",
	})
	if err != nil {
		t.Fatalf("UpdateContext() error = %v", err)
	}
	var m map[string]any
	_ = json.Unmarshal(got.Context, &m)
	if _, ok := m["x"]; ok {
		t.Errorf("context = %s, old key survived replacement", got.Context)
	}
	if nested, ok := m["y"].(map[string]any); !ok || nested["nested"] != float64(2) {
		t.Errorf("context = %s, want nested replacement", got.Context)
	}
	if !svcEventTypes(t, db, inst.ID)["context_updated"] {
		t.Error("event 'context_updated' missing")
	}
}

func TestUpdateContextRejectsNonObject(t *testing.T) {
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
	if _, err := svc.Pause(ctx, inst.ID); err != nil {
		t.Fatal(err)
	}

	for _, bad := range []string{`null`, `[1]`, `"str"`, `42`, `true`, ``} {
		if _, err := svc.UpdateContext(ctx, service.UpdateContext{InstanceID: inst.ID, Context: json.RawMessage(bad)}); !errors.Is(err, model.ErrInvalid) {
			t.Errorf("UpdateContext(%q) error = %v, want ErrInvalid", bad, err)
		}
	}
}

func TestUpdateContextRejectsNotPaused(t *testing.T) {
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
	// waiting instance, not paused
	if _, err := svc.UpdateContext(ctx, service.UpdateContext{InstanceID: inst.ID, Context: json.RawMessage(`{}`)}); !errors.Is(err, model.ErrConflict) {
		t.Errorf("UpdateContext(waiting) error = %v, want ErrConflict", err)
	}
	// finished instance
	driveEngine(t, db, inst.ID)
	if _, err := svc.UpdateContext(ctx, service.UpdateContext{InstanceID: inst.ID, Context: json.RawMessage(`{}`)}); !errors.Is(err, model.ErrConflict) {
		t.Errorf("UpdateContext(finished) error = %v, want ErrConflict", err)
	}
}

func TestUpdateContextNotFound(t *testing.T) {
	db := setupSvcDB(t)
	ctx := context.Background()
	svc := svcInstanceService(db)

	if _, err := svc.UpdateContext(ctx, service.UpdateContext{InstanceID: svcNewID(), Context: json.RawMessage(`{}`)}); !errors.Is(err, model.ErrNotFound) {
		t.Errorf("UpdateContext(missing) error = %v, want ErrNotFound", err)
	}
}

func TestUpdateContextFeedsResumedExecution(t *testing.T) {
	db := setupSvcDB(t)
	ctx := context.Background()
	svc := svcInstanceService(db)

	wfID := svcCreateWorkflow(t, db, "11111111-1111-7111-8111-111111111101",
		svcNodeJSON("11111111-1111-7111-8111-111111111101", "script", "a", "return context.debug_flag ? 'yes' : 'no';", "", map[string]any{"output_property": "result"}),
	)
	inst, err := svc.Create(ctx, service.CreateInstance{WorkflowDefinitionID: wfID, Context: json.RawMessage(`{}`)})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Pause(ctx, inst.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.UpdateContext(ctx, service.UpdateContext{InstanceID: inst.ID, Context: json.RawMessage(`{"debug_flag":true}`)}); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Resume(ctx, inst.ID); err != nil {
		t.Fatal(err)
	}
	cur := driveEngine(t, db, inst.ID)
	if cur.Status != model.WorkflowFinished {
		t.Fatalf("status = %s, want finished", cur.Status)
	}
	var m map[string]any
	_ = json.Unmarshal(cur.Context, &m)
	if m["result"] != "yes" {
		t.Errorf("context = %s, want script to see updated context", cur.Context)
	}
}

func TestDeliverInputPreHookRunsOnceBeforeParking(t *testing.T) {
	db := setupSvcDB(t)
	ctx := context.Background()
	svc := svcInstanceService(db)

	wfID := svcCreateWorkflow(t, db, "11111111-1111-7111-8111-111111111101",
		svcNodeJSON("11111111-1111-7111-8111-111111111101", "input", "ask", "", "11111111-1111-7111-8111-111111111102", map[string]any{
			"channel": "http", "context_path": "webhook",
			"pre_script": map[string]any{"script": "context.pre_ran = (context.pre_ran || 0) + 1;"},
		}),
		svcNodeJSON("11111111-1111-7111-8111-111111111102", "script", "after", "return 'ok';", "", map[string]any{"output_property": "after"}),
	)
	inst, err := svc.Create(ctx, service.CreateInstance{WorkflowDefinitionID: wfID, Context: json.RawMessage(`{}`)})
	if err != nil {
		t.Fatal(err)
	}
	cur := driveEngine(t, db, inst.ID)
	if cur.Status != model.WorkflowWaiting || cur.WaitingReason != model.WaitingReasonInput {
		t.Fatalf("instance = %+v, want waiting on input", cur)
	}
	got, _ := svc.GetContext(ctx, inst.ID)
	var ctxMap map[string]any
	_ = json.Unmarshal(got.Context, &ctxMap)
	if ctxMap["pre_ran"] != float64(1) {
		t.Errorf("context = %v, want pre_ran=1 checkpointed before parking", ctxMap)
	}

	delivery, err := svc.DeliverInput(ctx, service.DeliverInput{
		InstanceID: inst.ID, IdempotencyKey: "pre-1", Payload: []byte(`{"ok":1}`),
	})
	if err != nil {
		t.Fatalf("DeliverInput() error = %v", err)
	}
	if !delivery.Accepted {
		t.Errorf("delivery = %+v, want accepted", delivery)
	}
	cur = driveEngine(t, db, inst.ID)
	if cur.Status != model.WorkflowFinished {
		t.Fatalf("status = %s, want finished (error %q)", cur.Status, cur.Error)
	}
	got, _ = svc.GetContext(ctx, inst.ID)
	_ = json.Unmarshal(got.Context, &ctxMap)
	if ctxMap["pre_ran"] != float64(1) {
		t.Errorf("pre_ran = %v, want 1 (pre ran once, not on delivery)", ctxMap["pre_ran"])
	}
}

func TestDeliverInputPostHookSeesPayload(t *testing.T) {
	db := setupSvcDB(t)
	ctx := context.Background()
	svc := svcInstanceService(db)

	wfID := svcCreateWorkflow(t, db, "11111111-1111-7111-8111-111111111101",
		svcNodeJSON("11111111-1111-7111-8111-111111111101", "input", "ask", "", "", map[string]any{
			"channel": "http", "context_path": "webhook",
			"post_script": map[string]any{"script": "context.post_sees = output.ok; context.post_ran = true;"},
		}),
	)
	inst, err := svc.Create(ctx, service.CreateInstance{WorkflowDefinitionID: wfID, Context: json.RawMessage(`{}`)})
	if err != nil {
		t.Fatal(err)
	}
	driveEngine(t, db, inst.ID)
	delivery, err := svc.DeliverInput(ctx, service.DeliverInput{
		InstanceID: inst.ID, IdempotencyKey: "post-1", Payload: []byte(`{"ok":"yes"}`),
	})
	if err != nil {
		t.Fatalf("DeliverInput() error = %v", err)
	}
	if !delivery.Accepted {
		t.Fatalf("delivery = %+v, want accepted", delivery)
	}
	cur := driveEngine(t, db, inst.ID)
	if cur.Status != model.WorkflowFinished {
		t.Fatalf("status = %s, want finished (error %q)", cur.Status, cur.Error)
	}
	got, _ := svc.GetContext(ctx, inst.ID)
	var ctxMap map[string]any
	_ = json.Unmarshal(got.Context, &ctxMap)
	if ctxMap["post_ran"] != true || ctxMap["post_sees"] != "yes" {
		t.Errorf("context = %v, want post_ran=true and post_sees=yes", ctxMap)
	}
}

func TestDeliverInputRejectedRerunsNoHooks(t *testing.T) {
	db := setupSvcDB(t)
	ctx := context.Background()
	svc := svcInstanceService(db)

	wfID := svcCreateWorkflow(t, db, "11111111-1111-7111-8111-111111111101",
		svcNodeJSON("11111111-1111-7111-8111-111111111101", "input", "ask", "", "", map[string]any{
			"channel": "http", "context_path": "webhook",
			"pre_script":  map[string]any{"script": "context.pre_ran = (context.pre_ran || 0) + 1;"},
			"post_script": map[string]any{"script": "context.post_ran = true;"},
			"validation": map[string]any{
				"script": "input = JSON.parse(input); if (!input.success) { return 'no'; };",
			},
		}),
	)
	inst, err := svc.Create(ctx, service.CreateInstance{WorkflowDefinitionID: wfID})
	if err != nil {
		t.Fatal(err)
	}
	driveEngine(t, db, inst.ID)

	rejected, err := svc.DeliverInput(ctx, service.DeliverInput{
		InstanceID: inst.ID, IdempotencyKey: "bad-1", Payload: []byte(`{"success":false}`),
	})
	if err != nil {
		t.Fatalf("DeliverInput(bad) error = %v", err)
	}
	if rejected.Accepted {
		t.Errorf("delivery = %+v, want rejected", rejected)
	}
	got, _ := svc.GetContext(ctx, inst.ID)
	var ctxMap map[string]any
	_ = json.Unmarshal(got.Context, &ctxMap)
	if ctxMap["pre_ran"] != float64(1) {
		t.Errorf("pre_ran = %v, want 1 (rejection must not rerun pre)", ctxMap["pre_ran"])
	}
	if _, ok := ctxMap["post_ran"]; ok {
		t.Error("post_ran present, want absent for rejected delivery")
	}

	accepted, err := svc.DeliverInput(ctx, service.DeliverInput{
		InstanceID: inst.ID, IdempotencyKey: "good-1", Payload: []byte(`{"success":true}`),
	})
	if err != nil {
		t.Fatalf("DeliverInput(good) error = %v", err)
	}
	if !accepted.Accepted {
		t.Fatalf("delivery = %+v, want accepted", accepted)
	}
	cur := driveEngine(t, db, inst.ID)
	if cur.Status != model.WorkflowFinished {
		t.Fatalf("status = %s, want finished", cur.Status)
	}
	got, _ = svc.GetContext(ctx, inst.ID)
	_ = json.Unmarshal(got.Context, &ctxMap)
	if ctxMap["pre_ran"] != float64(1) {
		t.Errorf("pre_ran = %v, want 1 across the whole lifecycle", ctxMap["pre_ran"])
	}
	if ctxMap["post_ran"] != true {
		t.Errorf("post_ran = %v, want true for accepted delivery", ctxMap["post_ran"])
	}
}

func TestDeliverInputPostFailureFailsWorkflow(t *testing.T) {
	db := setupSvcDB(t)
	ctx := context.Background()
	svc := svcInstanceService(db)

	wfID := svcCreateWorkflow(t, db, "11111111-1111-7111-8111-111111111101",
		svcNodeJSON("11111111-1111-7111-8111-111111111101", "input", "ask", "", "", map[string]any{
			"channel": "http", "context_path": "webhook",
			"post_script": map[string]any{"script": "throw new Error('ipost boom');"},
		}),
	)
	inst, err := svc.Create(ctx, service.CreateInstance{WorkflowDefinitionID: wfID})
	if err != nil {
		t.Fatal(err)
	}
	driveEngine(t, db, inst.ID)

	delivery, err := svc.DeliverInput(ctx, service.DeliverInput{
		InstanceID: inst.ID, IdempotencyKey: "fail-1", Payload: []byte(`{"success":true}`),
	})
	if err != nil {
		t.Fatalf("DeliverInput() error = %v", err)
	}
	if !delivery.Accepted {
		t.Fatalf("delivery = %+v, want accepted (202) even though the workflow fails", delivery)
	}
	if !strings.Contains(delivery.Error, "ipost boom") {
		t.Errorf("delivery error = %q, want post-script cause", delivery.Error)
	}
	cur, _ := svc.GetStatus(ctx, inst.ID)
	if cur.Status != model.WorkflowFailed {
		t.Fatalf("status = %s, want failed after accepted input post failure", cur.Status)
	}
	if !strings.Contains(cur.Error, "ipost boom") {
		t.Errorf("instance error = %q, want post-script cause", cur.Error)
	}
	attempt, err := repository.NewInstanceRepository(db).GetNodeInstanceByNode(ctx, inst.ID, "11111111-1111-7111-8111-111111111101")
	if err != nil {
		t.Fatal(err)
	}
	if attempt.Status != model.NodeFailed {
		t.Errorf("attempt status = %s, want failed", attempt.Status)
	}
	got, _ := svc.GetContext(ctx, inst.ID)
	var ctxMap map[string]any
	_ = json.Unmarshal(got.Context, &ctxMap)
	if _, ok := ctxMap["webhook"]; !ok {
		t.Errorf("context = %v, want merged payload preserved", ctxMap)
	}
}

func TestDeliverInputGroupPostFailureFinishesInputAttempt(t *testing.T) {
	db := setupSvcDB(t)
	ctx := context.Background()
	svc := svcInstanceService(db)

	inner := "11111111-1111-7111-8111-111111111101"
	groupID := "11111111-1111-7111-8111-111111111102"
	wfID := svcCreateWorkflow(t, db, groupID,
		svcNodeJSON(groupID, "group", "wrap", "", "", map[string]any{
			"start_node_id": inner,
			"post_script":   map[string]any{"script": "throw new Error('gpost boom');"},
			"nodes": []map[string]any{
				{"id": inner, "type": "input", "name": "ask", "channel": "http", "context_path": "webhook"},
			},
		}),
	)
	inst, err := svc.Create(ctx, service.CreateInstance{WorkflowDefinitionID: wfID})
	if err != nil {
		t.Fatal(err)
	}
	driveEngine(t, db, inst.ID)

	delivery, err := svc.DeliverInput(ctx, service.DeliverInput{
		InstanceID: inst.ID, IdempotencyKey: "gpost-1", Payload: []byte(`{"success":true}`),
	})
	if err != nil {
		t.Fatalf("DeliverInput() error = %v", err)
	}
	if !delivery.Accepted {
		t.Fatalf("delivery = %+v, want accepted", delivery)
	}
	cur, _ := svc.GetStatus(ctx, inst.ID)
	if cur.Status != model.WorkflowFailed {
		t.Fatalf("status = %s, want failed after group post failure", cur.Status)
	}
	if !strings.Contains(cur.Error, "gpost boom") {
		t.Errorf("instance error = %q, want group post cause", cur.Error)
	}
	attempt, err := repository.NewInstanceRepository(db).GetNodeInstanceByNode(ctx, inst.ID, inner)
	if err != nil {
		t.Fatal(err)
	}
	if attempt.Status != model.NodeFinished {
		t.Errorf("input attempt status = %s, want finished", attempt.Status)
	}
	got, _ := svc.GetContext(ctx, inst.ID)
	var ctxMap map[string]any
	_ = json.Unmarshal(got.Context, &ctxMap)
	if _, ok := ctxMap["webhook"]; !ok {
		t.Errorf("context = %v, want merged payload preserved", ctxMap)
	}
}

func TestExternalCallFailureRoutesToInputAndResumes(t *testing.T) {
	db := setupSvcDB(t)
	ctx := context.Background()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
		_, _ = w.Write([]byte(`{"error":"gateway error"}`))
	}))
	defer srv.Close()

	nExt := "11111111-1111-7111-8111-111111111901"
	nInput := "11111111-1111-7111-8111-111111111902"
	nDone := "11111111-1111-7111-8111-111111111903"

	wfID := svcCreateWorkflow(t, db, nExt,
		svcNodeJSON(nExt, "external_call", "call-api", "", nDone, map[string]any{
			"http_config": map[string]any{"url": srv.URL, "method": "GET"},
			"on_failure": map[string]any{
				"next_node":       nInput,
				"output_property": "api_failure",
			},
		}),
		svcNodeJSON(nInput, "input", "fix-input", "", nDone, map[string]any{
			"channel":      "http",
			"context_path": "fallback_data",
		}),
		svcNodeJSON(nDone, "script", "finish", "return context.fallback_data.val;", "", map[string]any{
			"output_property": "final_result",
		}),
	)

	svc := svcInstanceService(db)
	inst, err := svc.Create(ctx, service.CreateInstance{
		WorkflowDefinitionID: wfID,
		Context:              json.RawMessage(`{"init":1}`),
	})
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	// 1. External call fails and routes to input
	cur := driveEngineWithExecLimits(t, db, inst.ID, executor.Limits{HTTPAllowlist: []string{"127.0.0.1"}})
	if cur.Status != model.WorkflowWaiting || cur.WaitingReason != model.WaitingReasonInput {
		t.Fatalf("status = %s / %s, want waiting/input", cur.Status, cur.WaitingReason)
	}

	// 2. Failure payload is visible in workflow context
	gotCtx, err := svc.GetContext(ctx, inst.ID)
	if err != nil {
		t.Fatal(err)
	}
	var ctxMap map[string]any
	if err := json.Unmarshal(gotCtx.Context, &ctxMap); err != nil {
		t.Fatal(err)
	}
	apiFail, ok := ctxMap["api_failure"].(map[string]any)
	if !ok {
		t.Fatalf("api_failure missing in context: %v", ctxMap)
	}
	if apiFail["reason"] != "http-status" {
		t.Errorf("api_failure.reason = %v, want http-status", apiFail["reason"])
	}

	// 3. Deliver input payload to input node
	delivery, err := svc.DeliverInput(ctx, service.DeliverInput{
		InstanceID:     inst.ID,
		IdempotencyKey: "fix-key-1",
		Payload:        []byte(`{"val":"recovered_value"}`),
	})
	if err != nil {
		t.Fatalf("DeliverInput() error = %v", err)
	}
	if !delivery.Accepted {
		t.Fatalf("delivery = %+v, want accepted", delivery)
	}

	// 4. Workflow resumes and finishes
	cur = driveEngineWithExecLimits(t, db, inst.ID, executor.Limits{HTTPAllowlist: []string{"127.0.0.1"}})
	if cur.Status != model.WorkflowFinished {
		t.Fatalf("status = %s, want finished (error: %q)", cur.Status, cur.Error)
	}

	// 5. Downstream node reads replacement data
	gotFinalCtx, err := svc.GetContext(ctx, inst.ID)
	if err != nil {
		t.Fatal(err)
	}
	var finalCtxMap map[string]any
	if err := json.Unmarshal(gotFinalCtx.Context, &finalCtxMap); err != nil {
		t.Fatal(err)
	}
	if finalCtxMap["final_result"] != "recovered_value" {
		t.Errorf("final_result = %v, want 'recovered_value'", finalCtxMap["final_result"])
	}
}
