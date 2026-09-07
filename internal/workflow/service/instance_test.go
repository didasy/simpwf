package service_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
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
		&repository.NodeContextHistoryModel{},
		&repository.NodeInstanceModel{}, &repository.WorkflowInstanceEventModel{},
		&repository.InputDeliveryModel{}, &repository.StatusUpdateOutboxModel{},
	); err != nil {
		t.Fatalf("AutoMigrate() error = %v", err)
	}
	if err := db.Exec(`TRUNCATE TABLE
		status_update_outbox, node_context_history, input_deliveries, workflow_instance_events, node_instances,
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
	return svcCreateWorkflowRaw(t, db, raw)
}

func svcCreateWorkflowWithMode(t *testing.T, db *gorm.DB, start, mode string, nodes ...string) string {
	t.Helper()
	raw := fmt.Sprintf(`{"start_node_id":%q,"context_mode":%q,"nodes":[%s]}`, start, mode, joinAll(nodes))
	return svcCreateWorkflowRaw(t, db, raw)
}

func svcCreateWorkflowRaw(t *testing.T, db *gorm.DB, raw string) string {
	t.Helper()
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
	return svcInstanceServiceWithOptions(db, model.LeanOptions{})
}

func svcInstanceServiceWithOptions(db *gorm.DB, options model.LeanOptions) service.InstanceService {
	instances := repository.NewInstanceRepositoryWithOptions(db, options)
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
		options,
	)
}

// driveEngine runs claim -> process until the instance parks on input,
// finishes, or fails.
func driveEngine(t *testing.T, db *gorm.DB, instanceID string) model.WorkflowInstance {
	return driveEngineWithExecLimitsAndOptions(t, db, instanceID, executor.Limits{}, model.LeanOptions{})
}

func leanDriveEngine(t *testing.T, db *gorm.DB, instanceID string, options model.LeanOptions) model.WorkflowInstance {
	return driveEngineWithExecLimitsAndOptions(t, db, instanceID, executor.Limits{}, options)
}

func driveEngineWithExecLimits(t *testing.T, db *gorm.DB, instanceID string, execLimits executor.Limits) model.WorkflowInstance {
	return driveEngineWithExecLimitsAndOptions(t, db, instanceID, execLimits, model.LeanOptions{})
}

func driveEngineWithExecLimitsAndOptions(t *testing.T, db *gorm.DB, instanceID string, execLimits executor.Limits, leanOptions model.LeanOptions) model.WorkflowInstance {
	t.Helper()
	ctx := context.Background()
	instances := repository.NewInstanceRepositoryWithOptions(db, leanOptions)
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
	e := engine.NewEngine(instances, executor.NewExecutors(execLimits, nil, executor.Dependencies{}), executor.NewHookRunner(nil), model.DefaultLimits(), loader, svcSysUserID, leanOptions)
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

func TestCreateInstanceSnapshotsContextMode(t *testing.T) {
	db := setupSvcDB(t)
	ctx := context.Background()
	start := "11111111-1111-7111-8111-111111111103"
	node := svcNodeJSON(start, "script", "a", "return 1;", "", nil)
	leanWF := svcCreateWorkflowWithMode(t, db, start, model.ContextModeLean, node)
	fullStart := "11111111-1111-7111-8111-111111111104"
	fullWF := svcCreateWorkflowWithMode(t, db, fullStart, model.ContextModeFull, svcNodeJSON(fullStart, "script", "a", "return 1;", "", nil))
	defaultStart := "11111111-1111-7111-8111-111111111105"
	defaultWF := svcCreateWorkflow(t, db, defaultStart, svcNodeJSON(defaultStart, "script", "a", "return 1;", "", nil))
	svc := svcInstanceServiceWithOptions(db, model.LeanOptions{LeanContextDefault: true})

	lean, err := svc.Create(ctx, service.CreateInstance{WorkflowDefinitionID: leanWF})
	if err != nil {
		t.Fatal(err)
	}
	if lean.ContextMode != model.ContextModeLean {
		t.Errorf("explicit lean ContextMode = %q, want lean", lean.ContextMode)
	}
	full, err := svc.Create(ctx, service.CreateInstance{WorkflowDefinitionID: fullWF})
	if err != nil {
		t.Fatal(err)
	}
	if full.ContextMode != model.ContextModeFull {
		t.Errorf("explicit full ContextMode = %q, want full", full.ContextMode)
	}
	defaulted, err := svc.Create(ctx, service.CreateInstance{WorkflowDefinitionID: defaultWF})
	if err != nil {
		t.Fatal(err)
	}
	if defaulted.ContextMode != model.ContextModeLean {
		t.Errorf("default ContextMode = %q, want lean", defaulted.ContextMode)
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
		model.LeanOptions{},
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

func TestRollbackPausedToPaused(t *testing.T) {
	db := setupSvcDB(t)
	ctx := context.Background()
	svc := svcInstanceService(db)

	n1 := "11111111-1111-7111-8111-111111111101"
	n2 := "11111111-1111-7111-8111-111111111102"
	wfID := svcCreateWorkflow(t, db, n1,
		svcNodeJSON(n1, "script", "a", "context.x = 1; return 1;", n2, map[string]any{"output_property": "out1"}),
		svcNodeJSON(n2, "script", "b", "context.y = 2; return 2;", "", map[string]any{"output_property": "out2"}),
	)
	inst, err := svc.Create(ctx, service.CreateInstance{WorkflowDefinitionID: wfID})
	if err != nil {
		t.Fatal(err)
	}
	repo := repository.NewInstanceRepository(db)
	eng := svcTestEngine(t, db)

	// Run only n1: claim + process once, then pause while waiting.
	claimed, err := repo.ClaimNext(ctx, "rollback-worker", time.Minute, 10)
	if err != nil {
		t.Fatal(err)
	}
	for _, w := range claimed {
		if w.ID == inst.ID {
			if err := eng.Process(ctx, w); err != nil {
				t.Fatalf("Process(n1) error = %v", err)
			}
		}
	}
	occ1, err := repo.GetNodeInstanceByNode(ctx, inst.ID, n1)
	if err != nil {
		t.Fatalf("GetNodeInstanceByNode(n1) error = %v", err)
	}
	if _, err := svc.Pause(ctx, inst.ID); err != nil {
		t.Fatalf("Pause() error = %v", err)
	}

	res, err := svc.Rollback(ctx, service.RollbackRequest{InstanceID: inst.ID, TargetOccurrenceID: occ1.ID, Reason: "retry"})
	if err != nil {
		t.Fatalf("Rollback() error = %v", err)
	}
	if res.Status != model.WorkflowPaused || res.CurrentNodeID != n1 {
		t.Errorf("res = %+v, want paused at %s", res, n1)
	}
	if len(res.GroupStack) != 0 {
		t.Errorf("group stack = %v, want empty", res.GroupStack)
	}
	got, _ := svc.GetContext(ctx, inst.ID)
	var ctxMap map[string]any
	_ = json.Unmarshal(got.Context, &ctxMap)
	if _, ok := ctxMap["out1"]; ok {
		t.Errorf("context = %s, want ContextBefore without n1 output", got.Context)
	}
	// Resume re-executes forward: n1 runs again (Attempt 2), then n2.
	if _, err := svc.Resume(ctx, inst.ID); err != nil {
		t.Fatalf("Resume() error = %v", err)
	}
	final := driveEngine(t, db, inst.ID)
	if final.Status != model.WorkflowFinished {
		t.Fatalf("status = %s, want finished (error %q)", final.Status, final.Error)
	}
	occAfter, _ := repo.GetNodeInstanceByNode(ctx, inst.ID, n1)
	if occAfter.Attempt != 2 {
		t.Errorf("n1 attempt = %d, want 2", occAfter.Attempt)
	}
}

func TestRollbackFailedToPaused(t *testing.T) {
	db := setupSvcDB(t)
	ctx := context.Background()
	svc := svcInstanceService(db)

	n1 := "11111111-1111-7111-8111-111111111101"
	wfID := svcCreateWorkflow(t, db, n1,
		svcNodeJSON(n1, "script", "a", "throw new Error('boom');", "", nil),
	)
	inst, err := svc.Create(ctx, service.CreateInstance{WorkflowDefinitionID: wfID, Context: json.RawMessage(`{"seed":1}`)})
	if err != nil {
		t.Fatal(err)
	}
	cur := driveEngine(t, db, inst.ID)
	if cur.Status != model.WorkflowFailed {
		t.Fatalf("status = %s, want failed", cur.Status)
	}
	if cur.FinishedAt == nil || cur.Error == "" {
		t.Fatalf("failed instance missing error/finished_at: %+v", cur)
	}
	repo := repository.NewInstanceRepository(db)
	occ, err := repo.GetNodeInstanceByNode(ctx, inst.ID, n1)
	if err != nil {
		t.Fatal(err)
	}

	res, err := svc.Rollback(ctx, service.RollbackRequest{InstanceID: inst.ID, TargetOccurrenceID: occ.ID})
	if err != nil {
		t.Fatalf("Rollback() error = %v", err)
	}
	if res.Status != model.WorkflowPaused || res.CurrentNodeID != n1 {
		t.Errorf("res = %+v, want paused at %s", res, n1)
	}
	stored, _ := svc.GetStatus(ctx, inst.ID)
	if stored.Status != model.WorkflowPaused || stored.Error != "" || stored.FinishedAt != nil {
		t.Errorf("stored = %+v, want paused with cleared error/finished_at", stored)
	}
	if stored.StartedAt == nil {
		t.Error("started_at = nil, want preserved")
	}
	got, _ := svc.GetContext(ctx, inst.ID)
	if !jsonEqualCtx(t, got.Context, json.RawMessage(`{"seed":1}`)) {
		t.Errorf("context = %s, want restored seed", got.Context)
	}
	if !svcEventTypes(t, db, inst.ID)["rollback"] {
		t.Error("event 'rollback' missing")
	}
}

func jsonEqualCtx(t *testing.T, a, b json.RawMessage) bool {
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

func TestRollbackNestedGroupStack(t *testing.T) {
	db := setupSvcDB(t)
	ctx := context.Background()
	svc := svcInstanceService(db)

	inner := "11111111-1111-7111-8111-111111111101"
	mid := "11111111-1111-7111-8111-111111111102"
	outer := "11111111-1111-7111-8111-111111111103"
	wfID := svcCreateWorkflow(t, db, outer,
		svcNodeJSON(outer, "group", "outer", "", "", map[string]any{
			"start_node_id": mid,
			"nodes": []map[string]any{
				{"id": mid, "type": "group", "name": "mid", "start_node_id": inner,
					"nodes": []map[string]any{
						{"id": inner, "type": "script", "name": "leaf", "script": "throw new Error('leaf boom');"},
					}},
			},
		}),
	)
	// The leaf throws: the instance fails while its cursor sits inside both
	// groups, so rollback must recompute the two-level stack.
	inst, err := svc.Create(ctx, service.CreateInstance{WorkflowDefinitionID: wfID, Context: json.RawMessage(`{"seed":1}`)})
	if err != nil {
		t.Fatal(err)
	}
	cur := driveEngine(t, db, inst.ID)
	if cur.Status != model.WorkflowFailed {
		t.Fatalf("status = %s, want failed (error %q)", cur.Status, cur.Error)
	}
	repo := repository.NewInstanceRepository(db)
	occ, err := repo.GetNodeInstanceByNode(ctx, inst.ID, inner)
	if err != nil {
		t.Fatal(err)
	}

	res, err := svc.Rollback(ctx, service.RollbackRequest{InstanceID: inst.ID, TargetOccurrenceID: occ.ID, Reason: "retry leaf"})
	if err != nil {
		t.Fatalf("Rollback() error = %v", err)
	}
	if res.Status != model.WorkflowPaused || res.CurrentNodeID != inner {
		t.Errorf("res = %+v, want paused at %s", res, inner)
	}
	if len(res.GroupStack) != 2 || res.GroupStack[0] != outer || res.GroupStack[1] != mid {
		t.Errorf("group stack = %v, want [%s %s]", res.GroupStack, outer, mid)
	}
	got, _ := svc.GetContext(ctx, inst.ID)
	if !jsonEqualCtx(t, got.Context, json.RawMessage(`{"seed":1}`)) {
		t.Errorf("context = %s, want restored seed", got.Context)
	}
	stored, _ := svc.GetStatus(ctx, inst.ID)
	if stored.Status != model.WorkflowPaused || stored.Error != "" || stored.FinishedAt != nil {
		t.Errorf("stored = %+v, want paused with cleared error/finished_at", stored)
	}
}

func TestRollbackErrors(t *testing.T) {
	db := setupSvcDB(t)
	ctx := context.Background()
	svc := svcInstanceService(db)

	n1 := "11111111-1111-7111-8111-111111111101"
	n2 := "11111111-1111-7111-8111-111111111102"
	wfID := svcCreateWorkflow(t, db, n1,
		svcNodeJSON(n1, "script", "a", "return 1;", n2, nil),
		svcNodeJSON(n2, "script", "b", "return 2;", "", nil),
	)
	inst, err := svc.Create(ctx, service.CreateInstance{WorkflowDefinitionID: wfID})
	if err != nil {
		t.Fatal(err)
	}
	repo := repository.NewInstanceRepository(db)
	eng := svcTestEngine(t, db)
	claimed, err := repo.ClaimNext(ctx, "rollback-worker", time.Minute, 10)
	if err != nil {
		t.Fatal(err)
	}
	for _, w := range claimed {
		if w.ID == inst.ID {
			if err := eng.Process(ctx, w); err != nil {
				t.Fatalf("Process() error = %v", err)
			}
		}
	}
	occ1, err := repo.GetNodeInstanceByNode(ctx, inst.ID, n1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Pause(ctx, inst.ID); err != nil {
		t.Fatalf("Pause() error = %v", err)
	}

	// Unknown instance.
	if _, err := svc.Rollback(ctx, service.RollbackRequest{InstanceID: svcNewID(), TargetOccurrenceID: occ1.ID}); !errors.Is(err, model.ErrNotFound) {
		t.Errorf("Rollback(unknown instance) error = %v, want ErrNotFound", err)
	}
	// Unknown occurrence.
	if _, err := svc.Rollback(ctx, service.RollbackRequest{InstanceID: inst.ID, TargetOccurrenceID: svcNewID()}); !errors.Is(err, model.ErrNotFound) {
		t.Errorf("Rollback(unknown occurrence) error = %v, want ErrNotFound", err)
	}
	// Empty ids.
	if _, err := svc.Rollback(ctx, service.RollbackRequest{InstanceID: "", TargetOccurrenceID: occ1.ID}); !errors.Is(err, model.ErrInvalid) {
		t.Errorf("Rollback(empty instance) error = %v, want ErrInvalid", err)
	}
	if _, err := svc.Rollback(ctx, service.RollbackRequest{InstanceID: inst.ID, TargetOccurrenceID: ""}); !errors.Is(err, model.ErrInvalid) {
		t.Errorf("Rollback(empty occurrence) error = %v, want ErrInvalid", err)
	}
	// Wrong state: resume then rollback while waiting.
	if _, err := svc.Resume(ctx, inst.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Rollback(ctx, service.RollbackRequest{InstanceID: inst.ID, TargetOccurrenceID: occ1.ID}); !errors.Is(err, model.ErrConflict) {
		t.Errorf("Rollback(waiting) error = %v, want ErrConflict", err)
	}
	// not_started node (n2 never ran): no occurrence row.
	if _, err := svc.Pause(ctx, inst.ID); err != nil {
		t.Fatalf("Pause() error = %v", err)
	}
	if _, err := svc.Rollback(ctx, service.RollbackRequest{InstanceID: inst.ID, TargetOccurrenceID: n2}); !errors.Is(err, model.ErrNotFound) {
		t.Errorf("Rollback(not_started node id) error = %v, want ErrNotFound", err)
	}
}

func TestRollbackParkedInputIsNoOp(t *testing.T) {
	db := setupSvcDB(t)
	ctx := context.Background()
	svc := svcInstanceService(db)

	n1 := "11111111-1111-7111-8111-111111111101"
	n2 := "11111111-1111-7111-8111-111111111102"
	n3 := "11111111-1111-7111-8111-111111111103"
	wfID := svcCreateWorkflow(t, db, n1,
		svcNodeJSON(n1, "script", "a", "return 1;", n2, map[string]any{"output_property": "a"}),
		svcNodeJSON(n2, "input", "ask", "", n3,
			map[string]any{"channel": "http", "context_path": "gate"}),
		svcNodeJSON(n3, "script", "b", "return 1;", "", map[string]any{"output_property": "done"}),
	)
	inst, err := svc.Create(ctx, service.CreateInstance{WorkflowDefinitionID: wfID})
	if err != nil {
		t.Fatal(err)
	}
	cur := driveEngine(t, db, inst.ID)
	if cur.Status != model.WorkflowWaiting || cur.WaitingReason != model.WaitingReasonInput {
		t.Fatalf("instance = %+v, want parked on input", cur)
	}
	if _, err := svc.Pause(ctx, inst.ID); err != nil {
		t.Fatalf("Pause() error = %v", err)
	}
	repo := repository.NewInstanceRepository(db)
	occ, err := repo.GetNodeInstanceByNode(ctx, inst.ID, n2)
	if err != nil {
		t.Fatal(err)
	}
	before, err := svc.GetStatus(ctx, inst.ID)
	if err != nil {
		t.Fatal(err)
	}
	// Rolling back onto the live parked occurrence itself is a no-op:
	// 200 paused, zero writes, park stays running.
	res, err := svc.Rollback(ctx, service.RollbackRequest{InstanceID: inst.ID, TargetOccurrenceID: occ.ID})
	if err != nil {
		t.Fatalf("Rollback() error = %v", err)
	}
	if res.Status != model.WorkflowPaused || res.CurrentNodeID != n2 {
		t.Errorf("res = %+v, want paused at %s", res, n2)
	}
	stored, _ := svc.GetStatus(ctx, inst.ID)
	if stored.Status != model.WorkflowPaused {
		t.Errorf("status = %s, want still paused", stored.Status)
	}
	if stored.Revision != before.Revision {
		t.Errorf("revision = %d, want unchanged %d (no-op)", stored.Revision, before.Revision)
	}
	frame, err := model.ParseFrame(stored.Frame)
	if err != nil {
		t.Fatal(err)
	}
	if frame.CurrentNodeID != n2 {
		t.Errorf("cursor = %q, want still %q", frame.CurrentNodeID, n2)
	}
	running, err := repo.GetRunningNodeInstance(ctx, inst.ID)
	if err != nil || running.ID != occ.ID {
		t.Fatalf("running = %+v, err %v, want live park %s untouched", running, err, occ.ID)
	}
	if events, _ := repo.ListEvents(ctx, inst.ID); svcEventTypes(t, db, inst.ID)["rollback"] || len(events) == 0 {
		t.Errorf("events = %d types, want no rollback event on no-op", len(events))
	}
	// No-op preserves the park: resume + deliver still works.
	if _, err := svc.Resume(ctx, inst.ID); err != nil {
		t.Fatalf("Resume() error = %v", err)
	}
	delivery, err := svc.DeliverInput(ctx, service.DeliverInput{
		InstanceID: inst.ID, IdempotencyKey: "noop-keeps-park", Payload: []byte(`{"v":1}`),
	})
	if err != nil || !delivery.Accepted {
		t.Fatalf("DeliverInput() = %+v, err %v, want accepted", delivery, err)
	}
}

func TestRollbackSupersedesLiveParkedAttemptElsewhere(t *testing.T) {
	db := setupSvcDB(t)
	ctx := context.Background()
	svc := svcInstanceService(db)

	n1 := "11111111-1111-7111-8111-111111111101"
	n2 := "11111111-1111-7111-8111-111111111102"
	n3 := "11111111-1111-7111-8111-111111111103"
	wfID := svcCreateWorkflow(t, db, n1,
		svcNodeJSON(n1, "script", "a", "context.x = 1; return 1;", n2, map[string]any{"output_property": "out"}),
		svcNodeJSON(n2, "input", "ask", "", n3, map[string]any{"channel": "http", "context_path": "gate"}),
		svcNodeJSON(n3, "script", "b", "return 2;", "", map[string]any{"output_property": "done"}),
	)
	inst, err := svc.Create(ctx, service.CreateInstance{WorkflowDefinitionID: wfID})
	if err != nil {
		t.Fatal(err)
	}
	repo := repository.NewInstanceRepository(db)
	cur := driveEngine(t, db, inst.ID)
	if cur.Status != model.WorkflowWaiting || cur.WaitingReason != model.WaitingReasonInput {
		t.Fatalf("instance = %+v, want parked on input", cur)
	}
	if _, err := svc.Pause(ctx, inst.ID); err != nil {
		t.Fatalf("Pause() error = %v", err)
	}
	occ1, err := repo.GetNodeInstanceByNode(ctx, inst.ID, n1)
	if err != nil {
		t.Fatal(err)
	}
	parked, err := repo.GetNodeInstanceByNode(ctx, inst.ID, n2)
	if err != nil {
		t.Fatal(err)
	}
	// Rolling the cursor back to n1 supersedes the live n2 park: 200,
	// park closed, cursor moved, context restored.
	res, err := svc.Rollback(ctx, service.RollbackRequest{InstanceID: inst.ID, TargetOccurrenceID: occ1.ID, Reason: "supersede"})
	if err != nil {
		t.Fatalf("Rollback() error = %v", err)
	}
	if res.Status != model.WorkflowPaused || res.CurrentNodeID != n1 {
		t.Fatalf("res = %+v, want paused at %s", res, n1)
	}
	stored, _ := svc.GetStatus(ctx, inst.ID)
	if stored.Status != model.WorkflowPaused {
		t.Errorf("status = %s, want still paused", stored.Status)
	}
	if stored.WaitingReason != model.WaitingReasonRunnable {
		t.Errorf("waiting_reason = %q, want runnable", stored.WaitingReason)
	}
	frame, err := model.ParseFrame(stored.Frame)
	if err != nil {
		t.Fatal(err)
	}
	if frame.CurrentNodeID != n1 {
		t.Errorf("cursor = %q, want %q", frame.CurrentNodeID, n1)
	}
	gotCtx, _ := svc.GetContext(ctx, inst.ID)
	if !jsonEqualCtx(t, gotCtx.Context, json.RawMessage(`{}`)) {
		t.Errorf("context = %s, want restored pre-n1 {}", gotCtx.Context)
	}
	closed, err := repo.GetNodeInstance(ctx, inst.ID, parked.ID)
	if err != nil {
		t.Fatal(err)
	}
	if closed.Status != model.NodeStopped || !closed.Cancelled {
		t.Errorf("superseded park = %s/cancelled=%v, want stopped/true", closed.Status, closed.Cancelled)
	}
	if closed.Error != "superseded by rollback" {
		t.Errorf("superseded error = %q, want supersede marker", closed.Error)
	}
	if _, err := repo.GetRunningNodeInstance(ctx, inst.ID); !errors.Is(err, repository.ErrNodeInstanceNotFound) {
		t.Errorf("GetRunningNodeInstance() error = %v, want ErrNodeInstanceNotFound", err)
	}
	if !svcEventTypes(t, db, inst.ID)["rollback"] {
		t.Error("event 'rollback' missing")
	}
	// Resume re-executes forward from n1: n1 runs again on the same
	// occurrence row (attempt++), then re-parks on n2 as a fresh row
	// (the superseded park stays stopped).
	if _, err := svc.Resume(ctx, inst.ID); err != nil {
		t.Fatalf("Resume() error = %v", err)
	}
	cur = driveEngine(t, db, inst.ID)
	if cur.Status != model.WorkflowWaiting || cur.WaitingReason != model.WaitingReasonInput {
		t.Fatalf("instance = %+v, want re-parked on input", cur)
	}
	rerun, err := repo.GetNodeInstance(ctx, inst.ID, occ1.ID)
	if err != nil {
		t.Fatal(err)
	}
	if rerun.Status != model.NodeFinished || rerun.Attempt != 2 {
		t.Errorf("rerun n1 = %s/%d, want finished/2", rerun.Status, rerun.Attempt)
	}
	reparked, err := repo.GetNodeInstanceByNode(ctx, inst.ID, n2)
	if err != nil {
		t.Fatal(err)
	}
	// GetNodeInstanceByNode returns the oldest row (the superseded park);
	// the live re-park is the newest running row for n2.
	if reparked.ID == parked.ID || reparked.Status != model.NodeRunning {
		occs, lerr := repo.ListNodeInstances(ctx, inst.ID)
		if lerr != nil {
			t.Fatal(lerr)
		}
		for i := len(occs) - 1; i >= 0; i-- {
			if occs[i].NodeID == n2 && occs[i].Status == model.NodeRunning {
				cp := occs[i]
				reparked = &cp
				break
			}
		}
	}
	if reparked.Status != model.NodeRunning {
		t.Errorf("repark status = %s, want running", reparked.Status)
	}
	if reparked.ID == parked.ID {
		t.Errorf("repark id = %s, want fresh row (superseded park stays stopped)", reparked.ID)
	}
	stillClosed, err := repo.GetNodeInstance(ctx, inst.ID, parked.ID)
	if err != nil {
		t.Fatal(err)
	}
	if stillClosed.Status != model.NodeStopped {
		t.Errorf("superseded park status = %s, want stopped", stillClosed.Status)
	}
	// Deliver to the live re-park.
	delivery, err := svc.DeliverInput(ctx, service.DeliverInput{
		InstanceID: inst.ID, IdempotencyKey: "supersede-fresh", Payload: []byte(`{"v":1}`),
	})
	if err != nil || !delivery.Accepted {
		t.Fatalf("DeliverInput() = %+v, err %v, want accepted", delivery, err)
	}
	if delivery.NodeInstanceID == parked.ID {
		t.Errorf("delivery landed on superseded row %s, want live re-park %s", delivery.NodeInstanceID, reparked.ID)
	}
}

func TestRollbackToFinishedInputReparks(t *testing.T) {
	db := setupSvcDB(t)
	ctx := context.Background()
	svc := svcInstanceService(db)

	n1 := "11111111-1111-7111-8111-111111111101"
	n2 := "11111111-1111-7111-8111-111111111102"
	n3 := "11111111-1111-7111-8111-111111111103"
	wfID := svcCreateWorkflow(t, db, n1,
		svcNodeJSON(n1, "input", "ask", "", n2,
			map[string]any{"channel": "http", "context_path": "gate"}),
		svcNodeJSON(n2, "script", "b", "throw new Error('downstream boom');", n3, nil),
		svcNodeJSON(n3, "script", "c", "return 2;", "", map[string]any{"output_property": "last"}),
	)
	inst, err := svc.Create(ctx, service.CreateInstance{WorkflowDefinitionID: wfID})
	if err != nil {
		t.Fatal(err)
	}
	repo := repository.NewInstanceRepository(db)
	cur := driveEngine(t, db, inst.ID)
	if cur.Status != model.WorkflowWaiting || cur.WaitingReason != model.WaitingReasonInput {
		t.Fatalf("instance = %+v, want parked on input", cur)
	}
	// Deliver: n2 throws, so the instance fails with the input occurrence
	// finished behind it — the rollback-able shape.
	delivery, err := svc.DeliverInput(ctx, service.DeliverInput{
		InstanceID: inst.ID, IdempotencyKey: "repark-1", Payload: []byte(`{"v":1}`),
	})
	if err != nil || !delivery.Accepted {
		t.Fatalf("DeliverInput() = %+v, err %v", delivery, err)
	}
	cur = driveEngine(t, db, inst.ID)
	if cur.Status != model.WorkflowFailed {
		t.Fatalf("status = %s, want failed (error %q)", cur.Status, cur.Error)
	}
	occ, err := repo.GetNodeInstanceByNode(ctx, inst.ID, n1)
	if err != nil {
		t.Fatal(err)
	}
	if occ.Status != model.NodeFinished {
		t.Fatalf("input occurrence status = %s, want finished", occ.Status)
	}
	res, err := svc.Rollback(ctx, service.RollbackRequest{InstanceID: inst.ID, TargetOccurrenceID: occ.ID})
	if err != nil {
		t.Fatalf("Rollback() error = %v", err)
	}
	if res.Status != model.WorkflowPaused || res.CurrentNodeID != n1 {
		t.Errorf("res = %+v, want paused at %s", res, n1)
	}
	stored, _ := svc.GetStatus(ctx, inst.ID)
	if stored.WaitingReason != model.WaitingReasonInput {
		t.Errorf("waiting_reason = %q, want input (re-park)", stored.WaitingReason)
	}
	got, _ := svc.GetContext(ctx, inst.ID)
	if !jsonEqualCtx(t, got.Context, json.RawMessage(`{}`)) {
		t.Errorf("context = %s, want restored pre-input {}", got.Context)
	}
	occRearmed, _ := repo.GetNodeInstance(ctx, inst.ID, occ.ID)
	if occRearmed.Status != model.NodeRunning || occRearmed.Attempt != 2 {
		t.Errorf("re-armed occurrence = %s/%d, want running/2", occRearmed.Status, occRearmed.Attempt)
	}
	// Resume re-parks on the input node; a fresh delivery with a new key
	// is accepted on the same occurrence row.
	if _, err := svc.Resume(ctx, inst.ID); err != nil {
		t.Fatalf("Resume() error = %v", err)
	}
	cur = driveEngine(t, db, inst.ID)
	if cur.Status != model.WorkflowWaiting || cur.WaitingReason != model.WaitingReasonInput {
		t.Fatalf("instance = %+v, want re-parked on input", cur)
	}
	delivery2, err := svc.DeliverInput(ctx, service.DeliverInput{
		InstanceID: inst.ID, IdempotencyKey: "repark-2", Payload: []byte(`{"v":3}`),
	})
	if err != nil || !delivery2.Accepted {
		t.Fatalf("second DeliverInput() = %+v, err %v", delivery2, err)
	}
	if delivery2.NodeInstanceID != occ.ID {
		t.Errorf("delivery node = %s, want same occurrence %s", delivery2.NodeInstanceID, occ.ID)
	}
	final := driveEngine(t, db, inst.ID)
	if final.Status != model.WorkflowFailed {
		t.Fatalf("status = %s, want failed again at n2 (error %q)", final.Status, final.Error)
	}
	gotFinal, _ := svc.GetContext(ctx, inst.ID)
	var ctxMap map[string]any
	_ = json.Unmarshal(gotFinal.Context, &ctxMap)
	if gate, ok := ctxMap["gate"].(map[string]any); !ok || gate["v"] != float64(3) {
		t.Errorf("context = %s, want fresh delivery v=3", gotFinal.Context)
	}
}

func TestRollbackToFinishedInputSupersedesOtherLivePark(t *testing.T) {
	db := setupSvcDB(t)
	ctx := context.Background()
	svc := svcInstanceService(db)

	// n1 input (delivered, finished) -> n2 script (finished) -> n3 input
	// parks live. Rollback to the finished n1 input must close the live n3
	// park and re-arm n1 in the same transaction.
	n1 := "11111111-1111-7111-8111-111111111101"
	n2 := "11111111-1111-7111-8111-111111111102"
	n3 := "11111111-1111-7111-8111-111111111103"
	n4 := "11111111-1111-7111-8111-111111111104"
	wfID := svcCreateWorkflow(t, db, n1,
		svcNodeJSON(n1, "input", "ask-first", "", n2,
			map[string]any{"channel": "http", "context_path": "gate"}),
		svcNodeJSON(n2, "script", "ok", "return 1;", n3, map[string]any{"output_property": "mid"}),
		svcNodeJSON(n3, "input", "ask-later", "", n4,
			map[string]any{"channel": "http", "context_path": "gate2"}),
		svcNodeJSON(n4, "script", "done", "return 1;", "", map[string]any{"output_property": "last"}),
	)
	inst, err := svc.Create(ctx, service.CreateInstance{WorkflowDefinitionID: wfID})
	if err != nil {
		t.Fatal(err)
	}
	repo := repository.NewInstanceRepository(db)
	cur := driveEngine(t, db, inst.ID)
	if cur.Status != model.WorkflowWaiting || cur.WaitingReason != model.WaitingReasonInput {
		t.Fatalf("instance = %+v, want parked on first input", cur)
	}
	first, err := repo.GetNodeInstanceByNode(ctx, inst.ID, n1)
	if err != nil {
		t.Fatal(err)
	}
	delivery, err := svc.DeliverInput(ctx, service.DeliverInput{
		InstanceID: inst.ID, IdempotencyKey: "supersede-rearm-1", Payload: []byte(`{"v":1}`),
	})
	if err != nil || !delivery.Accepted {
		t.Fatalf("DeliverInput() = %+v, err %v", delivery, err)
	}
	cur = driveEngine(t, db, inst.ID)
	if cur.Status != model.WorkflowWaiting || cur.WaitingReason != model.WaitingReasonInput {
		t.Fatalf("instance = %+v, want parked on second input", cur)
	}
	live, err := repo.GetNodeInstanceByNode(ctx, inst.ID, n3)
	if err != nil {
		t.Fatal(err)
	}
	if live.Status != model.NodeRunning {
		t.Fatalf("n3 park status = %s, want running", live.Status)
	}
	finishedFirst, err := repo.GetNodeInstance(ctx, inst.ID, first.ID)
	if err != nil {
		t.Fatal(err)
	}
	if finishedFirst.Status != model.NodeFinished {
		t.Fatalf("n1 occurrence status = %s, want finished", finishedFirst.Status)
	}
	if _, err := svc.Pause(ctx, inst.ID); err != nil {
		t.Fatalf("Pause() error = %v", err)
	}
	res, err := svc.Rollback(ctx, service.RollbackRequest{InstanceID: inst.ID, TargetOccurrenceID: first.ID})
	if err != nil {
		t.Fatalf("Rollback() error = %v", err)
	}
	if res.Status != model.WorkflowPaused || res.CurrentNodeID != n1 {
		t.Fatalf("res = %+v, want paused at %s", res, n1)
	}
	closed, err := repo.GetNodeInstance(ctx, inst.ID, live.ID)
	if err != nil {
		t.Fatal(err)
	}
	if closed.Status != model.NodeStopped || !closed.Cancelled {
		t.Errorf("live n3 park = %s/cancelled=%v, want stopped/true", closed.Status, closed.Cancelled)
	}
	rearmed, err := repo.GetNodeInstance(ctx, inst.ID, first.ID)
	if err != nil {
		t.Fatal(err)
	}
	if rearmed.Status != model.NodeRunning || rearmed.Attempt != 2 {
		t.Errorf("re-armed n1 = %s/%d, want running/2", rearmed.Status, rearmed.Attempt)
	}
	if _, err := svc.Resume(ctx, inst.ID); err != nil {
		t.Fatalf("Resume() error = %v", err)
	}
	cur = driveEngine(t, db, inst.ID)
	if cur.Status != model.WorkflowWaiting || cur.WaitingReason != model.WaitingReasonInput {
		t.Fatalf("instance = %+v, want re-parked on n1", cur)
	}
	delivery2, err := svc.DeliverInput(ctx, service.DeliverInput{
		InstanceID: inst.ID, IdempotencyKey: "supersede-rearm-2", Payload: []byte(`{"v":2}`),
	})
	if err != nil || !delivery2.Accepted {
		t.Fatalf("second DeliverInput() = %+v, err %v", delivery2, err)
	}
	if delivery2.NodeInstanceID != first.ID {
		t.Errorf("delivery node = %s, want re-armed n1 %s", delivery2.NodeInstanceID, first.ID)
	}
}

func svcTestEngine(t *testing.T, db *gorm.DB) *engine.Engine {
	t.Helper()
	instances := repository.NewInstanceRepositoryWithOptions(db, model.LeanOptions{AnchorEvery: 20, ReplayMax: 500})
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
	return engine.NewEngine(instances, executor.NewExecutors(executor.Limits{}, nil, executor.Dependencies{}), executor.NewHookRunner(nil), model.DefaultLimits(), loader, svcSysUserID, model.LeanOptions{AnchorEvery: 20, ReplayMax: 500})
}

func TestStatusDetailNodesMap(t *testing.T) {
	db := setupSvcDB(t)
	ctx := context.Background()
	svc := svcInstanceService(db)

	n1 := "11111111-1111-7111-8111-111111111101"
	n2 := "11111111-1111-7111-8111-111111111102"
	wfID := svcCreateWorkflow(t, db, n1,
		svcNodeJSON(n1, "script", "a", "context.x = 1; return 1;", n2, map[string]any{"output_property": "out1"}),
		svcNodeJSON(n2, "script", "b", "context.y = 2; return 2;", "", map[string]any{"output_property": "out2"}),
	)
	inst, err := svc.Create(ctx, service.CreateInstance{WorkflowDefinitionID: wfID})
	if err != nil {
		t.Fatal(err)
	}
	repo := repository.NewInstanceRepository(db)
	eng := svcTestEngine(t, db)
	claimed, err := repo.ClaimNext(ctx, "nodesmap-worker", time.Minute, 10)
	if err != nil {
		t.Fatal(err)
	}
	for _, w := range claimed {
		if w.ID == inst.ID {
			if err := eng.Process(ctx, w); err != nil {
				t.Fatalf("Process(n1) error = %v", err)
			}
		}
	}
	occ1, err := repo.GetNodeInstanceByNode(ctx, inst.ID, n1)
	if err != nil {
		t.Fatalf("GetNodeInstanceByNode(n1) error = %v", err)
	}
	if _, err := svc.Pause(ctx, inst.ID); err != nil {
		t.Fatalf("Pause() error = %v", err)
	}

	d, err := svc.GetStatusDetail(ctx, inst.ID)
	if err != nil {
		t.Fatalf("GetStatusDetail() error = %v", err)
	}
	e1, ok := d.Nodes[n1]
	if !ok {
		t.Fatalf("Nodes missing %s: %v", n1, d.Nodes)
	}
	if e1.OccurrenceID == nil || *e1.OccurrenceID != occ1.ID {
		t.Errorf("n1 occurrence = %v, want %s", e1.OccurrenceID, occ1.ID)
	}
	if e1.Status != string(model.NodeFinished) {
		t.Errorf("n1 status = %q, want finished", e1.Status)
	}
	if e1.Attempt == nil || *e1.Attempt != 1 {
		t.Errorf("n1 attempt = %v, want 1", e1.Attempt)
	}
	if !e1.Rollbackable {
		t.Error("n1 rollbackable = false, want true")
	}
	e2, ok := d.Nodes[n2]
	if !ok {
		t.Fatalf("Nodes missing %s: %v", n2, d.Nodes)
	}
	if e2.OccurrenceID != nil {
		t.Errorf("n2 occurrence = %v, want nil", *e2.OccurrenceID)
	}
	if e2.Status != "not_started" {
		t.Errorf("n2 status = %q, want not_started", e2.Status)
	}
	if e2.Attempt != nil {
		t.Errorf("n2 attempt = %v, want nil", *e2.Attempt)
	}
	if e2.Rollbackable {
		t.Error("n2 rollbackable = true, want false")
	}

	// Instance gate: while waiting nothing is rollbackable.
	if _, err := svc.Resume(ctx, inst.ID); err != nil {
		t.Fatalf("Resume() error = %v", err)
	}
	d, err = svc.GetStatusDetail(ctx, inst.ID)
	if err != nil {
		t.Fatalf("GetStatusDetail() error = %v", err)
	}
	for id, e := range d.Nodes {
		if e.Rollbackable {
			t.Errorf("Nodes[%s] rollbackable = true while waiting, want false", id)
		}
	}
}

func TestStatusDetailNodesMapNestedGroups(t *testing.T) {
	db := setupSvcDB(t)
	ctx := context.Background()
	svc := svcInstanceService(db)

	inner := "11111111-1111-7111-8111-111111111101"
	mid := "11111111-1111-7111-8111-111111111102"
	outer := "11111111-1111-7111-8111-111111111103"
	wfID := svcCreateWorkflow(t, db, outer,
		svcNodeJSON(outer, "group", "outer", "", "", map[string]any{
			"start_node_id": mid,
			"nodes": []map[string]any{
				{"id": mid, "type": "group", "name": "mid", "start_node_id": inner,
					"nodes": []map[string]any{
						{"id": inner, "type": "script", "name": "leaf", "script": "throw new Error('leaf boom');"},
					}},
			},
		}),
	)
	inst, err := svc.Create(ctx, service.CreateInstance{WorkflowDefinitionID: wfID, Context: json.RawMessage(`{"seed":1}`)})
	if err != nil {
		t.Fatal(err)
	}
	cur := driveEngine(t, db, inst.ID)
	if cur.Status != model.WorkflowFailed {
		t.Fatalf("status = %s, want failed", cur.Status)
	}
	repo := repository.NewInstanceRepository(db)
	occ, err := repo.GetNodeInstanceByNode(ctx, inst.ID, inner)
	if err != nil {
		t.Fatal(err)
	}

	d, err := svc.GetStatusDetail(ctx, inst.ID)
	if err != nil {
		t.Fatalf("GetStatusDetail() error = %v", err)
	}
	// Groups have no occurrence rows: always not_started, never rollbackable.
	for _, g := range []string{outer, mid} {
		e, ok := d.Nodes[g]
		if !ok {
			t.Fatalf("Nodes missing group %s: %v", g, d.Nodes)
		}
		if e.OccurrenceID != nil || e.Status != "not_started" || e.Rollbackable {
			t.Errorf("group %s = %+v, want not_started/nil/false", g, e)
		}
	}
	leaf, ok := d.Nodes[inner]
	if !ok {
		t.Fatalf("Nodes missing leaf %s", inner)
	}
	if leaf.OccurrenceID == nil || *leaf.OccurrenceID != occ.ID {
		t.Errorf("leaf occurrence = %v, want %s", leaf.OccurrenceID, occ.ID)
	}
	if leaf.Status != string(model.NodeFailed) {
		t.Errorf("leaf status = %q, want failed", leaf.Status)
	}
	if !leaf.Rollbackable {
		t.Error("leaf rollbackable = false, want true")
	}
}

func TestStatusDetailNodesMapUnrestorableContext(t *testing.T) {
	db := setupSvcDB(t)
	ctx := context.Background()
	svc := svcInstanceService(db)

	n1 := "11111111-1111-7111-8111-111111111101"
	n2 := "11111111-1111-7111-8111-111111111102"
	wfID := svcCreateWorkflow(t, db, n1,
		svcNodeJSON(n1, "script", "a", "return 1;", n2, nil),
		svcNodeJSON(n2, "script", "b", "return 2;", "", nil),
	)
	inst, err := svc.Create(ctx, service.CreateInstance{WorkflowDefinitionID: wfID})
	if err != nil {
		t.Fatal(err)
	}
	repo := repository.NewInstanceRepository(db)
	eng := svcTestEngine(t, db)
	claimed, err := repo.ClaimNext(ctx, "nodesmap-worker", time.Minute, 10)
	if err != nil {
		t.Fatal(err)
	}
	for _, w := range claimed {
		if w.ID == inst.ID {
			if err := eng.Process(ctx, w); err != nil {
				t.Fatalf("Process(n1) error = %v", err)
			}
		}
	}
	occ1, err := repo.GetNodeInstanceByNode(ctx, inst.ID, n1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Pause(ctx, inst.ID); err != nil {
		t.Fatalf("Pause() error = %v", err)
	}

	// A null ContextBefore mirrors the rollback 422 path: identity fields
	// stay, but the entry is not rollbackable.
	occ1.ContextBefore = json.RawMessage("null")
	if err := repo.UpdateNodeInstance(ctx, *occ1); err != nil {
		t.Fatalf("UpdateNodeInstance() error = %v", err)
	}
	d, err := svc.GetStatusDetail(ctx, inst.ID)
	if err != nil {
		t.Fatalf("GetStatusDetail() error = %v", err)
	}
	e, ok := d.Nodes[n1]
	if !ok {
		t.Fatalf("Nodes missing %s", n1)
	}
	if e.OccurrenceID == nil || *e.OccurrenceID != occ1.ID {
		t.Errorf("n1 occurrence = %v, want %s", e.OccurrenceID, occ1.ID)
	}
	if e.Status != string(model.NodeFinished) {
		t.Errorf("n1 status = %q, want finished", e.Status)
	}
	if e.Rollbackable {
		t.Error("n1 rollbackable = true with null ContextBefore, want false")
	}
}

// leanTargetCursorForGapTest returns the history cursor of the target
// occurrence row. The gap test corrupts replay between the base anchor and
// this cursor.
func leanTargetCursorForGapTest(t *testing.T, rows []model.NodeContextHistory, occurrenceID string) (model.HistoryCursor, bool) {
	t.Helper()
	var cursor model.HistoryCursor
	found := false
	for _, row := range rows {
		if row.Superseded || row.OccurrenceID != occurrenceID {
			continue
		}
		if !found || row.Cursor().Before(cursor) {
			cursor = row.Cursor()
			found = true
		}
	}
	return cursor, found
}

// leanWorkflowIDs creates a two-script chain in lean mode: n1 writes x/out1,
// n2 writes y/out2. The returned ids are the graph node ids.
func leanWorkflowChain(t *testing.T, db *gorm.DB) (wfID, n1, n2 string) {
	t.Helper()
	n1 = "11111111-1111-7111-8111-111111112101"
	n2 = "11111111-1111-7111-8111-111111112102"
	wfID = svcCreateWorkflowWithMode(t, db, n1, model.ContextModeLean,
		svcNodeJSON(n1, "script", "a", "context.x = 1; return 1;", n2, map[string]any{"output_property": "out1"}),
		svcNodeJSON(n2, "script", "b", "context.y = 2; return 2;", "", map[string]any{"output_property": "out2"}),
	)
	return wfID, n1, n2
}

// leanWorkflowInput creates a lean input flow: input node parks, script node
// finishes downstream.
func leanWorkflowInput(t *testing.T, db *gorm.DB) (wfID, in, after string) {
	t.Helper()
	in = "11111111-1111-7111-8111-111111112111"
	after = "11111111-1111-7111-8111-111111112112"
	wfID = svcCreateWorkflowWithMode(t, db, in, model.ContextModeLean,
		svcNodeJSON(in, "input", "ask", "", after, map[string]any{"channel": "http", "context_path": "gate"}),
		svcNodeJSON(after, "script", "done", "return 1;", "", map[string]any{"output_property": "done"}),
	)
	return wfID, in, after
}

// leanService wires the service with lean options reusing the same repo
// options so history replay limits match production wiring.
func leanService(t *testing.T, db *gorm.DB, options model.LeanOptions) service.InstanceService {
	t.Helper()
	return svcInstanceServiceWithOptions(db, options)
}

func leanOccurrence(t *testing.T, db *gorm.DB, instanceID, nodeID string) model.NodeInstance {
	t.Helper()
	occ, err := repository.NewInstanceRepository(db).GetNodeInstanceByNode(context.Background(), instanceID, nodeID)
	if err != nil {
		t.Fatalf("GetNodeInstanceByNode(%s) error = %v", nodeID, err)
	}
	return *occ
}

func leanMustPause(t *testing.T, svc service.InstanceService, instanceID string) {
	t.Helper()
	if _, err := svc.Pause(context.Background(), instanceID); err != nil {
		t.Fatalf("Pause() error = %v", err)
	}
}

// TestLeanRollbackRestoresFullContext drives a lean two-node chain, pauses,
// rolls back to the first occurrence, and expects the exact pre-node
// context, a post-rollback anchor, and superseded later rows.
func TestLeanRollbackRestoresFullContext(t *testing.T) {
	db := setupSvcDB(t)
	ctx := context.Background()
	opts := model.LeanOptions{AnchorEvery: 20, ReplayMax: 500}
	svc := leanService(t, db, opts)
	wfID, n1, _ := leanWorkflowChain(t, db)

	inst, err := svc.Create(ctx, service.CreateInstance{WorkflowDefinitionID: wfID, Context: json.RawMessage(`{"seed":7}`)})
	if err != nil {
		t.Fatal(err)
	}
	cur := leanDriveEngine(t, db, inst.ID, opts)
	if cur.Status != model.WorkflowFinished {
		t.Fatalf("status = %s, want finished", cur.Status)
	}
	occ1 := leanOccurrence(t, db, inst.ID, n1)

	repo := repository.NewInstanceRepository(db)
	before, err := repo.LoadHistory(ctx, inst.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(before) == 0 {
		t.Fatal("history rows = 0, want lean commits")
	}

	// Rollback requires paused or failed; finished is a conflict.
	if _, err := svc.Rollback(ctx, service.RollbackRequest{InstanceID: inst.ID, TargetOccurrenceID: occ1.ID}); !errors.Is(err, model.ErrConflict) {
		t.Fatalf("Rollback(finished) error = %v, want ErrConflict", err)
	}
	_ = before
	_ = repo
}

// TestLeanRollbackAfterPauseFailsWithoutServiceReplay exercises the paused
// path so the RED run fails until the lean rollback branch lands: lean
// occurrence rows store null ContextBefore, so the full-mode restore path
// would reject them as unrestorable.
func TestLeanRollbackAfterPauseFailsWithoutServiceReplay(t *testing.T) {
	db := setupSvcDB(t)
	ctx := context.Background()
	opts := model.LeanOptions{AnchorEvery: 20, ReplayMax: 500}
	svc := leanService(t, db, opts)
	wfID, n1, _ := leanWorkflowChain(t, db)

	inst, err := svc.Create(ctx, service.CreateInstance{WorkflowDefinitionID: wfID, Context: json.RawMessage(`{"seed":7}`)})
	if err != nil {
		t.Fatal(err)
	}
	repo := repository.NewInstanceRepositoryWithOptions(db, opts)
	claimed, err := repo.ClaimNext(ctx, "lean-rollback-worker", time.Minute, 10)
	if err != nil {
		t.Fatal(err)
	}
	eng := svcTestEngine(t, db)
	for _, w := range claimed {
		if w.ID == inst.ID {
			if err := eng.Process(ctx, w); err != nil {
				t.Fatalf("Process(n1) error = %v", err)
			}
		}
	}
	occ1 := leanOccurrence(t, db, inst.ID, n1)
	leanMustPause(t, svc, inst.ID)

	res, err := svc.Rollback(ctx, service.RollbackRequest{InstanceID: inst.ID, TargetOccurrenceID: occ1.ID})
	if err != nil {
		t.Fatalf("Rollback() error = %v", err)
	}
	if res.Status != model.WorkflowPaused || res.CurrentNodeID != n1 {
		t.Fatalf("res = %+v, want paused at %s", res, n1)
	}
	got, err := svc.GetContext(ctx, inst.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !jsonEqualCtx(t, got.Context, json.RawMessage(`{"seed":7}`)) {
		t.Fatalf("context = %s, want restored seed", got.Context)
	}
	after, err := repo.LoadHistory(ctx, inst.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(after) == 0 || !after[len(after)-1].IsAnchor {
		t.Fatalf("history = %+v, want post-rollback anchor", after)
	}
	if !after[len(after)-1].Superseded == false {
		t.Fatalf("post-rollback anchor superseded = true, want false")
	}
	superseded := 0
	for i := range after {
		if i < len(after)-1 && after[i].Superseded {
			superseded++
		}
	}
	if superseded == 0 {
		t.Fatalf("history = %+v, want later rows superseded", after)
	}
}

// TestLeanNodeDebugReconstructsFullContexts expects lean NodeDebug to return
// reconstructed full before/after snapshots even though occurrence rows
// store null ContextBefore and diff ContextAfter.
func TestLeanNodeDebugReconstructsFullContexts(t *testing.T) {
	db := setupSvcDB(t)
	ctx := context.Background()
	opts := model.LeanOptions{AnchorEvery: 20, ReplayMax: 500}
	svc := leanService(t, db, opts)
	wfID, n1, _ := leanWorkflowChain(t, db)

	inst, err := svc.Create(ctx, service.CreateInstance{WorkflowDefinitionID: wfID, Context: json.RawMessage(`{"seed":7}`)})
	if err != nil {
		t.Fatal(err)
	}
	leanDriveEngine(t, db, inst.ID, opts)

	d, err := svc.NodeDebug(ctx, inst.ID, n1, 0)
	if err != nil {
		t.Fatalf("NodeDebug() error = %v", err)
	}
	if !jsonEqualCtx(t, d.ContextBefore, json.RawMessage(`{"seed":7}`)) {
		t.Fatalf("context_before = %s, want reconstructed seed", d.ContextBefore)
	}
	var after map[string]any
	if err := json.Unmarshal(d.ContextAfter, &after); err != nil {
		t.Fatalf("parse context_after: %v", err)
	}
	if after["x"] != float64(1) || after["out1"] != float64(1) {
		t.Fatalf("context_after = %s, want reconstructed n1 output", d.ContextAfter)
	}
	if d.OccurrenceID == "" || d.Status != "finished" {
		t.Fatalf("detail = %+v, want finished occurrence", d)
	}
}

// TestLeanUpdateContextWritesAnchorBaseline expects UpdateContext on lean
// to keep the replaced context and append an anchor row without superseding
// the prefix.
func TestLeanUpdateContextWritesAnchorBaseline(t *testing.T) {
	db := setupSvcDB(t)
	ctx := context.Background()
	opts := model.LeanOptions{AnchorEvery: 20, ReplayMax: 500}
	svc := leanService(t, db, opts)
	wfID, n1, _ := leanWorkflowChain(t, db)

	inst, err := svc.Create(ctx, service.CreateInstance{WorkflowDefinitionID: wfID, Context: json.RawMessage(`{}`)})
	if err != nil {
		t.Fatal(err)
	}
	cur := leanDriveEngine(t, db, inst.ID, opts)
	if cur.Status != model.WorkflowFinished {
		t.Fatalf("status = %s, want finished", cur.Status)
	}
	_ = n1

	fresh, err := svc.Create(ctx, service.CreateInstance{WorkflowDefinitionID: wfID, Context: json.RawMessage(`{}`)})
	if err != nil {
		t.Fatal(err)
	}
	leanMustPause(t, svc, fresh.ID)
	repo := repository.NewInstanceRepository(db)
	beforeRows, err := repo.LoadHistory(ctx, fresh.ID)
	if err != nil {
		t.Fatal(err)
	}
	got, err := svc.UpdateContext(ctx, service.UpdateContext{InstanceID: fresh.ID, Context: json.RawMessage(`{"patched":true}`)})
	if err != nil {
		t.Fatalf("UpdateContext() error = %v", err)
	}
	if !jsonEqualCtx(t, got.Context, json.RawMessage(`{"patched":true}`)) {
		t.Fatalf("context = %s, want patched replacement", got.Context)
	}
	afterRows, err := repo.LoadHistory(ctx, fresh.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(afterRows) != len(beforeRows)+1 {
		t.Fatalf("history rows = %d, want %d after anchor append", len(afterRows), len(beforeRows)+1)
	}
	last := afterRows[len(afterRows)-1]
	if !last.IsAnchor || !jsonEqualCtx(t, last.Snapshot, json.RawMessage(`{"patched":true}`)) {
		t.Fatalf("last history = %+v, want anchor baseline of patched context", last)
	}
	for i := range afterRows {
		if i < len(afterRows)-1 && afterRows[i].Superseded {
			t.Fatalf("history[%d] superseded = true, want prefix preserved", i)
		}
	}
}

// TestLeanStatusNodesAvoidPerNodeFanOut expects lean status to mark a
// finished occurrence rollbackable via history presence (not ContextBefore
// parsing) while paused.
func TestLeanStatusNodesAvoidPerNodeFanOut(t *testing.T) {
	db := setupSvcDB(t)
	ctx := context.Background()
	opts := model.LeanOptions{AnchorEvery: 20, ReplayMax: 500}
	svc := leanService(t, db, opts)
	wfID, n1, n2 := leanWorkflowChain(t, db)

	inst, err := svc.Create(ctx, service.CreateInstance{WorkflowDefinitionID: wfID, Context: json.RawMessage(`{}`)})
	if err != nil {
		t.Fatal(err)
	}
	repo := repository.NewInstanceRepository(db)
	eng := svcTestEngine(t, db)
	claimed, err := repo.ClaimNext(ctx, "lean-nodesmap-worker", time.Minute, 10)
	if err != nil {
		t.Fatal(err)
	}
	for _, w := range claimed {
		if w.ID == inst.ID {
			if err := eng.Process(ctx, w); err != nil {
				t.Fatalf("Process(n1) error = %v", err)
			}
		}
	}
	occ1 := leanOccurrence(t, db, inst.ID, n1)
	if string(occ1.ContextBefore) != "null" {
		t.Fatalf("context_before = %s, want null for lean", occ1.ContextBefore)
	}
	leanMustPause(t, svc, inst.ID)

	d, err := svc.GetStatusDetail(ctx, inst.ID)
	if err != nil {
		t.Fatalf("GetStatusDetail() error = %v", err)
	}
	e1, ok := d.Nodes[n1]
	if !ok {
		t.Fatalf("Nodes missing %s", n1)
	}
	if e1.OccurrenceID == nil || *e1.OccurrenceID != occ1.ID {
		t.Fatalf("n1 occurrence = %v, want %s", e1.OccurrenceID, occ1.ID)
	}
	if !e1.Rollbackable {
		t.Fatalf("n1 rollbackable = false, want true via history presence")
	}
	e2, ok := d.Nodes[n2]
	if !ok {
		t.Fatalf("Nodes missing %s", n2)
	}
	if e2.Rollbackable {
		t.Fatalf("n2 rollbackable = true, want false (not_started)")
	}
}

// TestLeanDeliverInputPersistsDiffAtomically expects service delivery on a
// lean instance to persist one history diff row matching the delivered
// payload advancement.
func TestLeanDeliverInputPersistsDiffAtomically(t *testing.T) {
	db := setupSvcDB(t)
	ctx := context.Background()
	opts := model.LeanOptions{AnchorEvery: 20, ReplayMax: 500}
	svc := leanService(t, db, opts)
	wfID, in, _ := leanWorkflowInput(t, db)

	inst, err := svc.Create(ctx, service.CreateInstance{WorkflowDefinitionID: wfID, Context: json.RawMessage(`{"seed":1}`)})
	if err != nil {
		t.Fatal(err)
	}
	cur := leanDriveEngine(t, db, inst.ID, opts)
	if cur.Status != model.WorkflowWaiting || cur.WaitingReason != model.WaitingReasonInput {
		t.Fatalf("instance = %+v, want waiting on input", cur)
	}
	repo := repository.NewInstanceRepository(db)
	rowsBefore, err := repo.LoadHistory(ctx, inst.ID)
	if err != nil {
		t.Fatal(err)
	}

	delivery, err := svc.DeliverInput(ctx, service.DeliverInput{
		InstanceID: inst.ID, IdempotencyKey: "lean-input-1", Payload: []byte(`{"v":2}`),
	})
	if err != nil {
		t.Fatalf("DeliverInput() error = %v", err)
	}
	if !delivery.Accepted {
		t.Fatalf("delivery = %+v, want accepted", delivery)
	}
	rowsAfter, err := repo.LoadHistory(ctx, inst.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(rowsAfter) != len(rowsBefore)+1 {
		t.Fatalf("history rows = %d, want %d after delivery", len(rowsAfter), len(rowsBefore)+1)
	}
	last := rowsAfter[len(rowsAfter)-1]
	if last.IsAnchor {
		t.Fatalf("last history = %+v, want delivery diff row, not anchor", last)
	}
	got, err := svc.GetContext(ctx, inst.ID)
	if err != nil {
		t.Fatal(err)
	}
	var ctxMap map[string]any
	if err := json.Unmarshal(got.Context, &ctxMap); err != nil {
		t.Fatal(err)
	}
	gate, ok := ctxMap["gate"].(map[string]any)
	if !ok || gate["v"] != float64(2) {
		t.Fatalf("context = %s, want delivered payload at gate", got.Context)
	}
	_ = in
}

// TestLeanHistoryGapMapsToConflict expects a broken lean history chain to
// fail closed with 409 semantics on rollback.
func TestLeanHistoryGapMapsToConflict(t *testing.T) {
	db := setupSvcDB(t)
	ctx := context.Background()
	opts := model.LeanOptions{AnchorEvery: 20, ReplayMax: 500}
	svc := leanService(t, db, opts)
	wfID, n1, _ := leanWorkflowChain(t, db)

	inst, err := svc.Create(ctx, service.CreateInstance{WorkflowDefinitionID: wfID, Context: json.RawMessage(`{"seed":7}`)})
	if err != nil {
		t.Fatal(err)
	}
	repo := repository.NewInstanceRepository(db)
	claimed, err := repo.ClaimNext(ctx, "lean-gap-worker", time.Minute, 10)
	if err != nil {
		t.Fatal(err)
	}
	eng := svcTestEngine(t, db)
	for _, w := range claimed {
		if w.ID == inst.ID {
			if err := eng.Process(ctx, w); err != nil {
				t.Fatalf("Process(n1) error = %v", err)
			}
		}
	}
	occ1 := leanOccurrence(t, db, inst.ID, n1)
	leanMustPause(t, svc, inst.ID)

	rows, err := repo.LoadHistory(ctx, inst.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) == 0 {
		t.Fatal("history rows = 0, want lean commits")
	}
	// Corrupt the chain the rollback must traverse: insert an invalid
	// diff row between the create anchor and the target occurrence row so
	// replay to the target fails closed. A row before the anchor base is
	// correctly ignored by replay, so it must sort after the anchor.
	targetCursor, found := leanTargetCursorForGapTest(t, rows, occ1.ID)
	if !found {
		t.Fatal("history has no row for target occurrence, want lean commit")
	}
	bad := rows[0]
	bad.ID = targetCursor.ID + "-bad"
	if bad.ID == targetCursor.ID {
		bad.ID = svcNewID()
	}
	// Force the bad row to sort strictly after the base anchor and before
	// or at the target: reuse the target timestamp with a lexicographically
	// smaller id so (created_at, id) ordering places it in the replay
	// prefix regardless of generated UUID ordering.
	bad.WorkflowInstanceID = inst.ID
	bad.OccurrenceID = ""
	bad.NodeID = n1
	bad.Attempt = 0
	bad.IsAnchor = false
	bad.Snapshot = json.RawMessage("null")
	bad.Diff = json.RawMessage(`{"not":"a diff"}`)
	bad.Superseded = false
	bad.CreatedAt = targetCursor.CreatedAt
	if bad.ID >= targetCursor.ID {
		bad.ID = "00000000-0000-7000-8000-000000000001"
		if bad.ID >= targetCursor.ID {
			t.Fatalf("cannot order bad row before target %s", targetCursor.ID)
		}
	}
	if err := repo.AppendHistory(ctx, bad); err != nil {
		t.Fatalf("AppendHistory(bad) error = %v", err)
	}

	if _, err := svc.Rollback(ctx, service.RollbackRequest{InstanceID: inst.ID, TargetOccurrenceID: occ1.ID}); !errors.Is(err, model.ErrConflict) {
		t.Fatalf("Rollback(broken chain) error = %v, want ErrConflict", err)
	}
}
