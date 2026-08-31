package engine_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/simpwf/workflow-engine/internal/workflow/engine"
	"github.com/simpwf/workflow-engine/internal/workflow/executor"
	"github.com/simpwf/workflow-engine/internal/workflow/model"
	"github.com/simpwf/workflow-engine/internal/workflow/repository"
	"github.com/simpwf/workflow-engine/pkg/database"
	"github.com/simpwf/workflow-engine/pkg/ids"
	"gorm.io/gorm"
)

const (
	sysUserID = "11111111-1111-7111-8111-111111111111"
)

var testLimitsNode = model.NodeLimits{DefaultTimeout: 5 * time.Second, MaxTimeout: 10 * time.Second, ConditionTimeout: 2 * time.Second}

func setupEngineDB(t *testing.T) *gorm.DB {
	t.Helper()
	// Engine tests run in parallel with repository tests against the same
	// Postgres instance; they need their own database so the per-test
	// truncation never clobbers another package's fixtures.
	dsn := os.Getenv("TEST_DATABASE_DSN_ENGINE")
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
	if err := db.Exec(`TRUNCATE TABLE
		status_update_outbox, input_deliveries, workflow_instance_events, node_instances,
		workflow_instances, workflow_requests, workflow_definition_node_refs,
		workflow_definitions, node_definitions, users RESTART IDENTITY`).Error; err != nil {
		t.Fatalf("truncate tables: %v", err)
	}
	ctx := context.Background()
	system := model.User{ID: sysUserID, Name: "system", Email: "system@localhost"}
	if err := repository.UpsertSystemUser(ctx, db, system); err != nil {
		t.Fatalf("seed system user: %v", err)
	}
	return db
}

func testEngine(t *testing.T, db *gorm.DB, limits model.Limits) (*engine.Engine, repository.InstanceRepository) {
	t.Helper()
	return testEngineWithExec(t, db, limits, executor.Limits{})
}

func testEngineWithExec(t *testing.T, db *gorm.DB, limits model.Limits, execLimits executor.Limits) (*engine.Engine, repository.InstanceRepository) {
	t.Helper()
	instances := repository.NewInstanceRepository(db)
	e := engine.NewEngine(instances, executor.NewExecutors(execLimits, nil, executor.Dependencies{}), executor.NewHookRunner(nil), limits, testLoader(db), sysUserID)
	return e, instances
}

func testLoader(db *gorm.DB) engine.WorkflowLoader {
	return func(ctx context.Context, instanceID string) (*model.WorkflowContent, error) {
		inst, err := repository.NewInstanceRepository(db).GetByID(ctx, instanceID)
		if err != nil {
			return nil, err
		}
		def, err := repository.NewWorkflowDefinitionRepository(db).GetByID(ctx, inst.WorkflowDefinitionID)
		if err != nil {
			return nil, err
		}
		return model.ParseWorkflowContent(def.Content, testLimitsNode)
	}
}

func newEngineID() string {
	id, err := ids.NewString()
	if err != nil {
		panic(err)
	}
	return id
}

// nodeJSON renders an inline workflow node.
func nodeJSON(id, typ, name, script, next, outProp string, extra map[string]any) string {
	m := map[string]any{"id": id, "type": typ, "name": name}
	if script != "" {
		m["script"] = script
	}
	if next != "" {
		m["next_node"] = next
	}
	if outProp != "" {
		m["output_property"] = outProp
	}
	for k, v := range extra {
		m[k] = v
	}
	b, _ := json.Marshal(m)
	return string(b)
}

// createWorkflow inserts a workflow definition and returns its id.
func createWorkflow(t *testing.T, db *gorm.DB, start string, nodes ...string) string {
	return createWorkflowWithKeys(t, db, start, nil, nodes...)
}

func createWorkflowWithKeys(t *testing.T, db *gorm.DB, start string, keys map[string]any, nodes ...string) string {
	t.Helper()
	content := map[string]any{
		"start_node_id": start,
		"nodes":         json.RawMessage("[" + join(nodes) + "]"),
	}
	if keys != nil {
		content["keys"] = keys
	}
	raw, err := json.Marshal(content)
	if err != nil {
		t.Fatalf("marshal workflow definition: %v", err)
	}
	wf := model.WorkflowDefinition{
		ID:        newEngineID(),
		Name:      "engine-flow",
		Version:   1,
		LineageID: newEngineID(),
		Content:   raw,
		CreatedBy: sysUserID,
		UpdatedBy: sysUserID,
		CreatedAt: time.Now().UTC(),
		UpdatedAt: time.Now().UTC(),
	}
	if err := repository.NewWorkflowDefinitionRepository(db).Create(context.Background(), wf); err != nil {
		t.Fatalf("create workflow definition: %v", err)
	}
	return wf.ID
}

func join(parts []string) string {
	out := ""
	for i, p := range parts {
		if i > 0 {
			out += ","
		}
		out += p
	}
	return out
}

// insertInstance creates a runnable instance at the given start node.
func insertInstance(t *testing.T, db *gorm.DB, wfID string, start string, ctxMap map[string]any) string {
	t.Helper()
	id := newEngineID()
	ctxRaw, _ := json.Marshal(ctxMap)
	frameRaw, _ := model.NewFrame(start).JSON()
	w := model.WorkflowInstance{
		ID:                   id,
		WorkflowDefinitionID: wfID,
		Status:               model.WorkflowWaiting,
		WaitingReason:        model.WaitingReasonRunnable,
		Frame:                frameRaw,
		Context:              ctxRaw,
		Counters:             json.RawMessage(`{}`),
		CreatedBy:            sysUserID,
		UpdatedBy:            sysUserID,
		CreatedAt:            time.Now().UTC(),
		UpdatedAt:            time.Now().UTC(),
	}
	if err := repository.NewInstanceRepository(db).Insert(context.Background(), w); err != nil {
		t.Fatalf("insert instance: %v", err)
	}
	return id
}

// runEngine drives claim -> process until the instance is terminal or
// parked waiting on input, then returns the final instance.
func runEngine(t *testing.T, db *gorm.DB, e *engine.Engine, instanceID string) model.WorkflowInstance {
	t.Helper()
	ctx := context.Background()
	instances := repository.NewInstanceRepository(db)
	for i := 0; i < 200; i++ {
		claimed, err := instances.ClaimNext(ctx, "test-worker", time.Minute, 10)
		if err != nil {
			t.Fatalf("ClaimNext() error = %v", err)
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
		switch cur.Status {
		case model.WorkflowFinished, model.WorkflowFailed, model.WorkflowStopped:
			return *cur
		case model.WorkflowWaiting:
			if cur.WaitingReason == model.WaitingReasonInput {
				return *cur
			}
		case model.WorkflowPaused:
			return *cur
		}
	}
	t.Fatal("engine did not reach a terminal or waiting state")
	return model.WorkflowInstance{}
}

// TestEnginePollerRendersReservedIDs proves poller configuration templates
// can interpolate the reserved automatic roots workflow_instance_id and
// node_instance_id, that the reserved values always win over same-named
// user context values, and that they are never written back into the
// workflow context.
func TestEnginePollerRendersReservedIDs(t *testing.T) {
	db := setupEngineDB(t)
	var gotPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		w.WriteHeader(200)
		_, _ = io.WriteString(w, `{"ok":true}`)
	}))
	defer srv.Close()

	start := newEngineID()
	next := newEngineID()
	poller := nodeJSON(start, "poller", "poller", "", next, "out", map[string]any{
		"http": map[string]any{
			"url":   srv.URL + "/{{ workflow_instance_id }}/{{ node_instance_id }}",
			"until": "return response.status === 200;",
		},
	})
	done := nodeJSON(next, "script", "done", "return 1;", "", "", nil)
	wfID := createWorkflow(t, db, start, poller, done)
	instID := insertInstance(t, db, wfID, start, map[string]any{"workflow_instance_id": "user-value"})

	e, instances := testEngineWithExec(t, db, model.DefaultLimits(), executor.Limits{HTTPAllowlist: []string{"127.0.0.1"}})
	runEngine(t, db, e, instID)

	cur, err := instances.GetByID(context.Background(), instID)
	if err != nil {
		t.Fatal(err)
	}
	if cur.Status != model.WorkflowFinished {
		t.Fatalf("status = %s, want finished", cur.Status)
	}
	attempt, err := instances.GetNodeInstanceByNode(context.Background(), instID, start)
	if err != nil {
		t.Fatal(err)
	}
	if want := "/" + instID + "/" + attempt.ID; gotPath != want {
		t.Errorf("poller path = %q, want reserved ids %q", gotPath, want)
	}
	var ctxMap map[string]any
	if err := json.Unmarshal(cur.Context, &ctxMap); err != nil {
		t.Fatal(err)
	}
	if ctxMap["workflow_instance_id"] != "user-value" {
		t.Errorf("workflow_instance_id = %v, want unchanged user value", ctxMap["workflow_instance_id"])
	}
	if _, ok := ctxMap["node_instance_id"]; ok {
		t.Error("node_instance_id leaked into workflow context")
	}
	if ctxMap["out"] == nil {
		t.Error("poller output not written to output_property")
	}
}
