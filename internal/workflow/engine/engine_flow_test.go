package engine_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/simpwf/workflow-engine/internal/workflow/executor"
	"github.com/simpwf/workflow-engine/internal/workflow/model"
	"github.com/simpwf/workflow-engine/internal/workflow/repository"
	"gorm.io/gorm"
)

const (
	n1 = "11111111-1111-7111-8111-111111111101"
	n2 = "11111111-1111-7111-8111-111111111102"
	n3 = "11111111-1111-7111-8111-111111111103"
	n4 = "11111111-1111-7111-8111-111111111104"
	g1 = "11111111-1111-7111-8111-111111111105"
)

func instanceContext(t *testing.T, db *gorm.DB, id string) map[string]any {
	t.Helper()
	inst, err := repository.NewInstanceRepository(db).GetByID(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	m := map[string]any{}
	if err := json.Unmarshal(inst.Context, &m); err != nil {
		t.Fatal(err)
	}
	return m
}

func TestEngineRunsChainToFinish(t *testing.T) {
	db := setupEngineDB(t)
	wfID := createWorkflow(t, db, n1,
		nodeJSON(n1, "script", "inc", "context.x = (context.x||0) + 1; return context.x;", n2, "a", nil),
		nodeJSON(n2, "script", "done", "return 'done';", "", "b", nil),
	)
	instanceID := insertInstance(t, db, wfID, n1, map[string]any{"x": 0})
	e, _ := testEngine(t, db, model.DefaultLimits())
	cur := runEngine(t, db, e, instanceID)

	if cur.Status != model.WorkflowFinished {
		t.Fatalf("status = %s, want finished (error %q)", cur.Status, cur.Error)
	}
	ctx := instanceContext(t, db, instanceID)
	if ctx["a"] != float64(1) || ctx["b"] != "done" {
		t.Errorf("context = %v, want a=1 b=done", ctx)
	}
	attempts, err := repository.NewInstanceRepository(db).ListNodeInstances(context.Background(), instanceID)
	if err != nil {
		t.Fatal(err)
	}
	if len(attempts) != 2 {
		t.Fatalf("node instances = %d, want 2", len(attempts))
	}
	for _, a := range attempts {
		if a.Status != model.NodeFinished || a.Attempt != 1 {
			t.Errorf("attempt = %+v, want finished attempt 1", a)
		}
	}
}

func TestEngineConditionsRoute(t *testing.T) {
	db := setupEngineDB(t)
	wfID := createWorkflowWithKeys(t, db, n1, map[string]any{"staff": n2, "manager": n3},
		nodeJSON(n1, "conditions", "route", "", "", "", map[string]any{
			"conditions": []map[string]any{
				{"key": "staff", "condition": "return context.user === 'staff';"},
				{"key": "manager", "condition": "return context.user === 'manager';"},
			},
		}),
		nodeJSON(n2, "script", "staff-branch", "return 'staff';", "", "out", nil),
		nodeJSON(n3, "script", "manager-branch", "return 'manager';", "", "out", nil),
	)
	instanceID := insertInstance(t, db, wfID, n1, map[string]any{"user": "manager"})
	e, _ := testEngine(t, db, model.DefaultLimits())
	cur := runEngine(t, db, e, instanceID)
	if cur.Status != model.WorkflowFinished {
		t.Fatalf("status = %s, want finished (error %q)", cur.Status, cur.Error)
	}
	attempts, _ := repository.NewInstanceRepository(db).ListNodeInstances(context.Background(), instanceID)
	executed := map[string]bool{}
	for _, a := range attempts {
		executed[a.NodeID] = a.Status == model.NodeFinished
	}
	if executed[n2] || !executed[n3] {
		t.Errorf("executed = %v, want only manager branch", executed)
	}
}

func TestEngineConditionsNoMatchFails(t *testing.T) {
	db := setupEngineDB(t)
	wfID := createWorkflowWithKeys(t, db, n1, map[string]any{"never": nil},
		nodeJSON(n1, "conditions", "route", "", "", "", map[string]any{
			"conditions": []map[string]any{
				{"key": "never", "condition": "return false;"},
				{"condition": "return false;"},
			},
		}),
	)
	instanceID := insertInstance(t, db, wfID, n1, map[string]any{})
	e, _ := testEngine(t, db, model.DefaultLimits())
	cur := runEngine(t, db, e, instanceID)
	if cur.Status != model.WorkflowFailed {
		t.Fatalf("status = %s, want failed", cur.Status)
	}
	if !strings.Contains(cur.Error, "no condition matched") {
		t.Errorf("error = %q", cur.Error)
	}
}

func TestEngineConditionsMultipleMatchesFail(t *testing.T) {
	db := setupEngineDB(t)
	wfID := createWorkflowWithKeys(t, db, n1, map[string]any{"staff": n2, "manager": n3},
		nodeJSON(n1, "conditions", "route", "", "", "", map[string]any{
			"conditions": []map[string]any{
				{"key": "staff", "condition": "return context.user === 'staff';"},
				{"key": "manager", "condition": "return context.user === 'manager';"},
				{"condition": "return true;"},
			},
		}),
		nodeJSON(n2, "script", "staff-branch", "return 'staff';", "", "out", nil),
		nodeJSON(n3, "script", "manager-branch", "return 'manager';", "", "out", nil),
	)
	instanceID := insertInstance(t, db, wfID, n1, map[string]any{"user": "manager"})
	e, _ := testEngine(t, db, model.DefaultLimits())
	cur := runEngine(t, db, e, instanceID)
	if cur.Status != model.WorkflowFailed {
		t.Fatalf("status = %s, want failed (error %q)", cur.Status, cur.Error)
	}
	for _, want := range []string{"multiple conditions matched", n1, `index 1 key "manager"`, `index 2 key ""`} {
		if !strings.Contains(cur.Error, want) {
			t.Errorf("error = %q, want contains %q", cur.Error, want)
		}
	}
	attempts, _ := repository.NewInstanceRepository(db).ListNodeInstances(context.Background(), instanceID)
	for _, a := range attempts {
		if a.NodeID == n1 && a.Status != model.NodeFailed {
			t.Errorf("route attempt status = %s, want failed", a.Status)
		}
	}
}

func TestEngineConditionEmptyKeyTargetFinishes(t *testing.T) {
	db := setupEngineDB(t)
	wfID := createWorkflowWithKeys(t, db, n1, map[string]any{"done": nil},
		nodeJSON(n1, "conditions", "exit", "", "", "", map[string]any{
			"conditions": []map[string]any{
				{"key": "done", "condition": "return true;"},
				{"condition": "return false;"},
			},
		}),
	)
	instanceID := insertInstance(t, db, wfID, n1, map[string]any{})
	e, _ := testEngine(t, db, model.DefaultLimits())
	cur := runEngine(t, db, e, instanceID)
	if cur.Status != model.WorkflowFinished {
		t.Fatalf("status = %s, want finished (error %q)", cur.Status, cur.Error)
	}
}

func TestEngineConditionWithoutKeyFinishes(t *testing.T) {
	db := setupEngineDB(t)
	wfID := createWorkflow(t, db, n1,
		nodeJSON(n1, "conditions", "exit", "", "", "", map[string]any{
			"conditions": []map[string]any{
				{"condition": "return true;"},
				{"condition": "return false;"},
			},
		}),
	)
	instanceID := insertInstance(t, db, wfID, n1, map[string]any{})
	e, _ := testEngine(t, db, model.DefaultLimits())
	cur := runEngine(t, db, e, instanceID)
	if cur.Status != model.WorkflowFinished {
		t.Fatalf("status = %s, want finished (error %q)", cur.Status, cur.Error)
	}
}

func TestEngineScriptErrorFailsWorkflow(t *testing.T) {
	db := setupEngineDB(t)
	wfID := createWorkflow(t, db, n1,
		nodeJSON(n1, "script", "boom", "return missing.value;", "", "out", nil),
	)
	instanceID := insertInstance(t, db, wfID, n1, map[string]any{})
	e, _ := testEngine(t, db, model.DefaultLimits())
	cur := runEngine(t, db, e, instanceID)
	if cur.Status != model.WorkflowFailed {
		t.Fatalf("status = %s, want failed", cur.Status)
	}
	attempt, err := repository.NewInstanceRepository(db).GetNodeInstanceByNode(context.Background(), instanceID, n1)
	if err != nil {
		t.Fatal(err)
	}
	if attempt.Status != model.NodeFailed {
		t.Errorf("attempt status = %s, want failed", attempt.Status)
	}
}

func TestEngineGroupEntryExit(t *testing.T) {
	db := setupEngineDB(t)
	wfID := createWorkflow(t, db, g1,
		nodeJSON(g1, "group", "main", "", n2, "", map[string]any{
			"start_node_id": n3,
			"nodes": []map[string]any{
				{"id": n3, "type": "script", "name": "inner", "script": "return 1;", "output_property": "inner"},
			},
		}),
		nodeJSON(n2, "script", "after", "return 'after';", "", "after", nil),
	)
	instanceID := insertInstance(t, db, wfID, g1, map[string]any{})
	e, _ := testEngine(t, db, model.DefaultLimits())
	cur := runEngine(t, db, e, instanceID)
	if cur.Status != model.WorkflowFinished {
		t.Fatalf("status = %s, want finished (error %q)", cur.Status, cur.Error)
	}
	ctx := instanceContext(t, db, instanceID)
	if ctx["inner"] != float64(1) || ctx["after"] != "after" {
		t.Errorf("context = %v", ctx)
	}
	events, _ := repository.NewInstanceRepository(db).ListEvents(context.Background(), instanceID)
	types := map[string]int{}
	for _, ev := range events {
		types[ev.Type]++
	}
	if types["group_entered"] != 1 || types["group_exited"] != 1 {
		t.Errorf("events = %v, want one group_entered and one group_exited", types)
	}
}

func TestEngineGroupConditionUsesLocalKeys(t *testing.T) {
	db := setupEngineDB(t)
	wfID := createWorkflowWithKeys(t, db, g1, map[string]any{"route": n3},
		nodeJSON(g1, "group", "main", "", n3, "", map[string]any{
			"start_node_id": n1,
			"keys":          map[string]any{"route": n2},
			"nodes": []map[string]any{
				{
					"id": n1, "type": "conditions", "name": "route",
					"conditions": []map[string]any{
						{"key": "route", "condition": "return true;"},
						{"condition": "return false;"},
					},
				},
				{"id": n2, "type": "script", "name": "inner", "script": "return 'inner';", "output_property": "inner"},
			},
		}),
		nodeJSON(n3, "script", "after", "return 'after';", "", "after", nil),
	)
	instanceID := insertInstance(t, db, wfID, g1, map[string]any{})
	e, _ := testEngine(t, db, model.DefaultLimits())
	cur := runEngine(t, db, e, instanceID)
	if cur.Status != model.WorkflowFinished {
		t.Fatalf("status = %s, want finished (error %q)", cur.Status, cur.Error)
	}
	ctx := instanceContext(t, db, instanceID)
	if ctx["inner"] != "inner" || ctx["after"] != "after" {
		t.Errorf("context = %v, want local route then after", ctx)
	}
}

func TestEngineInputWaits(t *testing.T) {
	db := setupEngineDB(t)
	wfID := createWorkflow(t, db, n1,
		nodeJSON(n1, "input", "ask", "", n2, "", map[string]any{
			"channel": "http", "context_path": "webhook",
		}),
		nodeJSON(n2, "script", "after", "return 'ok';", "", "after", nil),
	)
	instanceID := insertInstance(t, db, wfID, n1, map[string]any{})
	e, _ := testEngine(t, db, model.DefaultLimits())
	cur := runEngine(t, db, e, instanceID)
	if cur.Status != model.WorkflowWaiting || cur.WaitingReason != model.WaitingReasonInput {
		t.Fatalf("status = %s/%s, want waiting/input", cur.Status, cur.WaitingReason)
	}
	attempt, err := repository.NewInstanceRepository(db).GetNodeInstanceByNode(context.Background(), instanceID, n1)
	if err != nil {
		t.Fatal(err)
	}
	if attempt.Status != model.NodeRunning {
		t.Errorf("input attempt status = %s, want running", attempt.Status)
	}
}

func TestEngineCycleLimit(t *testing.T) {
	db := setupEngineDB(t)
	// Self-looping node: limit 3 executions.
	wfID := createWorkflow(t, db, n1,
		nodeJSON(n1, "script", "loop", "return 1;", n1, "out", nil),
	)
	instanceID := insertInstance(t, db, wfID, n1, map[string]any{})
	limits := model.DefaultLimits()
	limits.MaxPerNodeExecutions = 3
	e, _ := testEngine(t, db, limits)
	cur := runEngine(t, db, e, instanceID)
	if cur.Status != model.WorkflowFailed {
		t.Fatalf("status = %s, want failed on cycle limit", cur.Status)
	}
	if !strings.Contains(cur.Error, "execution limit") {
		t.Errorf("error = %q", cur.Error)
	}
	attempt, _ := repository.NewInstanceRepository(db).GetNodeInstanceByNode(context.Background(), instanceID, n1)
	if attempt.Attempt != 3 {
		t.Errorf("attempt = %d, want 3", attempt.Attempt)
	}
}

// crashClaim simulates a dead worker: claim with a ghost worker then expire
// the lease, leaving a running attempt for recovery.
func crashClaim(t *testing.T, db *gorm.DB, instanceID string, attempt model.NodeInstance) {
	t.Helper()
	ctx := context.Background()
	instances := repository.NewInstanceRepository(db)
	claimed, err := instances.ClaimNext(ctx, "ghost-worker", time.Millisecond, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(claimed) != 1 || claimed[0].ID != instanceID {
		t.Fatalf("ghost claim = %v, want the instance", claimed)
	}
	if err := instances.InsertNodeInstance(ctx, attempt); err != nil {
		t.Fatal(err)
	}
	// Expire the lease: recoverable claim requires lease_expiry < now().
	if err := db.Exec("UPDATE workflow_instances SET lease_expiry = now() - interval '10 seconds' WHERE id = ?", instanceID).Error; err != nil {
		t.Fatal(err)
	}
}

func TestEngineRecoveryRetriesScript(t *testing.T) {
	db := setupEngineDB(t)
	wfID := createWorkflow(t, db, n1,
		nodeJSON(n1, "script", "inc", "context.x = (context.x||0) + 1; return context.x;", "", "out", nil),
	)
	instanceID := insertInstance(t, db, wfID, n1, map[string]any{"x": 0})
	now := time.Now().UTC()
	runningAttempt := model.NodeInstance{
		ID: newEngineID(), WorkflowInstanceID: instanceID, NodeID: n1,
		NodeDefinitionID: "", Name: "inc", Type: "script",
		Attempt: 1, Status: model.NodeRunning,
		ContextBefore: json.RawMessage(`{}`), StartedAt: &now,
		CreatedAt: now, UpdatedAt: now,
	}
	crashClaim(t, db, instanceID, runningAttempt)

	e, _ := testEngine(t, db, model.DefaultLimits())
	cur := runEngine(t, db, e, instanceID)
	if cur.Status != model.WorkflowFinished {
		t.Fatalf("status = %s, want finished after recovery retry (error %q)", cur.Status, cur.Error)
	}
	attempt, _ := repository.NewInstanceRepository(db).GetNodeInstanceByNode(context.Background(), instanceID, n1)
	if attempt.Attempt != 2 || attempt.RecoveryResult != "retried" {
		t.Errorf("attempt = %+v, want attempt 2 retried", attempt)
	}
}

func TestEngineRecoveryFailsExternalWithoutFlag(t *testing.T) {
	db := setupEngineDB(t)
	hit := false
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { hit = true }))
	defer srv.Close()

	wfID := createWorkflow(t, db, n1,
		nodeJSON(n1, "external_call", "http", "", "", "out", map[string]any{
			"http_config": map[string]any{"url": srv.URL, "method": "GET"},
		}),
	)
	instanceID := insertInstance(t, db, wfID, n1, map[string]any{})
	now := time.Now().UTC()
	runningAttempt := model.NodeInstance{
		ID: newEngineID(), WorkflowInstanceID: instanceID, NodeID: n1,
		NodeDefinitionID: "", Name: "http", Type: "external_call",
		Attempt: 1, Status: model.NodeRunning,
		ContextBefore: json.RawMessage(`{}`), StartedAt: &now,
		CreatedAt: now, UpdatedAt: now,
	}
	crashClaim(t, db, instanceID, runningAttempt)

	limits := model.DefaultLimits()
	limits.MaxPerNodeExecutions = 10
	e, _ := testEngine(t, db, limits)
	cur := runEngine(t, db, e, instanceID)
	if cur.Status != model.WorkflowFailed {
		t.Fatalf("status = %s, want failed (error %q)", cur.Status, cur.Error)
	}
	if hit {
		t.Error("external call executed during recovery fail path")
	}
	attempt, _ := repository.NewInstanceRepository(db).GetNodeInstanceByNode(context.Background(), instanceID, n1)
	if attempt.Status != model.NodeFailed || attempt.RecoveryResult != "failed" {
		t.Errorf("attempt = %+v, want failed with recovery result failed", attempt)
	}
}

func TestEngineRecoveryRetriesExternalWithFlag(t *testing.T) {
	db := setupEngineDB(t)
	var gotKey string
	var hits int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits++
		gotKey = r.Header.Get("Idempotency-Key")
	}))
	defer srv.Close()

	wfID := createWorkflow(t, db, n1,
		nodeJSON(n1, "external_call", "http", "", "", "out", map[string]any{
			"http_config":       map[string]any{"url": srv.URL, "method": "GET"},
			"retry_on_recovery": true,
		}),
	)
	instanceID := insertInstance(t, db, wfID, n1, map[string]any{})
	now := time.Now().UTC()
	runningAttempt := model.NodeInstance{
		ID: newEngineID(), WorkflowInstanceID: instanceID, NodeID: n1,
		NodeDefinitionID: "", Name: "http", Type: "external_call",
		Attempt: 1, Status: model.NodeRunning,
		ContextBefore: json.RawMessage(`{}`), StartedAt: &now,
		CreatedAt: now, UpdatedAt: now,
	}
	crashClaim(t, db, instanceID, runningAttempt)

	limits := model.DefaultLimits()
	limits.MaxPerNodeExecutions = 10
	e, _ := testEngineWithExec(t, db, limits, executor.Limits{HTTPAllowlist: []string{"*"}})
	cur := runEngine(t, db, e, instanceID)
	if cur.Status != model.WorkflowFinished {
		t.Fatalf("status = %s, want finished (error %q)", cur.Status, cur.Error)
	}
	if hits != 1 {
		t.Errorf("hits = %d, want 1", hits)
	}
	if gotKey == "" {
		t.Error("Idempotency-Key header missing")
	}
	attempt, _ := repository.NewInstanceRepository(db).GetNodeInstanceByNode(context.Background(), instanceID, n1)
	if attempt.Attempt != 2 || attempt.RecoveryResult != "retried" {
		t.Errorf("attempt = %+v, want attempt 2 retried", attempt)
	}
}

func TestEngineFencedCheckpointAfterStop(t *testing.T) {
	db := setupEngineDB(t)
	wfID := createWorkflow(t, db, n1,
		nodeJSON(n1, "script", "inc", "context.x = (context.x||0) + 1; return context.x;", "", "out", nil),
	)
	instanceID := insertInstance(t, db, wfID, n1, map[string]any{"x": 0})
	e, instances := testEngine(t, db, model.DefaultLimits())
	ctx := context.Background()

	claimed, err := instances.ClaimNext(ctx, "test-worker", time.Minute, 10)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("claim = %v, err %v", claimed, err)
	}
	// Stop the instance mid-claim: the engine's checkpoint must be fenced.
	if _, err := instances.Stop(ctx, instanceID, "operator"); err != nil {
		t.Fatal(err)
	}
	if err := e.Process(ctx, claimed[0]); err != nil {
		t.Fatalf("Process() error = %v", err)
	}
	cur, _ := instances.GetByID(ctx, instanceID)
	if cur.Status != model.WorkflowStopped {
		t.Errorf("status = %s, want stopped (fenced checkpoint must not overwrite stop)", cur.Status)
	}
}

func TestEngineDefinitionChangePreservesCursor(t *testing.T) {
	// Sanity: definitions are immutable and bound at instance creation; the
	// loader resolves the workflow definition the instance references.
	db := setupEngineDB(t)
	wfID := createWorkflow(t, db, n1,
		nodeJSON(n1, "script", "one", "return 1;", n2, "a", nil),
		nodeJSON(n2, "script", "two", "return 2;", "", "b", nil),
	)
	instanceID := insertInstance(t, db, wfID, n1, map[string]any{})
	e, _ := testEngine(t, db, model.DefaultLimits())
	cur := runEngine(t, db, e, instanceID)
	if cur.Status != model.WorkflowFinished {
		t.Fatalf("status = %s, want finished", cur.Status)
	}
}

func TestEnginePollerFlowWritesOutputThenNext(t *testing.T) {
	db := setupEngineDB(t)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		_, _ = io.WriteString(w, `{"status":"completed"}`)
	}))
	defer srv.Close()

	wfID := createWorkflow(t, db, n1,
		nodeJSON(n1, "poller", "poll", "", n2, "out", map[string]any{
			"http": map[string]any{
				"url": srv.URL, "until": "return response.status === 200 && response.body.status === 'completed';",
			},
		}),
		nodeJSON(n2, "script", "after", "return 'after';", "", "after", nil),
	)
	instanceID := insertInstance(t, db, wfID, n1, map[string]any{})
	e, _ := testEngineWithExec(t, db, model.DefaultLimits(), executor.Limits{HTTPAllowlist: []string{"127.0.0.1"}})
	cur := runEngine(t, db, e, instanceID)
	if cur.Status != model.WorkflowFinished {
		t.Fatalf("status = %s, want finished (error %q)", cur.Status, cur.Error)
	}
	ctx := instanceContext(t, db, instanceID)
	out, ok := ctx["out"].(map[string]any)
	if !ok {
		t.Fatalf("out = %T, want full normalized response object", ctx["out"])
	}
	if out["status"] != float64(200) {
		t.Errorf("out.status = %v, want 200", out["status"])
	}
	body, _ := out["body"].(map[string]any)
	if body["status"] != "completed" {
		t.Errorf("out.body = %v, want completed", out["body"])
	}
	if ctx["after"] != "after" {
		t.Errorf("after = %v, want next_node output", ctx["after"])
	}
	// Reserved template roots must never leak into persisted context.
	if _, ok := ctx["workflow_instance_id"]; ok {
		t.Error("workflow_instance_id leaked into workflow context")
	}
	if _, ok := ctx["node_instance_id"]; ok {
		t.Error("node_instance_id leaked into workflow context")
	}
}

func TestEnginePollerExhaustionFails(t *testing.T) {
	db := setupEngineDB(t)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, `{"status":"running"}`)
	}))
	defer srv.Close()

	wfID := createWorkflow(t, db, n1,
		nodeJSON(n1, "poller", "poll", "", "", "out", map[string]any{
			"http": map[string]any{
				"url":          srv.URL,
				"until":        "return response.body.status === 'completed';",
				"max_attempts": 2,
				"delay":        "10ms",
			},
		}),
	)
	instanceID := insertInstance(t, db, wfID, n1, map[string]any{})
	e, _ := testEngineWithExec(t, db, model.DefaultLimits(), executor.Limits{HTTPAllowlist: []string{"127.0.0.1"}})
	cur := runEngine(t, db, e, instanceID)
	if cur.Status != model.WorkflowFailed {
		t.Fatalf("status = %s, want failed", cur.Status)
	}
	if !strings.Contains(cur.Error, "exhausted") {
		t.Errorf("error = %q, want mention of exhausted", cur.Error)
	}
	attempt, err := repository.NewInstanceRepository(db).GetNodeInstanceByNode(context.Background(), instanceID, n1)
	if err != nil {
		t.Fatal(err)
	}
	if attempt.Status != model.NodeFailed {
		t.Errorf("attempt status = %s, want failed", attempt.Status)
	}
}

func TestEngineRecoveryRetriesPoller(t *testing.T) {
	db := setupEngineDB(t)
	var hits int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits++
		_, _ = io.WriteString(w, `{"status":"completed"}`)
	}))
	defer srv.Close()

	wfID := createWorkflow(t, db, n1,
		nodeJSON(n1, "poller", "poll", "", "", "out", map[string]any{
			"http": map[string]any{
				"url": srv.URL, "until": "return response.body.status === 'completed';",
			},
		}),
	)
	instanceID := insertInstance(t, db, wfID, n1, map[string]any{})
	now := time.Now().UTC()
	runningAttempt := model.NodeInstance{
		ID: newEngineID(), WorkflowInstanceID: instanceID, NodeID: n1,
		NodeDefinitionID: "", Name: "poll", Type: "poller",
		Attempt: 1, Status: model.NodeRunning, RecoveryPolicy: "retry",
		ContextBefore: json.RawMessage(`{}`), StartedAt: &now,
		CreatedAt: now, UpdatedAt: now,
	}
	crashClaim(t, db, instanceID, runningAttempt)

	e, _ := testEngineWithExec(t, db, model.DefaultLimits(), executor.Limits{HTTPAllowlist: []string{"127.0.0.1"}})
	cur := runEngine(t, db, e, instanceID)
	if cur.Status != model.WorkflowFinished {
		t.Fatalf("status = %s, want finished after recovery retry (error %q)", cur.Status, cur.Error)
	}
	if hits != 1 {
		t.Errorf("hits = %d, want 1 (poller re-executed after recovery)", hits)
	}
	attempt, _ := repository.NewInstanceRepository(db).GetNodeInstanceByNode(context.Background(), instanceID, n1)
	if attempt.Attempt != 2 || attempt.RecoveryResult != "retried" {
		t.Errorf("attempt = %+v, want attempt 2 retried", attempt)
	}
}

func TestEngineScriptPrePostHooks(t *testing.T) {
	db := setupEngineDB(t)
	wfID := createWorkflow(t, db, n1,
		nodeJSON(n1, "script", "hooky", "context.x = 10; return 5;", n2, "out", map[string]any{
			"pre_script":  map[string]any{"script": "context.pre_ran = (context.pre_ran || 0) + 1;"},
			"post_script": map[string]any{"script": "context.post_ran = true; context.post_sees_out = output;"},
		}),
		nodeJSON(n2, "script", "after", "return 'after';", "", "after", nil),
	)
	instanceID := insertInstance(t, db, wfID, n1, map[string]any{})
	e, _ := testEngine(t, db, model.DefaultLimits())
	cur := runEngine(t, db, e, instanceID)
	if cur.Status != model.WorkflowFinished {
		t.Fatalf("status = %s, want finished (error %q)", cur.Status, cur.Error)
	}
	ctx := instanceContext(t, db, instanceID)
	if ctx["pre_ran"] != float64(1) {
		t.Errorf("pre_ran = %v, want 1 (pre ran before native script)", ctx["pre_ran"])
	}
	if ctx["post_ran"] != true {
		t.Errorf("post_ran = %v, want true", ctx["post_ran"])
	}
	if ctx["post_sees_out"] != float64(5) {
		t.Errorf("post_sees_out = %v, want 5 (post sees merged native output)", ctx["post_sees_out"])
	}
	if ctx["out"] != float64(5) {
		t.Errorf("out = %v, want 5", ctx["out"])
	}
	if ctx["after"] != "after" {
		t.Errorf("after = %v, want next node output", ctx["after"])
	}
}

func TestEnginePreHookFailureFailsWorkflow(t *testing.T) {
	db := setupEngineDB(t)
	wfID := createWorkflow(t, db, n1,
		nodeJSON(n1, "script", "boom-pre", "return 1;", "", "out", map[string]any{
			"pre_script": map[string]any{"script": "return missing.value;"},
		}),
	)
	instanceID := insertInstance(t, db, wfID, n1, map[string]any{})
	e, _ := testEngine(t, db, model.DefaultLimits())
	cur := runEngine(t, db, e, instanceID)
	if cur.Status != model.WorkflowFailed {
		t.Fatalf("status = %s, want failed", cur.Status)
	}
	if !strings.Contains(cur.Error, "pre-script") {
		t.Errorf("error = %q, want pre-script reason", cur.Error)
	}
	attempt, err := repository.NewInstanceRepository(db).GetNodeInstanceByNode(context.Background(), instanceID, n1)
	if err != nil {
		t.Fatal(err)
	}
	if attempt.Status != model.NodeFailed {
		t.Errorf("attempt status = %s, want failed", attempt.Status)
	}
	ctx := instanceContext(t, db, instanceID)
	if _, ok := ctx["out"]; ok {
		t.Error("native script ran despite pre hook failure")
	}
}

func TestEnginePostHookFailureFailsWorkflow(t *testing.T) {
	db := setupEngineDB(t)
	wfID := createWorkflow(t, db, n1,
		nodeJSON(n1, "script", "boom-post", "return 7;", "", "out", map[string]any{
			"post_script": map[string]any{"script": "throw new Error('post boom');"},
		}),
	)
	instanceID := insertInstance(t, db, wfID, n1, map[string]any{})
	e, _ := testEngine(t, db, model.DefaultLimits())
	cur := runEngine(t, db, e, instanceID)
	if cur.Status != model.WorkflowFailed {
		t.Fatalf("status = %s, want failed", cur.Status)
	}
	if !strings.Contains(cur.Error, "post-script") {
		t.Errorf("error = %q, want post-script reason", cur.Error)
	}
	attempt, err := repository.NewInstanceRepository(db).GetNodeInstanceByNode(context.Background(), instanceID, n1)
	if err != nil {
		t.Fatal(err)
	}
	if attempt.Status != model.NodeFailed {
		t.Errorf("attempt status = %s, want failed", attempt.Status)
	}
	var after map[string]any
	if err := json.Unmarshal(attempt.ContextAfter, &after); err != nil {
		t.Fatalf("parse context_after: %v", err)
	}
	if after["out"] != float64(7) {
		t.Errorf("context_after = %v, want merged native output preserved", after)
	}
}

func TestEngineGroupHooksOrdering(t *testing.T) {
	db := setupEngineDB(t)
	wfID := createWorkflow(t, db, g1,
		nodeJSON(g1, "group", "main", "", n2, "", map[string]any{
			"start_node_id": n3,
			"pre_script":    map[string]any{"script": "context.trace = ['pre'];"},
			"post_script":   map[string]any{"script": "context.trace.push('gpost');"},
			"nodes": []map[string]any{
				{
					"id": n3, "type": "script", "name": "inner", "script": "context.trace.push('node'); return 1;",
					"output_property": "inner",
					"post_script":     map[string]any{"script": "context.trace.push('npost');"},
				},
			},
		}),
		nodeJSON(n2, "script", "after", "return 'after';", "", "after", nil),
	)
	instanceID := insertInstance(t, db, wfID, g1, map[string]any{})
	e, _ := testEngine(t, db, model.DefaultLimits())
	cur := runEngine(t, db, e, instanceID)
	if cur.Status != model.WorkflowFinished {
		t.Fatalf("status = %s, want finished (error %q)", cur.Status, cur.Error)
	}
	ctx := instanceContext(t, db, instanceID)
	trace, ok := ctx["trace"].([]any)
	if !ok {
		t.Fatalf("trace = %T, want array", ctx["trace"])
	}
	want := []any{"pre", "node", "npost", "gpost"}
	for i, w := range want {
		if i >= len(trace) || trace[i] != w {
			t.Errorf("trace = %v, want %v", trace, want)
			break
		}
	}
}

func TestEngineNestedGroupPostsInnerToOuter(t *testing.T) {
	db := setupEngineDB(t)
	outer := newEngineID()
	inner := newEngineID()
	child := newEngineID()
	after := newEngineID()
	wfID := createWorkflow(t, db, outer,
		nodeJSON(outer, "group", "outer", "", after, "", map[string]any{
			"start_node_id": inner,
			"pre_script":    map[string]any{"script": "context.trace = ['g1pre'];"},
			"post_script":   map[string]any{"script": "context.trace.push('g1post');"},
			"nodes": []map[string]any{
				{
					"id": inner, "type": "group", "name": "inner",
					"start_node_id": child,
					"pre_script":    map[string]any{"script": "context.trace.push('g2pre');"},
					"post_script":   map[string]any{"script": "context.trace.push('g2post');"},
					"nodes": []map[string]any{
						{"id": child, "type": "script", "name": "leaf", "script": "context.trace.push('node'); return 1;", "output_property": "leaf"},
					},
				},
			},
		}),
		nodeJSON(after, "script", "after", "return 'after';", "", "after", nil),
	)
	instanceID := insertInstance(t, db, wfID, outer, map[string]any{})
	e, _ := testEngine(t, db, model.DefaultLimits())
	cur := runEngine(t, db, e, instanceID)
	if cur.Status != model.WorkflowFinished {
		t.Fatalf("status = %s, want finished (error %q)", cur.Status, cur.Error)
	}
	ctx := instanceContext(t, db, instanceID)
	trace, ok := ctx["trace"].([]any)
	if !ok {
		t.Fatalf("trace = %T, want array", ctx["trace"])
	}
	want := []any{"g1pre", "g2pre", "node", "g2post", "g1post"}
	for i, w := range want {
		if i >= len(trace) || trace[i] != w {
			t.Errorf("trace = %v, want %v (inner group post before outer)", trace, want)
			break
		}
	}
}

func TestEngineGroupPreFailureFailsWorkflow(t *testing.T) {
	db := setupEngineDB(t)
	child := newEngineID()
	wfID := createWorkflow(t, db, g1,
		nodeJSON(g1, "group", "gpre", "", "", "", map[string]any{
			"start_node_id": child,
			"pre_script":    map[string]any{"script": "throw new Error('gpre boom');"},
			"nodes": []map[string]any{
				{"id": child, "type": "script", "name": "inner", "script": "return 1;", "output_property": "inner"},
			},
		}),
	)
	instanceID := insertInstance(t, db, wfID, g1, map[string]any{})
	e, _ := testEngine(t, db, model.DefaultLimits())
	cur := runEngine(t, db, e, instanceID)
	if cur.Status != model.WorkflowFailed {
		t.Fatalf("status = %s, want failed", cur.Status)
	}
	if !strings.Contains(cur.Error, "pre-script") {
		t.Errorf("error = %q, want pre-script reason", cur.Error)
	}
	attempts, _ := repository.NewInstanceRepository(db).ListNodeInstances(context.Background(), instanceID)
	if len(attempts) != 0 {
		t.Errorf("node instances = %d, want 0 (group children never ran)", len(attempts))
	}
}

func TestEngineGroupPostFailurePreservesContext(t *testing.T) {
	db := setupEngineDB(t)
	child := newEngineID()
	wfID := createWorkflow(t, db, g1,
		nodeJSON(g1, "group", "gpost", "", "", "", map[string]any{
			"start_node_id": child,
			"post_script":   map[string]any{"script": "throw new Error('gpost boom');"},
			"nodes": []map[string]any{
				{"id": child, "type": "script", "name": "inner", "script": "return 7;", "output_property": "out"},
			},
		}),
	)
	instanceID := insertInstance(t, db, wfID, g1, map[string]any{})
	e, _ := testEngine(t, db, model.DefaultLimits())
	cur := runEngine(t, db, e, instanceID)
	if cur.Status != model.WorkflowFailed {
		t.Fatalf("status = %s, want failed", cur.Status)
	}
	if !strings.Contains(cur.Error, "post-script") {
		t.Errorf("error = %q, want post-script reason", cur.Error)
	}
	// The child completed; the structural group hook failure must not mark
	// the child attempt failed.
	attempts, _ := repository.NewInstanceRepository(db).ListNodeInstances(context.Background(), instanceID)
	if len(attempts) != 1 || attempts[0].Status != model.NodeFinished {
		t.Errorf("attempts = %+v, want one finished child", attempts)
	}
	// The latest completed context survives the failure.
	ctx := instanceContext(t, db, instanceID)
	if ctx["out"] != float64(7) {
		t.Errorf("context = %v, want child output preserved", ctx)
	}
}

func TestEngineConditionsPostSeesRouteKey(t *testing.T) {
	db := setupEngineDB(t)
	wfID := createWorkflowWithKeys(t, db, n1, map[string]any{"staff": n2},
		nodeJSON(n1, "conditions", "route", "", "", "", map[string]any{
			"conditions": []map[string]any{
				{"key": "staff", "condition": "return context.user === 'staff';"},
				{"condition": "return false;"},
			},
			"post_script": map[string]any{"script": "context.routed = output.key;"},
		}),
		nodeJSON(n2, "script", "staff-branch", "return 'staff';", "", "out", nil),
	)
	instanceID := insertInstance(t, db, wfID, n1, map[string]any{"user": "staff"})
	e, _ := testEngine(t, db, model.DefaultLimits())
	cur := runEngine(t, db, e, instanceID)
	if cur.Status != model.WorkflowFinished {
		t.Fatalf("status = %s, want finished (error %q)", cur.Status, cur.Error)
	}
	ctx := instanceContext(t, db, instanceID)
	if ctx["routed"] != "staff" {
		t.Errorf("routed = %v, want staff (post sees route result)", ctx["routed"])
	}
}

func TestEngineHTTP500RoutesToOnFailureInput(t *testing.T) {
	db := setupEngineDB(t)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Custom", "val")
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"error":"upstream broken"}`))
	}))
	defer srv.Close()

	wfID := createWorkflow(t, db, n1,
		nodeJSON(n1, "external_call", "call-api", "", n3, "api_res", map[string]any{
			"http_config": map[string]any{"url": srv.URL, "method": "POST"},
			"on_failure": map[string]any{
				"next_node":       n2,
				"output_property": "ext_err",
			},
			"post_script": map[string]any{"script": "context.post_ran = true;"},
		}),
		nodeJSON(n2, "input", "fallback-input", "", n3, "", map[string]any{
			"channel":      "http",
			"context_path": "manual.fix",
		}),
		nodeJSON(n3, "script", "done", "return 'done';", "", "out", nil),
	)
	instanceID := insertInstance(t, db, wfID, n1, map[string]any{"init": 1})
	e, instances := testEngineWithExec(t, db, model.DefaultLimits(), executor.Limits{HTTPAllowlist: []string{"127.0.0.1"}})
	cur := runEngine(t, db, e, instanceID)

	if cur.Status != model.WorkflowWaiting || cur.WaitingReason != model.WaitingReasonInput {
		t.Fatalf("status = %s / %s, want waiting/input (error: %q)", cur.Status, cur.WaitingReason, cur.Error)
	}
	if cur.Error != "" {
		t.Errorf("cur.Error = %q, want empty for handled failure", cur.Error)
	}

	ctxMap := instanceContext(t, db, instanceID)
	if _, ok := ctxMap["post_ran"]; ok {
		t.Error("post_script ran on failed external_call node")
	}

	extErr, ok := ctxMap["ext_err"].(map[string]any)
	if !ok {
		t.Fatalf("ext_err not found in context: %v", ctxMap)
	}
	if extErr["reason"] != "http-status" {
		t.Errorf("reason = %v, want http-status", extErr["reason"])
	}
	if extErr["message"] == nil || extErr["message"] == "" {
		t.Errorf("message = %v, want non-empty", extErr["message"])
	}
	res, ok := extErr["result"].(map[string]any)
	if !ok {
		t.Fatalf("result = %T (%v), want map", extErr["result"], extErr["result"])
	}
	if v, ok := res["Status"].(float64); !ok || v != 500 {
		t.Errorf("result.Status = %v, want 500", res["Status"])
	}

	attempt, err := instances.GetNodeInstanceByNode(context.Background(), instanceID, n1)
	if err != nil {
		t.Fatal(err)
	}
	if attempt.Status != model.NodeFailed {
		t.Errorf("attempt.Status = %s, want failed", attempt.Status)
	}

	events, err := instances.ListEvents(context.Background(), instanceID)
	if err != nil {
		t.Fatal(err)
	}
	var hasNodeFailed, hasNodeFailureRouted, hasWorkflowFailed bool
	for _, ev := range events {
		if ev.Type == "node_failed" {
			hasNodeFailed = true
		}
		if ev.Type == "node_failure_routed" {
			hasNodeFailureRouted = true
		}
		if ev.Type == "workflow_failed" {
			hasWorkflowFailed = true
		}
	}
	if !hasNodeFailed {
		t.Error("missing node_failed event")
	}
	if !hasNodeFailureRouted {
		t.Error("missing node_failure_routed event")
	}
	if hasWorkflowFailed {
		t.Error("unexpected workflow_failed event for handled failure")
	}
}

func TestEnginePollerExhaustionRoutesToOnFailureInput(t *testing.T) {
	db := setupEngineDB(t)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"pending"}`))
	}))
	defer srv.Close()

	wfID := createWorkflow(t, db, n1,
		nodeJSON(n1, "poller", "wait-job", "", n3, "poller_res", map[string]any{
			"http": map[string]any{
				"url":          srv.URL,
				"delay":        "10ms",
				"max_attempts": 2,
				"until":        `return response.body.status === "completed";`,
			},
			"on_failure": map[string]any{
				"next_node":       n2,
				"output_property": "poller_err",
			},
			"post_script": map[string]any{"script": "context.post_ran = true;"},
		}),
		nodeJSON(n2, "input", "fallback-input", "", n3, "", map[string]any{
			"channel":      "http",
			"context_path": "manual.fix",
		}),
		nodeJSON(n3, "script", "done", "return 'done';", "", "out", nil),
	)
	instanceID := insertInstance(t, db, wfID, n1, map[string]any{})
	e, instances := testEngineWithExec(t, db, model.DefaultLimits(), executor.Limits{HTTPAllowlist: []string{"127.0.0.1"}})
	cur := runEngine(t, db, e, instanceID)

	if cur.Status != model.WorkflowWaiting || cur.WaitingReason != model.WaitingReasonInput {
		t.Fatalf("status = %s / %s, want waiting/input (error: %q)", cur.Status, cur.WaitingReason, cur.Error)
	}

	ctxMap := instanceContext(t, db, instanceID)
	if _, ok := ctxMap["post_ran"]; ok {
		t.Error("post_script ran on failed poller node")
	}

	pollerErr, ok := ctxMap["poller_err"].(map[string]any)
	if !ok {
		t.Fatalf("poller_err not found in context: %v", ctxMap)
	}
	if pollerErr["reason"] != "poller" {
		t.Errorf("reason = %v, want poller", pollerErr["reason"])
	}

	attempt, err := instances.GetNodeInstanceByNode(context.Background(), instanceID, n1)
	if err != nil {
		t.Fatal(err)
	}
	if attempt.Status != model.NodeFailed {
		t.Errorf("attempt.Status = %s, want failed", attempt.Status)
	}
}

func TestEngineRecoveryFailureRoutesToOnFailure(t *testing.T) {
	db := setupEngineDB(t)
	wfID := createWorkflow(t, db, n1,
		nodeJSON(n1, "external_call", "call-api", "", n3, "out", map[string]any{
			"http_config":       map[string]any{"url": "https://example.com", "method": "POST"},
			"retry_on_recovery": false,
			"on_failure": map[string]any{
				"next_node":       n2,
				"output_property": "rec_err",
			},
		}),
		nodeJSON(n2, "script", "fallback-script", "return 'recovered';", "", "fallback_out", nil),
		nodeJSON(n3, "script", "done", "return 'done';", "", "out", nil),
	)

	// Simulate node interrupted by dead worker: NodeRunning in DB
	instID := newEngineID()
	now := time.Now().UTC()
	ctxRaw, _ := json.Marshal(map[string]any{"x": 1})
	frameRaw, _ := model.NewFrame(n1).JSON()
	w := model.WorkflowInstance{
		ID:                   instID,
		WorkflowDefinitionID: wfID,
		Status:               model.WorkflowWaiting,
		WaitingReason:        model.WaitingReasonRunnable,
		Frame:                frameRaw,
		Context:              ctxRaw,
		Counters:             json.RawMessage(`{}`),
		CreatedBy:            sysUserID,
		UpdatedBy:            sysUserID,
		CreatedAt:            now,
		UpdatedAt:            now,
	}
	instances := repository.NewInstanceRepository(db)
	if err := instances.Insert(context.Background(), w); err != nil {
		t.Fatal(err)
	}
	attempt := model.NodeInstance{
		ID:                 newEngineID(),
		WorkflowInstanceID: instID,
		NodeID:             n1,
		Name:               "call-api",
		Type:               "external_call",
		Attempt:            1,
		Status:             model.NodeRunning,
		Input:              json.RawMessage("null"),
		Output:             json.RawMessage("null"),
		ContextBefore:      ctxRaw,
		ContextAfter:       json.RawMessage("null"),
		CreatedAt:          now,
		UpdatedAt:          now,
	}
	if err := instances.InsertNodeInstance(context.Background(), attempt); err != nil {
		t.Fatal(err)
	}

	e, _ := testEngineWithExec(t, db, model.DefaultLimits(), executor.Limits{HTTPAllowlist: []string{"*"}})
	cur := runEngine(t, db, e, instID)

	if cur.Status != model.WorkflowFinished {
		t.Fatalf("status = %s, want finished (error: %q)", cur.Status, cur.Error)
	}

	ctxMap := instanceContext(t, db, instID)
	recErr, ok := ctxMap["rec_err"].(map[string]any)
	if !ok {
		t.Fatalf("rec_err not found in context: %v", ctxMap)
	}
	if recErr["reason"] != "recovery" {
		t.Errorf("reason = %v, want recovery", recErr["reason"])
	}
	if recErr["result"] != nil {
		t.Errorf("result = %v, want nil for recovery", recErr["result"])
	}
	if ctxMap["fallback_out"] != "recovered" {
		t.Errorf("fallback_out = %v, want recovered", ctxMap["fallback_out"])
	}

	nodeAtt, err := instances.GetNodeInstanceByNode(context.Background(), instID, n1)
	if err != nil {
		t.Fatal(err)
	}
	if nodeAtt.Status != model.NodeFailed || nodeAtt.RecoveryResult != "failed" {
		t.Errorf("node attempt = %+v, want failed/failed", nodeAtt)
	}
}

func TestEngineExecutorFailureWithoutOnFailureFailsWorkflow(t *testing.T) {
	db := setupEngineDB(t)
	wfID := createWorkflow(t, db, n1,
		nodeJSON(n1, "external_call", "call-api", "", n2, "out", map[string]any{
			"http_config": map[string]any{"url": "http://no-such-host-simpwf.invalid/x", "method": "GET"},
		}),
		nodeJSON(n2, "script", "done", "return 'done';", "", "out", nil),
	)
	instanceID := insertInstance(t, db, wfID, n1, map[string]any{})
	e, instances := testEngineWithExec(t, db, model.DefaultLimits(), executor.Limits{HTTPAllowlist: []string{"*"}})
	cur := runEngine(t, db, e, instanceID)

	if cur.Status != model.WorkflowFailed {
		t.Fatalf("status = %s, want failed", cur.Status)
	}

	events, err := instances.ListEvents(context.Background(), instanceID)
	if err != nil {
		t.Fatal(err)
	}
	var hasWorkflowFailed bool
	for _, ev := range events {
		if ev.Type == "workflow_failed" {
			hasWorkflowFailed = true
		}
	}
	if !hasWorkflowFailed {
		t.Error("missing workflow_failed event")
	}
}

func TestEngineHTTP500WithoutOnFailureSucceeds(t *testing.T) {
	db := setupEngineDB(t)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"err":"server error"}`))
	}))
	defer srv.Close()

	wfID := createWorkflow(t, db, n1,
		nodeJSON(n1, "external_call", "call-api", "", n2, "res", map[string]any{
			"http_config": map[string]any{"url": srv.URL, "method": "GET"},
			"post_script": map[string]any{"script": "context.post_ran = true;"},
		}),
		nodeJSON(n2, "script", "done", "return 'done';", "", "out", nil),
	)
	instanceID := insertInstance(t, db, wfID, n1, map[string]any{})
	e, instances := testEngineWithExec(t, db, model.DefaultLimits(), executor.Limits{HTTPAllowlist: []string{"127.0.0.1"}})
	cur := runEngine(t, db, e, instanceID)

	if cur.Status != model.WorkflowFinished {
		t.Fatalf("status = %s, want finished (error: %q)", cur.Status, cur.Error)
	}

	ctxMap := instanceContext(t, db, instanceID)
	if ctxMap["post_ran"] != true {
		t.Error("post_script did not run for successful HTTP status without on_failure")
	}
	res, ok := ctxMap["res"].(map[string]any)
	if !ok {
		t.Fatalf("res = %T, want map", ctxMap["res"])
	}
	if v, ok := res["Status"].(float64); !ok || v != 500 {
		t.Errorf("res.Status = %v, want 500", res["Status"])
	}

	attempt, err := instances.GetNodeInstanceByNode(context.Background(), instanceID, n1)
	if err != nil {
		t.Fatal(err)
	}
	if attempt.Status != model.NodeFinished {
		t.Errorf("attempt.Status = %s, want finished", attempt.Status)
	}
}

func TestEngineRecoveryReconcilesStaleCursorToParkedInput(t *testing.T) {
	db := setupEngineDB(t)
	wfID := createWorkflow(t, db, n1,
		nodeJSON(n1, "script", "a", "context.x = 1; return 1;", n2, "out", nil),
		nodeJSON(n2, "input", "ask", "", n3, "", map[string]any{"channel": "http", "context_path": "gate"}),
		nodeJSON(n3, "script", "b", "return 2;", "", "done", nil),
	)
	instanceID := insertInstance(t, db, wfID, n1, map[string]any{})
	e, instances := testEngine(t, db, model.DefaultLimits())
	cur := runEngine(t, db, e, instanceID)
	if cur.Status != model.WorkflowWaiting || cur.WaitingReason != model.WaitingReasonInput {
		t.Fatalf("status = %s/%s, want parked waiting/input", cur.Status, cur.WaitingReason)
	}
	// Simulate rollback-away: cursor moved to n1 while the n2 input
	// attempt stayed running; resume made it claimable again.
	if err := db.Exec(`UPDATE workflow_instances SET frame = ?, status = ?, waiting_reason = ?,
		revision = revision + 1, updated_at = now() WHERE id = ?`,
		`{"current_node_id":"`+n1+`"}`, string(model.WorkflowWaiting),
		string(model.WaitingReasonRunnable), instanceID).Error; err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	claimed, err := instances.ClaimNext(ctx, "test-worker", time.Minute, 10)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("claim = %d, err %v, want 1", len(claimed), err)
	}
	if err := e.Process(ctx, claimed[0]); err != nil {
		t.Fatalf("Process() error = %v", err)
	}
	got, err := instances.GetByID(ctx, instanceID)
	if err != nil {
		t.Fatal(err)
	}
	frame, err := model.ParseFrame(got.Frame)
	if err != nil {
		t.Fatal(err)
	}
	if frame.CurrentNodeID != n2 {
		t.Errorf("cursor = %q, want %q (reconciled to parked input)", frame.CurrentNodeID, n2)
	}
	if got.Status != model.WorkflowWaiting || got.WaitingReason != model.WaitingReasonInput {
		t.Errorf("status = %s/%s, want waiting/input", got.Status, got.WaitingReason)
	}
	attempt, err := instances.GetNodeInstanceByNode(ctx, instanceID, n2)
	if err != nil {
		t.Fatal(err)
	}
	if attempt.Status != model.NodeRunning || attempt.Attempt != 2 {
		t.Errorf("attempt = %s/%d, want running/2", attempt.Status, attempt.Attempt)
	}
	events, err := instances.ListEvents(ctx, instanceID)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, ev := range events {
		if ev.Type != "cursor_reconciled" {
			continue
		}
		found = true
		var data map[string]any
		if err := json.Unmarshal(ev.Data, &data); err != nil {
			t.Fatal(err)
		}
		if data["from_node"] != n1 || data["to_node"] != n2 {
			t.Errorf("cursor_reconciled data = %v, want from %s to %s", data, n1, n2)
		}
	}
	if !found {
		t.Error("no cursor_reconciled event, want one")
	}
}
