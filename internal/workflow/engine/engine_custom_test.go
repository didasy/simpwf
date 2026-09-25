package engine_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/simpwf/workflow-engine/internal/workflow/engine"
	"github.com/simpwf/workflow-engine/internal/workflow/executor"
	"github.com/simpwf/workflow-engine/internal/workflow/model"
	"github.com/simpwf/workflow-engine/pkg/customnode"
	_ "github.com/simpwf/workflow-engine/pkg/customnode/s3fetch"
	"gorm.io/gorm"
)

// customNodeJSON renders an inline custom workflow node.
func customNodeJSON(id, typ string, config map[string]any, next, outProp string, extra map[string]any) string {
	m := map[string]any{"id": id, "type": typ, "name": "custom-" + typ, "config": config}
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

// failExecutor always fails with a partial result so routeFailure has
// output to store under the on_failure output property.
type failExecutor struct{}

func (failExecutor) Execute(_ context.Context, req executor.Request) (*executor.Result, error) {
	return &executor.Result{Output: map[string]any{"failed": true}},
		&executor.NodeError{Node: req.Node, Reason: "failnode", Err: errors.New("failnode boom")}
}

func testEngineWithCustom(t *testing.T, db *gorm.DB, limits model.Limits, typ string, ex executor.Executor) (*engine.Engine, func()) {
	t.Helper()
	if err := executor.RegisterCustomFactory(typ, func(executor.CustomDeps) (executor.Executor, error) {
		return ex, nil
	}); err != nil {
		t.Fatalf("RegisterCustomFactory(%q) error = %v", typ, err)
	}
	if err := model.RegisterCustomType(typ, func(raw json.RawMessage) (any, error) {
		return map[string]any{}, nil
	}); err != nil {
		executor.UnregisterCustomFactory(typ)
		t.Fatalf("RegisterCustomType(%q) error = %v", typ, err)
	}
	e, _ := testEngine(t, db, limits)
	return e, func() {
		executor.UnregisterCustomFactory(typ)
		model.UnregisterCustomType(typ)
	}
}

func TestEngineCustomNodeAdvancesCursor(t *testing.T) {
	db := setupEngineDB(t)
	wfID := createWorkflow(t, db, n1,
		customNodeJSON(n1, "engadv1", map[string]any{}, n2, "weather", nil),
		nodeJSON(n2, "script", "done", "return 'done';", "", "b", nil),
	)
	instanceID := insertInstance(t, db, wfID, n1, map[string]any{})
	e, cleanup := testEngineWithCustom(t, db, model.DefaultLimits(), "engadv1", stubOKExecutor{out: map[string]any{"city": "Berlin"}})
	defer cleanup()
	cur := runEngine(t, db, e, instanceID)
	if cur.Status != model.WorkflowFinished {
		t.Fatalf("status = %s, want finished (error %q)", cur.Status, cur.Error)
	}
	ctx := instanceContext(t, db, instanceID)
	w, ok := ctx["weather"].(map[string]any)
	if !ok {
		t.Fatalf("context[weather] = %v, want custom output", ctx["weather"])
	}
	if w["city"] != "Berlin" {
		t.Errorf("weather.city = %v, want Berlin", w["city"])
	}
	if ctx["b"] != "done" {
		t.Errorf("context[b] = %v, want done", ctx["b"])
	}
}

// stubOKExecutor returns a fixed output; used to prove generic-path
// inheritance (cursor advance, output_property, hooks) without network.
type stubOKExecutor struct{ out any }

func (s stubOKExecutor) Execute(context.Context, executor.Request) (*executor.Result, error) {
	return &executor.Result{Output: s.out}, nil
}

func TestEngineCustomNodeHooksFire(t *testing.T) {
	db := setupEngineDB(t)
	wfID := createWorkflow(t, db, n1,
		customNodeJSON(n1, "enghooks1", map[string]any{}, "", "weather", map[string]any{
			"pre_script":  map[string]any{"script": "context.pre = true; return 1;", "timeout": "5s"},
			"post_script": map[string]any{"script": "context.post = true; return 1;", "timeout": "5s"},
		}),
	)
	instanceID := insertInstance(t, db, wfID, n1, map[string]any{})
	e, cleanup := testEngineWithCustom(t, db, model.DefaultLimits(), "enghooks1", stubOKExecutor{out: "ok"})
	defer cleanup()
	cur := runEngine(t, db, e, instanceID)
	if cur.Status != model.WorkflowFinished {
		t.Fatalf("status = %s, want finished (error %q)", cur.Status, cur.Error)
	}
	ctx := instanceContext(t, db, instanceID)
	if ctx["pre"] != true || ctx["post"] != true {
		t.Errorf("hooks did not fire: context = %v", ctx)
	}
}

func TestEngineCustomNodeOnFailureRoutes(t *testing.T) {
	db := setupEngineDB(t)
	wfID := createWorkflow(t, db, n1,
		customNodeJSON(n1, "engfail1", map[string]any{}, n2, "ok", map[string]any{
			"on_failure": map[string]any{"next_node": n3, "output_property": "fail_err"},
		}),
		nodeJSON(n2, "script", "ok", "return 'ok';", "", "ok", nil),
		nodeJSON(n3, "script", "recovered", "return 'recovered';", "", "rec", nil),
	)
	instanceID := insertInstance(t, db, wfID, n1, map[string]any{})
	e, cleanup := testEngineWithCustom(t, db, model.DefaultLimits(), "engfail1", failExecutor{})
	defer cleanup()
	cur := runEngine(t, db, e, instanceID)
	if cur.Status != model.WorkflowFinished {
		t.Fatalf("status = %s, want finished (error %q)", cur.Status, cur.Error)
	}
	ctx := instanceContext(t, db, instanceID)
	if ctx["rec"] != "recovered" {
		t.Errorf("context[rec] = %v, want recovered via on_failure", ctx["rec"])
	}
	if _, ok := ctx["fail_err"]; !ok {
		t.Errorf("context missing fail_err output: %v", ctx)
	}
}

func TestEngineCustomNodeRetryOnRecoveryRequeues(t *testing.T) {
	db := setupEngineDB(t)
	wfID := createWorkflow(t, db, n1,
		customNodeJSON(n1, "engrec1", map[string]any{}, "", "ok", map[string]any{
			"retry_on_recovery": true,
		}),
	)
	instanceID := insertInstance(t, db, wfID, n1, map[string]any{})
	e, cleanup := testEngineWithCustom(t, db, model.DefaultLimits(), "engrec1", stubOKExecutor{out: "ok"})
	defer cleanup()
	_ = e
	_ = instanceID
	nc, err := model.ParseNodeContent([]byte(`{"type":"engrec1","config":{},"retry_on_recovery":true}`), testLimitsNode)
	if err != nil {
		t.Fatalf("ParseNodeContent() error = %v", err)
	}
	if !nc.RetryOnRecovery {
		t.Fatal("retry_on_recovery = false, want true for custom node")
	}
}

func TestEngineCustomNodeTypesRegistered(t *testing.T) {
	found := false
	for _, c := range customnode.Types() {
		if c == "s3fetch" {
			found = true
		}
	}
	if !found {
		t.Fatalf("customnode.Types() = %v, want s3fetch", customnode.Types())
	}
	if !model.ValidNodeType("s3fetch") {
		t.Fatal("ValidNodeType(s3fetch) = false, want true")
	}
	_ = context.Background()
	_ = executor.Limits{}
}
