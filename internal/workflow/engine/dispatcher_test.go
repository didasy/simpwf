package engine_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/simpwf/workflow-engine/internal/workflow/engine"
	"github.com/simpwf/workflow-engine/internal/workflow/executor"
	"github.com/simpwf/workflow-engine/internal/workflow/model"
	"github.com/simpwf/workflow-engine/internal/workflow/repository"
	"gorm.io/gorm"
)

func allowAll() executor.Limits {
	return executor.Limits{HTTPAllowlist: []string{"*"}}
}

func dispatcherFor(t *testing.T, db *gorm.DB, limits model.Limits, worker string, opts engine.DispatcherOptions, execLimits executor.Limits) *engine.Dispatcher {
	t.Helper()
	e, _ := testEngineWithExec(t, db, limits, execLimits)
	d, err := engine.NewDispatcher(context.Background(), e, repository.NewInstanceRepository(db), worker, opts)
	if err != nil {
		t.Fatalf("NewDispatcher() error = %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := d.Shutdown(ctx); err != nil {
			t.Errorf("Shutdown() error = %v", err)
		}
	})
	return d
}

func waitStatus(t *testing.T, db *gorm.DB, instanceID string, want ...model.WorkflowStatus) model.WorkflowInstance {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		cur, err := repository.NewInstanceRepository(db).GetByID(context.Background(), instanceID)
		if err != nil {
			t.Fatal(err)
		}
		for _, w := range want {
			if cur.Status == w {
				return *cur
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("instance %s did not reach %v", instanceID, want)
	return model.WorkflowInstance{}
}

func shortDispatcherOpts() engine.DispatcherOptions {
	return engine.DispatcherOptions{
		PollInterval: 20 * time.Millisecond,
		Lease:        30 * time.Second,
		BatchSize:    10,
		PoolSize:     4,
		Heartbeat:    200 * time.Millisecond,
	}
}

func TestDispatcherCompletesWorkflow(t *testing.T) {
	db := setupEngineDB(t)
	wfID := createWorkflow(t, db, n1,
		nodeJSON(n1, "script", "one", "context.seen = (context.seen||0)+1; return context.seen;", n2, "one", nil),
		nodeJSON(n2, "script", "two", "return 'done';", "", "two", nil),
	)
	instanceID := insertInstance(t, db, wfID, n1, map[string]any{})
	d := dispatcherFor(t, db, model.DefaultLimits(), "d1", shortDispatcherOpts(), executor.Limits{})
	d.Run()

	cur := waitStatus(t, db, instanceID, model.WorkflowFinished)
	if cur.Status != model.WorkflowFinished {
		t.Fatalf("status = %s", cur.Status)
	}
	inst := instanceContext(t, db, instanceID)
	if inst["one"] != float64(1) || inst["two"] != "done" {
		t.Errorf("context = %v", inst)
	}
}

func TestMultiDispatcherNoDuplicateHTTP(t *testing.T) {
	db := setupEngineDB(t)
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
	}))
	defer srv.Close()

	wfID := createWorkflow(t, db, n1,
		nodeJSON(n1, "script", "pre", "return 'pre';", n2, "pre", nil),
		nodeJSON(n2, "external_call", "http", "", n3, "http_out", map[string]any{
			"http_config": map[string]any{"url": srv.URL, "method": "GET"},
		}),
		nodeJSON(n3, "script", "post", "return 'post';", "", "post", nil),
	)
	instanceID := insertInstance(t, db, wfID, n1, map[string]any{})

	d1 := dispatcherFor(t, db, model.DefaultLimits(), "d1", shortDispatcherOpts(), allowAll())
	d2 := dispatcherFor(t, db, model.DefaultLimits(), "d2", shortDispatcherOpts(), allowAll())
	d1.Run()
	d2.Run()

	cur := waitStatus(t, db, instanceID, model.WorkflowFinished)
	if cur.Status != model.WorkflowFinished {
		t.Fatalf("status = %s", cur.Status)
	}
	// Concurrent dispatchers must not double-execute nodes: SKIP LOCKED
	// claims plus lease fencing guarantee a single execution.
	if hits.Load() != 1 {
		t.Errorf("http hits = %d, want exactly 1", hits.Load())
	}
	attempts, _ := repository.NewInstanceRepository(db).ListNodeInstances(context.Background(), instanceID)
	if len(attempts) != 3 {
		t.Errorf("node instances = %d, want 3", len(attempts))
	}
	for _, a := range attempts {
		if a.Attempt != 1 {
			t.Errorf("attempt %s = %d, want 1", a.NodeID, a.Attempt)
		}
	}
}

func TestDispatcherRecoversExpiredLease(t *testing.T) {
	db := setupEngineDB(t)
	wfID := createWorkflow(t, db, n1,
		nodeJSON(n1, "script", "inc", "context.x = (context.x||0)+1; return context.x;", "", "out", nil),
	)
	instanceID := insertInstance(t, db, wfID, n1, map[string]any{"x": 0})

	// A dead worker claimed the instance and left its attempt running.
	now := time.Now().UTC()
	runningAttempt := model.NodeInstance{
		ID: newEngineID(), WorkflowInstanceID: instanceID, NodeID: n1,
		NodeDefinitionID: "", Name: "inc", Type: "script",
		Attempt: 1, Status: model.NodeRunning,
		ContextBefore: json.RawMessage(`{}`), StartedAt: &now,
		CreatedAt: now, UpdatedAt: now,
	}
	crashClaim(t, db, instanceID, runningAttempt)

	d := dispatcherFor(t, db, model.DefaultLimits(), "d-recovery", shortDispatcherOpts(), executor.Limits{})
	d.Run()

	cur := waitStatus(t, db, instanceID, model.WorkflowFinished)
	if cur.Status != model.WorkflowFinished {
		t.Fatalf("status = %s", cur.Status)
	}
	attempt, err := repository.NewInstanceRepository(db).GetNodeInstanceByNode(context.Background(), instanceID, n1)
	if err != nil {
		t.Fatal(err)
	}
	if attempt.Attempt != 2 || attempt.RecoveryResult != "retried" {
		t.Errorf("attempt = %+v, want attempt 2 with recovery retried", attempt)
	}
}

func TestDispatcherShutdown(t *testing.T) {
	db := setupEngineDB(t)
	d := dispatcherFor(t, db, model.DefaultLimits(), "d-shutdown", shortDispatcherOpts(), executor.Limits{})
	d.Run()
	time.Sleep(100 * time.Millisecond) // let loops start
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := d.Shutdown(ctx); err != nil {
		t.Errorf("Shutdown() error = %v", err)
	}
}
