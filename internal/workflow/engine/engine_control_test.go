package engine_test

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/simpwf/workflow-engine/internal/workflow/engine"
	"github.com/simpwf/workflow-engine/internal/workflow/executor"
	"github.com/simpwf/workflow-engine/internal/workflow/model"
)

func TestEngineDeferredPauseParksPaused(t *testing.T) {
	db := setupEngineDB(t)
	wfID := createWorkflow(t, db, n1,
		nodeJSON(n1, "script", "inc", "return 1;", n2, "out", nil),
		nodeJSON(n2, "script", "done", "return 2;", "", "out2", nil),
	)
	instanceID := insertInstance(t, db, wfID, n1, map[string]any{})
	e, instances := testEngine(t, db, model.DefaultLimits())
	ctx := context.Background()

	claimed, err := instances.ClaimNext(ctx, "worker-1", time.Minute, 10)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("claim = %d, err %v", len(claimed), err)
	}
	deferred, err := instances.Pause(ctx, instanceID)
	if err != nil || !deferred {
		t.Fatalf("Pause(running) = deferred %v, err %v, want deferred", deferred, err)
	}
	if err := e.Process(ctx, claimed[0]); err != nil {
		t.Fatalf("Process() error = %v", err)
	}
	stored, _ := instances.GetByID(ctx, instanceID)
	if stored.Status != model.WorkflowPaused {
		t.Errorf("status = %s, want paused after deferred pause", stored.Status)
	}
	if err := instances.Resume(ctx, instanceID); err != nil {
		t.Fatalf("Resume() error = %v", err)
	}
	resumed, _ := instances.GetByID(ctx, instanceID)
	if resumed.Status != model.WorkflowWaiting {
		t.Errorf("status = %s, want waiting after resume", resumed.Status)
	}
}

func TestEngineStopInterruptsRunningScript(t *testing.T) {
	db := setupEngineDB(t)
	wfID := createWorkflow(t, db, n1,
		nodeJSON(n1, "script", "spin", "while (true) {}", "", "out", nil),
	)
	instanceID := insertInstance(t, db, wfID, n1, map[string]any{})
	e, instances := testEngine(t, db, model.DefaultLimits())
	ctx := context.Background()

	claimed, err := instances.ClaimNext(ctx, "worker-1", time.Minute, 10)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("claim = %d, err %v", len(claimed), err)
	}

	processDone := make(chan error, 1)
	go func() { processDone <- e.Process(context.Background(), claimed[0]) }()

	var attempt *model.NodeInstance
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		a, err := instances.GetRunningNodeInstance(context.Background(), instanceID)
		if err == nil {
			attempt = a
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if attempt == nil {
		t.Fatal("node never started")
	}

	pending, err := instances.Stop(ctx, instanceID, "operator")
	if err != nil {
		t.Fatalf("Stop() error = %v", err)
	}
	if !pending {
		t.Error("Stop(running) pending = false, want true")
	}
	e.Cancel(instanceID)

	select {
	case err := <-processDone:
		if err != nil {
			t.Fatalf("Process() error = %v, want nil after stop", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Process did not return after stop")
	}

	got, _ := instances.GetNodeInstance(ctx, instanceID, attempt.ID)
	if got.Status != model.NodeStopped || !got.Cancelled || got.StoppedAt == nil {
		t.Errorf("attempt = %+v, want stopped+cancelled", got)
	}
	stored, _ := instances.GetByID(ctx, instanceID)
	if stored.Status != model.WorkflowStopped || stored.TerminationPending {
		t.Errorf("instance = %+v, want stopped with pending cleared", stored)
	}
	events, _ := instances.ListEvents(ctx, instanceID)
	types := map[string]bool{}
	for _, ev := range events {
		types[ev.Type] = true
	}
	for _, want := range []string{"node_stopped", "cancellation"} {
		if !types[want] {
			t.Errorf("event type %q missing; got %v", want, types)
		}
	}
}

func TestDispatcherHeartbeatCancelsStoppedInstance(t *testing.T) {
	db := setupEngineDB(t)
	wfID := createWorkflow(t, db, n1,
		nodeJSON(n1, "script", "spin", "while (true) {}", "", "out", nil),
	)
	instanceID := insertInstance(t, db, wfID, n1, map[string]any{})
	e, instances := testEngine(t, db, model.DefaultLimits())
	ctx := context.Background()

	d, err := engine.NewDispatcher(ctx, e, instances, "dispatcher-1", engine.DispatcherOptions{
		PollInterval: 20 * time.Millisecond,
		Lease:        time.Minute,
		BatchSize:    10,
		PoolSize:     4,
		Heartbeat:    20 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	d.Run()
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = d.Shutdown(shutdownCtx)
	}()

	// Wait for the dispatcher to claim and start the script.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := instances.GetRunningNodeInstance(ctx, instanceID); err == nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if _, err := instances.GetRunningNodeInstance(ctx, instanceID); err != nil {
		t.Fatal("node never started")
	}

	// Stop from another "replica": the local heartbeat must notice the
	// pending termination and cancel the in-flight script.
	if _, err := instances.Stop(ctx, instanceID, "operator"); err != nil {
		t.Fatal(err)
	}

	deadline = time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		stored, _ := instances.GetByID(ctx, instanceID)
		attempts, _ := instances.ListNodeInstances(ctx, instanceID)
		if stored.Status == model.WorkflowStopped && !stored.TerminationPending && len(attempts) == 1 && attempts[0].Status == model.NodeStopped {
			if !attempts[0].Cancelled {
				t.Errorf("attempt = %+v, want cancelled", attempts[0])
			}
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("heartbeat did not cancel the stopped instance")
}

func TestCancellationRegistryConcurrent(t *testing.T) {
	e := engine.NewEngine(nil, nil, executor.NewHookRunner(nil), model.Limits{}, nil, "test")
	var wg sync.WaitGroup
	for i := 0; i < 200; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			id := fmt.Sprintf("instance-%d", i%7)
			_, cancel := context.WithCancel(context.Background())
			e.RegisterCancel(id, cancel)
			e.Cancel(id)
			e.UnregisterCancel(id)
		}(i)
	}
	wg.Wait()
}

func TestEngineStopInterruptsPoller(t *testing.T) {
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
				"delay":        "1h",
				"max_attempts": 5,
			},
		}),
	)
	instanceID := insertInstance(t, db, wfID, n1, map[string]any{})
	e, instances := testEngineWithExec(t, db, model.DefaultLimits(), executor.Limits{HTTPAllowlist: []string{"127.0.0.1"}})
	ctx := context.Background()

	claimed, err := instances.ClaimNext(ctx, "worker-1", time.Minute, 10)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("claim = %d, err %v", len(claimed), err)
	}

	processDone := make(chan error, 1)
	go func() { processDone <- e.Process(context.Background(), claimed[0]) }()

	var attempt *model.NodeInstance
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		a, err := instances.GetRunningNodeInstance(context.Background(), instanceID)
		if err == nil {
			attempt = a
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if attempt == nil {
		t.Fatal("poller node never started")
	}

	pending, err := instances.Stop(ctx, instanceID, "operator")
	if err != nil {
		t.Fatalf("Stop() error = %v", err)
	}
	if !pending {
		t.Error("Stop(running) pending = false, want true")
	}
	e.Cancel(instanceID)

	select {
	case err := <-processDone:
		if err != nil {
			t.Fatalf("Process() error = %v, want nil after stop", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Process did not return after stop")
	}

	got, _ := instances.GetNodeInstance(ctx, instanceID, attempt.ID)
	if got.Status != model.NodeStopped || !got.Cancelled || got.StoppedAt == nil {
		t.Errorf("attempt = %+v, want stopped+cancelled", got)
	}
	stored, _ := instances.GetByID(ctx, instanceID)
	if stored.Status != model.WorkflowStopped || stored.TerminationPending {
		t.Errorf("instance = %+v, want stopped with pending cleared", stored)
	}
	events, _ := instances.ListEvents(ctx, instanceID)
	types := map[string]bool{}
	for _, ev := range events {
		types[ev.Type] = true
	}
	for _, want := range []string{"node_stopped", "cancellation"} {
		if !types[want] {
			t.Errorf("event type %q missing; got %v", want, types)
		}
	}
}
