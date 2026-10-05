package engine_test

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/simpwf/workflow-engine/internal/workflow/engine"
	"github.com/simpwf/workflow-engine/internal/workflow/executor"
	"github.com/simpwf/workflow-engine/internal/workflow/model"
	"github.com/simpwf/workflow-engine/internal/workflow/repository"
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

	d, err := engine.NewDispatcher(ctx, e, instances, repository.NewParallelRepository(db), "dispatcher-1", engine.DispatcherOptions{
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
	e := engine.NewEngine(nil, nil, nil, executor.NewHookRunner(nil), model.Limits{}, nil, "test", model.LeanOptions{})
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

// blockingExecutor blocks inside Execute until release closes, so a control
// write can land deterministically mid-execution. started (buffered) fires
// on entry; calls counts executions for the exactly-once assertion.
type blockingExecutor struct {
	started chan struct{}
	release chan struct{}
	out     any
	calls   *int32
}

func (b blockingExecutor) Execute(ctx context.Context, _ executor.Request) (*executor.Result, error) {
	atomic.AddInt32(b.calls, 1)
	select {
	case b.started <- struct{}{}:
	default:
	}
	select {
	case <-b.release:
		return &executor.Result{Output: b.out}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func waitNodeStarted(t *testing.T, started chan struct{}) {
	t.Helper()
	select {
	case <-started:
	case <-time.After(10 * time.Second):
		t.Fatal("node execution never started")
	}
}

func waitProcessDone(t *testing.T, done chan error) error {
	t.Helper()
	select {
	case err := <-done:
		return err
	case <-time.After(10 * time.Second):
		t.Fatal("Process did not return")
		return nil
	}
}

func assertExecutedOnce(t *testing.T, instances repository.InstanceRepository, instanceID string, calls *int32) {
	t.Helper()
	ctx := context.Background()
	if got := atomic.LoadInt32(calls); got != 1 {
		t.Errorf("executor calls = %d, want exactly 1", got)
	}
	rows, err := instances.ListNodeInstances(ctx, instanceID)
	if err != nil {
		t.Fatalf("ListNodeInstances() error = %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("node occurrences = %d, want 1", len(rows))
	}
	if rows[0].Attempt != 1 {
		t.Errorf("attempt = %d, want 1", rows[0].Attempt)
	}
}

// TestEnginePauseMidExecutionRedrives: a Pause landing mid-execution must not
// wedge the instance. The worker retries the commit only (no re-execution),
// merging the fresh pause flag, so the instance reaches paused without any
// restart and the node executes exactly once.
func TestEnginePauseMidExecutionRedrives(t *testing.T) {
	db := setupEngineDB(t)
	wfID := createWorkflow(t, db, n1,
		customNodeJSON(n1, "midflight1", map[string]any{}, n2, "out", nil),
		nodeJSON(n2, "script", "done", "return 2;", "", "out2", nil),
	)
	instanceID := insertInstance(t, db, wfID, n1, map[string]any{})
	var calls int32
	ex := blockingExecutor{started: make(chan struct{}, 1), release: make(chan struct{}), out: "paused-ok", calls: &calls}
	e, cleanup := testEngineWithCustom(t, db, model.DefaultLimits(), "midflight1", ex)
	defer cleanup()
	instances := repository.NewInstanceRepository(db)
	ctx := context.Background()

	claimed, err := instances.ClaimNext(ctx, "worker-1", time.Minute, 10)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("claim = %d, err %v", len(claimed), err)
	}
	processDone := make(chan error, 1)
	go func() { processDone <- e.Process(context.Background(), claimed[0]) }()

	waitNodeStarted(t, ex.started)
	deferred, err := instances.Pause(ctx, instanceID)
	if err != nil || !deferred {
		t.Fatalf("Pause(running) = deferred %v, err %v, want deferred", deferred, err)
	}
	close(ex.release)

	if err := waitProcessDone(t, processDone); err != nil {
		t.Fatalf("Process() error = %v, want nil after mid-flight pause", err)
	}
	stored, _ := instances.GetByID(ctx, instanceID)
	if stored.Status != model.WorkflowPaused {
		t.Errorf("status = %s, want paused after mid-flight pause", stored.Status)
	}
	assertExecutedOnce(t, instances, instanceID, &calls)

	if err := instances.Resume(ctx, instanceID); err != nil {
		t.Fatalf("Resume() error = %v", err)
	}
	resumed, _ := instances.GetByID(ctx, instanceID)
	if resumed.Status != model.WorkflowWaiting {
		t.Fatalf("status = %s, want waiting after resume", resumed.Status)
	}
	reclaimed, err := instances.ClaimNext(ctx, "worker-1", time.Minute, 10)
	if err != nil || len(reclaimed) != 1 {
		t.Fatalf("re-claim = %d, err %v, want 1 (no wedge)", len(reclaimed), err)
	}
	if err := e.Process(ctx, reclaimed[0]); err != nil {
		t.Fatalf("Process() after resume error = %v", err)
	}
	final, _ := instances.GetByID(ctx, instanceID)
	if final.Status != model.WorkflowFinished {
		t.Errorf("status = %s, want finished after redrive", final.Status)
	}
}

// TestEngineResumeMidExecutionClearsPause: a Resume (pause-clear) landing
// mid-execution must also commit cleanly, with the fresh flag winning in the
// clear direction — the instance commits waiting, not paused.
func TestEngineResumeMidExecutionClearsPause(t *testing.T) {
	db := setupEngineDB(t)
	wfID := createWorkflow(t, db, n1,
		customNodeJSON(n1, "midflight2", map[string]any{}, n2, "out", nil),
		nodeJSON(n2, "script", "done", "return 2;", "", "out2", nil),
	)
	instanceID := insertInstance(t, db, wfID, n1, map[string]any{})
	var calls int32
	ex := blockingExecutor{started: make(chan struct{}, 1), release: make(chan struct{}), out: "resumed-ok", calls: &calls}
	e, cleanup := testEngineWithCustom(t, db, model.DefaultLimits(), "midflight2", ex)
	defer cleanup()
	instances := repository.NewInstanceRepository(db)
	ctx := context.Background()

	claimed, err := instances.ClaimNext(ctx, "worker-1", time.Minute, 10)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("claim = %d, err %v", len(claimed), err)
	}
	deferred, err := instances.Pause(ctx, instanceID)
	if err != nil || !deferred {
		t.Fatalf("Pause(running) = deferred %v, err %v, want deferred", deferred, err)
	}
	processDone := make(chan error, 1)
	go func() { processDone <- e.Process(context.Background(), claimed[0]) }()

	waitNodeStarted(t, ex.started)
	if err := instances.Resume(ctx, instanceID); err != nil {
		t.Fatalf("Resume(running) error = %v", err)
	}
	close(ex.release)

	if err := waitProcessDone(t, processDone); err != nil {
		t.Fatalf("Process() error = %v, want nil after mid-flight resume", err)
	}
	stored, _ := instances.GetByID(ctx, instanceID)
	if stored.Status != model.WorkflowWaiting {
		t.Errorf("status = %s, want waiting after mid-flight resume", stored.Status)
	}
	assertExecutedOnce(t, instances, instanceID, &calls)
}

// conflictInstances forces ErrRevisionConflict on every Checkpoint call so
// the exhaustion path is deterministic (no control-write spam timing).
type conflictInstances struct {
	repository.InstanceRepository
	checkpointCalls *int32
}

func (d *conflictInstances) Checkpoint(_ context.Context, _ repository.Checkpoint) error {
	atomic.AddInt32(d.checkpointCalls, 1)
	return repository.ErrRevisionConflict
}

// TestEngineCheckpointExhaustionReleasesLease: under persistent conflict the
// worker must never return the conflict error while holding the lease (the
// wedge). It degrades to a redrive: lease released, row reclaimable.
func TestEngineCheckpointExhaustionReleasesLease(t *testing.T) {
	db := setupEngineDB(t)
	wfID := createWorkflow(t, db, n1,
		nodeJSON(n1, "script", "inc", "return 1;", n2, "out", nil),
		nodeJSON(n2, "script", "done", "return 2;", "", "out2", nil),
	)
	instanceID := insertInstance(t, db, wfID, n1, map[string]any{})
	realInstances := repository.NewInstanceRepository(db)
	var checkpointCalls int32
	decorated := &conflictInstances{InstanceRepository: realInstances, checkpointCalls: &checkpointCalls}
	e := engine.NewEngine(decorated, repository.NewParallelRepository(db),
		executor.NewExecutors(executor.Limits{}, nil, executor.Dependencies{}),
		executor.NewHookRunner(nil), model.DefaultLimits(), testLoader(db), sysUserID, model.LeanOptions{})
	ctx := context.Background()

	claimed, err := realInstances.ClaimNext(ctx, "worker-1", time.Minute, 10)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("claim = %d, err %v", len(claimed), err)
	}
	if err := e.Process(ctx, claimed[0]); err != nil {
		t.Fatalf("Process() error = %v, want nil (release, never wedge)", err)
	}
	if got := atomic.LoadInt32(&checkpointCalls); got != 3 {
		t.Errorf("checkpoint attempts = %d, want 3 (initial + 2 retries)", got)
	}
	stored, _ := realInstances.GetByID(ctx, instanceID)
	if stored.Status != model.WorkflowRunning {
		t.Errorf("status = %s, want running (released, not parked)", stored.Status)
	}
	if stored.LeasedBy != "" {
		t.Errorf("leased_by = %q, want released", stored.LeasedBy)
	}
	reclaimed, err := realInstances.ClaimNext(ctx, "worker-2", time.Minute, 10)
	if err != nil || len(reclaimed) != 1 {
		t.Fatalf("re-claim = %d, err %v, want 1 (redrivable)", len(reclaimed), err)
	}
}
