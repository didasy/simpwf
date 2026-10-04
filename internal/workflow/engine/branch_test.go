package engine_test

import (
	"context"
	"encoding/json"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/simpwf/workflow-engine/internal/workflow/engine"
	"github.com/simpwf/workflow-engine/internal/workflow/model"
	"github.com/simpwf/workflow-engine/internal/workflow/repository"
	"gorm.io/gorm"
)

func parallelWorkflowIDs() (start, a, b, end string) {
	return newEngineID(), newEngineID(), newEngineID(), newEngineID()
}

func createParallelWorkflow(t *testing.T, db *gorm.DB) (wfID, start, a, b, end string) {
	t.Helper()
	wfID, start, a, b, end, _ = createParallelWorkflowFull(t, db, "context.x = 1", "context.y = 2", `context.x = branch["a"].context.x`)
	return wfID, start, a, b, end
}

func createParallelWorkflowWithScripts(t *testing.T, db *gorm.DB, scriptA, scriptB string) (wfID, start, a, b, end string) {
	t.Helper()
	wfID, start, a, b, end, _ = createParallelWorkflowFull(t, db, scriptA, scriptB, `context.x = branch["a"].context.x`)
	return wfID, start, a, b, end
}

func createParallelWorkflowFull(t *testing.T, db *gorm.DB, scriptA, scriptB, combining string) (wfID, start, a, b, end, done string) {
	t.Helper()
	start, a, b, end = parallelWorkflowIDs()
	done = newEngineID()
	wfID = createWorkflow(t, db, start,
		nodeJSON(start, "parallel_start", "fork", "", "", "", map[string]any{
			"branches":             map[string]any{"a": a, "b": b},
			"parallel_end_node_id": end,
		}),
		nodeJSON(a, "script", "a", scriptA, end, "", nil),
		nodeJSON(b, "script", "b", scriptB, end, "", nil),
		nodeJSON(end, "parallel_end", "join", "", done, "", map[string]any{
			"combining_script": combining,
		}),
		nodeJSON(done, "script", "done", "context.done = true", "", "", nil),
	)
	return wfID, start, a, b, end, done
}

// insertDebugInstance mirrors insertInstanceWithMode for a debug instance:
// the engine parks every step paused so each resume advances one step.
func insertDebugInstance(t *testing.T, db *gorm.DB, wfID string, start string, ctxMap map[string]any) string {
	t.Helper()
	id := newEngineID()
	ctxRaw, _ := json.Marshal(ctxMap)
	frameRaw, _ := model.NewFrame(start).JSON()
	w := model.WorkflowInstance{
		ID:                   id,
		WorkflowDefinitionID: wfID,
		Debug:                true,
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

// wakeBranch resumes one debug-parked branch so the next claim runs it.
func wakeBranch(t *testing.T, db *gorm.DB, branchID string) {
	t.Helper()
	if err := repository.NewParallelRepository(db).WakePausedBranch(context.Background(), branchID); err != nil {
		t.Fatalf("WakePausedBranch() error = %v", err)
	}
}

func driveToTerminal(t *testing.T, db *gorm.DB, e *engine.Engine, instanceID string) model.WorkflowInstance {
	t.Helper()
	ctx := context.Background()
	instances := repository.NewInstanceRepository(db)
	prepo := repository.NewParallelRepository(db)
	for i := 0; i < 10; i++ {
		claimed, err := instances.ClaimNext(ctx, "test-worker", time.Minute, 1)
		if err != nil {
			t.Fatalf("ClaimNext() error = %v", err)
		}
		if len(claimed) == 1 {
			if err := e.Process(ctx, claimed[0]); err != nil {
				t.Fatalf("Process() error = %v", err)
			}
		}
		bclaimed, err := prepo.ClaimNextBranches(ctx, "test-worker", time.Minute, 10)
		if err != nil {
			t.Fatalf("ClaimNextBranches() error = %v", err)
		}
		for _, b := range bclaimed {
			if err := e.ProcessBranch(ctx, b); err != nil {
				t.Fatalf("ProcessBranch() error = %v", err)
			}
		}
		inst, err := instances.GetByID(ctx, instanceID)
		if err != nil {
			t.Fatalf("GetByID() error = %v", err)
		}
		if inst.Status == model.WorkflowFinished || inst.Status == model.WorkflowFailed {
			return *inst
		}
	}
	t.Fatalf("instance %s did not finish", instanceID)
	return model.WorkflowInstance{}
}

func forkBranches(t *testing.T, db *gorm.DB, wfID, instanceID, start, a, b, end string) []model.ParallelBranch {
	t.Helper()
	ctx := context.Background()
	instances := repository.NewInstanceRepository(db)
	claimed, err := instances.ClaimNext(ctx, "test-worker", time.Minute, 1)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("ClaimNext() = %d, %v", len(claimed), err)
	}
	inst := claimed[0]
	frame, _ := model.ParseFrame(inst.Frame)
	counters, _ := model.ParseCounters(inst.Counters)
	if err := counters.Record(start, model.DefaultLimits()); err != nil {
		t.Fatalf("Record() error = %v", err)
	}
	forkFrame := model.NewFrame(end)
	forkFrame.GroupStack = frame.GroupStack
	prepo := repository.NewParallelRepository(db)
	_, branches, err := prepo.Fork(ctx, repository.ForkParallel{
		InstanceID:           instanceID,
		WorkflowDefinitionID: inst.WorkflowDefinitionID,
		WorkerID:             "test-worker",
		Revision:             inst.Revision,
		Depth:                1,
		StartNodeID:          start,
		EndNodeID:            end,
		Frame:                forkFrame,
		Counters:             counters,
		Context:              inst.Context,
		Branches: []repository.ForkBranch{
			{Name: "a", BranchIndex: 0, StartNodeID: a, Frame: model.NewFrame(a), Context: inst.Context},
			{Name: "b", BranchIndex: 1, StartNodeID: b, Frame: model.NewFrame(b), Context: inst.Context},
		},
	})
	if err != nil {
		t.Fatalf("Fork() error = %v", err)
	}
	return branches
}

func claimBranch(t *testing.T, db *gorm.DB, branchID string) model.ParallelBranch {
	t.Helper()
	claimed, err := repository.NewParallelRepository(db).ClaimNextBranches(context.Background(), "test-worker", time.Minute, 10)
	if err != nil {
		t.Fatalf("ClaimNextBranches() error = %v", err)
	}
	for _, b := range claimed {
		if b.ID == branchID {
			return b
		}
	}
	t.Fatalf("branch %s was not claimed", branchID)
	return model.ParallelBranch{}
}

func TestProcessBranchExecutesScriptAndCompletes(t *testing.T) {
	db := setupEngineDB(t)
	ctx := context.Background()
	wfID, start, a, b, end := createParallelWorkflow(t, db)
	_ = a
	_ = b
	_ = end
	instanceID := insertInstance(t, db, wfID, start, map[string]any{"v": 0})
	branches := forkBranches(t, db, wfID, instanceID, start, a, b, end)

	e, _ := testEngine(t, db, model.DefaultLimits())
	prepo := repository.NewParallelRepository(db)
	claimed, err := prepo.ClaimNextBranches(ctx, "test-worker", time.Minute, 10)
	if err != nil {
		t.Fatalf("ClaimNextBranches() error = %v", err)
	}
	byID := map[string]model.ParallelBranch{}
	for _, b := range claimed {
		byID[b.ID] = b
	}
	for i, br := range branches {
		c, ok := byID[br.ID]
		if !ok {
			t.Fatalf("branch %s was not claimed", br.Name)
		}
		if err := e.ProcessBranch(ctx, c); err != nil {
			t.Fatalf("ProcessBranch(%s) error = %v", br.Name, err)
		}
		got, err := prepo.GetBranch(ctx, br.ID)
		if err != nil {
			t.Fatalf("GetBranch() error = %v", err)
		}
		if got.Status != model.ParallelBranchCompleted {
			t.Fatalf("branch %s status = %s", br.Name, got.Status)
		}
		var ctxMap map[string]any
		if err := json.Unmarshal(got.Context, &ctxMap); err != nil {
			t.Fatalf("unmarshal branch context: %v", err)
		}
		if i == 0 && ctxMap["x"] != float64(1) {
			t.Fatalf("branch a context = %v", ctxMap)
		}
		if i == 1 && ctxMap["y"] != float64(2) {
			t.Fatalf("branch b context = %v", ctxMap)
		}
		if _, ok := ctxMap["v"]; !ok {
			t.Fatalf("branch %s lost forked context: %v", br.Name, ctxMap)
		}
	}
	exs, err := prepo.ListExecutions(ctx, instanceID)
	if err != nil || len(exs) != 1 {
		t.Fatalf("ListExecutions() = %d, %v", len(exs), err)
	}
	if exs[0].Status != model.ParallelReadyToJoin || exs[0].CompletedCount != 2 {
		t.Fatalf("execution = %+v", exs[0])
	}
	inst, err := repository.NewInstanceRepository(db).GetByID(ctx, instanceID)
	if err != nil {
		t.Fatalf("GetByID() error = %v", err)
	}
	if inst.WaitingReason != model.WaitingReasonRunnable {
		t.Fatalf("parent reason = %s", inst.WaitingReason)
	}
	var parentCtx map[string]any
	if err := json.Unmarshal(inst.Context, &parentCtx); err != nil {
		t.Fatalf("unmarshal parent context: %v", err)
	}
	if _, ok := parentCtx["x"]; ok {
		t.Fatalf("parent context mutated by branches: %v", parentCtx)
	}
	attempts, err := repository.NewInstanceRepository(db).ListNodeInstances(ctx, instanceID)
	if err != nil {
		t.Fatalf("ListNodeInstances() error = %v", err)
	}
	if len(attempts) != 2 {
		t.Fatalf("attempts = %d, want 2", len(attempts))
	}
	for _, at := range attempts {
		if at.BranchID == "" || at.Status != model.NodeFinished {
			t.Fatalf("attempt = %+v", at)
		}
	}
}

func TestProcessBranchFailureFailsExecution(t *testing.T) {
	db := setupEngineDB(t)
	ctx := context.Background()
	wfID, start, a, b, end := createParallelWorkflowWithScripts(t, db, `throw "boom"`, "context.y = 2")
	instanceID := insertInstance(t, db, wfID, start, map[string]any{})
	branches := forkBranches(t, db, wfID, instanceID, start, a, b, end)

	e, _ := testEngine(t, db, model.DefaultLimits())
	prepo := repository.NewParallelRepository(db)
	claimed := claimBranch(t, db, branches[0].ID)
	if err := e.ProcessBranch(ctx, claimed); err != nil {
		t.Fatalf("ProcessBranch() error = %v", err)
	}
	got, err := prepo.GetBranch(ctx, branches[0].ID)
	if err != nil {
		t.Fatalf("GetBranch() error = %v", err)
	}
	if got.Status != model.ParallelBranchFailed {
		t.Fatalf("branch status = %s", got.Status)
	}
	if !strings.Contains(got.Error, "boom") {
		t.Fatalf("branch error = %q", got.Error)
	}
	exs, err := prepo.ListExecutions(ctx, instanceID)
	if err != nil || len(exs) != 1 {
		t.Fatalf("ListExecutions() = %d, %v", len(exs), err)
	}
	if exs[0].Status != model.ParallelExecutionFailed {
		t.Fatalf("execution status = %s", exs[0].Status)
	}
}

func waitBranches(t *testing.T, db *gorm.DB, executionID string, want model.ParallelBranchStatus) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	prepo := repository.NewParallelRepository(db)
	for time.Now().Before(deadline) {
		branches, err := prepo.ListBranches(context.Background(), executionID)
		if err != nil {
			t.Fatal(err)
		}
		done := len(branches) > 0
		for _, b := range branches {
			if b.Status != want {
				done = false
			}
		}
		if done {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("branches of %s did not reach %s", executionID, want)
}

func TestDispatcherProcessesBranches(t *testing.T) {
	db := setupEngineDB(t)
	wfID, start, a, b, end := createParallelWorkflow(t, db)
	instanceID := insertInstance(t, db, wfID, start, map[string]any{"v": 0})
	forkBranches(t, db, wfID, instanceID, start, a, b, end)

	d := dispatcherFor(t, db, model.DefaultLimits(), "branch-worker", shortDispatcherOpts(), allowAll())
	d.Run()
	exs, err := repository.NewParallelRepository(db).ListExecutions(context.Background(), instanceID)
	if err != nil || len(exs) != 1 {
		t.Fatalf("ListExecutions() = %d, %v", len(exs), err)
	}
	waitBranches(t, db, exs[0].ID, model.ParallelBranchCompleted)
}

func TestProcessForksParallel(t *testing.T) {
	db := setupEngineDB(t)
	ctx := context.Background()
	wfID, start, a, b, end := createParallelWorkflow(t, db)
	instanceID := insertInstance(t, db, wfID, start, map[string]any{"v": 0})

	e, instances := testEngine(t, db, model.DefaultLimits())
	claimed, err := instances.ClaimNext(ctx, "test-worker", time.Minute, 1)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("ClaimNext() = %d, %v", len(claimed), err)
	}
	if err := e.Process(ctx, claimed[0]); err != nil {
		t.Fatalf("Process() error = %v", err)
	}
	inst, err := instances.GetByID(ctx, instanceID)
	if err != nil {
		t.Fatalf("GetByID() error = %v", err)
	}
	if inst.Status != model.WorkflowWaiting || inst.WaitingReason != model.WaitingReasonParallel {
		t.Fatalf("parent = %s/%s", inst.Status, inst.WaitingReason)
	}
	frame, _ := model.ParseFrame(inst.Frame)
	if frame.CurrentNodeID != end {
		t.Fatalf("parent cursor = %q", frame.CurrentNodeID)
	}
	prepo := repository.NewParallelRepository(db)
	exs, err := prepo.ListExecutions(ctx, instanceID)
	if err != nil || len(exs) != 1 {
		t.Fatalf("ListExecutions() = %d, %v", len(exs), err)
	}
	if exs[0].Status != model.ParallelWaitingForBranches || exs[0].BranchCount != 2 {
		t.Fatalf("execution = %+v", exs[0])
	}
	branches, err := prepo.ListBranches(ctx, exs[0].ID)
	if err != nil || len(branches) != 2 {
		t.Fatalf("ListBranches() = %d, %v", len(branches), err)
	}
	_ = a
	_ = b
	for _, br := range branches {
		if br.Status != model.ParallelBranchPending {
			t.Fatalf("branch %s status = %s", br.Name, br.Status)
		}
		var ctxMap map[string]any
		if err := json.Unmarshal(br.Context, &ctxMap); err != nil {
			t.Fatalf("unmarshal branch context: %v", err)
		}
		if ctxMap["v"] != float64(0) {
			t.Fatalf("branch %s context = %v", br.Name, ctxMap)
		}
	}
	var parentCtx map[string]any
	if err := json.Unmarshal(inst.Context, &parentCtx); err != nil {
		t.Fatalf("unmarshal parent context: %v", err)
	}
	if parentCtx["v"] != float64(0) || len(parentCtx) != 1 {
		t.Fatalf("parent context = %v", parentCtx)
	}
}

func TestProcessForkEnforcesActiveBranchLimit(t *testing.T) {
	db := setupEngineDB(t)
	ctx := context.Background()
	wfID, start, _, _, _ := createParallelWorkflow(t, db)
	instanceID := insertInstance(t, db, wfID, start, map[string]any{})

	limits := model.DefaultLimits()
	limits.MaxActiveBranchesPerInstance = 1
	e, instances := testEngine(t, db, limits)
	claimed, err := instances.ClaimNext(ctx, "test-worker", time.Minute, 1)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("ClaimNext() = %d, %v", len(claimed), err)
	}
	if err := e.Process(ctx, claimed[0]); err != nil {
		t.Fatalf("Process() error = %v", err)
	}
	inst, err := instances.GetByID(ctx, instanceID)
	if err != nil {
		t.Fatalf("GetByID() error = %v", err)
	}
	if inst.Status != model.WorkflowFailed {
		t.Fatalf("status = %s", inst.Status)
	}
	if !strings.Contains(inst.Error, "branch") {
		t.Fatalf("error = %q", inst.Error)
	}
	exs, err := repository.NewParallelRepository(db).ListExecutions(ctx, instanceID)
	if err != nil {
		t.Fatalf("ListExecutions() error = %v", err)
	}
	if len(exs) != 0 {
		t.Fatalf("fenced fork left %d executions", len(exs))
	}
}

func TestProcessJoinMergesBranchContexts(t *testing.T) {
	db := setupEngineDB(t)
	ctx := context.Background()
	wfID, start, _, _, _, _ := createParallelWorkflowFull(t, db,
		"context.x = 1", "context.y = 2", `context.total = branch["a"].context.x + branch["b"].context.y`)
	instanceID := insertInstance(t, db, wfID, start, map[string]any{"v": 0})

	e, _ := testEngine(t, db, model.DefaultLimits())
	inst := driveToTerminal(t, db, e, instanceID)
	if inst.Status != model.WorkflowFinished {
		t.Fatalf("status = %s (%s)", inst.Status, inst.Error)
	}
	var ctxMap map[string]any
	if err := json.Unmarshal(inst.Context, &ctxMap); err != nil {
		t.Fatalf("unmarshal context: %v", err)
	}
	if ctxMap["total"] != float64(3) || ctxMap["v"] != float64(0) || ctxMap["done"] != true {
		t.Fatalf("merged context = %v", ctxMap)
	}
	exs, err := repository.NewParallelRepository(db).ListExecutions(ctx, instanceID)
	if err != nil || len(exs) != 1 {
		t.Fatalf("ListExecutions() = %d, %v", len(exs), err)
	}
	if exs[0].Status != model.ParallelExecutionCompleted {
		t.Fatalf("execution status = %s", exs[0].Status)
	}
}

func TestProcessJoinBranchViewReadOnly(t *testing.T) {
	db := setupEngineDB(t)
	wfID, start, _, _, _, _ := createParallelWorkflowFull(t, db,
		"context.x = 1", "context.y = 2",
		`branch["a"].context.x = 99; context.total = branch["a"].context.x`)
	instanceID := insertInstance(t, db, wfID, start, map[string]any{})

	e, _ := testEngine(t, db, model.DefaultLimits())
	inst := driveToTerminal(t, db, e, instanceID)
	if inst.Status != model.WorkflowFinished {
		t.Fatalf("status = %s (%s)", inst.Status, inst.Error)
	}
	var ctxMap map[string]any
	if err := json.Unmarshal(inst.Context, &ctxMap); err != nil {
		t.Fatalf("unmarshal context: %v", err)
	}
	if ctxMap["total"] != float64(1) {
		t.Fatalf("branch view was mutable: %v", ctxMap)
	}
}

func TestProcessJoinReparksWhenNotReady(t *testing.T) {
	db := setupEngineDB(t)
	ctx := context.Background()
	wfID, start, a, b, end := createParallelWorkflow(t, db)
	instanceID := insertInstance(t, db, wfID, start, map[string]any{})
	branches := forkBranches(t, db, wfID, instanceID, start, a, b, end)
	_ = branches

	// Spuriously wake the parent while branches still run.
	if err := db.Exec(`UPDATE workflow_instances SET waiting_reason = '' WHERE id = ?`, instanceID).Error; err != nil {
		t.Fatalf("wake parent: %v", err)
	}
	e, instances := testEngine(t, db, model.DefaultLimits())
	claimed, err := instances.ClaimNext(ctx, "test-worker", time.Minute, 1)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("ClaimNext() = %d, %v", len(claimed), err)
	}
	if err := e.Process(ctx, claimed[0]); err != nil {
		t.Fatalf("Process() error = %v", err)
	}
	inst, err := instances.GetByID(ctx, instanceID)
	if err != nil {
		t.Fatalf("GetByID() error = %v", err)
	}
	if inst.Status != model.WorkflowWaiting || inst.WaitingReason != model.WaitingReasonParallel {
		t.Fatalf("parent = %s/%s", inst.Status, inst.WaitingReason)
	}
	frame, _ := model.ParseFrame(inst.Frame)
	if frame.CurrentNodeID != end {
		t.Fatalf("cursor moved to %q", frame.CurrentNodeID)
	}
	_ = end
}

func TestDispatcherCancelsBranchWorkersOnStop(t *testing.T) {
	db := setupEngineDB(t)
	ctx := context.Background()
	wfID, start, a, b, end := createParallelWorkflow(t, db)
	instanceID := insertInstance(t, db, wfID, start, map[string]any{})
	branches := forkBranches(t, db, wfID, instanceID, start, a, b, end)
	_ = end

	e, instances := testEngine(t, db, model.DefaultLimits())
	prepo := repository.NewParallelRepository(db)
	if _, err := prepo.ClaimNextBranches(ctx, "test-worker", time.Minute, 10); err != nil {
		t.Fatalf("ClaimNextBranches() error = %v", err)
	}
	cancelled := map[string]*atomic.Bool{}
	for _, br := range branches {
		flag := &atomic.Bool{}
		cancelled[br.ID] = flag
		e.RegisterCancel(br.ID, func() { flag.Store(true) })
		defer e.UnregisterCancel(br.ID)
	}
	if _, err := instances.Stop(ctx, instanceID, "stop test"); err != nil {
		t.Fatalf("Stop() error = %v", err)
	}
	d, err := engine.NewDispatcher(ctx, e, instances, prepo, "term-worker", shortDispatcherOpts())
	if err != nil {
		t.Fatalf("NewDispatcher() error = %v", err)
	}
	d.Run()
	defer func() {
		shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = d.Shutdown(shutdown)
	}()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		done := true
		for _, flag := range cancelled {
			if !flag.Load() {
				done = false
			}
		}
		if done {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("branch workers were not cancelled on stop")
}

func TestProcessBranchSkipsStaleClaim(t *testing.T) {
	db := setupEngineDB(t)
	ctx := context.Background()
	wfID, start, a, b, end := createParallelWorkflow(t, db)
	instanceID := insertInstance(t, db, wfID, start, map[string]any{})
	branches := forkBranches(t, db, wfID, instanceID, start, a, b, end)

	e, _ := testEngine(t, db, model.DefaultLimits())
	prepo := repository.NewParallelRepository(db)
	stale := claimBranch(t, db, branches[0].ID)
	// Another worker commits first, releasing the lease.
	frame, _ := model.ParseFrame(stale.Frame)
	if err := prepo.CheckpointBranch(ctx, repository.BranchCheckpoint{
		BranchID: stale.ID, WorkerID: "test-worker", Revision: stale.Revision,
		Status: model.ParallelBranchWaiting, Frame: frame, Context: stale.Context,
	}); err != nil {
		t.Fatalf("CheckpointBranch() error = %v", err)
	}
	if err := e.ProcessBranch(ctx, stale); err != nil {
		t.Fatalf("ProcessBranch() error = %v", err)
	}
	got, err := prepo.GetBranch(ctx, stale.ID)
	if err != nil {
		t.Fatalf("GetBranch() error = %v", err)
	}
	if got.Status != model.ParallelBranchWaiting || got.Revision != stale.Revision+1 {
		t.Fatalf("stale worker committed: %+v", got)
	}
	attempts, err := repository.NewInstanceRepository(db).ListNodeInstances(ctx, instanceID)
	if err != nil {
		t.Fatalf("ListNodeInstances() error = %v", err)
	}
	if len(attempts) != 0 {
		t.Fatalf("stale worker executed %d attempts", len(attempts))
	}
}

// createNestedParallelWorkflow builds a two-level workflow: the outer fork
// splits into na (which forks again) and nb; the nested fork splits into nx
// and ny and merges n = x + y back into branch a; the outer join merges
// total = a.n + b.nb.
func createNestedParallelWorkflow(t *testing.T, db *gorm.DB, scriptNX, scriptNY string) (wfID, start, na, nstart, nx, ny, nend, nb, end, done string) {
	t.Helper()
	start = newEngineID()
	na = newEngineID()
	nstart = newEngineID()
	nx = newEngineID()
	ny = newEngineID()
	nend = newEngineID()
	nb = newEngineID()
	end = newEngineID()
	done = newEngineID()
	wfID = createWorkflow(t, db, start,
		nodeJSON(start, "parallel_start", "fork", "", "", "", map[string]any{
			"branches":             map[string]any{"a": na, "b": nb},
			"parallel_end_node_id": end,
		}),
		nodeJSON(na, "script", "na", "context.na = 1", nstart, "", nil),
		nodeJSON(nstart, "parallel_start", "nested-fork", "", "", "", map[string]any{
			"branches":             map[string]any{"x": nx, "y": ny},
			"parallel_end_node_id": nend,
		}),
		nodeJSON(nx, "script", "nx", scriptNX, nend, "", nil),
		nodeJSON(ny, "script", "ny", scriptNY, nend, "", nil),
		nodeJSON(nend, "parallel_end", "nested-join", "", end, "", map[string]any{
			"combining_script": `context.n = branch["x"].context.x + branch["y"].context.y`,
		}),
		nodeJSON(nb, "script", "nb", "context.nb = 2", end, "", nil),
		nodeJSON(end, "parallel_end", "join", "", done, "", map[string]any{
			"combining_script": `context.total = branch["a"].context.n + branch["b"].context.nb`,
		}),
		nodeJSON(done, "script", "done", "context.done = true", "", "", nil),
	)
	return wfID, start, na, nstart, nx, ny, nend, nb, end, done
}

// driveToNestedFork runs the outer fork, branch a through na, and the nested
// fork, returning the parked parent, the nested execution, and its children.
func driveToNestedFork(t *testing.T, db *gorm.DB, e *engine.Engine, instanceID, branchAID string) (model.ParallelBranch, model.ParallelExecution, []model.ParallelBranch) {
	t.Helper()
	ctx := context.Background()
	prepo := repository.NewParallelRepository(db)
	claimed := claimBranch(t, db, branchAID)
	if err := e.ProcessBranch(ctx, claimed); err != nil {
		t.Fatalf("ProcessBranch(na) error = %v", err)
	}
	claimed = claimBranch(t, db, branchAID)
	if err := e.ProcessBranch(ctx, claimed); err != nil {
		t.Fatalf("ProcessBranch(nested fork) error = %v", err)
	}
	parent, err := prepo.GetBranch(ctx, branchAID)
	if err != nil {
		t.Fatalf("GetBranch() error = %v", err)
	}
	exs, err := prepo.ListExecutions(ctx, instanceID)
	if err != nil {
		t.Fatalf("ListExecutions() error = %v", err)
	}
	var nested *model.ParallelExecution
	for i := range exs {
		if exs[i].ParentBranchID != nil && *exs[i].ParentBranchID == branchAID {
			nested = &exs[i]
		}
	}
	if nested == nil {
		t.Fatalf("no nested execution under branch %s", branchAID)
	}
	children, err := prepo.ListBranches(ctx, nested.ID)
	if err != nil {
		t.Fatalf("ListBranches() error = %v", err)
	}
	return *parent, *nested, children
}

func TestProcessBranchNestedForkParksBranch(t *testing.T) {
	db := setupEngineDB(t)
	ctx := context.Background()
	wfID, start, na, _, _, _, nend, nb, end, _ := createNestedParallelWorkflow(t, db, "context.x = 10", "context.y = 20")
	instanceID := insertInstance(t, db, wfID, start, map[string]any{"v": 0})
	branches := forkBranches(t, db, wfID, instanceID, start, na, nb, end)
	_ = branches

	e, instances := testEngine(t, db, model.DefaultLimits())
	_ = instances
	// forkBranches hand-builds branches for node ids that must match the
	// workflow graph; re-derive branch a (named "a") for the nested drive.
	prepo := repository.NewParallelRepository(db)
	exs, err := prepo.ListExecutions(ctx, instanceID)
	if err != nil || len(exs) != 1 {
		t.Fatalf("ListExecutions() = %d, %v", len(exs), err)
	}
	listed, err := prepo.ListBranches(ctx, exs[0].ID)
	if err != nil || len(listed) != 2 {
		t.Fatalf("ListBranches() = %d, %v", len(listed), err)
	}
	parent, nested, children := driveToNestedFork(t, db, e, instanceID, listed[0].ID)
	if parent.Status != model.ParallelBranchWaiting || parent.WaitingReason != model.WaitingReasonParallel {
		t.Fatalf("parent = %s/%s", parent.Status, parent.WaitingReason)
	}
	frame, _ := model.ParseFrame(parent.Frame)
	if frame.CurrentNodeID != nend {
		t.Fatalf("parent cursor = %q, want nested join", frame.CurrentNodeID)
	}
	var pctx map[string]any
	if err := json.Unmarshal(parent.Context, &pctx); err != nil {
		t.Fatalf("unmarshal parent context: %v", err)
	}
	if pctx["na"] != float64(1) || pctx["v"] != float64(0) {
		t.Fatalf("parent context = %v", pctx)
	}
	if nested.Depth != 2 {
		t.Fatalf("nested depth = %d", nested.Depth)
	}
	if len(children) != 2 {
		t.Fatalf("nested children = %d", len(children))
	}
	for _, c := range children {
		if c.Status != model.ParallelBranchPending {
			t.Fatalf("child %s status = %s", c.Name, c.Status)
		}
	}
	inst, err := instances.GetByID(ctx, instanceID)
	if err != nil {
		t.Fatalf("GetByID() error = %v", err)
	}
	if inst.Status != model.WorkflowWaiting || inst.WaitingReason != model.WaitingReasonParallel {
		t.Fatalf("instance = %s/%s", inst.Status, inst.WaitingReason)
	}
	iframe, _ := model.ParseFrame(inst.Frame)
	if iframe.CurrentNodeID != end {
		t.Fatalf("instance cursor moved to %q", iframe.CurrentNodeID)
	}
}

func TestProcessNestedJoinMergesAndResumes(t *testing.T) {
	db := setupEngineDB(t)
	ctx := context.Background()
	wfID, start, na, _, _, _, _, nb, end, _ := createNestedParallelWorkflow(t, db, "context.x = 10", "context.y = 20")
	instanceID := insertInstance(t, db, wfID, start, map[string]any{"v": 0})
	forkBranches(t, db, wfID, instanceID, start, na, nb, end)

	e, _ := testEngine(t, db, model.DefaultLimits())
	prepo := repository.NewParallelRepository(db)
	exs, err := prepo.ListExecutions(ctx, instanceID)
	if err != nil || len(exs) != 1 {
		t.Fatalf("ListExecutions() = %d, %v", len(exs), err)
	}
	listed, err := prepo.ListBranches(ctx, exs[0].ID)
	if err != nil || len(listed) != 2 {
		t.Fatalf("ListBranches() = %d, %v", len(listed), err)
	}
	parent, nested, children := driveToNestedFork(t, db, e, instanceID, listed[0].ID)
	claimed, err := prepo.ClaimNextBranches(ctx, "test-worker", time.Minute, 10)
	if err != nil {
		t.Fatalf("ClaimNextBranches() error = %v", err)
	}
	byID := map[string]model.ParallelBranch{}
	for _, b := range claimed {
		byID[b.ID] = b
	}
	for _, c := range children {
		got, ok := byID[c.ID]
		if !ok {
			t.Fatalf("child %s was not claimed", c.Name)
		}
		if err := e.ProcessBranch(ctx, got); err != nil {
			t.Fatalf("ProcessBranch(%s) error = %v", c.Name, err)
		}
	}
	// The barrier armed and woke the parent; the nested join merges and
	// resumes the branch past the nested end.
	resumed := claimBranch(t, db, parent.ID)
	if err := e.ProcessBranch(ctx, resumed); err != nil {
		t.Fatalf("ProcessBranch(nested join) error = %v", err)
	}
	got, err := prepo.GetBranch(ctx, parent.ID)
	if err != nil {
		t.Fatalf("GetBranch() error = %v", err)
	}
	if got.Status != model.ParallelBranchWaiting || got.WaitingReason != model.WaitingReasonRunnable {
		t.Fatalf("parent = %s/%s", got.Status, got.WaitingReason)
	}
	frame, _ := model.ParseFrame(got.Frame)
	if frame.CurrentNodeID != end {
		t.Fatalf("parent cursor = %q, want outer join", frame.CurrentNodeID)
	}
	var pctx map[string]any
	if err := json.Unmarshal(got.Context, &pctx); err != nil {
		t.Fatalf("unmarshal parent context: %v", err)
	}
	if pctx["n"] != float64(30) || pctx["na"] != float64(1) {
		t.Fatalf("merged context = %v", pctx)
	}
	exGot, err := prepo.GetExecution(ctx, nested.ID)
	if err != nil {
		t.Fatalf("GetExecution() error = %v", err)
	}
	if exGot.Status != model.ParallelExecutionCompleted {
		t.Fatalf("nested execution = %s", exGot.Status)
	}
	// The resumed branch completes into the outer barrier on its next step.
	final := claimBranch(t, db, parent.ID)
	if err := e.ProcessBranch(ctx, final); err != nil {
		t.Fatalf("ProcessBranch(complete) error = %v", err)
	}
	done, err := prepo.GetBranch(ctx, parent.ID)
	if err != nil {
		t.Fatalf("GetBranch() error = %v", err)
	}
	if done.Status != model.ParallelBranchCompleted {
		t.Fatalf("parent = %s", done.Status)
	}
}

func TestNestedParallelEndToEnd(t *testing.T) {
	db := setupEngineDB(t)
	ctx := context.Background()
	wfID, start, _, _, _, _, _, _, _, _ := createNestedParallelWorkflow(t, db, "context.x = 10", "context.y = 20")
	instanceID := insertInstance(t, db, wfID, start, map[string]any{"v": 0})

	e, _ := testEngine(t, db, model.DefaultLimits())
	inst := driveToTerminal(t, db, e, instanceID)
	if inst.Status != model.WorkflowFinished {
		t.Fatalf("status = %s (%s)", inst.Status, inst.Error)
	}
	var ctxMap map[string]any
	if err := json.Unmarshal(inst.Context, &ctxMap); err != nil {
		t.Fatalf("unmarshal context: %v", err)
	}
	if ctxMap["total"] != float64(32) || ctxMap["v"] != float64(0) || ctxMap["done"] != true {
		t.Fatalf("merged context = %v", ctxMap)
	}
	exs, err := repository.NewParallelRepository(db).ListExecutions(ctx, instanceID)
	if err != nil || len(exs) != 2 {
		t.Fatalf("ListExecutions() = %d, %v", len(exs), err)
	}
	for _, ex := range exs {
		if ex.Status != model.ParallelExecutionCompleted {
			t.Fatalf("execution %+v not completed", ex)
		}
	}
}

func TestNestedBranchFailureClimbsToParent(t *testing.T) {
	db := setupEngineDB(t)
	ctx := context.Background()
	wfID, start, na, _, _, _, _, nb, end, _ := createNestedParallelWorkflow(t, db, `throw "nested-boom"`, "context.y = 20")
	instanceID := insertInstance(t, db, wfID, start, map[string]any{})
	forkBranches(t, db, wfID, instanceID, start, na, nb, end)

	e, instances := testEngine(t, db, model.DefaultLimits())
	prepo := repository.NewParallelRepository(db)
	exs, err := prepo.ListExecutions(ctx, instanceID)
	if err != nil || len(exs) != 1 {
		t.Fatalf("ListExecutions() = %d, %v", len(exs), err)
	}
	listed, err := prepo.ListBranches(ctx, exs[0].ID)
	if err != nil || len(listed) != 2 {
		t.Fatalf("ListBranches() = %d, %v", len(listed), err)
	}
	outer := exs[0]
	parent, nested, children := driveToNestedFork(t, db, e, instanceID, listed[0].ID)
	var leaf model.ParallelBranch
	for _, c := range children {
		if c.Name == "x" {
			leaf = c
		}
	}
	if leaf.ID == "" {
		t.Fatalf("nested child x not found")
	}
	claimed := claimBranch(t, db, leaf.ID)
	if err := e.ProcessBranch(ctx, claimed); err != nil {
		t.Fatalf("ProcessBranch() error = %v", err)
	}
	gotLeaf, _ := prepo.GetBranch(ctx, leaf.ID)
	if gotLeaf.Status != model.ParallelBranchFailed || !strings.Contains(gotLeaf.Error, "nested-boom") {
		t.Fatalf("leaf = %s (%q)", gotLeaf.Status, gotLeaf.Error)
	}
	exGot, _ := prepo.GetExecution(ctx, nested.ID)
	if exGot.Status != model.ParallelExecutionFailed {
		t.Fatalf("nested execution = %s", exGot.Status)
	}
	gotParent, _ := prepo.GetBranch(ctx, parent.ID)
	if gotParent.Status != model.ParallelBranchFailed {
		t.Fatalf("parent branch = %s", gotParent.Status)
	}
	outerGot, _ := prepo.GetExecution(ctx, outer.ID)
	if outerGot.Status != model.ParallelExecutionFailed {
		t.Fatalf("outer execution = %s", outerGot.Status)
	}
	inst, _ := instances.GetByID(ctx, instanceID)
	if inst.Status != model.WorkflowFailed || !strings.Contains(inst.Error, "nested-boom") {
		t.Fatalf("instance = %s (%q)", inst.Status, inst.Error)
	}
	sibling, _ := prepo.GetBranch(ctx, listed[1].ID)
	if sibling.Status != model.ParallelBranchCancelled {
		t.Fatalf("outer sibling = %s", sibling.Status)
	}
	for _, c := range children {
		if c.ID == leaf.ID {
			continue
		}
		got, _ := prepo.GetBranch(ctx, c.ID)
		if got.Status != model.ParallelBranchCancelled {
			t.Fatalf("nested sibling %s = %s", c.Name, got.Status)
		}
	}
}

func TestNestedForkRejectsDepthLimit(t *testing.T) {
	db := setupEngineDB(t)
	ctx := context.Background()
	wfID, start, na, _, _, _, _, nb, end, _ := createNestedParallelWorkflow(t, db, "context.x = 10", "context.y = 20")
	instanceID := insertInstance(t, db, wfID, start, map[string]any{})
	forkBranches(t, db, wfID, instanceID, start, na, nb, end)

	limits := model.DefaultLimits()
	limits.MaxParallelDepth = 1
	e, _ := testEngine(t, db, limits)
	prepo := repository.NewParallelRepository(db)
	exs, err := prepo.ListExecutions(ctx, instanceID)
	if err != nil || len(exs) != 1 {
		t.Fatalf("ListExecutions() = %d, %v", len(exs), err)
	}
	listed, err := prepo.ListBranches(ctx, exs[0].ID)
	if err != nil || len(listed) != 2 {
		t.Fatalf("ListBranches() = %d, %v", len(listed), err)
	}
	claimed := claimBranch(t, db, listed[0].ID)
	if err := e.ProcessBranch(ctx, claimed); err != nil {
		t.Fatalf("ProcessBranch(na) error = %v", err)
	}
	forked := claimBranch(t, db, listed[0].ID)
	if err := e.ProcessBranch(ctx, forked); err != nil {
		t.Fatalf("ProcessBranch(nested fork) error = %v", err)
	}
	got, err := prepo.GetBranch(ctx, listed[0].ID)
	if err != nil {
		t.Fatalf("GetBranch() error = %v", err)
	}
	if got.Status != model.ParallelBranchFailed {
		t.Fatalf("branch status = %s", got.Status)
	}
	if !strings.Contains(got.Error, "at most 1 allowed") {
		t.Fatalf("branch error = %q", got.Error)
	}
	exs, err = prepo.ListExecutions(ctx, instanceID)
	if err != nil {
		t.Fatalf("ListExecutions() error = %v", err)
	}
	if len(exs) != 1 {
		t.Fatalf("rejected fork left %d executions", len(exs))
	}
}

func TestProcessNestedJoinReparksWhenNotReady(t *testing.T) {
	db := setupEngineDB(t)
	ctx := context.Background()
	wfID, start, na, _, _, _, nend, nb, end, _ := createNestedParallelWorkflow(t, db, "context.x = 10", "context.y = 20")
	instanceID := insertInstance(t, db, wfID, start, map[string]any{})
	forkBranches(t, db, wfID, instanceID, start, na, nb, end)

	e, _ := testEngine(t, db, model.DefaultLimits())
	prepo := repository.NewParallelRepository(db)
	exs, err := prepo.ListExecutions(ctx, instanceID)
	if err != nil || len(exs) != 1 {
		t.Fatalf("ListExecutions() = %d, %v", len(exs), err)
	}
	listed, err := prepo.ListBranches(ctx, exs[0].ID)
	if err != nil || len(listed) != 2 {
		t.Fatalf("ListBranches() = %d, %v", len(listed), err)
	}
	parent, nested, _ := driveToNestedFork(t, db, e, instanceID, listed[0].ID)
	// Spuriously wake the parent while nested branches still run.
	if err := db.Exec(`UPDATE parallel_branches SET waiting_reason = '' WHERE id = ?`, parent.ID).Error; err != nil {
		t.Fatalf("wake parent: %v", err)
	}
	woken := claimBranch(t, db, parent.ID)
	if err := e.ProcessBranch(ctx, woken); err != nil {
		t.Fatalf("ProcessBranch() error = %v", err)
	}
	got, err := prepo.GetBranch(ctx, parent.ID)
	if err != nil {
		t.Fatalf("GetBranch() error = %v", err)
	}
	if got.Status != model.ParallelBranchWaiting || got.WaitingReason != model.WaitingReasonParallel {
		t.Fatalf("parent = %s/%s", got.Status, got.WaitingReason)
	}
	frame, _ := model.ParseFrame(got.Frame)
	if frame.CurrentNodeID != nend {
		t.Fatalf("cursor moved to %q", frame.CurrentNodeID)
	}
	exGot, err := prepo.GetExecution(ctx, nested.ID)
	if err != nil {
		t.Fatalf("GetExecution() error = %v", err)
	}
	if exGot.Status != model.ParallelWaitingForBranches {
		t.Fatalf("nested execution = %s", exGot.Status)
	}
}

// instanceEventPayloads groups an instance's event data payloads by type.
func instanceEventPayloads(t *testing.T, db *gorm.DB, instanceID string) map[string][]map[string]any {
	t.Helper()
	events, err := repository.NewInstanceRepository(db).ListEvents(context.Background(), instanceID)
	if err != nil {
		t.Fatalf("ListEvents() error = %v", err)
	}
	byType := map[string][]map[string]any{}
	for _, ev := range events {
		var data map[string]any
		if err := json.Unmarshal(ev.Data, &data); err != nil {
			t.Fatalf("unmarshal %s event data: %v", ev.Type, err)
		}
		byType[ev.Type] = append(byType[ev.Type], data)
	}
	return byType
}

func TestParallelEmitsLifecycleEvents(t *testing.T) {
	db := setupEngineDB(t)
	wfID, start, _, _, end, _ := createParallelWorkflowFull(t, db,
		"context.x = 1", "context.y = 2", `context.total = branch["a"].context.x + branch["b"].context.y`)
	instanceID := insertInstance(t, db, wfID, start, map[string]any{"v": 0})

	e, _ := testEngine(t, db, model.DefaultLimits())
	inst := driveToTerminal(t, db, e, instanceID)
	if inst.Status != model.WorkflowFinished {
		t.Fatalf("status = %s (%s)", inst.Status, inst.Error)
	}
	byType := instanceEventPayloads(t, db, instanceID)
	started := byType["parallel_started"]
	if len(started) != 1 {
		t.Fatalf("parallel_started events = %d, want 1", len(started))
	}
	if started[0]["node_id"] != start || started[0]["start_node_id"] != start || started[0]["end_node_id"] != end {
		t.Fatalf("parallel_started data = %v", started[0])
	}
	if started[0]["branch_count"] != float64(2) {
		t.Fatalf("parallel_started data = %v", started[0])
	}
	exID, _ := started[0]["parallel_execution_id"].(string)
	if exID == "" {
		t.Fatalf("parallel_started data = %v", started[0])
	}
	finished := byType["parallel_branch_finished"]
	if len(finished) != 2 {
		t.Fatalf("parallel_branch_finished events = %d, want 2", len(finished))
	}
	seen := map[string]float64{}
	for _, f := range finished {
		if f["parallel_execution_id"] != exID || f["end_node_id"] != end {
			t.Fatalf("parallel_branch_finished data = %v", f)
		}
		name, _ := f["branch_name"].(string)
		idx, _ := f["branch_index"].(float64)
		if name == "" || f["branch_id"] == "" || f["start_node_id"] == "" {
			t.Fatalf("parallel_branch_finished data = %v", f)
		}
		seen[name] = idx
	}
	if seen["a"] != 0 || seen["b"] != 1 {
		t.Fatalf("finished branch names/indexes = %v", seen)
	}
	completed := byType["parallel_completed"]
	if len(completed) != 1 {
		t.Fatalf("parallel_completed events = %d, want 1", len(completed))
	}
	if completed[0]["node_id"] != end || completed[0]["start_node_id"] != start ||
		completed[0]["end_node_id"] != end || completed[0]["parallel_execution_id"] != exID {
		t.Fatalf("parallel_completed data = %v", completed[0])
	}
}

func TestParallelBranchFailureEmitsEvents(t *testing.T) {
	db := setupEngineDB(t)
	ctx := context.Background()
	wfID, start, a, b, end := createParallelWorkflowWithScripts(t, db, `throw "boom"`, "context.y = 2")
	instanceID := insertInstance(t, db, wfID, start, map[string]any{})
	branches := forkBranches(t, db, wfID, instanceID, start, a, b, end)

	e, _ := testEngine(t, db, model.DefaultLimits())
	claimed := claimBranch(t, db, branches[0].ID)
	if err := e.ProcessBranch(ctx, claimed); err != nil {
		t.Fatalf("ProcessBranch() error = %v", err)
	}
	byType := instanceEventPayloads(t, db, instanceID)
	failed := byType["parallel_branch_failed"]
	if len(failed) != 1 {
		t.Fatalf("parallel_branch_failed events = %d, want 1", len(failed))
	}
	errMsg, _ := failed[0]["error"].(string)
	if !strings.Contains(errMsg, "boom") {
		t.Fatalf("parallel_branch_failed data = %v", failed[0])
	}
	if failed[0]["branch_name"] != "a" || failed[0]["branch_index"] != float64(0) {
		t.Fatalf("parallel_branch_failed data = %v", failed[0])
	}
	exID, _ := failed[0]["parallel_execution_id"].(string)
	if exID == "" || failed[0]["branch_id"] == "" {
		t.Fatalf("parallel_branch_failed data = %v", failed[0])
	}
	for _, f := range byType["parallel_branch_finished"] {
		if f["branch_name"] == "a" {
			t.Fatalf("failed branch a also emitted finished: %v", f)
		}
	}
	pfailed := byType["parallel_failed"]
	if len(pfailed) != 1 || pfailed[0]["parallel_execution_id"] != exID {
		t.Fatalf("parallel_failed events = %v", pfailed)
	}
	if len(byType["workflow_failed"]) != 1 {
		t.Fatalf("workflow_failed events = %d, want 1", len(byType["workflow_failed"]))
	}
}

func TestNestedParallelEmitsScopedEvents(t *testing.T) {
	db := setupEngineDB(t)
	wfID, start, _, _, _, _, _, _, _, _ := createNestedParallelWorkflow(t, db, "context.x = 10", "context.y = 20")
	instanceID := insertInstance(t, db, wfID, start, map[string]any{"v": 0})

	e, _ := testEngine(t, db, model.DefaultLimits())
	inst := driveToTerminal(t, db, e, instanceID)
	if inst.Status != model.WorkflowFinished {
		t.Fatalf("status = %s (%s)", inst.Status, inst.Error)
	}
	byType := instanceEventPayloads(t, db, instanceID)
	if len(byType["parallel_started"]) != 2 {
		t.Fatalf("parallel_started events = %d, want 2", len(byType["parallel_started"]))
	}
	var outer, nested string
	for _, s := range byType["parallel_started"] {
		id, _ := s["parallel_execution_id"].(string)
		if _, scoped := s["branch_id"]; scoped {
			nested = id
		} else {
			outer = id
		}
	}
	if outer == "" || nested == "" || outer == nested {
		t.Fatalf("started executions outer=%q nested=%q", outer, nested)
	}
	if len(byType["parallel_completed"]) != 2 {
		t.Fatalf("parallel_completed events = %d, want 2", len(byType["parallel_completed"]))
	}
	if len(byType["parallel_branch_finished"]) != 4 {
		t.Fatalf("parallel_branch_finished events = %d, want 4", len(byType["parallel_branch_finished"]))
	}
}

func TestStaleBranchCancelEmitsEvent(t *testing.T) {
	db := setupEngineDB(t)
	ctx := context.Background()
	wfID, start, a, b, end := createParallelWorkflowWithScripts(t, db, `throw "boom"`, "context.y = 2")
	instanceID := insertInstance(t, db, wfID, start, map[string]any{})
	branches := forkBranches(t, db, wfID, instanceID, start, a, b, end)

	e, _ := testEngine(t, db, model.DefaultLimits())
	prepo := repository.NewParallelRepository(db)
	claimed, err := prepo.ClaimNextBranches(ctx, "test-worker", time.Minute, 10)
	if err != nil {
		t.Fatalf("ClaimNextBranches() error = %v", err)
	}
	byID := map[string]model.ParallelBranch{}
	for _, c := range claimed {
		byID[c.ID] = c
	}
	// Branch a fails first, resolving the execution; branch b's stale
	// claim then cancels instead of committing.
	if err := e.ProcessBranch(ctx, byID[branches[0].ID]); err != nil {
		t.Fatalf("ProcessBranch(a) error = %v", err)
	}
	if err := e.ProcessBranch(ctx, byID[branches[1].ID]); err != nil {
		t.Fatalf("ProcessBranch(b) error = %v", err)
	}
	byType := instanceEventPayloads(t, db, instanceID)
	cancelled := byType["parallel_branch_cancelled"]
	if len(cancelled) != 1 {
		t.Fatalf("parallel_branch_cancelled events = %d, want 1", len(cancelled))
	}
	if cancelled[0]["branch_name"] != "b" {
		t.Fatalf("parallel_branch_cancelled data = %v", cancelled[0])
	}
}

// crashBranchClaim simulates a worker that claimed a branch and died
// mid-node: the branch stays running with an expired lease and a running
// attempt behind, so the next claim recovers it.
func crashBranchClaim(t *testing.T, db *gorm.DB, branchID string, attempt model.NodeInstance) {
	t.Helper()
	ctx := context.Background()
	prepo := repository.NewParallelRepository(db)
	claimed, err := prepo.ClaimNextBranches(ctx, "ghost-worker", time.Millisecond, 10)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, b := range claimed {
		if b.ID == branchID {
			found = true
		}
	}
	if !found {
		t.Fatalf("ghost claim missed branch %s", branchID)
	}
	if err := repository.NewInstanceRepository(db).InsertNodeInstance(ctx, attempt); err != nil {
		t.Fatal(err)
	}
	if err := db.Exec("UPDATE parallel_branches SET lease_expiry = now() - interval '10 seconds' WHERE id = ?", branchID).Error; err != nil {
		t.Fatal(err)
	}
}

func TestProcessBranchRecoversExpiredLease(t *testing.T) {
	db := setupEngineDB(t)
	ctx := context.Background()
	wfID, start, a, _, _ := createParallelWorkflow(t, db)
	instanceID := insertInstance(t, db, wfID, start, map[string]any{"v": 0})

	e, instances := testEngine(t, db, model.DefaultLimits())
	claimed, err := instances.ClaimNext(ctx, "test-worker", time.Minute, 1)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("ClaimNext() = %d, %v", len(claimed), err)
	}
	if err := e.Process(ctx, claimed[0]); err != nil {
		t.Fatalf("Process() error = %v", err)
	}
	prepo := repository.NewParallelRepository(db)
	exs, err := prepo.ListExecutions(ctx, instanceID)
	if err != nil || len(exs) != 1 {
		t.Fatalf("ListExecutions() = %d, %v", len(exs), err)
	}
	listed, err := prepo.ListBranches(ctx, exs[0].ID)
	if err != nil || len(listed) != 2 {
		t.Fatalf("ListBranches() = %d, %v", len(listed), err)
	}
	branchAID := listed[0].ID

	now := time.Now().UTC()
	crashBranchClaim(t, db, branchAID, model.NodeInstance{
		ID: newEngineID(), WorkflowInstanceID: instanceID, BranchID: branchAID, NodeID: a,
		NodeDefinitionID: "", Name: "a", Type: "script",
		Attempt: 1, Status: model.NodeRunning,
		ContextBefore: json.RawMessage(`{}`), StartedAt: &now,
		CreatedAt: now, UpdatedAt: now,
	})

	// The next claim picks up the expired lease and the shared recovery
	// requeues the script: attempt 2 runs and the branch completes into
	// the barrier.
	if err := e.ProcessBranch(ctx, claimBranch(t, db, branchAID)); err != nil {
		t.Fatalf("ProcessBranch() error = %v", err)
	}
	got, err := prepo.GetBranch(ctx, branchAID)
	if err != nil {
		t.Fatalf("GetBranch() error = %v", err)
	}
	if got.Status != model.ParallelBranchCompleted {
		t.Fatalf("branch = %s, want completed after recovery", got.Status)
	}
	attempt, err := repository.NewInstanceRepository(db).GetBranchNodeInstanceByNode(ctx, branchAID, a)
	if err != nil {
		t.Fatal(err)
	}
	if attempt.Attempt != 2 || attempt.RecoveryResult != "retried" {
		t.Fatalf("attempt = %+v, want attempt 2 retried", attempt)
	}
}

func TestProcessForkDebugParksPaused(t *testing.T) {
	db := setupEngineDB(t)
	ctx := context.Background()
	wfID, start, _, _, _ := createParallelWorkflow(t, db)
	instanceID := insertDebugInstance(t, db, wfID, start, map[string]any{"v": 0})

	e, instances := testEngine(t, db, model.DefaultLimits())
	claimed, err := instances.ClaimNext(ctx, "test-worker", time.Minute, 1)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("ClaimNext() = %d, %v", len(claimed), err)
	}
	if err := e.Process(ctx, claimed[0]); err != nil {
		t.Fatalf("Process() error = %v", err)
	}
	inst, err := instances.GetByID(ctx, instanceID)
	if err != nil {
		t.Fatalf("GetByID() error = %v", err)
	}
	if inst.Status != model.WorkflowPaused || inst.WaitingReason != model.WaitingReasonParallel {
		t.Fatalf("parent = %s/%s, want paused/parallel", inst.Status, inst.WaitingReason)
	}
	prepo := repository.NewParallelRepository(db)
	exs, err := prepo.ListExecutions(ctx, instanceID)
	if err != nil || len(exs) != 1 {
		t.Fatalf("ListExecutions() = %d, %v", len(exs), err)
	}
	branches, err := prepo.ListBranches(ctx, exs[0].ID)
	if err != nil || len(branches) != 2 {
		t.Fatalf("ListBranches() = %d, %v", len(branches), err)
	}
	for _, br := range branches {
		if br.Status != model.ParallelBranchWaiting || br.WaitingReason != model.WaitingReasonPaused {
			t.Fatalf("branch %s = %s/%s, want waiting/paused", br.Name, br.Status, br.WaitingReason)
		}
	}
	bclaimed, err := prepo.ClaimNextBranches(ctx, "test-worker", time.Minute, 10)
	if err != nil {
		t.Fatalf("ClaimNextBranches() error = %v", err)
	}
	if len(bclaimed) != 0 {
		t.Fatalf("claimed %d paused branches, want 0", len(bclaimed))
	}
}

func TestProcessBranchDebugParksPaused(t *testing.T) {
	db := setupEngineDB(t)
	ctx := context.Background()
	start, s1, s2, b, end, done := newEngineID(), newEngineID(), newEngineID(), newEngineID(), newEngineID(), newEngineID()
	wfID := createWorkflow(t, db, start,
		nodeJSON(start, "parallel_start", "fork", "", "", "", map[string]any{
			"branches":             map[string]any{"a": s1, "b": b},
			"parallel_end_node_id": end,
		}),
		nodeJSON(s1, "script", "s1", "context.s1 = 1", s2, "", nil),
		nodeJSON(s2, "script", "s2", "context.s2 = 2", end, "", nil),
		nodeJSON(b, "script", "b", "context.bb = 3", end, "", nil),
		nodeJSON(end, "parallel_end", "join", "", done, "", map[string]any{
			"combining_script": `context.x = branch["a"].context.s2`,
		}),
		nodeJSON(done, "script", "done", "context.done = true", "", "", nil),
	)
	instanceID := insertDebugInstance(t, db, wfID, start, map[string]any{})

	e, instances := testEngine(t, db, model.DefaultLimits())
	claimed, err := instances.ClaimNext(ctx, "test-worker", time.Minute, 1)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("ClaimNext() = %d, %v", len(claimed), err)
	}
	if err := e.Process(ctx, claimed[0]); err != nil {
		t.Fatalf("Process() error = %v", err)
	}
	prepo := repository.NewParallelRepository(db)
	exs, err := prepo.ListExecutions(ctx, instanceID)
	if err != nil || len(exs) != 1 {
		t.Fatalf("ListExecutions() = %d, %v", len(exs), err)
	}
	listed, err := prepo.ListBranches(ctx, exs[0].ID)
	if err != nil || len(listed) != 2 {
		t.Fatalf("ListBranches() = %d, %v", len(listed), err)
	}
	branchAID := listed[0].ID

	// The first resumed step runs s1 and parks paused on s2.
	wakeBranch(t, db, branchAID)
	if err := e.ProcessBranch(ctx, claimBranch(t, db, branchAID)); err != nil {
		t.Fatalf("ProcessBranch(s1) error = %v", err)
	}
	got, err := prepo.GetBranch(ctx, branchAID)
	if err != nil {
		t.Fatalf("GetBranch() error = %v", err)
	}
	if got.Status != model.ParallelBranchWaiting || got.WaitingReason != model.WaitingReasonPaused {
		t.Fatalf("branch = %s/%s, want waiting/paused", got.Status, got.WaitingReason)
	}
	frame, _ := model.ParseFrame(got.Frame)
	if frame.CurrentNodeID != s2 {
		t.Fatalf("branch cursor = %q, want s2", frame.CurrentNodeID)
	}
	var bctx map[string]any
	if err := json.Unmarshal(got.Context, &bctx); err != nil {
		t.Fatalf("unmarshal branch context: %v", err)
	}
	if bctx["s1"] != float64(1) {
		t.Fatalf("branch context = %v, want s1 ran", bctx)
	}
	// The second resumed step runs s2 and completes into the barrier.
	wakeBranch(t, db, branchAID)
	if err := e.ProcessBranch(ctx, claimBranch(t, db, branchAID)); err != nil {
		t.Fatalf("ProcessBranch(s2) error = %v", err)
	}
	doneBranch, err := prepo.GetBranch(ctx, branchAID)
	if err != nil {
		t.Fatalf("GetBranch() error = %v", err)
	}
	if doneBranch.Status != model.ParallelBranchCompleted {
		t.Fatalf("branch = %s, want completed", doneBranch.Status)
	}
}

func TestProcessNestedJoinDebugParksPaused(t *testing.T) {
	db := setupEngineDB(t)
	ctx := context.Background()
	wfID, start, na, _, _, _, _, nb, end, _ := createNestedParallelWorkflow(t, db, "context.x = 10", "context.y = 20")
	instanceID := insertDebugInstance(t, db, wfID, start, map[string]any{"v": 0})

	e, instances := testEngine(t, db, model.DefaultLimits())
	prepo := repository.NewParallelRepository(db)
	claimed, err := instances.ClaimNext(ctx, "test-worker", time.Minute, 1)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("ClaimNext() = %d, %v", len(claimed), err)
	}
	if err := e.Process(ctx, claimed[0]); err != nil {
		t.Fatalf("Process() error = %v", err)
	}
	_ = na
	_ = nb
	exs, err := prepo.ListExecutions(ctx, instanceID)
	if err != nil || len(exs) != 1 {
		t.Fatalf("ListExecutions() = %d, %v", len(exs), err)
	}
	listed, err := prepo.ListBranches(ctx, exs[0].ID)
	if err != nil || len(listed) != 2 {
		t.Fatalf("ListBranches() = %d, %v", len(listed), err)
	}
	branchAID := listed[0].ID

	// Step branch a to its nested fork: na runs and parks paused, then
	// the fork parks the owner barrier-waiting with paused children.
	wakeBranch(t, db, branchAID)
	if err := e.ProcessBranch(ctx, claimBranch(t, db, branchAID)); err != nil {
		t.Fatalf("ProcessBranch(na) error = %v", err)
	}
	wakeBranch(t, db, branchAID)
	if err := e.ProcessBranch(ctx, claimBranch(t, db, branchAID)); err != nil {
		t.Fatalf("ProcessBranch(nested fork) error = %v", err)
	}
	owner, err := prepo.GetBranch(ctx, branchAID)
	if err != nil {
		t.Fatalf("GetBranch() error = %v", err)
	}
	if owner.Status != model.ParallelBranchWaiting || owner.WaitingReason != model.WaitingReasonParallel {
		t.Fatalf("owner = %s/%s, want waiting/parallel", owner.Status, owner.WaitingReason)
	}
	exs, err = prepo.ListExecutions(ctx, instanceID)
	if err != nil {
		t.Fatalf("ListExecutions() error = %v", err)
	}
	var nested *model.ParallelExecution
	for i := range exs {
		if exs[i].ParentBranchID != nil && *exs[i].ParentBranchID == branchAID {
			nested = &exs[i]
		}
	}
	if nested == nil {
		t.Fatalf("no nested execution under branch %s", branchAID)
	}
	children, err := prepo.ListBranches(ctx, nested.ID)
	if err != nil || len(children) != 2 {
		t.Fatalf("ListBranches() = %d, %v", len(children), err)
	}
	for _, c := range children {
		wakeBranch(t, db, c.ID)
		if err := e.ProcessBranch(ctx, claimBranch(t, db, c.ID)); err != nil {
			t.Fatalf("ProcessBranch(%s) error = %v", c.Name, err)
		}
	}
	// The barrier wakes the owner runnable; the nested join runs, then
	// parks the owner paused past the nested end.
	if err := e.ProcessBranch(ctx, claimBranch(t, db, branchAID)); err != nil {
		t.Fatalf("ProcessBranch(nested join) error = %v", err)
	}
	joined, err := prepo.GetBranch(ctx, branchAID)
	if err != nil {
		t.Fatalf("GetBranch() error = %v", err)
	}
	if joined.Status != model.ParallelBranchWaiting || joined.WaitingReason != model.WaitingReasonPaused {
		t.Fatalf("owner = %s/%s, want waiting/paused", joined.Status, joined.WaitingReason)
	}
	frame, _ := model.ParseFrame(joined.Frame)
	if frame.CurrentNodeID != end {
		t.Fatalf("owner cursor = %q, want outer join", frame.CurrentNodeID)
	}
	exGot, err := prepo.GetExecution(ctx, nested.ID)
	if err != nil {
		t.Fatalf("GetExecution() error = %v", err)
	}
	if exGot.Status != model.ParallelExecutionCompleted {
		t.Fatalf("nested execution = %s", exGot.Status)
	}
}
