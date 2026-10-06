package repository_test

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/simpwf/workflow-engine/internal/workflow/model"
	"github.com/simpwf/workflow-engine/internal/workflow/repository"
	"gorm.io/gorm"
)

func TestParallelMappersRoundTrip(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Microsecond)
	parent := "01950000-0000-7000-8000-000000000099"
	ex := model.ParallelExecution{
		ID:             "01950000-0000-7000-8000-000000000001",
		InstanceID:     "01950000-0000-7000-8000-000000000002",
		ParentBranchID: &parent,
		Depth:          2,
		StartNodeID:    "01950000-0000-7000-8000-000000000003",
		EndNodeID:      "01950000-0000-7000-8000-000000000004",
		Status:         model.ParallelWaitingForBranches,
		BranchCount:    2,
		CompletedCount: 1,
		CreatedAt:      now,
		UpdatedAt:      now,
	}
	if got := repository.ParallelExecutionFromModel(repository.ParallelExecutionToModel(ex)); !reflect.DeepEqual(ex, got) {
		t.Fatalf("execution round trip diff:\ngot  %+v\nwant %+v", got, ex)
	}

	br := model.ParallelBranch{
		ID:                  "01950000-0000-7000-8000-000000000005",
		ParallelExecutionID: ex.ID,
		InstanceID:          ex.InstanceID,
		Name:                "orders",
		BranchIndex:         1,
		StartNodeID:         "01950000-0000-7000-8000-000000000006",
		Frame:               json.RawMessage(`{"current_node_id":"01950000-0000-7000-8000-000000000006"}`),
		Context:             json.RawMessage(`{"a":1}`),
		Counters:            json.RawMessage(`{"total":1}`),
		Status:              model.ParallelBranchRunning,
		Revision:            3,
		LeasedBy:            "worker-1",
		LeaseExpiry:         now.Add(time.Minute),
		CreatedAt:           now,
		UpdatedAt:           now,
	}
	if got := repository.ParallelBranchFromModel(repository.ParallelBranchToModel(br)); !reflect.DeepEqual(br, got) {
		t.Fatalf("branch round trip diff:\ngot  %+v\nwant %+v", got, br)
	}
}

const (
	parallelTestStart = "01950000-0000-7000-8000-0000000000a1"
	parallelTestEnd   = "01950000-0000-7000-8000-0000000000a2"
	parallelTestNodeA = "01950000-0000-7000-8000-0000000000a3"
	parallelTestNodeB = "01950000-0000-7000-8000-0000000000a4"
)

// assertJSON compares raw JSON semantically: Postgres JSONB normalizes
// whitespace on read-back, so byte comparison is meaningless.
func assertJSON(t *testing.T, raw json.RawMessage, want string) {
	t.Helper()
	var gotAny, wantAny any
	if err := json.Unmarshal(raw, &gotAny); err != nil {
		t.Fatalf("unmarshal got: %v", err)
	}
	if err := json.Unmarshal([]byte(want), &wantAny); err != nil {
		t.Fatalf("unmarshal want: %v", err)
	}
	if !reflect.DeepEqual(gotAny, wantAny) {
		t.Fatalf("json = %s, want %s", raw, want)
	}
}

// truncateParallel isolates parallel tests on the shared database: stale
// pending branches from earlier runs would otherwise fill the claim window.
func truncateParallel(t *testing.T, db *gorm.DB) {
	t.Helper()
	if err := db.Exec(`TRUNCATE TABLE parallel_branches, parallel_executions RESTART IDENTITY`).Error; err != nil {
		t.Fatalf("truncate parallel tables: %v", err)
	}
}

func insertLeasedInstance(t *testing.T, db *gorm.DB, id string) {
	t.Helper()
	w := newTestInstance(id, model.WorkflowRunning, model.WaitingReasonRunnable)
	w.LeasedBy = "worker-1"
	w.LeaseExpiry = time.Now().UTC().Add(time.Minute)
	insertInstance(t, db, w)
}

func parallelFork(id string) repository.ForkParallel {
	endFrame := model.NewFrame(parallelTestEnd)
	return repository.ForkParallel{
		InstanceID:           id,
		WorkflowDefinitionID: fixtureWorkflowDefID,
		WorkerID:             "worker-1",
		Revision:             0,
		Depth:                1,
		StartNodeID:          parallelTestStart,
		EndNodeID:            parallelTestEnd,
		Frame:                endFrame,
		Context:              json.RawMessage(`{"v":1}`),
		Branches: []repository.ForkBranch{
			{Name: "a", BranchIndex: 0, StartNodeID: parallelTestNodeA, Frame: model.NewFrame(parallelTestNodeA), Context: json.RawMessage(`{"v":1}`)},
			{Name: "b", BranchIndex: 1, StartNodeID: parallelTestNodeB, Frame: model.NewFrame(parallelTestNodeB), Context: json.RawMessage(`{"v":1}`)},
		},
	}
}

func parallelDebugFork(id string) repository.ForkParallel {
	f := parallelFork(id)
	f.Debug = true
	return f
}

func nestedDebugForkFor(id string, parent model.ParallelBranch) repository.ForkParallel {
	f := nestedForkFor(id, parent)
	f.Debug = true
	return f
}

func TestForkCreatesExecutionAndBranches(t *testing.T) {
	db := setupTestDB(t)
	seedInstanceFixture(t, db)
	truncateParallel(t, db)
	ctx := context.Background()
	id := "01950000-0000-7000-8000-0000000000b1"
	insertLeasedInstance(t, db, id)

	repo := repository.NewParallelRepository(db)
	ex, branches, err := repo.Fork(ctx, parallelFork(id))
	if err != nil {
		t.Fatalf("Fork() error = %v", err)
	}
	if ex.Status != model.ParallelWaitingForBranches || ex.BranchCount != 2 || ex.CompletedCount != 0 {
		t.Fatalf("execution = %+v", ex)
	}
	if len(branches) != 2 || branches[0].Name != "a" || branches[1].Name != "b" {
		t.Fatalf("branches = %+v", branches)
	}
	for _, b := range branches {
		if b.Status != model.ParallelBranchPending || b.ParallelExecutionID != ex.ID || b.InstanceID != id {
			t.Fatalf("branch = %+v", b)
		}
	}
	got, err := repository.NewInstanceRepository(db).GetByID(ctx, id)
	if err != nil {
		t.Fatalf("GetByID() error = %v", err)
	}
	if got.Status != model.WorkflowWaiting || got.WaitingReason != model.WaitingReasonParallel {
		t.Fatalf("parent = %s/%s", got.Status, got.WaitingReason)
	}
	frame, _ := model.ParseFrame(got.Frame)
	if frame.CurrentNodeID != parallelTestEnd {
		t.Fatalf("parent cursor = %q", frame.CurrentNodeID)
	}
	assertJSON(t, got.Context, `{"v":1}`)
}

func TestForkFencesStaleWorker(t *testing.T) {
	db := setupTestDB(t)
	seedInstanceFixture(t, db)
	truncateParallel(t, db)
	ctx := context.Background()
	id := "01950000-0000-7000-8000-0000000000b2"
	insertLeasedInstance(t, db, id)

	repo := repository.NewParallelRepository(db)
	badRev := parallelFork(id)
	badRev.Revision = 99
	if _, _, err := repo.Fork(ctx, badRev); !errors.Is(err, repository.ErrRevisionConflict) {
		t.Fatalf("stale revision error = %v", err)
	}
	badWorker := parallelFork(id)
	badWorker.WorkerID = "worker-2"
	if _, _, err := repo.Fork(ctx, badWorker); !errors.Is(err, repository.ErrLeaseLost) {
		t.Fatalf("stolen lease error = %v", err)
	}
	var n int64
	if err := db.Model(&repository.ParallelExecutionModel{}).Where("instance_id = ?", id).Count(&n).Error; err != nil {
		t.Fatalf("count executions: %v", err)
	}
	if n != 0 {
		t.Fatalf("fenced fork left %d executions", n)
	}
}

func TestClaimNextBranchesExclusive(t *testing.T) {
	db := setupTestDB(t)
	seedInstanceFixture(t, db)
	truncateParallel(t, db)
	ctx := context.Background()
	id := "01950000-0000-7000-8000-0000000000b3"
	insertLeasedInstance(t, db, id)

	repo := repository.NewParallelRepository(db)
	if _, _, err := repo.Fork(ctx, parallelFork(id)); err != nil {
		t.Fatalf("Fork() error = %v", err)
	}
	got, err := repo.ClaimNextBranches(ctx, "worker-1", time.Minute, 10)
	if err != nil {
		t.Fatalf("ClaimNextBranches() error = %v", err)
	}
	mine := 0
	for _, b := range got {
		if b.InstanceID == id {
			mine++
			if b.Status != model.ParallelBranchRunning || b.LeasedBy != "worker-1" {
				t.Fatalf("claimed branch = %+v", b)
			}
		}
	}
	if mine != 2 {
		t.Fatalf("worker-1 claimed %d branches of %s, want 2", mine, id)
	}
	again, err := repo.ClaimNextBranches(ctx, "worker-2", time.Minute, 10)
	if err != nil {
		t.Fatalf("ClaimNextBranches() error = %v", err)
	}
	for _, b := range again {
		if b.InstanceID == id {
			t.Fatalf("worker-2 claimed branch %+v already held by worker-1", b)
		}
	}
}

func TestCheckpointBranchFences(t *testing.T) {
	db := setupTestDB(t)
	seedInstanceFixture(t, db)
	truncateParallel(t, db)
	ctx := context.Background()
	id := "01950000-0000-7000-8000-0000000000b4"
	insertLeasedInstance(t, db, id)

	repo := repository.NewParallelRepository(db)
	if _, _, err := repo.Fork(ctx, parallelFork(id)); err != nil {
		t.Fatalf("Fork() error = %v", err)
	}
	claimed, err := repo.ClaimNextBranches(ctx, "worker-1", time.Minute, 10)
	if err != nil {
		t.Fatalf("ClaimNextBranches() error = %v", err)
	}
	var branch model.ParallelBranch
	for _, b := range claimed {
		if b.InstanceID == id {
			branch = b
		}
	}
	if branch.ID == "" {
		t.Fatalf("no branch claimed for %s", id)
	}
	cp := repository.BranchCheckpoint{
		BranchID: branch.ID, WorkerID: "worker-1", Revision: branch.Revision,
		Status: model.ParallelBranchWaiting, Frame: model.NewFrame(parallelTestEnd),
		Context: json.RawMessage(`{"v":2}`),
	}
	if err := repo.CheckpointBranch(ctx, cp); err != nil {
		t.Fatalf("CheckpointBranch() error = %v", err)
	}
	got, err := repo.GetBranch(ctx, branch.ID)
	if err != nil {
		t.Fatalf("GetBranch() error = %v", err)
	}
	if got.Status != model.ParallelBranchWaiting || got.LeasedBy != "" {
		t.Fatalf("checkpointed branch = %+v", got)
	}
	assertJSON(t, got.Context, `{"v":2}`)
	stale := cp
	stale.Revision = branch.Revision
	if err := repo.CheckpointBranch(ctx, stale); !errors.Is(err, repository.ErrLeaseLost) && !errors.Is(err, repository.ErrRevisionConflict) {
		t.Fatalf("stale checkpoint error = %v", err)
	}
}

func completionFor(t *testing.T, b model.ParallelBranch, worker string) repository.BranchCompletion {
	t.Helper()
	frame, err := model.ParseFrame(b.Frame)
	if err != nil {
		t.Fatalf("ParseFrame() error = %v", err)
	}
	counters, err := model.ParseCounters(b.Counters)
	if err != nil {
		t.Fatalf("ParseCounters() error = %v", err)
	}
	return repository.BranchCompletion{
		BranchID: b.ID, WorkerID: worker, Revision: b.Revision,
		Frame: frame, Counters: counters, Context: b.Context,
	}
}

func completeOne(t *testing.T, repo repository.ParallelRepository, db *gorm.DB, instanceID, worker string) (string, bool) {
	t.Helper()
	ctx := context.Background()
	claimed, err := repo.ClaimNextBranches(ctx, worker, time.Minute, 10)
	if err != nil {
		t.Fatalf("ClaimNextBranches() error = %v", err)
	}
	for _, b := range claimed {
		if b.InstanceID != instanceID {
			continue
		}
		ready, err := repo.CompleteBranch(ctx, completionFor(t, b, worker))
		if err != nil {
			t.Fatalf("CompleteBranch() error = %v", err)
		}
		return b.ID, ready
	}
	t.Fatalf("no claimable branch for %s", instanceID)
	return "", false
}

func TestCompleteBranchBarrier(t *testing.T) {
	db := setupTestDB(t)
	seedInstanceFixture(t, db)
	truncateParallel(t, db)
	ctx := context.Background()
	id := "01950000-0000-7000-8000-0000000000b5"
	insertLeasedInstance(t, db, id)

	repo := repository.NewParallelRepository(db)
	ex, _, err := repo.Fork(ctx, parallelFork(id))
	if err != nil {
		t.Fatalf("Fork() error = %v", err)
	}
	claimed, err := repo.ClaimNextBranches(ctx, "worker-1", time.Minute, 10)
	if err != nil {
		t.Fatalf("ClaimNextBranches() error = %v", err)
	}
	var mine []model.ParallelBranch
	for _, b := range claimed {
		if b.InstanceID == id {
			mine = append(mine, b)
		}
	}
	if len(mine) != 2 {
		t.Fatalf("claimed %d branches, want 2", len(mine))
	}
	if ready, err := repo.CompleteBranch(ctx, completionFor(t, mine[0], "worker-1")); err != nil || ready {
		t.Fatalf("first CompleteBranch() = (%v, %v)", ready, err)
	}
	got, _ := repository.NewInstanceRepository(db).GetByID(ctx, id)
	if got.WaitingReason != model.WaitingReasonParallel {
		t.Fatalf("parent woken early: %s", got.WaitingReason)
	}
	if ready, err := repo.CompleteBranch(ctx, completionFor(t, mine[1], "worker-1")); err != nil || !ready {
		t.Fatalf("last CompleteBranch() = (%v, %v)", ready, err)
	}
	exGot, err := repo.GetExecution(ctx, ex.ID)
	if err != nil {
		t.Fatalf("GetExecution() error = %v", err)
	}
	if exGot.Status != model.ParallelReadyToJoin || exGot.CompletedCount != 2 {
		t.Fatalf("execution = %+v", exGot)
	}
	got, _ = repository.NewInstanceRepository(db).GetByID(ctx, id)
	if got.Status != model.WorkflowWaiting || got.WaitingReason != model.WaitingReasonRunnable {
		t.Fatalf("parent = %s/%s", got.Status, got.WaitingReason)
	}
}

func TestCompleteBranchRaceArmsJoinOnce(t *testing.T) {
	db := setupTestDB(t)
	seedInstanceFixture(t, db)
	truncateParallel(t, db)
	ctx := context.Background()
	id := "01950000-0000-7000-8000-0000000000b6"
	insertLeasedInstance(t, db, id)

	repo := repository.NewParallelRepository(db)
	ex, branches, err := repo.Fork(ctx, parallelFork(id))
	if err != nil {
		t.Fatalf("Fork() error = %v", err)
	}
	claimed, err := repo.ClaimNextBranches(ctx, "worker-1", time.Minute, 10)
	if err != nil {
		t.Fatalf("ClaimNextBranches() error = %v", err)
	}
	byID := map[string]model.ParallelBranch{}
	for _, b := range claimed {
		byID[b.ID] = b
	}
	var wg sync.WaitGroup
	ready := make([]bool, len(branches))
	errs := make([]error, len(branches))
	for i, b := range branches {
		wg.Add(1)
		go func(i int, b model.ParallelBranch) {
			defer wg.Done()
			c := byID[b.ID]
			frame, _ := model.ParseFrame(c.Frame)
			counters, _ := model.ParseCounters(c.Counters)
			ready[i], errs[i] = repo.CompleteBranch(ctx, repository.BranchCompletion{
				BranchID: b.ID, WorkerID: "worker-1", Revision: c.Revision,
				Frame: frame, Counters: counters, Context: c.Context,
			})
		}(i, b)
	}
	wg.Wait()
	armed := 0
	for i := range branches {
		if errs[i] != nil {
			t.Fatalf("CompleteBranch(%d) error = %v", i, errs[i])
		}
		if ready[i] {
			armed++
		}
	}
	if armed != 1 {
		t.Fatalf("join armed %d times, want exactly once", armed)
	}
	exGot, _ := repo.GetExecution(ctx, ex.ID)
	if exGot.CompletedCount != 2 || exGot.Status != model.ParallelReadyToJoin {
		t.Fatalf("execution = %+v", exGot)
	}
}

func TestNodeInstanceBranchScoping(t *testing.T) {
	db := setupTestDB(t)
	seedInstanceFixture(t, db)
	truncateParallel(t, db)
	ctx := context.Background()
	id := "01950000-0000-7000-8000-0000000000c1"
	insertLeasedInstance(t, db, id)

	repo := repository.NewParallelRepository(db)
	instances := repository.NewInstanceRepository(db)
	_, branches, err := repo.Fork(ctx, parallelFork(id))
	if err != nil {
		t.Fatalf("Fork() error = %v", err)
	}
	parent := model.NodeInstance{
		ID: "01950000-0000-7000-8000-0000000000c2", WorkflowInstanceID: id,
		NodeID: parallelTestNodeA, Name: "a", Type: "script", Status: model.NodeRunning,
	}
	if err := instances.InsertNodeInstance(ctx, parent); err != nil {
		t.Fatalf("InsertNodeInstance() error = %v", err)
	}
	branchAttempt := parent
	branchAttempt.ID = "01950000-0000-7000-8000-0000000000c3"
	branchAttempt.BranchID = branches[0].ID
	if err := instances.InsertNodeInstance(ctx, branchAttempt); err != nil {
		t.Fatalf("InsertNodeInstance() error = %v", err)
	}
	got, err := instances.GetNodeInstanceByNode(ctx, id, parallelTestNodeA)
	if err != nil || got.BranchID != "" {
		t.Fatalf("GetNodeInstanceByNode() = %+v, %v", got, err)
	}
	gotBranch, err := instances.GetBranchNodeInstanceByNode(ctx, branches[0].ID, parallelTestNodeA)
	if err != nil || gotBranch.ID != branchAttempt.ID {
		t.Fatalf("GetBranchNodeInstanceByNode() = %+v, %v", gotBranch, err)
	}
	running, err := instances.GetRunningNodeInstance(ctx, id)
	if err != nil || running.BranchID != "" {
		t.Fatalf("GetRunningNodeInstance() = %+v, %v", running, err)
	}
	runningBranch, err := instances.GetRunningBranchNodeInstance(ctx, branches[0].ID)
	if err != nil || runningBranch.ID != branchAttempt.ID {
		t.Fatalf("GetRunningBranchNodeInstance() = %+v, %v", runningBranch, err)
	}
}

func TestCompleteBranchWakesParentBranch(t *testing.T) {
	db := setupTestDB(t)
	seedInstanceFixture(t, db)
	truncateParallel(t, db)
	ctx := context.Background()
	id := "01950000-0000-7000-8000-0000000000c4"
	insertLeasedInstance(t, db, id)

	repo := repository.NewParallelRepository(db)
	_, branches, err := repo.Fork(ctx, parallelFork(id))
	if err != nil {
		t.Fatalf("Fork() error = %v", err)
	}
	parent := claimBranchByID(t, repo, ctx, branches[0].ID)
	f := nestedForkFor(id, parent)
	f.Branches = f.Branches[:1]
	_, nestedChildren, err := repo.Fork(ctx, f)
	if err != nil {
		t.Fatalf("nested Fork() error = %v", err)
	}
	if len(nestedChildren) != 1 {
		t.Fatalf("nested branches = %d, want 1", len(nestedChildren))
	}
	claimed, err := repo.ClaimNextBranches(ctx, "worker-1", time.Minute, 10)
	if err != nil {
		t.Fatalf("ClaimNextBranches() error = %v", err)
	}
	var target model.ParallelBranch
	for _, b := range claimed {
		if b.ID == nestedChildren[0].ID {
			target = b
		}
		if b.ID == parent.ID {
			t.Fatalf("parked parent branch %s was claimed", parent.ID)
		}
	}
	if target.ID == "" {
		t.Fatalf("nested branch was not claimed")
	}
	ready, err := repo.CompleteBranch(ctx, completionFor(t, target, "worker-1"))
	if err != nil || !ready {
		t.Fatalf("CompleteBranch() = (%v, %v)", ready, err)
	}
	gotParent, err := repo.GetBranch(ctx, parent.ID)
	if err != nil {
		t.Fatalf("GetBranch() error = %v", err)
	}
	if gotParent.WaitingReason != model.WaitingReasonRunnable {
		t.Fatalf("parent branch reason = %q", gotParent.WaitingReason)
	}
	inst, _ := repository.NewInstanceRepository(db).GetByID(ctx, id)
	if inst.WaitingReason != model.WaitingReasonParallel {
		t.Fatalf("instance woken early: %s", inst.WaitingReason)
	}
}

func TestJoinParallelCompletesExactlyOnce(t *testing.T) {
	db := setupTestDB(t)
	seedInstanceFixture(t, db)
	truncateParallel(t, db)
	ctx := context.Background()
	id := "01950000-0000-7000-8000-0000000000d1"
	insertLeasedInstance(t, db, id)

	repo := repository.NewParallelRepository(db)
	instances := repository.NewInstanceRepository(db)
	ex, branches, err := repo.Fork(ctx, parallelFork(id))
	if err != nil {
		t.Fatalf("Fork() error = %v", err)
	}
	claimed, err := repo.ClaimNextBranches(ctx, "worker-1", time.Minute, 10)
	if err != nil {
		t.Fatalf("ClaimNextBranches() error = %v", err)
	}
	byID := map[string]model.ParallelBranch{}
	for _, b := range claimed {
		byID[b.ID] = b
	}
	for _, b := range branches {
		if _, err := repo.CompleteBranch(ctx, completionFor(t, byID[b.ID], "worker-1")); err != nil {
			t.Fatalf("CompleteBranch() error = %v", err)
		}
	}
	// Parent woken by the barrier; claim it for the join.
	pclaimed, err := instances.ClaimNext(ctx, "worker-1", time.Minute, 1)
	if err != nil || len(pclaimed) != 1 || pclaimed[0].ID != id {
		t.Fatalf("ClaimNext() = %d, %v", len(pclaimed), err)
	}
	parent := pclaimed[0]
	joinFrame := model.NewFrame("01950000-0000-7000-8000-0000000000d9")
	err = repo.JoinParallel(ctx, repository.JoinCheckpoint{
		Checkpoint: repository.Checkpoint{
			InstanceID: id, WorkerID: "worker-1", Revision: parent.Revision,
			WorkflowDefinitionID: fixtureWorkflowDefID,
			FromStatus:           model.WorkflowRunning,
			Status:               model.WorkflowWaiting,
			Frame:                joinFrame,
			Context:              json.RawMessage(`{"total":3}`),
		},
		ExecutionID: ex.ID,
	})
	if err != nil {
		t.Fatalf("JoinParallel() error = %v", err)
	}
	exGot, _ := repo.GetExecution(ctx, ex.ID)
	if exGot.Status != model.ParallelExecutionCompleted {
		t.Fatalf("execution = %s", exGot.Status)
	}
	instGot, _ := instances.GetByID(ctx, id)
	frame, _ := model.ParseFrame(instGot.Frame)
	if frame.CurrentNodeID != "01950000-0000-7000-8000-0000000000d9" {
		t.Fatalf("parent cursor = %q", frame.CurrentNodeID)
	}
	assertJSON(t, instGot.Context, `{"total":3}`)
	// A second join against the completed execution conflicts.
	if err := repo.JoinParallel(ctx, repository.JoinCheckpoint{
		Checkpoint: repository.Checkpoint{
			InstanceID: id, WorkerID: "worker-1", Revision: instGot.Revision,
			WorkflowDefinitionID: fixtureWorkflowDefID,
			FromStatus:           model.WorkflowWaiting,
			Status:               model.WorkflowWaiting,
			Frame:                joinFrame,
			Context:              json.RawMessage(`{"total":3}`),
		},
		ExecutionID: ex.ID,
	}); !errors.Is(err, repository.ErrStatusConflict) {
		t.Fatalf("second JoinParallel() = %v", err)
	}
}

func TestFailBranchCancelsSiblingsAndFailsParent(t *testing.T) {
	db := setupTestDB(t)
	seedInstanceFixture(t, db)
	truncateParallel(t, db)
	ctx := context.Background()
	id := "01950000-0000-7000-8000-0000000000e1"
	insertLeasedInstance(t, db, id)

	repo := repository.NewParallelRepository(db)
	instances := repository.NewInstanceRepository(db)
	ex, branches, err := repo.Fork(ctx, parallelFork(id))
	if err != nil {
		t.Fatalf("Fork() error = %v", err)
	}
	claimed, err := repo.ClaimNextBranches(ctx, "worker-1", time.Minute, 10)
	if err != nil {
		t.Fatalf("ClaimNextBranches() error = %v", err)
	}
	byID := map[string]model.ParallelBranch{}
	for _, b := range claimed {
		byID[b.ID] = b
	}
	a, b := branches[0], branches[1]
	failedParent, err := repo.FailBranch(ctx, a.ID, "worker-1", byID[a.ID].Revision, "boom")
	if err != nil || !failedParent {
		t.Fatalf("FailBranch() = (%v, %v)", failedParent, err)
	}
	gotA, _ := repo.GetBranch(ctx, a.ID)
	if gotA.Status != model.ParallelBranchFailed || gotA.Error != "boom" {
		t.Fatalf("leaf = %+v", gotA)
	}
	gotB, _ := repo.GetBranch(ctx, b.ID)
	if gotB.Status != model.ParallelBranchCancelled || gotB.LeasedBy != "" {
		t.Fatalf("sibling = %+v", gotB)
	}
	exGot, _ := repo.GetExecution(ctx, ex.ID)
	if exGot.Status != model.ParallelExecutionFailed {
		t.Fatalf("execution = %s", exGot.Status)
	}
	inst, _ := instances.GetByID(ctx, id)
	if inst.Status != model.WorkflowFailed {
		t.Fatalf("parent = %s", inst.Status)
	}
	if !strings.Contains(inst.Error, "boom") || !strings.Contains(inst.Error, `"a"`) {
		t.Fatalf("parent error = %q", inst.Error)
	}
	// The cancelled sibling's in-flight work is fenced.
	frame, _ := model.ParseFrame(byID[b.ID].Frame)
	if err := repo.CheckpointBranch(ctx, repository.BranchCheckpoint{
		BranchID: b.ID, WorkerID: "worker-1", Revision: byID[b.ID].Revision,
		Status: model.ParallelBranchWaiting, Frame: frame, Context: byID[b.ID].Context,
	}); !errors.Is(err, repository.ErrLeaseLost) {
		t.Fatalf("fenced checkpoint = %v", err)
	}
}

func insertNestedExecution(t *testing.T, db *gorm.DB, id, instanceID, parentBranchID string, depth int) {
	t.Helper()
	nested := repository.ParallelExecutionToModel(model.ParallelExecution{
		ID: id, InstanceID: instanceID, ParentBranchID: &parentBranchID, Depth: depth,
		StartNodeID: parallelTestStart, EndNodeID: parallelTestEnd,
		Status: model.ParallelWaitingForBranches, BranchCount: 1,
	})
	if err := db.Create(&nested).Error; err != nil {
		t.Fatalf("insert nested execution: %v", err)
	}
}

func insertNestedBranch(t *testing.T, db *gorm.DB, id, executionID, instanceID string) {
	t.Helper()
	nb := repository.ParallelBranchToModel(model.ParallelBranch{
		ID: id, ParallelExecutionID: executionID, InstanceID: instanceID,
		Name: "n", StartNodeID: parallelTestNodeA, Status: model.ParallelBranchPending,
	})
	if err := db.Create(&nb).Error; err != nil {
		t.Fatalf("insert nested branch: %v", err)
	}
}

func TestFailBranchPropagatesThroughNested(t *testing.T) {
	db := setupTestDB(t)
	seedInstanceFixture(t, db)
	truncateParallel(t, db)
	ctx := context.Background()
	id := "01950000-0000-7000-8000-0000000000e2"
	insertLeasedInstance(t, db, id)

	repo := repository.NewParallelRepository(db)
	instances := repository.NewInstanceRepository(db)
	ex, branches, err := repo.Fork(ctx, parallelFork(id))
	if err != nil {
		t.Fatalf("Fork() error = %v", err)
	}
	parent, sibling := branches[0], branches[1]
	if err := db.Model(&repository.ParallelBranchModel{}).Where("id = ?", parent.ID).
		Updates(map[string]any{"status": string(model.ParallelBranchWaiting), "waiting_reason": string(model.WaitingReasonParallel)}).Error; err != nil {
		t.Fatalf("park parent branch: %v", err)
	}
	nestedID := "01950000-0000-7000-8000-0000000000e3"
	nestedBranchID := "01950000-0000-7000-8000-0000000000e4"
	insertNestedExecution(t, db, nestedID, id, parent.ID, 2)
	insertNestedBranch(t, db, nestedBranchID, nestedID, id)

	claimed, err := repo.ClaimNextBranches(ctx, "worker-1", time.Minute, 10)
	if err != nil {
		t.Fatalf("ClaimNextBranches() error = %v", err)
	}
	var target model.ParallelBranch
	for _, b := range claimed {
		if b.ID == nestedBranchID {
			target = b
		}
	}
	if target.ID == "" {
		t.Fatalf("nested branch was not claimed")
	}
	if _, err := repo.FailBranch(ctx, target.ID, "worker-1", target.Revision, "nested boom"); err != nil {
		t.Fatalf("FailBranch() error = %v", err)
	}
	nestedGot, _ := repo.GetExecution(ctx, nestedID)
	if nestedGot.Status != model.ParallelExecutionFailed {
		t.Fatalf("nested execution = %s", nestedGot.Status)
	}
	parentGot, _ := repo.GetBranch(ctx, parent.ID)
	if parentGot.Status != model.ParallelBranchFailed {
		t.Fatalf("parent branch = %s", parentGot.Status)
	}
	exGot, _ := repo.GetExecution(ctx, ex.ID)
	if exGot.Status != model.ParallelExecutionFailed {
		t.Fatalf("top execution = %s", exGot.Status)
	}
	siblingGot, _ := repo.GetBranch(ctx, sibling.ID)
	if siblingGot.Status != model.ParallelBranchCancelled {
		t.Fatalf("sibling = %s", siblingGot.Status)
	}
	inst, _ := instances.GetByID(ctx, id)
	if inst.Status != model.WorkflowFailed {
		t.Fatalf("parent = %s", inst.Status)
	}
}

func TestFailBranchCancelsNestedUnderLiveSibling(t *testing.T) {
	db := setupTestDB(t)
	seedInstanceFixture(t, db)
	truncateParallel(t, db)
	ctx := context.Background()
	id := "01950000-0000-7000-8000-0000000000e5"
	insertLeasedInstance(t, db, id)

	repo := repository.NewParallelRepository(db)
	_, branches, err := repo.Fork(ctx, parallelFork(id))
	if err != nil {
		t.Fatalf("Fork() error = %v", err)
	}
	leaf, live := branches[0], branches[1]
	if err := db.Model(&repository.ParallelBranchModel{}).Where("id = ?", live.ID).
		Updates(map[string]any{"status": string(model.ParallelBranchWaiting), "waiting_reason": string(model.WaitingReasonParallel)}).Error; err != nil {
		t.Fatalf("park live branch: %v", err)
	}
	nestedID := "01950000-0000-7000-8000-0000000000e6"
	nestedBranchID := "01950000-0000-7000-8000-0000000000e7"
	insertNestedExecution(t, db, nestedID, id, live.ID, 2)
	insertNestedBranch(t, db, nestedBranchID, nestedID, id)

	claimed, err := repo.ClaimNextBranches(ctx, "worker-1", time.Minute, 10)
	if err != nil {
		t.Fatalf("ClaimNextBranches() error = %v", err)
	}
	var target model.ParallelBranch
	for _, b := range claimed {
		if b.ID == leaf.ID {
			target = b
		}
	}
	if _, err := repo.FailBranch(ctx, target.ID, "worker-1", target.Revision, "boom"); err != nil {
		t.Fatalf("FailBranch() error = %v", err)
	}
	liveGot, _ := repo.GetBranch(ctx, live.ID)
	if liveGot.Status != model.ParallelBranchCancelled {
		t.Fatalf("live sibling = %s", liveGot.Status)
	}
	nestedGot, _ := repo.GetExecution(ctx, nestedID)
	if nestedGot.Status != model.ParallelExecutionCancelled {
		t.Fatalf("nested execution = %s", nestedGot.Status)
	}
	nestedBranchGot, _ := repo.GetBranch(ctx, nestedBranchID)
	if nestedBranchGot.Status != model.ParallelBranchCancelled {
		t.Fatalf("nested branch = %s", nestedBranchGot.Status)
	}
}

func TestStopCancelsParallelTree(t *testing.T) {
	db := setupTestDB(t)
	seedInstanceFixture(t, db)
	truncateParallel(t, db)
	ctx := context.Background()
	id := "01950000-0000-7000-8000-0000000000f1"
	insertLeasedInstance(t, db, id)

	instances := repository.NewInstanceRepository(db)
	prepo := repository.NewParallelRepository(db)
	ex, branches, err := prepo.Fork(ctx, parallelFork(id))
	if err != nil {
		t.Fatalf("Fork() error = %v", err)
	}
	claimed, err := prepo.ClaimNextBranches(ctx, "worker-1", time.Minute, 10)
	if err != nil {
		t.Fatalf("ClaimNextBranches() error = %v", err)
	}
	byID := map[string]model.ParallelBranch{}
	for _, b := range claimed {
		byID[b.ID] = b
	}
	// A running branch attempt, as left by an interrupted worker.
	attempt := model.NodeInstance{
		ID: "01950000-0000-7000-8000-0000000000f2", WorkflowInstanceID: id,
		BranchID: branches[0].ID, NodeID: parallelTestNodeA, Status: model.NodeRunning,
	}
	if err := instances.InsertNodeInstance(ctx, attempt); err != nil {
		t.Fatalf("InsertNodeInstance() error = %v", err)
	}
	pending, err := instances.Stop(ctx, id, "stop test")
	if err != nil || !pending {
		t.Fatalf("Stop() = (%v, %v)", pending, err)
	}
	for _, b := range branches {
		got, _ := prepo.GetBranch(ctx, b.ID)
		if got.Status != model.ParallelBranchCancelled || got.LeasedBy != "" {
			t.Fatalf("branch %s = %+v", b.Name, got)
		}
	}
	exGot, _ := prepo.GetExecution(ctx, ex.ID)
	if exGot.Status != model.ParallelExecutionCancelled {
		t.Fatalf("execution = %s", exGot.Status)
	}
	gotAttempt, err := instances.GetBranchNodeInstanceByNode(ctx, branches[0].ID, parallelTestNodeA)
	if err != nil || gotAttempt.Status != model.NodeStopped {
		t.Fatalf("branch attempt = %+v, %v", gotAttempt, err)
	}
	// In-flight branch work is fenced.
	frame, _ := model.ParseFrame(byID[branches[0].ID].Frame)
	if err := prepo.CheckpointBranch(ctx, repository.BranchCheckpoint{
		BranchID: branches[0].ID, WorkerID: "worker-1", Revision: byID[branches[0].ID].Revision,
		Status: model.ParallelBranchWaiting, Frame: frame, Context: byID[branches[0].ID].Context,
	}); !errors.Is(err, repository.ErrLeaseLost) {
		t.Fatalf("fenced checkpoint = %v", err)
	}
}

func TestListBranchIDs(t *testing.T) {
	db := setupTestDB(t)
	seedInstanceFixture(t, db)
	truncateParallel(t, db)
	ctx := context.Background()
	id := "01950000-0000-7000-8000-0000000000f4"
	insertLeasedInstance(t, db, id)

	repo := repository.NewParallelRepository(db)
	_, branches, err := repo.Fork(ctx, parallelFork(id))
	if err != nil {
		t.Fatalf("Fork() error = %v", err)
	}
	ids, err := repo.ListBranchIDs(ctx, id)
	if err != nil {
		t.Fatalf("ListBranchIDs() error = %v", err)
	}
	if len(ids) != len(branches) {
		t.Fatalf("ids = %v", ids)
	}
	empty, err := repo.ListBranchIDs(ctx, "01950000-0000-7000-8000-0000000000f5")
	if err != nil || len(empty) != 0 {
		t.Fatalf("empty = %v, %v", empty, err)
	}
}

func TestListBranchesByExecutionIDs(t *testing.T) {
	db := setupTestDB(t)
	seedInstanceFixture(t, db)
	truncateParallel(t, db)
	ctx := context.Background()
	id := "01950000-0000-7000-8000-0000000000b1"
	insertLeasedInstance(t, db, id)

	repo := repository.NewParallelRepository(db)
	// Three branches inserted out of index order: read-back must follow
	// branch_index, proving the ORDER BY rather than insertion order.
	top := parallelFork(id)
	top.Branches = []repository.ForkBranch{
		{Name: "c", BranchIndex: 2, StartNodeID: parallelTestNodeB, Frame: model.NewFrame(parallelTestNodeB), Context: json.RawMessage(`{"v":1}`)},
		{Name: "a", BranchIndex: 0, StartNodeID: parallelTestNodeA, Frame: model.NewFrame(parallelTestNodeA), Context: json.RawMessage(`{"v":1}`)},
		{Name: "b", BranchIndex: 1, StartNodeID: parallelTestNodeB, Frame: model.NewFrame(parallelTestNodeB), Context: json.RawMessage(`{"v":1}`)},
	}
	outer, branches, err := repo.Fork(ctx, top)
	if err != nil {
		t.Fatalf("Fork() error = %v", err)
	}
	parent := claimBranchByID(t, repo, ctx, branches[1].ID)
	nested := nestedForkFor(id, parent)
	nested.Branches = []repository.ForkBranch{
		{Name: "y", BranchIndex: 1, StartNodeID: parallelTestNestedB, Frame: model.NewFrame(parallelTestNestedB), Context: parent.Context},
		{Name: "x", BranchIndex: 0, StartNodeID: parallelTestNestedA, Frame: model.NewFrame(parallelTestNestedA), Context: parent.Context},
		{Name: "z", BranchIndex: 2, StartNodeID: parallelTestNestedB, Frame: model.NewFrame(parallelTestNestedB), Context: parent.Context},
	}
	inner, _, err := repo.Fork(ctx, nested)
	if err != nil {
		t.Fatalf("nested Fork() error = %v", err)
	}

	got, err := repo.ListBranchesByExecutionIDs(ctx, []string{outer.ID, inner.ID})
	if err != nil {
		t.Fatalf("ListBranchesByExecutionIDs() error = %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("groups = %d, want 2", len(got))
	}
	assertBranchOrder(t, got[outer.ID], outer.ID, []string{"a", "b", "c"})
	assertBranchOrder(t, got[inner.ID], inner.ID, []string{"x", "y", "z"})

	// Unknown ids yield an empty map, not an error; executions without
	// branches simply have no entry.
	unknown, err := repo.ListBranchesByExecutionIDs(ctx, []string{"01950000-0000-7000-8000-0000000000b2"})
	if err != nil || len(unknown) != 0 {
		t.Fatalf("unknown = %v, %v", unknown, err)
	}
	for _, ids := range [][]string{nil, {}} {
		empty, err := repo.ListBranchesByExecutionIDs(ctx, ids)
		if err != nil || len(empty) != 0 {
			t.Fatalf("empty input = %v, %v", empty, err)
		}
	}
}

func assertBranchOrder(t *testing.T, branches []model.ParallelBranch, executionID string, wantNames []string) {
	t.Helper()
	if len(branches) != len(wantNames) {
		t.Fatalf("execution %s branches = %d, want %d", executionID, len(branches), len(wantNames))
	}
	for i, name := range wantNames {
		b := branches[i]
		if b.Name != name || b.BranchIndex != i || b.ParallelExecutionID != executionID {
			t.Fatalf("branches[%d] = %+v, want name %q index %d", i, b, name, i)
		}
	}
}

func TestStopParkedParallelReportsNoPending(t *testing.T) {
	db := setupTestDB(t)
	seedInstanceFixture(t, db)
	truncateParallel(t, db)
	ctx := context.Background()
	id := "01950000-0000-7000-8000-0000000000f3"
	insertLeasedInstance(t, db, id)

	instances := repository.NewInstanceRepository(db)
	prepo := repository.NewParallelRepository(db)
	if _, _, err := prepo.Fork(ctx, parallelFork(id)); err != nil {
		t.Fatalf("Fork() error = %v", err)
	}
	pending, err := instances.Stop(ctx, id, "stop test")
	if err != nil || pending {
		t.Fatalf("Stop() = (%v, %v)", pending, err)
	}
	exs, _ := prepo.ListExecutions(ctx, id)
	for _, ex := range exs {
		if ex.Status != model.ParallelExecutionCancelled {
			t.Fatalf("execution = %s", ex.Status)
		}
		branches, _ := prepo.ListBranches(ctx, ex.ID)
		for _, b := range branches {
			if b.Status != model.ParallelBranchCancelled {
				t.Fatalf("branch %s = %s", b.Name, b.Status)
			}
		}
	}
}

func TestFailExecutionIsIdempotent(t *testing.T) {
	db := setupTestDB(t)
	seedInstanceFixture(t, db)
	truncateParallel(t, db)
	ctx := context.Background()
	id := "01950000-0000-7000-8000-0000000000d2"
	insertLeasedInstance(t, db, id)

	repo := repository.NewParallelRepository(db)
	ex, _, err := repo.Fork(ctx, parallelFork(id))
	if err != nil {
		t.Fatalf("Fork() error = %v", err)
	}
	if err := repo.FailExecution(ctx, ex.ID); err != nil {
		t.Fatalf("FailExecution() error = %v", err)
	}
	if err := repo.FailExecution(ctx, ex.ID); err != nil {
		t.Fatalf("second FailExecution() error = %v", err)
	}
	exGot, _ := repo.GetExecution(ctx, ex.ID)
	if exGot.Status != model.ParallelExecutionFailed {
		t.Fatalf("execution = %s", exGot.Status)
	}
	if err := repo.FailExecution(ctx, "01950000-0000-7000-8000-0000000000d3"); !errors.Is(err, repository.ErrParallelExecutionNotFound) {
		t.Fatalf("missing execution error = %v", err)
	}
}

func TestCompleteBranchRetryIsIdempotent(t *testing.T) {
	db := setupTestDB(t)
	seedInstanceFixture(t, db)
	truncateParallel(t, db)
	ctx := context.Background()
	id := "01950000-0000-7000-8000-0000000000b7"
	insertLeasedInstance(t, db, id)

	repo := repository.NewParallelRepository(db)
	ex, _, err := repo.Fork(ctx, parallelFork(id))
	if err != nil {
		t.Fatalf("Fork() error = %v", err)
	}
	branchID, _ := completeOne(t, repo, db, id, "worker-1")
	b, _ := repo.GetBranch(ctx, branchID)
	if ready, err := repo.CompleteBranch(ctx, completionFor(t, *b, "worker-1")); err != nil || ready {
		t.Fatalf("retry CompleteBranch() = (%v, %v)", ready, err)
	}
	exGot, _ := repo.GetExecution(ctx, ex.ID)
	if exGot.CompletedCount != 1 {
		t.Fatalf("completed count = %d after retry", exGot.CompletedCount)
	}
}

const (
	parallelTestNestedStart = "01950000-0000-7000-8000-0000000000e1"
	parallelTestNestedEnd   = "01950000-0000-7000-8000-0000000000e2"
	parallelTestNestedA     = "01950000-0000-7000-8000-0000000000e3"
	parallelTestNestedB     = "01950000-0000-7000-8000-0000000000e4"
	parallelTestAfterNested = "01950000-0000-7000-8000-0000000000e5"
)

// nestedForkFor builds a nested fork parked on a claimed parent branch.
func nestedForkFor(id string, parent model.ParallelBranch) repository.ForkParallel {
	return repository.ForkParallel{
		InstanceID:           id,
		WorkflowDefinitionID: fixtureWorkflowDefID,
		WorkerID:             "worker-1",
		Revision:             parent.Revision,
		ParentBranchID:       &parent.ID,
		Depth:                2,
		StartNodeID:          parallelTestNestedStart,
		EndNodeID:            parallelTestNestedEnd,
		Frame:                model.NewFrame(parallelTestNestedEnd),
		Context:              parent.Context,
		Branches: []repository.ForkBranch{
			{Name: "x", BranchIndex: 0, StartNodeID: parallelTestNestedA, Frame: model.NewFrame(parallelTestNestedA), Context: parent.Context},
			{Name: "y", BranchIndex: 1, StartNodeID: parallelTestNestedB, Frame: model.NewFrame(parallelTestNestedB), Context: parent.Context},
		},
	}
}

// claimBranchByID claims runnable branches and returns the one with id.
func claimBranchByID(t *testing.T, repo repository.ParallelRepository, ctx context.Context, id string) model.ParallelBranch {
	t.Helper()
	claimed, err := repo.ClaimNextBranches(ctx, "worker-1", time.Minute, 10)
	if err != nil {
		t.Fatalf("ClaimNextBranches() error = %v", err)
	}
	for _, b := range claimed {
		if b.ID == id {
			return b
		}
	}
	t.Fatalf("branch %s was not claimed", id)
	return model.ParallelBranch{}
}

// completeBranches claims and completes every listed branch. One claim call
// grabs all runnable branches, so the completions map off that single call
// instead of claiming per branch.
func completeBranches(t *testing.T, repo repository.ParallelRepository, ctx context.Context, branches []model.ParallelBranch) {
	t.Helper()
	claimed, err := repo.ClaimNextBranches(ctx, "worker-1", time.Minute, 10)
	if err != nil {
		t.Fatalf("ClaimNextBranches() error = %v", err)
	}
	byID := map[string]model.ParallelBranch{}
	for _, b := range claimed {
		byID[b.ID] = b
	}
	for _, b := range branches {
		c, ok := byID[b.ID]
		if !ok {
			t.Fatalf("branch %s was not claimed", b.ID)
		}
		if _, err := repo.CompleteBranch(ctx, completionFor(t, c, "worker-1")); err != nil {
			t.Fatalf("CompleteBranch() error = %v", err)
		}
	}
}

func TestForkNestedParksBranchNotInstance(t *testing.T) {
	db := setupTestDB(t)
	seedInstanceFixture(t, db)
	truncateParallel(t, db)
	ctx := context.Background()
	id := "01950000-0000-7000-8000-0000000000e0"
	insertLeasedInstance(t, db, id)

	repo := repository.NewParallelRepository(db)
	instances := repository.NewInstanceRepository(db)
	_, branches, err := repo.Fork(ctx, parallelFork(id))
	if err != nil {
		t.Fatalf("Fork() error = %v", err)
	}
	parent := claimBranchByID(t, repo, ctx, branches[0].ID)

	nested, children, err := repo.Fork(ctx, nestedForkFor(id, parent))
	if err != nil {
		t.Fatalf("nested Fork() error = %v", err)
	}
	if nested.ParentBranchID == nil || *nested.ParentBranchID != parent.ID {
		t.Fatalf("nested parent = %+v, want %s", nested.ParentBranchID, parent.ID)
	}
	if nested.Depth != 2 {
		t.Fatalf("nested depth = %d, want 2", nested.Depth)
	}
	if len(children) != 2 {
		t.Fatalf("nested branches = %d, want 2", len(children))
	}
	gotParent, _ := repo.GetBranch(ctx, parent.ID)
	if gotParent.Status != model.ParallelBranchWaiting || gotParent.WaitingReason != model.WaitingReasonParallel {
		t.Fatalf("parent = %s/%s", gotParent.Status, gotParent.WaitingReason)
	}
	frame, _ := model.ParseFrame(gotParent.Frame)
	if frame.CurrentNodeID != parallelTestNestedEnd {
		t.Fatalf("parent cursor = %q", frame.CurrentNodeID)
	}
	// The instance row is untouched: still parked on the outer join at the
	// revision the outer fork left behind.
	inst, _ := instances.GetByID(ctx, id)
	if inst.Status != model.WorkflowWaiting || inst.WaitingReason != model.WaitingReasonParallel {
		t.Fatalf("instance = %s/%s", inst.Status, inst.WaitingReason)
	}
	if inst.Revision != 1 {
		t.Fatalf("instance revision = %d, want 1", inst.Revision)
	}
}

func TestForkNestedRejectsLostLease(t *testing.T) {
	db := setupTestDB(t)
	seedInstanceFixture(t, db)
	truncateParallel(t, db)
	ctx := context.Background()
	id := "01950000-0000-7000-8000-0000000000e6"
	insertLeasedInstance(t, db, id)

	repo := repository.NewParallelRepository(db)
	_, branches, err := repo.Fork(ctx, parallelFork(id))
	if err != nil {
		t.Fatalf("Fork() error = %v", err)
	}
	parent := claimBranchByID(t, repo, ctx, branches[0].ID)

	f := nestedForkFor(id, parent)
	f.WorkerID = "worker-2"
	if _, _, err := repo.Fork(ctx, f); !errors.Is(err, repository.ErrLeaseLost) {
		t.Fatalf("nested Fork() = %v, want ErrLeaseLost", err)
	}
	gotParent, _ := repo.GetBranch(ctx, parent.ID)
	if gotParent.Status != model.ParallelBranchRunning {
		t.Fatalf("parent = %s after lost fork", gotParent.Status)
	}
}

func TestJoinBranchParallelCommitsBranchAdvance(t *testing.T) {
	db := setupTestDB(t)
	seedInstanceFixture(t, db)
	truncateParallel(t, db)
	ctx := context.Background()
	id := "01950000-0000-7000-8000-0000000000e7"
	insertLeasedInstance(t, db, id)

	repo := repository.NewParallelRepository(db)
	instances := repository.NewInstanceRepository(db)
	_, branches, err := repo.Fork(ctx, parallelFork(id))
	if err != nil {
		t.Fatalf("Fork() error = %v", err)
	}
	parent := claimBranchByID(t, repo, ctx, branches[0].ID)
	nested, children, err := repo.Fork(ctx, nestedForkFor(id, parent))
	if err != nil {
		t.Fatalf("nested Fork() error = %v", err)
	}
	completeBranches(t, repo, ctx, children)
	// The barrier armed and woke the parent; claim it for the nested join.
	resumed := claimBranchByID(t, repo, ctx, parent.ID)
	err = repo.JoinBranchParallel(ctx, repository.JoinBranchCheckpoint{
		BranchCheckpoint: repository.BranchCheckpoint{
			BranchID: resumed.ID, WorkerID: "worker-1", Revision: resumed.Revision,
			Status: model.ParallelBranchWaiting, WaitingReason: model.WaitingReasonRunnable,
			Frame:   model.NewFrame(parallelTestAfterNested),
			Context: json.RawMessage(`{"merged":true}`),
		},
		ExecutionID: nested.ID,
	})
	if err != nil {
		t.Fatalf("JoinBranchParallel() error = %v", err)
	}
	gotParent, _ := repo.GetBranch(ctx, parent.ID)
	if gotParent.Status != model.ParallelBranchWaiting || gotParent.WaitingReason != model.WaitingReasonRunnable {
		t.Fatalf("parent = %s/%s", gotParent.Status, gotParent.WaitingReason)
	}
	frame, _ := model.ParseFrame(gotParent.Frame)
	if frame.CurrentNodeID != parallelTestAfterNested {
		t.Fatalf("parent cursor = %q", frame.CurrentNodeID)
	}
	assertJSON(t, gotParent.Context, `{"merged":true}`)
	exGot, _ := repo.GetExecution(ctx, nested.ID)
	if exGot.Status != model.ParallelExecutionCompleted {
		t.Fatalf("nested execution = %s", exGot.Status)
	}
	inst, _ := instances.GetByID(ctx, id)
	if inst.Status != model.WorkflowWaiting || inst.WaitingReason != model.WaitingReasonParallel {
		t.Fatalf("instance = %s/%s", inst.Status, inst.WaitingReason)
	}
}

func TestJoinBranchParallelConflictsWhenNotReady(t *testing.T) {
	db := setupTestDB(t)
	seedInstanceFixture(t, db)
	truncateParallel(t, db)
	ctx := context.Background()
	id := "01950000-0000-7000-8000-0000000000e8"
	insertLeasedInstance(t, db, id)

	repo := repository.NewParallelRepository(db)
	_, branches, err := repo.Fork(ctx, parallelFork(id))
	if err != nil {
		t.Fatalf("Fork() error = %v", err)
	}
	parent := claimBranchByID(t, repo, ctx, branches[0].ID)
	nested, _, err := repo.Fork(ctx, nestedForkFor(id, parent))
	if err != nil {
		t.Fatalf("nested Fork() error = %v", err)
	}
	err = repo.JoinBranchParallel(ctx, repository.JoinBranchCheckpoint{
		BranchCheckpoint: repository.BranchCheckpoint{
			BranchID: parent.ID, WorkerID: "worker-1", Revision: parent.Revision,
			Status: model.ParallelBranchWaiting, WaitingReason: model.WaitingReasonRunnable,
			Frame: model.NewFrame(parallelTestAfterNested),
		},
		ExecutionID: nested.ID,
	})
	if !errors.Is(err, repository.ErrStatusConflict) {
		t.Fatalf("JoinBranchParallel() = %v, want ErrStatusConflict", err)
	}
}

func TestJoinBranchParallelRejectsLostLease(t *testing.T) {
	db := setupTestDB(t)
	seedInstanceFixture(t, db)
	truncateParallel(t, db)
	ctx := context.Background()
	id := "01950000-0000-7000-8000-0000000000e9"
	insertLeasedInstance(t, db, id)

	repo := repository.NewParallelRepository(db)
	_, branches, err := repo.Fork(ctx, parallelFork(id))
	if err != nil {
		t.Fatalf("Fork() error = %v", err)
	}
	parent := claimBranchByID(t, repo, ctx, branches[0].ID)
	nested, children, err := repo.Fork(ctx, nestedForkFor(id, parent))
	if err != nil {
		t.Fatalf("nested Fork() error = %v", err)
	}
	completeBranches(t, repo, ctx, children)
	resumed := claimBranchByID(t, repo, ctx, parent.ID)
	err = repo.JoinBranchParallel(ctx, repository.JoinBranchCheckpoint{
		BranchCheckpoint: repository.BranchCheckpoint{
			BranchID: resumed.ID, WorkerID: "worker-2", Revision: resumed.Revision,
			Status: model.ParallelBranchWaiting, WaitingReason: model.WaitingReasonRunnable,
			Frame: model.NewFrame(parallelTestAfterNested),
		},
		ExecutionID: nested.ID,
	})
	if !errors.Is(err, repository.ErrLeaseLost) {
		t.Fatalf("JoinBranchParallel() = %v, want ErrLeaseLost", err)
	}
}

func insertDebugLeasedInstance(t *testing.T, db *gorm.DB, id string) {
	t.Helper()
	w := newTestInstance(id, model.WorkflowRunning, model.WaitingReasonRunnable)
	w.Debug = true
	w.LeasedBy = "worker-1"
	w.LeaseExpiry = time.Now().UTC().Add(time.Minute)
	insertInstance(t, db, w)
}

func TestForkDebugParksParentAndBranches(t *testing.T) {
	db := setupTestDB(t)
	seedInstanceFixture(t, db)
	truncateParallel(t, db)
	ctx := context.Background()
	id := "01950000-0000-7000-8000-0000000000c5"
	insertDebugLeasedInstance(t, db, id)

	repo := repository.NewParallelRepository(db)
	_, branches, err := repo.Fork(ctx, parallelDebugFork(id))
	if err != nil {
		t.Fatalf("Fork() error = %v", err)
	}
	// The fork step is done, so the debug parent parks paused; its
	// branches are born paused so the dispatcher never auto-runs them.
	inst, _ := repository.NewInstanceRepository(db).GetByID(ctx, id)
	if inst.Status != model.WorkflowPaused || inst.WaitingReason != model.WaitingReasonParallel {
		t.Fatalf("parent = %s/%s, want paused/parallel", inst.Status, inst.WaitingReason)
	}
	for _, b := range branches {
		if b.Status != model.ParallelBranchWaiting || b.WaitingReason != model.WaitingReasonPaused {
			t.Fatalf("returned branch %s = %s/%s, want waiting/paused", b.Name, b.Status, b.WaitingReason)
		}
		got, _ := repo.GetBranch(ctx, b.ID)
		if got.Status != model.ParallelBranchWaiting || got.WaitingReason != model.WaitingReasonPaused {
			t.Fatalf("branch %s = %s/%s, want waiting/paused", b.Name, got.Status, got.WaitingReason)
		}
	}
	claimed, err := repo.ClaimNextBranches(ctx, "worker-9", time.Minute, 10)
	if err != nil {
		t.Fatalf("ClaimNextBranches() error = %v", err)
	}
	if len(claimed) != 0 {
		t.Fatalf("claimed %d paused branches, want 0", len(claimed))
	}
}

func TestNestedForkDebugParksBranchesNotOwner(t *testing.T) {
	db := setupTestDB(t)
	seedInstanceFixture(t, db)
	truncateParallel(t, db)
	ctx := context.Background()
	id := "01950000-0000-7000-8000-0000000000c6"
	insertDebugLeasedInstance(t, db, id)

	repo := repository.NewParallelRepository(db)
	_, branches, err := repo.Fork(ctx, parallelDebugFork(id))
	if err != nil {
		t.Fatalf("Fork() error = %v", err)
	}
	if err := repo.WakePausedBranch(ctx, branches[0].ID); err != nil {
		t.Fatalf("WakePausedBranch() error = %v", err)
	}
	parent := claimBranchByID(t, repo, ctx, branches[0].ID)

	_, children, err := repo.Fork(ctx, nestedDebugForkFor(id, parent))
	if err != nil {
		t.Fatalf("nested Fork() error = %v", err)
	}
	// The owner stays barrier-parked (waiting/parallel) so the barrier
	// can wake it; only the fresh children park paused.
	gotParent, _ := repo.GetBranch(ctx, parent.ID)
	if gotParent.Status != model.ParallelBranchWaiting || gotParent.WaitingReason != model.WaitingReasonParallel {
		t.Fatalf("owner = %s/%s, want waiting/parallel", gotParent.Status, gotParent.WaitingReason)
	}
	for _, c := range children {
		got, _ := repo.GetBranch(ctx, c.ID)
		if got.Status != model.ParallelBranchWaiting || got.WaitingReason != model.WaitingReasonPaused {
			t.Fatalf("child %s = %s/%s, want waiting/paused", c.Name, got.Status, got.WaitingReason)
		}
	}
	inst, _ := repository.NewInstanceRepository(db).GetByID(ctx, id)
	if inst.Status != model.WorkflowPaused || inst.WaitingReason != model.WaitingReasonParallel {
		t.Fatalf("instance = %s/%s, want paused/parallel", inst.Status, inst.WaitingReason)
	}
}

func TestWakePausedBranch(t *testing.T) {
	db := setupTestDB(t)
	seedInstanceFixture(t, db)
	truncateParallel(t, db)
	ctx := context.Background()
	id := "01950000-0000-7000-8000-0000000000ca"
	insertDebugLeasedInstance(t, db, id)

	repo := repository.NewParallelRepository(db)
	_, branches, err := repo.Fork(ctx, parallelDebugFork(id))
	if err != nil {
		t.Fatalf("Fork() error = %v", err)
	}
	if err := repo.WakePausedBranch(ctx, branches[0].ID); err != nil {
		t.Fatalf("WakePausedBranch() error = %v", err)
	}
	got, _ := repo.GetBranch(ctx, branches[0].ID)
	if got.Status != model.ParallelBranchWaiting || got.WaitingReason != model.WaitingReasonRunnable {
		t.Fatalf("woken = %s/%s, want waiting/runnable", got.Status, got.WaitingReason)
	}
	if err := repo.WakePausedBranch(ctx, branches[0].ID); !errors.Is(err, repository.ErrStatusConflict) {
		t.Fatalf("second wake = %v, want ErrStatusConflict", err)
	}
	running := claimBranchByID(t, repo, ctx, branches[0].ID)
	if err := repo.WakePausedBranch(ctx, running.ID); !errors.Is(err, repository.ErrStatusConflict) {
		t.Fatalf("wake running = %v, want ErrStatusConflict", err)
	}
	if err := repo.WakePausedBranch(ctx, "01950000-0000-7000-8000-0000000000cb"); !errors.Is(err, repository.ErrParallelBranchNotFound) {
		t.Fatalf("wake missing = %v, want ErrParallelBranchNotFound", err)
	}
}

func TestCompleteBranchWakesPausedParent(t *testing.T) {
	db := setupTestDB(t)
	seedInstanceFixture(t, db)
	truncateParallel(t, db)
	ctx := context.Background()
	id := "01950000-0000-7000-8000-0000000000cc"
	insertLeasedInstance(t, db, id)

	repo := repository.NewParallelRepository(db)
	instances := repository.NewInstanceRepository(db)
	if _, _, err := repo.Fork(ctx, parallelFork(id)); err != nil {
		t.Fatalf("Fork() error = %v", err)
	}
	// Simulate a manual pause mid-parallel: the parent parks paused
	// while its branches still run.
	if _, err := instances.Pause(ctx, id); err != nil {
		t.Fatalf("Pause() error = %v", err)
	}
	claimed, err := repo.ClaimNextBranches(ctx, "worker-1", time.Minute, 10)
	if err != nil {
		t.Fatalf("ClaimNextBranches() error = %v", err)
	}
	var mine []model.ParallelBranch
	for _, b := range claimed {
		if b.InstanceID == id {
			mine = append(mine, b)
		}
	}
	if len(mine) != 2 {
		t.Fatalf("claimed %d branches, want 2", len(mine))
	}
	for _, b := range mine {
		if _, err := repo.CompleteBranch(ctx, completionFor(t, b, "worker-1")); err != nil {
			t.Fatalf("CompleteBranch() error = %v", err)
		}
	}
	// The barrier flips the reason but keeps the paused status: the
	// later resume lands claimable instead of stuck on parallel.
	got, _ := instances.GetByID(ctx, id)
	if got.Status != model.WorkflowPaused || got.WaitingReason != model.WaitingReasonRunnable {
		t.Fatalf("parent = %s/%s, want paused/runnable", got.Status, got.WaitingReason)
	}
}

func TestClaimNextBranchesConcurrentExclusive(t *testing.T) {
	db := setupTestDB(t)
	seedInstanceFixture(t, db)
	truncateParallel(t, db)
	ctx := context.Background()
	id := "01950000-0000-7000-8000-0000000000ce"
	insertLeasedInstance(t, db, id)

	f := parallelFork(id)
	f.Branches = []repository.ForkBranch{
		{Name: "w0", BranchIndex: 0, StartNodeID: parallelTestNodeA, Frame: model.NewFrame(parallelTestNodeA), Context: json.RawMessage(`{}`)},
		{Name: "w1", BranchIndex: 1, StartNodeID: parallelTestNodeA, Frame: model.NewFrame(parallelTestNodeA), Context: json.RawMessage(`{}`)},
		{Name: "w2", BranchIndex: 2, StartNodeID: parallelTestNodeB, Frame: model.NewFrame(parallelTestNodeB), Context: json.RawMessage(`{}`)},
		{Name: "w3", BranchIndex: 3, StartNodeID: parallelTestNodeB, Frame: model.NewFrame(parallelTestNodeB), Context: json.RawMessage(`{}`)},
	}
	repo := repository.NewParallelRepository(db)
	if _, _, err := repo.Fork(ctx, f); err != nil {
		t.Fatalf("Fork() error = %v", err)
	}
	// Four workers race for four branches: SKIP LOCKED deals each
	// branch to exactly one worker.
	var wg sync.WaitGroup
	claimed := make([][]model.ParallelBranch, 4)
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			got, err := repo.ClaimNextBranches(ctx, "race-worker-"+string(rune('0'+w)), time.Minute, 10)
			if err != nil {
				t.Errorf("ClaimNextBranches() error = %v", err)
				return
			}
			claimed[w] = got
		}(w)
	}
	wg.Wait()
	seen := map[string]string{}
	for w, got := range claimed {
		for _, b := range got {
			if b.InstanceID != id {
				continue
			}
			if prev, dup := seen[b.ID]; dup {
				t.Fatalf("branch %s claimed by workers %s and %d", b.ID, prev, w)
			}
			seen[b.ID] = "race-worker-" + string(rune('0'+w))
			if b.Status != model.ParallelBranchRunning {
				t.Fatalf("branch %s status = %s, want running", b.ID, b.Status)
			}
		}
	}
	if len(seen) != 4 {
		t.Fatalf("claimed %d branches, want 4", len(seen))
	}
}

func TestFailBranchFailsPausedParent(t *testing.T) {
	db := setupTestDB(t)
	seedInstanceFixture(t, db)
	truncateParallel(t, db)
	ctx := context.Background()
	id := "01950000-0000-7000-8000-0000000000cd"
	insertLeasedInstance(t, db, id)

	repo := repository.NewParallelRepository(db)
	instances := repository.NewInstanceRepository(db)
	if _, _, err := repo.Fork(ctx, parallelFork(id)); err != nil {
		t.Fatalf("Fork() error = %v", err)
	}
	if _, err := instances.Pause(ctx, id); err != nil {
		t.Fatalf("Pause() error = %v", err)
	}
	claimed, err := repo.ClaimNextBranches(ctx, "worker-1", time.Minute, 10)
	if err != nil {
		t.Fatalf("ClaimNextBranches() error = %v", err)
	}
	var leaf model.ParallelBranch
	for _, b := range claimed {
		if b.InstanceID == id {
			leaf = b
			break
		}
	}
	if leaf.ID == "" {
		t.Fatal("no branch claimed")
	}
	failedParent, err := repo.FailBranch(ctx, leaf.ID, "worker-1", leaf.Revision, "boom")
	if err != nil {
		t.Fatalf("FailBranch() error = %v", err)
	}
	if !failedParent {
		t.Fatal("failedParent = false, want true")
	}
	got, _ := instances.GetByID(ctx, id)
	if got.Status != model.WorkflowFailed {
		t.Fatalf("parent = %s, want failed", got.Status)
	}
	if !strings.Contains(got.Error, "boom") || !strings.Contains(got.Error, leaf.Name) {
		t.Fatalf("parent error = %q, want leaf failure", got.Error)
	}
}
