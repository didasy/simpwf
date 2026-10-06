package service_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/simpwf/workflow-engine/internal/workflow/auth"
	"github.com/simpwf/workflow-engine/internal/workflow/executor"
	"github.com/simpwf/workflow-engine/internal/workflow/model"
	"github.com/simpwf/workflow-engine/internal/workflow/repository"
	"github.com/simpwf/workflow-engine/internal/workflow/service"
	"gorm.io/gorm"
)

// Fixed UUIDs for PERF-4 status seeds. Graph node ids stay literal in the
// golden files; only runtime-generated row ids are normalized away.
const (
	shapeStartNode  = "33333333-3333-7333-8333-333333333301"
	shapeEndNode    = "33333333-3333-7333-8333-333333333302"
	shapeBranchNode = "33333333-3333-7333-8333-333333333303"

	goldenExID    = "33333333-3333-7333-8333-333333333311"
	goldenBrA     = "33333333-3333-7333-8333-333333333312"
	goldenBrB     = "33333333-3333-7333-8333-333333333313"
	goldenStart   = "33333333-3333-7333-8333-333333333314"
	goldenEnd     = "33333333-3333-7333-8333-333333333315"
	goldenAskNode = "33333333-3333-7333-8333-333333333316"
	goldenNBNode  = "33333333-3333-7333-8333-333333333317"
)

// seedParallelShape creates one instance on wfID and forks executions
// executions each with branchesPer branches via the Fork API: the first fork
// is top-level, the rest nest down branch 0 (a parked instance cannot fork
// again, so sibling top-level forks are impossible). It returns the instance
// id. The shape is exact: len(ListExecutions) == executions.
func seedParallelShape(t testing.TB, db *gorm.DB, svc service.InstanceService, wfID string, executions, branchesPer int) string {
	t.Helper()
	ctx := context.Background()
	inst, err := svc.Create(ctx, service.CreateInstance{WorkflowDefinitionID: wfID})
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	instances := repository.NewInstanceRepository(db)
	claimed, err := instances.ClaimNext(ctx, "shape-worker", time.Minute, 10)
	if err != nil {
		t.Fatalf("ClaimNext() error = %v", err)
	}
	var w *model.WorkflowInstance
	for i := range claimed {
		if claimed[i].ID == inst.ID {
			w = &claimed[i]
		}
	}
	if w == nil {
		t.Fatal("seed instance was not claimed")
	}
	prepo := repository.NewParallelRepository(db)
	mkBranches := func(n int) []repository.ForkBranch {
		out := make([]repository.ForkBranch, 0, n)
		for i := 0; i < n; i++ {
			out = append(out, repository.ForkBranch{
				Name:        fmt.Sprintf("b%d", i),
				BranchIndex: i,
				StartNodeID: shapeBranchNode,
				Frame:       model.NewFrame(shapeBranchNode),
				Context:     json.RawMessage(`{}`),
			})
		}
		return out
	}
	_, branches, err := prepo.Fork(ctx, repository.ForkParallel{
		InstanceID:           inst.ID,
		WorkflowDefinitionID: wfID,
		WorkerID:             "shape-worker",
		Revision:             w.Revision,
		Depth:                1,
		StartNodeID:          shapeStartNode,
		EndNodeID:            shapeEndNode,
		Frame:                model.NewFrame(shapeEndNode),
		Context:              json.RawMessage(`{}`),
		Branches:             mkBranches(branchesPer),
	})
	if err != nil {
		t.Fatalf("Fork() error = %v", err)
	}
	parent := claimShapeBranch(t, prepo, branches[0].ID)
	for e := 1; e < executions; e++ {
		_, children, err := prepo.Fork(ctx, repository.ForkParallel{
			InstanceID:           inst.ID,
			WorkflowDefinitionID: wfID,
			WorkerID:             "shape-worker",
			Revision:             parent.Revision,
			ParentBranchID:       &parent.ID,
			Depth:                e + 1,
			StartNodeID:          shapeStartNode,
			EndNodeID:            shapeEndNode,
			Frame:                model.NewFrame(shapeEndNode),
			Context:              parent.Context,
			Branches:             mkBranches(branchesPer),
		})
		if err != nil {
			t.Fatalf("nested Fork(%d) error = %v", e, err)
		}
		parent = claimShapeBranch(t, prepo, children[0].ID)
	}
	return inst.ID
}

func claimShapeBranch(t testing.TB, prepo repository.ParallelRepository, id string) model.ParallelBranch {
	t.Helper()
	claimed, err := prepo.ClaimNextBranches(context.Background(), "shape-worker", time.Minute, 100)
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

// insertGoldenParallel attaches one deterministic 2-branch execution to an
// instance with fixed row ids (raw inserts, no Fork fencing) so the golden
// files cover the Parallel section byte-exactly. terminal selects completed
// rows (no live parallel) versus live rows (a waiting branch).
func insertGoldenParallel(t *testing.T, db *gorm.DB, instID string, terminal bool) {
	t.Helper()
	now := time.Now().UTC()
	exStatus, completed := model.ParallelWaitingForBranches, 0
	brStatus, brReason := model.ParallelBranchWaiting, model.WaitingReasonInput
	if terminal {
		exStatus, completed = model.ParallelExecutionCompleted, 2
		brStatus, brReason = model.ParallelBranchCompleted, model.WaitingReasonRunnable
	}
	ex := model.ParallelExecution{
		ID: goldenExID, InstanceID: instID,
		Depth: 1, StartNodeID: goldenStart, EndNodeID: goldenEnd,
		Status: exStatus, BranchCount: 2, CompletedCount: completed,
		CreatedAt: now, UpdatedAt: now,
	}
	askFrame, err := model.NewFrame(goldenAskNode).JSON()
	if err != nil {
		t.Fatalf("frame JSON: %v", err)
	}
	nbFrame, err := model.NewFrame(goldenNBNode).JSON()
	if err != nil {
		t.Fatalf("frame JSON: %v", err)
	}
	branches := []model.ParallelBranch{
		{
			ID: goldenBrA, ParallelExecutionID: goldenExID, InstanceID: instID,
			Name: "a", BranchIndex: 0, StartNodeID: goldenAskNode,
			Frame: askFrame, Context: json.RawMessage(`{}`),
			Status: brStatus, WaitingReason: brReason,
			CreatedAt: now, UpdatedAt: now,
		},
		{
			ID: goldenBrB, ParallelExecutionID: goldenExID, InstanceID: instID,
			Name: "b", BranchIndex: 1, StartNodeID: goldenNBNode,
			Frame: nbFrame, Context: json.RawMessage(`{}`),
			Status: model.ParallelBranchPending, WaitingReason: model.WaitingReasonRunnable,
			CreatedAt: now, UpdatedAt: now,
		},
	}
	if terminal {
		// A pending branch is still live and would close the rollback gate;
		// the terminal variant completes both branches.
		branches[1].Status = model.ParallelBranchCompleted
	}
	em := repository.ParallelExecutionToModel(ex)
	if err := db.Create(&em).Error; err != nil {
		t.Fatalf("insert execution: %v", err)
	}
	for _, b := range branches {
		bm := repository.ParallelBranchToModel(b)
		if err := db.Create(&bm).Error; err != nil {
			t.Fatalf("insert branch %s: %v", b.Name, err)
		}
	}
}

var statusTSRe = regexp.MustCompile(`\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?Z`)

// normalizeStatusDetail serializes a StatusDetail with runtime-generated ids
// replaced by the volatile mapping and every timestamp replaced by <TS>.
// Fixed graph node ids stay literal so the golden keeps its meaning.
func normalizeStatusDetail(t *testing.T, d *service.StatusDetail, volatile map[string]string) string {
	t.Helper()
	raw, err := json.MarshalIndent(d, "", "  ")
	if err != nil {
		t.Fatalf("marshal status: %v", err)
	}
	got := string(raw)
	for id, placeholder := range volatile {
		if id == "" {
			continue
		}
		got = strings.ReplaceAll(got, id, placeholder)
	}
	return statusTSRe.ReplaceAllString(got, "<TS>")
}

func assertStatusGolden(t *testing.T, name, got string) {
	t.Helper()
	path := filepath.Join("testdata", name)
	if os.Getenv("UPDATE_GOLDEN") == "1" {
		if err := os.MkdirAll("testdata", 0o755); err != nil {
			t.Fatalf("mkdir testdata: %v", err)
		}
		if err := os.WriteFile(path, []byte(got), 0o644); err != nil {
			t.Fatalf("write golden: %v", err)
		}
		t.Logf("wrote %s", path)
		return
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read golden %s: %v (run with UPDATE_GOLDEN=1 to capture)", path, err)
	}
	if got != string(want) {
		t.Fatalf("golden %s mismatch (-want +got):\n%s", name, firstDiffLines(string(want), got, 30))
	}
}

// TestGetStatusDetailGoldenPaused captures the status contract for a paused
// instance with a finished occurrence (rollbackable Nodes entry) plus a
// parallel section. Run with UPDATE_GOLDEN=1 to capture; otherwise asserts.
func TestGetStatusDetailGoldenPaused(t *testing.T) {
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
	if _, err := svc.Pause(ctx, service.ControlRequest{InstanceID: inst.ID}); err != nil {
		t.Fatalf("Pause() error = %v", err)
	}
	insertGoldenParallel(t, db, inst.ID, true)

	d, err := svc.GetStatusDetail(ctx, inst.ID, auth.Principal{})
	if err != nil {
		t.Fatalf("GetStatusDetail() error = %v", err)
	}
	occs, err := repo.ListNodeInstances(ctx, inst.ID)
	if err != nil {
		t.Fatalf("ListNodeInstances() error = %v", err)
	}
	volatile := map[string]string{wfID: "<WF>", inst.ID: "<INST>"}
	for _, o := range occs {
		volatile[o.ID] = "<OCC-" + o.NodeID[len(o.NodeID)-3:] + ">"
	}
	assertStatusGolden(t, "status_detail_paused.golden.json", normalizeStatusDetail(t, d, volatile))
}

// TestGetStatusDetailGoldenWaitingInput captures the status contract for an
// instance parked on an input node (PendingInput contract) plus a parallel
// section. Run with UPDATE_GOLDEN=1 to capture; otherwise asserts.
func TestGetStatusDetailGoldenWaitingInput(t *testing.T) {
	db := setupSvcDB(t)
	ctx := context.Background()
	svc := svcInstanceService(db)

	n1 := "11111111-1111-7111-8111-111111111101"
	wfID := svcCreateWorkflow(t, db, n1,
		svcNodeJSON(n1, "input", "ask", "", "", map[string]any{
			"channel": "http", "output_property": "user",
			"form": map[string]any{
				"schema": map[string]any{
					"type": "object", "required": []string{"email"},
					"properties": map[string]any{"email": map[string]any{"type": "string"}},
				},
				"ui": map[string]any{"order": []string{"email"}},
			},
		}),
	)
	inst, err := svc.Create(ctx, service.CreateInstance{WorkflowDefinitionID: wfID})
	if err != nil {
		t.Fatal(err)
	}
	driveEngine(t, db, inst.ID)
	insertGoldenParallel(t, db, inst.ID, false)

	d, err := svc.GetStatusDetail(ctx, inst.ID, auth.Principal{})
	if err != nil {
		t.Fatalf("GetStatusDetail() error = %v", err)
	}
	occs, err := repository.NewInstanceRepository(db).ListNodeInstances(ctx, inst.ID)
	if err != nil {
		t.Fatalf("ListNodeInstances() error = %v", err)
	}
	volatile := map[string]string{wfID: "<WF>", inst.ID: "<INST>"}
	for _, o := range occs {
		volatile[o.ID] = "<OCC-" + o.NodeID[len(o.NodeID)-3:] + ">"
	}
	assertStatusGolden(t, "status_detail_input.golden.json", normalizeStatusDetail(t, d, volatile))
}

// BenchmarkGetStatusDetail_5x8 measures one status poll on a 5-execution x
// 8-branch instance. The PERF-4 refactor collapses the per-execution branch
// fan-out, so this benchmark must drop sharply after Task 2.
func BenchmarkGetStatusDetail_5x8(b *testing.B) {
	db := setupSvcDB(b)
	ctx := context.Background()
	svc := svcInstanceService(db)
	n1 := "11111111-1111-7111-8111-111111111101"
	wfID := svcCreateWorkflow(b, db, n1,
		svcNodeJSON(n1, "script", "a", "return 1;", "", map[string]any{"output_property": "out"}),
	)
	instID := seedParallelShape(b, db, svc, wfID, 5, 8)
	// Warm the Resolve definition cache: steady-state polls do zero SQL
	// for the definition on both the before and after trees.
	if _, err := svc.GetStatusDetail(ctx, instID, auth.Principal{}); err != nil {
		b.Fatalf("warm GetStatusDetail() error = %v", err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := svc.GetStatusDetail(ctx, instID, auth.Principal{}); err != nil {
			b.Fatalf("GetStatusDetail() error = %v", err)
		}
	}
}

// statusStubInstances serves one canned instance and occurrence set. DB-free:
// GetStatusDetail degrades section load failures to nil instead of failing.
type statusStubInstances struct {
	repository.InstanceRepository
	inst *model.WorkflowInstance
	occs []model.NodeInstance
	occ  *model.NodeInstance
}

func (s *statusStubInstances) GetByID(_ context.Context, _ string) (*model.WorkflowInstance, error) {
	return s.inst, nil
}

func (s *statusStubInstances) ListNodeInstances(_ context.Context, _ string) ([]model.NodeInstance, error) {
	return s.occs, nil
}

func (s *statusStubInstances) GetNodeInstanceByNode(_ context.Context, _, _ string) (*model.NodeInstance, error) {
	return s.occ, nil
}

type statusStubParallel struct {
	repository.ParallelRepository
	exs      []model.ParallelExecution
	exErr    error
	branches map[string][]model.ParallelBranch
	brErr    error
}

func (s *statusStubParallel) ListExecutions(_ context.Context, _ string) ([]model.ParallelExecution, error) {
	return s.exs, s.exErr
}

func (s *statusStubParallel) ListBranchesByExecutionIDs(_ context.Context, _ []string) (map[string][]model.ParallelBranch, error) {
	return s.branches, s.brErr
}

type statusStubMaterializer struct {
	wc  *model.WorkflowContent
	err error
}

func (s *statusStubMaterializer) Materialize(_ context.Context, wc *model.WorkflowContent) (*model.WorkflowContent, error) {
	return wc, nil
}

func (s *statusStubMaterializer) Resolve(_ context.Context, _ string) (*model.WorkflowContent, error) {
	return s.wc, s.err
}

type statusStubSecrets struct {
	service.SecretSnapshotter
}

type statusStubWorkflowDefs struct {
	repository.WorkflowDefinitionRepository
}

func statusDegradationService(inst *model.WorkflowInstance, occs []model.NodeInstance, occ *model.NodeInstance, mat *statusStubMaterializer, par *statusStubParallel) service.InstanceService {
	return service.NewInstanceService(
		&statusStubInstances{inst: inst, occs: occs, occ: occ},
		par,
		&statusStubWorkflowDefs{},
		&statusStubSecrets{},
		mat,
		&executor.InputExecutor{},
		executor.NewHookRunner(nil),
		svcSysUserID,
		svcLimits,
		nil,
		model.LeanOptions{},
	)
}

// TestGetStatusDetailDegradation pins the failure table of the single-pass
// refactor: graph failure nils Nodes and PendingInput but keeps Parallel;
// executions failure nils Parallel but leaves the Nodes rollback gate to the
// endpoint (unchanged); branches failure nils Parallel only. No row fails
// the call. DB-free: every dependency is a stub.
func TestGetStatusDetailDegradation(t *testing.T) {
	n1 := "11111111-1111-7111-8111-111111111101"
	frame, err := model.NewFrame(n1).JSON()
	if err != nil {
		t.Fatalf("frame JSON: %v", err)
	}
	wc := &model.WorkflowContent{
		StartNodeID: n1,
		Nodes:       []*model.NodeContent{{ID: n1, Type: model.NodeTypeScript}},
	}
	occ := &model.NodeInstance{
		ID: "occ-1", NodeID: n1, Status: model.NodeFinished, Attempt: 1,
		ContextBefore: json.RawMessage(`{"a":1}`),
	}
	exs := []model.ParallelExecution{{
		ID: "ex-1", Depth: 1, StartNodeID: n1, EndNodeID: n1,
		Status: model.ParallelExecutionCompleted, BranchCount: 1, CompletedCount: 1,
	}}
	branches := map[string][]model.ParallelBranch{
		"ex-1": {{ID: "b-1", Name: "a", Status: model.ParallelBranchCompleted}},
	}
	ctx := context.Background()

	t.Run("graph failure", func(t *testing.T) {
		inst := &model.WorkflowInstance{
			ID: "inst-1", Status: model.WorkflowWaiting, WaitingReason: model.WaitingReasonInput,
			Frame: frame, Context: json.RawMessage(`{}`), Counters: json.RawMessage(`{}`),
		}
		svc := statusDegradationService(inst, []model.NodeInstance{*occ}, occ,
			&statusStubMaterializer{err: errors.New("def store down")},
			&statusStubParallel{exs: exs, branches: branches})
		d, err := svc.GetStatusDetail(ctx, "inst-1", auth.Principal{})
		if err != nil {
			t.Fatalf("GetStatusDetail() error = %v, want degradation", err)
		}
		if d.Nodes != nil {
			t.Errorf("Nodes = %+v, want nil", d.Nodes)
		}
		if d.PendingInput != nil {
			t.Errorf("PendingInput = %+v, want nil", d.PendingInput)
		}
		if len(d.Parallel) != 1 {
			t.Errorf("Parallel = %+v, want 1 execution", d.Parallel)
		}
	})

	t.Run("executions failure", func(t *testing.T) {
		inst := &model.WorkflowInstance{
			ID: "inst-1", Status: model.WorkflowPaused,
			Frame: frame, Context: json.RawMessage(`{}`), Counters: json.RawMessage(`{}`),
		}
		svc := statusDegradationService(inst, []model.NodeInstance{*occ}, occ,
			&statusStubMaterializer{wc: wc},
			&statusStubParallel{exErr: errors.New("executions down")})
		d, err := svc.GetStatusDetail(ctx, "inst-1", auth.Principal{})
		if err != nil {
			t.Fatalf("GetStatusDetail() error = %v, want degradation", err)
		}
		if d.Parallel != nil {
			t.Errorf("Parallel = %+v, want nil", d.Parallel)
		}
		e, ok := d.Nodes[n1]
		if !ok {
			t.Fatalf("Nodes missing %s: %+v", n1, d.Nodes)
		}
		// Parity with the old `err == nil && live` guard: the load
		// failure must not close the gate the endpoint rechecks.
		if !e.Rollbackable {
			t.Error("Nodes[n1].Rollbackable = false, want true (gate unchanged)")
		}
	})

	t.Run("branches failure", func(t *testing.T) {
		inst := &model.WorkflowInstance{
			ID: "inst-1", Status: model.WorkflowPaused,
			Frame: frame, Context: json.RawMessage(`{}`), Counters: json.RawMessage(`{}`),
		}
		svc := statusDegradationService(inst, []model.NodeInstance{*occ}, occ,
			&statusStubMaterializer{wc: wc},
			&statusStubParallel{exs: exs, brErr: errors.New("branches down")})
		d, err := svc.GetStatusDetail(ctx, "inst-1", auth.Principal{})
		if err != nil {
			t.Fatalf("GetStatusDetail() error = %v, want degradation", err)
		}
		if d.Parallel != nil {
			t.Errorf("Parallel = %+v, want nil", d.Parallel)
		}
		if _, ok := d.Nodes[n1]; !ok {
			t.Errorf("Nodes missing %s: %+v", n1, d.Nodes)
		}
	})
}

func firstDiffLines(want, got string, context int) string {
	wl := strings.Split(want, "\n")
	gl := strings.Split(got, "\n")
	var b strings.Builder
	shown := 0
	for i := 0; i < len(wl) || i < len(gl); i++ {
		var w, g string
		if i < len(wl) {
			w = wl[i]
		}
		if i < len(gl) {
			g = gl[i]
		}
		if w != g {
			fmt.Fprintf(&b, "line %d:\n- %s\n+ %s\n", i+1, w, g)
			shown++
			if shown >= context {
				b.WriteString("... (truncated)\n")
				break
			}
		}
	}
	return b.String()
}
