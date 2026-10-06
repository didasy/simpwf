// Internal test: the PERF-4 pure helpers are asserted white-box. They never
// touch the database, so these tests run without DSNs.
package service

import (
	"reflect"
	"testing"
	"time"

	"github.com/simpwf/workflow-engine/internal/workflow/model"
)

func TestFlattenNodeMap(t *testing.T) {
	nested := &model.NodeContent{ID: "nested", Type: model.NodeTypeScript}
	group := &model.NodeContent{
		ID: "grp", Type: model.NodeTypeGroup,
		Group: &model.GroupContent{Nodes: []*model.NodeContent{nested, nil}},
	}
	top := &model.NodeContent{ID: "top", Type: model.NodeTypeScript}
	got := flattenNodeMap([]*model.NodeContent{top, nil, group})

	// Membership matches flattenNodeIDs exactly: every id including the
	// group node itself and its nested children, nil nodes tolerated.
	if len(got) != 3 {
		t.Fatalf("map keys = %v, want 3 entries", keysOf(got))
	}
	for _, id := range []string{"top", "grp", "nested"} {
		if _, ok := got[id]; !ok {
			t.Errorf("missing %q in %v", id, keysOf(got))
		}
	}
	if got["top"] != top || got["grp"] != group || got["nested"] != nested {
		t.Error("values must be the tree's own node pointers")
	}
	if empty := flattenNodeMap(nil); len(empty) != 0 {
		t.Errorf("nil input = %v, want empty map", keysOf(empty))
	}
}

func keysOf(m map[string]*model.NodeContent) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

func TestHasLiveExecution(t *testing.T) {
	live := []model.ParallelExecutionStatus{
		model.ParallelWaitingForBranches,
		model.ParallelReadyToJoin,
	}
	done := []model.ParallelExecutionStatus{
		model.ParallelExecutionCompleted,
		model.ParallelExecutionFailed,
		model.ParallelExecutionCancelled,
	}
	for _, s := range live {
		if !hasLiveExecution([]model.ParallelExecution{{Status: s}}) {
			t.Errorf("status %q: live = false, want true", s)
		}
	}
	for _, s := range done {
		if hasLiveExecution([]model.ParallelExecution{{Status: s}}) {
			t.Errorf("status %q: live = true, want false", s)
		}
	}
	if hasLiveExecution(nil) {
		t.Error("empty executions: live = true, want false")
	}
	mixed := []model.ParallelExecution{
		{Status: model.ParallelExecutionCompleted},
		{Status: model.ParallelReadyToJoin},
	}
	if !hasLiveExecution(mixed) {
		t.Error("mixed executions: live = false, want true")
	}
}

func TestStatusParallel(t *testing.T) {
	now := time.Date(2026, 10, 5, 12, 0, 0, 0, time.UTC)
	parent := "parent-branch-id"
	exs := []model.ParallelExecution{
		{
			ID: "ex-1", Depth: 1,
			StartNodeID: "start", EndNodeID: "end",
			Status:      model.ParallelWaitingForBranches,
			BranchCount: 2, CompletedCount: 1,
		},
		{
			ID: "ex-2", ParentBranchID: &parent, Depth: 2,
			StartNodeID: "nstart", EndNodeID: "nend",
			Status:      model.ParallelExecutionCompleted,
			BranchCount: 1, CompletedCount: 1,
		},
	}
	branchesByEx := map[string][]model.ParallelBranch{
		"ex-1": {
			{
				ID: "b-1", Name: "a", BranchIndex: 0, StartNodeID: "na",
				Status: model.ParallelBranchWaiting, WaitingReason: model.WaitingReasonInput,
				UpdatedAt: now,
			},
			{
				ID: "b-2", Name: "b", BranchIndex: 1, StartNodeID: "nb",
				Status: model.ParallelBranchFailed, WaitingReason: model.WaitingReasonRunnable,
				Error: "boom", UpdatedAt: now,
			},
		},
		// ex-2 deliberately missing: an execution without listed
		// branches renders an empty (non-nil) branch list.
	}
	got := statusParallel(exs, branchesByEx)
	want := []ParallelExecutionView{
		{
			ID: "ex-1", Depth: 1,
			StartNodeID: "start", EndNodeID: "end",
			Status: "waiting_for_branches", BranchCount: 2, CompletedCount: 1,
			Branches: []ParallelBranchView{
				{ID: "b-1", Name: "a", BranchIndex: 0, StartNodeID: "na", Status: "waiting", WaitingReason: "input", UpdatedAt: now},
				{ID: "b-2", Name: "b", BranchIndex: 1, StartNodeID: "nb", Status: "failed", WaitingReason: "", Error: "boom", UpdatedAt: now},
			},
		},
		{
			ID: "ex-2", ParentBranchID: &parent, Depth: 2,
			StartNodeID: "nstart", EndNodeID: "nend",
			Status: "completed", BranchCount: 1, CompletedCount: 1,
			Branches: []ParallelBranchView{},
		},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("statusParallel() =\n%+v\nwant\n%+v", got, want)
	}

	if got := statusParallel(nil, nil); got != nil {
		t.Errorf("nil executions = %+v, want nil", got)
	}
	if got := statusParallel([]model.ParallelExecution{}, branchesByEx); got != nil {
		t.Errorf("empty executions = %+v, want nil", got)
	}
}
