package engine

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"

	"github.com/simpwf/workflow-engine/internal/workflow/model"
	"github.com/simpwf/workflow-engine/internal/workflow/repository"
)

// forkParallel executes a parallel_start node in any scope: it checks the
// depth and active-branch limits, records the fork, and commits the scope
// park, the execution, and the branches atomically. The forking cursor
// moves to the join while its context stays frozen; each branch starts
// from a snapshot.
func (e *Engine) forkParallel(ctx context.Context, sc *execScope, frame *model.Frame, counters model.Counters, nc *model.NodeContent) error {
	if err := counters.Record(nc.ID, e.limits); err != nil {
		return e.fail(ctx, sc, err)
	}
	depth := sc.depth + 1
	if max := e.limits.MaxParallelDepth; max > 0 && depth > max {
		return e.fail(ctx, sc, fmt.Errorf("parallel_start %q nests %d deep, at most %d allowed", nc.ID, depth, max))
	}
	if max := e.limits.MaxActiveBranchesPerInstance; max > 0 {
		active, err := e.parallel.CountActiveBranches(ctx, sc.instanceID)
		if err != nil {
			return err
		}
		if fresh := int64(len(nc.ParallelBranches)); active+fresh > int64(max) {
			return e.fail(ctx, sc, fmt.Errorf("parallel_start %q would exceed the active branch limit: %d active + %d new > %d", nc.ID, active, fresh, max))
		}
	}
	names := make([]string, 0, len(nc.ParallelBranches))
	for name := range nc.ParallelBranches {
		names = append(names, name)
	}
	sort.Strings(names)
	branches := make([]repository.ForkBranch, 0, len(names))
	for i, name := range names {
		target := nc.ParallelBranches[name]
		branches = append(branches, repository.ForkBranch{
			Name:        name,
			BranchIndex: i,
			StartNodeID: target,
			Frame:       model.NewFrame(target),
			Context:     append([]byte(nil), sc.contextRaw...),
		})
	}
	forkFrame := model.NewFrame(nc.ParallelEndNodeID)
	forkFrame.GroupStack = append([]string(nil), frame.GroupStack...)
	f := repository.ForkParallel{
		InstanceID:           sc.instanceID,
		WorkflowDefinitionID: sc.definitionID,
		WorkerID:             sc.leasedBy,
		Revision:             sc.revision,
		Depth:                depth,
		StartNodeID:          nc.ID,
		EndNodeID:            nc.ParallelEndNodeID,
		Frame:                forkFrame,
		Counters:             counters,
		Context:              sc.contextRaw,
		Branches:             branches,
		Debug:                sc.debug,
	}
	if sc.isBranch() {
		f.ParentBranchID = &sc.branchID
	}
	ex, _, err := e.parallel.Fork(ctx, f)
	if err != nil {
		if errors.Is(err, repository.ErrLeaseLost) {
			return nil
		}
		return err
	}
	_ = e.appendEvent(ctx, sc, "parallel_started", map[string]any{
		"node_id":               nc.ID,
		"parallel_execution_id": ex.ID,
		"branch_count":          len(branches),
		"start_node_id":         nc.ID,
		"end_node_id":           nc.ParallelEndNodeID,
	})
	slog.Info("parallel forked", "instance_id", sc.instanceID,
		"parallel_execution_id", ex.ID, "node_id", nc.ID,
		"depth", ex.Depth, "branch_count", len(branches))
	return nil
}
