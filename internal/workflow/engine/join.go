package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/simpwf/workflow-engine/internal/workflow/executor"
	"github.com/simpwf/workflow-engine/internal/workflow/kernel"
	"github.com/simpwf/workflow-engine/internal/workflow/model"
	"github.com/simpwf/workflow-engine/internal/workflow/repository"
)

// joinParallel executes a parallel_end node in the parent scope: it resolves
// the execution joining here, runs combining_script over the frozen branch
// view, and commits the merged parent advance with the execution completion
// atomically.
func (e *Engine) joinParallel(ctx context.Context, sc *execScope, g *workflowGraph, frame *model.Frame, counters model.Counters, nc *model.NodeContent) error {
	ex, err := e.joinableExecution(ctx, sc, nc.ID)
	if err != nil {
		return e.fail(ctx, sc, err)
	}
	switch ex.Status {
	case model.ParallelWaitingForBranches:
		// Spuriously woken before the barrier armed: re-park on the join
		// so the claim is released instead of spinning.
		ctxMap, err := unmarshalContext(sc.contextRaw)
		if err != nil {
			return e.fail(ctx, sc, err)
		}
		return e.checkpoint(ctx, sc, frame, counters, ctxMap, model.WorkflowWaiting, model.WaitingReasonParallel, "", nil, nil)
	case model.ParallelExecutionFailed, model.ParallelExecutionCancelled:
		return e.fail(ctx, sc, fmt.Errorf("parallel execution %s %s", ex.ID, ex.Status))
	case model.ParallelReadyToJoin:
		// Join below.
	default:
		return e.fail(ctx, sc, fmt.Errorf("no active parallel execution joins at %q", nc.ID))
	}

	branches, err := e.parallel.ListBranches(ctx, ex.ID)
	if err != nil {
		return err
	}
	ctxMap, err := unmarshalContext(sc.contextRaw)
	if err != nil {
		return e.failJoin(ctx, sc, ex.ID, err)
	}
	merged, interrupted, err := e.runJoinScript(ctx, branches, nc, ctxMap, counters)
	if err != nil {
		return e.failJoin(ctx, sc, ex.ID, err)
	}
	if interrupted {
		return nil
	}

	done, exited, err := kernel.Advance(frame, g, nc.NextNode)
	if err != nil {
		return e.failJoin(ctx, sc, ex.ID, err)
	}
	finalCtx := merged
	if len(exited) > 0 {
		finalCtx, err = kernel.RunExitedGroupPosts(ctx, e.hooks, g, exited, merged)
		if err != nil {
			return e.failJoin(ctx, sc, ex.ID, err)
		}
	}

	status := e.nextStatus(sc)
	var finished *time.Time
	now := e.now()
	if done {
		status = model.WorkflowFinished
		finished = &now
	}
	var history *model.NodeContextHistory
	if sc.contextMode == "lean" {
		history, err = e.historyForCommit(ctx, sc, &contextCommit{start: ctxMap, end: finalCtx, nodeID: nc.ID, attempt: 1}, finalCtx)
		if err != nil {
			return e.failJoin(ctx, sc, ex.ID, err)
		}
	}
	ctxRaw, err := marshal(finalCtx)
	if err != nil {
		return e.failJoin(ctx, sc, ex.ID, err)
	}
	err = e.parallel.JoinParallel(ctx, repository.JoinCheckpoint{
		Checkpoint: repository.Checkpoint{
			InstanceID:           sc.instanceID,
			WorkerID:             sc.leasedBy,
			Revision:             sc.revision,
			WorkflowDefinitionID: sc.definitionID,
			FromStatus:           model.WorkflowRunning,
			FromWaitingReason:    model.WaitingReasonRunnable,
			Status:               status,
			WaitingReason:        "",
			PauseRequested:       sc.pauseRequested,
			Frame:                *frame,
			Counters:             counters,
			Context:              ctxRaw,
			History:              history,
			FinishedAt:           finished,
		},
		ExecutionID: ex.ID,
	})
	if errors.Is(err, repository.ErrLeaseLost) {
		return nil
	}
	if errors.Is(err, repository.ErrStatusConflict) {
		// The execution resolved between our read and the commit.
		return e.joinConflict(ctx, sc, ex.ID)
	}
	if err != nil {
		return err
	}
	_ = e.appendEvent(ctx, sc, "parallel_completed", map[string]any{
		"node_id":               nc.ID,
		"parallel_execution_id": ex.ID,
		"start_node_id":         ex.StartNodeID,
		"end_node_id":           nc.ID,
	})
	slog.Info("parallel join completed", "instance_id", sc.instanceID,
		"parallel_execution_id", ex.ID, "node_id", nc.ID)
	return nil
}

// joinNestedParallel executes a parallel_end node in a branch scope: it
// resolves the execution nested directly under the branch, runs
// combining_script over the frozen branch view, and commits the merged
// branch advance with the nested execution completion atomically. The
// branch resumes past the join; the instance row is untouched.
func (e *Engine) joinNestedParallel(ctx context.Context, sc *execScope, g *workflowGraph, frame *model.Frame, counters model.Counters, nc *model.NodeContent) error {
	ex, err := e.joinableExecution(ctx, sc, nc.ID)
	if err != nil {
		return e.fail(ctx, sc, err)
	}
	switch ex.Status {
	case model.ParallelWaitingForBranches:
		// Spuriously woken before the barrier armed: re-park on the join
		// so the claim is released instead of spinning.
		ctxMap, err := unmarshalContext(sc.contextRaw)
		if err != nil {
			return e.fail(ctx, sc, err)
		}
		return e.checkpoint(ctx, sc, frame, counters, ctxMap, model.WorkflowWaiting, model.WaitingReasonParallel, "", nil, nil)
	case model.ParallelExecutionFailed, model.ParallelExecutionCancelled:
		return e.fail(ctx, sc, fmt.Errorf("parallel execution %s %s", ex.ID, ex.Status))
	case model.ParallelReadyToJoin:
		// Join below.
	default:
		return e.fail(ctx, sc, fmt.Errorf("no active parallel execution joins at %q", nc.ID))
	}

	branches, err := e.parallel.ListBranches(ctx, ex.ID)
	if err != nil {
		return err
	}
	ctxMap, err := unmarshalContext(sc.contextRaw)
	if err != nil {
		return e.failJoin(ctx, sc, ex.ID, err)
	}
	merged, interrupted, err := e.runJoinScript(ctx, branches, nc, ctxMap, counters)
	if err != nil {
		return e.failJoin(ctx, sc, ex.ID, err)
	}
	if interrupted {
		return nil
	}

	done, exited, err := kernel.Advance(frame, g, nc.NextNode)
	if err != nil {
		return e.failJoin(ctx, sc, ex.ID, err)
	}
	if done {
		// Unreachable: validation guarantees every branch path reaches
		// the outer join. Fail loudly instead of dropping the branch.
		return e.failJoin(ctx, sc, ex.ID, errors.New("branch terminated without reaching its parallel_end"))
	}
	finalCtx := merged
	if len(exited) > 0 {
		finalCtx, err = kernel.RunExitedGroupPosts(ctx, e.hooks, g, exited, merged)
		if err != nil {
			return e.failJoin(ctx, sc, ex.ID, err)
		}
	}

	ctxRaw, err := marshal(finalCtx)
	if err != nil {
		return e.failJoin(ctx, sc, ex.ID, err)
	}
	err = e.parallel.JoinBranchParallel(ctx, repository.JoinBranchCheckpoint{
		BranchCheckpoint: repository.BranchCheckpoint{
			BranchID:      sc.branchID,
			WorkerID:      sc.leasedBy,
			Revision:      sc.revision,
			Status:        model.ParallelBranchWaiting,
			WaitingReason: nextBranchReason(sc, model.WaitingReasonRunnable),
			Frame:         *frame,
			Counters:      counters,
			Context:       ctxRaw,
		},
		ExecutionID: ex.ID,
	})
	if errors.Is(err, repository.ErrLeaseLost) {
		return nil
	}
	if errors.Is(err, repository.ErrStatusConflict) {
		// The execution resolved between our read and the commit.
		return e.joinConflict(ctx, sc, ex.ID)
	}
	if err != nil {
		return err
	}
	_ = e.appendEvent(ctx, sc, "parallel_completed", map[string]any{
		"node_id":               nc.ID,
		"parallel_execution_id": ex.ID,
		"start_node_id":         ex.StartNodeID,
		"end_node_id":           nc.ID,
	})
	slog.Info("parallel join completed", "instance_id", sc.instanceID,
		"parallel_execution_id", ex.ID, "node_id", nc.ID)
	return nil
}

// joinableExecution returns the non-terminal execution joining at an end
// node in the scope: loop iterations leave completed executions behind,
// and at most one execution per end node is ever active per owner. A
// branch scope only sees executions nested directly under it, so a branch
// can never join a sibling's barrier.
func (e *Engine) joinableExecution(ctx context.Context, sc *execScope, endNodeID string) (*model.ParallelExecution, error) {
	exs, err := e.parallel.ListExecutions(ctx, sc.instanceID)
	if err != nil {
		return nil, err
	}
	for _, ex := range exs {
		if ex.EndNodeID != endNodeID {
			continue
		}
		if sc.isBranch() {
			if ex.ParentBranchID == nil || *ex.ParentBranchID != sc.branchID {
				continue
			}
		} else if ex.ParentBranchID != nil {
			continue
		}
		switch ex.Status {
		case model.ParallelExecutionCompleted:
			continue
		default:
			return &ex, nil
		}
	}
	return nil, fmt.Errorf("no active parallel execution joins at %q", endNodeID)
}

// runJoinScript records the join visit and runs combining_script over the
// frozen branch view. It reports interrupted when the context died before
// anything committed, so the caller releases the claim for a re-drive.
func (e *Engine) runJoinScript(ctx context.Context, branches []model.ParallelBranch, nc *model.NodeContent, ctxMap map[string]any, counters model.Counters) (map[string]any, bool, error) {
	if err := counters.Record(nc.ID, e.limits); err != nil {
		return nil, false, err
	}
	view := make(map[string]any, len(branches))
	for _, b := range branches {
		var bctx map[string]any
		if err := json.Unmarshal(b.Context, &bctx); err != nil {
			return nil, false, err
		}
		view[b.Name] = map[string]any{
			"context":       bctx,
			"status":        string(b.Status),
			"branch_id":     b.ID,
			"start_node_id": b.StartNodeID,
		}
	}
	res, err := executor.RunScript(ctx, executor.ScriptOptions{
		Source:     nc.CombiningScript,
		Context:    ctxMap,
		Timeout:    e.limits.ConditionTimeout,
		Vars:       map[string]any{"branch": view},
		FrozenVars: []string{"branch"},
	})
	if err != nil {
		if ctx.Err() != nil {
			// Interrupted before committing: the lease expires and the
			// join is re-driven; the script re-runs like a node retry.
			return nil, true, nil
		}
		return nil, false, fmt.Errorf("combining_script: %w", err)
	}
	return res.Context, false, nil
}

// joinConflict resolves a lost join race: the execution resolved after our
// readiness read. A failed execution fails the scope; anything else is
// defensive.
func (e *Engine) joinConflict(ctx context.Context, sc *execScope, executionID string) error {
	ex, err := e.parallel.GetExecution(ctx, executionID)
	if err != nil {
		return err
	}
	switch ex.Status {
	case model.ParallelExecutionFailed, model.ParallelExecutionCancelled:
		return e.fail(ctx, sc, fmt.Errorf("parallel execution %s %s", ex.ID, ex.Status))
	default:
		return e.fail(ctx, sc, fmt.Errorf("parallel execution %s already %s", ex.ID, ex.Status))
	}
}

// failJoin fails the execution and the scope together so a broken join never
// leaves a ready execution behind. The execution failure is recorded here
// because FailExecution is silent and the scope failure below reports the
// scope, not the join.
func (e *Engine) failJoin(ctx context.Context, sc *execScope, executionID string, cause error) error {
	if err := e.parallel.FailExecution(ctx, executionID); err != nil {
		return err
	}
	_ = e.appendEvent(ctx, sc, "parallel_failed", map[string]any{
		"parallel_execution_id": executionID,
		"error":                 cause.Error(),
	})
	slog.Info("parallel join failed", "instance_id", sc.instanceID,
		"parallel_execution_id", executionID, "error", cause.Error())
	return e.fail(ctx, sc, cause)
}
