package engine

import (
	"context"
	"errors"
	"log/slog"

	"github.com/simpwf/workflow-engine/internal/workflow/model"
	"github.com/simpwf/workflow-engine/internal/workflow/repository"
)

// ProcessBranch performs one node transition for a claimed parallel branch.
// It mirrors Process: reload the leased row, recover an interrupted attempt,
// and run the cursor node through the shared transition methods.
func (e *Engine) ProcessBranch(ctx context.Context, b model.ParallelBranch) error {
	runCtx, cancel := context.WithCancel(ctx)
	e.RegisterCancel(b.ID, cancel)
	defer func() {
		cancel()
		e.UnregisterCancel(b.ID)
	}()
	ctx = runCtx

	got, err := e.parallel.GetBranch(ctx, b.ID)
	if err != nil {
		return err
	}
	cur := *got
	if cur.Status != model.ParallelBranchRunning {
		// A cancel, failure, or completion won the race; nothing to commit.
		// A stale claim on a cancelled branch is the engine's only sighting
		// of the cancel, so it records the event here (best-effort: a
		// cancel with no in-flight claim leaves no branch event).
		if cur.Status == model.ParallelBranchCancelled {
			_ = e.appendBranchEvent(ctx, cur, "parallel_branch_cancelled", map[string]any{})
			slog.Info("parallel branch cancelled", "instance_id", cur.InstanceID,
				"parallel_execution_id", cur.ParallelExecutionID, "branch_id", cur.ID,
				"branch_name", cur.Name)
		}
		return nil
	}
	ex, err := e.parallel.GetExecution(ctx, cur.ParallelExecutionID)
	if err != nil {
		return err
	}
	if ex.Status != model.ParallelWaitingForBranches {
		// The execution resolved while this branch was leased; release the
		// branch as cancelled instead of committing into a dead execution.
		return e.cancelStaleBranch(ctx, cur)
	}
	inst, err := e.instances.GetByID(ctx, cur.InstanceID)
	if err != nil {
		return err
	}
	sc := branchScope(cur, *ex, inst.Debug)

	wf, err := e.loader(ctx, cur.InstanceID)
	if err != nil {
		return e.fail(ctx, sc, err)
	}
	g := buildGraph(wf)

	frame, err := model.ParseFrame(cur.Frame)
	if err != nil {
		return e.fail(ctx, sc, err)
	}
	counters, err := model.ParseCounters(cur.Counters)
	if err != nil {
		return e.fail(ctx, sc, err)
	}

	// A branch parked at its own join completes without executing.
	if frame.CurrentNodeID == ex.EndNodeID {
		ready, err := e.parallel.CompleteBranch(ctx, repository.BranchCompletion{
			BranchID: cur.ID, WorkerID: cur.LeasedBy, Revision: cur.Revision,
			Frame: frame, Counters: counters, Context: cur.Context,
		})
		if errors.Is(err, repository.ErrLeaseLost) {
			return nil
		}
		if err != nil {
			return err
		}
		_ = e.appendEvent(ctx, sc, "parallel_branch_finished", sc.branchIdentity())
		slog.Info("parallel branch finished", "instance_id", sc.instanceID,
			"parallel_execution_id", sc.executionID, "branch_id", sc.branchID,
			"branch_name", sc.branchName)
		if ready {
			slog.Info("parallel ready to join", "instance_id", sc.instanceID,
				"parallel_execution_id", sc.executionID)
		}
		return nil
	}

	attempt, err := e.instances.GetRunningBranchNodeInstance(ctx, cur.ID)
	if err == nil {
		return e.recover(ctx, sc, g, &frame, counters, attempt)
	}
	if !errors.Is(err, repository.ErrNodeInstanceNotFound) {
		return err
	}

	nc, err := g.Node(frame.CurrentNodeID)
	if err != nil {
		return e.fail(ctx, sc, err)
	}
	switch nc.Type {
	case model.NodeTypeGroup:
		return e.enterGroup(ctx, sc, g, &frame, counters, nc)
	case model.NodeTypeInput:
		return e.waitInput(ctx, sc, &frame, counters, nc)
	case model.NodeTypeParallelStart:
		return e.forkParallel(ctx, sc, &frame, counters, nc)
	case model.NodeTypeParallelEnd:
		return e.joinNestedParallel(ctx, sc, g, &frame, counters, nc)
	default:
		return e.runNode(ctx, sc, g, &frame, counters, nc)
	}
}

// cancelStaleBranch releases a branch whose execution already resolved. The
// cursor is preserved on a best-effort basis; a cancelled branch never
// resumes, so a corrupt frame must not wedge the lease.
func (e *Engine) cancelStaleBranch(ctx context.Context, cur model.ParallelBranch) error {
	frame, _ := model.ParseFrame(cur.Frame)
	counters, _ := model.ParseCounters(cur.Counters)
	// Best-effort cursor preservation: fall back to the stored bytes
	// whenever the context cannot be re-encoded.
	ctxRaw := cur.Context
	if ctxMap, uerr := unmarshalContext(cur.Context); uerr == nil {
		if raw, merr := marshal(ctxMap); merr == nil {
			ctxRaw = raw
		}
	}
	err := e.parallel.CheckpointBranch(ctx, repository.BranchCheckpoint{
		BranchID: cur.ID, WorkerID: cur.LeasedBy, Revision: cur.Revision,
		Status: model.ParallelBranchCancelled, Frame: frame, Counters: counters,
		Context: ctxRaw,
	})
	if errors.Is(err, repository.ErrLeaseLost) {
		return nil
	}
	if err != nil {
		return err
	}
	_ = e.appendBranchEvent(ctx, cur, "parallel_branch_cancelled", map[string]any{})
	slog.Info("parallel branch cancelled", "instance_id", cur.InstanceID,
		"parallel_execution_id", cur.ParallelExecutionID, "branch_id", cur.ID,
		"branch_name", cur.Name)
	return nil
}

// getAttemptByNode loads the occurrence for a scope: branch-owned rows for
// branch scopes, parent-scope rows otherwise.
func (e *Engine) getAttemptByNode(ctx context.Context, sc *execScope, nodeID string) (*model.NodeInstance, error) {
	if sc.isBranch() {
		return e.instances.GetBranchNodeInstanceByNode(ctx, sc.branchID, nodeID)
	}
	return e.instances.GetNodeInstanceByNode(ctx, sc.instanceID, nodeID)
}
