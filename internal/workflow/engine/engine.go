// Package engine implements the durable cursor/frame state machine, the
// leased dispatcher, and node execution for workflow instances.
package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/simpwf/workflow-engine/internal/workflow/executor"
	"github.com/simpwf/workflow-engine/internal/workflow/model"
	"github.com/simpwf/workflow-engine/internal/workflow/repository"
	"github.com/simpwf/workflow-engine/pkg/contextdiff"
	"github.com/simpwf/workflow-engine/pkg/contextpath"
	"github.com/simpwf/workflow-engine/pkg/ids"
)

// WorkflowLoader resolves the materialized executable node tree for an
// instance's workflow definition.
type WorkflowLoader func(ctx context.Context, instanceID string) (*model.WorkflowContent, error)

// Engine executes one node transition per claimed instance: it recovers
// interrupted attempts, runs the current node through the executors, and
// commits the new cursor and full context snapshot via a fenced checkpoint.
// The cancellation registry maps instance ids to the cancel function of
// their in-flight transition, so a stop can interrupt local execution and
// any replica's heartbeat can interrupt execution on this worker.
type Engine struct {
	instances repository.InstanceRepository
	parallel  repository.ParallelRepository
	executors map[model.NodeType]executor.Executor
	hooks     *executor.HookRunner
	limits    model.Limits
	loader    WorkflowLoader
	actor     string
	lean      model.LeanOptions
	now       func() time.Time

	mu      sync.RWMutex
	cancels map[string]context.CancelFunc
}

// NewEngine builds the engine. actor is the audit actor for appended events.
// parallel may be nil only in tests that never touch parallel execution.
func NewEngine(
	instances repository.InstanceRepository,
	parallel repository.ParallelRepository,
	executors map[model.NodeType]executor.Executor,
	hooks *executor.HookRunner,
	limits model.Limits,
	loader WorkflowLoader,
	actor string,
	lean model.LeanOptions,
) *Engine {
	return &Engine{
		instances: instances,
		parallel:  parallel,
		executors: executors,
		hooks:     hooks,
		limits:    limits,
		loader:    loader,
		actor:     actor,
		lean:      lean,
		now:       time.Now,
		cancels:   map[string]context.CancelFunc{},
	}
}

// RegisterCancel records the cancel function of an instance's in-flight
// transition.
func (e *Engine) RegisterCancel(instanceID string, cancel context.CancelFunc) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.cancels[instanceID] = cancel
}

// UnregisterCancel removes a finished transition from the registry.
func (e *Engine) UnregisterCancel(instanceID string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	delete(e.cancels, instanceID)
}

// Cancel interrupts the in-flight transition of an instance on this worker,
// if any. It is a no-op when the instance is not executing locally.
func (e *Engine) Cancel(instanceID string) {
	e.mu.RLock()
	cancel, ok := e.cancels[instanceID]
	e.mu.RUnlock()
	if ok {
		cancel()
	}
}

// Process performs one node transition for a claimed instance.
func (e *Engine) Process(ctx context.Context, w model.WorkflowInstance) error {
	// The transition is cancellable while it runs so a stop can interrupt
	// executors mid-flight; the dispatcher heartbeat finds cross-replica
	// stops and calls Cancel through this registration.
	runCtx, cancel := context.WithCancel(ctx)
	e.RegisterCancel(w.ID, cancel)
	defer func() {
		cancel()
		e.UnregisterCancel(w.ID)
	}()
	ctx = runCtx

	got, err := e.instances.GetByID(ctx, w.ID)
	if err != nil {
		return err
	}
	cur := *got
	if cur.Status != model.WorkflowRunning {
		// A stop or terminal transition won the race; nothing to commit.
		return nil
	}
	sc := instanceScope(cur)

	wf, err := e.loader(ctx, cur.ID)
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

	// Recovery: an interrupted attempt left running by a dead worker. A
	// superseded (stopped + cancelled) row is terminal audit, never
	// recovery fuel: skip it so the cursor re-executes forward instead.
	attempt, err := e.instances.GetRunningNodeInstance(ctx, cur.ID)
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
		return e.joinParallel(ctx, sc, g, &frame, counters, nc)
	default:
		return e.runNode(ctx, sc, g, &frame, counters, nc)
	}
}

// enterGroup runs the group pre hook, pushes the group, and commits the new
// cursor without an attempt.
func (e *Engine) enterGroup(ctx context.Context, sc *execScope, g *workflowGraph, frame *model.Frame, counters model.Counters, nc *model.NodeContent) error {
	ctxMap, err := unmarshalContext(sc.contextRaw)
	if err != nil {
		return e.fail(ctx, sc, err)
	}
	preCtx, err := e.hooks.RunPre(ctx, nc, ctxMap)
	if err != nil {
		return e.failWithContext(ctx, sc, err, ctxMap, nil)
	}
	if _, err := EnterGroup(frame, g, nc.ID); err != nil {
		return e.fail(ctx, sc, err)
	}
	_ = e.appendEvent(ctx, sc, "group_entered", map[string]any{"node_id": nc.ID})
	return e.checkpoint(ctx, sc, frame, counters, preCtx, e.nextStatus(sc), "", "", nil, &contextCommit{
		start:  ctxMap,
		end:    preCtx,
		nodeID: nc.ID,
	})
}

// waitInput runs the input pre hook, creates the input node occurrence, and
// parks the scope as waiting with reason input. The transformed context
// is checkpointed so the pre hook runs exactly once per parking.
func (e *Engine) waitInput(ctx context.Context, sc *execScope, frame *model.Frame, counters model.Counters, nc *model.NodeContent) error {
	now := e.now()
	ctxMap, err := unmarshalContext(sc.contextRaw)
	if err != nil {
		return e.fail(ctx, sc, err)
	}
	preCtx, err := e.hooks.RunPre(ctx, nc, ctxMap)
	if err != nil {
		return e.failWithContext(ctx, sc, err, ctxMap, nil)
	}
	attempt := newAttempt(sc.instanceID, sc.branchID, nc, now)
	attempt.Status = model.NodeRunning
	if sc.contextMode == "lean" {
		attempt.ContextBefore = json.RawMessage("null")
		diff, err := contextdiff.DiffMaps(ctxMap, preCtx)
		if err != nil {
			return e.fail(ctx, sc, err)
		}
		attempt.ContextAfter = diff.JSON()
	} else {
		ctxBefore, err := marshal(preCtx)
		if err != nil {
			return e.fail(ctx, sc, err)
		}
		attempt.ContextBefore = ctxBefore
	}
	attempt.StartedAt = &now
	if err := e.instances.InsertNodeInstance(ctx, attempt); err != nil {
		return err
	}
	_ = e.appendEvent(ctx, sc, "node_started", map[string]any{"node_id": nc.ID, "occurrence_id": attempt.ID, "attempt": attempt.Attempt})
	_ = e.appendEvent(ctx, sc, "input_waiting", map[string]any{"node_id": nc.ID, "occurrence_id": attempt.ID})
	if err := e.inputCheckpoint(ctx, sc, frame, counters, preCtx, &contextCommit{
		start:        ctxMap,
		end:          preCtx,
		occurrenceID: attempt.ID,
		nodeID:       nc.ID,
		attempt:      attempt.Attempt,
	}); err != nil {
		return err
	}
	return e.fenceParkedAttempt(ctx, sc, attempt)
}

// fenceParkedAttempt marks a parked attempt stopped when a stop fenced its
// checkpoint. Branch scopes have no preemption source yet (branch
// cancellation lands with the cancellation cascade), so only parent scopes
// check.
func (e *Engine) fenceParkedAttempt(ctx context.Context, sc *execScope, attempt model.NodeInstance) error {
	if sc.isBranch() {
		return nil
	}
	// A stop may have fenced the input checkpoint; the parked attempt must
	// be marked stopped rather than left running forever.
	bg := context.WithoutCancel(ctx)
	got, err := e.instances.GetByID(bg, sc.instanceID)
	if err == nil && got.Status == model.WorkflowStopped {
		now := e.now()
		attempt.Status = model.NodeStopped
		attempt.Cancelled = true
		attempt.StoppedAt = &now
		attempt.UpdatedAt = now
		if err := e.instances.UpdateNodeInstance(bg, attempt); err != nil {
			return err
		}
		return e.instances.ResolveTermination(bg, sc.instanceID)
	}
	return nil
}

// runNode prepares a fresh or looped occurrence and executes it.
func (e *Engine) runNode(ctx context.Context, sc *execScope, g *workflowGraph, frame *model.Frame, counters model.Counters, nc *model.NodeContent) error {
	if err := counters.Record(nc.ID, e.limits); err != nil {
		return e.fail(ctx, sc, err)
	}
	now := e.now()
	ctxMap, err := unmarshalContext(sc.contextRaw)
	if err != nil {
		return e.fail(ctx, sc, err)
	}

	attempt, err := e.getAttemptByNode(ctx, sc, nc.ID)
	if errors.Is(err, repository.ErrNodeInstanceNotFound) {
		a := newAttempt(sc.instanceID, sc.branchID, nc, now)
		attempt = &a
		attempt.Status = model.NodeRunning
		if sc.contextMode == "lean" {
			attempt.ContextBefore = json.RawMessage("null")
		} else {
			ctxBefore, err := marshal(ctxMap)
			if err != nil {
				return e.fail(ctx, sc, err)
			}
			attempt.ContextBefore = ctxBefore
		}
		attempt.StartedAt = &now
		if err := e.instances.InsertNodeInstance(ctx, *attempt); err != nil {
			return err
		}
	} else if err != nil {
		return err
	} else if attempt.Status == model.NodeStopped && attempt.Cancelled {
		// A rollback superseded this occurrence while it was parked. Rewind
		// to a fresh occurrence row so the superseded attempt keeps its
		// stopped audit trail instead of being resurrected.
		a := newAttempt(sc.instanceID, sc.branchID, nc, now)
		attempt = &a
		attempt.Status = model.NodeRunning
		if sc.contextMode == "lean" {
			attempt.ContextBefore = json.RawMessage("null")
		} else {
			ctxBefore, err := marshal(ctxMap)
			if err != nil {
				return e.fail(ctx, sc, err)
			}
			attempt.ContextBefore = ctxBefore
		}
		attempt.StartedAt = &now
		if err := e.instances.InsertNodeInstance(ctx, *attempt); err != nil {
			return err
		}
	} else {
		// Loop iteration: a new attempt of the same occurrence.
		attempt.Attempt++
		attempt.Status = model.NodeRunning
		if sc.contextMode == "lean" {
			attempt.ContextBefore = json.RawMessage("null")
		} else {
			ctxBefore, err := marshal(ctxMap)
			if err != nil {
				return e.fail(ctx, sc, err)
			}
			attempt.ContextBefore = ctxBefore
		}
		attempt.Output = json.RawMessage("null")
		attempt.ContextAfter = json.RawMessage("null")
		attempt.Error = ""
		attempt.StartedAt = &now
		attempt.FinishedAt = nil
		attempt.StoppedAt = nil
		attempt.Cancelled = false
		attempt.UpdatedAt = now
		if err := e.instances.UpdateNodeInstance(ctx, *attempt); err != nil {
			return err
		}
	}
	_ = e.appendEvent(ctx, sc, "node_started", map[string]any{"node_id": nc.ID, "occurrence_id": attempt.ID, "attempt": attempt.Attempt})
	startCtx := ctxMap
	if sc.contextMode == "lean" {
		startCtx, err = cloneContextMap(ctxMap)
		if err != nil {
			return e.fail(ctx, sc, err)
		}
	}
	return e.executeStep(ctx, sc, g, frame, counters, nc, attempt, startCtx, ctxMap)
}

// recover handles an attempt left running by a dead worker: pure nodes and
// external_call nodes with retry_on_recovery requeue (new attempt), anything
// else fails the node and the workflow.
func (e *Engine) recover(ctx context.Context, sc *execScope, g *workflowGraph, frame *model.Frame, counters model.Counters, attempt *model.NodeInstance) error {
	nc, err := g.Node(attempt.NodeID)
	if err != nil {
		return e.fail(ctx, sc, err)
	}
	ctxMap, err := unmarshalContext(sc.contextRaw)
	if err != nil {
		return e.fail(ctx, sc, err)
	}
	now := e.now()

	// Input nodes re-enter waiting (they never execute on recovery).
	if attempt.Type == string(model.NodeTypeInput) {
		// A rollback may have moved the cursor away while this parked
		// attempt stayed running. Re-park on the attempt's own node so
		// the cursor and the park never diverge (DeliverInput keys off
		// the cursor).
		reconciledFrom := ""
		if frame.CurrentNodeID != attempt.NodeID {
			stack, serr := g.groupStack(attempt.NodeID)
			if serr != nil {
				return e.fail(ctx, sc, serr)
			}
			reconciledFrom = frame.CurrentNodeID
			frame.CurrentNodeID = attempt.NodeID
			frame.GroupStack = stack
		}
		attempt.Attempt++
		if sc.contextMode == "lean" {
			attempt.ContextAfter = emptyDiffJSON()
		}
		attempt.Status = model.NodeRunning
		if sc.contextMode == "lean" {
			attempt.ContextBefore = json.RawMessage("null")
		} else {
			ctxBefore, err := marshal(ctxMap)
			if err != nil {
				return e.fail(ctx, sc, err)
			}
			attempt.ContextBefore = ctxBefore
		}
		attempt.StartedAt = &now
		attempt.RecoveryResult = "retried"
		attempt.UpdatedAt = now
		if err := e.instances.UpdateNodeInstance(ctx, *attempt); err != nil {
			return err
		}
		_ = e.appendEvent(ctx, sc, "node_retried", map[string]any{"node_id": nc.ID, "occurrence_id": attempt.ID, "attempt": attempt.Attempt})
		if reconciledFrom != "" {
			_ = e.appendEvent(ctx, sc, "cursor_reconciled", map[string]any{"from_node": reconciledFrom, "to_node": attempt.NodeID, "occurrence_id": attempt.ID, "attempt": attempt.Attempt})
		}
		return e.inputCheckpoint(ctx, sc, frame, counters, ctxMap, &contextCommit{
			start:        ctxMap,
			end:          ctxMap,
			occurrenceID: attempt.ID,
			nodeID:       nc.ID,
			attempt:      attempt.Attempt,
		})
	}

	retry := attempt.Type == string(model.NodeTypeScript) || nc.RetryOnRecovery
	if !retry {
		if nc.OnFailure != nil {
			recErr := &executor.NodeError{Node: nc, Reason: "recovery", Err: errors.New("node interrupted by recovery; retry_on_recovery=false")}
			attempt.RecoveryResult = "failed"
			return e.routeFailure(ctx, sc, g, frame, counters, nc, attempt, ctxMap, ctxMap, recErr, nil)
		}
		attempt.Status = model.NodeFailed
		attempt.Error = "node interrupted by recovery; retry_on_recovery=false"
		attempt.RecoveryResult = "failed"
		if sc.contextMode == "lean" {
			attempt.ContextBefore = json.RawMessage("null")
			attempt.ContextAfter = emptyDiffJSON()
		}
		attempt.FinishedAt = &now
		attempt.UpdatedAt = now
		if err := e.instances.UpdateNodeInstance(ctx, *attempt); err != nil {
			return err
		}
		_ = e.appendEvent(ctx, sc, "node_failed", map[string]any{"node_id": nc.ID, "occurrence_id": attempt.ID, "error": attempt.Error})
		return e.failWithContext(ctx, sc, errors.New("node interrupted by recovery; retry_on_recovery=false"), ctxMap, &contextCommit{
			start:        ctxMap,
			end:          ctxMap,
			occurrenceID: attempt.ID,
			nodeID:       attempt.NodeID,
			attempt:      attempt.Attempt,
		})
	}

	attempt.Attempt++
	attempt.Status = model.NodeRunning
	if sc.contextMode == "lean" {
		attempt.ContextBefore = json.RawMessage("null")
	} else {
		ctxBefore, err := marshal(ctxMap)
		if err != nil {
			return e.fail(ctx, sc, err)
		}
		attempt.ContextBefore = ctxBefore
	}
	attempt.Output = json.RawMessage("null")
	attempt.ContextAfter = json.RawMessage("null")
	attempt.Error = ""
	attempt.StartedAt = &now
	attempt.FinishedAt = nil
	attempt.StoppedAt = nil
	attempt.Cancelled = false
	attempt.RecoveryResult = "retried"
	attempt.UpdatedAt = now
	if err := e.instances.UpdateNodeInstance(ctx, *attempt); err != nil {
		return err
	}
	_ = e.appendEvent(ctx, sc, "node_retried", map[string]any{"node_id": nc.ID, "occurrence_id": attempt.ID, "attempt": attempt.Attempt})
	if err := counters.Record(nc.ID, e.limits); err != nil {
		return e.fail(ctx, sc, err)
	}
	startCtx := ctxMap
	if sc.contextMode == "lean" {
		startCtx, err = cloneContextMap(ctxMap)
		if err != nil {
			return e.fail(ctx, sc, err)
		}
	}
	return e.executeStep(ctx, sc, g, frame, counters, nc, attempt, startCtx, ctxMap)
}

// executeStep runs the pre hook, the executor, and the post hook, persists
// the attempt outcome, and commits the next cursor transition. Exited groups
// run their post hooks innermost-first before the checkpoint.
func (e *Engine) executeStep(ctx context.Context, sc *execScope, g *workflowGraph, frame *model.Frame, counters model.Counters, nc *model.NodeContent, attempt *model.NodeInstance, startCtx map[string]any, ctxMap map[string]any) error {
	preCtx, err := e.hooks.RunPre(ctx, nc, ctxMap)
	if err != nil {
		return e.failNode(ctx, sc, attempt, startCtx, ctxMap, err)
	}
	ctxMap = preCtx

	req := executor.Request{Node: nc, Context: ctxMap, IdempotencyKey: sc.idempotencyKey(attempt.ID), InstanceID: sc.instanceID, NodeInstanceID: attempt.ID}
	if nc.InputData != nil {
		v, err := contextpath.Get(ctxMap, *nc.InputData)
		if err != nil {
			return e.failNode(ctx, sc, attempt, startCtx, ctxMap, err)
		}
		req.Vars = map[string]any{"input": v}
	}

	res, err := e.executors[nc.Type].Execute(ctx, req)
	if err != nil {
		if ctx.Err() != nil {
			return e.interrupted(ctx, sc, attempt)
		}
		if nc.OnFailure != nil {
			return e.routeFailure(ctx, sc, g, frame, counters, nc, attempt, startCtx, ctxMap, err, res)
		}
		return e.failNode(ctx, sc, attempt, startCtx, ctxMap, err)
	}

	next := nc.NextNode
	outCtx := ctxMap
	var hookOutput any
	if nc.Type == model.NodeTypeConditions {
		cr, ok := res.Output.(*executor.ConditionResult)
		if !ok {
			return e.failNode(ctx, sc, attempt, startCtx, ctxMap, errors.New("conditions executor returned an invalid result"))
		}
		if !cr.Matched {
			return e.failNode(ctx, sc, attempt, startCtx, ctxMap, fmt.Errorf("no condition matched in node %s", nc.ID))
		}
		if cr.Key == "" {
			next = ""
		} else {
			target, ok := g.KeyTarget(nc.ID, cr.Key)
			if !ok {
				return e.failNode(ctx, sc, attempt, startCtx, ctxMap, fmt.Errorf("condition key %q of node %s is not defined in its workflow or group", cr.Key, nc.ID))
			}
			next = target
		}
		hookOutput = map[string]any{"matched": cr.Matched, "index": cr.Index, "key": cr.Key}
	} else {
		if res.Context != nil {
			outCtx = res.Context
		}
		key := strings.TrimSpace(nc.OutputProperty)
		if key == "" {
			key = nc.ID
		}
		if outCtx == nil {
			outCtx = map[string]any{}
		}
		outCtx[key] = res.Output
		hookOutput = res.Output
	}

	postCtx, err := e.hooks.RunPost(ctx, nc, outCtx, hookOutput)
	if err != nil {
		return e.failNode(ctx, sc, attempt, startCtx, outCtx, err)
	}
	outCtx = postCtx

	now := e.now()
	attempt.Status = model.NodeFinished
	output, err := marshal(res.Output)
	if err != nil {
		return e.failNode(ctx, sc, attempt, startCtx, outCtx, err)
	}
	attempt.Output = output
	if sc.contextMode == "lean" {
		diff, err := contextdiff.DiffMaps(startCtx, outCtx)
		if err != nil {
			return e.failNode(ctx, sc, attempt, startCtx, outCtx, err)
		}
		attempt.ContextBefore = json.RawMessage("null")
		attempt.ContextAfter = diff.JSON()
	} else {
		ctxAfter, err := marshal(outCtx)
		if err != nil {
			return e.failNode(ctx, sc, attempt, startCtx, outCtx, err)
		}
		attempt.ContextAfter = ctxAfter
	}
	attempt.FinishedAt = &now
	attempt.UpdatedAt = now
	if err := e.instances.UpdateNodeInstance(ctx, *attempt); err != nil {
		return err
	}
	_ = e.appendEvent(ctx, sc, "node_finished", map[string]any{"node_id": nc.ID, "occurrence_id": attempt.ID, "attempt": attempt.Attempt})

	done, exited, err := Advance(frame, g, next)
	if err != nil {
		return e.fail(ctx, sc, err)
	}
	finalCtx := outCtx
	if len(exited) > 0 {
		finalCtx, err = RunExitedGroupPosts(ctx, e.hooks, g, exited, outCtx)
		if err != nil {
			// The child attempt is already finished; the failure is a
			// structural hook failure. Preserve the latest context instead
			// of rolling back to the last checkpoint.
			return e.failWithContext(ctx, sc, err, finalCtx, &contextCommit{
				start:        startCtx,
				end:          finalCtx,
				occurrenceID: attempt.ID,
				nodeID:       attempt.NodeID,
				attempt:      attempt.Attempt,
			})
		}
	}
	for _, gid := range exited {
		_ = e.appendEvent(ctx, sc, "group_exited", map[string]any{"node_id": gid})
	}
	commit := &contextCommit{
		start:        startCtx,
		end:          finalCtx,
		occurrenceID: attempt.ID,
		nodeID:       attempt.NodeID,
		attempt:      attempt.Attempt,
	}
	if done {
		return e.checkpoint(ctx, sc, frame, counters, finalCtx, model.WorkflowFinished, "", "", &now, commit)
	}
	return e.checkpoint(ctx, sc, frame, counters, finalCtx, e.nextStatus(sc), "", "", nil, commit)
}

// ExitedGroupLookup resolves a group node by id for post-exit hooks.
type ExitedGroupLookup interface {
	Node(id string) (*model.NodeContent, error)
}

// RunExitedGroupPosts executes the post_script of each exited group
// innermost-first, threading the workflow context through the hooks in
// order. The returned context is the last successfully transformed one:
// when a hook fails it is the context before that hook, so the caller can
// persist the latest completed state.
func RunExitedGroupPosts(ctx context.Context, hooks *executor.HookRunner, lookup ExitedGroupLookup, exited []string, ctxMap map[string]any) (map[string]any, error) {
	curCtx := ctxMap
	for _, gid := range exited {
		gnc, err := lookup.Node(gid)
		if err != nil {
			return curCtx, err
		}
		next, err := hooks.RunPost(ctx, gnc, curCtx, nil)
		if err != nil {
			return curCtx, err
		}
		curCtx = next
	}
	return curCtx, nil
}

// routeFailure records a handled execution failure on an external_call,
// poller, or custom node, stores structured failure details at
// on_failure.output_property in context,
// advances the frame to on_failure.next_node without running post_script, and
// checkpoints the workflow in a runnable/waiting state without workflow error.
func (e *Engine) routeFailure(ctx context.Context, sc *execScope, g *workflowGraph, frame *model.Frame, counters model.Counters, nc *model.NodeContent, attempt *model.NodeInstance, startCtx map[string]any, ctxMap map[string]any, cause error, res *executor.Result) error {
	reason := "error"
	var ne *executor.NodeError
	if errors.As(cause, &ne) && ne.Reason != "" {
		reason = ne.Reason
	}
	var resOutput any
	if res != nil && res.Output != nil {
		resOutput = res.Output
	}

	failurePayload := map[string]any{
		"message": cause.Error(),
		"reason":  reason,
		"result":  resOutput,
	}

	outCtx := make(map[string]any, len(ctxMap)+1)
	for k, v := range ctxMap {
		outCtx[k] = v
	}
	outCtx[nc.OnFailure.OutputProperty] = failurePayload

	now := e.now()
	attempt.Status = model.NodeFailed
	attempt.Error = cause.Error()
	if resOutput != nil {
		output, err := marshal(resOutput)
		if err != nil {
			return e.failNode(ctx, sc, attempt, startCtx, outCtx, err)
		}
		attempt.Output = output
	} else {
		attempt.Output = json.RawMessage("null")
	}
	if sc.contextMode == "lean" {
		diff, err := contextdiff.DiffMaps(startCtx, outCtx)
		if err != nil {
			return e.failNode(ctx, sc, attempt, startCtx, outCtx, err)
		}
		attempt.ContextBefore = json.RawMessage("null")
		attempt.ContextAfter = diff.JSON()
	} else {
		ctxAfter, err := marshal(outCtx)
		if err != nil {
			return e.failNode(ctx, sc, attempt, startCtx, outCtx, err)
		}
		attempt.ContextAfter = ctxAfter
	}
	attempt.FinishedAt = &now
	attempt.UpdatedAt = now
	if err := e.instances.UpdateNodeInstance(ctx, *attempt); err != nil {
		return err
	}

	_ = e.appendEvent(ctx, sc, "node_failed", map[string]any{
		"node_id":       attempt.NodeID,
		"occurrence_id": attempt.ID,
		"error":         cause.Error(),
	})
	_ = e.appendEvent(ctx, sc, "node_failure_routed", map[string]any{
		"node_id":         nc.ID,
		"occurrence_id":   attempt.ID,
		"target_node_id":  nc.OnFailure.NextNode,
		"output_property": nc.OnFailure.OutputProperty,
		"reason":          reason,
	})

	done, exited, err := Advance(frame, g, nc.OnFailure.NextNode)
	if err != nil {
		return e.fail(ctx, sc, err)
	}
	finalCtx := outCtx
	if len(exited) > 0 {
		finalCtx, err = RunExitedGroupPosts(ctx, e.hooks, g, exited, outCtx)
		if err != nil {
			return e.failWithContext(ctx, sc, err, finalCtx, &contextCommit{
				start:        startCtx,
				end:          finalCtx,
				occurrenceID: attempt.ID,
				nodeID:       attempt.NodeID,
				attempt:      attempt.Attempt,
			})
		}
	}
	for _, gid := range exited {
		_ = e.appendEvent(ctx, sc, "group_exited", map[string]any{"node_id": gid})
	}
	if done {
		return e.checkpoint(ctx, sc, frame, counters, finalCtx, model.WorkflowFinished, "", "", &now, nil)
	}
	return e.checkpoint(ctx, sc, frame, counters, finalCtx, e.nextStatus(sc), "", "", nil, nil)
}

// failNode marks the running attempt failed and fails the scope.
func (e *Engine) failNode(ctx context.Context, sc *execScope, attempt *model.NodeInstance, startCtx map[string]any, ctxMap map[string]any, cause error) error {
	now := e.now()
	attempt.Status = model.NodeFailed
	attempt.Error = cause.Error()
	if sc.contextMode == "lean" {
		diff, err := contextdiff.DiffMaps(startCtx, ctxMap)
		if err != nil {
			return err
		}
		attempt.ContextBefore = json.RawMessage("null")
		attempt.ContextAfter = diff.JSON()
	} else {
		ctxAfter, err := marshal(ctxMap)
		if err != nil {
			// ctxMap itself is unserializable (a rejected executor
			// value was already merged in): fall back to the
			// pre-transition context, which derives from stored JSON
			// and is JSON-clean by construction. Never guess-persist.
			ctxAfter, err = marshal(startCtx)
			if err != nil {
				return err
			}
		}
		attempt.ContextAfter = ctxAfter
	}
	attempt.FinishedAt = &now
	attempt.UpdatedAt = now
	if err := e.instances.UpdateNodeInstance(ctx, *attempt); err != nil {
		return err
	}
	_ = e.appendEvent(ctx, sc, "node_failed", map[string]any{"node_id": attempt.NodeID, "occurrence_id": attempt.ID, "error": cause.Error()})
	if sc.contextMode != "lean" {
		return e.fail(ctx, sc, cause)
	}
	return e.failWithContext(ctx, sc, cause, ctxMap, &contextCommit{
		start:        startCtx,
		end:          ctxMap,
		occurrenceID: attempt.ID,
		nodeID:       attempt.NodeID,
		attempt:      attempt.Attempt,
	})
}

// interrupted cleans up an attempt whose executor was cancelled. When a stop
// committed, the attempt becomes stopped and the termination flag is
// resolved; when the interruption came from somewhere else (e.g. dispatcher
// shutdown), the attempt is left running so another worker recovers it.
func (e *Engine) interrupted(ctx context.Context, sc *execScope, attempt *model.NodeInstance) error {
	bg := context.WithoutCancel(ctx)
	stopped, err := e.scopeStopped(bg, sc)
	if err != nil || !stopped {
		// No stop committed: leave the attempt running for recovery.
		return nil
	}

	now := e.now()
	attempt.Status = model.NodeStopped
	attempt.Cancelled = true
	attempt.StoppedAt = &now
	if attempt.Error == "" {
		attempt.Error = "node interrupted by stop"
	}
	attempt.UpdatedAt = now
	if err := e.instances.UpdateNodeInstance(bg, *attempt); err != nil {
		return err
	}
	_ = e.appendEvent(bg, sc, "node_stopped", map[string]any{"node_id": attempt.NodeID, "occurrence_id": attempt.ID, "error": attempt.Error})
	_ = e.appendEvent(bg, sc, "cancellation", map[string]any{"node_id": attempt.NodeID, "occurrence_id": attempt.ID, "reason": "stop"})
	if sc.isBranch() {
		return nil
	}
	return e.instances.ResolveTermination(bg, sc.instanceID)
}

// scopeStopped reports whether the scope was preempted: a stopped parent
// instance, or a branch whose row already resolved terminal.
func (e *Engine) scopeStopped(ctx context.Context, sc *execScope) (bool, error) {
	if !sc.isBranch() {
		got, err := e.instances.GetByID(ctx, sc.instanceID)
		if err != nil {
			return false, err
		}
		return got.Status == model.WorkflowStopped, nil
	}
	got, err := e.parallel.GetBranch(ctx, sc.branchID)
	if err != nil {
		return false, err
	}
	switch got.Status {
	case model.ParallelBranchFailed, model.ParallelBranchCancelled, model.ParallelBranchCompleted:
		return true, nil
	default:
		return false, nil
	}
}

// fail commits a failed terminal state with the cause.
func (e *Engine) fail(ctx context.Context, sc *execScope, cause error) error {
	ctxMap, err := unmarshalContext(sc.contextRaw)
	if err != nil {
		ctxMap = map[string]any{}
	}
	return e.failWithContext(ctx, sc, cause, ctxMap, nil)
}

// failWithContext commits a failed terminal state with the cause, persisting
// the given context instead of the last checkpoint. Structural hook failures
// (group pre/post) use it so the latest completed context survives.
func (e *Engine) failWithContext(ctx context.Context, sc *execScope, cause error, ctxMap map[string]any, commit *contextCommit) error {
	now := e.now()
	if !sc.isBranch() {
		_ = e.appendEvent(ctx, sc, "workflow_failed", map[string]any{"error": cause.Error()})
	}
	frame, ferr := model.ParseFrame(sc.frameRaw)
	if ferr != nil {
		frame = model.Frame{}
	}
	counters, cerr := model.ParseCounters(sc.countersRaw)
	if cerr != nil {
		counters = model.Counters{}
	}
	if sc.contextMode == "lean" && commit == nil {
		startCtx, err := unmarshalContext(sc.contextRaw)
		if err != nil {
			startCtx = map[string]any{}
		}
		commit = &contextCommit{start: startCtx, end: ctxMap}
	}
	return e.checkpoint(ctx, sc, &frame, counters, ctxMap, model.WorkflowFailed, "", cause.Error(), &now, commit)
}

// checkpoint commits the transition under the worker's lease and revision.
// A debug step that lands paused appends a debug-marked paused event so
// UIs can tell step-through pauses from operator pauses. Branch scopes map
// terminal states onto the branch lifecycle and complete at the join
// instead of checkpointing past it.
func (e *Engine) checkpoint(ctx context.Context, sc *execScope, frame *model.Frame, counters model.Counters, ctxMap map[string]any, status model.WorkflowStatus, reason model.WaitingReason, errMsg string, finished *time.Time, commit *contextCommit) error {
	if sc.isBranch() {
		return e.branchCheckpoint(ctx, sc, frame, counters, ctxMap, status, reason, errMsg)
	}
	var history *model.NodeContextHistory
	var err error
	if sc.contextMode == "lean" {
		history, err = e.historyForCommit(ctx, sc, commit, ctxMap)
		if err != nil {
			return err
		}
	}
	ctxRaw, err := marshal(ctxMap)
	if err != nil {
		return err
	}
	cp := repository.Checkpoint{
		InstanceID:           sc.instanceID,
		WorkerID:             sc.leasedBy,
		Revision:             sc.revision,
		WorkflowDefinitionID: sc.definitionID,
		FromStatus:           sc.fromStatus,
		FromWaitingReason:    sc.fromReason,
		Status:               status,
		WaitingReason:        reason,
		PauseRequested:       sc.pauseRequested,
		Frame:                *frame,
		Counters:             counters,
		Context:              ctxRaw,
		Error:                errMsg,
		FinishedAt:           finished,
		History:              history,
	}
	err = e.instances.Checkpoint(ctx, cp)
	if errors.Is(err, repository.ErrLeaseLost) {
		// Stop won the race; the worker is fenced and aborts silently.
		return nil
	}
	if err != nil {
		return err
	}
	if status == model.WorkflowPaused && reason != model.WaitingReasonInput && sc.debug && !sc.pauseRequested {
		// The checkpoint releases the lease, so append outside the
		// cancelled Process scope to survive dispatcher shutdown.
		_ = e.appendEvent(context.WithoutCancel(ctx), sc, "paused", map[string]any{"debug": true, "node_id": commitNodeID(commit)})
	}
	return nil
}

// branchCheckpoint commits one branch transition: failures fail the branch
// (and its execution), arrivals at the join complete through the barrier,
// and every other step checkpoints as waiting.
func (e *Engine) branchCheckpoint(ctx context.Context, sc *execScope, frame *model.Frame, counters model.Counters, ctxMap map[string]any, status model.WorkflowStatus, reason model.WaitingReason, errMsg string) error {
	if status == model.WorkflowFailed {
		failed := sc.branchIdentity()
		failed["error"] = errMsg
		_ = e.appendEvent(ctx, sc, "parallel_branch_failed", failed)
		failedParent, err := e.parallel.FailBranch(ctx, sc.branchID, sc.leasedBy, sc.revision, errMsg)
		if err != nil {
			if errors.Is(err, repository.ErrLeaseLost) {
				return nil
			}
			return err
		}
		slog.Info("parallel branch failed", "instance_id", sc.instanceID,
			"parallel_execution_id", sc.executionID, "branch_id", sc.branchID,
			"branch_name", sc.branchName, "error", errMsg)
		if failedParent {
			_ = e.appendEvent(ctx, sc, "parallel_failed", map[string]any{"parallel_execution_id": sc.executionID, "error": errMsg})
			_ = e.appendEvent(ctx, sc, "workflow_failed", map[string]any{"error": errMsg})
		}
		return nil
	}
	if status == model.WorkflowFinished {
		// Unreachable: validation guarantees every branch path reaches the
		// join. Fail loudly instead of silently dropping the branch.
		failed := sc.branchIdentity()
		failed["error"] = "branch terminated without reaching its parallel_end"
		_ = e.appendEvent(ctx, sc, "parallel_branch_failed", failed)
		failedParent, err := e.parallel.FailBranch(ctx, sc.branchID, sc.leasedBy, sc.revision, "branch terminated without reaching its parallel_end")
		if err != nil {
			if errors.Is(err, repository.ErrLeaseLost) {
				return nil
			}
			return err
		}
		if failedParent {
			_ = e.appendEvent(ctx, sc, "parallel_failed", map[string]any{"parallel_execution_id": sc.executionID, "error": "branch terminated without reaching its parallel_end"})
			_ = e.appendEvent(ctx, sc, "workflow_failed", map[string]any{"error": "branch terminated without reaching its parallel_end"})
		}
		return nil
	}
	if frame.CurrentNodeID == sc.endNodeID {
		ctxRaw, err := marshal(ctxMap)
		if err != nil {
			return err
		}
		ready, err := e.parallel.CompleteBranch(ctx, repository.BranchCompletion{
			BranchID: sc.branchID, WorkerID: sc.leasedBy, Revision: sc.revision,
			Frame: *frame, Counters: counters, Context: ctxRaw,
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
	ctxRaw, err := marshal(ctxMap)
	if err != nil {
		return err
	}
	err = e.parallel.CheckpointBranch(ctx, repository.BranchCheckpoint{
		BranchID: sc.branchID, WorkerID: sc.leasedBy, Revision: sc.revision,
		Status: model.ParallelBranchWaiting, WaitingReason: nextBranchReason(sc, reason),
		Frame: *frame, Counters: counters, Context: ctxRaw, Error: errMsg,
	})
	if errors.Is(err, repository.ErrLeaseLost) {
		return nil
	}
	return err
}

// commitNodeID extracts the stepped node for the debug paused event.
func commitNodeID(commit *contextCommit) string {
	if commit == nil {
		return ""
	}
	return commit.nodeID
}

// inputCheckpoint parks the cursor waiting (or paused) on an input node.
func (e *Engine) inputCheckpoint(ctx context.Context, sc *execScope, frame *model.Frame, counters model.Counters, ctxMap map[string]any, commit *contextCommit) error {
	status := model.WorkflowWaiting
	if sc.pauseRequested {
		status = model.WorkflowPaused
	}
	return e.checkpoint(ctx, sc, frame, counters, ctxMap, status, model.WaitingReasonInput, "", nil, commit)
}

type contextCommit struct {
	start        map[string]any
	end          map[string]any
	occurrenceID string
	nodeID       string
	attempt      int
}

func (e *Engine) historyForCommit(ctx context.Context, sc *execScope, commit *contextCommit, endCtx map[string]any) (*model.NodeContextHistory, error) {
	if commit == nil {
		startCtx, err := unmarshalContext(sc.contextRaw)
		if err != nil {
			startCtx = map[string]any{}
		}
		commit = &contextCommit{start: startCtx, end: endCtx}
	}
	if commit.end == nil {
		commit.end = endCtx
	}
	diff, err := contextdiff.DiffMaps(commit.start, commit.end)
	if err != nil {
		return nil, err
	}
	history, err := e.instances.LoadHistory(ctx, sc.instanceID)
	if err != nil {
		return nil, err
	}
	row := &model.NodeContextHistory{
		OccurrenceID: commit.occurrenceID,
		NodeID:       commit.nodeID,
		Attempt:      commit.attempt,
		Snapshot:     json.RawMessage("null"),
		Diff:         diff.JSON(),
	}
	if e.lean.AnchorEvery > 0 && (len(history)+1)%e.lean.AnchorEvery == 0 {
		row.IsAnchor = true
		snapshot, err := marshal(commit.end)
		if err != nil {
			return nil, err
		}
		row.Snapshot = snapshot
		row.Diff = json.RawMessage("null")
	}
	return row, nil
}

func emptyDiffJSON() json.RawMessage {
	return json.RawMessage(`{"set":{},"unset":[]}`)
}

// nextStatus decides the post-checkpoint status, honoring a deferred
// pause and debug step-through: debug runs re-pause after every node
// transition so each resume advances exactly one step. Input parks go
// through inputCheckpoint, never here.
func (e *Engine) nextStatus(sc *execScope) model.WorkflowStatus {
	if sc.pauseRequested || sc.debug {
		return model.WorkflowPaused
	}
	return model.WorkflowWaiting
}

// nextBranchReason mirrors nextStatus for branch parks: debug runs park
// each branch step paused so every resume advances exactly one step. Input
// and barrier parks pass through: an input park still waits for its
// delivery and a barrier park must stay wakeable.
func nextBranchReason(sc *execScope, reason model.WaitingReason) model.WaitingReason {
	if sc.debug && reason == model.WaitingReasonRunnable {
		return model.WaitingReasonPaused
	}
	return reason
}

func (e *Engine) appendEvent(ctx context.Context, sc *execScope, typ string, data map[string]any) error {
	if sc.isBranch() {
		data["branch_id"] = sc.branchID
		if _, ok := data["parallel_execution_id"]; !ok {
			data["parallel_execution_id"] = sc.executionID
		}
	}
	raw, _ := json.Marshal(data)
	return e.instances.AppendEvent(ctx, model.WorkflowInstanceEvent{
		ID:                 newID(),
		WorkflowInstanceID: sc.instanceID,
		Type:               typ,
		Data:               raw,
		CreatedBy:          e.actor,
		CreatedAt:          e.now(),
	})
}

// appendBranchEvent records a lifecycle event for a branch row observed
// outside a built scope (stale claims): the row carries the same stable
// identifiers appendEvent tags from a scope.
func (e *Engine) appendBranchEvent(ctx context.Context, b model.ParallelBranch, typ string, data map[string]any) error {
	data["branch_id"] = b.ID
	data["parallel_execution_id"] = b.ParallelExecutionID
	data["branch_name"] = b.Name
	data["branch_index"] = b.BranchIndex
	data["start_node_id"] = b.StartNodeID
	raw, _ := json.Marshal(data)
	return e.instances.AppendEvent(ctx, model.WorkflowInstanceEvent{
		ID:                 newID(),
		WorkflowInstanceID: b.InstanceID,
		Type:               typ,
		Data:               raw,
		CreatedBy:          e.actor,
		CreatedAt:          e.now(),
	})
}

func newAttempt(instanceID, branchID string, nc *model.NodeContent, now time.Time) model.NodeInstance {
	policy := ""
	if nc.RetryOnRecovery {
		policy = "retry"
	}
	return model.NodeInstance{
		ID:                 newID(),
		WorkflowInstanceID: instanceID,
		BranchID:           branchID,
		NodeID:             nc.ID,
		NodeDefinitionID:   nc.NodeDefinitionID,
		Name:               nc.Name,
		Type:               string(nc.Type),
		Attempt:            1,
		Status:             model.NodeWaiting,
		Input:              json.RawMessage("null"),
		Output:             json.RawMessage("null"),
		ContextBefore:      json.RawMessage("null"),
		ContextAfter:       json.RawMessage("null"),
		RecoveryPolicy:     policy,
		CreatedAt:          now,
		UpdatedAt:          now,
	}
}

func newID() string {
	id, err := ids.New()
	if err != nil {
		panic(err)
	}
	return id.String()
}

// marshal encodes v for persistence. It returns an error instead of a
// silent "null" so marshal failures surface as node failures (CORR-1).
func marshal(v any) (json.RawMessage, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("engine: value is not JSON-serializable: %w", err)
	}
	return b, nil
}

func unmarshalContext(raw json.RawMessage) (map[string]any, error) {
	m := map[string]any{}
	if len(raw) == 0 {
		return m, nil
	}
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil, fmt.Errorf("engine: parse instance context: %w", err)
	}
	if m == nil {
		// A stored "null" unmarshals to a nil map; normalize so
		// callers always get a writable context (CORR-1).
		m = map[string]any{}
	}
	return m, nil
}

func cloneContextMap(ctxMap map[string]any) (map[string]any, error) {
	raw, err := marshal(ctxMap)
	if err != nil {
		return nil, err
	}
	return unmarshalContext(raw)
}
