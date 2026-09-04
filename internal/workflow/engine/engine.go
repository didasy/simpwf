// Package engine implements the durable cursor/frame state machine, the
// leased dispatcher, and node execution for workflow instances.
package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/simpwf/workflow-engine/internal/workflow/executor"
	"github.com/simpwf/workflow-engine/internal/workflow/model"
	"github.com/simpwf/workflow-engine/internal/workflow/repository"
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
	executors map[model.NodeType]executor.Executor
	hooks     *executor.HookRunner
	limits    model.Limits
	loader    WorkflowLoader
	actor     string
	now       func() time.Time

	mu      sync.RWMutex
	cancels map[string]context.CancelFunc
}

// NewEngine builds the engine. actor is the audit actor for appended events.
func NewEngine(
	instances repository.InstanceRepository,
	executors map[model.NodeType]executor.Executor,
	hooks *executor.HookRunner,
	limits model.Limits,
	loader WorkflowLoader,
	actor string,
) *Engine {
	return &Engine{
		instances: instances,
		executors: executors,
		hooks:     hooks,
		limits:    limits,
		loader:    loader,
		actor:     actor,
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

	wf, err := e.loader(ctx, cur.ID)
	if err != nil {
		return e.fail(ctx, cur, "", err)
	}
	g := buildGraph(wf)

	frame, err := model.ParseFrame(cur.Frame)
	if err != nil {
		return e.fail(ctx, cur, "", err)
	}
	counters, err := model.ParseCounters(cur.Counters)
	if err != nil {
		return e.fail(ctx, cur, "", err)
	}

	// Recovery: an interrupted attempt left running by a dead worker.
	attempt, err := e.instances.GetRunningNodeInstance(ctx, cur.ID)
	if err == nil {
		return e.recover(ctx, cur, g, &frame, counters, attempt)
	}
	if !errors.Is(err, repository.ErrNodeInstanceNotFound) {
		return err
	}

	nc, err := g.Node(frame.CurrentNodeID)
	if err != nil {
		return e.fail(ctx, cur, "", err)
	}
	switch nc.Type {
	case model.NodeTypeGroup:
		return e.enterGroup(ctx, cur, g, &frame, counters, nc)
	case model.NodeTypeInput:
		return e.waitInput(ctx, cur, &frame, counters, nc)
	default:
		return e.runNode(ctx, cur, g, &frame, counters, nc)
	}
}

// enterGroup runs the group pre hook, pushes the group, and commits the new
// cursor without an attempt.
func (e *Engine) enterGroup(ctx context.Context, cur model.WorkflowInstance, g *workflowGraph, frame *model.Frame, counters model.Counters, nc *model.NodeContent) error {
	ctxMap, err := unmarshalContext(cur.Context)
	if err != nil {
		return e.fail(ctx, cur, "", err)
	}
	preCtx, err := e.hooks.RunPre(ctx, nc, ctxMap)
	if err != nil {
		return e.failWithContext(ctx, cur, err, ctxMap)
	}
	if _, err := EnterGroup(frame, g, nc.ID); err != nil {
		return e.fail(ctx, cur, "", err)
	}
	_ = e.appendEvent(ctx, cur.ID, "group_entered", map[string]any{"node_id": nc.ID})
	return e.checkpoint(ctx, cur, frame, counters, preCtx, e.nextStatus(cur), "", "", nil)
}

// waitInput runs the input pre hook, creates the input node occurrence, and
// parks the instance as waiting with reason input. The transformed context
// is checkpointed so the pre hook runs exactly once per parking.
func (e *Engine) waitInput(ctx context.Context, cur model.WorkflowInstance, frame *model.Frame, counters model.Counters, nc *model.NodeContent) error {
	now := e.now()
	ctxMap, err := unmarshalContext(cur.Context)
	if err != nil {
		return e.fail(ctx, cur, "", err)
	}
	preCtx, err := e.hooks.RunPre(ctx, nc, ctxMap)
	if err != nil {
		return e.failWithContext(ctx, cur, err, ctxMap)
	}
	attempt := newAttempt(cur.ID, nc, now)
	attempt.Status = model.NodeRunning
	attempt.ContextBefore = marshal(preCtx)
	attempt.StartedAt = &now
	if err := e.instances.InsertNodeInstance(ctx, attempt); err != nil {
		return err
	}
	_ = e.appendEvent(ctx, cur.ID, "node_started", map[string]any{"node_id": nc.ID, "occurrence_id": attempt.ID, "attempt": attempt.Attempt})
	_ = e.appendEvent(ctx, cur.ID, "input_waiting", map[string]any{"node_id": nc.ID, "occurrence_id": attempt.ID})
	if err := e.inputCheckpoint(ctx, cur, frame, counters, preCtx); err != nil {
		return err
	}
	// A stop may have fenced the input checkpoint; the parked attempt must
	// be marked stopped rather than left running forever.
	bg := context.WithoutCancel(ctx)
	got, err := e.instances.GetByID(bg, cur.ID)
	if err == nil && got.Status == model.WorkflowStopped {
		now := e.now()
		attempt.Status = model.NodeStopped
		attempt.Cancelled = true
		attempt.StoppedAt = &now
		attempt.UpdatedAt = now
		if err := e.instances.UpdateNodeInstance(bg, attempt); err != nil {
			return err
		}
		return e.instances.ResolveTermination(bg, cur.ID)
	}
	return nil
}

// runNode prepares a fresh or looped occurrence and executes it.
func (e *Engine) runNode(ctx context.Context, cur model.WorkflowInstance, g *workflowGraph, frame *model.Frame, counters model.Counters, nc *model.NodeContent) error {
	if err := counters.Record(nc.ID, e.limits); err != nil {
		return e.fail(ctx, cur, "", err)
	}
	now := e.now()
	ctxMap, err := unmarshalContext(cur.Context)
	if err != nil {
		return e.fail(ctx, cur, "", err)
	}

	attempt, err := e.instances.GetNodeInstanceByNode(ctx, cur.ID, nc.ID)
	if errors.Is(err, repository.ErrNodeInstanceNotFound) {
		a := newAttempt(cur.ID, nc, now)
		attempt = &a
		attempt.Status = model.NodeRunning
		attempt.ContextBefore = marshal(ctxMap)
		attempt.StartedAt = &now
		if err := e.instances.InsertNodeInstance(ctx, *attempt); err != nil {
			return err
		}
	} else if err != nil {
		return err
	} else {
		// Loop iteration: a new attempt of the same occurrence.
		attempt.Attempt++
		attempt.Status = model.NodeRunning
		attempt.ContextBefore = marshal(ctxMap)
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
	_ = e.appendEvent(ctx, cur.ID, "node_started", map[string]any{"node_id": nc.ID, "occurrence_id": attempt.ID, "attempt": attempt.Attempt})
	return e.executeStep(ctx, cur, g, frame, counters, nc, attempt, ctxMap)
}

// recover handles an attempt left running by a dead worker: pure nodes and
// external_call nodes with retry_on_recovery requeue (new attempt), anything
// else fails the node and the workflow.
func (e *Engine) recover(ctx context.Context, cur model.WorkflowInstance, g *workflowGraph, frame *model.Frame, counters model.Counters, attempt *model.NodeInstance) error {
	nc, err := g.Node(attempt.NodeID)
	if err != nil {
		return e.fail(ctx, cur, "", err)
	}
	ctxMap, err := unmarshalContext(cur.Context)
	if err != nil {
		return e.fail(ctx, cur, "", err)
	}
	now := e.now()

	// Input nodes re-enter waiting (they never execute on recovery).
	if attempt.Type == string(model.NodeTypeInput) {
		if frame.CurrentNodeID != attempt.NodeID {
			from := frame.CurrentNodeID
			stack, gerr := g.groupStack(attempt.NodeID)
			if gerr != nil {
				return e.fail(ctx, cur, "", gerr)
			}
			frame.CurrentNodeID = attempt.NodeID
			frame.GroupStack = stack
			_ = e.appendEvent(ctx, cur.ID, "cursor_reconciled", map[string]any{"from_node": from, "to_node": attempt.NodeID})
		}
		attempt.Attempt++
		attempt.Status = model.NodeRunning
		attempt.ContextBefore = marshal(ctxMap)
		attempt.StartedAt = &now
		attempt.RecoveryResult = "retried"
		attempt.UpdatedAt = now
		if err := e.instances.UpdateNodeInstance(ctx, *attempt); err != nil {
			return err
		}
		_ = e.appendEvent(ctx, cur.ID, "node_retried", map[string]any{"node_id": nc.ID, "occurrence_id": attempt.ID, "attempt": attempt.Attempt})
		return e.inputCheckpoint(ctx, cur, frame, counters, ctxMap)
	}

	retry := attempt.Type == string(model.NodeTypeScript) || nc.RetryOnRecovery
	if !retry {
		if nc.OnFailure != nil {
			recErr := &executor.NodeError{Node: nc, Reason: "recovery", Err: errors.New("node interrupted by recovery; retry_on_recovery=false")}
			attempt.RecoveryResult = "failed"
			return e.routeFailure(ctx, cur, g, frame, counters, nc, attempt, ctxMap, recErr, nil)
		}
		attempt.Status = model.NodeFailed
		attempt.Error = "node interrupted by recovery; retry_on_recovery=false"
		attempt.RecoveryResult = "failed"
		attempt.FinishedAt = &now
		attempt.UpdatedAt = now
		if err := e.instances.UpdateNodeInstance(ctx, *attempt); err != nil {
			return err
		}
		_ = e.appendEvent(ctx, cur.ID, "node_failed", map[string]any{"node_id": nc.ID, "occurrence_id": attempt.ID, "error": attempt.Error})
		return e.fail(ctx, cur, "", errors.New("node interrupted by recovery; retry_on_recovery=false"))
	}

	attempt.Attempt++
	attempt.Status = model.NodeRunning
	attempt.ContextBefore = marshal(ctxMap)
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
	_ = e.appendEvent(ctx, cur.ID, "node_retried", map[string]any{"node_id": nc.ID, "occurrence_id": attempt.ID, "attempt": attempt.Attempt})
	if err := counters.Record(nc.ID, e.limits); err != nil {
		return e.fail(ctx, cur, "", err)
	}
	return e.executeStep(ctx, cur, g, frame, counters, nc, attempt, ctxMap)
}

// executeStep runs the pre hook, the executor, and the post hook, persists
// the attempt outcome, and commits the next cursor transition. Exited groups
// run their post hooks innermost-first before the checkpoint.
func (e *Engine) executeStep(ctx context.Context, cur model.WorkflowInstance, g *workflowGraph, frame *model.Frame, counters model.Counters, nc *model.NodeContent, attempt *model.NodeInstance, ctxMap map[string]any) error {
	preCtx, err := e.hooks.RunPre(ctx, nc, ctxMap)
	if err != nil {
		return e.failNode(ctx, cur, attempt, ctxMap, err)
	}
	ctxMap = preCtx

	req := executor.Request{Node: nc, Context: ctxMap, IdempotencyKey: cur.ID + ":" + attempt.ID, InstanceID: cur.ID, NodeInstanceID: attempt.ID}
	if nc.InputData != nil {
		v, err := contextpath.Get(ctxMap, *nc.InputData)
		if err != nil {
			return e.failNode(ctx, cur, attempt, ctxMap, err)
		}
		req.Vars = map[string]any{"input": v}
	}

	res, err := e.executors[nc.Type].Execute(ctx, req)
	if err != nil {
		if ctx.Err() != nil {
			return e.interrupted(ctx, cur, attempt)
		}
		if nc.OnFailure != nil {
			return e.routeFailure(ctx, cur, g, frame, counters, nc, attempt, ctxMap, err, res)
		}
		return e.failNode(ctx, cur, attempt, ctxMap, err)
	}

	next := nc.NextNode
	outCtx := ctxMap
	var hookOutput any
	if nc.Type == model.NodeTypeConditions {
		cr, ok := res.Output.(*executor.ConditionResult)
		if !ok {
			return e.failNode(ctx, cur, attempt, ctxMap, errors.New("conditions executor returned an invalid result"))
		}
		if !cr.Matched {
			return e.failNode(ctx, cur, attempt, ctxMap, fmt.Errorf("no condition matched in node %s", nc.ID))
		}
		if cr.Key == "" {
			next = ""
		} else {
			target, ok := g.KeyTarget(nc.ID, cr.Key)
			if !ok {
				return e.failNode(ctx, cur, attempt, ctxMap, fmt.Errorf("condition key %q of node %s is not defined in its workflow or group", cr.Key, nc.ID))
			}
			next = target
		}
		hookOutput = map[string]any{"matched": cr.Matched, "index": cr.Index, "key": cr.Key}
	} else {
		if res.Context != nil {
			outCtx = res.Context
		}
		key := nc.OutputProperty
		if key == "" {
			key = attempt.ID
		}
		outCtx[key] = res.Output
		hookOutput = res.Output
	}

	postCtx, err := e.hooks.RunPost(ctx, nc, outCtx, hookOutput)
	if err != nil {
		return e.failNode(ctx, cur, attempt, outCtx, err)
	}
	outCtx = postCtx

	now := e.now()
	attempt.Status = model.NodeFinished
	attempt.Output = marshal(res.Output)
	attempt.ContextAfter = marshal(outCtx)
	attempt.FinishedAt = &now
	attempt.UpdatedAt = now
	if err := e.instances.UpdateNodeInstance(ctx, *attempt); err != nil {
		return err
	}
	_ = e.appendEvent(ctx, cur.ID, "node_finished", map[string]any{"node_id": nc.ID, "occurrence_id": attempt.ID, "attempt": attempt.Attempt})

	done, exited, err := Advance(frame, g, next)
	if err != nil {
		return e.fail(ctx, cur, "", err)
	}
	finalCtx := outCtx
	if len(exited) > 0 {
		finalCtx, err = RunExitedGroupPosts(ctx, e.hooks, g, exited, outCtx)
		if err != nil {
			// The child attempt is already finished; the failure is a
			// structural hook failure. Preserve the latest context instead
			// of rolling back to the last checkpoint.
			return e.failWithContext(ctx, cur, err, finalCtx)
		}
	}
	for _, gid := range exited {
		_ = e.appendEvent(ctx, cur.ID, "group_exited", map[string]any{"node_id": gid})
	}
	if done {
		return e.checkpoint(ctx, cur, frame, counters, finalCtx, model.WorkflowFinished, "", "", &now)
	}
	return e.checkpoint(ctx, cur, frame, counters, finalCtx, e.nextStatus(cur), "", "", nil)
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

// routeFailure records a handled execution failure on an external_call or poller
// node, stores structured failure details at on_failure.output_property in context,
// advances the frame to on_failure.next_node without running post_script, and
// checkpoints the workflow in a runnable/waiting state without workflow error.
func (e *Engine) routeFailure(ctx context.Context, cur model.WorkflowInstance, g *workflowGraph, frame *model.Frame, counters model.Counters, nc *model.NodeContent, attempt *model.NodeInstance, ctxMap map[string]any, cause error, res *executor.Result) error {
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
		attempt.Output = marshal(resOutput)
	} else {
		attempt.Output = json.RawMessage("null")
	}
	attempt.ContextAfter = marshal(outCtx)
	attempt.FinishedAt = &now
	attempt.UpdatedAt = now
	if err := e.instances.UpdateNodeInstance(ctx, *attempt); err != nil {
		return err
	}

	_ = e.appendEvent(ctx, cur.ID, "node_failed", map[string]any{
		"node_id":       attempt.NodeID,
		"occurrence_id": attempt.ID,
		"error":         cause.Error(),
	})
	_ = e.appendEvent(ctx, cur.ID, "node_failure_routed", map[string]any{
		"node_id":         nc.ID,
		"occurrence_id":   attempt.ID,
		"target_node_id":  nc.OnFailure.NextNode,
		"output_property": nc.OnFailure.OutputProperty,
		"reason":          reason,
	})

	done, exited, err := Advance(frame, g, nc.OnFailure.NextNode)
	if err != nil {
		return e.fail(ctx, cur, "", err)
	}
	finalCtx := outCtx
	if len(exited) > 0 {
		finalCtx, err = RunExitedGroupPosts(ctx, e.hooks, g, exited, outCtx)
		if err != nil {
			return e.failWithContext(ctx, cur, err, finalCtx)
		}
	}
	for _, gid := range exited {
		_ = e.appendEvent(ctx, cur.ID, "group_exited", map[string]any{"node_id": gid})
	}
	if done {
		return e.checkpoint(ctx, cur, frame, counters, finalCtx, model.WorkflowFinished, "", "", &now)
	}
	return e.checkpoint(ctx, cur, frame, counters, finalCtx, e.nextStatus(cur), "", "", nil)
}

// failNode marks the running attempt failed and fails the workflow.
func (e *Engine) failNode(ctx context.Context, cur model.WorkflowInstance, attempt *model.NodeInstance, ctxMap map[string]any, cause error) error {
	now := e.now()
	attempt.Status = model.NodeFailed
	attempt.Error = cause.Error()
	attempt.ContextAfter = marshal(ctxMap)
	attempt.FinishedAt = &now
	attempt.UpdatedAt = now
	if err := e.instances.UpdateNodeInstance(ctx, *attempt); err != nil {
		return err
	}
	_ = e.appendEvent(ctx, cur.ID, "node_failed", map[string]any{"node_id": attempt.NodeID, "occurrence_id": attempt.ID, "error": cause.Error()})
	return e.fail(ctx, cur, "", cause)
}

// interrupted cleans up an attempt whose executor was cancelled. When a stop
// committed, the attempt becomes stopped and the termination flag is
// resolved; when the interruption came from somewhere else (e.g. dispatcher
// shutdown), the attempt is left running so another worker recovers it.
func (e *Engine) interrupted(ctx context.Context, cur model.WorkflowInstance, attempt *model.NodeInstance) error {
	bg := context.WithoutCancel(ctx)
	got, err := e.instances.GetByID(bg, cur.ID)
	if err != nil || got.Status != model.WorkflowStopped {
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
	_ = e.appendEvent(bg, cur.ID, "node_stopped", map[string]any{"node_id": attempt.NodeID, "occurrence_id": attempt.ID, "error": attempt.Error})
	_ = e.appendEvent(bg, cur.ID, "cancellation", map[string]any{"node_id": attempt.NodeID, "occurrence_id": attempt.ID, "reason": "stop"})
	return e.instances.ResolveTermination(bg, cur.ID)
}

// fail commits a failed terminal state with the cause.
func (e *Engine) fail(ctx context.Context, cur model.WorkflowInstance, _ string, cause error) error {
	ctxMap, err := unmarshalContext(cur.Context)
	if err != nil {
		ctxMap = map[string]any{}
	}
	return e.failWithContext(ctx, cur, cause, ctxMap)
}

// failWithContext commits a failed terminal state with the cause, persisting
// the given context instead of the last checkpoint. Structural hook failures
// (group pre/post) use it so the latest completed context survives.
func (e *Engine) failWithContext(ctx context.Context, cur model.WorkflowInstance, cause error, ctxMap map[string]any) error {
	now := e.now()
	_ = e.appendEvent(ctx, cur.ID, "workflow_failed", map[string]any{"error": cause.Error()})
	frame, ferr := model.ParseFrame(cur.Frame)
	if ferr != nil {
		frame = model.Frame{}
	}
	counters, cerr := model.ParseCounters(cur.Counters)
	if cerr != nil {
		counters = model.Counters{}
	}
	return e.checkpoint(ctx, cur, &frame, counters, ctxMap, model.WorkflowFailed, "", cause.Error(), &now)
}

// checkpoint commits the transition under the worker's lease and revision.
func (e *Engine) checkpoint(ctx context.Context, cur model.WorkflowInstance, frame *model.Frame, counters model.Counters, ctxMap map[string]any, status model.WorkflowStatus, reason model.WaitingReason, errMsg string, finished *time.Time) error {
	cp := repository.Checkpoint{
		InstanceID:           cur.ID,
		WorkerID:             cur.LeasedBy,
		Revision:             cur.Revision,
		WorkflowDefinitionID: cur.WorkflowDefinitionID,
		FromStatus:           cur.Status,
		FromWaitingReason:    cur.WaitingReason,
		Status:               status,
		WaitingReason:        reason,
		Frame:                *frame,
		Counters:             counters,
		Context:              marshal(ctxMap),
		Error:                errMsg,
		FinishedAt:           finished,
	}
	err := e.instances.Checkpoint(ctx, cp)
	if errors.Is(err, repository.ErrLeaseLost) {
		// Stop won the race; the worker is fenced and aborts silently.
		return nil
	}
	return err
}

// inputCheckpoint parks the cursor waiting (or paused) on an input node.
func (e *Engine) inputCheckpoint(ctx context.Context, cur model.WorkflowInstance, frame *model.Frame, counters model.Counters, ctxMap map[string]any) error {
	status := model.WorkflowWaiting
	if cur.PauseRequested {
		status = model.WorkflowPaused
	}
	return e.checkpoint(ctx, cur, frame, counters, ctxMap, status, model.WaitingReasonInput, "", nil)
}

// nextStatus decides the post-checkpoint status, honoring a deferred pause.
func (e *Engine) nextStatus(cur model.WorkflowInstance) model.WorkflowStatus {
	if cur.PauseRequested {
		return model.WorkflowPaused
	}
	return model.WorkflowWaiting
}

func (e *Engine) appendEvent(ctx context.Context, instanceID, typ string, data map[string]any) error {
	raw, _ := json.Marshal(data)
	return e.instances.AppendEvent(ctx, model.WorkflowInstanceEvent{
		ID:                 newID(),
		WorkflowInstanceID: instanceID,
		Type:               typ,
		Data:               raw,
		CreatedBy:          e.actor,
		CreatedAt:          e.now(),
	})
}

func newAttempt(instanceID string, nc *model.NodeContent, now time.Time) model.NodeInstance {
	policy := ""
	if nc.RetryOnRecovery {
		policy = "retry"
	}
	return model.NodeInstance{
		ID:                 newID(),
		WorkflowInstanceID: instanceID,
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

func marshal(v any) json.RawMessage {
	b, err := json.Marshal(v)
	if err != nil {
		return json.RawMessage("null")
	}
	return b
}

func unmarshalContext(raw json.RawMessage) (map[string]any, error) {
	m := map[string]any{}
	if len(raw) == 0 {
		return m, nil
	}
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil, fmt.Errorf("engine: parse instance context: %w", err)
	}
	return m, nil
}
