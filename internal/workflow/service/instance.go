// Package service implements the use-case orchestration for immutable
// definitions, workflow instances, input delivery, and runtime controls.
package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/simpwf/workflow-engine/internal/workflow/engine"
	"github.com/simpwf/workflow-engine/internal/workflow/executor"
	"github.com/simpwf/workflow-engine/internal/workflow/model"
	"github.com/simpwf/workflow-engine/internal/workflow/repository"
	"github.com/simpwf/workflow-engine/pkg/contextpath"
)

// CreateInstance starts a workflow instance.
type CreateInstance struct {
	WorkflowDefinitionID string
	Context              json.RawMessage
}

// UpdateContext replaces the full context of a paused instance. Reason is an
// optional audit annotation recorded on the context_updated event.
type UpdateContext struct {
	InstanceID string
	Context    json.RawMessage
	Reason     string
}

// DeliverInput delivers a payload to a waiting input node. Source is the
// transport the payload arrived on ("http", "redis", or "rabbitmq"); an
// empty Source defaults to "http".
type DeliverInput struct {
	InstanceID     string
	IdempotencyKey string
	Payload        []byte
	Source         string
}

// StatusDetail is the status view with the current node occurrence resolved.
type StatusDetail struct {
	Instance              model.WorkflowInstance
	CurrentNodeInstanceID *string
	Attempt               int
	// Nodes maps every workflow graph node id (groups included) to its
	// occurrence state. Nil when the definition cannot be loaded, so the
	// status response omits the map instead of failing.
	Nodes map[string]NodeOccurrence
}

// NodeOccurrence is the per-node status view: the already-executed
// occurrence id (nil when never ran) plus whether it is a valid rollback
// target. Rollbackable is advisory and instance-aware (false unless the
// instance itself is paused or failed); the rollback endpoint stays the
// source of truth.
type NodeOccurrence struct {
	OccurrenceID *string
	Status       string
	Attempt      *int
	Rollbackable bool
}

// NodeDebugDetail is the resolved node-debug view. Occurrences that never ran
// carry status "not_started", nil attempts, and empty snapshots.
type NodeDebugDetail struct {
	OccurrenceID           string
	SourceNodeDefinitionID string
	Name                   string
	Type                   string
	SelectedAttempt        *int
	LatestAttempt          *int
	AttemptCount           int
	Status                 string
	ContextBefore          json.RawMessage
	ContextAfter           json.RawMessage
	Input                  json.RawMessage
	Output                 json.RawMessage
	Error                  *string
	RecoveryPolicy         *string
	RecoveryResult         *string
	Cancelled              bool
	StartedAt              *time.Time
	FinishedAt             *time.Time
	StoppedAt              *time.Time
	DurationMS             *int64
	CreatedAt              time.Time
	UpdatedAt              time.Time
}

// ControlResult reports the post-transition state of a pause/resume/stop
// control call.
type ControlResult struct {
	Status             model.WorkflowStatus
	PauseRequested     bool
	TerminationPending bool
}

// RollbackRequest moves a paused or failed instance's cursor back to an
// already-executed node occurrence. Reason is an optional audit annotation
// recorded on the rollback event only.
type RollbackRequest struct {
	InstanceID         string
	TargetOccurrenceID string
	Reason             string
}

// RollbackResult reports the post-rollback cursor: the instance is always
// paused, parked on CurrentNodeID with GroupStack recomputed from the
// materialized definition.
type RollbackResult struct {
	Status        model.WorkflowStatus
	CurrentNodeID string
	GroupStack    []string
}

// Canceller interrupts the local in-flight transition of an instance. The
// engine implements it via its cancellation registry; a nil Canceller
// disables local cancellation (cross-replica propagation still works
// through the dispatcher heartbeat).
type Canceller interface {
	Cancel(instanceID string)
}

// WorkflowMaterializer resolves node_definition_id references into the
// executable node tree. DeliverInput must use the same materialized graph
// as the engine so input nodes supplied through reusable node definitions
// are recognized.
type WorkflowMaterializer interface {
	Materialize(ctx context.Context, wc *model.WorkflowContent) (*model.WorkflowContent, error)
}

// InstanceService is the use-case boundary for workflow instances.
type InstanceService interface {
	Create(ctx context.Context, req CreateInstance) (model.WorkflowInstance, error)
	GetStatus(ctx context.Context, id string) (*model.WorkflowInstance, error)
	// GetStatusDetail resolves the current node occurrence for the status
	// response.
	GetStatusDetail(ctx context.Context, id string) (*StatusDetail, error)
	// List returns the instances matching the query with pagination and
	// ordering, mirroring the repository query.
	List(ctx context.Context, q repository.InstanceListQuery) ([]model.WorkflowInstance, int64, error)
	GetContext(ctx context.Context, id string) (*model.WorkflowInstance, error)
	// UpdateContext replaces the full context of a paused instance. The
	// body must be a JSON object; a non-paused instance conflicts.
	UpdateContext(ctx context.Context, req UpdateContext) (*model.WorkflowInstance, error)
	// DeliverInput validates, records, and resumes an input delivery. The
	// Source (transport) must match the channel of the input node the
	// instance is parked on. A rejected payload yields a delivery with
	// Accepted=false and the validation message in Error. Replays of an
	// idempotency key return the originally recorded delivery.
	DeliverInput(ctx context.Context, req DeliverInput) (*model.InputDelivery, error)
	// NodeDebug resolves the debug detail for one node occurrence of an
	// instance. nodeID is either the workflow graph node id or the
	// occurrence id; attempt <= 0 selects the latest attempt, a positive
	// attempt selects an exact loop execution.
	NodeDebug(ctx context.Context, instanceID, nodeID string, attempt int) (*NodeDebugDetail, error)
	// Pause pauses a waiting instance immediately and requests a deferred
	// pause for a running instance. Idempotent while paused.
	Pause(ctx context.Context, id string) (*ControlResult, error)
	// Resume returns a paused instance to waiting and clears a pending
	// pause on a running instance. Idempotent on active instances.
	Resume(ctx context.Context, id string) (*ControlResult, error)
	// Stop moves an instance to the terminal stopped state, fences worker
	// commits, and signals local cancellation. Idempotent on stopped
	// instances.
	Stop(ctx context.Context, id, reason string) (*ControlResult, error)
	// Rollback moves a paused or failed instance's cursor back to an
	// already-executed node occurrence so the next resume re-executes
	// forward from there. The instance is always paused afterwards and its
	// context is restored from the target occurrence's ContextBefore.
	// History is immutable; the next execution increments the target
	// occurrence's attempt.
	Rollback(ctx context.Context, req RollbackRequest) (*RollbackResult, error)
}

type instanceService struct {
	instances    repository.InstanceRepository
	wfDefs       repository.WorkflowDefinitionRepository
	materializer WorkflowMaterializer
	validator    *executor.InputExecutor
	hooks        *executor.HookRunner
	actor        string
	limits       model.NodeLimits
	cancels      Canceller
}

// NewInstanceService builds the instance service. cancels may be nil; when
// set it receives the local cancellation signal for stopped instances.
func NewInstanceService(
	instances repository.InstanceRepository,
	wfDefs repository.WorkflowDefinitionRepository,
	materializer WorkflowMaterializer,
	validator *executor.InputExecutor,
	hooks *executor.HookRunner,
	actor string,
	limits model.NodeLimits,
	cancels Canceller,
) InstanceService {
	return &instanceService{instances: instances, wfDefs: wfDefs, materializer: materializer, validator: validator, hooks: hooks, actor: actor, limits: limits, cancels: cancels}
}

func (s *instanceService) Create(ctx context.Context, req CreateInstance) (model.WorkflowInstance, error) {
	if strings.TrimSpace(req.WorkflowDefinitionID) == "" {
		return model.WorkflowInstance{}, fmt.Errorf("%w: workflow_definition_id is required", model.ErrInvalid)
	}
	def, err := s.wfDefs.GetByID(ctx, req.WorkflowDefinitionID)
	if err != nil {
		return model.WorkflowInstance{}, err
	}
	wc, err := model.ParseWorkflowContent(def.Content, s.limits)
	if err != nil {
		return model.WorkflowInstance{}, fmt.Errorf("%w: %v", model.ErrInvalid, err)
	}
	contextRaw := req.Context
	if len(contextRaw) == 0 || string(contextRaw) == "null" {
		contextRaw = json.RawMessage("{}")
	}
	var obj map[string]any
	if err := json.Unmarshal(contextRaw, &obj); err != nil {
		return model.WorkflowInstance{}, fmt.Errorf("%w: context must be a JSON object", model.ErrInvalid)
	}

	frameRaw, err := model.NewFrame(wc.StartNodeID).JSON()
	if err != nil {
		return model.WorkflowInstance{}, err
	}
	now := nowUTC()
	w := model.WorkflowInstance{
		ID:                   mustNewID(),
		WorkflowDefinitionID: def.ID,
		Status:               model.WorkflowWaiting,
		WaitingReason:        model.WaitingReasonRunnable,
		Frame:                frameRaw,
		Context:              contextRaw,
		Counters:             mustMarshal(model.Counters{}),
		CreatedBy:            s.actor,
		UpdatedBy:            s.actor,
		CreatedAt:            now,
		UpdatedAt:            now,
	}
	if err := s.instances.Insert(ctx, w); err != nil {
		return model.WorkflowInstance{}, err
	}
	_ = s.instances.AppendEvent(ctx, model.WorkflowInstanceEvent{
		ID: mustNewID(), WorkflowInstanceID: w.ID, Type: "instance_created",
		Data:      json.RawMessage(`{"workflow_definition_id":"` + def.ID + `"}`),
		CreatedBy: s.actor, CreatedAt: now,
	})
	return w, nil
}

func (s *instanceService) GetStatus(ctx context.Context, id string) (*model.WorkflowInstance, error) {
	return s.instances.GetByID(ctx, id)
}

func (s *instanceService) List(ctx context.Context, q repository.InstanceListQuery) ([]model.WorkflowInstance, int64, error) {
	return s.instances.List(ctx, q)
}

func (s *instanceService) GetStatusDetail(ctx context.Context, id string) (*StatusDetail, error) {
	inst, err := s.instances.GetByID(ctx, id)
	if err != nil {
		return nil, err
	}
	d := &StatusDetail{Instance: *inst}
	d.Nodes = s.statusNodes(ctx, inst)
	frame, err := model.ParseFrame(inst.Frame)
	if err != nil {
		return d, nil
	}
	if frame.CurrentNodeID == "" {
		return d, nil
	}
	attempt, err := s.instances.GetNodeInstanceByNode(ctx, inst.ID, frame.CurrentNodeID)
	if err != nil {
		return d, nil
	}
	nodeInstanceID := attempt.ID
	d.CurrentNodeInstanceID = &nodeInstanceID
	d.Attempt = attempt.Attempt
	return d, nil
}

func (s *instanceService) GetContext(ctx context.Context, id string) (*model.WorkflowInstance, error) {
	return s.instances.GetByID(ctx, id)
}

// statusNodes builds the graph-node-id → occurrence map for the status
// response. It returns nil when the definition cannot be loaded, parsed, or
// materialized, so the status view degrades to omitting the map instead of
// failing the whole call.
func (s *instanceService) statusNodes(ctx context.Context, inst *model.WorkflowInstance) map[string]NodeOccurrence {
	wf, err := s.wfDefs.GetByID(ctx, inst.WorkflowDefinitionID)
	if err != nil {
		return nil
	}
	wc, err := model.ParseWorkflowContent(wf.Content, s.limits)
	if err != nil {
		return nil
	}
	wc, err = s.materializer.Materialize(ctx, wc)
	if err != nil {
		return nil
	}
	ids := flattenNodeIDs(wc.Nodes)
	if len(ids) == 0 {
		return nil
	}
	occs, err := s.instances.ListNodeInstances(ctx, inst.ID)
	if err != nil {
		return nil
	}
	byNode := make(map[string]*model.NodeInstance, len(occs))
	for i := range occs {
		byNode[occs[i].NodeID] = &occs[i]
	}
	instanceGate := inst.Status == model.WorkflowPaused || inst.Status == model.WorkflowFailed
	instanceGate = instanceGate && !inst.TerminationPending
	out := make(map[string]NodeOccurrence, len(ids))
	for _, id := range ids {
		nc, err := findNode(wc.Nodes, id)
		if err != nil {
			continue
		}
		occ, ok := byNode[id]
		if !ok {
			out[id] = NodeOccurrence{Status: "not_started"}
			continue
		}
		e := NodeOccurrence{Status: string(occ.Status)}
		occID := occ.ID
		e.OccurrenceID = &occID
		attempt := occ.Attempt
		e.Attempt = &attempt
		e.Rollbackable = instanceGate && rollbackableOccurrence(nc.Type, occ)
		out[id] = e
	}
	return out
}

// flattenNodeIDs lists every graph node id in the materialized tree,
// including group nodes themselves and their nested children.
func flattenNodeIDs(nodes []*model.NodeContent) []string {
	var out []string
	for _, n := range nodes {
		if n == nil {
			continue
		}
		out = append(out, n.ID)
		if n.Group != nil {
			out = append(out, flattenNodeIDs(n.Group.Nodes)...)
		}
	}
	return out
}

// rollbackableOccurrence mirrors the rollback endpoint's target validation:
// group nodes never qualify (they have no occurrence), only terminal
// occurrence states qualify, and the ContextBefore snapshot must parse as
// a JSON object. The caller gates on instance status.
func rollbackableOccurrence(typ model.NodeType, occ *model.NodeInstance) bool {
	if typ == model.NodeTypeGroup {
		return false
	}
	switch occ.Status {
	case model.NodeFinished, model.NodeFailed, model.NodeStopped:
	default:
		return false
	}
	return restorableContext(occ.ContextBefore)
}

// restorableContext reports whether raw is a JSON object (the rollback
// endpoint restores target.ContextBefore as the instance context).
func restorableContext(raw json.RawMessage) bool {
	if len(raw) == 0 {
		return false
	}
	var obj map[string]any
	if err := json.Unmarshal(raw, &obj); err != nil || obj == nil {
		return false
	}
	return true
}

func (s *instanceService) UpdateContext(ctx context.Context, req UpdateContext) (*model.WorkflowInstance, error) {
	if strings.TrimSpace(req.InstanceID) == "" {
		return nil, fmt.Errorf("%w: instance id is required", model.ErrInvalid)
	}
	if len(req.Context) == 0 {
		return nil, fmt.Errorf("%w: context must be a JSON object", model.ErrInvalid)
	}
	var obj map[string]any
	if err := json.Unmarshal(req.Context, &obj); err != nil || obj == nil {
		return nil, fmt.Errorf("%w: context must be a JSON object", model.ErrInvalid)
	}
	inst, err := s.instances.ReplaceContext(ctx, repository.ContextUpdate{
		InstanceID: req.InstanceID,
		Context:    req.Context,
		Actor:      s.actor,
		Reason:     req.Reason,
	})
	if err != nil {
		if errors.Is(err, repository.ErrInstanceNotFound) {
			return nil, fmt.Errorf("%w: instance %s", model.ErrNotFound, req.InstanceID)
		}
		if errors.Is(err, repository.ErrStatusConflict) {
			return nil, fmt.Errorf("%w: instance %s is not paused", model.ErrConflict, req.InstanceID)
		}
		return nil, err
	}
	return inst, nil
}

func (s *instanceService) NodeDebug(ctx context.Context, instanceID, nodeID string, attempt int) (*NodeDebugDetail, error) {
	inst, err := s.instances.GetByID(ctx, instanceID)
	if err != nil {
		if errors.Is(err, repository.ErrInstanceNotFound) {
			return nil, fmt.Errorf("%w: instance %s", model.ErrNotFound, instanceID)
		}
		return nil, err
	}
	wf, err := s.wfDefs.GetByID(ctx, inst.WorkflowDefinitionID)
	if err != nil {
		return nil, err
	}
	wc, err := model.ParseWorkflowContent(wf.Content, s.limits)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", model.ErrInvalid, err)
	}
	graph := &contentGraph{wc: wc}

	// nodeID is the graph node id; an occurrence id is resolved to its graph
	// node as a fallback.
	var nodeInst *model.NodeInstance
	nc, err := graph.Node(nodeID)
	if err != nil {
		occ, gerr := s.instances.GetNodeInstance(ctx, instanceID, nodeID)
		if gerr != nil {
			return nil, fmt.Errorf("%w: node %q does not exist in instance %s", model.ErrNotFound, nodeID, instanceID)
		}
		nc, err = graph.Node(occ.NodeID)
		if err != nil {
			return nil, fmt.Errorf("%w: node %q does not exist in instance %s", model.ErrNotFound, nodeID, instanceID)
		}
		nodeInst = occ
	} else {
		occ, gerr := s.instances.GetNodeInstanceByNode(ctx, instanceID, nc.ID)
		if gerr != nil && !errors.Is(gerr, repository.ErrNodeInstanceNotFound) {
			return nil, gerr
		}
		nodeInst = occ
	}

	d := &NodeDebugDetail{
		SourceNodeDefinitionID: nc.NodeDefinitionID,
		Name:                   nc.Name,
		Type:                   string(nc.Type),
		Status:                 "not_started",
	}
	if nodeInst == nil {
		d.OccurrenceID = nc.ID
		return d, nil
	}

	selected := attempt
	if selected <= 0 {
		selected = nodeInst.Attempt
	}
	if attempt > nodeInst.Attempt {
		return nil, fmt.Errorf("%w: attempt %d of node %q never ran (latest %d)", model.ErrNotFound, attempt, nodeID, nodeInst.Attempt)
	}
	latest := nodeInst.Attempt
	d.OccurrenceID = nodeInst.ID
	d.SelectedAttempt = &selected
	d.LatestAttempt = &latest
	d.AttemptCount = nodeInst.Attempt
	d.Status = string(nodeInst.Status)
	d.ContextBefore = nodeInst.ContextBefore
	d.ContextAfter = nodeInst.ContextAfter
	d.Input = nodeInst.Input
	d.Output = nodeInst.Output
	d.Error = nullableString(nodeInst.Error)
	d.RecoveryPolicy = nullableString(nodeInst.RecoveryPolicy)
	d.RecoveryResult = nullableString(nodeInst.RecoveryResult)
	d.Cancelled = nodeInst.Cancelled
	d.StartedAt = nodeInst.StartedAt
	d.FinishedAt = nodeInst.FinishedAt
	d.StoppedAt = nodeInst.StoppedAt
	if nodeInst.StartedAt != nil && nodeInst.FinishedAt != nil {
		ms := nodeInst.FinishedAt.Sub(*nodeInst.StartedAt).Milliseconds()
		d.DurationMS = &ms
	}
	d.CreatedAt = nodeInst.CreatedAt
	d.UpdatedAt = nodeInst.UpdatedAt
	return d, nil
}

func nullableString(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

func (s *instanceService) Pause(ctx context.Context, id string) (*ControlResult, error) {
	inst, err := s.instance(ctx, id)
	if err != nil {
		return nil, err
	}
	switch inst.Status {
	case model.WorkflowPaused:
		return &ControlResult{Status: model.WorkflowPaused}, nil
	case model.WorkflowFinished, model.WorkflowFailed, model.WorkflowStopped:
		return nil, fmt.Errorf("%w: instance %s is terminal", model.ErrConflict, id)
	}
	deferred, err := s.instances.Pause(ctx, id)
	if err != nil {
		if errors.Is(err, repository.ErrStatusConflict) {
			return nil, fmt.Errorf("%w: instance %s is terminal", model.ErrConflict, id)
		}
		return nil, err
	}
	if deferred {
		_ = s.instances.AppendEvent(ctx, model.WorkflowInstanceEvent{
			ID: mustNewID(), WorkflowInstanceID: id, Type: "pause_requested",
			Data: json.RawMessage(`{}`), CreatedBy: s.actor, CreatedAt: nowUTC(),
		})
		return &ControlResult{Status: model.WorkflowRunning, PauseRequested: true}, nil
	}
	_ = s.instances.AppendEvent(ctx, model.WorkflowInstanceEvent{
		ID: mustNewID(), WorkflowInstanceID: id, Type: "paused",
		Data: json.RawMessage(`{}`), CreatedBy: s.actor, CreatedAt: nowUTC(),
	})
	return &ControlResult{Status: model.WorkflowPaused}, nil
}

func (s *instanceService) Resume(ctx context.Context, id string) (*ControlResult, error) {
	inst, err := s.instance(ctx, id)
	if err != nil {
		return nil, err
	}
	switch inst.Status {
	case model.WorkflowWaiting:
		return &ControlResult{Status: model.WorkflowWaiting}, nil
	case model.WorkflowFinished, model.WorkflowFailed, model.WorkflowStopped:
		return nil, fmt.Errorf("%w: instance %s is terminal", model.ErrConflict, id)
	}
	if err := s.instances.Resume(ctx, id); err != nil {
		if errors.Is(err, repository.ErrStatusConflict) {
			return nil, fmt.Errorf("%w: instance %s is terminal", model.ErrConflict, id)
		}
		return nil, err
	}
	eventType := "resumed"
	if inst.Status == model.WorkflowRunning {
		eventType = "resume"
	}
	_ = s.instances.AppendEvent(ctx, model.WorkflowInstanceEvent{
		ID: mustNewID(), WorkflowInstanceID: id, Type: eventType,
		Data: json.RawMessage(`{}`), CreatedBy: s.actor, CreatedAt: nowUTC(),
	})
	return &ControlResult{Status: model.WorkflowWaiting}, nil
}

func (s *instanceService) Stop(ctx context.Context, id, reason string) (*ControlResult, error) {
	inst, err := s.instance(ctx, id)
	if err != nil {
		return nil, err
	}
	switch inst.Status {
	case model.WorkflowStopped:
		return &ControlResult{Status: model.WorkflowStopped, TerminationPending: inst.TerminationPending}, nil
	case model.WorkflowFinished, model.WorkflowFailed:
		return nil, fmt.Errorf("%w: instance %s is terminal", model.ErrConflict, id)
	}
	pending, err := s.instances.Stop(ctx, id, reason)
	if err != nil {
		if errors.Is(err, repository.ErrStatusConflict) {
			return nil, fmt.Errorf("%w: instance %s is terminal", model.ErrConflict, id)
		}
		return nil, err
	}
	_ = s.instances.AppendEvent(ctx, model.WorkflowInstanceEvent{
		ID: mustNewID(), WorkflowInstanceID: id, Type: "stop",
		Data: json.RawMessage(`{"reason":"` + reason + `"}`), CreatedBy: s.actor, CreatedAt: nowUTC(),
	})
	// A parked input attempt has no worker to interrupt; stop it here.
	if !pending {
		_ = s.instances.StopRunningAttempts(ctx, id)
	}
	if pending && s.cancels != nil {
		s.cancels.Cancel(id)
	}
	return &ControlResult{Status: model.WorkflowStopped, TerminationPending: pending}, nil
}

// instance loads an instance for the control endpoints, mapping a missing
// row to the domain not-found error.
func (s *instanceService) instance(ctx context.Context, id string) (*model.WorkflowInstance, error) {
	inst, err := s.instances.GetByID(ctx, id)
	if err != nil {
		if errors.Is(err, repository.ErrInstanceNotFound) {
			return nil, fmt.Errorf("%w: instance %s", model.ErrNotFound, id)
		}
		return nil, err
	}
	return inst, nil
}

func (s *instanceService) Rollback(ctx context.Context, req RollbackRequest) (*RollbackResult, error) {
	if strings.TrimSpace(req.InstanceID) == "" {
		return nil, fmt.Errorf("%w: instance id is required", model.ErrInvalid)
	}
	if strings.TrimSpace(req.TargetOccurrenceID) == "" {
		return nil, fmt.Errorf("%w: target_occurrence_id is required", model.ErrInvalid)
	}
	inst, err := s.instance(ctx, req.InstanceID)
	if err != nil {
		return nil, err
	}
	switch inst.Status {
	case model.WorkflowPaused, model.WorkflowFailed:
	default:
		return nil, fmt.Errorf("%w: instance %s is not paused or failed", model.ErrConflict, req.InstanceID)
	}
	if inst.TerminationPending {
		return nil, fmt.Errorf("%w: instance %s has termination pending", model.ErrConflict, req.InstanceID)
	}

	// The target is an already-executed node occurrence: its row carries
	// the graph node id and the ContextBefore snapshot to restore. Nodes
	// that never ran (NodeDebug "not_started") have no occurrence.
	occ, err := s.instances.GetNodeInstance(ctx, inst.ID, req.TargetOccurrenceID)
	if err != nil {
		if errors.Is(err, repository.ErrNodeInstanceNotFound) {
			return nil, fmt.Errorf("%w: occurrence %q of instance %s", model.ErrNotFound, req.TargetOccurrenceID, req.InstanceID)
		}
		return nil, err
	}
	if occ.WorkflowInstanceID != inst.ID {
		return nil, fmt.Errorf("%w: occurrence %q of instance %s", model.ErrNotFound, req.TargetOccurrenceID, req.InstanceID)
	}

	wf, err := s.wfDefs.GetByID(ctx, inst.WorkflowDefinitionID)
	if err != nil {
		return nil, err
	}
	wc, err := model.ParseWorkflowContent(wf.Content, s.limits)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", model.ErrInvalid, err)
	}
	wc, err = s.materializer.Materialize(ctx, wc)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", model.ErrInvalid, err)
	}
	target, err := findNode(wc.Nodes, occ.NodeID)
	if err != nil {
		return nil, fmt.Errorf("%w: occurrence node %q is not in the workflow definition", model.ErrInvalid, occ.NodeID)
	}
	if target.Type == model.NodeTypeGroup {
		return nil, fmt.Errorf("%w: group nodes cannot be rollback targets", model.ErrInvalid)
	}
	running, rerr := s.instances.GetRunningNodeInstance(ctx, inst.ID)
	if rerr != nil && !errors.Is(rerr, repository.ErrNodeInstanceNotFound) {
		return nil, rerr
	}
	if rerr == nil && (target.Type != model.NodeTypeInput || running.NodeID != target.ID) {
		return nil, fmt.Errorf("%w: instance %s has running attempt on node %q; resume and deliver input instead", model.ErrConflict, req.InstanceID, running.NodeID)
	}
	if target.Type == model.NodeTypeInput {
		// Rolling back to an input node re-arms its wait as a fresh
		// attempt of the same finished occurrence (delivery history stays
		// attached for audit) so the next resume re-parks it and a fresh
		// delivery with a new idempotency key is accepted. Parking on the
		// node still collides with a live parked attempt, which must be
		// resumed + delivered instead.
		if rerr == nil {
			return nil, fmt.Errorf("%w: instance %s is parked on input node %q; resume and deliver input instead", model.ErrConflict, req.InstanceID, target.ID)
		}
		if occ.Status != model.NodeFinished {
			return nil, fmt.Errorf("%w: input occurrence %q is not finished", model.ErrConflict, req.TargetOccurrenceID)
		}
	}
	stack, err := groupStack(wc, occ.NodeID)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", model.ErrInvalid, err)
	}
	var restore map[string]any
	if err := json.Unmarshal(occ.ContextBefore, &restore); err != nil || restore == nil {
		return nil, fmt.Errorf("%w: occurrence %q has no restorable context", model.ErrInvalid, req.TargetOccurrenceID)
	}

	frame, err := model.ParseFrame(inst.Frame)
	if err != nil {
		return nil, err
	}
	// The park reason follows the target: input targets stay parked for
	// their delivery, everything else becomes runnable. Rolling back to a
	// finished input occurrence re-arms it as a fresh attempt of the same
	// row (delivery history stays attached for audit), so a fresh
	// Idempotency-Key is accepted on resume.
	waitingReason := model.WaitingReasonRunnable
	rearm := false
	if target.Type == model.NodeTypeInput {
		waitingReason = model.WaitingReasonInput
		rearm = true
	}
	rolled, err := s.instances.RollbackInstance(ctx, repository.RollbackUpdate{
		InstanceID:           inst.ID,
		Frame:                model.Frame{CurrentNodeID: target.ID, GroupStack: stack},
		Context:              occ.ContextBefore,
		Actor:                s.actor,
		Reason:               req.Reason,
		FromNode:             frame.CurrentNodeID,
		ToNode:               target.ID,
		ToOccurrence:         occ.ID,
		WaitingReason:        waitingReason,
		RearmInputOccurrence: rearm,
	})
	if err != nil {
		if errors.Is(err, repository.ErrInstanceNotFound) {
			return nil, fmt.Errorf("%w: instance %s", model.ErrNotFound, req.InstanceID)
		}
		if errors.Is(err, repository.ErrStatusConflict) {
			return nil, fmt.Errorf("%w: instance %s is not paused or failed", model.ErrConflict, req.InstanceID)
		}
		return nil, err
	}
	newFrame, err := model.ParseFrame(rolled.Frame)
	if err != nil {
		return nil, err
	}
	return &RollbackResult{Status: rolled.Status, CurrentNodeID: newFrame.CurrentNodeID, GroupStack: newFrame.GroupStack}, nil
}

// findNode resolves a graph node id anywhere in the materialized tree.
func findNode(nodes []*model.NodeContent, id string) (*model.NodeContent, error) {
	for _, n := range nodes {
		if n.ID == id {
			return n, nil
		}
		if n.Group != nil {
			if found, err := findNode(n.Group.Nodes, id); err == nil {
				return found, nil
			}
		}
	}
	return nil, fmt.Errorf("service: unknown node %q in workflow graph", id)
}

// groupStack recomputes the ancestor group chain of a graph node by walking
// the materialized definition from its start node. The stack holds the
// enclosing groups outermost-first; a top-level node yields an empty stack.
func groupStack(wc *model.WorkflowContent, id string) ([]string, error) {
	if id == wc.StartNodeID {
		if _, err := findNode(wc.Nodes, id); err != nil {
			return nil, err
		}
		return nil, nil
	}
	var walk func(nodes []*model.NodeContent, ancestors []string) ([]string, error)
	walk = func(nodes []*model.NodeContent, ancestors []string) ([]string, error) {
		for _, n := range nodes {
			if n.ID == id {
				return ancestors, nil
			}
			if n.Group != nil {
				if stack, err := walk(n.Group.Nodes, append(ancestors, n.ID)); err == nil {
					return stack, nil
				}
			}
		}
		return nil, fmt.Errorf("service: unknown node %q in workflow graph", id)
	}
	return walk(wc.Nodes, nil)
}

func (s *instanceService) DeliverInput(ctx context.Context, req DeliverInput) (*model.InputDelivery, error) {
	if strings.TrimSpace(req.IdempotencyKey) == "" {
		return nil, fmt.Errorf("%w: Idempotency-Key header is required", model.ErrInvalid)
	}
	if len(req.Payload) == 0 {
		return nil, fmt.Errorf("%w: input body is required", model.ErrInvalid)
	}
	inst, err := s.instances.GetByID(ctx, req.InstanceID)
	if err != nil {
		return nil, err
	}

	// Idempotent replay: an already recorded delivery for this key wins
	// regardless of the current instance state.
	if existing, err := s.instances.GetDeliveryByKey(ctx, inst.ID, req.IdempotencyKey); err == nil {
		return existing, nil
	} else if !errors.Is(err, repository.ErrDeliveryNotFound) {
		return nil, err
	}

	switch inst.Status {
	case model.WorkflowFinished, model.WorkflowFailed, model.WorkflowStopped:
		return nil, fmt.Errorf("%w: instance %s is terminal", model.ErrConflict, req.InstanceID)
	}

	frame, err := model.ParseFrame(inst.Frame)
	if err != nil {
		return nil, err
	}
	wf, err := s.wfDefs.GetByID(ctx, inst.WorkflowDefinitionID)
	if err != nil {
		return nil, err
	}
	wc, err := model.ParseWorkflowContent(wf.Content, s.limits)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", model.ErrInvalid, err)
	}
	wc, err = s.materializer.Materialize(ctx, wc)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", model.ErrInvalid, err)
	}
	graph := &contentGraph{wc: wc}
	inputNode, err := graph.Node(frame.CurrentNodeID)
	if err != nil {
		return nil, err
	}
	if inputNode.Type != model.NodeTypeInput {
		return nil, fmt.Errorf("%w: current node is not an input node", model.ErrConflict)
	}
	source := req.Source
	if source == "" {
		source = model.InputChannelHTTP
	}
	if inputNode.Channel != source {
		return nil, fmt.Errorf("%w: input source %q does not match input node channel %q", model.ErrConflict, source, inputNode.Channel)
	}
	attempt, err := s.instances.GetNodeInstanceByNode(ctx, inst.ID, inputNode.ID)
	if err != nil {
		return nil, err
	}

	if inst.Status != model.WorkflowWaiting || inst.WaitingReason != model.WaitingReasonInput {
		return nil, fmt.Errorf("%w: instance %s is not waiting for input", model.ErrConflict, req.InstanceID)
	}
	if attempt.Status != model.NodeRunning {
		return nil, fmt.Errorf("%w: input node is not waiting for input", model.ErrConflict)
	}

	ctxMap, err := unmarshalJSON(inst.Context)
	if err != nil {
		return nil, err
	}
	vr, err := s.validator.Validate(ctx, executor.Request{Node: inputNode, Context: ctxMap, Payload: req.Payload})
	if err != nil {
		return nil, fmt.Errorf("%w: input validation failed: %v", model.ErrInvalid, err)
	}
	if !vr.Valid {
		return s.instances.DeliverInput(ctx, repository.InputCompletion{
			InstanceID:     inst.ID,
			NodeInstanceID: attempt.ID,
			IdempotencyKey: req.IdempotencyKey,
			Payload:        req.Payload,
			Accepted:       false,
			Error:          vr.Message,
			CreatedBy:      s.actor,
		})
	}

	var payload any
	if err := json.Unmarshal(req.Payload, &payload); err != nil {
		return nil, fmt.Errorf("%w: input body must be valid JSON: %v", model.ErrInvalid, err)
	}
	newCtx, err := cloneContext(ctxMap)
	if err != nil {
		return nil, err
	}
	if err := contextpath.Set(newCtx, inputNode.ContextPath, payload); err != nil {
		return nil, fmt.Errorf("%w: write input to context_path: %v", model.ErrInvalid, err)
	}

	// The post hook sees the accepted payload as a frozen output global and
	// may transform the context after the payload was written.
	postCtx, err := s.hooks.RunPost(ctx, inputNode, newCtx, payload)
	if err != nil {
		return s.failAcceptedInput(ctx, inst, attempt, req, model.NodeFailed, newCtx, err)
	}
	newCtx = postCtx

	next := inputNode.NextNode
	done, exited, err := engine.Advance(&frame, graph, next)
	if err != nil {
		return nil, err
	}
	if len(exited) > 0 {
		finalCtx, herr := engine.RunExitedGroupPosts(ctx, s.hooks, graph, exited, newCtx)
		if herr != nil {
			// The input attempt finished; the structural group hook failure
			// fails the workflow with the latest completed context.
			return s.failAcceptedInput(ctx, inst, attempt, req, model.NodeFinished, finalCtx, herr)
		}
		newCtx = finalCtx
	}
	status := model.WorkflowWaiting
	var finished *time.Time
	if done {
		status = model.WorkflowFinished
		t := nowUTC()
		finished = &t
	}

	return s.instances.DeliverInput(ctx, repository.InputCompletion{
		InstanceID:     inst.ID,
		NodeInstanceID: attempt.ID,
		IdempotencyKey: req.IdempotencyKey,
		Payload:        req.Payload,
		Accepted:       true,
		NewFrame:       frame,
		NewContext:     mustMarshal(newCtx),
		Status:         status,
		FinishedAt:     finished,
		CreatedBy:      s.actor,
	})
}

// failAcceptedInput records an accepted input delivery whose post processing
// failed: the delivery stays accepted (the API returns 202), the node
// attempt takes nodeStatus, and the workflow fails with the merged context.
func (s *instanceService) failAcceptedInput(ctx context.Context, inst *model.WorkflowInstance, attempt *model.NodeInstance, req DeliverInput, nodeStatus model.NodeStatus, ctxMap map[string]any, cause error) (*model.InputDelivery, error) {
	return s.instances.DeliverInput(ctx, repository.InputCompletion{
		InstanceID:     inst.ID,
		NodeInstanceID: attempt.ID,
		IdempotencyKey: req.IdempotencyKey,
		Payload:        req.Payload,
		Accepted:       true,
		PostFailure:    true,
		NodeStatus:     nodeStatus,
		NewContext:     mustMarshal(ctxMap),
		Error:          cause.Error(),
		CreatedBy:      s.actor,
	})
}

// contentGraph adapts a parsed workflow content to the engine cursor graph.
type contentGraph struct {
	wc *model.WorkflowContent
}

func (g *contentGraph) Node(id string) (*model.NodeContent, error) {
	var walk func(nodes []*model.NodeContent) *model.NodeContent
	walk = func(nodes []*model.NodeContent) *model.NodeContent {
		for _, n := range nodes {
			if n.ID == id {
				return n
			}
			if n.Group != nil {
				if found := walk(n.Group.Nodes); found != nil {
					return found
				}
			}
		}
		return nil
	}
	n := walk(g.wc.Nodes)
	if n == nil {
		return nil, fmt.Errorf("service: unknown node %q in workflow graph", id)
	}
	return n, nil
}

func (g *contentGraph) TypeOf(id string) (model.NodeType, error) {
	n, err := g.Node(id)
	if err != nil {
		return "", err
	}
	return n.Type, nil
}

func (g *contentGraph) NextOf(id string) (string, error) {
	n, err := g.Node(id)
	if err != nil {
		return "", err
	}
	return n.NextNode, nil
}

func (g *contentGraph) StartOf(id string) (string, error) {
	n, err := g.Node(id)
	if err != nil {
		return "", err
	}
	if n.Group == nil {
		return "", fmt.Errorf("service: node %q is not a group", id)
	}
	return n.Group.StartNodeID, nil
}

func unmarshalJSON(raw json.RawMessage) (map[string]any, error) {
	m := map[string]any{}
	if len(raw) == 0 {
		return m, nil
	}
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil, fmt.Errorf("service: parse context: %w", err)
	}
	return m, nil
}

func cloneContext(m map[string]any) (map[string]any, error) {
	raw, err := json.Marshal(m)
	if err != nil {
		return nil, err
	}
	out := map[string]any{}
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, err
	}
	return out, nil
}

func mustMarshal(v any) json.RawMessage {
	b, err := json.Marshal(v)
	if err != nil {
		return json.RawMessage("null")
	}
	return b
}
