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
