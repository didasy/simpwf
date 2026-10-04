// Package service implements the use-case orchestration for immutable
// definitions, workflow instances, input delivery, and runtime controls.
package service

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/simpwf/workflow-engine/internal/workflow/auth"
	"github.com/simpwf/workflow-engine/internal/workflow/engine"
	"github.com/simpwf/workflow-engine/internal/workflow/executor"
	"github.com/simpwf/workflow-engine/internal/workflow/form"
	"github.com/simpwf/workflow-engine/internal/workflow/model"
	"github.com/simpwf/workflow-engine/internal/workflow/repository"
	"github.com/simpwf/workflow-engine/pkg/contextdiff"
	"github.com/simpwf/workflow-engine/pkg/envsnapshot"
)

// CreateInstance starts a workflow instance. Debug marks a step-through
// run: the instance starts paused and re-pauses after every node until
// termination. It is immutable after create.
type CreateInstance struct {
	WorkflowDefinitionID string
	Context              json.RawMessage
	Debug                bool
	// Actor is the users.id uuid recorded as created_by/updated_by. An
	// empty Actor falls back to the service default (the system user), so
	// a caller that predates per-request actors keeps working unchanged.
	Actor string
}

// UpdateContext replaces the full context of a paused instance. Reason is an
// optional audit annotation recorded on the context_updated event.
type UpdateContext struct {
	InstanceID string
	Context    json.RawMessage
	Reason     string
	// Principal is the caller. A nil Principal is the service principal
	// (API token, broker, or internal caller), which bypasses the object
	// check exactly as it bypasses the endpoint gate.
	Principal *auth.Principal
	// Actor is the users.id uuid recorded on the update. Empty falls back
	// to the resolved principal's user id and then to the service default.
	Actor string
}

// DeliverInput delivers a payload to a waiting input node. Source is the
// transport the payload arrived on ("http", "redis", or "rabbitmq"); an
// empty Source defaults to "http".
type DeliverInput struct {
	InstanceID string
	// BranchID targets a branch-parked input node instead of the instance
	// cursor. Empty selects the instance cursor (the parent scope).
	BranchID       string
	IdempotencyKey string
	Payload        []byte
	Source         string
	// Principal is the caller. The API-token and broker paths pass the
	// service principal, which skips both authorization gates but is still
	// recorded as the deliverer. A nil Principal means the request carried
	// no credential at all: an anonymous caller. It is refused everywhere
	// except a public input node, which accepts it and marks the delivery
	// as anonymous in the audit trail.
	Principal *auth.Principal
	// Actor is the users.id uuid recorded on the delivery and the audit
	// event. Empty falls back to the resolved principal's user id and then
	// to the service default.
	Actor string
	// Anonymous is true when the HTTP layer saw no credential on a route
	// whose auth is optional. It separates "the caller proved nothing" from
	// "the caller proved the wrong thing": an anonymous request is refused
	// on a private node, and on a public node it is accepted and audited as
	// anonymous rather than as the service principal. The broker paths
	// leave it false so they keep their trusted-internal bypass.
	Anonymous bool
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
	// PendingInput describes the input the instance waits for. Non-nil only
	// when the instance waits on an input node; Form is nil when that node
	// carries no form contract.
	PendingInput *PendingInput
	// Parallel lists every parallel execution of the instance in creation
	// order with its branches. Nil when the instance never forked. Loops
	// fork repeatedly, so this is a list rather than the single object an
	// early sketch showed; nested executions link to their owner branch
	// via ParentBranchID.
	Parallel []ParallelExecutionView
}

// ParallelExecutionView is one parallel fork/join scope on status: cursors
// and counters without lease internals or branch contexts.
type ParallelExecutionView struct {
	ID             string
	ParentBranchID *string
	Depth          int
	StartNodeID    string
	EndNodeID      string
	Status         string
	BranchCount    int
	CompletedCount int
	Branches       []ParallelBranchView
}

// ParallelBranchView is one branch on status: identity, lifecycle state,
// and the last error. WaitingReason is "" unless the branch waits.
type ParallelBranchView struct {
	ID            string
	Name          string
	BranchIndex   int
	StartNodeID   string
	Status        string
	WaitingReason string
	Error         string
	UpdatedAt     time.Time
}

// PendingInput is the waiting-input contract served on status: the frontend
// renders its dynamic form from Form, then delivers the payload.
type PendingInput struct {
	NodeID         string
	Channel        string
	OutputProperty string
	Form           *model.InputForm
	// AllowedRoles is the node's role gate, so a frontend can hide or
	// explain a delivery the caller's roles would be refused for. It is
	// empty when the node is open.
	AllowedRoles []string
	// RecordActor reports whether an accepted delivery is written as the
	// attribution envelope {user_id, input_data}, so the frontend builds
	// the follow-up form against the right shape.
	RecordActor bool
	// Public reports that the node accepts anonymous deliveries over HTTP
	// with no credential, even when auth is on. The form still stays
	// behind authentication, so an anonymous caller learns the shape
	// out-of-band rather than from this contract.
	Public bool
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

// DebugContextDetail is the resolved debug-position context view: the
// redacted snapshot rendered as a TypeScript declaration. OccurrenceID
// and Attempt are nil when the position has no occurrence (the live
// instance context is the source).
type DebugContextDetail struct {
	InstanceID    string
	NodeID        string
	OccurrenceID  *string
	Attempt       *int
	IsDebugPaused bool
	TypeScript    string
}

// ControlResult reports the post-transition state of a pause/resume/stop
// control call.
type ControlResult struct {
	Status             model.WorkflowStatus
	PauseRequested     bool
	TerminationPending bool
}

// ControlRequest is the input for a pause, resume, or stop control. The
// three share one shape so a single caller identity carries through every
// control path.
type ControlRequest struct {
	InstanceID string
	// Reason is the caller-supplied stop reason, recorded on the event.
	Reason string
	// Principal is the caller. A nil Principal is the service principal
	// (API token, broker, or internal caller), which bypasses the object
	// check exactly as it bypasses the endpoint gate.
	Principal *auth.Principal
	// Actor is the users.id uuid recorded on the control event. Empty falls
	// back to the resolved principal's user id and then to the service
	// default, so a service call or an unauthenticated deployment keeps
	// working.
	Actor string
}

// RollbackRequest moves a paused or failed instance's cursor back to an
// already-executed node occurrence. Reason is an optional audit annotation
// recorded on the rollback event only.
type RollbackRequest struct {
	InstanceID         string
	TargetOccurrenceID string
	Reason             string
	// Principal is the caller. A nil Principal is the service principal
	// (API token, broker, or internal caller), which bypasses the object
	// check exactly as it bypasses the endpoint gate.
	Principal *auth.Principal
	// Actor is the users.id uuid recorded on the rollback. Empty falls back
	// to the resolved principal's user id and then to the service default.
	Actor string
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

// InstanceService is the use-case boundary for workflow instances. Read
// and control calls take the caller's resolved users.id as actor alongside
// the request principal where one exists: the route permission gate answers
// "may this caller act at all", while the service answers "may this caller
// touch this instance" from the principal, so a service principal (API
// token, broker, or internal caller) bypasses the object check exactly as
// it bypasses the endpoint gate.
type InstanceService interface {
	Create(ctx context.Context, req CreateInstance) (model.WorkflowInstance, error)
	GetStatus(ctx context.Context, id string, p auth.Principal) (*model.WorkflowInstance, error)
	// GetStatusDetail resolves the current node occurrence for the status
	// response.
	GetStatusDetail(ctx context.Context, id string, p auth.Principal) (*StatusDetail, error)
	// List returns the instances matching the query with pagination and
	// ordering, mirroring the repository query.
	List(ctx context.Context, q repository.InstanceListQuery, p auth.Principal) ([]model.WorkflowInstance, int64, error)
	GetContext(ctx context.Context, id string, p auth.Principal) (*model.WorkflowInstance, error)
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
	NodeDebug(ctx context.Context, instanceID, nodeID string, attempt int, p auth.Principal) (*NodeDebugDetail, error)
	// DebugContext resolves the debug-position context of a debug instance
	// as a redacted TypeScript declaration plus inferred JSON Schema.
	// nodeID is either the workflow graph node id or the occurrence id;
	// empty selects the debug cursor (falling back to the live instance
	// context when the cursor is empty or terminal). attempt <= 0 selects
	// the latest attempt, a positive attempt selects an exact execution.
	DebugContext(ctx context.Context, instanceID, nodeID string, attempt int, p auth.Principal) (*DebugContextDetail, error)
	// Pause pauses a waiting instance immediately and requests a deferred
	// pause for a running instance. Idempotent while paused.
	Pause(ctx context.Context, req ControlRequest) (*ControlResult, error)
	// Resume returns a paused instance to waiting and clears a pending
	// pause on a running instance. Idempotent on active instances.
	Resume(ctx context.Context, req ControlRequest) (*ControlResult, error)
	// Stop moves an instance to the terminal stopped state, fences worker
	// commits, and signals local cancellation. Idempotent on stopped
	// instances.
	Stop(ctx context.Context, req ControlRequest) (*ControlResult, error)
	// Rollback moves a paused or failed instance's cursor back to an
	// already-executed node occurrence so the next resume re-executes
	// forward from there. The instance is always paused afterwards and its
	// context is restored from the target occurrence's ContextBefore.
	// History is immutable; the next execution increments the target
	// occurrence's attempt. A live parked input attempt never blocks the
	// rollback: targeting the park itself is a no-op, any other target
	// supersedes (closes) it atomically in the same transaction.
	Rollback(ctx context.Context, req RollbackRequest) (*RollbackResult, error)
}

type instanceService struct {
	instances    repository.InstanceRepository
	parallel     repository.ParallelRepository
	wfDefs       repository.WorkflowDefinitionRepository
	secrets      SecretSnapshotter
	materializer WorkflowMaterializer
	validator    *executor.InputExecutor
	hooks        *executor.HookRunner
	actor        string
	limits       model.NodeLimits
	cancels      Canceller
	leanOptions  model.LeanOptions
	// catalog is the read-only role catalog used to turn a caller's roles
	// into the permissions the input endpoint gate checks.
	catalog auth.Catalog
}

// NewInstanceService builds the instance service. cancels may be nil; when
// set it receives the local cancellation signal for stopped instances. The
// catalog is consulted only by the input authorization gate.
func NewInstanceService(
	instances repository.InstanceRepository,
	parallel repository.ParallelRepository,
	wfDefs repository.WorkflowDefinitionRepository,
	secrets SecretSnapshotter,
	materializer WorkflowMaterializer,
	validator *executor.InputExecutor,
	hooks *executor.HookRunner,
	actor string,
	limits model.NodeLimits,
	cancels Canceller,
	leanOptions model.LeanOptions,
) InstanceService {
	return newInstanceService(
		instances, parallel, wfDefs, secrets, materializer, validator, hooks,
		actor, limits, cancels, leanOptions, auth.Catalog{},
	)
}

// NewInstanceServiceWithCatalog is NewInstanceService plus the role catalog
// the input authorization gate reads. An empty catalog denies every
// resource-action, which is the correct default for a deployment that never
// configured roles.
func NewInstanceServiceWithCatalog(
	instances repository.InstanceRepository,
	parallel repository.ParallelRepository,
	wfDefs repository.WorkflowDefinitionRepository,
	secrets SecretSnapshotter,
	materializer WorkflowMaterializer,
	validator *executor.InputExecutor,
	hooks *executor.HookRunner,
	actor string,
	limits model.NodeLimits,
	cancels Canceller,
	leanOptions model.LeanOptions,
	catalog auth.Catalog,
) InstanceService {
	return newInstanceService(
		instances, parallel, wfDefs, secrets, materializer, validator, hooks,
		actor, limits, cancels, leanOptions, catalog,
	)
}

func newInstanceService(
	instances repository.InstanceRepository,
	parallel repository.ParallelRepository,
	wfDefs repository.WorkflowDefinitionRepository,
	secrets SecretSnapshotter,
	materializer WorkflowMaterializer,
	validator *executor.InputExecutor,
	hooks *executor.HookRunner,
	actor string,
	limits model.NodeLimits,
	cancels Canceller,
	leanOptions model.LeanOptions,
	catalog auth.Catalog,
) InstanceService {
	return &instanceService{
		instances:    instances,
		parallel:     parallel,
		wfDefs:       wfDefs,
		secrets:      secrets,
		materializer: materializer,
		validator:    validator,
		hooks:        hooks,
		actor:        actor,
		limits:       limits,
		cancels:      cancels,
		leanOptions:  leanOptions,
		catalog:      catalog,
	}
}

// createdEventData renders the instance_created audit payload. Debug runs
// carry debug:true so step-through runs are distinguishable in the trail.
func createdEventData(definitionID string, debug bool) json.RawMessage {
	if !debug {
		return json.RawMessage(`{"workflow_definition_id":"` + definitionID + `"}`)
	}
	return json.RawMessage(`{"workflow_definition_id":"` + definitionID + `","debug":true}`)
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
	if err := json.Unmarshal(contextRaw, &obj); err != nil || obj == nil {
		return model.WorkflowInstance{}, fmt.Errorf("%w: context must be a JSON object", model.ErrInvalid)
	}
	// Snapshot SIMPWF_* process env under the reserved env root so every
	// existing {{ }} render site resolves {{ env.SIMPWF_X }} with zero
	// template-engine changes. Snapshot wins per key: callers cannot
	// shadow env-provided credentials with request-body values. The
	// caller-supplied base is stripped of denied names first, so a request
	// body cannot smuggle a denied var past a snapshot that excluded it.
	obj["env"] = envsnapshot.Merge(envsnapshot.StripDenied(obj["env"]), envsnapshot.Snapshot(envsnapshot.Prefix))
	var secretSnapshot map[string]string
	if s.secrets != nil {
		secretSnapshot, err = s.secrets.GetAll(ctx)
		if err != nil {
			return model.WorkflowInstance{}, err
		}
	}
	mergeSecretSnapshot(obj, secretSnapshot)
	if rebased, err := json.Marshal(obj); err == nil {
		contextRaw = rebased
	} else {
		return model.WorkflowInstance{}, fmt.Errorf("%w: %v", model.ErrInvalid, err)
	}

	frameRaw, err := model.NewFrame(wc.StartNodeID).JSON()
	if err != nil {
		return model.WorkflowInstance{}, err
	}
	now := nowUTC()
	// A request carrying its own actor records that user; every other
	// caller (a service call, a test, the broker) records the system user.
	actor := s.actorOrDefault(req.Actor)
	contextMode := model.ContextModeFull
	if wc.ContextMode != nil {
		contextMode = *wc.ContextMode
	} else if s.leanOptions.LeanContextDefault {
		contextMode = model.ContextModeLean
	}
	w := model.WorkflowInstance{
		ID:                   mustNewID(),
		WorkflowDefinitionID: def.ID,
		ContextMode:          contextMode,
		Debug:                req.Debug,
		Status:               model.WorkflowWaiting,
		WaitingReason:        model.WaitingReasonRunnable,
		Frame:                frameRaw,
		Context:              contextRaw,
		Counters:             mustMarshal(model.Counters{}),
		CreatedBy:            actor,
		UpdatedBy:            actor,
		CreatedAt:            now,
		UpdatedAt:            now,
	}
	if req.Debug {
		// Debug runs start paused so nothing executes before the first
		// manual resume. The dispatcher only claims waiting/runnable
		// rows, so the instance parks until resumed.
		w.Status = model.WorkflowPaused
	}
	if err := s.instances.Insert(ctx, w); err != nil {
		return model.WorkflowInstance{}, err
	}
	if contextMode == model.ContextModeLean {
		// The create context is the replay base but has no history row yet.
		// Anchor it at create so later UpdateContext baselines and
		// post-rollback anchors replay from the true initial object instead
		// of an empty map.
		anchor := leanAnchorRow(contextRaw)
		anchor.WorkflowInstanceID = w.ID
		if err := s.instances.AppendHistory(ctx, *anchor); err != nil {
			return model.WorkflowInstance{}, err
		}
	}
	_ = s.instances.AppendEvent(ctx, model.WorkflowInstanceEvent{
		ID: mustNewID(), WorkflowInstanceID: w.ID, Type: "instance_created",
		Data:      createdEventData(def.ID, req.Debug),
		CreatedBy: actor, CreatedAt: now,
	})
	return w, nil
}

// requestPrincipal resolves the caller of a request carrying an optional
// principal. A nil Principal is the service principal: the API-token path,
// the broker consumers, and any internal caller carry no other credential,
// and they are exactly the paths the bypass describes.
func (s *instanceService) requestPrincipal(p *auth.Principal) auth.Principal {
	if p != nil {
		return *p
	}
	return auth.SystemPrincipal(s.actor)
}

// actorOrDefault resolves the audit actor of a request: an explicit actor
// wins, otherwise the call is attributed to the system user.
func (s *instanceService) actorOrDefault(actor string) string {
	return resolveActor(actor, s.actor)
}

// requestActor picks the users.id uuid recorded on a request. An explicit
// Actor wins, then the resolved principal's user id, then the service
// default, so an audited write is never recorded without an actor.
func (s *instanceService) requestActor(p *auth.Principal, actor string) string {
	if actor != "" {
		return actor
	}
	if p != nil && p.UserID != "" {
		return p.UserID
	}
	return s.actor
}

// resolveActor is the shared form of actorOrDefault, for the services that
// carry a default actor but are not instanceService. An empty actor always
// falls back to the configured system user, so a request that predates
// per-request actors keeps working unchanged.
func resolveActor(actor, fallback string) string {
	if actor != "" {
		return actor
	}
	return fallback
}

func (s *instanceService) GetStatus(ctx context.Context, id string, p auth.Principal) (*model.WorkflowInstance, error) {
	inst, err := s.instances.GetByID(ctx, id)
	if err != nil {
		return nil, err
	}
	if err := authorizeInstance(inst, p); err != nil {
		return nil, err
	}
	return redactInstanceView(inst), nil
}

func (s *instanceService) List(ctx context.Context, q repository.InstanceListQuery, p auth.Principal) ([]model.WorkflowInstance, int64, error) {
	items, total, err := s.instances.List(ctx, ownedInstances(q, p))
	if err != nil {
		return nil, 0, err
	}
	var errorIDs []string
	for _, item := range items {
		if item.Error != "" {
			errorIDs = append(errorIDs, item.ID)
		}
	}
	if len(errorIDs) == 0 {
		return items, total, nil
	}
	reader, ok := s.instances.(repository.InstanceContextReader)
	if !ok {
		for i := range items {
			if items[i].Error != "" {
				items[i].Error = SecretMask
			}
		}
		return items, total, nil
	}
	contexts, err := reader.GetContextsByIDs(ctx, errorIDs)
	if err != nil {
		return nil, 0, err
	}
	redactInstanceListItems(items, contexts)
	return items, total, nil
}

func (s *instanceService) GetStatusDetail(ctx context.Context, id string, p auth.Principal) (*StatusDetail, error) {
	inst, err := s.instances.GetByID(ctx, id)
	if err != nil {
		return nil, err
	}
	if err := authorizeInstance(inst, p); err != nil {
		return nil, err
	}
	d := &StatusDetail{Instance: *redactInstanceView(inst)}
	d.Nodes = s.statusNodes(ctx, inst)
	d.Parallel = s.statusParallel(ctx, inst.ID)
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
	d.PendingInput = s.pendingInput(ctx, inst, frame.CurrentNodeID)
	return d, nil
}

// pendingInput resolves the waiting-input contract for status responses. It
// returns nil unless the instance waits on an input node, and on any graph
// load failure (status never fails for graph reasons).
func (s *instanceService) pendingInput(ctx context.Context, inst *model.WorkflowInstance, currentNodeID string) *PendingInput {
	if inst.Status != model.WorkflowWaiting || inst.WaitingReason != model.WaitingReasonInput {
		return nil
	}
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
	graph := &contentGraph{wc: wc}
	node, err := graph.Node(currentNodeID)
	if err != nil {
		return nil
	}
	if node.Type != model.NodeTypeInput {
		return nil
	}
	key := strings.TrimSpace(node.OutputProperty)
	if key == "" {
		key = node.ID
	}
	return &PendingInput{
		NodeID:         node.ID,
		Channel:        node.Channel,
		OutputProperty: key,
		Form:           node.Form,
		AllowedRoles:   node.AllowedRoles,
		RecordActor:    node.RecordActor,
		Public:         node.Public,
	}
}

func (s *instanceService) GetContext(ctx context.Context, id string, p auth.Principal) (*model.WorkflowInstance, error) {
	inst, err := s.instances.GetByID(ctx, id)
	if err != nil {
		return nil, err
	}
	if err := authorizeInstance(inst, p); err != nil {
		return nil, err
	}
	return redactInstanceView(inst), nil
}

// leanMode reports whether the instance runs with replayable diff history.
func leanMode(inst *model.WorkflowInstance) bool {
	return inst != nil && inst.ContextMode == model.ContextModeLean
}

// mapHistoryError maps history replay failures to 409 conflict semantics:
// a gap or depth overflow fails closed, never with a silent wrong restore.
func mapHistoryError(err error) error {
	if errors.Is(err, repository.ErrHistoryGap) || errors.Is(err, repository.ErrHistoryTooDeep) {
		return fmt.Errorf("%w: %v", model.ErrConflict, err)
	}
	return err
}

// leanHistoryCursorForOccurrence resolves the non-superseded history cursor
// for one occurrence: the earliest non-superseded row carrying that
// occurrence id, so reconstruction returns that occurrence's before
// context. The boolean reports presence. A positive attempt selects that
// exact attempt row.
func leanHistoryCursorForOccurrence(rows []model.NodeContextHistory, occurrenceID string, attempt int) (model.HistoryCursor, bool) {
	var cursor model.HistoryCursor
	found := false
	for _, row := range rows {
		if row.Superseded || row.OccurrenceID != occurrenceID {
			continue
		}
		if attempt > 0 && row.Attempt != attempt {
			continue
		}
		if !found || row.Cursor().Before(cursor) {
			cursor = row.Cursor()
			found = true
		}
	}
	return cursor, found
}

// leanOccurrenceSet builds the set of occurrence ids with non-superseded
// history rows. Callers load history once and reuse the set per node.
func leanOccurrenceSet(rows []model.NodeContextHistory) map[string]struct{} {
	out := make(map[string]struct{}, len(rows))
	for _, row := range rows {
		if row.Superseded || row.OccurrenceID == "" {
			continue
		}
		out[row.OccurrenceID] = struct{}{}
	}
	return out
}

// leanReconstructBefore replays lean history up to (excluding) the target
// cursor, mapping replay failures to 409. The create anchor (or a later
// UpdateContext/rollback anchor) is the replay base, so the passed initial
// seed is only a fallback for instances created before anchors existed.
func (s *instanceService) leanReconstructBefore(ctx context.Context, inst *model.WorkflowInstance, target model.HistoryCursor) (json.RawMessage, error) {
	initial := json.RawMessage("{}")
	restored, err := s.instances.ReconstructBefore(ctx, inst.ID, target, initial)
	if err != nil {
		return nil, mapHistoryError(err)
	}
	return restored, nil
}

// leanAnchorRow builds a post-commit anchor snapshot row for the restored
// full context. Occurrence id stays empty: the row is a system baseline,
// not a node commit.
func leanAnchorRow(restored json.RawMessage) *model.NodeContextHistory {
	return &model.NodeContextHistory{
		IsAnchor: true,
		Snapshot: restored,
		Diff:     json.RawMessage("null"),
	}
}

// validateRestoredObject ensures the replayed context is a JSON object
// before it becomes the instance context. Anything else fails closed.
func validateRestoredObject(restored json.RawMessage) (map[string]any, error) {
	var obj map[string]any
	if err := json.Unmarshal(restored, &obj); err != nil || obj == nil {
		return nil, fmt.Errorf("restored context is not a JSON object")
	}
	return obj, nil
}

// toStringMap normalizes a decoded JSON value to map[string]any. Non-map
// inputs (nil, scalars, slices) yield an empty map so Merge overlays a
// clean snapshot instead of inheriting garbage.
func toStringMap(v any) map[string]any {
	if m, ok := v.(map[string]any); ok {
		return m
	}
	return map[string]any{}
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
	if instanceGate {
		// Mirror the rollback endpoint: live parallel branches block
		// every target. A load failure leaves the gate to the
		// endpoint, which rechecks authoritatively.
		if live, err := s.hasLiveParallel(ctx, inst.ID); err == nil && live {
			instanceGate = false
		}
	}
	// Lean mode gates rollbackability on history presence, not on
	// ContextBefore parsing (lean occurrence rows store null). The history
	// occurrence-id set loads once per call: no per-node query fan-out.
	var leanSet map[string]struct{}
	if leanMode(inst) && instanceGate {
		if rows, herr := s.instances.LoadHistory(ctx, inst.ID); herr == nil {
			leanSet = leanOccurrenceSet(rows)
		}
	}
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
		if leanSet != nil {
			e.Rollbackable = instanceGate && leanRollbackableOccurrence(nc.Type, occ, leanSet)
		} else {
			e.Rollbackable = instanceGate && rollbackableOccurrence(nc.Type, occ)
		}
		out[id] = e
	}
	return out
}

// statusParallel builds the parallel execution tree for the status
// response. It returns nil when the instance never forked or when the rows
// cannot be loaded, so the status view degrades to omitting the section
// instead of failing the whole call.
func (s *instanceService) statusParallel(ctx context.Context, instanceID string) []ParallelExecutionView {
	exs, err := s.parallel.ListExecutions(ctx, instanceID)
	if err != nil || len(exs) == 0 {
		return nil
	}
	out := make([]ParallelExecutionView, 0, len(exs))
	for _, ex := range exs {
		branches, err := s.parallel.ListBranches(ctx, ex.ID)
		if err != nil {
			return nil
		}
		v := ParallelExecutionView{
			ID:             ex.ID,
			ParentBranchID: ex.ParentBranchID,
			Depth:          ex.Depth,
			StartNodeID:    ex.StartNodeID,
			EndNodeID:      ex.EndNodeID,
			Status:         string(ex.Status),
			BranchCount:    ex.BranchCount,
			CompletedCount: ex.CompletedCount,
			Branches:       make([]ParallelBranchView, 0, len(branches)),
		}
		for _, b := range branches {
			v.Branches = append(v.Branches, ParallelBranchView{
				ID:            b.ID,
				Name:          b.Name,
				BranchIndex:   b.BranchIndex,
				StartNodeID:   b.StartNodeID,
				Status:        string(b.Status),
				WaitingReason: string(b.WaitingReason),
				Error:         b.Error,
				UpdatedAt:     b.UpdatedAt,
			})
		}
		out = append(out, v)
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

// leanRollbackableOccurrence mirrors rollbackableOccurrence for lean mode:
// group nodes never qualify, only terminal occurrence states qualify, and
// the occurrence must have non-superseded history (lean rows store null
// ContextBefore, so parsing it would always fail).
func leanRollbackableOccurrence(typ model.NodeType, occ *model.NodeInstance, set map[string]struct{}) bool {
	if typ == model.NodeTypeGroup {
		return false
	}
	if occ.BranchID != "" {
		return false
	}
	switch occ.Status {
	case model.NodeFinished, model.NodeFailed, model.NodeStopped:
	default:
		return false
	}
	if set == nil {
		return false
	}
	_, ok := set[occ.ID]
	return ok
}

// rollbackableOccurrence mirrors the rollback endpoint's target validation:
// group nodes never qualify (they have no occurrence), only terminal
// occurrence states qualify, and the ContextBefore snapshot must parse as
// a JSON object. Branch-owned occurrences never qualify: rollback moves
// the parent cursor only. The caller gates on instance status.
func rollbackableOccurrence(typ model.NodeType, occ *model.NodeInstance) bool {
	if typ == model.NodeTypeGroup {
		return false
	}
	if occ.BranchID != "" {
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
	// Carry stored env and secret snapshots forward. Replacement bodies must
	// not drop them or inject values under reserved roots; stored values win.
	// Denied names are stripped from the replacement body for the same reason
	// they are excluded from the snapshot.
	var storedEnv any
	storedSecrets := map[string]string{}
	cur, curErr := s.instances.GetByID(ctx, req.InstanceID)
	if curErr == nil && cur != nil {
		curObj, ok := decodeContextObject(cur.Context)
		if !ok {
			return nil, fmt.Errorf("%w: stored context is invalid", model.ErrConflict)
		}
		var trusted bool
		storedSecrets, trusted = secretValuesFromObject(curObj)
		if !trusted {
			return nil, fmt.Errorf("%w: stored secret snapshot is invalid", model.ErrConflict)
		}
		storedEnv = curObj["env"]
	} else if curErr != nil && !errors.Is(curErr, repository.ErrInstanceNotFound) {
		return nil, curErr
	}
	obj["env"] = envsnapshot.Merge(envsnapshot.StripDenied(obj["env"]), toStringMap(storedEnv))
	mergeSecretSnapshot(obj, storedSecrets)
	rebased, err := json.Marshal(obj)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", model.ErrInvalid, err)
	}
	req.Context = rebased
	principal := s.requestPrincipal(req.Principal)
	if err := authorizeInstance(cur, principal); err != nil {
		return nil, err
	}
	inst, err := s.instances.ReplaceContext(ctx, repository.ContextUpdate{
		InstanceID: req.InstanceID,
		Context:    req.Context,
		Actor:      s.requestActor(req.Principal, req.Actor),
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
	// Lean mode appends the replaced context as a new anchor baseline so
	// later replays start from it. The prefix stays intact: no supersede.
	if leanMode(inst) {
		anchor := leanAnchorRow(req.Context)
		anchor.WorkflowInstanceID = inst.ID
		if err := s.instances.AppendHistory(ctx, *anchor); err != nil {
			return nil, err
		}
		updated, err := s.instances.GetByID(ctx, req.InstanceID)
		if err != nil {
			return nil, err
		}
		return redactInstanceView(updated), nil
	}
	return redactInstanceView(inst), nil
}

func (s *instanceService) NodeDebug(ctx context.Context, instanceID, nodeID string, attempt int, p auth.Principal) (*NodeDebugDetail, error) {
	inst, err := s.instances.GetByID(ctx, instanceID)
	if err != nil {
		if errors.Is(err, repository.ErrInstanceNotFound) {
			return nil, fmt.Errorf("%w: instance %s", model.ErrNotFound, instanceID)
		}
		return nil, err
	}
	if err := authorizeInstance(inst, p); err != nil {
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
	if leanMode(inst) {
		// Lean occurrence rows store null ContextBefore and diff
		// ContextAfter; reconstruct full snapshots without changing the
		// DTO shape. Anchor targets use their snapshot, diff targets
		// replay history then apply the occurrence diff.
		before, after, derr := s.leanNodeDebugContexts(ctx, inst, nodeInst, selected)
		if derr != nil {
			return nil, derr
		}
		d.ContextBefore = before
		d.ContextAfter = after
	}
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
	secrets, trusted := secretValuesFromContext(inst.Context)
	redactNodeDebugSecrets(d, secrets, trusted)
	return d, nil
}

func nullableString(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

// leanNodeDebugContexts reconstructs full before/after contexts for a lean
// occurrence. The after context replays history through the selected row;
// the before context replays through the immediately preceding history
// row. Anchor rows use their snapshot as the after context. Failures map
// to 409 conflict semantics.
func (s *instanceService) leanNodeDebugContexts(ctx context.Context, inst *model.WorkflowInstance, occ *model.NodeInstance, selected int) (json.RawMessage, json.RawMessage, error) {
	rows, err := s.instances.LoadHistory(ctx, inst.ID)
	if err != nil {
		return nil, nil, err
	}
	target, found := leanHistoryCursorForOccurrence(rows, occ.ID, 0)
	if !found {
		return nil, nil, fmt.Errorf("%w: occurrence %q has no history", model.ErrConflict, occ.ID)
	}
	_ = selected
	before, err := s.leanReconstructBefore(ctx, inst, target)
	if err != nil {
		return nil, nil, err
	}
	// The selected row's own history payload is the after context: anchors
	// carry snapshots and diffs apply on the before context. Replaying
	// through a successor can skip this rule, so resolve directly.
	for _, row := range rows {
		if row.Superseded || row.OccurrenceID != occ.ID {
			continue
		}
		if row.Cursor() != target {
			continue
		}
		if row.IsAnchor {
			if _, verr := validateRestoredObject(row.Snapshot); verr != nil {
				return nil, nil, fmt.Errorf("%w: %v", model.ErrConflict, verr)
			}
			return before, row.Snapshot, nil
		}
		targetDiff, derr := contextdiff.ParseDiff(row.Diff)
		if derr != nil {
			return nil, nil, fmt.Errorf("%w: %v", model.ErrConflict, derr)
		}
		beforeMap, merr := unmarshalJSON(before)
		if merr != nil {
			return nil, nil, fmt.Errorf("%w: %v", model.ErrConflict, merr)
		}
		afterMap, aerr := contextdiff.Apply(beforeMap, targetDiff)
		if aerr != nil {
			return nil, nil, fmt.Errorf("%w: %v", model.ErrConflict, aerr)
		}
		after, merr := json.Marshal(afterMap)
		if merr != nil {
			return nil, nil, fmt.Errorf("%w: %v", model.ErrConflict, merr)
		}
		return before, after, nil
	}
	return nil, nil, fmt.Errorf("%w: occurrence %q has no history", model.ErrConflict, occ.ID)
}

func (s *instanceService) Pause(ctx context.Context, req ControlRequest) (*ControlResult, error) {
	id := req.InstanceID
	principal := s.requestPrincipal(req.Principal)
	actor := s.requestActor(req.Principal, req.Actor)
	inst, err := s.instance(ctx, id)
	if err != nil {
		return nil, err
	}
	if err := authorizeInstance(inst, principal); err != nil {
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
			Data: json.RawMessage(`{}`), CreatedBy: actor, CreatedAt: nowUTC(),
		})
		return &ControlResult{Status: model.WorkflowRunning, PauseRequested: true}, nil
	}
	_ = s.instances.AppendEvent(ctx, model.WorkflowInstanceEvent{
		ID: mustNewID(), WorkflowInstanceID: id, Type: "paused",
		Data: json.RawMessage(`{}`), CreatedBy: actor, CreatedAt: nowUTC(),
	})
	return &ControlResult{Status: model.WorkflowPaused}, nil
}

func (s *instanceService) Resume(ctx context.Context, req ControlRequest) (*ControlResult, error) {
	id := req.InstanceID
	principal := s.requestPrincipal(req.Principal)
	actor := s.requestActor(req.Principal, req.Actor)
	inst, err := s.instance(ctx, id)
	if err != nil {
		return nil, err
	}
	if err := authorizeInstance(inst, principal); err != nil {
		return nil, err
	}
	if inst.Debug && (inst.Status == model.WorkflowPaused ||
		(inst.Status == model.WorkflowWaiting && inst.WaitingReason == model.WaitingReasonParallel)) {
		// Step-through: each resume advances exactly one paused
		// branch while the parent stays parked. A branch input
		// delivery can leave paused branches behind a waiting
		// parent, so waiting parents step branches too; a generic
		// parent wake here would join-spin against paused branches.
		if stepped, err := s.resumeDebugStep(ctx, inst, actor); err != nil || stepped != nil {
			return stepped, err
		}
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
		Data: json.RawMessage(`{}`), CreatedBy: actor, CreatedAt: nowUTC(),
	})
	return &ControlResult{Status: model.WorkflowWaiting}, nil
}

// resumeDebugStep wakes one paused branch for a debug step-through
// resume: the shallowest execution wins, then the alphabetically first
// branch, so stepping order is deterministic. It returns nil when no
// branch needs stepping so the caller wakes the parent instead. A branch
// with a step already in flight (running or claimed-runnable) conflicts:
// two steps at once would break the one-step-per-resume contract.
func (s *instanceService) resumeDebugStep(ctx context.Context, inst *model.WorkflowInstance, actor string) (*ControlResult, error) {
	exs, err := s.parallel.ListExecutions(ctx, inst.ID)
	if err != nil {
		return nil, err
	}
	if len(exs) == 0 {
		return nil, nil
	}
	depth := make(map[string]int, len(exs))
	for _, ex := range exs {
		depth[ex.ID] = ex.Depth
	}
	var pick *model.ParallelBranch
	for _, ex := range exs {
		branches, err := s.parallel.ListBranches(ctx, ex.ID)
		if err != nil {
			return nil, err
		}
		for i := range branches {
			b := &branches[i]
			switch {
			case b.Status == model.ParallelBranchRunning ||
				(b.Status == model.ParallelBranchWaiting && b.WaitingReason == model.WaitingReasonRunnable):
				return nil, fmt.Errorf("%w: branch %q has a step in flight", model.ErrConflict, b.Name)
			case b.Status == model.ParallelBranchWaiting && b.WaitingReason == model.WaitingReasonPaused:
				if pick == nil || debugPickBefore(depth, b, pick) {
					c := *b
					pick = &c
				}
			}
		}
	}
	if pick == nil {
		return nil, nil
	}
	if err := s.parallel.WakePausedBranch(ctx, pick.ID); err != nil {
		if errors.Is(err, repository.ErrStatusConflict) || errors.Is(err, repository.ErrParallelBranchNotFound) {
			return nil, fmt.Errorf("%w: branch %q is no longer paused", model.ErrConflict, pick.Name)
		}
		return nil, err
	}
	_ = s.instances.AppendEvent(ctx, model.WorkflowInstanceEvent{
		ID: mustNewID(), WorkflowInstanceID: inst.ID, Type: "parallel_branch_resumed",
		Data: mustMarshal(map[string]any{
			"branch_id":             pick.ID,
			"parallel_execution_id": pick.ParallelExecutionID,
			"branch_name":           pick.Name,
			"branch_index":          pick.BranchIndex,
			"start_node_id":         pick.StartNodeID,
		}),
		CreatedBy: actor, CreatedAt: nowUTC(),
	})
	return &ControlResult{Status: inst.Status}, nil
}

// debugPickBefore orders paused branches for step-through: shallowest
// execution first, then branch index (alphabetical), then oldest, then id.
func debugPickBefore(depth map[string]int, b, pick *model.ParallelBranch) bool {
	if depth[b.ParallelExecutionID] != depth[pick.ParallelExecutionID] {
		return depth[b.ParallelExecutionID] < depth[pick.ParallelExecutionID]
	}
	if b.BranchIndex != pick.BranchIndex {
		return b.BranchIndex < pick.BranchIndex
	}
	if !b.CreatedAt.Equal(pick.CreatedAt) {
		return b.CreatedAt.Before(pick.CreatedAt)
	}
	return b.ID < pick.ID
}

func (s *instanceService) Stop(ctx context.Context, req ControlRequest) (*ControlResult, error) {
	id := req.InstanceID
	principal := s.requestPrincipal(req.Principal)
	actor := s.requestActor(req.Principal, req.Actor)
	inst, err := s.instance(ctx, id)
	if err != nil {
		return nil, err
	}
	if err := authorizeInstance(inst, principal); err != nil {
		return nil, err
	}
	switch inst.Status {
	case model.WorkflowStopped:
		return &ControlResult{Status: model.WorkflowStopped, TerminationPending: inst.TerminationPending}, nil
	case model.WorkflowFinished, model.WorkflowFailed:
		return nil, fmt.Errorf("%w: instance %s is terminal", model.ErrConflict, id)
	}
	pending, err := s.instances.Stop(ctx, id, req.Reason)
	if err != nil {
		if errors.Is(err, repository.ErrStatusConflict) {
			return nil, fmt.Errorf("%w: instance %s is terminal", model.ErrConflict, id)
		}
		return nil, err
	}
	_ = s.instances.AppendEvent(ctx, model.WorkflowInstanceEvent{
		ID: mustNewID(), WorkflowInstanceID: id, Type: "stop",
		Data:      mustMarshal(map[string]string{"reason": req.Reason}),
		CreatedBy: actor, CreatedAt: nowUTC(),
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
	if err := authorizeInstance(inst, s.requestPrincipal(req.Principal)); err != nil {
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
	if live, err := s.hasLiveParallel(ctx, inst.ID); err != nil {
		return nil, err
	} else if live {
		return nil, fmt.Errorf("%w: instance %s has live parallel branches; rollback is supported only before a parallel block or after it completes", model.ErrConflict, req.InstanceID)
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
	if occ.BranchID != "" {
		return nil, fmt.Errorf("%w: occurrence %q ran inside a parallel branch; roll back to a node before or after the parallel block", model.ErrInvalid, req.TargetOccurrenceID)
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
	hasLive := rerr == nil

	frame, err := model.ParseFrame(inst.Frame)
	if err != nil {
		return nil, err
	}

	// Self rollback: paused on the live park itself. No-op without writes.
	if hasLive && inst.Status == model.WorkflowPaused &&
		running.ID == occ.ID && frame.CurrentNodeID == target.ID {
		return &RollbackResult{
			Status:        model.WorkflowPaused,
			CurrentNodeID: target.ID,
			GroupStack:    frame.GroupStack,
		}, nil
	}
	if target.Type == model.NodeTypeInput {
		// Rolling back to an input node re-arms its wait as a fresh
		// attempt of the same finished occurrence (delivery history stays
		// attached for audit) so the next resume re-parks it and a fresh
		// delivery with a new idempotency key is accepted. Any other live
		// park is superseded (closed) atomically by the repository.
		if occ.Status != model.NodeFinished {
			return nil, fmt.Errorf("%w: input occurrence %q is not finished", model.ErrConflict, req.TargetOccurrenceID)
		}
	}
	stack, err := groupStack(wc, occ.NodeID)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", model.ErrInvalid, err)
	}
	restoreCtx, restoreCursor, rerr := s.resolveRollbackContext(ctx, inst, occ, req.TargetOccurrenceID)
	if rerr != nil {
		return nil, rerr
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
	// Any live attempt other than the re-armed target is superseded: the
	// repository closes it in the same transaction so the cursor and the
	// park never diverge.
	supersede := hasLive && (!rearm || running.ID != occ.ID)
	supersededOccurrence := ""
	supersededNode := ""
	if supersede {
		supersededOccurrence = running.ID
		supersededNode = running.NodeID
	}
	rolled, err := s.instances.RollbackInstance(ctx, repository.RollbackUpdate{
		InstanceID:           inst.ID,
		Frame:                model.Frame{CurrentNodeID: target.ID, GroupStack: stack},
		Context:              restoreCtx,
		Actor:                s.requestActor(req.Principal, req.Actor),
		Reason:               req.Reason,
		FromNode:             frame.CurrentNodeID,
		ToNode:               target.ID,
		ToOccurrence:         occ.ID,
		WaitingReason:        waitingReason,
		RearmInputOccurrence: rearm,
		SupersedeRunning:     supersede,
		SupersededOccurrence: supersededOccurrence,
		SupersededNode:       supersededNode,
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
	// Lean mode supersedes the target occurrence row and everything after
	// it, then appends a post-rollback anchor. The start anchor (create or
	// UpdateContext baseline) is never superseded, so the next replay
	// starts from the restored context while older non-superseded rows
	// stay available for a second rollback to an older target.
	if leanMode(inst) && restoreCursor != nil {
		if serr := s.instances.SupersedeAfter(ctx, inst.ID, *restoreCursor); serr != nil {
			return nil, mapHistoryError(serr)
		}
		anchor := leanAnchorRow(restoreCtx)
		anchor.WorkflowInstanceID = inst.ID
		if aerr := s.instances.AppendHistory(ctx, *anchor); aerr != nil {
			return nil, aerr
		}
	}
	newFrame, err := model.ParseFrame(rolled.Frame)
	if err != nil {
		return nil, err
	}
	return &RollbackResult{Status: rolled.Status, CurrentNodeID: newFrame.CurrentNodeID, GroupStack: newFrame.GroupStack}, nil
}

// hasLiveParallel reports whether an instance has a parallel execution
// still waiting for branches or for its join. Completed, failed, and
// cancelled executions are history and never block rollback.
func (s *instanceService) hasLiveParallel(ctx context.Context, instanceID string) (bool, error) {
	exs, err := s.parallel.ListExecutions(ctx, instanceID)
	if err != nil {
		return false, err
	}
	for _, ex := range exs {
		switch ex.Status {
		case model.ParallelExecutionCompleted, model.ParallelExecutionFailed, model.ParallelExecutionCancelled:
		default:
			return true, nil
		}
	}
	return false, nil
}

// resolveRollbackContext restores the full context for a rollback target.
// Full mode restores the occurrence ContextBefore snapshot; lean mode
// resolves the target occurrence cursor over non-superseded history and
// replays from the instance create context, validating the result is a JSON
// object. The returned cursor is nil for full mode.
func (s *instanceService) resolveRollbackContext(ctx context.Context, inst *model.WorkflowInstance, occ *model.NodeInstance, targetOccurrenceID string) (json.RawMessage, *model.HistoryCursor, error) {
	if !leanMode(inst) {
		var restore map[string]any
		if err := json.Unmarshal(occ.ContextBefore, &restore); err != nil || restore == nil {
			return nil, nil, fmt.Errorf("%w: occurrence %q has no restorable context", model.ErrInvalid, targetOccurrenceID)
		}
		return occ.ContextBefore, nil, nil
	}
	rows, err := s.instances.LoadHistory(ctx, inst.ID)
	if err != nil {
		return nil, nil, err
	}
	cursor, found := leanHistoryCursorForOccurrence(rows, occ.ID, 0)
	if !found {
		return nil, nil, fmt.Errorf("%w: occurrence %q has no history", model.ErrConflict, targetOccurrenceID)
	}
	restored, err := s.leanReconstructBefore(ctx, inst, cursor)
	if err != nil {
		return nil, nil, err
	}
	if _, verr := validateRestoredObject(restored); verr != nil {
		return nil, nil, fmt.Errorf("%w: %v", model.ErrConflict, verr)
	}
	return restored, &cursor, nil
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

// contentGraph loads and materializes the executable graph of the
// definition an instance was created from, so a delivery decision reads
// the same node tree the engine executes.
func (s *instanceService) contentGraph(ctx context.Context, inst *model.WorkflowInstance) (*contentGraph, error) {
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
	return &contentGraph{wc: wc}, nil
}

// replayDelivery answers a repeated delivery. The stored row is the first
// writer's result and is never rewritten, but the caller is authorized
// against the node that delivery targeted, so a key alone does not hand a
// refused caller someone else's delivery. A mismatched payload is a
// conflict: the first delivery wins, so a corrected payload needs a fresh
// key. The target node is resolved fail-closed: a row whose node no longer
// resolves, or resolves to something that is not an input, denies rather
// than returning an unauthorized stored result.
func (s *instanceService) replayDelivery(
	ctx context.Context,
	inst *model.WorkflowInstance,
	graph *contentGraph,
	existing *model.InputDelivery,
	req DeliverInput,
	p auth.Principal,
) (*model.InputDelivery, error) {
	if !jsonEqual(existing.Payload, req.Payload) {
		return nil, fmt.Errorf("%w: Idempotency-Key %q was already used with a different payload", model.ErrConflict, req.IdempotencyKey)
	}
	// The row records the occurrence it targeted, so the gate reads the
	// node the delivery actually addressed even when the instance has
	// since advanced past it or finished.
	occ, err := s.instances.GetNodeInstance(ctx, inst.ID, existing.NodeInstanceID)
	if err != nil {
		return nil, fmt.Errorf("%w: delivery %s targets an unresolvable node instance", model.ErrForbidden, existing.ID)
	}
	if occ.BranchID != req.BranchID {
		return nil, fmt.Errorf("%w: Idempotency-Key %q was already used for a different branch", model.ErrConflict, req.IdempotencyKey)
	}
	nc, err := graph.Node(occ.NodeID)
	if err != nil {
		return nil, fmt.Errorf("%w: delivery %s targets an unknown node %q", model.ErrForbidden, existing.ID, occ.NodeID)
	}
	if nc.Type != model.NodeTypeInput {
		return nil, fmt.Errorf("%w: delivery %s targets node %q, which is not an input node", model.ErrForbidden, existing.ID, occ.NodeID)
	}
	// A replay re-authorizes whoever is asking now, not whoever wrote the
	// row: an anonymous caller replays only a public node's delivery, and
	// an authenticated caller on a public node still passes the endpoint
	// permission like any other delivery.
	if err := s.authorizeDelivery(ctx, inst, nc, p, isAnonymousDelivery(req, p)); err != nil {
		return nil, err
	}
	return existing, nil
}

// inputTarget is the resolved cursor of an input delivery: the instance
// cursor for parent deliveries, a branch cursor for branch deliveries.
// Resolution loads the cursor and node ahead of authorization (the gate
// needs the node); parked-state assertions stay behind the channel match,
// where they were, so error precedence is unchanged.
type inputTarget struct {
	frame  model.Frame
	node   *model.NodeContent
	branch *model.ParallelBranch
	// branchID is "" for the parent scope.
	branchID string
	attempt  *model.NodeInstance
	// startCtx is the pre-delivery context the lean history diff starts
	// from on post failures.
	startCtx map[string]any
}

func (s *instanceService) resolveInputTarget(ctx context.Context, inst *model.WorkflowInstance, graph *contentGraph, req DeliverInput) (*inputTarget, error) {
	if req.BranchID == "" {
		frame, err := model.ParseFrame(inst.Frame)
		if err != nil {
			return nil, err
		}
		node, err := graph.Node(frame.CurrentNodeID)
		if err != nil {
			return nil, err
		}
		if node.Type != model.NodeTypeInput {
			return nil, fmt.Errorf("%w: current node is not an input node", model.ErrConflict)
		}
		return &inputTarget{frame: frame, node: node}, nil
	}
	branch, err := s.parallel.GetBranch(ctx, req.BranchID)
	if err != nil {
		if errors.Is(err, repository.ErrParallelBranchNotFound) {
			return nil, fmt.Errorf("%w: branch %s not found", model.ErrNotFound, req.BranchID)
		}
		return nil, err
	}
	if branch.InstanceID != inst.ID {
		return nil, fmt.Errorf("%w: branch %s does not belong to instance %s", model.ErrConflict, req.BranchID, req.InstanceID)
	}
	frame, err := model.ParseFrame(branch.Frame)
	if err != nil {
		return nil, err
	}
	node, err := graph.Node(frame.CurrentNodeID)
	if err != nil {
		return nil, err
	}
	if node.Type != model.NodeTypeInput {
		return nil, fmt.Errorf("%w: current node is not an input node", model.ErrConflict)
	}
	return &inputTarget{frame: frame, node: node, branch: branch, branchID: branch.ID}, nil
}

// assertInputParked verifies the target is parked on its input node and
// returns the live attempt with the target context. The repository
// re-validates under lock; these checks only shape the error.
func (s *instanceService) assertInputParked(ctx context.Context, inst *model.WorkflowInstance, tgt *inputTarget, req DeliverInput) (*model.NodeInstance, map[string]any, error) {
	if tgt.branchID == "" {
		attempt, err := s.instances.GetLiveNodeInstanceByNode(ctx, inst.ID, tgt.node.ID)
		if err != nil {
			if errors.Is(err, repository.ErrNodeInstanceNotFound) {
				return nil, nil, fmt.Errorf("%w: input node is not waiting for input", model.ErrConflict)
			}
			return nil, nil, err
		}
		if inst.Status != model.WorkflowWaiting || inst.WaitingReason != model.WaitingReasonInput {
			return nil, nil, fmt.Errorf("%w: instance %s is not waiting for input", model.ErrConflict, req.InstanceID)
		}
		ctxMap, err := unmarshalJSON(inst.Context)
		if err != nil {
			return nil, nil, err
		}
		return attempt, ctxMap, nil
	}
	// Branch deliveries require the parent parked on its join, waiting
	// or paused (debug step-through and manual pauses hold the parent
	// paused while its branches run). A running parent must not have its
	// branches advanced underneath it.
	if inst.WaitingReason != model.WaitingReasonParallel ||
		(inst.Status != model.WorkflowWaiting && inst.Status != model.WorkflowPaused) {
		return nil, nil, fmt.Errorf("%w: instance %s is not parked on a parallel join", model.ErrConflict, req.InstanceID)
	}
	if tgt.branch.Status != model.ParallelBranchWaiting || tgt.branch.WaitingReason != model.WaitingReasonInput {
		return nil, nil, fmt.Errorf("%w: branch %s is not waiting for input", model.ErrConflict, tgt.branchID)
	}
	attempt, err := s.instances.GetLiveBranchNodeInstanceByNode(ctx, tgt.branchID, tgt.node.ID)
	if err != nil {
		if errors.Is(err, repository.ErrNodeInstanceNotFound) {
			return nil, nil, fmt.Errorf("%w: input node is not waiting for input", model.ErrConflict)
		}
		return nil, nil, err
	}
	ctxMap, err := unmarshalJSON(tgt.branch.Context)
	if err != nil {
		return nil, nil, err
	}
	return attempt, ctxMap, nil
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

	principal := s.deliveryPrincipal(req)
	actor := s.deliveryActor(req, principal)
	anonymous := isAnonymousDelivery(req, principal)
	if err := authorizeInstance(inst, principal); err != nil {
		return nil, err
	}

	// Idempotent replay: an already recorded delivery for this key returns
	// the stored result. Reusing a key with a different payload is a client
	// error (409): the first delivery wins, so a corrected payload needs a
	// fresh key. The lookup comes first because a finished workflow no
	// longer has a cursor: the replay is authorized against the node its
	// own delivery targeted, which the row records.
	existing, err := s.instances.GetDeliveryByKey(ctx, inst.ID, req.IdempotencyKey)
	switch {
	case err == nil:
		graph, gerr := s.contentGraph(ctx, inst)
		if gerr != nil {
			return nil, gerr
		}
		return s.replayDelivery(ctx, inst, graph, existing, req, principal)
	case !errors.Is(err, repository.ErrDeliveryNotFound):
		return nil, err
	}

	// A terminal instance is refused once the key is known to be fresh, so a
	// retry of the delivery that finished the workflow still replays its
	// stored result.
	switch inst.Status {
	case model.WorkflowFinished, model.WorkflowFailed, model.WorkflowStopped:
		return nil, fmt.Errorf("%w: instance %s is terminal", model.ErrConflict, req.InstanceID)
	}

	graph, err := s.contentGraph(ctx, inst)
	if err != nil {
		return nil, err
	}
	tgt, err := s.resolveInputTarget(ctx, inst, graph, req)
	if err != nil {
		return nil, err
	}
	frame, inputNode := tgt.frame, tgt.node
	// Authorization is decided here, once the node the delivery targets is
	// known, and before the channel match, so a source header cannot skip
	// the gate. The two gates are independent: a caller may hold
	// input:deliver and still be refused by a node that does not list one
	// of its roles. An anonymous caller passes only on a public node and
	// is marked anonymous in the audit trail; an invalid credential never
	// degrades to anonymous, because the router authenticates first.
	if err := s.authorizeDelivery(ctx, inst, inputNode, principal, anonymous); err != nil {
		return nil, err
	}
	source := req.Source
	if source == "" {
		source = model.InputChannelHTTP
	}
	if inputNode.Channel != source {
		return nil, fmt.Errorf("%w: input source %q does not match input node channel %q", model.ErrConflict, source, inputNode.Channel)
	}
	attempt, ctxMap, err := s.assertInputParked(ctx, inst, tgt, req)
	if err != nil {
		return nil, err
	}
	tgt.attempt = attempt
	tgt.startCtx = ctxMap
	// Schema-first enforcement: the form schema rejects bad payloads before
	// the validation script runs. Rejection persists Accepted=false, same as
	// a script rejection; the script never runs after a schema failure.
	// Anonymous deliveries record the anonymous marker alongside any
	// rejection, so the audit trail still distinguishes them.
	if inputNode.Form != nil {
		if err := form.Validate(inputNode, req.Payload); err != nil {
			return s.instances.DeliverInput(ctx, repository.InputCompletion{
				InstanceID:     inst.ID,
				NodeInstanceID: attempt.ID,
				IdempotencyKey: req.IdempotencyKey,
				Payload:        req.Payload,
				Accepted:       false,
				Error:          err.Error(),
				CreatedBy:      actor,
				Anonymous:      anonymous,
			})
		}
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
			CreatedBy:      actor,
			Anonymous:      anonymous,
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
	key := strings.TrimSpace(inputNode.OutputProperty)
	if key == "" {
		key = inputNode.ID
	}
	// The form schema, the validation script, and the post hook's frozen
	// output global all see the raw payload. Only the context merge
	// changes, so record_actor cannot be used to smuggle a different shape
	// past validation.
	newCtx[key] = inputContextValue(inputNode, principal, actor, payload)

	// The post hook sees the accepted payload as a frozen output global and
	// may transform the context after the payload was written.
	postCtx, err := s.hooks.RunPost(ctx, inputNode, newCtx, payload)
	if err != nil {
		return s.failAcceptedInput(ctx, inst, tgt, req, model.NodeFailed, newCtx, err)
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
			// fails the scope with the latest completed context.
			return s.failAcceptedInput(ctx, inst, tgt, req, model.NodeFinished, finalCtx, herr)
		}
		newCtx = finalCtx
	}
	c := repository.InputCompletion{
		InstanceID:     inst.ID,
		BranchID:       tgt.branchID,
		NodeInstanceID: attempt.ID,
		IdempotencyKey: req.IdempotencyKey,
		Payload:        req.Payload,
		Accepted:       true,
		NewFrame:       frame,
		NewContext:     mustMarshal(newCtx),
		CreatedBy:      actor,
		Anonymous:      anonymous,
	}
	if tgt.branchID != "" {
		if done {
			// Unreachable on validated graphs: every branch path
			// reaches the join. Fail the branch loudly instead of
			// dropping it.
			return s.failAcceptedInput(ctx, inst, tgt, req, model.NodeFinished, newCtx, errors.New("branch terminated without reaching its parallel_end"))
		}
		return s.instances.DeliverInput(ctx, c)
	}
	status := model.WorkflowWaiting
	var finished *time.Time
	if done {
		status = model.WorkflowFinished
		t := nowUTC()
		finished = &t
	} else if inst.Debug {
		// Step-through: a delivery that advances the cursor parks
		// paused, not runnable, so the next node waits for a resume.
		// A delivery that finishes the workflow stays terminal.
		status = model.WorkflowPaused
	}
	c.Status = status
	c.FinishedAt = finished
	c.History = s.leanInputHistory(inst, ctxMap, newCtx, attempt)
	return s.instances.DeliverInput(ctx, c)
}

// inputContextValue is what an accepted delivery writes under the node's
// output property. Without record_actor that is the bare payload, exactly
// as before the field existed. With it, the value is the attribution
// envelope, which carries the deliverer alongside the payload and nothing
// else, so a template reads {{ key.input_data.x }} and the acting user
// {{ key.user_id }}.
func inputContextValue(nc *model.NodeContent, p auth.Principal, actor string, payload any) any {
	if nc == nil || !nc.RecordActor {
		return payload
	}
	userID := actor
	if userID == "" {
		userID = p.UserID
	}
	return map[string]any{
		"user_id":    userID,
		"input_data": payload,
	}
}

// deliveryPrincipal resolves the caller of a delivery. A request that
// carries no principal is the service principal: the broker consumers and
// any internal caller carry no credential, and they are exactly the paths
// the bypass matrix describes. An anonymous HTTP delivery (Anonymous with
// no valid principal) is not the service principal: it skips only the
// public-node path, and the actor resolution marks it instead of
// attributing it to the system user.
func (s *instanceService) deliveryPrincipal(req DeliverInput) auth.Principal {
	if req.Principal != nil {
		return *req.Principal
	}
	return auth.SystemPrincipal(s.actor)
}

// isAnonymousDelivery reports whether the request is an unauthenticated
// HTTP delivery rather than a trusted internal one. The flag travels on
// the request so a present-but-invalid credential is never confused with
// no credential: only the router sets it, and only when the route's auth
// is optional and the request carried nothing to check. A nil principal
// resolves to the service principal with the configured system user, so
// the check reads the flag rather than the resolved identity.
func isAnonymousDelivery(req DeliverInput, p auth.Principal) bool {
	return req.Anonymous && req.Principal == nil && req.Actor == ""
}

// deliveryActor picks the users.id uuid recorded on the delivery. An
// explicit Actor wins, then the resolved principal's user id, then the
// service default, so a delivery is never written without an actor.
func (s *instanceService) deliveryActor(req DeliverInput, p auth.Principal) string {
	if req.Actor != "" {
		return req.Actor
	}
	if p.UserID != "" {
		return p.UserID
	}
	return s.actor
}

// authorizeDelivery applies the two independent input gates. A service
// principal passes both: it is a trusted internal caller with the wildcard
// permission and no roles to intersect. An anonymous delivery passes only
// on a public node, which is open and unattributed by construction. A
// denial writes no delivery row and records an input_forbidden audit event
// carrying the instance, the node, and the refused roles, never the
// payload.
func (s *instanceService) authorizeDelivery(
	ctx context.Context,
	inst *model.WorkflowInstance,
	inputNode *model.NodeContent,
	p auth.Principal,
	anonymous bool,
) error {
	// An anonymous delivery is the broker-shaped principal with the
	// anonymous flag: it passes only on a public node and never touches
	// the endpoint permission or the role gate, which need a real caller.
	// A present-but-invalid credential never sets the flag, so it cannot
	// degrade to anonymous here.
	if anonymous {
		if inputNode.Public {
			return nil
		}
		return s.denyAnonymousInput(ctx, inst, inputNode, p, "anonymous delivery requires a public input node")
	}
	if p.Service {
		return nil
	}
	if !p.HasPermission(auth.ActionInputDeliver, s.catalog) {
		return s.denyInput(ctx, inst, inputNode, p, "the "+auth.ActionInputDeliver+" permission is required")
	}
	if !p.CanSatisfyRoles(inputNode.AllowedRoles) {
		return s.denyInput(ctx, inst, inputNode, p,
			"roles ["+strings.Join(inputNode.AllowedRoles, " ")+"] are required to deliver to this input node")
	}
	return nil
}

// denyInput records the refusal and returns the 403-mapped error. The event
// names the instance, the node, and the roles the caller actually held, so
// an operator can tell a wrong role from a missing permission. An anonymous
// refusal carries the anonymous marker alongside the empty caller identity,
// so it reads as "nobody knocked" rather than as the system user. It never
// carries the payload: a refused delivery is not data the engine stored.
func (s *instanceService) denyInput(
	ctx context.Context,
	inst *model.WorkflowInstance,
	inputNode *model.NodeContent,
	p auth.Principal,
	reason string,
) error {
	return s.denyInputAs(ctx, inst, inputNode, p, reason, false)
}

// denyAnonymousInput records an anonymous refusal the same way denyInput
// does, except the marker names the caller anonymous: the resolved
// principal is the broker-shaped service principal, which would otherwise
// mislabel the event as a service-principal refusal.
func (s *instanceService) denyAnonymousInput(
	ctx context.Context,
	inst *model.WorkflowInstance,
	inputNode *model.NodeContent,
	p auth.Principal,
	reason string,
) error {
	return s.denyInputAs(ctx, inst, inputNode, p, reason, true)
}

func (s *instanceService) denyInputAs(
	ctx context.Context,
	inst *model.WorkflowInstance,
	inputNode *model.NodeContent,
	p auth.Principal,
	reason string,
	anonymous bool,
) error {
	actor := p.UserID
	if actor == "" {
		actor = s.actor
	}
	roles := p.Roles
	if roles == nil {
		roles = []string{}
	}
	actorType := deliveryActorType(p)
	if anonymous {
		actorType = "anonymous"
	}
	data, err := json.Marshal(map[string]any{
		"node_id":      inputNode.ID,
		"channel":      inputNode.Channel,
		"reason":       reason,
		"roles":        roles,
		"subject":      p.Subject,
		"delivered_by": actorType,
	})
	if err != nil {
		data = json.RawMessage(`{}`)
	}
	_ = s.instances.AppendEvent(ctx, model.WorkflowInstanceEvent{
		ID: mustNewID(), WorkflowInstanceID: inst.ID, Type: "input_forbidden",
		Data: data, CreatedBy: actor, CreatedAt: nowUTC(),
	})
	return fmt.Errorf("%w: %s", model.ErrForbidden, reason)
}

// deliveryActorType names the kind of caller a delivery event records. The
// audit trail reads "who" from created_by and "what kind of who" from this:
// the service principal is the operator's machine identity, a user is a
// resolved human, and anonymous is a caller that proved nothing on a public
// node. A zero principal only reaches here for an unauthenticated
// deployment, where the pre-authentication behavior is preserved.
func deliveryActorType(p auth.Principal) string {
	if p.Service {
		return "service"
	}
	if p.UserID != "" || p.Subject != "" {
		return "user"
	}
	return "anonymous"
}

// leanInputHistory computes the delivery diff in the service (DiffMaps of
// the pre-delivery request context to the final context) so the repository
// persists history atomically with delivery without duplicating diff logic.
func (s *instanceService) leanInputHistory(inst *model.WorkflowInstance, before, after map[string]any, attempt *model.NodeInstance) *model.NodeContextHistory {
	if !leanMode(inst) {
		return nil
	}
	diff, err := contextdiff.DiffMaps(before, after)
	if err != nil {
		return &model.NodeContextHistory{
			WorkflowInstanceID: inst.ID,
			OccurrenceID:       attempt.ID,
			NodeID:             attempt.NodeID,
			Attempt:            attempt.Attempt,
			Diff:               json.RawMessage(`{"set":{},"unset":[]}`),
		}
	}
	return &model.NodeContextHistory{
		WorkflowInstanceID: inst.ID,
		OccurrenceID:       attempt.ID,
		NodeID:             attempt.NodeID,
		Attempt:            attempt.Attempt,
		Diff:               diff.JSON(),
	}
}

// failAcceptedInput records an accepted input delivery whose post processing
// failed: the delivery stays accepted (the API returns 202), the node
// attempt takes nodeStatus, and the workflow fails with the merged context.
func (s *instanceService) failAcceptedInput(ctx context.Context, inst *model.WorkflowInstance, tgt *inputTarget, req DeliverInput, nodeStatus model.NodeStatus, ctxMap map[string]any, cause error) (*model.InputDelivery, error) {
	c := repository.InputCompletion{
		InstanceID:     inst.ID,
		BranchID:       tgt.branchID,
		NodeInstanceID: tgt.attempt.ID,
		IdempotencyKey: req.IdempotencyKey,
		Payload:        req.Payload,
		Accepted:       true,
		PostFailure:    true,
		NodeStatus:     nodeStatus,
		NewContext:     mustMarshal(ctxMap),
		Error:          cause.Error(),
		CreatedBy:      s.deliveryActor(req, s.deliveryPrincipal(req)),
	}
	if tgt.branchID == "" {
		// Branches run full-context snapshots; their diffs never join
		// the instance history the parent replays.
		c.History = s.leanInputHistory(inst, tgt.startCtx, ctxMap, tgt.attempt)
	}
	return s.instances.DeliverInput(ctx, c)
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

// jsonEqual compares two JSON payloads semantically: whitespace and key
// order do not count as a difference. Either side failing to parse falls
// back to a trimmed byte comparison.
func jsonEqual(a, b json.RawMessage) bool {
	var va, vb any
	if err := json.Unmarshal(a, &va); err != nil {
		return bytes.Equal(bytes.TrimSpace(a), bytes.TrimSpace(b))
	}
	if err := json.Unmarshal(b, &vb); err != nil {
		return bytes.Equal(bytes.TrimSpace(a), bytes.TrimSpace(b))
	}
	return reflect.DeepEqual(va, vb)
}
