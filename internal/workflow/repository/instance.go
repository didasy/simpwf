package repository

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/simpwf/workflow-engine/internal/workflow/model"
	"github.com/simpwf/workflow-engine/pkg/ids"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// Repository errors for the instance state machine. Fenced writes fail with
// ErrLeaseLost so a worker whose lease was stopped or taken over aborts its
// work instead of continuing to commit.
var (
	ErrInstanceNotFound     = errors.New("repository: instance not found")
	ErrLeaseLost            = errors.New("repository: lease lost")
	ErrRevisionConflict     = errors.New("repository: revision conflict")
	ErrStatusConflict       = errors.New("repository: status conflict")
	ErrNodeInstanceNotFound = errors.New("repository: node instance not found")
	ErrDeliveryNotFound     = errors.New("repository: input delivery not found")
)

// Checkpoint is the fenced write that commits one node transition: the new
// cursor (frame), counters, full context snapshot, and next status.
type Checkpoint struct {
	InstanceID string
	WorkerID   string
	Revision   int64
	// WorkflowDefinitionID and the From* fields feed the status-update
	// outbox, which commits atomically with the checkpoint.
	WorkflowDefinitionID string
	FromStatus           model.WorkflowStatus
	FromWaitingReason    model.WaitingReason
	Status               model.WorkflowStatus
	WaitingReason        model.WaitingReason
	PauseRequested       bool
	Frame                model.Frame
	Counters             model.Counters
	Context              json.RawMessage
	Error                string
	FinishedAt           *time.Time
}

// InputCompletion is an atomic input delivery: the delivery row plus, when
// accepted, the completed node attempt and the advanced instance cursor.
type InputCompletion struct {
	InstanceID     string
	NodeInstanceID string
	IdempotencyKey string
	Payload        json.RawMessage
	Accepted       bool
	Error          string
	CreatedBy      string
	// PostFailure marks an accepted delivery whose post processing failed:
	// the delivery is recorded accepted, the node attempt takes NodeStatus
	// (finished when a structural group hook failed, failed when the input
	// post hook itself failed), and the workflow fails with the merged
	// context instead of advancing the cursor.
	PostFailure bool
	NodeStatus  model.NodeStatus
	// Accepted fields below.
	NewFrame   model.Frame
	NewContext json.RawMessage
	Status     model.WorkflowStatus
	FinishedAt *time.Time
}

// ContextUpdate is an atomic replacement of a paused instance's context: the
// context column, revision, and a context_updated audit event change together
// under a status = paused guard.
type ContextUpdate struct {
	InstanceID string
	Context    json.RawMessage
	Actor      string
	Reason     string
}

// RollbackUpdate moves a paused or failed instance's cursor back to an
// already-executed node occurrence. Frame carries the recomputed cursor
// (target graph node + group stack) and Context carries the target
// occurrence's ContextBefore, both resolved by the caller. WaitingReason is
// the post-rollback park reason: runnable, or input when the target is an
// input node so the instance keeps waiting for its delivery.
// RearmInputOccurrence, set only when the target is a finished input
// occurrence of this instance, re-arms it as a fresh attempt of the same
// row (delivery history stays attached for audit) so the next resume
// re-parks it. SupersedeRunning closes any other live attempt of the
// instance (status stopped, cancelled, error "superseded by rollback") in
// the same transaction so the cursor and the park never diverge;
// SupersededOccurrence/SupersededNode record the closed attempt on the
// rollback audit event. Reason is an audit annotation recorded on the
// rollback event only, never context values.
type RollbackUpdate struct {
	InstanceID           string
	Frame                model.Frame
	Context              json.RawMessage
	Actor                string
	Reason               string
	FromNode             string
	ToNode               string
	ToOccurrence         string
	WaitingReason        model.WaitingReason
	RearmInputOccurrence bool
	SupersedeRunning     bool
	SupersededOccurrence string
	SupersededNode       string
}

var errDeliveryExists = errors.New("repository: input delivery already exists")

func newRepoID() string {
	id, err := ids.NewString()
	if err != nil {
		panic(err)
	}
	return id
}

// InstanceRepository is the durable state store for workflow instances.
type InstanceRepository interface {
	// Insert creates a new instance (terminal states are not enforced here).
	Insert(ctx context.Context, w model.WorkflowInstance) error
	// GetByID loads one instance.
	GetByID(ctx context.Context, id string) (*model.WorkflowInstance, error)
	// List returns the instances matching the query with the requested
	// ordering (an allowlisted field plus an id tie-breaker) and offsets
	// the page by Page/PerPage. Total is the count of matches before
	// pagination.
	List(ctx context.Context, q InstanceListQuery) ([]model.WorkflowInstance, int64, error)
	// ReplaceContext atomically replaces the context of a paused instance,
	// increments its revision, and appends a context_updated audit event
	// carrying the actor and optional reason (never the context values).
	// Returns ErrInstanceNotFound when the instance is missing and
	// ErrStatusConflict when it is not paused.
	ReplaceContext(ctx context.Context, u ContextUpdate) (*model.WorkflowInstance, error)
	// RollbackInstance atomically moves a paused or failed instance's cursor
	// back to an already-executed node occurrence: status becomes paused
	// (failed instances clear their error and finished_at; started_at is
	// kept), waiting_reason becomes the caller-supplied park reason
	// (runnable, or input when the target is an input node), pause_requested
	// clears, the frame and context are replaced, the revision increments,
	// and a rollback audit event is appended. With SupersedeRunning set, any
	// other live attempt is closed (stopped, cancelled, error "superseded by
	// rollback") in the same transaction so the cursor and the park never
	// diverge; the closed attempt's ids ride on the rollback event.
	// Rolling back to a finished
	// input occurrence additionally re-arms it to running (delivery history
	// stays attached for audit) so the next resume re-parks it; other
	// occurrences are untouched. No status-update outbox rows are enqueued.
	// The failed -> paused move is the explicit rollback-only exception to
	// model.CanWorkflowTransition, which stays unchanged for generic paths.
	// Returns ErrInstanceNotFound when the instance is missing and
	// ErrStatusConflict when it is not paused/failed, has termination
	// pending, or lost a concurrent race.
	RollbackInstance(ctx context.Context, u RollbackUpdate) (*model.WorkflowInstance, error)
	// ClaimNext leases up to limit runnable instances (waiting with no
	// active lease, or running with an expired lease) using FOR UPDATE SKIP
	// LOCKED, moving them to running and fencing them to workerID.
	ClaimNext(ctx context.Context, workerID string, lease time.Duration, limit int) ([]model.WorkflowInstance, error)
	// Checkpoint commits a transition. It succeeds only while the worker
	// still holds the lease at the expected revision; otherwise it returns
	// ErrLeaseLost or ErrRevisionConflict.
	Checkpoint(ctx context.Context, c Checkpoint) error
	// RenewLease extends the lease; ErrLeaseLost when the lease was lost.
	RenewLease(ctx context.Context, instanceID, workerID string, lease time.Duration) error
	// RenewLeases extends every lease held by a worker (heartbeat).
	RenewLeases(ctx context.Context, workerID string, lease time.Duration) error
	// Pause pauses immediately when waiting (returning deferred=false) or
	// sets pause_requested when running (deferred=true). Idempotent while
	// paused; ErrStatusConflict on terminal states.
	Pause(ctx context.Context, id string) (deferred bool, err error)
	// Resume returns a paused instance to waiting (preserving the waiting
	// reason) and clears a pending pause on a running instance.
	Resume(ctx context.Context, id string) error
	// Stop moves waiting/running/paused instances to stopped, clears the
	// lease (fencing in-flight workers), and is idempotent. pending reports
	// whether the instance was running at stop time, meaning the active
	// node attempt still needs cancellation and cleanup.
	Stop(ctx context.Context, id, reason string) (pending bool, err error)
	// StopRunningAttempts marks every in-flight node attempt of an instance
	// stopped and cancelled. Used when stopping an instance that was parked
	// on an input node.
	StopRunningAttempts(ctx context.Context, instanceID string) error
	// ListTerminationPending returns instance ids whose stop requested
	// in-flight cancellation.
	ListTerminationPending(ctx context.Context) ([]string, error)
	// ResolveTermination clears the termination-pending flag and releases
	// the instance lease. Idempotent.
	ResolveTermination(ctx context.Context, instanceID string) error
	// SweepTermination clears termination-pending flags of stopped instances
	// whose node attempts are no longer running (their cleanup already
	// completed or there was nothing to cancel).
	SweepTermination(ctx context.Context) error
	// DeliverInput atomically records an input delivery and, when accepted,
	// completes the input node attempt and advances the waiting instance.
	// Replaying an idempotency key returns the previously recorded delivery.
	DeliverInput(ctx context.Context, c InputCompletion) (*model.InputDelivery, error)
	// GetDeliveryByKey returns the recorded delivery for an instance and key,
	// or ErrDeliveryNotFound. Replays are resolved here so delivery
	// idempotency works regardless of the current instance state.
	GetDeliveryByKey(ctx context.Context, workflowInstanceID, idempotencyKey string) (*model.InputDelivery, error)

	// Node attempt persistence.
	InsertNodeInstance(ctx context.Context, n model.NodeInstance) error
	UpdateNodeInstance(ctx context.Context, n model.NodeInstance) error
	GetNodeInstance(ctx context.Context, workflowInstanceID, occurrenceID string) (*model.NodeInstance, error)
	// GetNodeInstanceByNode returns the occurrence of a workflow graph node
	// within an instance, or ErrNodeInstanceNotFound. Multiple rows per
	// node exist after loop iterations and rollback re-parks; callers that
	// need the latest row must filter ListNodeInstances instead.
	GetNodeInstanceByNode(ctx context.Context, workflowInstanceID, nodeID string) (*model.NodeInstance, error)
	// GetLiveNodeInstanceByNode returns the newest live (running) occurrence
	// of a workflow graph node, or ErrNodeInstanceNotFound. Used by input
	// delivery so a superseded (stopped) park is never resurrected.
	GetLiveNodeInstanceByNode(ctx context.Context, workflowInstanceID, nodeID string) (*model.NodeInstance, error)
	// GetRunningNodeInstance returns the in-flight attempt of an instance,
	// or ErrNodeInstanceNotFound.
	GetRunningNodeInstance(ctx context.Context, workflowInstanceID string) (*model.NodeInstance, error)
	ListNodeInstances(ctx context.Context, workflowInstanceID string) ([]model.NodeInstance, error)

	// Audit events.
	AppendEvent(ctx context.Context, e model.WorkflowInstanceEvent) error
	ListEvents(ctx context.Context, workflowInstanceID string) ([]model.WorkflowInstanceEvent, error)
}

type instanceRepo struct{ db *gorm.DB }

// NewInstanceRepository builds the GORM-backed instance repository.
func NewInstanceRepository(db *gorm.DB) InstanceRepository { return &instanceRepo{db: db} }

func (r *instanceRepo) Insert(ctx context.Context, w model.WorkflowInstance) error {
	m := WorkflowInstanceToModel(w)
	return r.db.WithContext(ctx).Create(&m).Error
}

func (r *instanceRepo) GetByID(ctx context.Context, id string) (*model.WorkflowInstance, error) {
	var m WorkflowInstanceModel
	if err := r.db.WithContext(ctx).Where("id = ?", id).First(&m).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrInstanceNotFound
		}
		return nil, err
	}
	w := WorkflowInstanceFromModel(m)
	return &w, nil
}

func (r *instanceRepo) List(ctx context.Context, q InstanceListQuery) ([]model.WorkflowInstance, int64, error) {
	query := r.db.WithContext(ctx).Model(&WorkflowInstanceModel{})
	if len(q.IDs) > 0 {
		query = query.Where("id IN ?", q.IDs)
	}
	if q.WorkflowDefinitionID != "" {
		query = query.Where("workflow_definition_id = ?", q.WorkflowDefinitionID)
	}
	if len(q.Statuses) > 0 {
		query = query.Where("status IN ?", q.Statuses)
	}

	var total int64
	if err := query.Count(&total).Error; err != nil {
		return nil, 0, fmt.Errorf("workflow instance count: %w", err)
	}

	order := instanceOrderSQL(q.Order)
	if order == "" {
		order = `"created_at" DESC`
	}
	page := q.Page
	if page < 1 {
		page = 1
	}
	perPage := q.PerPage
	if perPage < 1 {
		perPage = 50
	}

	var rows []WorkflowInstanceModel
	if err := query.Select(`"id", "workflow_definition_id", "status", "waiting_reason", "pause_requested", "termination_pending", "error", "started_at", "finished_at", "created_by", "updated_by", "created_at", "updated_at"`).
		Order(order + `, "id" ASC`).
		Offset((page - 1) * perPage).
		Limit(perPage).
		Find(&rows).Error; err != nil {
		return nil, 0, fmt.Errorf("workflow instance list: %w", err)
	}

	items := make([]model.WorkflowInstance, 0, len(rows))
	for _, row := range rows {
		items = append(items, WorkflowInstanceFromModel(row))
	}
	return items, total, nil
}

func (r *instanceRepo) ReplaceContext(ctx context.Context, u ContextUpdate) (*model.WorkflowInstance, error) {
	now := time.Now().UTC()
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		res := tx.Model(&WorkflowInstanceModel{}).
			Where("id = ? AND status = ?", u.InstanceID, string(model.WorkflowPaused)).
			Updates(map[string]any{
				"context":    jsonCol(u.Context, "{}"),
				"revision":   gorm.Expr("revision + 1"),
				"updated_at": now,
			})
		if res.Error != nil {
			return res.Error
		}
		if res.RowsAffected == 0 {
			var m WorkflowInstanceModel
			if err := tx.Where("id = ?", u.InstanceID).First(&m).Error; err != nil {
				if errors.Is(err, gorm.ErrRecordNotFound) {
					return ErrInstanceNotFound
				}
				return err
			}
			return ErrStatusConflict
		}
		data := json.RawMessage(`{}`)
		if u.Reason != "" {
			raw, err := json.Marshal(map[string]string{"reason": u.Reason})
			if err != nil {
				return err
			}
			data = raw
		}
		ev := WorkflowInstanceEventToModel(model.WorkflowInstanceEvent{
			ID:                 newRepoID(),
			WorkflowInstanceID: u.InstanceID,
			Type:               "context_updated",
			Data:               data,
			CreatedBy:          u.Actor,
			CreatedAt:          now,
		})
		return tx.Create(&ev).Error
	})
	if err != nil {
		return nil, err
	}
	return r.GetByID(ctx, u.InstanceID)
}

func (r *instanceRepo) RollbackInstance(ctx context.Context, u RollbackUpdate) (*model.WorkflowInstance, error) {
	frameRaw, err := u.Frame.JSON()
	if err != nil {
		return nil, err
	}
	now := time.Now().UTC()
	err = r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		res := tx.Model(&WorkflowInstanceModel{}).
			Where("id = ? AND status IN ? AND termination_pending = ?",
				u.InstanceID,
				[]string{string(model.WorkflowPaused), string(model.WorkflowFailed)},
				false).
			Updates(map[string]any{
				"status":          string(model.WorkflowPaused),
				"waiting_reason":  string(u.WaitingReason),
				"pause_requested": false,
				"frame":           frameRaw,
				"context":         jsonCol(u.Context, "{}"),
				"error":           "",
				"finished_at":     nil,
				"revision":        gorm.Expr("revision + 1"),
				"updated_at":      now,
			})
		if res.Error != nil {
			return res.Error
		}
		if res.RowsAffected == 0 {
			var m WorkflowInstanceModel
			if err := tx.Where("id = ?", u.InstanceID).First(&m).Error; err != nil {
				if errors.Is(err, gorm.ErrRecordNotFound) {
					return ErrInstanceNotFound
				}
				return err
			}
			return ErrStatusConflict
		}
		if u.SupersedeRunning {
			// Close the superseded live park so the moved cursor and the
			// park never diverge: running -> stopped + cancelled with the
			// error naming the rollback. A stale late delivery to it fails
			// its running guard. The re-armed target (finished at txn
			// start) is excluded so its own rollback never closes it.
			res := tx.Model(&NodeInstanceModel{}).
				Where("workflow_instance_id = ? AND status = ? AND id != ?",
					u.InstanceID, string(model.NodeRunning), u.ToOccurrence).
				Updates(map[string]any{
					"status":     string(model.NodeStopped),
					"cancelled":  true,
					"error":      "superseded by rollback",
					"stopped_at": now,
					"updated_at": now,
				})
			if res.Error != nil {
				return res.Error
			}
		}
		if u.RearmInputOccurrence {
			// Rolling back to a finished input occurrence re-arms its
			// wait as a fresh attempt of the same row (delivery history
			// stays attached for audit) so the next resume re-parks it
			// and a fresh delivery is accepted. The update is guarded on
			// finished so a concurrent delivery wins. It runs after the
			// supersede close (which only touches running rows), so the
			// re-armed target is never closed by its own rollback.
			res := tx.Model(&NodeInstanceModel{}).
				Where("id = ? AND workflow_instance_id = ? AND status = ? AND type = ?",
					u.ToOccurrence, u.InstanceID, string(model.NodeFinished), string(model.NodeTypeInput)).
				Updates(map[string]any{
					"status":        string(model.NodeRunning),
					"attempt":       gorm.Expr("attempt + 1"),
					"output":        jsonCol(nil, "null"),
					"context_after": jsonCol(nil, "null"),
					"error":         "",
					"finished_at":   nil,
					"started_at":    now,
					"updated_at":    now,
				})
			if res.Error != nil {
				return res.Error
			}
		}
		data := map[string]string{
			"from_node":     u.FromNode,
			"to_node":       u.ToNode,
			"to_occurrence": u.ToOccurrence,
			"context_mode":  "restore",
			"reason":        u.Reason,
		}
		if u.SupersededOccurrence != "" {
			data["superseded_occurrence"] = u.SupersededOccurrence
		}
		if u.SupersededNode != "" {
			data["superseded_node"] = u.SupersededNode
		}
		raw, err := json.Marshal(data)
		if err != nil {
			return err
		}
		ev := WorkflowInstanceEventToModel(model.WorkflowInstanceEvent{
			ID:                 newRepoID(),
			WorkflowInstanceID: u.InstanceID,
			Type:               "rollback",
			Data:               raw,
			CreatedBy:          u.Actor,
			CreatedAt:          now,
		})
		return tx.Create(&ev).Error
	})
	if err != nil {
		return nil, err
	}
	return r.GetByID(ctx, u.InstanceID)
}

func (r *instanceRepo) ClaimNext(ctx context.Context, workerID string, lease time.Duration, limit int) ([]model.WorkflowInstance, error) {
	tx := r.db.WithContext(ctx).Begin()
	if tx.Error != nil {
		return nil, tx.Error
	}
	defer tx.Rollback()

	// Lock claimable rows (SKIP LOCKED) so concurrent workers never claim
	// the same instance, then fence them to this worker in the same
	// transaction.
	var models []WorkflowInstanceModel
	err := tx.Clauses(clause.Locking{Strength: "UPDATE", Options: "SKIP LOCKED"}).
		Where(`(status = ? AND waiting_reason = ? AND (lease_expiry IS NULL OR lease_expiry < now()))
		       OR (status = ? AND lease_expiry < now())`,
			string(model.WorkflowWaiting), string(model.WaitingReasonRunnable),
			string(model.WorkflowRunning)).
		Order("updated_at").
		Limit(limit).
		Find(&models).Error
	if err != nil {
		return nil, err
	}

	now := time.Now().UTC()
	expiry := now.Add(lease)
	for i := range models {
		m := &models[i]
		m.Status = string(model.WorkflowRunning)
		if m.StartedAt == nil {
			m.StartedAt = &now
		}
		m.LeasedBy = workerID
		m.LeaseExpiry = &expiry
		m.Revision++
		m.UpdatedAt = now
		if err := tx.Save(m).Error; err != nil {
			return nil, err
		}
	}
	if err := tx.Commit().Error; err != nil {
		return nil, err
	}

	out := make([]model.WorkflowInstance, len(models))
	for i := range models {
		out[i] = WorkflowInstanceFromModel(models[i])
	}
	return out, nil
}

const checkpointSQL = `
UPDATE workflow_instances
SET status = ?, waiting_reason = ?, pause_requested = ?,
    frame = ?, counters = ?, context = ?, error = ?,
    leased_by = '', lease_expiry = NULL, finished_at = ?,
    revision = revision + 1, updated_at = now()
WHERE id = ? AND revision = ? AND leased_by = ?`

func (r *instanceRepo) Checkpoint(ctx context.Context, c Checkpoint) error {
	frame, err := c.Frame.JSON()
	if err != nil {
		return err
	}
	counters, err := c.Counters.JSON()
	if err != nil {
		return err
	}
	now := time.Now().UTC()
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		res := tx.Exec(checkpointSQL,
			string(c.Status), string(c.WaitingReason), c.PauseRequested,
			frame, counters, jsonCol(c.Context, "{}"), c.Error,
			c.FinishedAt,
			c.InstanceID, c.Revision, c.WorkerID,
		)
		if res.Error != nil {
			return res.Error
		}
		if res.RowsAffected != 1 {
			return r.diagnoseFence(ctx, tx, c.InstanceID, c.WorkerID, c.Revision)
		}
		from := statusWithReason{status: c.FromStatus, waitingReason: c.FromWaitingReason}
		to := statusWithReason{status: c.Status, waitingReason: c.WaitingReason}
		return r.enqueueStatusUpdate(ctx, tx, c.InstanceID, c.WorkflowDefinitionID, c.Revision+1, from, to, transitionEvents(from, to), c.Error, now)
	})
}

func (r *instanceRepo) diagnoseFence(ctx context.Context, g *gorm.DB, id, workerID string, revision int64) error {
	var m WorkflowInstanceModel
	if err := g.WithContext(ctx).Where("id = ?", id).First(&m).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return ErrInstanceNotFound
		}
		return err
	}
	if m.LeasedBy != workerID {
		return ErrLeaseLost
	}
	if m.Revision != revision {
		return ErrRevisionConflict
	}
	return ErrLeaseLost
}

func (r *instanceRepo) RenewLease(ctx context.Context, instanceID, workerID string, lease time.Duration) error {
	res := r.db.WithContext(ctx).Model(&WorkflowInstanceModel{}).
		Where("id = ? AND leased_by = ? AND status = ?", instanceID, workerID, string(model.WorkflowRunning)).
		UpdateColumn("lease_expiry", gorm.Expr("now() + ? * interval '1 second'", int(lease.Seconds())))
	if res.Error != nil {
		return res.Error
	}
	if res.RowsAffected == 0 {
		return ErrLeaseLost
	}
	return nil
}

func (r *instanceRepo) RenewLeases(ctx context.Context, workerID string, lease time.Duration) error {
	res := r.db.WithContext(ctx).Model(&WorkflowInstanceModel{}).
		Where("leased_by = ? AND status = ?", workerID, string(model.WorkflowRunning)).
		UpdateColumn("lease_expiry", gorm.Expr("now() + ? * interval '1 second'", int(lease.Seconds())))
	return res.Error
}

func (r *instanceRepo) Pause(ctx context.Context, id string) (bool, error) {
	var deferred bool
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var m WorkflowInstanceModel
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("id = ?", id).First(&m).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return ErrStatusConflict
			}
			return err
		}
		now := time.Now().UTC()
		switch m.Status {
		case string(model.WorkflowWaiting):
			// Immediate: waiting (runnable or input-waiting) -> paused.
			if err := tx.Model(&WorkflowInstanceModel{}).Where("id = ?", id).Updates(map[string]any{
				"status":          string(model.WorkflowPaused),
				"pause_requested": false,
				"revision":        gorm.Expr("revision + 1"),
				"updated_at":      now,
			}).Error; err != nil {
				return err
			}
			from := statusWithReason{status: model.WorkflowWaiting, waitingReason: model.WaitingReason(m.WaitingReason)}
			to := statusWithReason{status: model.WorkflowPaused, waitingReason: model.WaitingReason(m.WaitingReason)}
			return r.enqueueStatusUpdate(ctx, tx, id, m.WorkflowDefinitionID, m.Revision+1, from, to, transitionEvents(from, to), "", now)
		case string(model.WorkflowRunning):
			// Deferred: running -> pause_requested (no status change).
			deferred = true
			return tx.Model(&WorkflowInstanceModel{}).Where("id = ?", id).Updates(map[string]any{
				"pause_requested": true,
				"revision":        gorm.Expr("revision + 1"),
				"updated_at":      now,
			}).Error
		case string(model.WorkflowPaused):
			// Already paused: idempotent.
			return nil
		default:
			return ErrStatusConflict
		}
	})
	return deferred, err
}

func (r *instanceRepo) Resume(ctx context.Context, id string) error {
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var m WorkflowInstanceModel
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("id = ?", id).First(&m).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return ErrStatusConflict
			}
			return err
		}
		now := time.Now().UTC()
		switch m.Status {
		case string(model.WorkflowPaused):
			if err := tx.Model(&WorkflowInstanceModel{}).Where("id = ?", id).Updates(map[string]any{
				"status":          string(model.WorkflowWaiting),
				"pause_requested": false,
				"revision":        gorm.Expr("revision + 1"),
				"updated_at":      now,
			}).Error; err != nil {
				return err
			}
			from := statusWithReason{status: model.WorkflowPaused, waitingReason: model.WaitingReason(m.WaitingReason)}
			to := statusWithReason{status: model.WorkflowWaiting, waitingReason: model.WaitingReason(m.WaitingReason)}
			return r.enqueueStatusUpdate(ctx, tx, id, m.WorkflowDefinitionID, m.Revision+1, from, to, transitionEvents(from, to), "", now)
		case string(model.WorkflowRunning):
			// Clear a pending pause: no status change, no event.
			return tx.Model(&WorkflowInstanceModel{}).Where("id = ?", id).Updates(map[string]any{
				"pause_requested": false,
				"revision":        gorm.Expr("revision + 1"),
				"updated_at":      now,
			}).Error
		case string(model.WorkflowWaiting):
			// Already waiting: idempotent.
			return nil
		default:
			return ErrStatusConflict
		}
	})
}

func (r *instanceRepo) Stop(ctx context.Context, id, reason string) (bool, error) {
	var pending bool
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var m WorkflowInstanceModel
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("id = ?", id).First(&m).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return ErrStatusConflict
			}
			return err
		}
		now := time.Now().UTC()
		switch m.Status {
		case string(model.WorkflowWaiting), string(model.WorkflowRunning), string(model.WorkflowPaused):
			pending = m.Status == string(model.WorkflowRunning)
			if err := tx.Model(&WorkflowInstanceModel{}).Where("id = ?", id).Updates(map[string]any{
				"status":              string(model.WorkflowStopped),
				"leased_by":           "",
				"lease_expiry":        nil,
				"termination_pending": pending,
				"error":               reason,
				"finished_at":         now,
				"revision":            gorm.Expr("revision + 1"),
				"updated_at":          now,
			}).Error; err != nil {
				return err
			}
			from := statusWithReason{status: model.WorkflowStatus(m.Status), waitingReason: model.WaitingReason(m.WaitingReason)}
			to := statusWithReason{status: model.WorkflowStopped}
			return r.enqueueStatusUpdate(ctx, tx, id, m.WorkflowDefinitionID, m.Revision+1, from, to, transitionEvents(from, to), reason, now)
		case string(model.WorkflowStopped):
			// Already stopped: idempotent; report the current pending state.
			pending = m.TerminationPending
			return nil
		default:
			return ErrStatusConflict
		}
	})
	return pending, err
}

func (r *instanceRepo) StopRunningAttempts(ctx context.Context, instanceID string) error {
	now := time.Now().UTC()
	return r.db.WithContext(ctx).Model(&NodeInstanceModel{}).
		Where("workflow_instance_id = ? AND status = ?", instanceID, string(model.NodeRunning)).
		Updates(map[string]any{
			"status":     string(model.NodeStopped),
			"cancelled":  true,
			"stopped_at": now,
			"error":      "instance stopped",
			"updated_at": now,
		}).Error
}

func (r *instanceRepo) ListTerminationPending(ctx context.Context) ([]string, error) {
	var ids []string
	if err := r.db.WithContext(ctx).Model(&WorkflowInstanceModel{}).
		Where("termination_pending = ?", true).
		Pluck("id", &ids).Error; err != nil {
		return nil, err
	}
	return ids, nil
}

func (r *instanceRepo) ResolveTermination(ctx context.Context, instanceID string) error {
	res := r.db.WithContext(ctx).Model(&WorkflowInstanceModel{}).
		Where("id = ? AND termination_pending = ?", instanceID, true).
		Updates(map[string]any{
			"termination_pending": false,
			"leased_by":           "",
			"lease_expiry":        nil,
			"revision":            gorm.Expr("revision + 1"),
			"updated_at":          time.Now().UTC(),
		})
	if res.Error != nil {
		return res.Error
	}
	return nil
}

func (r *instanceRepo) SweepTermination(ctx context.Context) error {
	return r.db.WithContext(ctx).Exec(`
		UPDATE workflow_instances
		SET termination_pending = false, leased_by = '', lease_expiry = NULL,
		    revision = revision + 1, updated_at = now()
		WHERE termination_pending = true AND status = ?
		  AND NOT EXISTS (
		      SELECT 1 FROM node_instances
		      WHERE node_instances.workflow_instance_id = workflow_instances.id
		        AND node_instances.status = ?
		  )`,
		string(model.WorkflowStopped), string(model.NodeRunning)).Error
}

func (r *instanceRepo) DeliverInput(ctx context.Context, c InputCompletion) (*model.InputDelivery, error) {
	now := time.Now().UTC()
	delivery := model.InputDelivery{
		ID:                 newRepoID(),
		WorkflowInstanceID: c.InstanceID,
		NodeInstanceID:     c.NodeInstanceID,
		IdempotencyKey:     c.IdempotencyKey,
		Payload:            c.Payload,
		Accepted:           c.Accepted,
		Error:              c.Error,
		CreatedAt:          now,
	}
	if !c.Accepted {
		m := InputDeliveryToModel(delivery)
		res := r.db.WithContext(ctx).Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "node_instance_id"}, {Name: "idempotency_key"}},
			DoNothing: true,
		}).Create(&m)
		if res.Error != nil {
			return nil, res.Error
		}
		if res.RowsAffected == 1 {
			return &delivery, nil
		}
		return r.getDelivery(ctx, c.InstanceID, c.NodeInstanceID, c.IdempotencyKey)
	}
	if c.PostFailure {
		return r.failInput(ctx, c, &delivery)
	}

	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var instModel WorkflowInstanceModel
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("id = ?", c.InstanceID).First(&instModel).Error; err != nil {
			return err
		}
		m := InputDeliveryToModel(delivery)
		res := tx.Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "node_instance_id"}, {Name: "idempotency_key"}},
			DoNothing: true,
		}).Create(&m)
		if res.Error != nil {
			return res.Error
		}
		if res.RowsAffected == 0 {
			return errDeliveryExists
		}
		fin := tx.Model(&NodeInstanceModel{}).
			Where("id = ? AND status = ?", c.NodeInstanceID, string(model.NodeRunning)).
			Updates(map[string]any{
				"status":        string(model.NodeFinished),
				"output":        jsonCol(c.Payload, "null"),
				"context_after": jsonCol(c.NewContext, "null"),
				"finished_at":   now,
				"updated_at":    now,
			})
		if fin.Error != nil {
			return fin.Error
		}
		if fin.RowsAffected == 0 {
			return ErrStatusConflict
		}
		frameRaw, err := c.NewFrame.JSON()
		if err != nil {
			return err
		}
		upd := tx.Model(&WorkflowInstanceModel{}).
			Where("id = ? AND status = ? AND waiting_reason = ?",
				c.InstanceID, string(model.WorkflowWaiting), string(model.WaitingReasonInput)).
			Updates(map[string]any{
				"status":         string(c.Status),
				"waiting_reason": string(model.WaitingReasonRunnable),
				"frame":          frameRaw,
				"context":        jsonCol(c.NewContext, "{}"),
				"finished_at":    c.FinishedAt,
				"revision":       gorm.Expr("revision + 1"),
				"updated_at":     now,
			})
		if upd.Error != nil {
			return upd.Error
		}
		if upd.RowsAffected == 0 {
			return ErrStatusConflict
		}
		from := statusWithReason{status: model.WorkflowWaiting, waitingReason: model.WaitingReasonInput}
		to := statusWithReason{status: c.Status, waitingReason: model.WaitingReasonRunnable}
		events := []string{model.StatusUpdateEventInputReceived}
		events = append(events, transitionEvents(from, to)...)
		if err := r.enqueueStatusUpdate(ctx, tx, c.InstanceID, instModel.WorkflowDefinitionID, instModel.Revision+1, from, to, events, "", now); err != nil {
			return err
		}
		for _, ev := range []model.WorkflowInstanceEvent{
			{
				ID: newRepoID(), WorkflowInstanceID: c.InstanceID,
				Type: "input_received", Data: json.RawMessage(`{"node_instance_id":"` + c.NodeInstanceID + `"}`),
				CreatedBy: c.CreatedBy, CreatedAt: now,
			},
		} {
			em := WorkflowInstanceEventToModel(ev)
			if err := tx.Create(&em).Error; err != nil {
				return err
			}
		}
		if c.Status == model.WorkflowFinished {
			em := WorkflowInstanceEventToModel(model.WorkflowInstanceEvent{
				ID: newRepoID(), WorkflowInstanceID: c.InstanceID,
				Type: "workflow_finished", Data: json.RawMessage(`{}`),
				CreatedBy: c.CreatedBy, CreatedAt: now,
			})
			if err := tx.Create(&em).Error; err != nil {
				return err
			}
		}
		return nil
	})
	if errors.Is(err, errDeliveryExists) {
		return r.getDelivery(ctx, c.InstanceID, c.NodeInstanceID, c.IdempotencyKey)
	}
	if err != nil {
		return nil, err
	}
	return &delivery, nil
}

// failInput atomically records an accepted delivery whose post processing
// failed: the delivery row, the node attempt taking its final status, the
// failed workflow instance with the merged context, and the failed status
// update change together.
func (r *instanceRepo) failInput(ctx context.Context, c InputCompletion, delivery *model.InputDelivery) (*model.InputDelivery, error) {
	now := time.Now().UTC()
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var instModel WorkflowInstanceModel
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("id = ?", c.InstanceID).First(&instModel).Error; err != nil {
			return err
		}
		m := InputDeliveryToModel(*delivery)
		res := tx.Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "node_instance_id"}, {Name: "idempotency_key"}},
			DoNothing: true,
		}).Create(&m)
		if res.Error != nil {
			return res.Error
		}
		if res.RowsAffected == 0 {
			return errDeliveryExists
		}
		fin := tx.Model(&NodeInstanceModel{}).
			Where("id = ? AND status = ?", c.NodeInstanceID, string(model.NodeRunning)).
			Updates(map[string]any{
				"status":        string(c.NodeStatus),
				"output":        jsonCol(c.Payload, "null"),
				"context_after": jsonCol(c.NewContext, "null"),
				"error":         c.Error,
				"finished_at":   now,
				"updated_at":    now,
			})
		if fin.Error != nil {
			return fin.Error
		}
		if fin.RowsAffected == 0 {
			return ErrStatusConflict
		}
		upd := tx.Model(&WorkflowInstanceModel{}).
			Where("id = ? AND status = ? AND waiting_reason = ?",
				c.InstanceID, string(model.WorkflowWaiting), string(model.WaitingReasonInput)).
			Updates(map[string]any{
				"status":         string(model.WorkflowFailed),
				"waiting_reason": string(model.WaitingReasonRunnable),
				"context":        jsonCol(c.NewContext, "{}"),
				"error":          c.Error,
				"finished_at":    now,
				"revision":       gorm.Expr("revision + 1"),
				"updated_at":     now,
			})
		if upd.Error != nil {
			return upd.Error
		}
		if upd.RowsAffected == 0 {
			return ErrStatusConflict
		}
		from := statusWithReason{status: model.WorkflowWaiting, waitingReason: model.WaitingReasonInput}
		to := statusWithReason{status: model.WorkflowFailed, waitingReason: model.WaitingReasonRunnable}
		events := []string{model.StatusUpdateEventInputReceived}
		events = append(events, transitionEvents(from, to)...)
		if err := r.enqueueStatusUpdate(ctx, tx, c.InstanceID, instModel.WorkflowDefinitionID, instModel.Revision+1, from, to, events, c.Error, now); err != nil {
			return err
		}
		failData, err := json.Marshal(map[string]any{"error": c.Error})
		if err != nil {
			return err
		}
		for _, ev := range []model.WorkflowInstanceEvent{
			{
				ID: newRepoID(), WorkflowInstanceID: c.InstanceID,
				Type: "input_received", Data: json.RawMessage(`{"node_instance_id":"` + c.NodeInstanceID + `"}`),
				CreatedBy: c.CreatedBy, CreatedAt: now,
			},
			{
				ID: newRepoID(), WorkflowInstanceID: c.InstanceID,
				Type: "workflow_failed", Data: failData,
				CreatedBy: c.CreatedBy, CreatedAt: now,
			},
		} {
			em := WorkflowInstanceEventToModel(ev)
			if err := tx.Create(&em).Error; err != nil {
				return err
			}
		}
		return nil
	})
	if errors.Is(err, errDeliveryExists) {
		return r.getDelivery(ctx, c.InstanceID, c.NodeInstanceID, c.IdempotencyKey)
	}
	if err != nil {
		return nil, err
	}
	return delivery, nil
}

func (r *instanceRepo) GetDeliveryByKey(ctx context.Context, workflowInstanceID, idempotencyKey string) (*model.InputDelivery, error) {
	var m InputDeliveryModel
	if err := r.db.WithContext(ctx).
		Where("workflow_instance_id = ? AND idempotency_key = ?", workflowInstanceID, idempotencyKey).
		Order("created_at").
		First(&m).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrDeliveryNotFound
		}
		return nil, err
	}
	d := InputDeliveryFromModel(m)
	return &d, nil
}

func (r *instanceRepo) getDelivery(ctx context.Context, instanceID, nodeInstanceID, idempotencyKey string) (*model.InputDelivery, error) {
	var m InputDeliveryModel
	if err := r.db.WithContext(ctx).
		Where("workflow_instance_id = ? AND node_instance_id = ? AND idempotency_key = ?",
			instanceID, nodeInstanceID, idempotencyKey).
		First(&m).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrDeliveryNotFound
		}
		return nil, err
	}
	d := InputDeliveryFromModel(m)
	return &d, nil
}

func (r *instanceRepo) InsertNodeInstance(ctx context.Context, n model.NodeInstance) error {
	m := NodeInstanceToModel(n)
	return r.db.WithContext(ctx).Create(&m).Error
}

func (r *instanceRepo) UpdateNodeInstance(ctx context.Context, n model.NodeInstance) error {
	m := NodeInstanceToModel(n)
	return r.db.WithContext(ctx).Save(&m).Error
}

func (r *instanceRepo) GetNodeInstance(ctx context.Context, workflowInstanceID, occurrenceID string) (*model.NodeInstance, error) {
	var m NodeInstanceModel
	if err := r.db.WithContext(ctx).
		Where("workflow_instance_id = ? AND id = ?", workflowInstanceID, occurrenceID).
		First(&m).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrNodeInstanceNotFound
		}
		return nil, err
	}
	n := NodeInstanceFromModel(m)
	return &n, nil
}

func (r *instanceRepo) GetNodeInstanceByNode(ctx context.Context, workflowInstanceID, nodeID string) (*model.NodeInstance, error) {
	var m NodeInstanceModel
	if err := r.db.WithContext(ctx).
		Where("workflow_instance_id = ? AND node_id = ?", workflowInstanceID, nodeID).
		First(&m).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrNodeInstanceNotFound
		}
		return nil, err
	}
	n := NodeInstanceFromModel(m)
	return &n, nil
}

func (r *instanceRepo) GetLiveNodeInstanceByNode(ctx context.Context, workflowInstanceID, nodeID string) (*model.NodeInstance, error) {
	var m NodeInstanceModel
	if err := r.db.WithContext(ctx).
		Where("workflow_instance_id = ? AND node_id = ? AND status = ?", workflowInstanceID, nodeID, string(model.NodeRunning)).
		Order("created_at DESC, id DESC").
		First(&m).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrNodeInstanceNotFound
		}
		return nil, err
	}
	n := NodeInstanceFromModel(m)
	return &n, nil
}

func (r *instanceRepo) GetRunningNodeInstance(ctx context.Context, workflowInstanceID string) (*model.NodeInstance, error) {
	var m NodeInstanceModel
	if err := r.db.WithContext(ctx).
		Where("workflow_instance_id = ? AND status = ?", workflowInstanceID, string(model.NodeRunning)).
		Order("updated_at DESC").
		First(&m).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrNodeInstanceNotFound
		}
		return nil, err
	}
	n := NodeInstanceFromModel(m)
	return &n, nil
}

func (r *instanceRepo) ListNodeInstances(ctx context.Context, workflowInstanceID string) ([]model.NodeInstance, error) {
	var models []NodeInstanceModel
	if err := r.db.WithContext(ctx).
		Where("workflow_instance_id = ?", workflowInstanceID).
		Order("created_at, id").
		Find(&models).Error; err != nil {
		return nil, err
	}
	out := make([]model.NodeInstance, len(models))
	for i := range models {
		out[i] = NodeInstanceFromModel(models[i])
	}
	return out, nil
}

func (r *instanceRepo) AppendEvent(ctx context.Context, e model.WorkflowInstanceEvent) error {
	m := WorkflowInstanceEventToModel(e)
	return r.db.WithContext(ctx).Create(&m).Error
}

func (r *instanceRepo) ListEvents(ctx context.Context, workflowInstanceID string) ([]model.WorkflowInstanceEvent, error) {
	var models []WorkflowInstanceEventModel
	if err := r.db.WithContext(ctx).
		Where("workflow_instance_id = ?", workflowInstanceID).
		Order("created_at, id").
		Find(&models).Error; err != nil {
		return nil, err
	}
	out := make([]model.WorkflowInstanceEvent, len(models))
	for i := range models {
		out[i] = WorkflowInstanceEventFromModel(models[i])
	}
	return out, nil
}
