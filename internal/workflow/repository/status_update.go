package repository

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/simpwf/workflow-engine/internal/workflow/model"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// ErrStatusUpdateClaimLost reports a delivery attempt on an outbox row the
// worker no longer holds (lease expired, taken over, or already resolved).
var ErrStatusUpdateClaimLost = errors.New("repository: status update claim lost")

// PendingStatusUpdate is a claimed outbox row awaiting delivery. ID is the
// row id; LogicalID is the event id shared across the transports of one
// logical event, used by receivers for cross-transport deduplication.
type PendingStatusUpdate struct {
	ID                   string
	LogicalID            string
	WorkflowInstanceID   string
	WorkflowDefinitionID string
	Transport            string
	Payload              json.RawMessage
}

// StatusUpdateRepository is the durable outbox for status-update
// notifications.
type StatusUpdateRepository interface {
	// ClaimNextStatusUpdates leases up to limit ready events. It returns the
	// oldest undelivered, non-dead event per workflow instance — never a
	// later event while an earlier sibling is still pending — ordering
	// results by next_attempt_at. Concurrent claims are excluded with FOR
	// UPDATE SKIP LOCKED and expired claims are reclaimed.
	ClaimNextStatusUpdates(ctx context.Context, workerID string, lease time.Duration, limit int) ([]PendingStatusUpdate, error)
	// MarkStatusUpdateDelivered finalizes a claimed event. Returns
	// ErrStatusUpdateClaimLost when the worker no longer holds the claim.
	MarkStatusUpdateDelivered(ctx context.Context, eventID, workerID string) error
	// FailStatusUpdate records a failed attempt: it schedules a retry after
	// retryDelay, or dead-letters the event once its attempt count exceeds
	// maxRetry (maxRetry = retries after the initial attempt).
	FailStatusUpdate(ctx context.Context, eventID, workerID string, retryDelay time.Duration, maxRetry int, errMsg string) error
}

type statusUpdateRepo struct{ db *gorm.DB }

// NewStatusUpdateRepository builds the outbox repository.
func NewStatusUpdateRepository(db *gorm.DB) StatusUpdateRepository {
	return &statusUpdateRepo{db: db}
}

func (r *statusUpdateRepo) ClaimNextStatusUpdates(ctx context.Context, workerID string, lease time.Duration, limit int) ([]PendingStatusUpdate, error) {
	tx := r.db.WithContext(ctx).Begin()
	if tx.Error != nil {
		return nil, tx.Error
	}
	defer tx.Rollback()

	var models []StatusUpdateOutboxModel
	err := tx.Clauses(clause.Locking{Strength: "UPDATE", Options: "SKIP LOCKED"}).
		Where(`delivered_at IS NULL AND dead_at IS NULL
		       AND next_attempt_at <= now()
		       AND (claimed_by = '' OR claim_expiry < now())
		       AND NOT EXISTS (
			   SELECT 1 FROM status_update_outbox o2
			   WHERE o2.workflow_instance_id = status_update_outbox.workflow_instance_id
			     AND o2.transport = status_update_outbox.transport
			     AND o2.delivered_at IS NULL AND o2.dead_at IS NULL
			     AND (o2.revision < status_update_outbox.revision
			          OR (o2.revision = status_update_outbox.revision
			              AND o2.event_index < status_update_outbox.event_index))
		       )`).
		Order("next_attempt_at, created_at").
		Limit(limit).
		Find(&models).Error
	if err != nil {
		return nil, err
	}

	now := time.Now().UTC()
	expiry := now.Add(lease)
	for i := range models {
		m := &models[i]
		m.ClaimedBy = workerID
		m.ClaimExpiry = &expiry
		m.UpdatedAt = now
		if err := tx.Save(m).Error; err != nil {
			return nil, err
		}
	}
	if err := tx.Commit().Error; err != nil {
		return nil, err
	}

	out := make([]PendingStatusUpdate, len(models))
	for i := range models {
		out[i] = PendingStatusUpdate{
			ID:                   models[i].ID,
			LogicalID:            logicalEventID(json.RawMessage(models[i].Payload)),
			WorkflowInstanceID:   models[i].WorkflowInstanceID,
			WorkflowDefinitionID: models[i].WorkflowDefinitionID,
			Transport:            models[i].Transport,
			Payload:              json.RawMessage(models[i].Payload),
		}
	}
	return out, nil
}

// logicalEventID extracts the event id embedded in an outbox payload,
// falling back to the empty string when it is missing.
func logicalEventID(payload json.RawMessage) string {
	var p struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(payload, &p); err == nil {
		return p.ID
	}
	return ""
}

func (r *statusUpdateRepo) MarkStatusUpdateDelivered(ctx context.Context, eventID, workerID string) error {
	now := time.Now().UTC()
	res := r.db.WithContext(ctx).Model(&StatusUpdateOutboxModel{}).
		Where("id = ? AND claimed_by = ?", eventID, workerID).
		Updates(map[string]any{
			"delivered_at": now,
			"claimed_by":   "",
			"claim_expiry": nil,
			"updated_at":   now,
		})
	if res.Error != nil {
		return res.Error
	}
	if res.RowsAffected == 0 {
		return ErrStatusUpdateClaimLost
	}
	return nil
}

func (r *statusUpdateRepo) FailStatusUpdate(ctx context.Context, eventID, workerID string, retryDelay time.Duration, maxRetry int, errMsg string) error {
	now := time.Now().UTC()
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var m StatusUpdateOutboxModel
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("id = ? AND claimed_by = ?", eventID, workerID).
			First(&m).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return ErrStatusUpdateClaimLost
			}
			return err
		}
		m.Attempts++
		m.LastError = errMsg
		m.ClaimedBy = ""
		m.ClaimExpiry = nil
		m.UpdatedAt = now
		if m.Attempts > maxRetry {
			m.DeadAt = &now
			m.NextAttemptAt = now
		} else {
			m.NextAttemptAt = now.Add(retryDelay)
		}
		return tx.Save(&m).Error
	})
}

// statusWithReason captures the workflow status and waiting reason of a
// transition boundary, so the outbox payload is faithful to the persisted
// row.
type statusWithReason struct {
	status        model.WorkflowStatus
	waitingReason model.WaitingReason
}

// transitionEvents maps a transition to the notification events it emits.
// Scheduler churn (waiting <-> running) and pause_requested flag changes
// emit nothing; paused -> waiting is a resume even when the preserved
// waiting reason is input.
func transitionEvents(from, to statusWithReason) []string {
	if from.status == model.WorkflowPaused && to.status == model.WorkflowWaiting {
		return []string{model.StatusUpdateEventResumed}
	}
	switch {
	case to.status == model.WorkflowWaiting && to.waitingReason == model.WaitingReasonInput:
		return []string{model.StatusUpdateEventWaitingForInput}
	case to.status == model.WorkflowPaused:
		return []string{model.StatusUpdateEventPaused}
	case to.status == model.WorkflowFinished:
		return []string{model.StatusUpdateEventFinished}
	case to.status == model.WorkflowFailed:
		return []string{model.StatusUpdateEventFailed}
	case to.status == model.WorkflowStopped:
		return []string{model.StatusUpdateEventStopped}
	}
	return nil
}

// enqueueStatusUpdate writes one outbox row per configured transport per
// event, in order, when the instance's immutable workflow definition
// configures status_update. All transports of one event share a single
// logical event id (embedded in the payload) while keeping one row per
// transport so delivery progress and retries advance independently. It must
// run inside the same transaction that commits the status transition, so
// the notification commits atomically with the state change. A missing
// definition id (legacy callers) or a definition without status_update
// skips silently; a definition with an invalid status_update block fails
// the transaction.
func (r *instanceRepo) enqueueStatusUpdate(ctx context.Context, tx *gorm.DB, instanceID, definitionID string, revision int64, from, to statusWithReason, events []string, errMsg string, at time.Time) error {
	if len(events) == 0 || definitionID == "" {
		return nil
	}
	var def WorkflowDefinitionModel
	if err := tx.Where("id = ?", definitionID).First(&def).Error; err != nil {
		return err
	}
	cfg, err := model.ParseStatusUpdate(json.RawMessage(def.Content))
	if err != nil {
		return err
	}
	if cfg == nil {
		return nil
	}
	transports := cfg.Transports()
	if len(transports) == 0 {
		return nil
	}
	for i, ev := range events {
		eventID := newRepoID()
		payload, err := json.Marshal(model.StatusUpdateEventPayload{
			ID:                   eventID,
			Type:                 model.StatusUpdateEventType,
			Event:                ev,
			OccurredAt:           at,
			WorkflowDefinitionID: definitionID,
			WorkflowInstanceID:   instanceID,
			FromStatus:           string(from.status),
			ToStatus:             string(to.status),
			FromWaitingReason:    string(from.waitingReason),
			ToWaitingReason:      string(to.waitingReason),
			Revision:             revision,
			Error:                errMsg,
		})
		if err != nil {
			return err
		}
		for _, tr := range transports {
			m := StatusUpdateOutboxModel{
				ID:                   newRepoID(),
				WorkflowInstanceID:   instanceID,
				WorkflowDefinitionID: definitionID,
				Revision:             revision,
				EventIndex:           i,
				Transport:            tr,
				Payload:              payload,
				NextAttemptAt:        at,
				CreatedAt:            at,
				UpdatedAt:            at,
			}
			if err := tx.Create(&m).Error; err != nil {
				return err
			}
		}
	}
	return nil
}
