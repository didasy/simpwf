package repository

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/simpwf/workflow-engine/internal/workflow/model"
	"github.com/simpwf/workflow-engine/pkg/contextdiff"
	"gorm.io/gorm"
)

const defaultHistoryReplayMax = 500

var (
	// ErrHistoryGap means history cannot be safely replayed from its base.
	ErrHistoryGap = errors.New("repository: history gap")
	// ErrHistoryTooDeep means replay would exceed the configured maximum.
	ErrHistoryTooDeep = errors.New("repository: history replay too deep")
)

// HistoryCursor is the stable (created_at, id) history ordering key.
type HistoryCursor = model.HistoryCursor

// NodeContextHistory is one immutable context anchor or diff.
type NodeContextHistory = model.NodeContextHistory

func (r *instanceRepo) AppendHistory(ctx context.Context, h model.NodeContextHistory) error {
	return appendHistoryTx(r.db.WithContext(ctx), h.WorkflowInstanceID, &h, time.Now().UTC())
}

func appendHistoryTx(tx *gorm.DB, instanceID string, h *model.NodeContextHistory, now time.Time) error {
	if h == nil {
		return nil
	}
	if h.WorkflowInstanceID == "" {
		h.WorkflowInstanceID = instanceID
	}
	if h.WorkflowInstanceID != instanceID {
		return fmt.Errorf("history workflow instance mismatch: %q != %q", h.WorkflowInstanceID, instanceID)
	}
	if h.ID == "" {
		h.ID = newRepoID()
	}
	if h.CreatedAt.IsZero() {
		h.CreatedAt = now
	}
	row := NodeContextHistoryToModel(*h)
	return tx.Create(&row).Error
}

func (r *instanceRepo) LoadHistory(ctx context.Context, instanceID string) ([]model.NodeContextHistory, error) {
	var rows []NodeContextHistoryModel
	if err := r.db.WithContext(ctx).
		Where("workflow_instance_id = ?", instanceID).
		Order(`"created_at" ASC, "id" ASC`).
		Find(&rows).Error; err != nil {
		return nil, err
	}
	out := make([]model.NodeContextHistory, len(rows))
	for i, row := range rows {
		out[i] = NodeContextHistoryFromModel(row)
	}
	return out, nil
}

func (r *instanceRepo) ReconstructBefore(
	ctx context.Context,
	instanceID string,
	target model.HistoryCursor,
	initial json.RawMessage,
) (json.RawMessage, error) {
	var rows []NodeContextHistoryModel
	if err := r.db.WithContext(ctx).
		Where(`workflow_instance_id = ? AND superseded = ? AND
			("created_at" < ? OR ("created_at" = ? AND "id" < ?))`,
			instanceID, false, target.CreatedAt, target.CreatedAt, target.ID).
		Order(`"created_at" ASC, "id" ASC`).
		Find(&rows).Error; err != nil {
		return nil, err
	}

	start := 0
	var contextMap map[string]any
	lastAnchor := -1
	for i := range rows {
		if rows[i].IsAnchor {
			lastAnchor = i
		}
	}
	if lastAnchor >= 0 {
		var err error
		contextMap, err = historySnapshot(json.RawMessage(rows[lastAnchor].Snapshot))
		if err != nil {
			return nil, historyGap(err)
		}
		start = lastAnchor + 1
	} else {
		var err error
		contextMap, err = historySnapshot(initial)
		if err != nil {
			return nil, historyGap(err)
		}
	}

	replayed := 0
	for i := start; i < len(rows); i++ {
		if rows[i].IsAnchor {
			var err error
			contextMap, err = historySnapshot(json.RawMessage(rows[i].Snapshot))
			if err != nil {
				return nil, historyGap(err)
			}
			replayed = 0
			continue
		}
		replayed++
		if replayed > r.replayMax {
			return nil, ErrHistoryTooDeep
		}
		diff, err := historyDiff(json.RawMessage(rows[i].Diff))
		if err != nil {
			return nil, historyGap(err)
		}
		contextMap, err = contextdiff.Apply(contextMap, diff)
		if err != nil {
			return nil, historyGap(err)
		}
	}
	raw, err := json.Marshal(contextMap)
	if err != nil {
		return nil, historyGap(err)
	}
	return raw, nil
}

func (r *instanceRepo) SupersedeAfter(ctx context.Context, instanceID string, target model.HistoryCursor) error {
	// Supersede the target row and everything after it. The start anchor
	// (create or UpdateContext baseline) carries no occurrence id and is
	// never superseded, so post-rollback replays keep their true base.
	return r.db.WithContext(ctx).
		Model(&NodeContextHistoryModel{}).
		Where(`workflow_instance_id = ? AND superseded = ? AND occurrence_id <> '' AND
			(("created_at" > ?) OR ("created_at" = ? AND "id" >= ?))`,
			instanceID, false, target.CreatedAt, target.CreatedAt, target.ID).
		Update("superseded", true).Error
}

func historySnapshot(raw json.RawMessage) (map[string]any, error) {
	if len(bytes.TrimSpace(raw)) == 0 || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return nil, errors.New("snapshot is null or empty")
	}
	var snapshot map[string]any
	if err := json.Unmarshal(raw, &snapshot); err != nil {
		return nil, fmt.Errorf("decode snapshot: %w", err)
	}
	if snapshot == nil {
		return nil, errors.New("snapshot is not an object")
	}
	return snapshot, nil
}

func historyDiff(raw json.RawMessage) (contextdiff.Diff, error) {
	var envelope struct {
		Set   *map[string]json.RawMessage `json:"set"`
		Unset *[]string                   `json:"unset"`
	}
	if err := json.Unmarshal(raw, &envelope); err != nil {
		return contextdiff.Diff{}, err
	}
	if envelope.Set == nil || envelope.Unset == nil {
		return contextdiff.Diff{}, errors.New("diff must contain set and unset")
	}
	return contextdiff.ParseDiff(raw)
}

func historyGap(err error) error {
	if errors.Is(err, ErrHistoryGap) {
		return err
	}
	return fmt.Errorf("%w: %w", ErrHistoryGap, err)
}

var _ HistoryRepository = (*instanceRepo)(nil)
