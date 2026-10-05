package repository

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/simpwf/workflow-engine/internal/workflow/model"
	"gorm.io/gorm"
)

// ScheduleListQuery pages the cron schedule list. Ordering is fixed:
// newest first. A non-nil Enabled filters to that state; nil lists all.
type ScheduleListQuery struct {
	Page    int
	PerPage int
	Enabled *bool
}

// ScheduleRepository persists cron schedules.
type ScheduleRepository interface {
	// Create inserts a schedule. A duplicate id yields model.ErrConflict.
	Create(ctx context.Context, s model.CronSchedule) error
	GetByID(ctx context.Context, id string) (model.CronSchedule, error)
	List(ctx context.Context, q ScheduleListQuery) ([]model.CronSchedule, int64, error)
	// Delete removes a schedule. A missing id yields model.ErrNotFound.
	Delete(ctx context.Context, id string) error
	// SetEnabled flips a schedule on or off, recording the actor, and
	// returns the updated row. A missing id yields model.ErrNotFound.
	SetEnabled(ctx context.Context, id string, enabled bool, actor string) (model.CronSchedule, error)
	// ClaimFire inserts the mutual-exclusion claim for one schedule tick.
	// The first replica to claim (scheduleID, fireAt) wins (claimed=true);
	// a concurrent or replayed claim for the same tick loses
	// (claimed=false, nil error). fireAt is the tick truncated to whole
	// seconds UTC.
	ClaimFire(ctx context.Context, scheduleID string, fireAt time.Time, workerID string) (claimed bool, err error)
	// RecordFireInstance attaches the created instance to a won claim. A
	// missing claim yields model.ErrNotFound.
	RecordFireInstance(ctx context.Context, scheduleID string, fireAt time.Time, instanceID string) error
	// SweepFires deletes claims older than olderThan, returning the rows
	// removed.
	SweepFires(ctx context.Context, olderThan time.Time) (int64, error)
}

type gormScheduleRepository struct {
	db *gorm.DB
}

// NewScheduleRepository builds the GORM-backed repository.
func NewScheduleRepository(db *gorm.DB) ScheduleRepository {
	return &gormScheduleRepository{db: db}
}

func (r *gormScheduleRepository) Create(ctx context.Context, s model.CronSchedule) error {
	m := CronScheduleToModel(s)
	if err := r.db.WithContext(ctx).Create(&m).Error; err != nil {
		if isUniqueViolation(err) {
			return fmt.Errorf("%w: cron schedule %q already exists", model.ErrConflict, s.ID)
		}
		return fmt.Errorf("cron schedule create: %w", err)
	}
	return nil
}

func (r *gormScheduleRepository) GetByID(ctx context.Context, id string) (model.CronSchedule, error) {
	var m CronScheduleModel
	err := r.db.WithContext(ctx).Where("id = ?", id).First(&m).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return model.CronSchedule{}, fmt.Errorf("%w: cron schedule %s", model.ErrNotFound, id)
	}
	if err != nil {
		return model.CronSchedule{}, fmt.Errorf("cron schedule get: %w", err)
	}
	return CronScheduleFromModel(m), nil
}

func (r *gormScheduleRepository) List(ctx context.Context, q ScheduleListQuery) ([]model.CronSchedule, int64, error) {
	query := r.db.WithContext(ctx).Model(&CronScheduleModel{})
	if q.Enabled != nil {
		query = query.Where("enabled = ?", *q.Enabled)
	}

	var total int64
	if err := query.Count(&total).Error; err != nil {
		return nil, 0, fmt.Errorf("cron schedule count: %w", err)
	}

	page := q.Page
	if page < 1 {
		page = 1
	}
	perPage := q.PerPage
	if perPage < 1 {
		perPage = 50
	}

	var rows []CronScheduleModel
	if err := query.Order(`"created_at" DESC, "id" ASC`).
		Offset((page - 1) * perPage).
		Limit(perPage).
		Find(&rows).Error; err != nil {
		return nil, 0, fmt.Errorf("cron schedule list: %w", err)
	}

	items := make([]model.CronSchedule, 0, len(rows))
	for _, row := range rows {
		items = append(items, CronScheduleFromModel(row))
	}
	return items, total, nil
}

func (r *gormScheduleRepository) Delete(ctx context.Context, id string) error {
	res := r.db.WithContext(ctx).Delete(&CronScheduleModel{}, "id = ?", id)
	if res.Error != nil {
		return fmt.Errorf("cron schedule delete: %w", res.Error)
	}
	if res.RowsAffected == 0 {
		return fmt.Errorf("%w: cron schedule %s", model.ErrNotFound, id)
	}
	return nil
}

func (r *gormScheduleRepository) SetEnabled(ctx context.Context, id string, enabled bool, actor string) (model.CronSchedule, error) {
	res := r.db.WithContext(ctx).Model(&CronScheduleModel{}).
		Where("id = ?", id).
		Updates(map[string]any{
			"enabled":    enabled,
			"updated_by": actor,
			"updated_at": time.Now().UTC(),
		})
	if res.Error != nil {
		return model.CronSchedule{}, fmt.Errorf("cron schedule set enabled: %w", res.Error)
	}
	if res.RowsAffected == 0 {
		return model.CronSchedule{}, fmt.Errorf("%w: cron schedule %s", model.ErrNotFound, id)
	}
	return r.GetByID(ctx, id)
}

func (r *gormScheduleRepository) ClaimFire(ctx context.Context, scheduleID string, fireAt time.Time, workerID string) (bool, error) {
	now := time.Now().UTC()
	m := ScheduleFireModel{
		ScheduleID: scheduleID,
		FireAt:     fireAt.UTC(),
		FiredBy:    workerID,
		CreatedAt:  now,
		UpdatedAt:  now,
	}
	if err := r.db.WithContext(ctx).Create(&m).Error; err != nil {
		if isUniqueViolation(err) {
			return false, nil
		}
		return false, fmt.Errorf("schedule fire claim: %w", err)
	}
	return true, nil
}

func (r *gormScheduleRepository) RecordFireInstance(ctx context.Context, scheduleID string, fireAt time.Time, instanceID string) error {
	res := r.db.WithContext(ctx).Model(&ScheduleFireModel{}).
		Where("schedule_id = ? AND fire_at = ?", scheduleID, fireAt).
		Updates(map[string]any{
			"instance_id": instanceID,
			"updated_at":  time.Now().UTC(),
		})
	if res.Error != nil {
		return fmt.Errorf("schedule fire record instance: %w", res.Error)
	}
	if res.RowsAffected == 0 {
		return fmt.Errorf("%w: schedule fire %s @ %s", model.ErrNotFound, scheduleID, fireAt.Format(time.RFC3339))
	}
	return nil
}

func (r *gormScheduleRepository) SweepFires(ctx context.Context, olderThan time.Time) (int64, error) {
	res := r.db.WithContext(ctx).Where("fire_at < ?", olderThan).Delete(&ScheduleFireModel{})
	if res.Error != nil {
		return 0, fmt.Errorf("schedule fire sweep: %w", res.Error)
	}
	return res.RowsAffected, nil
}
