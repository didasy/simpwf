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
