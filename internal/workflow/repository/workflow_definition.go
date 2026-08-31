package repository

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/simpwf/workflow-engine/internal/workflow/model"
	"gorm.io/gorm"
)

// WorkflowDefinitionRepository persists immutable workflow definitions.
type WorkflowDefinitionRepository interface {
	// Create inserts a definition. A duplicate previous_version_id yields
	// model.ErrConflict.
	Create(ctx context.Context, def model.WorkflowDefinition) error
	GetByID(ctx context.Context, id string) (model.WorkflowDefinition, error)
	List(ctx context.Context, q DefinitionListQuery) ([]model.WorkflowDefinition, int64, error)
	// Delete removes a definition unless a workflow request or instance
	// references it (model.ErrConflict).
	Delete(ctx context.Context, id string) error
	// SetNodeRefs replaces the node definitions referenced by a workflow
	// definition (delete-conflict tracking).
	SetNodeRefs(ctx context.Context, workflowDefinitionID string, nodeDefinitionIDs []string) error
}

type gormWorkflowDefinitionRepository struct {
	db *gorm.DB
}

// NewWorkflowDefinitionRepository builds the GORM-backed repository.
func NewWorkflowDefinitionRepository(db *gorm.DB) WorkflowDefinitionRepository {
	return &gormWorkflowDefinitionRepository{db: db}
}

func (r *gormWorkflowDefinitionRepository) Create(ctx context.Context, def model.WorkflowDefinition) error {
	m := WorkflowDefinitionToModel(def)
	if err := r.db.WithContext(ctx).Create(&m).Error; err != nil {
		if isUniqueViolation(err) {
			return fmt.Errorf("%w: workflow definition %q version %d already exists", model.ErrConflict, def.Name, def.Version)
		}
		return fmt.Errorf("workflow definition create: %w", err)
	}
	return nil
}

func (r *gormWorkflowDefinitionRepository) GetByID(ctx context.Context, id string) (model.WorkflowDefinition, error) {
	var m WorkflowDefinitionModel
	err := r.db.WithContext(ctx).Where("id = ?", id).First(&m).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return model.WorkflowDefinition{}, fmt.Errorf("%w: workflow definition %s", model.ErrNotFound, id)
	}
	if err != nil {
		return model.WorkflowDefinition{}, fmt.Errorf("workflow definition get: %w", err)
	}
	return WorkflowDefinitionFromModel(m), nil
}

func (r *gormWorkflowDefinitionRepository) List(ctx context.Context, q DefinitionListQuery) ([]model.WorkflowDefinition, int64, error) {
	q.Type = "" // workflow definitions have no type column
	query := r.db.WithContext(ctx).Model(&WorkflowDefinitionModel{})
	query = applyDefinitionFilters(query, q)

	if q.LatestOnly {
		sub := r.db.Model(&WorkflowDefinitionModel{}).
			Select("lineage_id, MAX(version) AS version").
			Group("lineage_id")
		query = query.Where("(lineage_id, version) IN (?)", sub)
	}

	var total int64
	if err := query.Count(&total).Error; err != nil {
		return nil, 0, fmt.Errorf("workflow definition count: %w", err)
	}

	order := definitionOrderSQL(q.Order)
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

	var rows []WorkflowDefinitionModel
	if err := query.Order(order + `, "id" ASC`).
		Offset((page - 1) * perPage).
		Limit(perPage).
		Find(&rows).Error; err != nil {
		return nil, 0, fmt.Errorf("workflow definition list: %w", err)
	}

	items := make([]model.WorkflowDefinition, 0, len(rows))
	for _, row := range rows {
		items = append(items, WorkflowDefinitionFromModel(row))
	}
	return items, total, nil
}

func (r *gormWorkflowDefinitionRepository) Delete(ctx context.Context, id string) error {
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var requests int64
		if err := tx.Model(&WorkflowRequestModel{}).
			Where("workflow_definition_id = ?", id).Count(&requests).Error; err != nil {
			return fmt.Errorf("workflow definition request check: %w", err)
		}
		if requests > 0 {
			return fmt.Errorf("%w: workflow definition %s is referenced by workflow requests", model.ErrConflict, id)
		}
		var instances int64
		if err := tx.Model(&WorkflowInstanceModel{}).
			Where("workflow_definition_id = ?", id).Count(&instances).Error; err != nil {
			return fmt.Errorf("workflow definition instance check: %w", err)
		}
		if instances > 0 {
			return fmt.Errorf("%w: workflow definition %s is referenced by workflow instances", model.ErrConflict, id)
		}
		res := tx.Delete(&WorkflowDefinitionModel{}, "id = ?", id)
		if res.Error != nil {
			return fmt.Errorf("workflow definition delete: %w", res.Error)
		}
		if res.RowsAffected == 0 {
			return fmt.Errorf("%w: workflow definition %s", model.ErrNotFound, id)
		}
		return nil
	})
}

// SetNodeRefs replaces the node definition references of a workflow
// definition.
func (r *gormWorkflowDefinitionRepository) SetNodeRefs(ctx context.Context, workflowDefinitionID string, nodeDefinitionIDs []string) error {
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Where("workflow_definition_id = ?", workflowDefinitionID).
			Delete(&WorkflowDefinitionNodeRefModel{}).Error; err != nil {
			return fmt.Errorf("workflow definition node refs clear: %w", err)
		}
		if len(nodeDefinitionIDs) == 0 {
			return nil
		}
		now := time.Now().UTC()
		rows := make([]WorkflowDefinitionNodeRefModel, 0, len(nodeDefinitionIDs))
		for _, nodeID := range nodeDefinitionIDs {
			rows = append(rows, WorkflowDefinitionNodeRefModel{
				WorkflowDefinitionID: workflowDefinitionID,
				NodeDefinitionID:     nodeID,
				CreatedAt:            now,
			})
		}
		if err := tx.Create(&rows).Error; err != nil {
			return fmt.Errorf("workflow definition node refs create: %w", err)
		}
		return nil
	})
}
