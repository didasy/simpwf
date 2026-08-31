package repository

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/simpwf/workflow-engine/internal/workflow/model"
	"gorm.io/gorm"
)

// NodeDefinitionRepository persists immutable node definitions.
type NodeDefinitionRepository interface {
	// Create inserts a definition. A duplicate previous_version_id yields
	// model.ErrConflict.
	Create(ctx context.Context, def model.NodeDefinition) error
	GetByID(ctx context.Context, id string) (model.NodeDefinition, error)
	List(ctx context.Context, q DefinitionListQuery) ([]model.NodeDefinition, int64, error)
	// Delete removes a definition unless a workflow definition or node
	// instance references it (model.ErrConflict).
	Delete(ctx context.Context, id string) error
}

type gormNodeDefinitionRepository struct {
	db *gorm.DB
}

// NewNodeDefinitionRepository builds the GORM-backed repository.
func NewNodeDefinitionRepository(db *gorm.DB) NodeDefinitionRepository {
	return &gormNodeDefinitionRepository{db: db}
}

func (r *gormNodeDefinitionRepository) Create(ctx context.Context, def model.NodeDefinition) error {
	m := NodeDefinitionToModel(def)
	if err := r.db.WithContext(ctx).Create(&m).Error; err != nil {
		if isUniqueViolation(err) {
			return fmt.Errorf("%w: node definition %q version %d already exists", model.ErrConflict, def.Name, def.Version)
		}
		return fmt.Errorf("node definition create: %w", err)
	}
	return nil
}

func (r *gormNodeDefinitionRepository) GetByID(ctx context.Context, id string) (model.NodeDefinition, error) {
	var m NodeDefinitionModel
	err := r.db.WithContext(ctx).Where("id = ?", id).First(&m).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return model.NodeDefinition{}, fmt.Errorf("%w: node definition %s", model.ErrNotFound, id)
	}
	if err != nil {
		return model.NodeDefinition{}, fmt.Errorf("node definition get: %w", err)
	}
	return NodeDefinitionFromModel(m), nil
}

func (r *gormNodeDefinitionRepository) List(ctx context.Context, q DefinitionListQuery) ([]model.NodeDefinition, int64, error) {
	query := r.db.WithContext(ctx).Model(&NodeDefinitionModel{})
	query = applyDefinitionFilters(query, q)

	if q.LatestOnly {
		sub := r.db.Model(&NodeDefinitionModel{}).
			Select("lineage_id, MAX(version) AS version").
			Group("lineage_id")
		query = query.Where("(lineage_id, version) IN (?)", sub)
	}

	var total int64
	if err := query.Count(&total).Error; err != nil {
		return nil, 0, fmt.Errorf("node definition count: %w", err)
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

	var rows []NodeDefinitionModel
	if err := query.Order(order + `, "id" ASC`).
		Offset((page - 1) * perPage).
		Limit(perPage).
		Find(&rows).Error; err != nil {
		return nil, 0, fmt.Errorf("node definition list: %w", err)
	}

	items := make([]model.NodeDefinition, 0, len(rows))
	for _, row := range rows {
		items = append(items, NodeDefinitionFromModel(row))
	}
	return items, total, nil
}

func (r *gormNodeDefinitionRepository) Delete(ctx context.Context, id string) error {
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var refs int64
		if err := tx.Model(&WorkflowDefinitionNodeRefModel{}).
			Where("node_definition_id = ?", id).Count(&refs).Error; err != nil {
			return fmt.Errorf("node definition refs check: %w", err)
		}
		if refs > 0 {
			return fmt.Errorf("%w: node definition %s is referenced by workflow definitions", model.ErrConflict, id)
		}
		var instances int64
		if err := tx.Model(&NodeInstanceModel{}).
			Where("node_definition_id = ?", id).Count(&instances).Error; err != nil {
			return fmt.Errorf("node definition instance check: %w", err)
		}
		if instances > 0 {
			return fmt.Errorf("%w: node definition %s is referenced by node instances", model.ErrConflict, id)
		}
		res := tx.Delete(&NodeDefinitionModel{}, "id = ?", id)
		if res.Error != nil {
			return fmt.Errorf("node definition delete: %w", res.Error)
		}
		if res.RowsAffected == 0 {
			return fmt.Errorf("%w: node definition %s", model.ErrNotFound, id)
		}
		return nil
	})
}

// applyDefinitionFilters scopes a definition query by the list filters.
func applyDefinitionFilters(query *gorm.DB, q DefinitionListQuery) *gorm.DB {
	if len(q.IDs) > 0 {
		query = query.Where("id IN ?", q.IDs)
	}
	if q.Name != "" {
		query = query.Where("name = ?", q.Name)
	}
	if q.LineageID != "" {
		query = query.Where("lineage_id = ?", q.LineageID)
	}
	if q.Version != nil {
		query = query.Where("version = ?", *q.Version)
	}
	if q.Type != "" {
		query = query.Where("type = ?", q.Type)
	}
	return query
}

// AddWorkflowDefinitionNodeRefs records which node definitions a workflow
// definition references (for delete-conflict tracking).
func AddWorkflowDefinitionNodeRefs(ctx context.Context, db *gorm.DB, workflowDefinitionID string, nodeDefinitionIDs []string) error {
	now := time.Now().UTC()
	rows := make([]WorkflowDefinitionNodeRefModel, 0, len(nodeDefinitionIDs))
	for _, nodeID := range nodeDefinitionIDs {
		rows = append(rows, WorkflowDefinitionNodeRefModel{
			WorkflowDefinitionID: workflowDefinitionID,
			NodeDefinitionID:     nodeID,
			CreatedAt:            now,
		})
	}
	if len(rows) > 0 {
		if err := db.WithContext(ctx).Create(&rows).Error; err != nil {
			return fmt.Errorf("workflow definition node refs create: %w", err)
		}
	}
	return nil
}

// WorkflowDefinitionNodeRefs lists the node definition ids referenced by a
// workflow definition.
func WorkflowDefinitionNodeRefs(ctx context.Context, db *gorm.DB, workflowDefinitionID string) ([]string, error) {
	var rows []WorkflowDefinitionNodeRefModel
	if err := db.WithContext(ctx).
		Where("workflow_definition_id = ?", workflowDefinitionID).
		Find(&rows).Error; err != nil {
		return nil, fmt.Errorf("workflow definition node refs list: %w", err)
	}
	ids := make([]string, 0, len(rows))
	for _, row := range rows {
		ids = append(ids, row.NodeDefinitionID)
	}
	return ids, nil
}
