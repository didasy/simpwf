// Package service orchestrates use cases over the repository layer:
// immutable definitions, instance lifecycle, controls, and queries.
package service

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/simpwf/workflow-engine/internal/workflow/model"
	"github.com/simpwf/workflow-engine/internal/workflow/repository"
	"github.com/simpwf/workflow-engine/pkg/ids"
)

// CreateNodeDefinition is the input for creating a node definition (or the
// next version of an existing lineage).
type CreateNodeDefinition struct {
	Name              string
	Type              string
	PreviousVersionID *string
	Content           json.RawMessage
}

// NodeDefinitionService is the use-case boundary for node definitions.
type NodeDefinitionService interface {
	Create(ctx context.Context, req CreateNodeDefinition) (model.NodeDefinition, error)
	Get(ctx context.Context, id string) (model.NodeDefinition, error)
	List(ctx context.Context, q repository.DefinitionListQuery) ([]model.NodeDefinition, int64, error)
	Delete(ctx context.Context, id string) error
}

type nodeDefinitionService struct {
	repo   repository.NodeDefinitionRepository
	limits model.NodeLimits
	actor  string
}

// NewNodeDefinitionService builds the service. limits carries the global
// engine limits; actor is the configured system user id for audit fields.
func NewNodeDefinitionService(repo repository.NodeDefinitionRepository, limits model.NodeLimits, actor string) NodeDefinitionService {
	return &nodeDefinitionService{repo: repo, limits: limits, actor: actor}
}

func (s *nodeDefinitionService) Create(ctx context.Context, req CreateNodeDefinition) (model.NodeDefinition, error) {
	if strings.TrimSpace(req.Name) == "" {
		return model.NodeDefinition{}, fmt.Errorf("%w: node definition name is required", model.ErrInvalid)
	}
	if !model.ValidNodeType(req.Type) {
		return model.NodeDefinition{}, fmt.Errorf("%w: node type %q is not supported", model.ErrInvalid, req.Type)
	}
	nc, err := model.ParseNodeContent(req.Content, s.limits)
	if err != nil {
		return model.NodeDefinition{}, fmt.Errorf("%w: %v", model.ErrInvalid, err)
	}
	if nodeCarriesKeys(nc) {
		return model.NodeDefinition{}, fmt.Errorf("%w: node definitions cannot carry workflow or group keys", model.ErrInvalid)
	}

	now := nowUTC()
	def := model.NodeDefinition{
		ID:        mustNewID(),
		Name:      req.Name,
		Type:      req.Type,
		Content:   req.Content,
		CreatedBy: s.actor,
		UpdatedBy: s.actor,
		CreatedAt: now,
		UpdatedAt: now,
	}

	if req.PreviousVersionID != nil {
		prev, err := s.repo.GetByID(ctx, *req.PreviousVersionID)
		if err != nil {
			return model.NodeDefinition{}, err
		}
		def.Version = prev.Version + 1
		def.LineageID = prev.LineageID
		def.PreviousVersionID = &prev.ID
	} else {
		def.Version = 1
		def.LineageID = mustNewID()
	}

	if err := s.repo.Create(ctx, def); err != nil {
		return model.NodeDefinition{}, err
	}
	return def, nil
}

func nodeCarriesKeys(n *model.NodeContent) bool {
	if n.Group == nil {
		return false
	}
	if n.Group.Keys != nil {
		return true
	}
	for _, child := range n.Group.Nodes {
		if nodeCarriesKeys(child) {
			return true
		}
	}
	return false
}

func (s *nodeDefinitionService) Get(ctx context.Context, id string) (model.NodeDefinition, error) {
	return s.repo.GetByID(ctx, id)
}

func (s *nodeDefinitionService) List(ctx context.Context, q repository.DefinitionListQuery) ([]model.NodeDefinition, int64, error) {
	return s.repo.List(ctx, q)
}

func (s *nodeDefinitionService) Delete(ctx context.Context, id string) error {
	return s.repo.Delete(ctx, id)
}

// mustNewID generates a UUIDv7 id; generation cannot fail at runtime.
func mustNewID() string {
	id, err := ids.NewString()
	if err != nil {
		panic(fmt.Sprintf("generate uuid: %v", err))
	}
	return id
}

func nowUTC() time.Time { return time.Now().UTC() }
