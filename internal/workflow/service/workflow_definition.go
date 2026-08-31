package service

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/simpwf/workflow-engine/internal/workflow/model"
	"github.com/simpwf/workflow-engine/internal/workflow/repository"
)

// CreateWorkflowDefinition is the input for creating a workflow definition
// (or the next version of an existing lineage).
type CreateWorkflowDefinition struct {
	Name              string
	PreviousVersionID *string
	Content           json.RawMessage
}

// WorkflowDefinitionService is the use-case boundary for workflow
// definitions.
type WorkflowDefinitionService interface {
	Create(ctx context.Context, req CreateWorkflowDefinition) (model.WorkflowDefinition, error)
	Get(ctx context.Context, id string) (model.WorkflowDefinition, error)
	List(ctx context.Context, q repository.DefinitionListQuery) ([]model.WorkflowDefinition, int64, error)
	Delete(ctx context.Context, id string) error
	// Materialize resolves node_definition_id references against the
	// immutable node definitions and returns the executable node tree.
	Materialize(ctx context.Context, wc *model.WorkflowContent) (*model.WorkflowContent, error)
}

type workflowDefinitionService struct {
	repo     repository.WorkflowDefinitionRepository
	nodeRepo repository.NodeDefinitionRepository
	limits   model.NodeLimits
	actor    string
}

// NewWorkflowDefinitionService builds the service.
func NewWorkflowDefinitionService(
	repo repository.WorkflowDefinitionRepository,
	nodeRepo repository.NodeDefinitionRepository,
	limits model.NodeLimits,
	actor string,
) WorkflowDefinitionService {
	return &workflowDefinitionService{repo: repo, nodeRepo: nodeRepo, limits: limits, actor: actor}
}

func (s *workflowDefinitionService) Create(ctx context.Context, req CreateWorkflowDefinition) (model.WorkflowDefinition, error) {
	if strings.TrimSpace(req.Name) == "" {
		return model.WorkflowDefinition{}, fmt.Errorf("%w: workflow definition name is required", model.ErrInvalid)
	}
	wc, err := model.ParseWorkflowContent(req.Content, s.limits)
	if err != nil {
		return model.WorkflowDefinition{}, fmt.Errorf("%w: %v", model.ErrInvalid, err)
	}
	// Resolving the references validates that every node_definition_id
	// exists and merges cleanly; the authored content is stored as-is.
	if _, err := s.Materialize(ctx, wc); err != nil {
		return model.WorkflowDefinition{}, err
	}

	now := nowUTC()
	def := model.WorkflowDefinition{
		ID:        mustNewID(),
		Name:      req.Name,
		Content:   req.Content,
		CreatedBy: s.actor,
		UpdatedBy: s.actor,
		CreatedAt: now,
		UpdatedAt: now,
	}
	if req.PreviousVersionID != nil {
		prev, err := s.repo.GetByID(ctx, *req.PreviousVersionID)
		if err != nil {
			return model.WorkflowDefinition{}, err
		}
		def.Version = prev.Version + 1
		def.LineageID = prev.LineageID
		def.PreviousVersionID = &prev.ID
	} else {
		def.Version = 1
		def.LineageID = mustNewID()
	}

	if err := s.repo.Create(ctx, def); err != nil {
		return model.WorkflowDefinition{}, err
	}
	if err := s.repo.SetNodeRefs(ctx, def.ID, collectNodeRefs(wc)); err != nil {
		return model.WorkflowDefinition{}, fmt.Errorf("record node refs: %w", err)
	}
	return def, nil
}

func (s *workflowDefinitionService) Get(ctx context.Context, id string) (model.WorkflowDefinition, error) {
	return s.repo.GetByID(ctx, id)
}

func (s *workflowDefinitionService) List(ctx context.Context, q repository.DefinitionListQuery) ([]model.WorkflowDefinition, int64, error) {
	return s.repo.List(ctx, q)
}

func (s *workflowDefinitionService) Delete(ctx context.Context, id string) error {
	return s.repo.Delete(ctx, id)
}

// Materialize walks the node tree, replacing node_definition_id references
// with the executable fields of the referenced node definitions while keeping
// the workflow-owned graph fields.
func (s *workflowDefinitionService) Materialize(ctx context.Context, wc *model.WorkflowContent) (*model.WorkflowContent, error) {
	nodes := make([]*model.NodeContent, 0, len(wc.Nodes))
	for _, n := range wc.Nodes {
		m, err := s.materializeNode(ctx, n)
		if err != nil {
			return nil, err
		}
		nodes = append(nodes, m)
	}
	materialized := &model.WorkflowContent{StartNodeID: wc.StartNodeID, Keys: wc.Keys, Nodes: nodes, StatusUpdate: wc.StatusUpdate}
	if err := model.ValidateWorkflowContent(materialized); err != nil {
		return nil, fmt.Errorf("%w: %v", model.ErrInvalid, err)
	}
	return materialized, nil
}

func (s *workflowDefinitionService) materializeNode(ctx context.Context, n *model.NodeContent) (*model.NodeContent, error) {
	if n.NodeDefinitionID != "" {
		def, err := s.nodeRepo.GetByID(ctx, n.NodeDefinitionID)
		if err != nil {
			return nil, fmt.Errorf("%w: node definition %s: %v", model.ErrInvalid, n.NodeDefinitionID, err)
		}
		dc, err := model.ParseNodeContent(def.Content, s.limits)
		if err != nil {
			return nil, fmt.Errorf("%w: node definition %s content is invalid: %v", model.ErrInvalid, n.NodeDefinitionID, err)
		}
		if nodeCarriesKeys(dc) {
			return nil, fmt.Errorf("%w: node definition %s cannot carry workflow or group keys", model.ErrInvalid, n.NodeDefinitionID)
		}
		if n.Type != "" && n.Type != dc.Type {
			return nil, fmt.Errorf("%w: node %s declares type %s but node definition %s is %s",
				model.ErrInvalid, n.ID, n.Type, n.NodeDefinitionID, dc.Type)
		}
		merged := *dc
		merged.ID = n.ID
		merged.NodeDefinitionID = n.NodeDefinitionID
		if n.Name != "" {
			merged.Name = n.Name
		}
		// Lifecycle hooks: an omitted occurrence hook inherits the
		// definition's; an explicit object replaces it; an explicit null
		// disables it.
		if n.PreScriptSet {
			merged.PreScript = n.PreScript
			merged.PreScriptSet = true
		}
		if n.PostScriptSet {
			merged.PostScript = n.PostScript
			merged.PostScriptSet = true
		}
		merged.NextNode = n.NextNode
		merged.OutputProperty = n.OutputProperty
		if n.OnFailure != nil {
			if merged.Type != model.NodeTypeExternalCall && merged.Type != model.NodeTypePoller {
				return nil, fmt.Errorf("%w: node %s is %s which does not support on_failure", model.ErrInvalid, n.ID, merged.Type)
			}
			merged.OnFailure = n.OnFailure
		}
		if n.Group != nil {
			if merged.Group == nil {
				return nil, fmt.Errorf("%w: node %s defines keys but node definition %s is not a group",
					model.ErrInvalid, n.ID, n.NodeDefinitionID)
			}
			merged.Group.Keys = n.Group.Keys
		}
		if n.RetryOnRecovery {
			merged.RetryOnRecovery = true
		}
		if n.Metadata != nil {
			merged.Metadata = n.Metadata
		}
		return &merged, nil
	}
	if n.Group != nil {
		g := &model.GroupContent{StartNodeID: n.Group.StartNodeID, Keys: n.Group.Keys}
		for _, child := range n.Group.Nodes {
			m, err := s.materializeNode(ctx, child)
			if err != nil {
				return nil, err
			}
			g.Nodes = append(g.Nodes, m)
		}
		ng := *n
		ng.Group = g
		return &ng, nil
	}
	return n, nil
}

// collectNodeRefs gathers the distinct node definition ids referenced by a
// workflow's node tree.
func collectNodeRefs(wc *model.WorkflowContent) []string {
	seen := map[string]bool{}
	var refs []string
	var walk func(nodes []*model.NodeContent)
	walk = func(nodes []*model.NodeContent) {
		for _, n := range nodes {
			if n.NodeDefinitionID != "" && !seen[n.NodeDefinitionID] {
				seen[n.NodeDefinitionID] = true
				refs = append(refs, n.NodeDefinitionID)
			}
			if n.Group != nil {
				walk(n.Group.Nodes)
			}
		}
	}
	walk(wc.Nodes)
	return refs
}
