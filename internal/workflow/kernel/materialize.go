package kernel

import (
	"context"
	"fmt"
	"strings"

	"github.com/simpwf/workflow-engine/internal/workflow/model"
	"github.com/simpwf/workflow-engine/internal/workflow/repository"
)

// Materializer resolves node_definition_id references against the immutable
// node definitions and returns the executable node tree.
type Materializer struct {
	nodeRepo repository.NodeDefinitionRepository
	limits   model.NodeLimits
}

// NewMaterializer builds a Materializer over the given node definition
// repository and limits.
func NewMaterializer(nodeRepo repository.NodeDefinitionRepository, limits model.NodeLimits) *Materializer {
	return &Materializer{nodeRepo: nodeRepo, limits: limits}
}

// Materialize walks the node tree, replacing node_definition_id references
// with the executable fields of the referenced node definitions while keeping
// the workflow-owned graph fields.
func (m *Materializer) Materialize(ctx context.Context, wc *model.WorkflowContent) (*model.WorkflowContent, error) {
	nodes := make([]*model.NodeContent, 0, len(wc.Nodes))
	for _, n := range wc.Nodes {
		merged, err := m.materializeNode(ctx, n)
		if err != nil {
			return nil, err
		}
		nodes = append(nodes, merged)
	}
	materialized := &model.WorkflowContent{
		StartNodeID:  wc.StartNodeID,
		Keys:         wc.Keys,
		Nodes:        nodes,
		ContextMode:  wc.ContextMode,
		StatusUpdate: wc.StatusUpdate,
	}
	if err := model.ValidateWorkflowContent(materialized, m.limits.Parallel); err != nil {
		return nil, fmt.Errorf("%w: %v", model.ErrInvalid, err)
	}
	return materialized, nil
}

func (m *Materializer) materializeNode(ctx context.Context, n *model.NodeContent) (*model.NodeContent, error) {
	if n.NodeDefinitionID != "" {
		def, err := m.nodeRepo.GetByID(ctx, n.NodeDefinitionID)
		if err != nil {
			return nil, fmt.Errorf("%w: node definition %s: %v", model.ErrInvalid, n.NodeDefinitionID, err)
		}
		dc, err := model.ParseNodeContent(def.Content, m.limits)
		if err != nil {
			return nil, fmt.Errorf("%w: node definition %s content is invalid: %v", model.ErrInvalid, n.NodeDefinitionID, err)
		}
		if NodeCarriesKeys(dc) {
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
		if strings.TrimSpace(n.OutputProperty) != "" {
			merged.OutputProperty = n.OutputProperty
		}
		if n.OnFailure != nil {
			if merged.Type != model.NodeTypeExternalCall && merged.Type != model.NodeTypePoller {
				if _, ok := model.LookupCustomType(string(merged.Type)); !ok {
					return nil, fmt.Errorf("%w: node %s is %s which does not support on_failure", model.ErrInvalid, n.ID, merged.Type)
				}
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
			merged, err := m.materializeNode(ctx, child)
			if err != nil {
				return nil, err
			}
			g.Nodes = append(g.Nodes, merged)
		}
		ng := *n
		ng.Group = g
		return &ng, nil
	}
	return n, nil
}

// NodeCarriesKeys reports whether a node tree carries workflow or group
// keys. Node definitions must be key-free: keys are workflow-owned.
func NodeCarriesKeys(n *model.NodeContent) bool {
	if n.Group == nil {
		return false
	}
	if n.Group.Keys != nil {
		return true
	}
	for _, child := range n.Group.Nodes {
		if NodeCarriesKeys(child) {
			return true
		}
	}
	return false
}
