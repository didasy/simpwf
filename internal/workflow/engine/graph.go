package engine

import (
	"fmt"

	"github.com/simpwf/workflow-engine/internal/workflow/model"
)

// workflowGraph indexes a materialized node tree by node id for cursor
// routing and content lookup.
type workflowGraph struct {
	byID        map[string]*model.NodeContent
	keysByNode  map[string]map[string]string
	stackByNode map[string][]string
}

func buildGraph(wc *model.WorkflowContent) *workflowGraph {
	g := &workflowGraph{
		byID:        make(map[string]*model.NodeContent, 16),
		keysByNode:  make(map[string]map[string]string, 16),
		stackByNode: make(map[string][]string, 16),
	}
	var walk func(nodes []*model.NodeContent, keys map[string]string, ancestors []string)
	walk = func(nodes []*model.NodeContent, keys map[string]string, ancestors []string) {
		for _, n := range nodes {
			g.byID[n.ID] = n
			g.keysByNode[n.ID] = keys
			g.stackByNode[n.ID] = append([]string(nil), ancestors...)
			if n.Group != nil {
				walk(n.Group.Nodes, n.Group.Keys, append(ancestors, n.ID))
			}
		}
	}
	walk(wc.Nodes, wc.Keys, nil)
	return g
}

// Node returns the content of a graph node.
func (g *workflowGraph) Node(id string) (*model.NodeContent, error) {
	n, ok := g.byID[id]
	if !ok {
		return nil, fmt.Errorf("engine: unknown node %q in workflow graph", id)
	}
	return n, nil
}

func (g *workflowGraph) TypeOf(id string) (model.NodeType, error) {
	n, err := g.Node(id)
	if err != nil {
		return "", err
	}
	return n.Type, nil
}

func (g *workflowGraph) NextOf(id string) (string, error) {
	n, err := g.Node(id)
	if err != nil {
		return "", err
	}
	return n.NextNode, nil
}

func (g *workflowGraph) StartOf(id string) (string, error) {
	n, err := g.Node(id)
	if err != nil {
		return "", err
	}
	if n.Group == nil {
		return "", fmt.Errorf("engine: node %q is not a group", id)
	}
	return n.Group.StartNodeID, nil
}

func (g *workflowGraph) KeyTarget(nodeID, key string) (string, bool) {
	target, ok := g.keysByNode[nodeID][key]
	return target, ok
}

// groupStack returns the ancestor group chain outermost-first (nil for
// top-level nodes), mirroring service.groupStack.
func (g *workflowGraph) groupStack(id string) ([]string, error) {
	stack, ok := g.stackByNode[id]
	if !ok {
		return nil, fmt.Errorf("engine: unknown node %q in workflow graph", id)
	}
	return append([]string(nil), stack...), nil
}
