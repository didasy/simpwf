// Package engine implements the durable cursor/frame state machine, the
// leased dispatcher, and node execution for workflow instances.
package engine

import (
	"fmt"

	"github.com/simpwf/workflow-engine/internal/workflow/model"
)

// Graph is the read view of a materialized workflow definition the cursor
// walks. Implementations bind node occurrence ids to immutable node
// definition content.
type Graph interface {
	// TypeOf returns the node type of a node occurrence.
	TypeOf(nodeID string) (model.NodeType, error)
	// NextOf returns the next_node_id link of a node ("" when none).
	NextOf(nodeID string) (string, error)
	// StartOf returns the start_node_id of a group node.
	StartOf(groupID string) (string, error)
}

// EnterGroup transitions the cursor into a group: the group node id is
// pushed onto the stack and the cursor points at the group's start node.
// It returns the first node to execute.
func EnterGroup(frame *model.Frame, g Graph, groupID string) (string, error) {
	start, err := g.StartOf(groupID)
	if err != nil {
		return "", err
	}
	frame.GroupStack = append(frame.GroupStack, groupID)
	frame.CurrentNodeID = start
	return start, nil
}

// Advance moves the cursor after the current node finished, given its
// resolved next link ("" = no target). When a node inside a group finishes
// without a target, the group is popped and traversal continues at the
// group's own link; nested groups pop in order. It returns true when
// traversal finished at top level (no more work), plus the ids of the
// groups that were exited in pop order.
func Advance(frame *model.Frame, g Graph, next string) (done bool, exited []string, err error) {
	for next == "" && len(frame.GroupStack) > 0 {
		groupID := frame.GroupStack[len(frame.GroupStack)-1]
		frame.GroupStack = frame.GroupStack[:len(frame.GroupStack)-1]
		exited = append(exited, groupID)
		var err error
		next, err = g.NextOf(groupID)
		if err != nil {
			return false, exited, fmt.Errorf("engine: exit group %q: %w", groupID, err)
		}
	}
	if next == "" {
		frame.CurrentNodeID = ""
		return true, exited, nil
	}
	frame.CurrentNodeID = next
	return false, exited, nil
}
