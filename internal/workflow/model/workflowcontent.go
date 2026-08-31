package model

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/simpwf/workflow-engine/pkg/ids"
)

// WorkflowContent is the validated top-level content of a workflow
// definition: a start node and the flat list of its nodes (groups nest their
// own nodes).
type WorkflowContent struct {
	StartNodeID string
	Keys        map[string]string
	Nodes       []*NodeContent
	// StatusUpdate is the optional webhook notification configuration; nil
	// when the definition publishes no status updates.
	StatusUpdate *StatusUpdateConfig
}

// ParseWorkflowContent parses and validates workflow definition content.
//
// Validation rules:
//   - a valid start_node_id is required and must reference a top-level node;
//   - every node id is a UUIDv7, unique across the whole definition;
//   - next_node and condition key targets stay inside the workflow/group;
//   - nodes may reference registered node definitions via node_definition_id.
func ParseWorkflowContent(raw []byte, limits NodeLimits) (*WorkflowContent, error) {
	if len(raw) == 0 {
		return nil, errors.New("workflow content is required")
	}
	var wf struct {
		StartNodeID *string           `json:"start_node_id"`
		Keys        json.RawMessage   `json:"keys"`
		Nodes       []json.RawMessage `json:"nodes"`
	}
	if err := json.Unmarshal(raw, &wf); err != nil {
		return nil, fmt.Errorf("parse workflow content: %w", err)
	}
	if wf.StartNodeID == nil || !ids.Valid(*wf.StartNodeID) {
		return nil, errors.New("workflow requires a valid start_node_id")
	}
	if len(wf.Nodes) == 0 {
		return nil, errors.New("workflow requires a non-empty nodes array")
	}

	seen := make(map[string]bool, len(wf.Nodes))
	nodes := make([]*NodeContent, 0, len(wf.Nodes))
	for _, raw := range wf.Nodes {
		var rn rawNode
		if err := json.Unmarshal(raw, &rn); err != nil {
			return nil, fmt.Errorf("parse workflow node: %w", err)
		}
		if rn.ID == "" || !ids.Valid(rn.ID) {
			return nil, fmt.Errorf("workflow node id %q is not a valid uuid", rn.ID)
		}
		if seen[rn.ID] {
			return nil, fmt.Errorf("duplicate node id %q in workflow", rn.ID)
		}
		seen[rn.ID] = true
		nc, err := parseRawNode(&rn, limits)
		if err != nil {
			return nil, err
		}
		nodes = append(nodes, nc)
	}
	if !seen[*wf.StartNodeID] {
		return nil, fmt.Errorf("workflow start_node_id %q does not match any node", *wf.StartNodeID)
	}

	keys, err := parseKeys(wf.Keys)
	if err != nil {
		return nil, err
	}
	statusUpdate, err := ParseStatusUpdate(raw)
	if err != nil {
		return nil, err
	}
	wc := &WorkflowContent{StartNodeID: *wf.StartNodeID, Keys: keys, Nodes: nodes, StatusUpdate: statusUpdate}
	if err := ValidateWorkflowContent(wc); err != nil {
		return nil, err
	}
	return wc, nil
}

// ValidateWorkflowContent validates a parsed or materialized workflow tree.
// Referenced nodes with unresolved types defer condition coverage checks until
// materialization provides their executable fields.
func ValidateWorkflowContent(wc *WorkflowContent) error {
	if wc == nil {
		return errors.New("workflow content is required")
	}
	if !ids.Valid(wc.StartNodeID) {
		return errors.New("workflow requires a valid start_node_id")
	}
	if len(wc.Nodes) == 0 {
		return errors.New("workflow requires a non-empty nodes array")
	}
	siblings := make(map[string]bool, len(wc.Nodes))
	for _, nc := range wc.Nodes {
		if nc == nil || !ids.Valid(nc.ID) {
			return errors.New("workflow contains a node without a valid id")
		}
		if siblings[nc.ID] {
			return fmt.Errorf("duplicate node id %q in workflow", nc.ID)
		}
		siblings[nc.ID] = true
	}
	if !siblings[wc.StartNodeID] {
		return fmt.Errorf("workflow start_node_id %q does not match any node", wc.StartNodeID)
	}
	if err := validateKeys(wc.Keys, siblings, "workflow"); err != nil {
		return err
	}
	global := make(map[string]string, len(wc.Nodes))
	for _, nc := range wc.Nodes {
		if err := walkIDs(nc, "workflow", global); err != nil {
			return err
		}
	}
	for _, nc := range wc.Nodes {
		if err := validateLinks(nc, siblings, wc.Keys); err != nil {
			return err
		}
	}
	return nil
}

// walkIDs records every node id in the tree, flagging duplicates.
func walkIDs(nc *NodeContent, owner string, seen map[string]string) error {
	if prev, dup := seen[nc.ID]; dup {
		return fmt.Errorf("duplicate node id %q in workflow (also in %s)", nc.ID, prev)
	}
	seen[nc.ID] = owner
	if nc.Group != nil {
		for _, child := range nc.Group.Nodes {
			if err := walkIDs(child, nc.ID, seen); err != nil {
				return err
			}
		}
	}
	return nil
}

// validateLinks ensures every next_node, on_failure target, and condition key resolves inside the
// node's containing workflow/group scope.
func validateLinks(nc *NodeContent, siblings map[string]bool, keys map[string]string) error {
	if nc.NextNode != "" && !siblings[nc.NextNode] {
		return fmt.Errorf("next_node %q of node %q must reference a node in the same group", nc.NextNode, nc.ID)
	}
	if nc.OnFailure != nil && !siblings[nc.OnFailure.NextNode] {
		return fmt.Errorf("on_failure.next_node %q of node %q must reference a node in the same group", nc.OnFailure.NextNode, nc.ID)
	}
	if nc.Type == NodeTypeConditions {
		for _, cond := range nc.Conditions {
			if cond.Key == "" {
				continue
			}
			if _, ok := keys[cond.Key]; !ok {
				return fmt.Errorf("condition key %q of node %q is not defined in its workflow or group", cond.Key, nc.ID)
			}
		}
	}
	if nc.Group != nil {
		if nc.NodeDefinitionID != "" && len(nc.Group.Nodes) == 0 {
			return nil
		}
		childSiblings := make(map[string]bool, len(nc.Group.Nodes))
		for _, child := range nc.Group.Nodes {
			childSiblings[child.ID] = true
		}
		if err := validateKeys(nc.Group.Keys, childSiblings, fmt.Sprintf("group %q", nc.ID)); err != nil {
			return err
		}
		for _, child := range nc.Group.Nodes {
			if err := validateLinks(child, childSiblings, nc.Group.Keys); err != nil {
				return err
			}
		}
	}
	return nil
}

func validateKeys(keys map[string]string, siblings map[string]bool, scope string) error {
	for key, target := range keys {
		if strings.TrimSpace(key) == "" {
			return fmt.Errorf("%s contains an empty key name", scope)
		}
		if target != "" && !siblings[target] {
			return fmt.Errorf("key %q of %s must reference a node in the same scope", key, scope)
		}
	}
	return nil
}
