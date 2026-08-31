package model

import (
	"encoding/json"
	"fmt"
	"time"
)

// Frame is the durable execution cursor persisted on each workflow instance:
// the node occurrence to execute next and the stack of currently entered
// groups (innermost last). Frame evolves only through the engine's cursor
// machine transitions.
type Frame struct {
	CurrentNodeID string   `json:"current_node_id"`
	GroupStack    []string `json:"group_stack,omitempty"`
}

// NewFrame starts execution at the workflow's start node occurrence.
func NewFrame(startNodeID string) Frame {
	return Frame{CurrentNodeID: startNodeID}
}

// InGroup reports whether the cursor is inside at least one group.
func (f Frame) InGroup() bool { return len(f.GroupStack) > 0 }

// CurrentGroupID returns the innermost group node id, or "".
func (f Frame) CurrentGroupID() string {
	if len(f.GroupStack) == 0 {
		return ""
	}
	return f.GroupStack[len(f.GroupStack)-1]
}

// JSON marshals the frame for persistence.
func (f Frame) JSON() (json.RawMessage, error) { return json.Marshal(f) }

// ParseFrame unmarshals a persisted frame. Empty input yields an empty frame.
func ParseFrame(raw json.RawMessage) (Frame, error) {
	var f Frame
	if len(raw) == 0 {
		return f, nil
	}
	if err := json.Unmarshal(raw, &f); err != nil {
		return Frame{}, fmt.Errorf("model: parse frame: %w", err)
	}
	if f.CurrentNodeID == "" {
		return Frame{}, fmt.Errorf("model: frame missing current_node_id")
	}
	return f, nil
}

// Counters tracks node execution counts for cycle limiting.
type Counters struct {
	Total int            `json:"total"`
	Nodes map[string]int `json:"nodes,omitempty"`
}

// Record counts one execution of nodeID and returns an error when a configured
// limit would be exceeded.
func (c *Counters) Record(nodeID string, limits Limits) error {
	if limits.MaxPerNodeExecutions > 0 && c.Nodes[nodeID]+1 > limits.MaxPerNodeExecutions {
		return fmt.Errorf("model: node %q execution limit %d exceeded", nodeID, limits.MaxPerNodeExecutions)
	}
	if limits.MaxTotalExecutions > 0 && c.Total+1 > limits.MaxTotalExecutions {
		return fmt.Errorf("model: total execution limit %d exceeded", limits.MaxTotalExecutions)
	}
	c.Total++
	if c.Nodes == nil {
		c.Nodes = map[string]int{}
	}
	c.Nodes[nodeID]++
	return nil
}

// JSON marshals the counters for persistence.
func (c Counters) JSON() (json.RawMessage, error) { return json.Marshal(c) }

// ParseCounters unmarshals persisted counters.
func ParseCounters(raw json.RawMessage) (Counters, error) {
	var c Counters
	if len(raw) == 0 {
		return c, nil
	}
	if err := json.Unmarshal(raw, &c); err != nil {
		return c, fmt.Errorf("model: parse counters: %w", err)
	}
	if c.Nodes == nil {
		c.Nodes = map[string]int{}
	}
	return c, nil
}

// Limits are the engine-wide execution limits. Zero values disable the
// corresponding cap.
type Limits struct {
	MaxPerNodeExecutions int
	MaxTotalExecutions   int
	DefaultNodeTimeout   time.Duration
	ConditionTimeout     time.Duration
	LeaseDuration        time.Duration
	ClaimBatchSize       int
}

// DefaultLimits returns the engine defaults.
func DefaultLimits() Limits {
	return Limits{
		MaxPerNodeExecutions: 1000,
		MaxTotalExecutions:   10000,
		DefaultNodeTimeout:   30 * time.Second,
		ConditionTimeout:     5 * time.Second,
		LeaseDuration:        30 * time.Second,
		ClaimBatchSize:       10,
	}
}
