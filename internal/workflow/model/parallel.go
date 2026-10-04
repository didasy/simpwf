package model

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"
)

// validateParallelScopes checks the cross-node structure of every parallel
// block in the workflow tree: branch targets and join nodes resolve inside
// the parallel_start's own scope, every parallel_end is paired with exactly
// one parallel_start, and (in the convergence pass) branch paths cannot
// escape their scope. Nodes that reference a node definition without
// resolved executable fields defer their checks until materialization.
func validateParallelScopes(wc *WorkflowContent, limits ParallelLimits) error {
	if limits.MaxDepth <= 0 {
		limits.MaxDepth = DefaultParallelLimits().MaxDepth
	}
	if limits.MaxBranchesPerParallel <= 0 {
		limits.MaxBranchesPerParallel = DefaultParallelLimits().MaxBranchesPerParallel
	}
	if err := validateParallelScope(wc.Nodes); err != nil {
		return err
	}
	return validateParallelConvergence(wc, limits)
}

// validateParallelScope validates the parallel blocks whose start nodes sit
// directly in nodes, then recurses into child groups. Groups do not add
// parallel depth; nesting is measured over branch reachability.
func validateParallelScope(nodes []*NodeContent) error {
	byID := make(map[string]*NodeContent, len(nodes))
	for _, nc := range nodes {
		byID[nc.ID] = nc
	}
	paired := make(map[string]string, len(nodes))
	for _, nc := range nodes {
		if nc.Type != NodeTypeParallelStart || parallelUnresolved(nc) {
			continue
		}
		for name, target := range nc.ParallelBranches {
			if target == nc.ID {
				return fmt.Errorf("parallel_start %q branch %q must not target the start node itself", nc.ID, name)
			}
			if _, ok := byID[target]; !ok {
				return fmt.Errorf("parallel_start %q branch %q target %q must reference a node in the same group", nc.ID, name, target)
			}
		}
		end, ok := byID[nc.ParallelEndNodeID]
		if !ok {
			return fmt.Errorf("parallel_start %q parallel_end_node_id %q must reference a node in the same group", nc.ID, nc.ParallelEndNodeID)
		}
		if end.Type != NodeTypeParallelEnd {
			return fmt.Errorf("parallel_start %q parallel_end_node_id %q must reference a parallel_end node", nc.ID, nc.ParallelEndNodeID)
		}
		if prev, dup := paired[nc.ParallelEndNodeID]; dup {
			return fmt.Errorf("parallel_end %q is referenced by more than one parallel_start (%q and %q)", nc.ParallelEndNodeID, prev, nc.ID)
		}
		paired[nc.ParallelEndNodeID] = nc.ID
	}
	for _, nc := range nodes {
		if nc.Type != NodeTypeParallelEnd || parallelUnresolved(nc) {
			continue
		}
		if _, ok := paired[nc.ID]; !ok {
			return fmt.Errorf("parallel_end %q is not referenced by any parallel_start in the same group", nc.ID)
		}
	}
	for _, nc := range nodes {
		if nc.Group == nil {
			continue
		}
		if err := validateParallelScope(nc.Group.Nodes); err != nil {
			return err
		}
	}
	return nil
}

// parallelUnresolved reports whether a parallel node is a bare
// node_definition_id reference whose executable fields materialize later.
func parallelUnresolved(nc *NodeContent) bool {
	if nc.NodeDefinitionID == "" {
		return false
	}
	switch nc.Type {
	case NodeTypeParallelStart:
		return nc.ParallelBranches == nil
	case NodeTypeParallelEnd:
		return nc.CombiningScript == ""
	default:
		return false
	}
}

// parallelTreeIndex maps the workflow tree for the convergence walk: every
// node by id, the scope each node sits in, the keys of every scope, and the
// enclosing group chain of every node.
type parallelTreeIndex struct {
	byID  map[string]*NodeContent
	scope map[string]string
	keys  map[string]map[string]string
	base  map[string][]string
}

func indexParallelTree(wc *WorkflowContent) *parallelTreeIndex {
	idx := &parallelTreeIndex{
		byID:  map[string]*NodeContent{},
		scope: map[string]string{},
		keys:  map[string]map[string]string{},
		base:  map[string][]string{},
	}
	var walk func(nodes []*NodeContent, scopeID string, stack []string, keys map[string]string)
	walk = func(nodes []*NodeContent, scopeID string, stack []string, keys map[string]string) {
		idx.keys[scopeID] = keys
		for _, nc := range nodes {
			idx.byID[nc.ID] = nc
			idx.scope[nc.ID] = scopeID
			idx.base[nc.ID] = append([]string(nil), stack...)
			if nc.Group != nil {
				walk(nc.Group.Nodes, nc.ID, append(append([]string(nil), stack...), nc.ID), nc.Group.Keys)
			}
		}
	}
	walk(wc.Nodes, "", nil, wc.Keys)
	return idx
}

// validateParallelConvergence walks every resolved parallel_start's branch
// paths with an abstract cursor: each branch must reach its join, no path
// may terminate elsewhere or pop below the start's group stack, and nesting
// edges feed the depth check.
func validateParallelConvergence(wc *WorkflowContent, limits ParallelLimits) error {
	idx := indexParallelTree(wc)
	var starts []*NodeContent
	var collect func(nodes []*NodeContent)
	collect = func(nodes []*NodeContent) {
		for _, nc := range nodes {
			if nc.Type == NodeTypeParallelStart && !parallelUnresolved(nc) {
				starts = append(starts, nc)
			}
			if nc.Group != nil {
				collect(nc.Group.Nodes)
			}
		}
	}
	collect(wc.Nodes)
	parents := map[string][]string{}
	for _, s := range starts {
		if err := idx.validateStartConvergence(s, parents); err != nil {
			return err
		}
	}
	return validateParallelDepth(starts, parents, limits.MaxDepth)
}

func (idx *parallelTreeIndex) validateStartConvergence(s *NodeContent, parents map[string][]string) error {
	names := make([]string, 0, len(s.ParallelBranches))
	for name := range s.ParallelBranches {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		if err := idx.walkBranch(s, name, parents); err != nil {
			return err
		}
	}
	return nil
}

type parallelCursor struct {
	node  string
	stack []string
}

func (idx *parallelTreeIndex) walkBranch(s *NodeContent, branch string, parents map[string][]string) error {
	startID := s.ID
	end := s.ParallelEndNodeID
	base := idx.base[startID]
	visited := map[string]bool{}
	reached := false
	work := []parallelCursor{{node: s.ParallelBranches[branch], stack: append([]string(nil), base...)}}
	// advance mirrors engine.Advance but never pops below the start's own
	// group stack: leaving it means the path escaped the parallel scope.
	advance := func(work []parallelCursor, from, next string, stack []string) ([]parallelCursor, error) {
		stack = append([]string(nil), stack...)
		for next == "" && len(stack) > len(base) {
			groupID := stack[len(stack)-1]
			stack = stack[:len(stack)-1]
			g, ok := idx.byID[groupID]
			if !ok {
				return work, fmt.Errorf("parallel_start %q branch %q exits unknown group %q", startID, branch, groupID)
			}
			next = g.NextNode
		}
		if next == "" {
			return work, fmt.Errorf("parallel_start %q branch %q reaches %q which never leads to parallel_end %q", startID, branch, from, end)
		}
		return append(work, parallelCursor{node: next, stack: stack}), nil
	}
	for len(work) > 0 {
		c := work[len(work)-1]
		work = work[:len(work)-1]
		if c.node == end {
			reached = true
			continue
		}
		key := c.node + "\x00" + strings.Join(c.stack, "\x00")
		if visited[key] {
			continue
		}
		visited[key] = true
		nc, ok := idx.byID[c.node]
		if !ok {
			return fmt.Errorf("parallel_start %q branch %q reaches unknown node %q", startID, branch, c.node)
		}
		if parallelOpaque(nc) {
			continue
		}
		var err error
		switch nc.Type {
		case NodeTypeParallelStart:
			if nc.ID == startID {
				return fmt.Errorf("parallel_start %q branch %q loops back to its own start node", startID, branch)
			}
			parents[nc.ID] = appendParallelParent(parents[nc.ID], startID)
			work = append(work, parallelCursor{node: nc.ParallelEndNodeID, stack: append([]string(nil), c.stack...)})
		case NodeTypeConditions:
			scopeKeys := idx.keys[idx.scope[nc.ID]]
			for _, cond := range nc.Conditions {
				if cond.Key == "" {
					if work, err = advance(work, nc.ID, "", c.stack); err != nil {
						return err
					}
					continue
				}
				target, ok := scopeKeys[cond.Key]
				if !ok {
					return fmt.Errorf("condition key %q of node %q is not defined in its workflow or group", cond.Key, nc.ID)
				}
				if work, err = advance(work, nc.ID, target, c.stack); err != nil {
					return err
				}
			}
		case NodeTypeGroup:
			work = append(work, parallelCursor{node: nc.Group.StartNodeID, stack: append(append([]string(nil), c.stack...), nc.ID)})
		default:
			if work, err = advance(work, nc.ID, nc.NextNode, c.stack); err != nil {
				return err
			}
		}
		if nc.OnFailure != nil {
			if work, err = advance(work, nc.ID, nc.OnFailure.NextNode, c.stack); err != nil {
				return err
			}
		}
	}
	if !reached {
		return fmt.Errorf("parallel_start %q branch %q has no path to parallel_end %q", startID, branch, end)
	}
	return nil
}

// parallelOpaque reports whether a definition-referencing node hides its
// routing until materialization. Other def-ref types route over the
// occurrence's own graph fields and stay walkable.
func parallelOpaque(nc *NodeContent) bool {
	if nc.NodeDefinitionID == "" {
		return false
	}
	switch nc.Type {
	case NodeTypeConditions:
		return len(nc.Conditions) == 0
	case NodeTypeGroup:
		return nc.Group == nil || len(nc.Group.Nodes) == 0
	case NodeTypeParallelStart:
		return nc.ParallelBranches == nil
	default:
		return false
	}
}

func appendParallelParent(parents []string, parent string) []string {
	for _, p := range parents {
		if p == parent {
			return parents
		}
	}
	return append(parents, parent)
}

// validateParallelDepth computes structural nesting from the recorded
// parent edges (roots are depth 1) and rejects cycles and over-limit depth.
func validateParallelDepth(starts []*NodeContent, parents map[string][]string, maxDepth int) error {
	depth := map[string]int{}
	visiting := map[string]bool{}
	var compute func(id string) (int, error)
	compute = func(id string) (int, error) {
		if d, ok := depth[id]; ok {
			return d, nil
		}
		if visiting[id] {
			return 0, fmt.Errorf("parallel_start %q is nested in its own branches", id)
		}
		visiting[id] = true
		d := 1
		for _, p := range parents[id] {
			pd, err := compute(p)
			if err != nil {
				return 0, err
			}
			if pd+1 > d {
				d = pd + 1
			}
		}
		delete(visiting, id)
		depth[id] = d
		return d, nil
	}
	for _, s := range starts {
		d, err := compute(s.ID)
		if err != nil {
			return err
		}
		if d > maxDepth {
			return fmt.Errorf("parallel_start %q nests %d deep, at most %d allowed", s.ID, d, maxDepth)
		}
	}
	return nil
}

// ParallelExecutionStatus is the lifecycle status of one parallel fork/join
// execution. An execution is created waiting_for_branches together with its
// branches in the fork transaction; there is no observable running state.
type ParallelExecutionStatus string

const (
	ParallelWaitingForBranches ParallelExecutionStatus = "waiting_for_branches"
	ParallelReadyToJoin        ParallelExecutionStatus = "ready_to_join"
	ParallelExecutionCompleted ParallelExecutionStatus = "completed"
	ParallelExecutionFailed    ParallelExecutionStatus = "failed"
	ParallelExecutionCancelled ParallelExecutionStatus = "cancelled"
)

// ValidParallelExecutionStatus reports whether s is a known execution status.
func ValidParallelExecutionStatus(s ParallelExecutionStatus) bool {
	switch s {
	case ParallelWaitingForBranches, ParallelReadyToJoin,
		ParallelExecutionCompleted, ParallelExecutionFailed, ParallelExecutionCancelled:
		return true
	default:
		return false
	}
}

// CanParallelExecutionTransition reports whether the execution state machine
// allows from -> to.
func CanParallelExecutionTransition(from, to ParallelExecutionStatus) bool {
	switch from {
	case ParallelWaitingForBranches:
		return to == ParallelReadyToJoin || to == ParallelExecutionFailed || to == ParallelExecutionCancelled
	case ParallelReadyToJoin:
		return to == ParallelExecutionCompleted || to == ParallelExecutionFailed || to == ParallelExecutionCancelled
	default:
		return false
	}
}

// ParallelBranchStatus is the lifecycle status of one branch of a parallel
// execution. Pending branches never started; waiting branches checkpointed
// with more work to do. Both are claimable, as are running branches whose
// lease expired (recovery).
type ParallelBranchStatus string

const (
	ParallelBranchPending   ParallelBranchStatus = "pending"
	ParallelBranchRunning   ParallelBranchStatus = "running"
	ParallelBranchWaiting   ParallelBranchStatus = "waiting"
	ParallelBranchCompleted ParallelBranchStatus = "completed"
	ParallelBranchFailed    ParallelBranchStatus = "failed"
	ParallelBranchCancelled ParallelBranchStatus = "cancelled"
)

// ValidParallelBranchStatus reports whether s is a known branch status.
func ValidParallelBranchStatus(s ParallelBranchStatus) bool {
	switch s {
	case ParallelBranchPending, ParallelBranchRunning, ParallelBranchWaiting,
		ParallelBranchCompleted, ParallelBranchFailed, ParallelBranchCancelled:
		return true
	default:
		return false
	}
}

// CanParallelBranchTransition reports whether the branch state machine
// allows from -> to.
func CanParallelBranchTransition(from, to ParallelBranchStatus) bool {
	switch from {
	case ParallelBranchPending:
		return to == ParallelBranchRunning || to == ParallelBranchFailed || to == ParallelBranchCancelled
	case ParallelBranchRunning:
		return to == ParallelBranchWaiting || to == ParallelBranchCompleted || to == ParallelBranchFailed || to == ParallelBranchCancelled
	case ParallelBranchWaiting:
		return to == ParallelBranchRunning || to == ParallelBranchFailed || to == ParallelBranchCancelled
	default:
		return false
	}
}

// ParallelExecution is one durable fork/join scope of a workflow instance.
// ParentBranchID is nil for a top-level parallel block and names the branch
// containing a nested fork. Depth counts structural nesting (1 = top level).
type ParallelExecution struct {
	ID             string
	InstanceID     string
	ParentBranchID *string
	Depth          int
	StartNodeID    string
	EndNodeID      string
	Status         ParallelExecutionStatus
	BranchCount    int
	CompletedCount int
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

// ParallelBranch is one durable branch of a parallel execution: its own
// cursor, context, counters, status, and lease. BranchIndex is the
// alphabetical position of Name among its siblings, so debug stepping and
// status output stay deterministic.
type ParallelBranch struct {
	ID                  string
	ParallelExecutionID string
	InstanceID          string
	Name                string
	BranchIndex         int
	StartNodeID         string
	Frame               json.RawMessage
	Context             json.RawMessage
	Counters            json.RawMessage
	Status              ParallelBranchStatus
	WaitingReason       WaitingReason
	Revision            int64
	LeasedBy            string
	LeaseExpiry         time.Time
	Error               string
	CreatedAt           time.Time
	UpdatedAt           time.Time
}
