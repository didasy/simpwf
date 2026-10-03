package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/simpwf/workflow-engine/internal/workflow/auth"
	"github.com/simpwf/workflow-engine/internal/workflow/model"
	"github.com/simpwf/workflow-engine/internal/workflow/repository"
	"github.com/simpwf/workflow-engine/pkg/contextschema"
)

// DebugContext resolves the debug-position context of a debug instance.
// The target follows NodeDebug resolution: an explicit nodeID is the graph
// node id (or an occurrence id as fallback); empty selects the debug cursor
// from the persisted frame, falling back to the live redacted instance
// context when the cursor is empty or terminal. An occurrence's
// ContextBefore is the source (lean-reconstructed when lean mode applies);
// a node that never ran, or a cursor with no occurrence, uses the live
// redacted instance context. The snapshot is always secret-redacted before
// rendering, so the declaration carries types but never values.
func (s *instanceService) DebugContext(ctx context.Context, instanceID, nodeID string, attempt int, p auth.Principal) (*DebugContextDetail, error) {
	inst, err := s.instances.GetByID(ctx, instanceID)
	if err != nil {
		if errors.Is(err, repository.ErrInstanceNotFound) {
			return nil, fmt.Errorf("%w: instance %s", model.ErrNotFound, instanceID)
		}
		return nil, err
	}
	if err := authorizeInstance(inst, p); err != nil {
		return nil, err
	}
	if !inst.Debug {
		return nil, fmt.Errorf("%w: instance %s is not in debug mode", model.ErrConflict, instanceID)
	}
	graph, err := s.contentGraph(ctx, inst)
	if err != nil {
		return nil, err
	}

	target := nodeID
	if target == "" {
		// No explicit node: follow the debug cursor.
		frame, ferr := model.ParseFrame(inst.Frame)
		if ferr != nil {
			return nil, ferr
		}
		target = frame.CurrentNodeID
	}

	d := &DebugContextDetail{
		InstanceID:    inst.ID,
		IsDebugPaused: inst.Status == model.WorkflowPaused,
	}
	// An empty or terminal cursor resolves to the live redacted instance
	// context.
	if target == "" {
		return s.debugLiveContext(inst, "", d)
	}

	var nodeInst *model.NodeInstance
	nc, err := graph.Node(target)
	if err != nil {
		occ, gerr := s.instances.GetNodeInstance(ctx, instanceID, target)
		if gerr != nil {
			return nil, fmt.Errorf("%w: node %q does not exist in instance %s", model.ErrNotFound, target, instanceID)
		}
		nc, err = graph.Node(occ.NodeID)
		if err != nil {
			return nil, fmt.Errorf("%w: node %q does not exist in instance %s", model.ErrNotFound, target, instanceID)
		}
		nodeInst = occ
	} else {
		occ, gerr := s.instances.GetNodeInstanceByNode(ctx, instanceID, nc.ID)
		if gerr != nil && !errors.Is(gerr, repository.ErrNodeInstanceNotFound) {
			return nil, gerr
		}
		nodeInst = occ
	}
	d.NodeID = nc.ID

	if nodeInst == nil {
		// Never ran: the live redacted instance context is the source.
		return s.debugLiveContext(inst, nc.ID, d)
	}

	selected := attempt
	if selected <= 0 {
		selected = nodeInst.Attempt
	}
	if attempt > nodeInst.Attempt {
		return nil, fmt.Errorf("%w: attempt %d of node %q never ran (latest %d)", model.ErrNotFound, attempt, target, nodeInst.Attempt)
	}
	before := nodeInst.ContextBefore
	if leanMode(inst) {
		target, found, lerr := s.debugLeanBeforeCursor(ctx, inst, nodeInst, selected)
		if lerr != nil {
			return nil, lerr
		}
		if !found {
			return nil, fmt.Errorf("%w: occurrence %q has no history", model.ErrConflict, nodeInst.ID)
		}
		before, lerr = s.leanReconstructBefore(ctx, inst, target)
		if lerr != nil {
			return nil, lerr
		}
	}
	redacted := s.debugRedactSnapshot(inst, before)
	occID := nodeInst.ID
	sel := selected
	d.OccurrenceID = &occID
	d.Attempt = &sel
	d.TypeScript = redacted.TypeScript
	return d, nil
}

// debugLiveContext renders the live redacted instance context: the source
// when the cursor is empty, the cursor node is terminal (finished), or the
// target node never ran.
func (s *instanceService) debugLiveContext(inst *model.WorkflowInstance, nodeID string, d *DebugContextDetail) (*DebugContextDetail, error) {
	d.NodeID = nodeID
	redacted := s.debugRedactSnapshot(inst, inst.Context)
	d.TypeScript = redacted.TypeScript
	return d, nil
}

// debugLeanBeforeCursor resolves the history cursor for the occurrence's
// before context at the selected attempt. Failures map to 409 conflict
// semantics via mapHistoryError.
func (s *instanceService) debugLeanBeforeCursor(ctx context.Context, inst *model.WorkflowInstance, occ *model.NodeInstance, selected int) (model.HistoryCursor, bool, error) {
	rows, err := s.instances.LoadHistory(ctx, inst.ID)
	if err != nil {
		return model.HistoryCursor{}, false, mapHistoryError(err)
	}
	target, found := leanHistoryCursorForOccurrence(rows, occ.ID, selected)
	if !found {
		return model.HistoryCursor{}, false, nil
	}
	return target, true, nil
}

// redactContextSnapshot redacts the secret root of a decoded snapshot to
// the mask placeholder and drops a non-object secret root entirely, so an
// untrusted secret shape collapses to unknown instead of leaking.
func redactContextSnapshot(obj map[string]any) {
	raw, exists := obj["secret"]
	if !exists {
		return
	}
	values, ok := raw.(map[string]any)
	if !ok {
		delete(obj, "secret")
		return
	}
	for key := range values {
		values[key] = SecretMask
	}
}

// debugRedactSnapshot redacts secret plaintext out of a raw snapshot, then
// renders the TypeScript declaration from the redacted value. The secret
// root is masked the same way as NodeDebug; an untrusted secret shape drops
// the root so rendering degrades instead of leaking.
func (s *instanceService) debugRedactSnapshot(inst *model.WorkflowInstance, raw json.RawMessage) *DebugContextDetail {
	d := &DebugContextDetail{}
	secrets, _ := secretValuesFromContext(inst.Context)
	raw = redactJSONValues(raw, secrets)
	v, err := contextschema.DecodeSnapshot(raw)
	if err != nil {
		v = map[string]any{}
	}
	if obj, ok := v.(map[string]any); ok {
		redactContextSnapshot(obj)
		v = obj
	}
	d.TypeScript = contextschema.RenderTypeScript(v)
	return d
}
