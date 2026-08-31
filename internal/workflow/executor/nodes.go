package executor

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/simpwf/workflow-engine/internal/workflow/model"
	"github.com/simpwf/workflow-engine/pkg/jsfunc"
)

var errMissingExternalConfig = errors.New("external_call node has neither http nor execution config")

func errConditionNotBoolean(i int) error {
	return fmt.Errorf("condition %d must return a boolean", i)
}

func errMultipleConditionsMatched(nodeID string, matches []ConditionResult) error {
	parts := make([]string, 0, len(matches))
	for _, m := range matches {
		parts = append(parts, fmt.Sprintf("index %d key %q", m.Index, m.Key))
	}
	return fmt.Errorf("multiple conditions matched in node %s: %s", nodeID, strings.Join(parts, ", "))
}

// ScriptExecutor runs a mutable script against a cloned context and returns
// the return value plus the mutated context.
type ScriptExecutor struct {
	funcs *jsfunc.Registry
}

func (e *ScriptExecutor) Execute(ctx context.Context, req Request) (*Result, error) {
	res, err := RunScript(ctx, ScriptOptions{
		Source:  req.Node.Script,
		Context: req.Context,
		Timeout: req.Node.Timeout,
		Funcs:   e.funcs,
		Vars:    req.Vars,
	})
	if err != nil {
		return nil, err
	}
	return &Result{Output: res.Value, Context: res.Context}, nil
}

// ConditionExecutor evaluates a condition group in order, frozen, and
// requires exactly one match. A match with an empty NextNode exits the group.
type ConditionExecutor struct {
	funcs *jsfunc.Registry
}

func (e *ConditionExecutor) Execute(ctx context.Context, req Request) (*Result, error) {
	cr, err := e.evaluate(ctx, req)
	if err != nil {
		return nil, err
	}
	return &Result{Output: cr, Context: req.Context}, nil
}

func (e *ConditionExecutor) evaluate(ctx context.Context, req Request) (*ConditionResult, error) {
	var matches []ConditionResult
	for i, cond := range req.Node.Conditions {
		res, err := RunScript(ctx, ScriptOptions{
			Source:  cond.Condition,
			Context: req.Context,
			Timeout: req.Node.Timeout,
			Funcs:   e.funcs,
			Frozen:  true,
		})
		if err != nil {
			return nil, &NodeError{Node: req.Node, Reason: "conditions", Err: err}
		}
		matched, ok := res.Value.(bool)
		if !ok {
			return nil, &NodeError{
				Node:   req.Node,
				Reason: "conditions",
				Err:    errConditionNotBoolean(i),
			}
		}
		if matched {
			matches = append(matches, ConditionResult{Matched: true, Index: i, Key: cond.Key})
		}
	}
	switch len(matches) {
	case 0:
		return &ConditionResult{Matched: false, Index: -1}, nil
	case 1:
		return &matches[0], nil
	default:
		return nil, &NodeError{Node: req.Node, Reason: "conditions", Err: errMultipleConditionsMatched(req.Node.ID, matches)}
	}
}

// InputExecutor runs an input validation script. The script must return a
// string to reject the payload with that message; any other result or no
// result accepts it.
type InputExecutor struct {
	funcs *jsfunc.Registry
}

func (e *InputExecutor) Execute(ctx context.Context, req Request) (*Result, error) {
	vr, err := e.Validate(ctx, req)
	if err != nil {
		return nil, err
	}
	return &Result{Output: vr, Context: req.Context}, nil
}

func (e *InputExecutor) Validate(ctx context.Context, req Request) (*ValidationResult, error) {
	if req.Node.Validation == nil || req.Node.Validation.Script == "" {
		return &ValidationResult{Valid: true}, nil
	}
	res, err := RunScript(ctx, ScriptOptions{
		Source:  req.Node.Validation.Script,
		Context: req.Context,
		Timeout: req.Node.Timeout,
		Funcs:   e.funcs,
		Frozen:  true,
		Vars:    map[string]any{"input": string(req.Payload)},
	})
	if err != nil {
		return nil, &NodeError{Node: req.Node, Reason: "input-validation", Err: err}
	}
	msg, isString := res.Value.(string)
	if isString && msg != "" {
		return &ValidationResult{Valid: false, Message: msg}, nil
	}
	return &ValidationResult{Valid: true}, nil
}

// ExternalCallExecutor dispatches external_call nodes to the HTTP or command
// executor based on which configuration the node carries.
type ExternalCallExecutor struct {
	http *HTTPExecutor
	cmd  *CommandExecutor
}

func (e *ExternalCallExecutor) Execute(ctx context.Context, req Request) (*Result, error) {
	switch {
	case req.Node.HTTP != nil:
		return e.http.Execute(ctx, req)
	case req.Node.Execution != nil:
		return e.cmd.Execute(ctx, req)
	default:
		return nil, &NodeError{Node: req.Node, Reason: "external-call", Err: errMissingExternalConfig}
	}
}

// NodeError wraps an execution failure with the node that produced it.
type NodeError struct {
	Node   *model.NodeContent
	Reason string
	Err    error
}

func (e *NodeError) Error() string {
	if e.Node == nil {
		return "executor: " + e.Reason + ": " + e.Err.Error()
	}
	return "executor: " + e.Reason + " (node " + e.Node.Name + "): " + e.Err.Error()
}

func (e *NodeError) Unwrap() error { return e.Err }
