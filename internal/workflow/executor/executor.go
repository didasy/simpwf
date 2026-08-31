// Package executor runs workflow node content: scripts, condition groups,
// input validation, outbound HTTP calls and external commands, each under
// the security limits configured for the engine.
package executor

import (
	"context"

	"github.com/simpwf/workflow-engine/internal/workflow/model"
	"github.com/simpwf/workflow-engine/pkg/jsfunc"
)

// Limits carries the engine-wide security limits applied by the executors.
type Limits struct {
	// HTTPAllowlist restricts outbound HTTP targets. Entries are host or
	// host:port (e.g. "example.com", "10.0.0.5:8080"); "*" allows any
	// target for development and logs a warning.
	HTTPAllowlist []string
	// ExecAllowlist restricts executables run by command nodes. Entries
	// match the executable name or its absolute path.
	ExecAllowlist []string
	// MaxOutputBytes caps captured command stdout/stderr and response bodies.
	MaxOutputBytes int
	// MaxRedirects caps outbound HTTP redirects.
	MaxRedirects int
}

// Request is the input to a node execution.
type Request struct {
	Node *model.NodeContent
	// Context is the workflow context snapshot.
	Context map[string]any
	// Vars are extra script variables (e.g. "input" fed by input_data).
	Vars map[string]any
	// Payload is the raw JSON input body for input nodes.
	Payload []byte
	// IdempotencyKey is the stable key sent on outbound HTTP calls so
	// retries of the same logical step hit the same external request.
	IdempotencyKey string
	// InstanceID is the workflow instance id; output nodes use it to derive
	// their redis channel, and poller templates expose it as the reserved
	// read-only root workflow_instance_id.
	InstanceID string
	// NodeInstanceID is the current node occurrence id; poller templates
	// expose it as the reserved read-only root node_instance_id.
	NodeInstanceID string
}

// Result is produced by a successful node execution.
type Result struct {
	// Output is the node output data (type depends on the executor).
	Output any
	// Context is the post-execution context for mutating nodes (script).
	Context map[string]any
}

// Executor executes one type of node content.
type Executor interface {
	Execute(ctx context.Context, req Request) (*Result, error)
}

// ConditionResult is the output of a condition group evaluation.
type ConditionResult struct {
	Matched bool
	Index   int
	Key     string
}

// ValidationResult is the output of an input validation script.
type ValidationResult struct {
	Valid   bool
	Message string
}

// HTTPResult is the output of an outbound HTTP call.
type HTTPResult struct {
	Status  int
	Headers map[string][]string
	Body    any
}

// CommandResult is the output of an external command.
type CommandResult struct {
	ExitCode  int
	Stdout    string
	Stderr    string
	TimedOut  bool
	Truncated bool
}

// Dependencies carries the optional infrastructure the executors need.
type Dependencies struct {
	// Output is optional: when nil, output nodes fail with "output publisher
	// is not configured" instead of executing.
	Output OutputPublisher
	// RedisPoller is optional: when nil, redis poller nodes fail with a
	// missing-transport error.
	RedisPoller RedisPollerClient
	// RabbitPoller is optional: when nil, rabbitmq poller nodes fail with a
	// missing-transport error.
	RabbitPoller RabbitPollerClient
}

// NewExecutors builds the executor set for every node type.
func NewExecutors(limits Limits, funcs *jsfunc.Registry, deps Dependencies) map[model.NodeType]Executor {
	httpEx := NewHTTPExecutor(limits)
	pollerEx := NewPollerExecutor(httpEx, funcs)
	pollerEx.SetRedisClient(deps.RedisPoller)
	pollerEx.SetRabbitClient(deps.RabbitPoller)
	return map[model.NodeType]Executor{
		model.NodeTypeScript:       &ScriptExecutor{funcs: funcs},
		model.NodeTypeConditions:   &ConditionExecutor{funcs: funcs},
		model.NodeTypeInput:        &InputExecutor{funcs: funcs},
		model.NodeTypeExternalCall: &ExternalCallExecutor{http: httpEx, cmd: &CommandExecutor{allowlist: limits.ExecAllowlist, maxOutput: limits.MaxOutputBytes}},
		model.NodeTypeOutput:       &OutputExecutor{publisher: deps.Output},
		model.NodeTypePoller:       pollerEx,
	}
}
