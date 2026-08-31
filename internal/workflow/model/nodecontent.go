package model

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/simpwf/workflow-engine/pkg/contextpath"
	"github.com/simpwf/workflow-engine/pkg/ids"
)

// NodeType is the kind of a node.
type NodeType string

const (
	NodeTypeScript       NodeType = "script"
	NodeTypeConditions   NodeType = "conditions"
	NodeTypeInput        NodeType = "input"
	NodeTypeGroup        NodeType = "group"
	NodeTypeExternalCall NodeType = "external_call"
	NodeTypeOutput       NodeType = "output"
	NodeTypePoller       NodeType = "poller"
)

// Input channel names. An input node's channel fixes which transport may
// deliver its payload; the engine parks the instance the same way for all
// channels.
const (
	InputChannelHTTP     = "http"
	InputChannelRedis    = "redis"
	InputChannelRabbitMQ = "rabbitmq"
)

// ValidNodeType reports whether t is an accepted node type.
func ValidNodeType(t string) bool {
	switch NodeType(t) {
	case NodeTypeScript, NodeTypeConditions, NodeTypeInput, NodeTypeGroup, NodeTypeExternalCall, NodeTypeOutput, NodeTypePoller:
		return true
	default:
		return false
	}
}

// NodeLimits carries the global engine limits applied during parsing.
type NodeLimits struct {
	// DefaultTimeout applies to script and external_call nodes without one.
	DefaultTimeout time.Duration
	// MaxTimeout hard-caps script and external_call timeouts.
	MaxTimeout time.Duration
	// ConditionTimeout is the fixed timeout for condition evaluation and
	// input validation scripts.
	ConditionTimeout time.Duration
}

// NodeContent is the validated, typed content of a node (definition or
// inline workflow node).
type NodeContent struct {
	Type             NodeType
	ID               string
	NodeDefinitionID string
	Name             string
	Timeout          time.Duration
	InputData        *string           // script: context path to feed the script
	Script           string            // script
	Conditions       []Condition       // conditions
	Channel          string            // input
	ContextPath      string            // input: target context path
	Validation       *ValidationScript // input
	HTTP             *HTTPConfig       // external_call
	Execution        *ExecutionConfig  // external_call
	OutputProperty   string
	NextNode         string
	// OnFailure routes execution failures on external_call and poller nodes to
	// a fallback node in the same scope without failing the workflow.
	OnFailure *FailureRoute
	Metadata  map[string]any
	Group     *GroupContent // group
	// PreScript and PostScript are optional lifecycle hooks on every node
	// type. PreScriptSet / PostScriptSet record whether the JSON key was
	// present at all (including an explicit null), so materialization can
	// distinguish "inherit the definition hook" (absent) from "disable it"
	// (null) for nodes referencing a node definition.
	PreScript     *HookScript
	PostScript    *HookScript
	PreScriptSet  bool
	PostScriptSet bool
	// RetryOnRecovery requeues an interrupted external_call node instead of
	// failing it when its worker lease expired (recovery). Script,
	// condition, input, and group nodes always requeue. Pollers default to
	// true when omitted.
	RetryOnRecovery bool
	PollerHTTP      *PollerHTTPConfig     // poller
	PollerRedis     *PollerRedisConfig    // poller
	PollerRabbitMQ  *PollerRabbitMQConfig // poller
	// PredicateTimeout is the fixed timeout for poller until evaluation,
	// taken from NodeLimits.ConditionTimeout. It is not capped like node
	// timeouts.
	PredicateTimeout time.Duration
}

// Condition is one branch of a conditions node.
type Condition struct {
	Condition string
	Key       string
}

// ValidationScript validates an input payload.
type ValidationScript struct {
	Script string
}

// HookScript is an optional lifecycle context-transform script. A pre hook
// runs before node behavior; a post hook runs after the native node output
// is merged into the workflow context. Hook return values are always
// ignored; only exported context mutations persist.
type HookScript struct {
	Script  string
	Timeout time.Duration
}

// FailureRoute defines the fallback target and context output property when an
// external_call or poller node execution fails.
type FailureRoute struct {
	NextNode       string
	OutputProperty string
}

// HTTPConfig describes an outbound HTTP call.
type HTTPConfig struct {
	URL     string
	Method  string
	Headers map[string]string
	Body    json.RawMessage
}

// ExecutionConfig describes an allowlisted command execution.
type ExecutionConfig struct {
	Command []string
	Stdin   string
}

// PollerHTTPConfig describes repeated outbound HTTP calls. The first
// request starts immediately; delay applies between attempts. until is
// evaluated against the normalized response.
type PollerHTTPConfig struct {
	URL            string
	Method         string
	Headers        map[string]string
	Body           json.RawMessage
	Delay          time.Duration
	RequestTimeout time.Duration
	MaxAttempts    int
	Until          string
}

// PollerRedisConfig describes a Redis poller: repeated GET on a key, or a
// subscription wait on a channel (method SUB). A missing GET key is a
// normal response with body null.
type PollerRedisConfig struct {
	Method         string
	Key            string
	Channel        string
	Delay          time.Duration
	RequestTimeout time.Duration
	MaxAttempts    int
	MaxWaitTime    time.Duration
	Until          string
}

// PollerRabbitMQConfig describes a wait on a pre-provisioned queue; the
// first matching message completes the node. The queue is not declared by
// the engine and must be exclusive to one active poller execution.
type PollerRabbitMQConfig struct {
	Queue       string
	MaxWaitTime time.Duration
	Until       string
}

// GroupContent is the nested node list of a group node.
type GroupContent struct {
	StartNodeID string
	Keys        map[string]string
	Nodes       []*NodeContent
}

// rawNode mirrors the JSON shape of a node for lenient parsing.
type rawNode struct {
	Type             string                   `json:"type"`
	ID               string                   `json:"id"`
	NodeDefinitionID *string                  `json:"node_definition_id"`
	Name             string                   `json:"name"`
	Timeout          json.RawMessage          `json:"timeout"`
	InputData        *string                  `json:"input_data"`
	Script           *string                  `json:"script"`
	Conditions       []rawCondition           `json:"conditions"`
	Branches         json.RawMessage          `json:"branches"`
	Keys             json.RawMessage          `json:"keys"`
	Channel          *string                  `json:"channel"`
	ContextPath      *string                  `json:"context_path"`
	Validation       *rawValidation           `json:"validation"`
	HTTPConfig       *rawHTTPConfig           `json:"http_config"`
	ExecutionConfig  *rawExecutionConfig      `json:"execution_config"`
	PollerHTTP       *rawPollerHTTPConfig     `json:"http"`
	PollerRedis      *rawPollerRedisConfig    `json:"redis"`
	PollerRabbitMQ   *rawPollerRabbitMQConfig `json:"rabbitmq"`
	OutputProperty   *string                  `json:"output_property"`
	NextNode         *string                  `json:"next_node"`
	OnFailure        json.RawMessage          `json:"on_failure"`
	Metadata         map[string]any           `json:"metadata"`
	PreScript        json.RawMessage          `json:"pre_script"`
	PostScript       json.RawMessage          `json:"post_script"`
	StartNodeID      *string                  `json:"start_node_id"`
	Nodes            []json.RawMessage        `json:"nodes"`
	RetryOnRecovery  *bool                    `json:"retry_on_recovery"`
}

type rawCondition struct {
	Condition string  `json:"condition"`
	Key       *string `json:"key"`
	NextNode  *string `json:"next_node"`
}

type rawValidation struct {
	Script string `json:"script"`
}

type rawHTTPConfig struct {
	URL     string            `json:"url"`
	Method  string            `json:"method"`
	Headers map[string]string `json:"headers"`
	Body    json.RawMessage   `json:"body"`
}

type rawExecutionConfig struct {
	Command []string `json:"command"`
	Stdin   string   `json:"stdin"`
}

// rawPollerHTTPConfig mirrors the top-level http block of a poller node.
// Presence of an explicitly-set field is tracked where it matters for
// validation, so cross-method fields are rejected rather than ignored.
type rawPollerHTTPConfig struct {
	URL            string            `json:"url"`
	Method         string            `json:"method"`
	Headers        map[string]string `json:"headers"`
	Body           json.RawMessage   `json:"body"`
	Delay          json.RawMessage   `json:"delay"`
	RequestTimeout json.RawMessage   `json:"request_timeout"`
	MaxAttempts    *int              `json:"max_attempts"`
	Until          *string           `json:"until"`
}

type rawPollerRedisConfig struct {
	Method         string          `json:"method"`
	Key            *string         `json:"key"`
	Channel        *string         `json:"channel"`
	Delay          json.RawMessage `json:"delay"`
	RequestTimeout json.RawMessage `json:"request_timeout"`
	MaxAttempts    *int            `json:"max_attempts"`
	MaxWaitTime    json.RawMessage `json:"max_wait_time"`
	Until          *string         `json:"until"`
}

type rawPollerRabbitMQConfig struct {
	Queue       string          `json:"queue"`
	MaxWaitTime json.RawMessage `json:"max_wait_time"`
	Until       *string         `json:"until"`
}

// ParseNodeContent parses and validates raw node content JSON against limits.
func ParseNodeContent(raw []byte, limits NodeLimits) (*NodeContent, error) {
	if len(raw) == 0 {
		return nil, errors.New("node content is required")
	}
	var r rawNode
	if err := json.Unmarshal(raw, &r); err != nil {
		return nil, fmt.Errorf("parse node content: %w", err)
	}
	return parseRawNode(&r, limits)
}

func parseRawNode(r *rawNode, limits NodeLimits) (*NodeContent, error) {
	nc := &NodeContent{
		ID:       r.ID,
		Name:     r.Name,
		Metadata: r.Metadata,
	}
	// Lifecycle hooks parse before type-specific validation and before the
	// node_definition_id branch: hooks are graph-independent lifecycle
	// fields, so referenced nodes may carry (or explicitly null) them while
	// still rejecting inline executable fields.
	pre, preSet, err := parseHookScript(r.PreScript, "pre_script", limits)
	if err != nil {
		return nil, err
	}
	post, postSet, err := parseHookScript(r.PostScript, "post_script", limits)
	if err != nil {
		return nil, err
	}
	nc.PreScript = pre
	nc.PreScriptSet = preSet
	nc.PostScript = post
	nc.PostScriptSet = postSet
	onFailure, err := parseFailureRoute(r.OnFailure)
	if err != nil {
		return nil, err
	}
	nc.OnFailure = onFailure
	if r.ID != "" && !ids.Valid(r.ID) {
		return nil, fmt.Errorf("node id %q is not a valid uuid", r.ID)
	}
	if r.NextNode != nil {
		if *r.NextNode != "" && !ids.Valid(*r.NextNode) {
			return nil, fmt.Errorf("next_node %q is not a valid uuid", *r.NextNode)
		}
		nc.NextNode = *r.NextNode
	}
	if r.OutputProperty != nil {
		nc.OutputProperty = *r.OutputProperty
	}
	if r.InputData != nil {
		nc.InputData = r.InputData
	}
	if r.RetryOnRecovery != nil {
		nc.RetryOnRecovery = *r.RetryOnRecovery
	}
	if len(r.Branches) > 0 {
		return nil, errors.New("branches is not supported; define keys on the workflow or group")
	}

	// A node referencing a registered node definition carries only graph
	// fields; executable fields are materialized from the definition.
	if r.NodeDefinitionID != nil {
		if !ids.Valid(*r.NodeDefinitionID) {
			return nil, fmt.Errorf("node_definition_id %q is not a valid uuid", *r.NodeDefinitionID)
		}
		if r.Type != "" && !ValidNodeType(r.Type) {
			return nil, fmt.Errorf("node type %q is not supported", r.Type)
		}
		if r.Type != "" && r.Type != string(NodeTypeExternalCall) && r.Type != string(NodeTypePoller) && nc.OnFailure != nil {
			return nil, fmt.Errorf("node type %q does not support on_failure", r.Type)
		}
		if r.Script != nil || r.Conditions != nil || r.Channel != nil || r.HTTPConfig != nil || r.ExecutionConfig != nil || r.Nodes != nil ||
			r.PollerHTTP != nil || r.PollerRedis != nil || r.PollerRabbitMQ != nil {
			return nil, errors.New("node referencing a node_definition_id cannot carry inline executable fields")
		}
		nc.NodeDefinitionID = *r.NodeDefinitionID
		nc.Type = NodeType(r.Type)
		if len(r.Keys) > 0 {
			keys, err := parseKeys(r.Keys)
			if err != nil {
				return nil, err
			}
			nc.Group = &GroupContent{Keys: keys}
		}
		return nc, nil
	}

	if r.Type == "" {
		return nil, errors.New("node type is required")
	}
	if !ValidNodeType(r.Type) {
		return nil, fmt.Errorf("node type %q is not supported (allowed: script, conditions, input, group, external_call, output, poller)", r.Type)
	}
	nc.Type = NodeType(r.Type)
	if nc.Type != NodeTypeExternalCall && nc.Type != NodeTypePoller && nc.OnFailure != nil {
		return nil, fmt.Errorf("node type %q does not support on_failure", nc.Type)
	}
	if nc.Type != NodeTypeGroup && len(r.Keys) > 0 {
		return nil, errors.New("keys are only valid for group nodes")
	}

	switch nc.Type {
	case NodeTypeScript:
		if r.Script == nil || strings.TrimSpace(*r.Script) == "" {
			return nil, errors.New("script node requires a non-empty script")
		}
		nc.Script = *r.Script
		d, err := nodeTimeout(r.Timeout, limits)
		if err != nil {
			return nil, err
		}
		nc.Timeout = d

	case NodeTypeConditions:
		if len(r.Conditions) < 2 {
			return nil, errors.New("conditions node requires at least two conditions")
		}
		if r.NextNode != nil {
			return nil, errors.New("conditions node cannot have next_node")
		}
		if r.OutputProperty != nil {
			return nil, errors.New("conditions node cannot have output_property")
		}
		keys := make(map[string]bool, len(r.Conditions))
		for _, rc := range r.Conditions {
			if strings.TrimSpace(rc.Condition) == "" {
				return nil, errors.New("condition script must be non-empty")
			}
			key := ""
			if rc.Key != nil && strings.TrimSpace(*rc.Key) != "" {
				key = *rc.Key
			}
			if key != "" && keys[key] {
				return nil, fmt.Errorf("duplicate condition key %q", key)
			}
			keys[key] = true
			if rc.NextNode != nil {
				return nil, errors.New("condition next_node is not supported; use workflow or group keys")
			}
			nc.Conditions = append(nc.Conditions, Condition{Key: key, Condition: rc.Condition})
		}
		nc.Timeout = limits.ConditionTimeout

	case NodeTypeInput:
		if r.Channel == nil || strings.TrimSpace(*r.Channel) == "" {
			return nil, errors.New("input node requires a channel")
		}
		switch *r.Channel {
		case InputChannelHTTP, InputChannelRedis, InputChannelRabbitMQ:
		default:
			return nil, fmt.Errorf("input channel %q is not supported (allowed: http, redis, rabbitmq)", *r.Channel)
		}
		if r.ContextPath == nil || strings.TrimSpace(*r.ContextPath) == "" {
			return nil, errors.New("input node requires a context_path")
		}
		nc.Channel = *r.Channel
		nc.ContextPath = *r.ContextPath
		if r.Validation != nil {
			if strings.TrimSpace(r.Validation.Script) == "" {
				return nil, errors.New("input validation script must be non-empty")
			}
			nc.Validation = &ValidationScript{Script: r.Validation.Script}
		}
		nc.Timeout = limits.ConditionTimeout

	case NodeTypeOutput:
		if r.Channel == nil {
			return nil, errors.New("output node requires a channel")
		}
		if *r.Channel != InputChannelRedis && *r.Channel != InputChannelRabbitMQ {
			return nil, fmt.Errorf("output channel %q is not supported (allowed: redis, rabbitmq)", *r.Channel)
		}
		if r.ContextPath == nil || strings.TrimSpace(*r.ContextPath) == "" {
			return nil, errors.New("output node requires a context_path")
		}
		nc.Channel = *r.Channel
		nc.ContextPath = *r.ContextPath
		d, err := nodeTimeout(r.Timeout, limits)
		if err != nil {
			return nil, err
		}
		nc.Timeout = d

	case NodeTypeExternalCall:
		hasHTTP := r.HTTPConfig != nil
		hasExec := r.ExecutionConfig != nil
		if hasHTTP == hasExec {
			return nil, errors.New("external_call node requires exactly one of http_config or execution_config")
		}
		if hasHTTP {
			cfg := r.HTTPConfig
			if strings.TrimSpace(cfg.URL) == "" {
				return nil, errors.New("http_config.url is required")
			}
			// Templated URLs are validated at execution time after rendering;
			// static URLs must already be absolute http(s).
			if !contextpath.HasTemplate(cfg.URL) {
				u, err := url.Parse(cfg.URL)
				if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
					return nil, fmt.Errorf("http_config.url %q must be an absolute http(s) url", cfg.URL)
				}
			}
			method := cfg.Method
			if method == "" {
				method = "GET"
			} else if !contextpath.HasTemplate(method) {
				method = strings.ToUpper(method)
			}
			nc.HTTP = &HTTPConfig{URL: cfg.URL, Method: method, Headers: cfg.Headers, Body: cfg.Body}
		} else {
			cmd := r.ExecutionConfig.Command
			if len(cmd) == 0 {
				return nil, errors.New("execution_config.command must be non-empty")
			}
			for _, arg := range cmd {
				if strings.TrimSpace(arg) == "" {
					return nil, errors.New("execution_config.command arguments must be non-empty")
				}
			}
			nc.Execution = &ExecutionConfig{Command: cmd, Stdin: r.ExecutionConfig.Stdin}
		}
		d, err := nodeTimeout(r.Timeout, limits)
		if err != nil {
			return nil, err
		}
		nc.Timeout = d

	case NodeTypePoller:
		hasHTTP := r.PollerHTTP != nil
		hasRedis := r.PollerRedis != nil
		hasRabbit := r.PollerRabbitMQ != nil
		if (hasHTTP && (hasRedis || hasRabbit)) || (hasRedis && hasRabbit) || (!hasHTTP && !hasRedis && !hasRabbit) {
			return nil, errors.New("poller node requires exactly one of http, redis, or rabbitmq")
		}
		switch {
		case hasHTTP:
			cfg := r.PollerHTTP
			if strings.TrimSpace(cfg.URL) == "" {
				return nil, errors.New("http.url is required")
			}
			// Templated URLs are validated at execution time after rendering;
			// static URLs must already be absolute http(s).
			if !contextpath.HasTemplate(cfg.URL) {
				u, err := url.Parse(cfg.URL)
				if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
					return nil, fmt.Errorf("http.url %q must be an absolute http(s) url", cfg.URL)
				}
			}
			method := cfg.Method
			if method == "" {
				method = "GET"
			} else if !contextpath.HasTemplate(method) {
				method = strings.ToUpper(method)
			}
			delay, err := pollerDuration(cfg.Delay, "delay", 5*time.Second)
			if err != nil {
				return nil, fmt.Errorf("http.%w", err)
			}
			reqTimeout, err := pollerDuration(cfg.RequestTimeout, "request_timeout", 30*time.Second)
			if err != nil {
				return nil, fmt.Errorf("http.%w", err)
			}
			attempts := 10
			if cfg.MaxAttempts != nil {
				if *cfg.MaxAttempts <= 0 {
					return nil, errors.New("http.max_attempts must be greater than zero")
				}
				attempts = *cfg.MaxAttempts
			}
			until, err := requiredUntil(cfg.Until)
			if err != nil {
				return nil, err
			}
			nc.PollerHTTP = &PollerHTTPConfig{URL: cfg.URL, Method: method, Headers: cfg.Headers, Body: cfg.Body, Delay: delay, RequestTimeout: reqTimeout, MaxAttempts: attempts, Until: until}

		case hasRedis:
			cfg := r.PollerRedis
			switch strings.ToUpper(strings.TrimSpace(cfg.Method)) {
			case "GET":
				if cfg.Channel != nil {
					return nil, errors.New("redis GET does not support channel")
				}
				if cfg.MaxWaitTime != nil {
					return nil, errors.New("redis GET does not support max_wait_time")
				}
				if cfg.Key == nil || strings.TrimSpace(*cfg.Key) == "" {
					return nil, errors.New("redis GET requires a key")
				}
				delay, err := pollerDuration(cfg.Delay, "delay", 5*time.Second)
				if err != nil {
					return nil, fmt.Errorf("redis.%w", err)
				}
				reqTimeout, err := pollerDuration(cfg.RequestTimeout, "request_timeout", 30*time.Second)
				if err != nil {
					return nil, fmt.Errorf("redis.%w", err)
				}
				attempts := 10
				if cfg.MaxAttempts != nil {
					if *cfg.MaxAttempts <= 0 {
						return nil, errors.New("redis.max_attempts must be greater than zero")
					}
					attempts = *cfg.MaxAttempts
				}
				until, err := requiredUntil(cfg.Until)
				if err != nil {
					return nil, err
				}
				nc.PollerRedis = &PollerRedisConfig{Method: "GET", Key: *cfg.Key, Delay: delay, RequestTimeout: reqTimeout, MaxAttempts: attempts, Until: until}

			case "SUB":
				if cfg.Key != nil {
					return nil, errors.New("redis SUB does not support key")
				}
				if cfg.Delay != nil {
					return nil, errors.New("redis SUB does not support delay")
				}
				if cfg.RequestTimeout != nil {
					return nil, errors.New("redis SUB does not support request_timeout")
				}
				if cfg.MaxAttempts != nil {
					return nil, errors.New("redis SUB does not support max_attempts")
				}
				if cfg.Channel == nil || strings.TrimSpace(*cfg.Channel) == "" {
					return nil, errors.New("redis SUB requires a channel")
				}
				wait, err := pollerDuration(cfg.MaxWaitTime, "max_wait_time", 5*time.Minute)
				if err != nil {
					return nil, fmt.Errorf("redis.%w", err)
				}
				until, err := requiredUntil(cfg.Until)
				if err != nil {
					return nil, err
				}
				nc.PollerRedis = &PollerRedisConfig{Method: "SUB", Channel: *cfg.Channel, MaxWaitTime: wait, Until: until}

			default:
				return nil, fmt.Errorf("redis method %q is not supported (allowed: GET, SUB)", cfg.Method)
			}

		case hasRabbit:
			cfg := r.PollerRabbitMQ
			if strings.TrimSpace(cfg.Queue) == "" {
				return nil, errors.New("rabbitmq.queue is required")
			}
			wait, err := pollerDuration(cfg.MaxWaitTime, "max_wait_time", 5*time.Minute)
			if err != nil {
				return nil, fmt.Errorf("rabbitmq.%w", err)
			}
			until, err := requiredUntil(cfg.Until)
			if err != nil {
				return nil, err
			}
			nc.PollerRabbitMQ = &PollerRabbitMQConfig{Queue: cfg.Queue, MaxWaitTime: wait, Until: until}
		}
		// Pollers are active waits; an interrupted attempt restarts by
		// default rather than failing the workflow.
		if r.RetryOnRecovery == nil {
			nc.RetryOnRecovery = true
		}
		nc.PredicateTimeout = limits.ConditionTimeout

	case NodeTypeGroup:
		if r.StartNodeID == nil || !ids.Valid(*r.StartNodeID) {
			return nil, errors.New("group node requires a valid start_node_id")
		}
		if len(r.Nodes) == 0 {
			return nil, errors.New("group node requires a non-empty nodes array")
		}
		keys, err := parseKeys(r.Keys)
		if err != nil {
			return nil, err
		}
		g := &GroupContent{StartNodeID: *r.StartNodeID, Keys: keys}
		seen := make(map[string]bool, len(r.Nodes))
		for _, rawChild := range r.Nodes {
			var child rawNode
			if err := json.Unmarshal(rawChild, &child); err != nil {
				return nil, fmt.Errorf("parse group node: %w", err)
			}
			if child.ID == "" || !ids.Valid(child.ID) {
				return nil, fmt.Errorf("group node child id %q is not a valid uuid", child.ID)
			}
			if seen[child.ID] {
				return nil, fmt.Errorf("duplicate node id %q in group", child.ID)
			}
			seen[child.ID] = true
			childNc, err := parseRawNode(&child, limits)
			if err != nil {
				return nil, err
			}
			g.Nodes = append(g.Nodes, childNc)
		}
		if !seen[g.StartNodeID] {
			return nil, fmt.Errorf("group start_node_id %q does not match any node in the group", g.StartNodeID)
		}
		nc.Group = g
	}
	return nc, nil
}

// parseHookScript resolves a pre/post lifecycle hook object. The returned
// bool reports whether the JSON key was present at all (including explicit
// null): materialization uses it to distinguish "inherit the definition
// hook" from "disable it" for nodes referencing a node definition.
func parseHookScript(raw json.RawMessage, name string, limits NodeLimits) (*HookScript, bool, error) {
	if len(raw) == 0 {
		return nil, false, nil
	}
	if string(raw) == "null" {
		return nil, true, nil
	}
	var r struct {
		Script  *string         `json:"script"`
		Timeout json.RawMessage `json:"timeout"`
	}
	if err := json.Unmarshal(raw, &r); err != nil {
		return nil, true, fmt.Errorf("%s must be an object: %w", name, err)
	}
	if r.Script == nil || strings.TrimSpace(*r.Script) == "" {
		return nil, true, fmt.Errorf("%s.script is required", name)
	}
	d, err := nodeTimeout(r.Timeout, limits)
	if err != nil {
		return nil, true, fmt.Errorf("%s.%w", name, err)
	}
	return &HookScript{Script: *r.Script, Timeout: d}, true, nil
}

func parseKeys(raw json.RawMessage) (map[string]string, error) {
	if len(raw) == 0 || string(raw) == "null" {
		return nil, nil
	}
	var values map[string]*string
	if err := json.Unmarshal(raw, &values); err != nil {
		return nil, fmt.Errorf("keys must be an object: %w", err)
	}
	keys := make(map[string]string, len(values))
	for key, value := range values {
		if strings.TrimSpace(key) == "" {
			return nil, errors.New("workflow key name must be non-empty")
		}
		target := ""
		if value != nil {
			target = *value
		}
		if target != "" && !ids.Valid(target) {
			return nil, fmt.Errorf("workflow key target %q is not a valid uuid", target)
		}
		keys[key] = target
	}
	return keys, nil
}

// nodeTimeout resolves the timeout for script/external_call nodes: explicit
// value parsed and capped, otherwise the global default.
func nodeTimeout(raw json.RawMessage, limits NodeLimits) (time.Duration, error) {
	if len(raw) == 0 || string(raw) == "null" {
		return limits.DefaultTimeout, nil
	}
	var s string
	if err := json.Unmarshal(raw, &s); err != nil {
		return 0, fmt.Errorf("timeout must be a duration string, got %s", raw)
	}
	d, err := time.ParseDuration(s)
	if err != nil {
		return 0, fmt.Errorf("invalid timeout %q", s)
	}
	if d <= 0 {
		return 0, errors.New("timeout must be positive")
	}
	if d > limits.MaxTimeout {
		return 0, fmt.Errorf("timeout %s exceeds the cap of %s", d, limits.MaxTimeout)
	}
	return d, nil
}

// pollerDuration resolves a poller duration field: explicit positive value
// parsed without the global timeout cap, otherwise the transport default.
func pollerDuration(raw json.RawMessage, name string, def time.Duration) (time.Duration, error) {
	if len(raw) == 0 || string(raw) == "null" {
		return def, nil
	}
	var s string
	if err := json.Unmarshal(raw, &s); err != nil {
		return 0, fmt.Errorf("%s must be a duration string", name)
	}
	d, err := time.ParseDuration(s)
	if err != nil {
		return 0, fmt.Errorf("invalid %s %q", name, s)
	}
	if d <= 0 {
		return 0, fmt.Errorf("%s must be positive", name)
	}
	return d, nil
}

// parseFailureRoute resolves the on_failure routing object for external_call and poller nodes.
func parseFailureRoute(raw json.RawMessage) (*FailureRoute, error) {
	if len(raw) == 0 || string(raw) == "null" {
		return nil, nil
	}
	var r struct {
		NextNode       *string `json:"next_node"`
		OutputProperty *string `json:"output_property"`
	}
	if err := json.Unmarshal(raw, &r); err != nil {
		return nil, fmt.Errorf("on_failure must be an object: %w", err)
	}
	if r.NextNode == nil || strings.TrimSpace(*r.NextNode) == "" {
		return nil, errors.New("on_failure.next_node is required")
	}
	if !ids.Valid(*r.NextNode) {
		return nil, fmt.Errorf("on_failure.next_node %q is not a valid uuid", *r.NextNode)
	}
	if r.OutputProperty == nil || strings.TrimSpace(*r.OutputProperty) == "" {
		return nil, errors.New("on_failure.output_property is required")
	}
	return &FailureRoute{
		NextNode:       *r.NextNode,
		OutputProperty: strings.TrimSpace(*r.OutputProperty),
	}, nil
}

// requiredUntil enforces the poller until predicate.
func requiredUntil(raw *string) (string, error) {
	if raw == nil || strings.TrimSpace(*raw) == "" {
		return "", errors.New("until is required")
	}
	return *raw, nil
}
