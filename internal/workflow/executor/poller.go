package executor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/simpwf/workflow-engine/internal/workflow/model"
	"github.com/simpwf/workflow-engine/internal/workflow/transport"
	"github.com/simpwf/workflow-engine/pkg/contextpath"
	"github.com/simpwf/workflow-engine/pkg/jsfunc"
)

// PollerResponse is the normalized response injected as the "response"
// variable into a poller until predicate and stored as the node output.
// Fields are lowercase; unavailable fields are omitted. body is always
// present: a Redis GET of a missing key is body null, not an omitted field.
type PollerResponse struct {
	Body    any `json:"body"`
	Headers any `json:"headers,omitempty"`
	Status  int `json:"status,omitempty"`
}

// NewHTTPPollerResponse builds the HTTP shape {body, headers, status} with
// map[string][]string headers.
func NewHTTPPollerResponse(status int, headers map[string][]string, body any) *PollerResponse {
	return &PollerResponse{Body: body, Headers: headers, Status: status}
}

// NewRedisPollerResponse builds the Redis shape {body} only.
func NewRedisPollerResponse(body any) *PollerResponse {
	return &PollerResponse{Body: body}
}

// NewRabbitPollerResponse builds the RabbitMQ shape {body, headers} with
// map[string]string headers.
func NewRabbitPollerResponse(headers map[string]string, body any) *PollerResponse {
	return &PollerResponse{Body: body, Headers: headers}
}

// NormalizePollerBody parses a raw response body: valid JSON becomes its
// parsed object, array, string, number, boolean, or null; invalid JSON
// remains the raw string.
func NormalizePollerBody(raw []byte) any {
	var v any
	if len(raw) > 0 && json.Unmarshal(raw, &v) == nil {
		return v
	}
	return string(raw)
}

// PollerPredicateEvaluator evaluates poller until predicates in the frozen
// script sandbox with the normalized response frozen as the "response"
// variable.
type PollerPredicateEvaluator struct {
	funcs *jsfunc.Registry
}

// NewPollerPredicateEvaluator builds the evaluator; funcs may be nil.
func NewPollerPredicateEvaluator(funcs *jsfunc.Registry) *PollerPredicateEvaluator {
	return &PollerPredicateEvaluator{funcs: funcs}
}

// Evaluate runs the until script with the frozen workflow context and the
// frozen response and requires an actual boolean result. Script errors,
// timeouts, and non-boolean results fail the poller.
func (e *PollerPredicateEvaluator) Evaluate(ctx context.Context, until string, timeout time.Duration, wfCtx map[string]any, response *PollerResponse) (bool, error) {
	res, err := RunScript(ctx, ScriptOptions{
		Source:     until,
		Context:    wfCtx,
		Timeout:    timeout,
		Funcs:      e.funcs,
		Frozen:     true,
		Vars:       map[string]any{"response": response},
		FrozenVars: []string{"response"},
	})
	if err != nil {
		return false, err
	}
	matched, ok := res.Value.(bool)
	if !ok {
		return false, fmt.Errorf("until must return a boolean, got %T", res.Value)
	}
	return matched, nil
}

// RedisPollerClient is the narrow redis surface poller nodes need.
// *transport.RedisClient implements it.
type RedisPollerClient interface {
	// Get returns the stored value of key; ok is false when the key is
	// missing (distinct from an empty stored value).
	Get(ctx context.Context, key string) ([]byte, bool, error)
	// Subscribe invokes handler for every message on the exact channel
	// until ctx is cancelled.
	Subscribe(ctx context.Context, channel string, handler func(ctx context.Context, channel string, payload []byte) error) error
}

// RabbitPollerClient is the narrow rabbitmq surface poller nodes need.
// *transport.RabbitClient implements it.
type RabbitPollerClient interface {
	// ConsumeQueue consumes one delivery at a time from the named
	// pre-provisioned queue with manual settlement until ctx is cancelled.
	// A handler error acknowledges the delivery and fails the consume call.
	ConsumeQueue(ctx context.Context, queue, consumerTag string, handler func(msg transport.RabbitPollerMessage) (transport.RabbitPollerSettlement, error)) error
}

// PollerExecutor runs active poller nodes: repeated HTTP requests, Redis
// GET/SUB, and RabbitMQ queue waits, each until its until predicate matches
// or its budget is exhausted. Every attempt occupies the engine worker.
type PollerExecutor struct {
	http   *HTTPExecutor
	pred   *PollerPredicateEvaluator
	redis  RedisPollerClient
	rabbit RabbitPollerClient
}

// NewPollerExecutor builds the poller executor; funcs may be nil.
func NewPollerExecutor(http *HTTPExecutor, funcs *jsfunc.Registry) *PollerExecutor {
	return &PollerExecutor{http: http, pred: NewPollerPredicateEvaluator(funcs)}
}

// SetRedisClient wires the optional redis poller transport. A nil client
// makes redis poller nodes fail with a missing-transport error.
func (e *PollerExecutor) SetRedisClient(client RedisPollerClient) {
	e.redis = client
}

// SetRabbitClient wires the optional rabbitmq poller transport. A nil
// client makes rabbitmq poller nodes fail with a missing-transport error.
func (e *PollerExecutor) SetRabbitClient(client RabbitPollerClient) {
	e.rabbit = client
}

// Execute implements Executor.
func (e *PollerExecutor) Execute(ctx context.Context, req Request) (*Result, error) {
	switch {
	case req.Node.PollerHTTP != nil:
		return e.pollHTTP(ctx, req)
	case req.Node.PollerRedis != nil:
		return e.pollRedis(ctx, req)
	case req.Node.PollerRabbitMQ != nil:
		return e.pollRabbit(ctx, req)
	default:
		return nil, &NodeError{Node: req.Node, Reason: "poller", Err: errors.New("poller node has no transport config")}
	}
}

// templateData returns a shallow copy of the workflow context enriched with
// the reserved automatic template roots workflow_instance_id and
// node_instance_id, which always win over same-named user values. The
// reserved values are only visible while rendering poller configuration and
// are never written back to the workflow context.
func (e *PollerExecutor) templateData(req Request) map[string]any {
	data := make(map[string]any, len(req.Context)+2)
	for k, v := range req.Context {
		data[k] = v
	}
	data["workflow_instance_id"] = req.InstanceID
	data["node_instance_id"] = req.NodeInstanceID
	return data
}

// pollHTTP repeats an outbound HTTP call until the predicate matches or
// max_attempts calls are exhausted. The first request starts immediately;
// delay applies only between attempts. Every call counts toward
// max_attempts, including transport errors; HTTP status codes are responses
// evaluated by the predicate, not transport errors.
func (e *PollerExecutor) pollHTTP(ctx context.Context, req Request) (*Result, error) {
	cfg := req.Node.PollerHTTP
	built, err := e.http.buildConfig(ctx, cfg.URL, cfg.Method, cfg.Headers, cfg.Body, e.templateData(req))
	if err != nil {
		return nil, &NodeError{Node: req.Node, Reason: "poller", Err: err}
	}
	headers := make(map[string]string, len(built.headers)+1)
	if req.IdempotencyKey != "" {
		headers["Idempotency-Key"] = req.IdempotencyKey
	}
	for k, v := range built.headers {
		headers[k] = v
	}

	for attempt := 1; attempt <= cfg.MaxAttempts; attempt++ {
		body, status, respHeaders, err := e.http.Do(ctx, built.method, built.target, headers, built.body, cfg.RequestTimeout)
		if err != nil {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			if attempt == cfg.MaxAttempts {
				return nil, &NodeError{Node: req.Node, Reason: "poller", Err: fmt.Errorf("http polling exhausted %d attempts: %w", cfg.MaxAttempts, err)}
			}
			if err := sleepCtx(ctx, cfg.Delay); err != nil {
				return nil, err
			}
			continue
		}
		outHeaders := make(map[string][]string, len(respHeaders))
		for k, v := range respHeaders {
			outHeaders[k] = v
		}
		resp := NewHTTPPollerResponse(status, outHeaders, NormalizePollerBody(body))
		matched, err := e.pred.Evaluate(ctx, cfg.Until, req.Node.PredicateTimeout, req.Context, resp)
		if err != nil {
			return nil, &NodeError{Node: req.Node, Reason: "poller-until", Err: err}
		}
		if matched {
			return &Result{Output: resp}, nil
		}
		if attempt == cfg.MaxAttempts {
			return nil, &NodeError{Node: req.Node, Reason: "poller", Err: fmt.Errorf("http polling exhausted %d attempts without a match", cfg.MaxAttempts)}
		}
		if err := sleepCtx(ctx, cfg.Delay); err != nil {
			return nil, err
		}
	}
	return nil, &NodeError{Node: req.Node, Reason: "poller", Err: errors.New("http polling exhausted attempts")}
}

// sleepCtx waits d, aborting early when ctx is cancelled.
func sleepCtx(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// pollRedis dispatches a redis poller node to its method implementation.
func (e *PollerExecutor) pollRedis(ctx context.Context, req Request) (*Result, error) {
	cfg := req.Node.PollerRedis
	if e.redis == nil {
		return nil, &NodeError{Node: req.Node, Reason: "poller", Err: errors.New("redis polling client is not configured")}
	}
	if cfg.Method == "SUB" {
		return e.redisSub(ctx, req, cfg)
	}
	return e.redisGet(ctx, req, cfg)
}

// redisGet repeats GET on a rendered key until the predicate matches or
// max_attempts calls are exhausted. The first call starts immediately;
// delay applies only between attempts. A missing key is a normal response
// with body null. Every call, including one ending in a transport error,
// counts toward max_attempts.
func (e *PollerExecutor) redisGet(ctx context.Context, req Request, cfg *model.PollerRedisConfig) (*Result, error) {
	key, err := renderString(cfg.Key, e.templateData(req))
	if err != nil {
		return nil, &NodeError{Node: req.Node, Reason: "poller", Err: err}
	}
	if key == "" {
		return nil, &NodeError{Node: req.Node, Reason: "poller", Err: errors.New("redis key is empty after rendering")}
	}
	for attempt := 1; attempt <= cfg.MaxAttempts; attempt++ {
		callCtx, cancel := context.WithTimeout(ctx, cfg.RequestTimeout)
		val, ok, err := e.redis.Get(callCtx, key)
		cancel()
		if err != nil {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			if attempt == cfg.MaxAttempts {
				return nil, &NodeError{Node: req.Node, Reason: "poller", Err: fmt.Errorf("redis polling exhausted %d attempts: %w", cfg.MaxAttempts, err)}
			}
			if err := sleepCtx(ctx, cfg.Delay); err != nil {
				return nil, err
			}
			continue
		}
		var resp *PollerResponse
		if ok {
			resp = NewRedisPollerResponse(NormalizePollerBody(val))
		} else {
			resp = NewRedisPollerResponse(nil)
		}
		matched, err := e.pred.Evaluate(ctx, cfg.Until, req.Node.PredicateTimeout, req.Context, resp)
		if err != nil {
			return nil, &NodeError{Node: req.Node, Reason: "poller-until", Err: err}
		}
		if matched {
			return &Result{Output: resp}, nil
		}
		if attempt == cfg.MaxAttempts {
			return nil, &NodeError{Node: req.Node, Reason: "poller", Err: fmt.Errorf("redis polling exhausted %d attempts without a match", cfg.MaxAttempts)}
		}
		if err := sleepCtx(ctx, cfg.Delay); err != nil {
			return nil, err
		}
	}
	return nil, &NodeError{Node: req.Node, Reason: "poller", Err: errors.New("redis polling exhausted attempts")}
}

type redisSubResult struct {
	resp *PollerResponse
	err  error
}

// redisSub subscribes to the rendered channel and waits up to max_wait_time
// for a message whose predicate matches. The channel is rendered once;
// nonmatching messages are discarded and consumption continues. Redis
// pub/sub is broadcast: multiple active pollers may accept the same message
// and messages published while nobody is subscribed are lost.
func (e *PollerExecutor) redisSub(ctx context.Context, req Request, cfg *model.PollerRedisConfig) (*Result, error) {
	channel, err := renderString(cfg.Channel, e.templateData(req))
	if err != nil {
		return nil, &NodeError{Node: req.Node, Reason: "poller", Err: err}
	}
	if channel == "" {
		return nil, &NodeError{Node: req.Node, Reason: "poller", Err: errors.New("redis channel is empty after rendering")}
	}
	waitCtx, cancel := context.WithTimeout(ctx, cfg.MaxWaitTime)
	defer cancel()
	results := make(chan redisSubResult, 1)
	handler := func(ctx context.Context, _ string, payload []byte) error {
		resp := NewRedisPollerResponse(NormalizePollerBody(payload))
		matched, err := e.pred.Evaluate(ctx, cfg.Until, req.Node.PredicateTimeout, req.Context, resp)
		if err != nil {
			select {
			case results <- redisSubResult{err: err}:
			case <-waitCtx.Done():
			}
			return nil
		}
		if matched {
			select {
			case results <- redisSubResult{resp: resp}:
			case <-waitCtx.Done():
			}
		}
		return nil
	}
	subDone := make(chan error, 1)
	go func() { subDone <- e.redis.Subscribe(waitCtx, channel, handler) }()
	select {
	case r := <-results:
		if r.err != nil {
			return nil, &NodeError{Node: req.Node, Reason: "poller-until", Err: r.err}
		}
		return &Result{Output: r.resp}, nil
	case <-waitCtx.Done():
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, &NodeError{Node: req.Node, Reason: "poller", Err: fmt.Errorf("redis subscription on %q elapsed without a match after %s", channel, cfg.MaxWaitTime)}
	case err := <-subDone:
		if err != nil {
			return nil, &NodeError{Node: req.Node, Reason: "poller", Err: err}
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, &NodeError{Node: req.Node, Reason: "poller", Err: fmt.Errorf("redis subscription on %q ended without a match", channel)}
	}
}

// renderString renders a template and requires a nonblank string result.
func renderString(tpl string, data map[string]any) (string, error) {
	v, err := contextpath.RenderTemplate(tpl, data)
	if err != nil {
		return "", err
	}
	s, ok := v.(string)
	if !ok {
		return "", fmt.Errorf("template rendered to %T, want string", v)
	}
	return strings.TrimSpace(s), nil
}

type rabbitSubResult struct {
	resp *PollerResponse
	err  error
}

// pollRabbit consumes the rendered pre-provisioned queue and waits up to
// max_wait_time for a delivery whose predicate matches. The queue is
// rendered once and must already exist (the engine never declares queues).
// Every consumed delivery is acknowledged: false messages are discarded,
// the first match completes the node, and predicate or normalization errors
// acknowledge the message before failing the node so a poison message
// cannot requeue.
func (e *PollerExecutor) pollRabbit(ctx context.Context, req Request) (*Result, error) {
	cfg := req.Node.PollerRabbitMQ
	if e.rabbit == nil {
		return nil, &NodeError{Node: req.Node, Reason: "poller", Err: errors.New("rabbitmq polling client is not configured")}
	}
	queue, err := renderString(cfg.Queue, e.templateData(req))
	if err != nil {
		return nil, &NodeError{Node: req.Node, Reason: "poller", Err: err}
	}
	if queue == "" {
		return nil, &NodeError{Node: req.Node, Reason: "poller", Err: errors.New("rabbitmq queue is empty after rendering")}
	}
	waitCtx, cancel := context.WithTimeout(ctx, cfg.MaxWaitTime)
	defer cancel()
	results := make(chan rabbitSubResult, 1)
	handler := func(msg transport.RabbitPollerMessage) (transport.RabbitPollerSettlement, error) {
		resp := NewRabbitPollerResponse(msg.Headers, NormalizePollerBody(msg.Body))
		matched, err := e.pred.Evaluate(ctx, cfg.Until, req.Node.PredicateTimeout, req.Context, resp)
		if err != nil {
			select {
			case results <- rabbitSubResult{err: err}:
			case <-waitCtx.Done():
			}
			return transport.RabbitPollerAck, nil
		}
		if matched {
			select {
			case results <- rabbitSubResult{resp: resp}:
			case <-waitCtx.Done():
			}
		}
		return transport.RabbitPollerAck, nil
	}
	consumeDone := make(chan error, 1)
	go func() { consumeDone <- e.rabbit.ConsumeQueue(waitCtx, queue, "poller-"+req.IdempotencyKey, handler) }()
	select {
	case r := <-results:
		if r.err != nil {
			return nil, &NodeError{Node: req.Node, Reason: "poller-until", Err: r.err}
		}
		return &Result{Output: r.resp}, nil
	case <-waitCtx.Done():
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, &NodeError{Node: req.Node, Reason: "poller", Err: fmt.Errorf("rabbitmq consumption on %q elapsed without a match after %s", queue, cfg.MaxWaitTime)}
	case err := <-consumeDone:
		if err != nil {
			return nil, &NodeError{Node: req.Node, Reason: "poller", Err: err}
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, &NodeError{Node: req.Node, Reason: "poller", Err: fmt.Errorf("rabbitmq consumption on %q ended without a match", queue)}
	}
}
