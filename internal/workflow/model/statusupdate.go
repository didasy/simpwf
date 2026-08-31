package model

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"
)

// Default status-update webhook settings applied when the definition omits
// them. MaxRetry counts retries after the initial attempt.
const (
	defaultStatusUpdateMaxRetry   = 3
	defaultStatusUpdateRetryDelay = 5 * time.Second
)

// StatusUpdateConfig is the notification configuration embedded at the top
// level of a workflow definition's content. Any combination of transports
// may be configured; one outbox row is enqueued per transport per event,
// all sharing one logical event id, and each transport retries
// independently.
type StatusUpdateConfig struct {
	HTTP     *HTTPStatusUpdateConfig   `json:"http"`
	Redis    *RedisStatusUpdateConfig  `json:"redis"`
	RabbitMQ *RabbitStatusUpdateConfig `json:"rabbitmq"`
}

// HTTPStatusUpdateConfig describes the outbound webhook called on externally
// meaningful workflow-instance status changes.
type HTTPStatusUpdateConfig struct {
	URL        string
	Method     string
	Headers    map[string]string
	MaxRetry   int
	RetryDelay time.Duration
}

// RedisStatusUpdateConfig enables publishing status events to the fixed
// workflow:status:<instance> channel. Only the retry policy is
// configurable; the channel name is fixed.
type RedisStatusUpdateConfig struct {
	MaxRetry   int
	RetryDelay time.Duration
}

// RabbitStatusUpdateConfig enables publishing status events to the
// configured status queue. Only the retry policy is configurable; the queue
// comes from the infrastructure configuration.
type RabbitStatusUpdateConfig struct {
	MaxRetry   int
	RetryDelay time.Duration
}

// rawStatusUpdateHTTP mirrors the JSON shape of the http transport for
// lenient parsing, keeping absent values distinguishable from explicit ones.
type rawStatusUpdateHTTP struct {
	URL        string            `json:"url"`
	Method     *string           `json:"method"`
	Headers    map[string]string `json:"headers"`
	MaxRetry   *int              `json:"max_retry"`
	RetryDelay *string           `json:"retry_delay"`
}

// rawStatusUpdateRetry mirrors the retry fields shared by the redis and
// rabbitmq transports.
type rawStatusUpdateRetry struct {
	MaxRetry   *int    `json:"max_retry"`
	RetryDelay *string `json:"retry_delay"`
}

// statusUpdateMethods is the set of HTTP methods accepted for webhooks.
var statusUpdateMethods = map[string]bool{
	"GET": true, "POST": true, "PUT": true, "PATCH": true, "DELETE": true, "HEAD": true,
}

// Status-update event names. waiting_for_input is emitted when the engine
// parks an instance on an input node; input_received when a delivered payload
// is accepted; the rest mirror the workflow-instance statuses.
const (
	StatusUpdateEventWaitingForInput = "waiting_for_input"
	StatusUpdateEventInputReceived   = "input_received"
	StatusUpdateEventPaused          = "paused"
	StatusUpdateEventResumed         = "resumed"
	StatusUpdateEventFinished        = "finished"
	StatusUpdateEventFailed          = "failed"
	StatusUpdateEventStopped         = "stopped"

	// StatusUpdateEventType is the fixed type discriminator of webhook
	// payloads.
	StatusUpdateEventType = "workflow.status_changed"

	// StatusUpdateTransportHTTP is the http outbox transport.
	StatusUpdateTransportHTTP = "http"
	// StatusUpdateTransportRedis is the redis pub/sub outbox transport.
	StatusUpdateTransportRedis = "redis"
	// StatusUpdateTransportRabbitMQ is the rabbitmq queue outbox transport.
	StatusUpdateTransportRabbitMQ = "rabbitmq"
)

// StatusUpdateEventPayload is the immutable webhook body delivered for one
// status-change event. It deliberately carries no workflow context.
type StatusUpdateEventPayload struct {
	ID                   string    `json:"id"`
	Type                 string    `json:"type"`
	Event                string    `json:"event"`
	OccurredAt           time.Time `json:"occurred_at"`
	WorkflowDefinitionID string    `json:"workflow_definition_id"`
	WorkflowInstanceID   string    `json:"workflow_instance_id"`
	FromStatus           string    `json:"from_status"`
	ToStatus             string    `json:"to_status"`
	FromWaitingReason    string    `json:"from_waiting_reason"`
	ToWaitingReason      string    `json:"to_waiting_reason"`
	Revision             int64     `json:"revision"`
	Error                string    `json:"error"`
}

// ParseStatusUpdate extracts and validates the top-level status_update
// configuration from workflow definition content. A missing or null
// status_update block returns (nil, nil). At least one transport block is
// required.
func ParseStatusUpdate(content json.RawMessage) (*StatusUpdateConfig, error) {
	if len(content) == 0 {
		return nil, nil
	}
	var wf struct {
		StatusUpdate json.RawMessage `json:"status_update"`
	}
	if err := json.Unmarshal(content, &wf); err != nil {
		return nil, err
	}
	if len(wf.StatusUpdate) == 0 || string(wf.StatusUpdate) == "null" {
		return nil, nil
	}
	var raw struct {
		HTTP     *rawStatusUpdateHTTP  `json:"http"`
		Redis    *rawStatusUpdateRetry `json:"redis"`
		RabbitMQ *rawStatusUpdateRetry `json:"rabbitmq"`
	}
	if err := json.Unmarshal(wf.StatusUpdate, &raw); err != nil {
		return nil, fmt.Errorf("parse status_update: %w", err)
	}
	if raw.HTTP == nil && raw.Redis == nil && raw.RabbitMQ == nil {
		return nil, errors.New("status_update requires at least one of http, redis, rabbitmq")
	}
	cfg := &StatusUpdateConfig{}
	if raw.HTTP != nil {
		h, err := parseHTTPStatusUpdate(raw.HTTP)
		if err != nil {
			return nil, err
		}
		cfg.HTTP = h
	}
	if raw.Redis != nil {
		maxRetry, retryDelay, err := parseStatusUpdateRetry(raw.Redis, "redis")
		if err != nil {
			return nil, err
		}
		cfg.Redis = &RedisStatusUpdateConfig{MaxRetry: maxRetry, RetryDelay: retryDelay}
	}
	if raw.RabbitMQ != nil {
		maxRetry, retryDelay, err := parseStatusUpdateRetry(raw.RabbitMQ, "rabbitmq")
		if err != nil {
			return nil, err
		}
		cfg.RabbitMQ = &RabbitStatusUpdateConfig{MaxRetry: maxRetry, RetryDelay: retryDelay}
	}
	return cfg, nil
}

// Transports returns the configured transport names in canonical order
// (http, redis, rabbitmq).
func (c *StatusUpdateConfig) Transports() []string {
	var out []string
	if c.HTTP != nil {
		out = append(out, StatusUpdateTransportHTTP)
	}
	if c.Redis != nil {
		out = append(out, StatusUpdateTransportRedis)
	}
	if c.RabbitMQ != nil {
		out = append(out, StatusUpdateTransportRabbitMQ)
	}
	return out
}

// RetryPolicy returns the retry settings of one transport. ok is false when
// the transport is not configured.
func (c *StatusUpdateConfig) RetryPolicy(tr string) (maxRetry int, retryDelay time.Duration, ok bool) {
	switch tr {
	case StatusUpdateTransportHTTP:
		if c.HTTP == nil {
			return 0, 0, false
		}
		return c.HTTP.MaxRetry, c.HTTP.RetryDelay, true
	case StatusUpdateTransportRedis:
		if c.Redis == nil {
			return 0, 0, false
		}
		return c.Redis.MaxRetry, c.Redis.RetryDelay, true
	case StatusUpdateTransportRabbitMQ:
		if c.RabbitMQ == nil {
			return 0, 0, false
		}
		return c.RabbitMQ.MaxRetry, c.RabbitMQ.RetryDelay, true
	}
	return 0, 0, false
}

func parseStatusUpdateRetry(r *rawStatusUpdateRetry, name string) (int, time.Duration, error) {
	maxRetry := defaultStatusUpdateMaxRetry
	if r.MaxRetry != nil {
		if *r.MaxRetry < 0 {
			return 0, 0, fmt.Errorf("status_update %s max_retry must be >= 0", name)
		}
		maxRetry = *r.MaxRetry
	}
	retryDelay := defaultStatusUpdateRetryDelay
	if r.RetryDelay != nil {
		d, err := time.ParseDuration(*r.RetryDelay)
		if err != nil || d <= 0 {
			return 0, 0, fmt.Errorf("status_update %s retry_delay %q must be a positive duration", name, *r.RetryDelay)
		}
		retryDelay = d
	}
	return maxRetry, retryDelay, nil
}

func parseHTTPStatusUpdate(r *rawStatusUpdateHTTP) (*HTTPStatusUpdateConfig, error) {
	u, err := url.Parse(r.URL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return nil, fmt.Errorf("status_update http url %q must be an absolute http(s) url", r.URL)
	}
	method := "POST"
	if r.Method != nil {
		method = strings.ToUpper(*r.Method)
	}
	if !statusUpdateMethods[method] {
		return nil, fmt.Errorf("status_update http method %q is not supported", method)
	}
	headers := make(map[string]string, len(r.Headers))
	for k, v := range r.Headers {
		if strings.TrimSpace(k) == "" {
			return nil, errors.New("status_update http header names must be non-empty")
		}
		if strings.ContainsAny(k, "\r\n") || strings.ContainsAny(v, "\r\n") {
			return nil, fmt.Errorf("status_update http header %q contains a newline", k)
		}
		headers[k] = v
	}
	maxRetry := defaultStatusUpdateMaxRetry
	if r.MaxRetry != nil {
		if *r.MaxRetry < 0 {
			return nil, errors.New("status_update http max_retry must be >= 0")
		}
		maxRetry = *r.MaxRetry
	}
	retryDelay := defaultStatusUpdateRetryDelay
	if r.RetryDelay != nil {
		d, err := time.ParseDuration(*r.RetryDelay)
		if err != nil || d <= 0 {
			return nil, fmt.Errorf("status_update http retry_delay %q must be a positive duration", *r.RetryDelay)
		}
		retryDelay = d
	}
	return &HTTPStatusUpdateConfig{
		URL:        r.URL,
		Method:     method,
		Headers:    headers,
		MaxRetry:   maxRetry,
		RetryDelay: retryDelay,
	}, nil
}
