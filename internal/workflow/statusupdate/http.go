// Package statusupdate delivers workflow status-change notifications from
// the transactional outbox to their configured transports. The http, redis,
// and rabbitmq publishers all implement the same Publisher interface and
// are routed by the outbox row's transport.
package statusupdate

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/simpwf/workflow-engine/internal/workflow/executor"
	"github.com/simpwf/workflow-engine/internal/workflow/model"
	"github.com/simpwf/workflow-engine/internal/workflow/repository"
)

// DefaultHTTPTimeout bounds each webhook request.
const DefaultHTTPTimeout = 10 * time.Second

// HTTPPublisher sends status-update events as HTTP requests under the engine
// outbound security policy (allowlist, DNS, redirect revalidation). Any
// non-2xx response fails the delivery.
type HTTPPublisher struct {
	client  *executor.HTTPExecutor
	timeout time.Duration
}

// NewHTTPPublisher builds an HTTP publisher. A non-positive timeout falls
// back to DefaultHTTPTimeout.
func NewHTTPPublisher(client *executor.HTTPExecutor, timeout time.Duration) *HTTPPublisher {
	if timeout <= 0 {
		timeout = DefaultHTTPTimeout
	}
	return &HTTPPublisher{client: client, timeout: timeout}
}

// Publish delivers one event. The logical event id travels as both
// Idempotency-Key and X-SimpWF-Event-ID so receivers can dedupe
// at-least-once deliveries across all transports.
func (p *HTTPPublisher) Publish(ctx context.Context, cfg *model.StatusUpdateConfig, ev repository.PendingStatusUpdate) error {
	if cfg.HTTP == nil {
		return errors.New("status_update http transport not configured")
	}
	httpCfg := cfg.HTTP
	method := httpCfg.Method
	if method == "" {
		method = "POST"
	}
	headers := map[string]string{
		"Content-Type":      "application/json",
		"Idempotency-Key":   ev.LogicalID,
		"X-SimpWF-Event-ID": ev.LogicalID,
	}
	for k, v := range httpCfg.Headers {
		headers[k] = v
	}
	_, code, _, err := p.client.Do(ctx, method, httpCfg.URL, headers, ev.Payload, p.timeout)
	if err != nil {
		return fmt.Errorf("status webhook request: %w", err)
	}
	if code < 200 || code >= 300 {
		return fmt.Errorf("status webhook returned %d", code)
	}
	return nil
}
