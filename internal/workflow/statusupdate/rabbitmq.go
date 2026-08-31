package statusupdate

import (
	"context"
	"errors"

	"github.com/simpwf/workflow-engine/internal/workflow/model"
	"github.com/simpwf/workflow-engine/internal/workflow/repository"
	"github.com/simpwf/workflow-engine/internal/workflow/transport"
)

// RabbitPublisher publishes status events to the configured status queue as
// persistent, publisher-confirmed messages carrying the logical event id as
// the AMQP message id and Idempotency-Key header for cross-transport
// deduplication.
type RabbitPublisher struct {
	client      transport.RabbitPublisher
	statusQueue string
}

// NewRabbitPublisher builds the publisher for the configured status queue.
func NewRabbitPublisher(client transport.RabbitPublisher, statusQueue string) *RabbitPublisher {
	return &RabbitPublisher{client: client, statusQueue: statusQueue}
}

// Publish implements Publisher.
func (p *RabbitPublisher) Publish(ctx context.Context, cfg *model.StatusUpdateConfig, ev repository.PendingStatusUpdate) error {
	if cfg.RabbitMQ == nil {
		return errors.New("status_update rabbitmq transport not configured")
	}
	headers := map[string]string{
		"NodeInstanceId": ev.WorkflowInstanceID,
		"IdempotencyKey": ev.LogicalID,
	}
	return p.client.PublishJSON(ctx, p.statusQueue, ev.Payload, ev.LogicalID, headers)
}
