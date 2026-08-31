package statusupdate

import (
	"context"
	"errors"

	"github.com/simpwf/workflow-engine/internal/workflow/model"
	"github.com/simpwf/workflow-engine/internal/workflow/repository"
	"github.com/simpwf/workflow-engine/internal/workflow/transport"
)

// RedisPublisher publishes status events to the instance's fixed
// workflow:status:<instance> channel. Delivery is best effort: a successful
// publish counts as delivered even with zero subscribers.
type RedisPublisher struct {
	client transport.RedisPublisher
}

// NewRedisPublisher builds the publisher.
func NewRedisPublisher(client transport.RedisPublisher) *RedisPublisher {
	return &RedisPublisher{client: client}
}

// Publish implements Publisher.
func (p *RedisPublisher) Publish(ctx context.Context, cfg *model.StatusUpdateConfig, ev repository.PendingStatusUpdate) error {
	if cfg.Redis == nil {
		return errors.New("status_update redis transport not configured")
	}
	return p.client.PublishJSON(ctx, transport.StatusChannel(ev.WorkflowInstanceID), ev.Payload)
}
