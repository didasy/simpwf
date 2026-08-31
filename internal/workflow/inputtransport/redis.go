// Package inputtransport consumes broker input for waiting workflow
// instances and delivers it through the instance service with the source
// channel matching the transport.
package inputtransport

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/simpwf/workflow-engine/internal/workflow/model"
	"github.com/simpwf/workflow-engine/internal/workflow/service"
	"github.com/simpwf/workflow-engine/internal/workflow/transport"
)

// RedisSubscriber is the pattern-subscribe surface the Redis input consumer
// needs. *transport.RedisClient implements it.
type RedisSubscriber interface {
	PSubscribe(ctx context.Context, pattern string, handler func(ctx context.Context, channel string, payload []byte) error) error
}

// RedisInput consumes the workflow:input:* channel pattern and delivers
// each envelope to its instance. Redis pub/sub is best effort: messages
// published while no consumer is subscribed are lost, and a failed delivery
// is logged while consumption continues.
type RedisInput struct {
	sub RedisSubscriber
	svc service.InstanceService
}

// NewRedisInput builds the consumer.
func NewRedisInput(sub RedisSubscriber, svc service.InstanceService) *RedisInput {
	return &RedisInput{sub: sub, svc: svc}
}

// Run subscribes to every instance's input channel and blocks until ctx is
// cancelled.
func (r *RedisInput) Run(ctx context.Context) error {
	return r.sub.PSubscribe(ctx, transport.InputChannelPattern(), r.handle)
}

func (r *RedisInput) handle(ctx context.Context, channel string, payload []byte) error {
	instanceID := transport.InstanceFromInputChannel(channel)
	if instanceID == "" {
		return fmt.Errorf("inputtransport: %q is not an input channel", channel)
	}
	var env struct {
		IdempotencyKey string          `json:"idempotency_key"`
		Payload        json.RawMessage `json:"payload"`
	}
	if err := json.Unmarshal(payload, &env); err != nil {
		return fmt.Errorf("inputtransport: decode redis envelope: %w", err)
	}
	if env.IdempotencyKey == "" {
		return errors.New("inputtransport: redis envelope requires idempotency_key")
	}
	if len(env.Payload) == 0 || string(env.Payload) == "null" {
		return errors.New("inputtransport: redis envelope requires payload")
	}
	_, err := r.svc.DeliverInput(ctx, service.DeliverInput{
		InstanceID:     instanceID,
		IdempotencyKey: env.IdempotencyKey,
		Payload:        env.Payload,
		Source:         model.InputChannelRedis,
	})
	if err != nil {
		return fmt.Errorf("inputtransport: deliver redis input: %w", err)
	}
	return nil
}
