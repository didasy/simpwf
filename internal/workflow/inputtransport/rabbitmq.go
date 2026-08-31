package inputtransport

import (
	"context"
	"errors"
	"fmt"

	"github.com/simpwf/workflow-engine/internal/workflow/model"
	"github.com/simpwf/workflow-engine/internal/workflow/repository"
	"github.com/simpwf/workflow-engine/internal/workflow/service"
	"github.com/simpwf/workflow-engine/internal/workflow/transport"
)

// RabbitConsumer is the consume surface the RabbitMQ input consumer needs.
// *transport.RabbitClient implements it.
type RabbitConsumer interface {
	Consume(ctx context.Context, consumerTag string, handler transport.ConsumeFunc) error
}

// RabbitInput consumes the configured input queue and delivers each message
// to its instance. Deliveries are settled manually: permanent outcomes are
// acknowledged or rejected without requeue, and transient repository
// failures are requeued with a bounded backoff by the client.
type RabbitInput struct {
	cons RabbitConsumer
	svc  service.InstanceService
	tag  string
}

// NewRabbitInput builds the consumer. consumerTag names this worker's
// consumer in RabbitMQ.
func NewRabbitInput(cons RabbitConsumer, svc service.InstanceService, consumerTag string) *RabbitInput {
	return &RabbitInput{cons: cons, svc: svc, tag: consumerTag}
}

// Run consumes until ctx is cancelled.
func (r *RabbitInput) Run(ctx context.Context) error {
	return r.cons.Consume(ctx, r.tag, r.handle)
}

func (r *RabbitInput) handle(ctx context.Context, msg transport.RabbitMessage) (transport.ConsumeResult, error) {
	instanceID := msg.Headers["NodeInstanceId"]
	idempotencyKey := msg.Headers["IdempotencyKey"]
	if idempotencyKey == "" {
		// AMQP message_id is the idempotency fallback.
		idempotencyKey = msg.MessageID
	}
	if instanceID == "" || idempotencyKey == "" {
		// Missing routing metadata can never succeed: reject without
		// requeue so the malformed message does not loop.
		return transport.ConsumeReject, nil
	}
	_, err := r.svc.DeliverInput(ctx, service.DeliverInput{
		InstanceID:     instanceID,
		IdempotencyKey: idempotencyKey,
		Payload:        msg.Body,
		Source:         model.InputChannelRabbitMQ,
	})
	if err != nil {
		if isPermanentInputError(err) {
			return transport.ConsumeReject, nil
		}
		return transport.ConsumeRequeue, fmt.Errorf("inputtransport: deliver rabbit input: %w", err)
	}
	return transport.ConsumeAck, nil
}

// isPermanentInputError classifies a delivery failure as never retryable.
// Domain sentinel errors (invalid input, conflicts, missing rows) are
// permanent; any other error (e.g. a database outage) is treated as
// transient and requeued.
func isPermanentInputError(err error) bool {
	return errors.Is(err, model.ErrInvalid) ||
		errors.Is(err, model.ErrConflict) ||
		errors.Is(err, model.ErrNotFound) ||
		errors.Is(err, repository.ErrInstanceNotFound) ||
		errors.Is(err, repository.ErrNodeInstanceNotFound) ||
		errors.Is(err, repository.ErrDeliveryNotFound)
}
