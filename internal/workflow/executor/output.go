package executor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/simpwf/workflow-engine/internal/workflow/model"
	"github.com/simpwf/workflow-engine/internal/workflow/transport"
	"github.com/simpwf/workflow-engine/pkg/contextpath"
)

// PublishReceipt is the metadata returned by a successful output publish;
// it is written to the workflow context as the node output through the
// normal output_property behavior.
type PublishReceipt struct {
	Channel     string `json:"channel"`
	Destination string `json:"destination"`
	MessageID   string `json:"message_id"`
}

// OutputPublisher publishes an output node's payload to its broker channel.
type OutputPublisher interface {
	// Publish sends payload to the node's transport ("redis" or
	// "rabbitmq"). instanceID seeds the redis channel name; messageID is
	// the stable execution id stamped as the AMQP message id for rabbitmq.
	Publish(ctx context.Context, channel, instanceID string, payload []byte, messageID string) (*PublishReceipt, error)
}

// BrokerOutputPublisher routes output-node payloads to the configured
// broker. A nil client for a channel makes that channel fail at execution
// time, so broker-disabled deployments surface the missing transport as a
// normal node failure.
type BrokerOutputPublisher struct {
	redis       transport.RedisPublisher
	rabbit      transport.RabbitPublisher
	rabbitQueue string
}

// NewBrokerOutputPublisher builds the adapter. rabbitQueue is the configured
// durable output queue used when a node's channel is rabbitmq.
func NewBrokerOutputPublisher(redis transport.RedisPublisher, rabbit transport.RabbitPublisher, rabbitQueue string) *BrokerOutputPublisher {
	return &BrokerOutputPublisher{redis: redis, rabbit: rabbit, rabbitQueue: rabbitQueue}
}

// Publish implements OutputPublisher.
func (p *BrokerOutputPublisher) Publish(ctx context.Context, channel, instanceID string, payload []byte, messageID string) (*PublishReceipt, error) {
	switch channel {
	case model.InputChannelRedis:
		if p.redis == nil {
			return nil, errors.New("redis output publisher is not configured")
		}
		dest := transport.OutputChannel(instanceID)
		if err := p.redis.PublishJSON(ctx, dest, payload); err != nil {
			return nil, err
		}
		return &PublishReceipt{Channel: channel, Destination: dest, MessageID: messageID}, nil
	case model.InputChannelRabbitMQ:
		if p.rabbit == nil {
			return nil, errors.New("rabbitmq output publisher is not configured")
		}
		headers := map[string]string{
			"NodeInstanceId": instanceID,
			"IdempotencyKey": messageID,
		}
		if err := p.rabbit.PublishJSON(ctx, p.rabbitQueue, payload, messageID, headers); err != nil {
			return nil, err
		}
		return &PublishReceipt{Channel: channel, Destination: p.rabbitQueue, MessageID: messageID}, nil
	default:
		return nil, fmt.Errorf("output channel %q is not supported", channel)
	}
}

// OutputExecutor resolves the value at the node's context_path, publishes
// its exact JSON to the broker channel, and returns the receipt as the node
// output. Publish and resolution errors fail the node normally.
type OutputExecutor struct {
	publisher OutputPublisher
}

// Execute implements Executor.
func (e *OutputExecutor) Execute(ctx context.Context, req Request) (*Result, error) {
	if e.publisher == nil {
		return nil, &NodeError{Node: req.Node, Reason: "output", Err: errors.New("output publisher is not configured")}
	}
	v, err := contextpath.Get(req.Context, req.Node.ContextPath)
	if err != nil {
		return nil, &NodeError{Node: req.Node, Reason: "output", Err: fmt.Errorf("resolve context_path %q: %w", req.Node.ContextPath, err)}
	}
	payload, err := json.Marshal(v)
	if err != nil {
		return nil, &NodeError{Node: req.Node, Reason: "output", Err: err}
	}
	receipt, err := e.publisher.Publish(ctx, req.Node.Channel, req.InstanceID, payload, req.IdempotencyKey)
	if err != nil {
		return nil, &NodeError{Node: req.Node, Reason: "output", Err: err}
	}
	return &Result{Output: receipt}, nil
}
