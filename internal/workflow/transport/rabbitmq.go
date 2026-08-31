package transport

import (
	"context"
	"fmt"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// RabbitPublisher is the narrow publish surface shared by the output node
// executor and the status publishers. Messages are persistent and
// publisher-confirmed; messageID is stamped as the AMQP message id.
type RabbitPublisher interface {
	PublishJSON(ctx context.Context, queue string, payload []byte, messageID string, headers map[string]string) error
}

// HeaderValue returns the string form of an AMQP header value; non-string
// values (numbers, booleans, timestamps) are rendered with %v.
func HeaderValue(v any) string {
	switch val := v.(type) {
	case nil:
		return ""
	case string:
		return val
	default:
		return fmt.Sprintf("%v", val)
	}
}

// ConsumeResult tells the consumer how to settle a delivery.
type ConsumeResult int

const (
	// ConsumeAck acknowledges the delivery as processed.
	ConsumeAck ConsumeResult = iota
	// ConsumeRequeue requeues the delivery for another attempt.
	ConsumeRequeue
	// ConsumeReject dead-letters the delivery (nack without requeue).
	ConsumeReject
)

// RabbitMessage is one consumed input delivery.
type RabbitMessage struct {
	Body      []byte
	MessageID string
	Headers   map[string]string
}

// RabbitPollerMessage is one consumed poller delivery with
// string-normalized headers.
type RabbitPollerMessage struct {
	Body    []byte
	Headers map[string]string
}

// RabbitPollerSettlement instructs ConsumeQueue how to settle a delivery.
type RabbitPollerSettlement int

const (
	// RabbitPollerAck acknowledges the delivery as processed.
	RabbitPollerAck RabbitPollerSettlement = iota
	// RabbitPollerReject nacks the delivery without requeue.
	RabbitPollerReject
)

// ConsumeFunc handles one input delivery and decides its settlement. An
// error from the handler is treated as a transient failure and requeued
// after a bounded backoff.
type ConsumeFunc func(ctx context.Context, msg RabbitMessage) (ConsumeResult, error)

// RabbitClient wraps an amqp connection with separate publisher and
// consumer channels and declares the three durable queues.
type RabbitClient struct {
	conn       *amqp.Connection
	pub        *amqp.Channel
	cons       *amqp.Channel
	inputQueue string
	closeMu    sync.Mutex
	closed     bool
}

// NewRabbitClient dials the DSN, enables publisher confirms, declares the
// input/output/status queues as durable, and configures the consumer
// channel with a prefetch of 1. A dial or declaration failure returns an
// error so startup fails fast on a configured-but-unreachable broker.
func NewRabbitClient(ctx context.Context, dsn, inputQueue, outputQueue, statusQueue string) (*RabbitClient, error) {
	conn, err := amqp.Dial(dsn)
	if err != nil {
		return nil, fmt.Errorf("rabbitmq: dial: %w", err)
	}
	closeOnErr := func(err error) (*RabbitClient, error) {
		_ = conn.Close()
		return nil, err
	}
	pub, err := conn.Channel()
	if err != nil {
		return closeOnErr(fmt.Errorf("rabbitmq: publisher channel: %w", err))
	}
	cons, err := conn.Channel()
	if err != nil {
		return closeOnErr(fmt.Errorf("rabbitmq: consumer channel: %w", err))
	}
	if err := pub.Confirm(false); err != nil {
		return closeOnErr(fmt.Errorf("rabbitmq: confirm mode: %w", err))
	}
	for _, queue := range []string{inputQueue, outputQueue, statusQueue} {
		if _, err := pub.QueueDeclare(queue, true, false, false, false, nil); err != nil {
			return closeOnErr(fmt.Errorf("rabbitmq: declare queue %s: %w", queue, err))
		}
	}
	if err := cons.Qos(1, 0, false); err != nil {
		return closeOnErr(fmt.Errorf("rabbitmq: qos: %w", err))
	}
	return &RabbitClient{conn: conn, pub: pub, cons: cons, inputQueue: inputQueue}, nil
}

// PublishJSON implements RabbitPublisher: a mandatory, persistent,
// publisher-confirmed publish to the named queue (default exchange routes
// by queue name). Returns an error when the publish is rejected or not
// acknowledged before ctx is done.
func (c *RabbitClient) PublishJSON(ctx context.Context, queue string, payload []byte, messageID string, headers map[string]string) error {
	table := make(amqp.Table, len(headers))
	for k, v := range headers {
		table[k] = v
	}
	deferred, err := c.pub.PublishWithDeferredConfirmWithContext(ctx, "", queue, true, false, amqp.Publishing{
		DeliveryMode: amqp.Persistent,
		ContentType:  "application/json",
		MessageId:    messageID,
		Headers:      table,
		Body:         payload,
	})
	if err != nil {
		return fmt.Errorf("rabbitmq: publish to %s: %w", queue, err)
	}
	acked, err := deferred.WaitContext(ctx)
	if err != nil {
		return fmt.Errorf("rabbitmq: confirm publish to %s: %w", queue, err)
	}
	if !acked {
		return fmt.Errorf("rabbitmq: publish to %s not acknowledged", queue)
	}
	return nil
}

// Consume delivers input messages to handler until ctx is cancelled or the
// connection fails, then returns. Deliveries are acknowledged manually: a
// nil handler error acknowledges, a transient error requeues after a
// bounded backoff, and a ConsumeReject result nacks without requeue.
func (c *RabbitClient) Consume(ctx context.Context, consumerTag string, handler ConsumeFunc) error {
	deliveries, err := c.cons.Consume(c.inputQueue, consumerTag, false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("rabbitmq: consume %s: %w", c.inputQueue, err)
	}
	failures := 0
	for {
		select {
		case <-ctx.Done():
			return nil
		case d, ok := <-deliveries:
			if !ok {
				return fmt.Errorf("rabbitmq: consumer %s closed", consumerTag)
			}
			msg := RabbitMessage{
				Body:      d.Body,
				MessageID: d.MessageId,
				Headers:   make(map[string]string, len(d.Headers)),
			}
			for k, v := range d.Headers {
				msg.Headers[k] = HeaderValue(v)
			}
			res, err := handler(ctx, msg)
			if err != nil {
				failures++
				time.Sleep(requeueBackoff(failures))
				_ = d.Nack(false, true)
				continue
			}
			failures = 0
			switch res {
			case ConsumeAck:
				_ = d.Ack(false)
			case ConsumeRequeue:
				_ = d.Nack(false, true)
			case ConsumeReject:
				_ = d.Nack(false, false)
			}
		}
	}
}

// ConsumeQueue consumes one delivery at a time from the named
// pre-provisioned queue with manual settlement, over a fresh channel
// created per call. The queue is checked passively and never declared or
// created; it must already exist and be exclusive to one active poller
// execution. The channel closes when the call returns or is cancelled, and
// the fixed input consumer channel is never reused. A handler error
// acknowledges the delivery (poller policy: never requeue predicate
// failures) and fails the consume call.
func (c *RabbitClient) ConsumeQueue(ctx context.Context, queue, consumerTag string, handler func(msg RabbitPollerMessage) (RabbitPollerSettlement, error)) error {
	ch, err := c.conn.Channel()
	if err != nil {
		return fmt.Errorf("rabbitmq: poller channel: %w", err)
	}
	defer func() { _ = ch.Close() }()
	if _, err := ch.QueueDeclarePassive(queue, true, false, false, false, nil); err != nil {
		return fmt.Errorf("rabbitmq: poller queue %s: %w", queue, err)
	}
	if err := ch.Qos(1, 0, false); err != nil {
		return fmt.Errorf("rabbitmq: poller qos: %w", err)
	}
	deliveries, err := ch.Consume(queue, consumerTag, false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("rabbitmq: poller consume %s: %w", queue, err)
	}
	for {
		select {
		case <-ctx.Done():
			return nil
		case d, ok := <-deliveries:
			if !ok {
				return fmt.Errorf("rabbitmq: poller consumer %s closed", consumerTag)
			}
			msg := RabbitPollerMessage{
				Body:    d.Body,
				Headers: make(map[string]string, len(d.Headers)),
			}
			for k, v := range d.Headers {
				msg.Headers[k] = HeaderValue(v)
			}
			settle, err := handler(msg)
			if err != nil {
				_ = d.Ack(false)
				return fmt.Errorf("rabbitmq: poller handler: %w", err)
			}
			switch settle {
			case RabbitPollerAck:
				_ = d.Ack(false)
			case RabbitPollerReject:
				_ = d.Nack(false, false)
			}
		}
	}
}

// requeueBackoff bounds the sleep before requeueing a transiently failed
// delivery: 100ms, 200ms, ... capped at 10s, reset on the next success.
func requeueBackoff(failures int) time.Duration {
	d := 100 * time.Millisecond
	for i := 1; i < failures && d < 10*time.Second; i++ {
		d *= 2
	}
	if d > 10*time.Second {
		return 10 * time.Second
	}
	return d
}

// Close shuts the channels and connection down. It is safe to call
// multiple times.
func (c *RabbitClient) Close() error {
	c.closeMu.Lock()
	defer c.closeMu.Unlock()
	if c.closed {
		return nil
	}
	c.closed = true
	_ = c.cons.Close()
	_ = c.pub.Close()
	return c.conn.Close()
}
