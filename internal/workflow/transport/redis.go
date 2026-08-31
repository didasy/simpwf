// Package transport provides the optional Redis and RabbitMQ broker
// adapters behind narrow publish interfaces. The engine works without
// brokers: adapters are constructed only when their DSN is configured, and
// consumers depend on the narrow interfaces so tests can substitute fakes.
package transport

import (
	"context"
	"errors"
	"fmt"

	"github.com/redis/go-redis/v9"
)

// RedisPublisher is the narrow publish surface shared by the output node
// executor and the status publishers.
type RedisPublisher interface {
	// PublishJSON publishes payload to a channel. A successful publish
	// counts as delivery even when no subscriber is listening.
	PublishJSON(ctx context.Context, channel string, payload []byte) error
}

// Channel name helpers for the fixed per-instance pub/sub channels.
const (
	inputChannelPrefix  = "workflow:input:"
	outputChannelPrefix = "workflow:output:"
	statusChannelPrefix = "workflow:status:"
)

// InputChannel is the Redis channel an input node of the instance consumes.
func InputChannel(instanceID string) string { return inputChannelPrefix + instanceID }

// InputChannelPattern matches every instance's input channel.
func InputChannelPattern() string { return inputChannelPrefix + "*" }

// InstanceFromInputChannel extracts the instance id from an input channel.
// It returns the empty string when the channel is not an input channel.
func InstanceFromInputChannel(channel string) string {
	if len(channel) <= len(inputChannelPrefix) || channel[:len(inputChannelPrefix)] != inputChannelPrefix {
		return ""
	}
	return channel[len(inputChannelPrefix):]
}

// OutputChannel is the Redis channel an output node of the instance
// publishes to.
func OutputChannel(instanceID string) string { return outputChannelPrefix + instanceID }

// StatusChannel is the Redis channel status-update events of the instance
// are published to.
func StatusChannel(instanceID string) string { return statusChannelPrefix + instanceID }

// RedisClient wraps a go-redis client for publishing and pattern
// subscription.
type RedisClient struct {
	rdb *redis.Client
}

// NewRedisClient connects to the DSN and pings. A failed ping returns an
// error so startup fails fast on a configured-but-unreachable broker.
func NewRedisClient(ctx context.Context, dsn string) (*RedisClient, error) {
	opts, err := redis.ParseURL(dsn)
	if err != nil {
		return nil, fmt.Errorf("redis: parse dsn: %w", err)
	}
	rdb := redis.NewClient(opts)
	if err := rdb.Ping(ctx).Err(); err != nil {
		_ = rdb.Close()
		return nil, fmt.Errorf("redis: ping: %w", err)
	}
	return &RedisClient{rdb: rdb}, nil
}

// PublishJSON implements RedisPublisher.
func (c *RedisClient) PublishJSON(ctx context.Context, channel string, payload []byte) error {
	return c.rdb.Publish(ctx, channel, payload).Err()
}

// Get returns the stored value of key. ok is false when the key does not
// exist, distinguishing a missing key from an empty stored value.
func (c *RedisClient) Get(ctx context.Context, key string) ([]byte, bool, error) {
	val, err := c.rdb.Get(ctx, key).Bytes()
	if errors.Is(err, redis.Nil) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	return val, true, nil
}

// Subscribe invokes handler for every message on the exact channel until
// ctx is cancelled, then returns nil. Redis pub/sub is best effort: a
// handler error is logged and consumption continues rather than tearing the
// subscription down.
func (c *RedisClient) Subscribe(ctx context.Context, channel string, handler func(ctx context.Context, channel string, payload []byte) error) error {
	pubsub := c.rdb.Subscribe(ctx, channel)
	defer func() { _ = pubsub.Close() }()
	ch := pubsub.Channel()
	for {
		select {
		case <-ctx.Done():
			return nil
		case msg, ok := <-ch:
			if !ok {
				return nil
			}
			if err := handler(ctx, msg.Channel, []byte(msg.Payload)); err != nil {
				logf("redis: handler failed", "channel", msg.Channel, "error", err)
			}
		}
	}
}

// PSubscribe invokes handler for every message on channels matching pattern
// until ctx is cancelled, then returns nil. Redis pub/sub is best effort: a
// handler error is logged and consumption continues rather than tearing the
// subscription down.
func (c *RedisClient) PSubscribe(ctx context.Context, pattern string, handler func(ctx context.Context, channel string, payload []byte) error) error {
	pubsub := c.rdb.PSubscribe(ctx, pattern)
	defer func() { _ = pubsub.Close() }()
	ch := pubsub.Channel()
	for {
		select {
		case <-ctx.Done():
			return nil
		case msg, ok := <-ch:
			if !ok {
				return nil
			}
			if err := handler(ctx, msg.Channel, []byte(msg.Payload)); err != nil {
				logf("redis: input handler failed", "channel", msg.Channel, "error", err)
			}
		}
	}
}

// Close shuts the client down.
func (c *RedisClient) Close() error {
	return c.rdb.Close()
}
