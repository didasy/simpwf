package transport

import (
	"context"
	"testing"
	"time"
)

// redisPollerContract is the poller-facing surface of the redis client:
// exact-channel subscription plus keyed GET with missing-key detection.
type redisPollerContract interface {
	Get(ctx context.Context, key string) ([]byte, bool, error)
	Subscribe(ctx context.Context, channel string, handler func(ctx context.Context, channel string, payload []byte) error) error
	PSubscribe(ctx context.Context, pattern string, handler func(ctx context.Context, channel string, payload []byte) error) error
}

func TestRedisClientPollerContract(t *testing.T) {
	var _ redisPollerContract = (*RedisClient)(nil)
}

// rabbitPollerContract is the poller-facing surface of the rabbitmq client:
// per-execution arbitrary-queue consumption with explicit settlement.
type rabbitPollerContract interface {
	ConsumeQueue(ctx context.Context, queue, consumerTag string, handler func(msg RabbitPollerMessage) (RabbitPollerSettlement, error)) error
}

func TestRabbitClientPollerContract(t *testing.T) {
	var _ rabbitPollerContract = (*RabbitClient)(nil)
}

func TestChannelNames(t *testing.T) {
	inst := "11111111-1111-7111-8111-111111111111"
	if got := InputChannel(inst); got != "workflow:input:"+inst {
		t.Errorf("InputChannel() = %q", got)
	}
	if got := OutputChannel(inst); got != "workflow:output:"+inst {
		t.Errorf("OutputChannel() = %q", got)
	}
	if got := StatusChannel(inst); got != "workflow:status:"+inst {
		t.Errorf("StatusChannel() = %q", got)
	}
	if got := InputChannelPattern(); got != "workflow:input:*" {
		t.Errorf("InputChannelPattern() = %q", got)
	}
}

func TestInstanceFromInputChannel(t *testing.T) {
	inst := "11111111-1111-7111-8111-111111111111"
	cases := []struct {
		channel string
		want    string
	}{
		{"workflow:input:" + inst, inst},
		{"workflow:input:", ""},
		{"workflow:input", ""},
		{"workflow:output:" + inst, ""},
		{"workflow:status:" + inst, ""},
		{"", ""},
	}
	for _, c := range cases {
		if got := InstanceFromInputChannel(c.channel); got != c.want {
			t.Errorf("InstanceFromInputChannel(%q) = %q, want %q", c.channel, got, c.want)
		}
	}
}

func TestHeaderValue(t *testing.T) {
	cases := []struct {
		in   any
		want string
	}{
		{"abc", "abc"},
		{nil, ""},
		{int64(42), "42"},
		{true, "true"},
		{3.5, "3.5"},
	}
	for _, c := range cases {
		if got := HeaderValue(c.in); got != c.want {
			t.Errorf("HeaderValue(%v) = %q, want %q", c.in, got, c.want)
		}
	}
}

func TestRequeueBackoffIsBounded(t *testing.T) {
	if got := requeueBackoff(0); got != 100*time.Millisecond {
		t.Errorf("requeueBackoff(0) = %v, want 100ms", got)
	}
	if got := requeueBackoff(1); got != 100*time.Millisecond {
		t.Errorf("requeueBackoff(1) = %v, want 100ms", got)
	}
	for failures := 2; failures < 100; failures++ {
		if got := requeueBackoff(failures); got > 10*time.Second {
			t.Errorf("requeueBackoff(%d) = %v, exceeds 10s cap", failures, got)
		}
	}
}
