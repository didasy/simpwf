package statusupdate_test

import (
	"context"
	"testing"

	"github.com/simpwf/workflow-engine/internal/workflow/model"
	"github.com/simpwf/workflow-engine/internal/workflow/statusupdate"
)

type fakeRedisPublisher struct {
	calls   int
	channel string
	payload []byte
	err     error
}

func (f *fakeRedisPublisher) PublishJSON(_ context.Context, channel string, payload []byte) error {
	f.calls++
	f.channel = channel
	f.payload = payload
	return f.err
}

type fakeRabbitPublisher struct {
	calls     int
	queue     string
	payload   []byte
	messageID string
	headers   map[string]string
	err       error
}

func (f *fakeRabbitPublisher) PublishJSON(_ context.Context, queue string, payload []byte, messageID string, headers map[string]string) error {
	f.calls++
	f.queue = queue
	f.payload = payload
	f.messageID = messageID
	f.headers = headers
	return f.err
}

func TestRedisPublisherPublishesToStatusChannel(t *testing.T) {
	fake := &fakeRedisPublisher{}
	p := statusupdate.NewRedisPublisher(fake)
	ev := testEvent("logical-1")
	cfg := &model.StatusUpdateConfig{Redis: &model.RedisStatusUpdateConfig{MaxRetry: 2}}
	if err := p.Publish(context.Background(), cfg, ev); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	if fake.calls != 1 {
		t.Fatalf("publish calls = %d, want 1", fake.calls)
	}
	want := "workflow:status:" + ev.WorkflowInstanceID
	if fake.channel != want {
		t.Errorf("channel = %q, want %q", fake.channel, want)
	}
	if string(fake.payload) != string(ev.Payload) {
		t.Errorf("payload = %s, want event payload", fake.payload)
	}
}

func TestRedisPublisherFailsWithoutRedisBlock(t *testing.T) {
	fake := &fakeRedisPublisher{}
	p := statusupdate.NewRedisPublisher(fake)
	cfg := &model.StatusUpdateConfig{HTTP: &model.HTTPStatusUpdateConfig{URL: "https://example.com"}}
	if err := p.Publish(context.Background(), cfg, testEvent("logical-1")); err == nil {
		t.Error("Publish() error = nil, want missing-redis-block failure")
	}
	if fake.calls != 0 {
		t.Errorf("publish calls = %d, want 0", fake.calls)
	}
}

func TestRabbitPublisherPublishesToStatusQueue(t *testing.T) {
	fake := &fakeRabbitPublisher{}
	p := statusupdate.NewRabbitPublisher(fake, "simpwf.status")
	ev := testEvent("logical-2")
	cfg := &model.StatusUpdateConfig{RabbitMQ: &model.RabbitStatusUpdateConfig{MaxRetry: 1, RetryDelay: 0}}
	if err := p.Publish(context.Background(), cfg, ev); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	if fake.calls != 1 || fake.queue != "simpwf.status" {
		t.Errorf("publish = %+v, want queue simpwf.status", fake)
	}
	if fake.messageID != "logical-2" {
		t.Errorf("message_id = %q, want logical event id", fake.messageID)
	}
	if fake.headers["NodeInstanceId"] != ev.WorkflowInstanceID || fake.headers["IdempotencyKey"] != "logical-2" {
		t.Errorf("headers = %+v", fake.headers)
	}
	if string(fake.payload) != string(ev.Payload) {
		t.Errorf("payload = %s, want event payload", fake.payload)
	}
}

func TestRabbitPublisherFailsWithoutRabbitBlock(t *testing.T) {
	fake := &fakeRabbitPublisher{}
	p := statusupdate.NewRabbitPublisher(fake, "simpwf.status")
	cfg := &model.StatusUpdateConfig{Redis: &model.RedisStatusUpdateConfig{}}
	if err := p.Publish(context.Background(), cfg, testEvent("logical-2")); err == nil {
		t.Error("Publish() error = nil, want missing-rabbitmq-block failure")
	}
	if fake.calls != 0 {
		t.Errorf("publish calls = %d, want 0", fake.calls)
	}
}
