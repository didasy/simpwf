package inputtransport

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"github.com/simpwf/workflow-engine/internal/workflow/model"
	"github.com/simpwf/workflow-engine/internal/workflow/repository"
	"github.com/simpwf/workflow-engine/internal/workflow/service"
	"github.com/simpwf/workflow-engine/internal/workflow/transport"
)

// fakeInstanceService implements service.InstanceService for broker input
// tests, recording deliveries and returning a configurable error.
type fakeInstanceService struct {
	deliverErr error
	calls      []service.DeliverInput
}

func (f *fakeInstanceService) Create(context.Context, service.CreateInstance) (model.WorkflowInstance, error) {
	return model.WorkflowInstance{}, nil
}
func (f *fakeInstanceService) List(context.Context, repository.InstanceListQuery) ([]model.WorkflowInstance, int64, error) {
	return nil, 0, nil
}
func (f *fakeInstanceService) GetStatus(context.Context, string) (*model.WorkflowInstance, error) {
	return nil, nil
}
func (f *fakeInstanceService) GetStatusDetail(context.Context, string) (*service.StatusDetail, error) {
	return nil, nil
}
func (f *fakeInstanceService) GetContext(context.Context, string) (*model.WorkflowInstance, error) {
	return nil, nil
}
func (f *fakeInstanceService) UpdateContext(context.Context, service.UpdateContext) (*model.WorkflowInstance, error) {
	return nil, nil
}
func (f *fakeInstanceService) DeliverInput(_ context.Context, req service.DeliverInput) (*model.InputDelivery, error) {
	f.calls = append(f.calls, req)
	if f.deliverErr != nil {
		return nil, f.deliverErr
	}
	return &model.InputDelivery{
		ID:                 "delivery-1",
		WorkflowInstanceID: req.InstanceID,
		IdempotencyKey:     req.IdempotencyKey,
		Payload:            req.Payload,
		Accepted:           true,
	}, nil
}
func (f *fakeInstanceService) NodeDebug(context.Context, string, string, int) (*service.NodeDebugDetail, error) {
	return nil, nil
}
func (f *fakeInstanceService) Pause(context.Context, string) (*service.ControlResult, error) {
	return nil, nil
}
func (f *fakeInstanceService) Resume(context.Context, string) (*service.ControlResult, error) {
	return nil, nil
}
func (f *fakeInstanceService) Stop(context.Context, string, string) (*service.ControlResult, error) {
	return nil, nil
}

// fakeRedisSubscriber captures the subscribe pattern so tests can verify
// the subscription target.
type fakeRedisSubscriber struct {
	pattern string
}

func (f *fakeRedisSubscriber) PSubscribe(ctx context.Context, pattern string, _ func(ctx context.Context, channel string, payload []byte) error) error {
	f.pattern = pattern
	<-ctx.Done()
	return nil
}

const testInstanceID = "11111111-1111-7111-8111-111111111111"

func TestRedisInputSubscribesToInputPattern(t *testing.T) {
	sub := &fakeRedisSubscriber{}
	in := NewRedisInput(sub, &fakeInstanceService{})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- in.Run(ctx) }()
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if sub.pattern != "workflow:input:*" {
		t.Errorf("subscribe pattern = %q, want workflow:input:*", sub.pattern)
	}
}

func TestRedisInputDeliversEnvelope(t *testing.T) {
	svc := &fakeInstanceService{}
	in := NewRedisInput(&fakeRedisSubscriber{}, svc)
	ctx := context.Background()
	payload := json.RawMessage(`{"order":"abc"}`)
	envelope := fmt.Sprintf(`{"idempotency_key":"redis-key-1","payload":%s}`, payload)
	if err := in.handle(ctx, "workflow:input:"+testInstanceID, []byte(envelope)); err != nil {
		t.Fatalf("handle() error = %v", err)
	}
	if len(svc.calls) != 1 {
		t.Fatalf("deliver calls = %d, want 1", len(svc.calls))
	}
	got := svc.calls[0]
	if got.InstanceID != testInstanceID || got.IdempotencyKey != "redis-key-1" || got.Source != "redis" {
		t.Errorf("deliver = %+v, want instance %s key redis-key-1 source redis", got, testInstanceID)
	}
	var gotPayload any
	if err := json.Unmarshal(got.Payload, &gotPayload); err != nil || gotPayload == nil {
		t.Errorf("payload = %s, want JSON object", got.Payload)
	}
}

func TestRedisInputRejectsMalformedEnvelopes(t *testing.T) {
	svc := &fakeInstanceService{}
	in := NewRedisInput(&fakeRedisSubscriber{}, svc)
	ctx := context.Background()
	cases := []struct {
		name    string
		channel string
		payload string
	}{
		{"non-input channel", "workflow:output:" + testInstanceID, `{}`},
		{"bad json", "workflow:input:" + testInstanceID, `{`},
		{"missing key", "workflow:input:" + testInstanceID, `{"payload":{}}`},
		{"missing payload", "workflow:input:" + testInstanceID, `{"idempotency_key":"k"}`},
		{"null payload", "workflow:input:" + testInstanceID, `{"idempotency_key":"k","payload":null}`},
	}
	for _, c := range cases {
		if err := in.handle(ctx, c.channel, []byte(c.payload)); err == nil {
			t.Errorf("%s: error = nil, want error", c.name)
		}
	}
	if len(svc.calls) != 0 {
		t.Errorf("deliver calls = %d, want 0 for malformed envelopes", len(svc.calls))
	}
}

// fakeRabbitConsumer captures the consumer tag and handler.
type fakeRabbitConsumer struct {
	tag     string
	handler transport.ConsumeFunc
}

func (f *fakeRabbitConsumer) Consume(ctx context.Context, tag string, handler transport.ConsumeFunc) error {
	f.tag = tag
	f.handler = handler
	<-ctx.Done()
	return nil
}

func rabbitMsg(body string, headers map[string]string, messageID string) transport.RabbitMessage {
	return transport.RabbitMessage{Body: []byte(body), Headers: headers, MessageID: messageID}
}

func TestRabbitInputAcksSuccessfulDelivery(t *testing.T) {
	svc := &fakeInstanceService{}
	cons := &fakeRabbitConsumer{}
	in := NewRabbitInput(cons, svc, "consumer-1")
	res, err := in.handle(context.Background(), rabbitMsg(`{"ok":1}`, map[string]string{
		"NodeInstanceId": testInstanceID, "IdempotencyKey": "rabbit-key-1",
	}, ""))
	if err != nil {
		t.Fatalf("handle() error = %v", err)
	}
	if res != transport.ConsumeAck {
		t.Errorf("result = %v, want ConsumeAck", res)
	}
	if len(svc.calls) != 1 || svc.calls[0].Source != "rabbitmq" || svc.calls[0].IdempotencyKey != "rabbit-key-1" {
		t.Errorf("deliver = %+v, want source rabbitmq key rabbit-key-1", svc.calls)
	}
}

func TestRabbitInputUsesMessageIDFallback(t *testing.T) {
	svc := &fakeInstanceService{}
	cons := &fakeRabbitConsumer{}
	in := NewRabbitInput(cons, svc, "consumer-1")
	_, err := in.handle(context.Background(), rabbitMsg(`{"ok":1}`, map[string]string{
		"NodeInstanceId": testInstanceID,
	}, "amqp-message-id"))
	if err != nil {
		t.Fatalf("handle() error = %v", err)
	}
	if len(svc.calls) != 1 || svc.calls[0].IdempotencyKey != "amqp-message-id" {
		t.Errorf("deliver = %+v, want message_id fallback", svc.calls)
	}
}

func TestRabbitInputRejectsMissingMetadata(t *testing.T) {
	svc := &fakeInstanceService{}
	in := NewRabbitInput(&fakeRabbitConsumer{}, svc, "consumer-1")
	ctx := context.Background()
	cases := []transport.RabbitMessage{
		rabbitMsg(`{}`, nil, ""),
		rabbitMsg(`{}`, map[string]string{"NodeInstanceId": testInstanceID}, ""),
		rabbitMsg(`{}`, map[string]string{"IdempotencyKey": "k"}, "m"),
	}
	for i, msg := range cases {
		res, err := in.handle(ctx, msg)
		if err != nil {
			t.Errorf("case %d: error = %v, want nil", i, err)
		}
		if res != transport.ConsumeReject {
			t.Errorf("case %d: result = %v, want ConsumeReject", i, res)
		}
	}
	if len(svc.calls) != 0 {
		t.Errorf("deliver calls = %d, want 0", len(svc.calls))
	}
}

func TestRabbitInputRequeuesTransientFailures(t *testing.T) {
	svc := &fakeInstanceService{deliverErr: errors.New("database connection refused")}
	in := NewRabbitInput(&fakeRabbitConsumer{}, svc, "consumer-1")
	res, err := in.handle(context.Background(), rabbitMsg(`{}`, map[string]string{
		"NodeInstanceId": testInstanceID, "IdempotencyKey": "k",
	}, ""))
	if err == nil {
		t.Fatal("handle() error = nil, want requeue error")
	}
	if res != transport.ConsumeRequeue {
		t.Errorf("result = %v, want ConsumeRequeue", res)
	}
}

func TestRabbitInputRejectsPermanentDomainErrors(t *testing.T) {
	svc := &fakeInstanceService{deliverErr: fmt.Errorf("%w: instance is terminal", model.ErrConflict)}
	in := NewRabbitInput(&fakeRabbitConsumer{}, svc, "consumer-1")
	res, err := in.handle(context.Background(), rabbitMsg(`{}`, map[string]string{
		"NodeInstanceId": testInstanceID, "IdempotencyKey": "k",
	}, ""))
	if err != nil {
		t.Fatalf("handle() error = %v, want nil", err)
	}
	if res != transport.ConsumeReject {
		t.Errorf("result = %v, want ConsumeReject", res)
	}
}
