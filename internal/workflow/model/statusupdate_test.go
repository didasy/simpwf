package model_test

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/simpwf/workflow-engine/internal/workflow/model"
)

const (
	validURL    = "https://hooks.example.com/simpwf"
	validNodeID = "11111111-1111-7111-8111-111111111111"
)

func mustStatusUpdate(t *testing.T, raw string) *model.StatusUpdateConfig {
	t.Helper()
	cfg, err := model.ParseStatusUpdate(json.RawMessage(raw))
	if err != nil {
		t.Fatalf("ParseStatusUpdate(%s) error = %v", raw, err)
	}
	if cfg == nil {
		t.Fatalf("ParseStatusUpdate(%s) = nil, want config", raw)
	}
	return cfg
}

func TestParseStatusUpdateAbsent(t *testing.T) {
	if cfg, err := model.ParseStatusUpdate(nil); err != nil || cfg != nil {
		t.Errorf("ParseStatusUpdate(nil) = %v, %v; want nil, nil", cfg, err)
	}
	if cfg, err := model.ParseStatusUpdate(json.RawMessage(`{"start_node_id":"` + validNodeID + `","nodes":[]}`)); err != nil || cfg != nil {
		t.Errorf("ParseStatusUpdate(no status_update) = %v, %v; want nil, nil", cfg, err)
	}
}

func TestParseStatusUpdateDefaults(t *testing.T) {
	cfg := mustStatusUpdate(t, `{"status_update":{"http":{"url":"`+validURL+`"}}}`)
	h := cfg.HTTP
	if h.URL != validURL {
		t.Errorf("url = %q, want %q", h.URL, validURL)
	}
	if h.Method != "POST" {
		t.Errorf("method = %q, want POST", h.Method)
	}
	if h.MaxRetry != 3 {
		t.Errorf("max_retry = %d, want 3", h.MaxRetry)
	}
	if h.RetryDelay != 5*time.Second {
		t.Errorf("retry_delay = %v, want 5s", h.RetryDelay)
	}
	if len(h.Headers) != 0 {
		t.Errorf("headers = %v, want empty", h.Headers)
	}
}

func TestParseStatusUpdateExplicitValues(t *testing.T) {
	cfg := mustStatusUpdate(t, `{"status_update":{"http":{
		"url":"`+validURL+`/wf",
		"method":"put",
		"headers":{"Authorization":"Bearer x","X-Custom":"y"},
		"max_retry":0,
		"retry_delay":"10s"
	}}}`)
	h := cfg.HTTP
	if h.Method != "PUT" {
		t.Errorf("method = %q, want PUT", h.Method)
	}
	if h.MaxRetry != 0 {
		t.Errorf("max_retry = %d, want 0 (no retries)", h.MaxRetry)
	}
	if h.RetryDelay != 10*time.Second {
		t.Errorf("retry_delay = %v, want 10s", h.RetryDelay)
	}
	if h.Headers["Authorization"] != "Bearer x" || h.Headers["X-Custom"] != "y" {
		t.Errorf("headers = %v", h.Headers)
	}
}

func TestParseStatusUpdateRejectsInvalid(t *testing.T) {
	cases := []struct {
		name    string
		content string
		wantErr string
	}{
		{"missing url", `{"status_update":{"http":{"method":"POST"}}}`, "url"},
		{"relative url", `{"status_update":{"http":{"url":"/hooks"}}}`, "url"},
		{"bad scheme", `{"status_update":{"http":{"url":"ftp://x/y"}}}`, "url"},
		{"unsupported method", `{"status_update":{"http":{"url":"` + validURL + `","method":"TRACE"}}}`, "method"},
		{"negative max retry", `{"status_update":{"http":{"url":"` + validURL + `","max_retry":-1}}}`, "max_retry"},
		{"bad retry delay", `{"status_update":{"http":{"url":"` + validURL + `","retry_delay":"abc"}}}`, "retry_delay"},
		{"zero retry delay", `{"status_update":{"http":{"url":"` + validURL + `","retry_delay":"0s"}}}`, "retry_delay"},
		{"empty header key", `{"status_update":{"http":{"url":"` + validURL + `","headers":{"":"v"}}}}`, "header"},
		{"header injection", `{"status_update":{"http":{"url":"` + validURL + `","headers":{"X":"a\r\nInjected: 1"}}}}`, "header"},
		{"no transport", `{"status_update":{}}`, "at least one"},
		{"redis negative max retry", `{"status_update":{"redis":{"max_retry":-1}}}`, "redis"},
		{"rabbit bad retry delay", `{"status_update":{"rabbitmq":{"retry_delay":"soon"}}}`, "rabbitmq"},
		{"malformed", `{"status_update":"not an object"}`, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := model.ParseStatusUpdate(json.RawMessage(tc.content))
			if err == nil {
				t.Fatalf("ParseStatusUpdate(%s) error = nil, want error", tc.content)
			}
			if tc.wantErr != "" && !strings.Contains(err.Error(), tc.wantErr) {
				t.Errorf("error = %q, want containing %q", err, tc.wantErr)
			}
		})
	}
}

func TestParseStatusUpdateBrokerBlocks(t *testing.T) {
	cfg := mustStatusUpdate(t, `{"status_update":{
		"redis":{"max_retry":1,"retry_delay":"2s"},
		"rabbitmq":{"max_retry":0}
	}}`)
	if cfg.HTTP != nil {
		t.Errorf("http = %+v, want nil", cfg.HTTP)
	}
	if cfg.Redis == nil || cfg.Redis.MaxRetry != 1 || cfg.Redis.RetryDelay != 2*time.Second {
		t.Errorf("redis = %+v, want max_retry 1 delay 2s", cfg.Redis)
	}
	if cfg.RabbitMQ == nil || cfg.RabbitMQ.MaxRetry != 0 || cfg.RabbitMQ.RetryDelay != 5*time.Second {
		t.Errorf("rabbitmq = %+v, want max_retry 0 delay default 5s", cfg.RabbitMQ)
	}
	if got := cfg.Transports(); len(got) != 2 || got[0] != model.StatusUpdateTransportRedis || got[1] != model.StatusUpdateTransportRabbitMQ {
		t.Errorf("Transports() = %v, want [redis rabbitmq]", got)
	}
}

func TestStatusUpdateRetryPolicy(t *testing.T) {
	cfg := mustStatusUpdate(t, `{"status_update":{"http":{"url":"`+validURL+`","max_retry":7},"redis":{},"rabbitmq":{}}}`)
	if maxRetry, delay, ok := cfg.RetryPolicy(model.StatusUpdateTransportHTTP); !ok || maxRetry != 7 || delay != 5*time.Second {
		t.Errorf("http retry = %d %v %v, want 7 5s true", maxRetry, delay, ok)
	}
	if _, _, ok := cfg.RetryPolicy(model.StatusUpdateTransportRedis); !ok {
		t.Error("redis retry ok = false, want true")
	}
	if _, _, ok := cfg.RetryPolicy(model.StatusUpdateTransportRabbitMQ); !ok {
		t.Error("rabbitmq retry ok = false, want true")
	}
	if _, _, ok := cfg.RetryPolicy("kafka"); ok {
		t.Error("unknown transport retry ok = true, want false")
	}
}

func TestParseWorkflowContentCarriesStatusUpdate(t *testing.T) {
	wf := `{
		"start_node_id": "` + validNodeID + `",
		"status_update": {"http": {"url": "` + validURL + `", "max_retry": 2}},
		"nodes": [
			{"id": "` + validNodeID + `", "type": "script", "script": "return 1;"}
		]
	}`
	wc, err := model.ParseWorkflowContent(json.RawMessage(wf), model.NodeLimits{
		DefaultTimeout: 30 * time.Second, MaxTimeout: 5 * time.Minute, ConditionTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("ParseWorkflowContent() error = %v", err)
	}
	if wc.StatusUpdate == nil || wc.StatusUpdate.HTTP == nil {
		t.Fatal("StatusUpdate = nil, want parsed config")
	}
	if wc.StatusUpdate.HTTP.URL != validURL || wc.StatusUpdate.HTTP.MaxRetry != 2 {
		t.Errorf("StatusUpdate = %+v", wc.StatusUpdate)
	}
}

func TestParseWorkflowContentRejectsInvalidStatusUpdate(t *testing.T) {
	wf := `{
		"start_node_id": "` + validNodeID + `",
		"status_update": {"http": {"url": "not-a-url"}},
		"nodes": [
			{"id": "` + validNodeID + `", "type": "script", "script": "return 1;"}
		]
	}`
	if _, err := model.ParseWorkflowContent(json.RawMessage(wf), model.NodeLimits{
		DefaultTimeout: 30 * time.Second, MaxTimeout: 5 * time.Minute, ConditionTimeout: 5 * time.Second,
	}); err == nil {
		t.Error("ParseWorkflowContent() error = nil, want status_update validation error")
	}
}
