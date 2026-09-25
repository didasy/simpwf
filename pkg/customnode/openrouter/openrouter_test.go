package openrouter_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/simpwf/workflow-engine/internal/workflow/executor"
	"github.com/simpwf/workflow-engine/internal/workflow/model"
	"github.com/simpwf/workflow-engine/pkg/customnode"
	"github.com/simpwf/workflow-engine/pkg/customnode/openrouter"
)

const chatConfig = `{
  "api_key": "{{ env.SIMPWF_OPENROUTER_KEY }}",
  "prompt": "Hello from {{ source }}",
  "system": "Answer briefly.",
  "temperature": 0.2,
  "max_tokens": 64,
  "top_p": 0.9,
  "stop": ["DONE"],
  "timeout": "45s"
}`

const responsesConfig = `{
  "api": "responses",
  "model": "openai/gpt-4o-mini",
  "api_key": "{{ env.SIMPWF_OPENROUTER_KEY }}",
  "messages": [
    {"role": "developer", "content": "Answer briefly."},
    {"role": "user", "content": "Hello from {{ source }}"}
  ],
  "temperature": 0.4,
  "max_tokens": 128
}`

type fakeHTTP struct {
	body       []byte
	status     int
	err        error
	gotMethod  string
	gotURL     string
	gotHeaders map[string]string
	gotBody    []byte
	gotTimeout time.Duration
}

func (f *fakeHTTP) Do(_ context.Context, method, target string, headers map[string]string, body []byte, timeout time.Duration) ([]byte, int, http.Header, error) {
	f.gotMethod = method
	f.gotURL = target
	f.gotHeaders = headers
	f.gotBody = body
	f.gotTimeout = timeout
	return f.body, f.status, http.Header{}, f.err
}

func openRouterNode(t *testing.T, config string) *model.NodeContent {
	t.Helper()
	raw := `{"type":"openrouter","config":` + config + `}`
	nc, err := model.ParseNodeContent([]byte(raw), model.NodeLimits{DefaultTimeout: 30 * time.Second, MaxTimeout: 5 * time.Minute, ConditionTimeout: 5 * time.Second})
	if err != nil {
		t.Fatalf("ParseNodeContent() error = %v", err)
	}
	return nc
}

func runOpenRouter(nc *model.NodeContent, h *fakeHTTP, maxBytes int, ctxMap map[string]any) (*executor.Result, error) {
	return openrouter.NewForTest(h, maxBytes).Execute(context.Background(), executor.Request{
		Node:    nc,
		Context: ctxMap,
	})
}

func resultMap(t *testing.T, res *executor.Result) map[string]any {
	t.Helper()
	if res == nil {
		t.Fatal("Execute() result = nil")
	}
	out, ok := res.Output.(map[string]any)
	if !ok {
		t.Fatalf("Output = %#v, want map", res.Output)
	}
	return out
}

func TestRegistered(t *testing.T) {
	if _, ok := customnode.ByType("openrouter"); !ok {
		t.Fatal("ByType(openrouter) missing; want registered via blank import")
	}
}

func TestValidateDefaults(t *testing.T) {
	v, _ := customnode.ByType("openrouter")
	got, err := v.Validate(json.RawMessage(`{"api_key":"{{ env.SIMPWF_OPENROUTER_KEY }}","prompt":"hi","timeout":"45s"}`))
	if err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
	cfg := got.(openrouter.Config)
	if cfg.API != openrouter.APIChat {
		t.Errorf("API = %q, want %q", cfg.API, openrouter.APIChat)
	}
	if cfg.Model != openrouter.DefaultModel {
		t.Errorf("Model = %q, want %q", cfg.Model, openrouter.DefaultModel)
	}
	if cfg.Endpoint != openrouter.DefaultChatEndpoint {
		t.Errorf("Endpoint = %q, want %q", cfg.Endpoint, openrouter.DefaultChatEndpoint)
	}
	if cfg.Timeout != 45*time.Second {
		t.Errorf("Timeout = %s, want 45s", cfg.Timeout)
	}
}

func TestValidateBlankEndpointUsesDefault(t *testing.T) {
	v, _ := customnode.ByType("openrouter")
	got, err := v.Validate(json.RawMessage(`{"api_key":"k","prompt":"hi","endpoint":"  "}`))
	if err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
	cfg := got.(openrouter.Config)
	if cfg.Endpoint != openrouter.DefaultChatEndpoint {
		t.Fatalf("Endpoint = %q, want default", cfg.Endpoint)
	}
}

func TestValidateResponsesDefaults(t *testing.T) {
	v, _ := customnode.ByType("openrouter")
	got, err := v.Validate(json.RawMessage(`{"api":"responses","api_key":"k","messages":[{"role":"developer","content":"rules"},{"role":"user","content":"hello"}]}`))
	if err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
	cfg := got.(openrouter.Config)
	if cfg.API != openrouter.APIResponses || cfg.Endpoint != openrouter.DefaultResponsesEndpoint {
		t.Fatalf("unexpected responses defaults: %+v", cfg)
	}
}

func TestValidateAcceptsMessageParts(t *testing.T) {
	v, _ := customnode.ByType("openrouter")
	if _, err := v.Validate(json.RawMessage(`{
		"api":"responses","api_key":"k",
		"messages":[
			{"role":"developer","content":"rules"},
			{"role":"user","content":[{"type":"input_text","text":"{{ question }}"}]}
		]
	}`)); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
}

func TestValidateAcceptsTemplatedRole(t *testing.T) {
	v, _ := customnode.ByType("openrouter")
	if _, err := v.Validate(json.RawMessage(`{
		"api_key":"k",
		"messages":[{"role":"{{ role }}","content":"hello"}]
	}`)); err != nil {
		t.Fatalf("Validate() error = %v, want templated role to pass until runtime", err)
	}
}

func TestValidateRejects(t *testing.T) {
	v, _ := customnode.ByType("openrouter")
	cases := map[string]string{
		"bad api":                `{"api":"legacy","api_key":"k","prompt":"hi"}`,
		"bad endpoint":           `{"api_key":"k","prompt":"hi","endpoint":"ftp://openrouter.ai/x"}`,
		"missing api key":        `{"prompt":"hi"}`,
		"missing input form":     `{"api_key":"k"}`,
		"both input forms":       `{"api_key":"k","prompt":"hi","messages":[{"role":"user","content":"hello"}]}`,
		"system with messages":   `{"api_key":"k","system":"rules","messages":[{"role":"user","content":"hello"}]}`,
		"empty messages":         `{"api_key":"k","messages":[]}`,
		"bad temperature":        `{"api_key":"k","prompt":"hi","temperature":2.1}`,
		"negative max tokens":    `{"api_key":"k","prompt":"hi","max_tokens":-1}`,
		"bad top p":              `{"api_key":"k","prompt":"hi","top_p":1.1}`,
		"bad timeout":            `{"api_key":"k","prompt":"hi","timeout":"0s"}`,
		"developer role in chat": `{"api_key":"k","messages":[{"role":"developer","content":"rules"}]}`,
		"blank prompt":           `{"api_key":"k","prompt":"  "}`,
		"system without prompt":  `{"api_key":"k","system":"rules"}`,
		"content type":           `{"api_key":"k","messages":[{"role":"user","content":42}]}`,
		"null config":            `null`,
	}
	for name, raw := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := v.Validate(json.RawMessage(raw)); err != nil {
				return
			}
			t.Fatalf("Validate() = nil, want error for %s", raw)
		})
	}
}

func TestExecuteChatSuccess(t *testing.T) {
	nc := openRouterNode(t, chatConfig)
	h := &fakeHTTP{
		status: 200,
		body: []byte(`{
			"model":"openai/gpt-4o-mini-2024-07-18",
			"choices":[{"message":{"content":"Chat answer"},"finish_reason":"stop"}],
			"usage":{"prompt_tokens":7,"completion_tokens":2,"total_tokens":9}
		}`),
	}
	res, err := runOpenRouter(nc, h, 0, map[string]any{
		"env":    map[string]any{"SIMPWF_OPENROUTER_KEY": "test-secret"},
		"source": "workflow",
	})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if h.gotMethod != "POST" || h.gotURL != openrouter.DefaultChatEndpoint {
		t.Errorf("request = %s %s", h.gotMethod, h.gotURL)
	}
	if h.gotHeaders["Authorization"] != "Bearer test-secret" || h.gotHeaders["Content-Type"] != "application/json" {
		t.Errorf("headers = %#v", h.gotHeaders)
	}
	if h.gotTimeout != 45*time.Second {
		t.Errorf("timeout = %s, want 45s", h.gotTimeout)
	}
	var payload map[string]any
	if err := json.Unmarshal(h.gotBody, &payload); err != nil {
		t.Fatalf("request body not JSON: %v", err)
	}
	if payload["model"] != openrouter.DefaultModel {
		t.Errorf("model = %v", payload["model"])
	}
	messages := payload["messages"].([]any)
	if len(messages) != 2 || messages[0].(map[string]any)["content"] != "Answer briefly." || messages[1].(map[string]any)["content"] != "Hello from workflow" {
		t.Errorf("messages = %#v", messages)
	}
	out := resultMap(t, res)
	if out["model"] != "openai/gpt-4o-mini-2024-07-18" || out["api"] != openrouter.APIChat || out["text"] != "Chat answer" || out["finish_reason"] != "stop" {
		t.Errorf("output = %#v", out)
	}
	usage := out["usage"].(map[string]any)
	if usage["prompt_tokens"] != float64(7) {
		t.Errorf("usage.prompt_tokens = %#v, want float64(7)", usage["prompt_tokens"])
	}
}

func TestExecuteUsesDefaultForBlankEndpoint(t *testing.T) {
	nc := openRouterNode(t, `{"api_key":"k","prompt":"hi","endpoint":"  "}`)
	h := &fakeHTTP{status: 200, body: []byte(`{"model":"m","choices":[{"message":{"content":"ok"}}]}`)}
	if _, err := runOpenRouter(nc, h, 0, nil); err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if h.gotURL != openrouter.DefaultChatEndpoint {
		t.Errorf("URL = %q, want default", h.gotURL)
	}
}

func TestExecuteRendersEndpoint(t *testing.T) {
	nc := openRouterNode(t, `{
		"endpoint":"https://{{ openrouter_host }}/api/v1/chat/completions",
		"api_key":"k",
		"prompt":"hi"
	}`)
	h := &fakeHTTP{status: 200, body: []byte(`{"model":"m","choices":[{"message":{"content":"ok"}}]}`)}
	if _, err := runOpenRouter(nc, h, 0, map[string]any{"openrouter_host": "example.test"}); err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if h.gotURL != "https://example.test/api/v1/chat/completions" {
		t.Errorf("URL = %q", h.gotURL)
	}
}

func TestExecutePreservesLargeNumberInMessageParts(t *testing.T) {
	nc := openRouterNode(t, `{
		"api":"responses",
		"api_key":"k",
		"messages":[{"role":"user","content":[{"type":"input_text","text":"exact number"},{"type":"metadata","value":9007199254740993}]}]
	}`)
	h := &fakeHTTP{status: 200, body: []byte(`{"model":"m","output_text":"ok"}`)}
	if _, err := runOpenRouter(nc, h, 0, nil); err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if !strings.Contains(string(h.gotBody), "9007199254740993") {
		t.Errorf("request body lost exact large number: %s", h.gotBody)
	}
}

func TestExecuteRendersModelAndTuning(t *testing.T) {
	nc := openRouterNode(t, `{
		"model":"{{ model }}",
		"api_key":"{{ env.SIMPWF_OPENROUTER_KEY }}",
		"prompt":"hi",
		"temperature":"{{ temperature }}",
		"max_tokens":"{{ max_tokens }}",
		"top_p":"{{ top_p }}",
		"stop":["{{ stop }}"],
		"timeout":"{{ request_timeout }}"
	}`)
	h := &fakeHTTP{status: 200, body: []byte(`{"model":"rendered/model","choices":[{"message":{"content":"ok"}}]}`)}
	res, err := runOpenRouter(nc, h, 0, map[string]any{
		"env":             map[string]any{"SIMPWF_OPENROUTER_KEY": "k"},
		"model":           "openai/rendered",
		"temperature":     0.7,
		"max_tokens":      123,
		"top_p":           0.8,
		"stop":            "END",
		"request_timeout": "75s",
	})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if h.gotTimeout != 75*time.Second {
		t.Errorf("timeout = %s, want 75s", h.gotTimeout)
	}
	var payload map[string]any
	if err := json.Unmarshal(h.gotBody, &payload); err != nil {
		t.Fatalf("request body not JSON: %v", err)
	}
	if payload["model"] != "openai/rendered" || payload["temperature"] != 0.7 || payload["max_tokens"] != float64(123) || payload["top_p"] != 0.8 {
		t.Errorf("payload = %#v", payload)
	}
	if got := payload["stop"].([]any); len(got) != 1 || got[0] != "END" {
		t.Errorf("stop = %#v", payload["stop"])
	}
	if got := resultMap(t, res)["model"]; got != "rendered/model" {
		t.Errorf("response model = %v", got)
	}
}

func TestExecuteResponsesSuccess(t *testing.T) {
	nc := openRouterNode(t, responsesConfig)
	h := &fakeHTTP{
		status: 200,
		body: []byte(`{
			"model":"openai/gpt-4o-mini",
			"output_text":"Responses answer",
			"usage":{"input_tokens":5,"output_tokens":3,"total_tokens":8},
			"status":"completed"
		}`),
	}
	res, err := runOpenRouter(nc, h, 0, map[string]any{"env": map[string]any{"SIMPWF_OPENROUTER_KEY": "test-secret"}, "source": "workflow"})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if h.gotURL != openrouter.DefaultResponsesEndpoint {
		t.Errorf("URL = %q", h.gotURL)
	}
	var payload map[string]any
	if err := json.Unmarshal(h.gotBody, &payload); err != nil {
		t.Fatalf("request body not JSON: %v", err)
	}
	input := payload["input"].([]any)
	if len(input) != 2 {
		t.Fatalf("input = %#v, want 2 messages", input)
	}
	if input[0].(map[string]any)["role"] != "developer" || input[1].(map[string]any)["content"] != "Hello from workflow" {
		t.Errorf("input = %#v", input)
	}
	if payload["max_output_tokens"] != float64(128) {
		t.Errorf("max_output_tokens = %v, want 128", payload["max_output_tokens"])
	}
	if _, exists := payload["max_tokens"]; exists {
		t.Errorf("responses payload contains chat-only max_tokens: %#v", payload)
	}
	out := resultMap(t, res)
	if out["text"] != "Responses answer" || out["api"] != openrouter.APIResponses || out["finish_reason"] == "" {
		t.Errorf("output = %#v", out)
	}
}

func TestExecuteResponsesOutputParts(t *testing.T) {
	nc := openRouterNode(t, responsesConfig)
	h := &fakeHTTP{
		status: 200,
		body: []byte(`{
			"model":"openai/gpt-4o-mini",
			"output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"Nested answer","annotations":[]}]}],
			"status":"completed"
		}`),
	}
	res, err := runOpenRouter(nc, h, 0, map[string]any{"env": map[string]any{"SIMPWF_OPENROUTER_KEY": "test-secret"}, "source": "workflow"})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if got := resultMap(t, res)["text"]; got != "Nested answer" {
		t.Errorf("text = %v, want Nested answer", got)
	}
}

func TestExecuteUsesNodeTimeoutFallback(t *testing.T) {
	nc := openRouterNode(t, `{"api_key":"k","prompt":"hi"}`)
	h := &fakeHTTP{status: 200, body: []byte(`{"model":"m","choices":[{"message":{"content":"ok"}}]}`)}
	if _, err := runOpenRouter(nc, h, 0, nil); err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if h.gotTimeout != 30*time.Second {
		t.Errorf("timeout = %s, want node timeout 30s", h.gotTimeout)
	}
}

func TestExecuteErrors(t *testing.T) {
	cases := map[string]struct {
		config    string
		body      string
		status    int
		ctx       map[string]any
		reason    string
		onFailure bool
	}{
		"missing chat answer": {
			config: chatConfig,
			body:   `{"model":"m","choices":[]}`, status: 200,
			ctx: map[string]any{"env": map[string]any{"SIMPWF_OPENROUTER_KEY": "k"}, "source": "s"}, reason: "openrouter",
		},
		"blank responses text": {
			config: responsesConfig,
			body:   `{"model":"m","output_text":"   "}`, status: 200,
			ctx: map[string]any{"env": map[string]any{"SIMPWF_OPENROUTER_KEY": "k"}}, reason: "openrouter",
		},
		"status": {
			config: chatConfig,
			body:   `oops`, status: 429,
			ctx: map[string]any{"env": map[string]any{"SIMPWF_OPENROUTER_KEY": "k"}, "source": "s"}, reason: "openrouter-status",
		},
		"blank key": {
			config: chatConfig,
			body:   `{}`, status: 200,
			ctx: map[string]any{"env": map[string]any{"SIMPWF_OPENROUTER_KEY": "  "}, "source": "s"}, reason: "openrouter",
		},
		"output cap": {
			config: chatConfig,
			body:   `{"model":"m","choices":[{"message":{"content":"too large"}}]}`, status: 200,
			ctx: map[string]any{"env": map[string]any{"SIMPWF_OPENROUTER_KEY": "k"}, "source": "s"}, reason: "openrouter",
		},
		"status routes failure": {
			config: chatConfig,
			body:   `oops`, status: 503,
			ctx: map[string]any{"env": map[string]any{"SIMPWF_OPENROUTER_KEY": "k"}, "source": "s"}, reason: "openrouter-status", onFailure: true,
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			nc := openRouterNode(t, tc.config)
			if tc.onFailure {
				nc.OnFailure = &model.FailureRoute{NextNode: "11111111-1111-7111-8111-111111111111", OutputProperty: "openrouter_err"}
			}
			maxBytes := 0
			if name == "output cap" {
				maxBytes = 8
			}
			res, err := runOpenRouter(nc, &fakeHTTP{body: []byte(tc.body), status: tc.status}, maxBytes, tc.ctx)
			if err == nil {
				t.Fatal("Execute() error = nil")
			}
			var nodeErr *executor.NodeError
			if !errors.As(err, &nodeErr) || nodeErr.Reason != tc.reason {
				t.Errorf("error = %v, want NodeError reason %q", err, tc.reason)
			}
			if tc.onFailure {
				if len(resultMap(t, res)) != 0 {
					t.Errorf("failure output = %#v, want empty map", res.Output)
				}
			} else if res != nil {
				t.Errorf("result = %#v, want nil", res)
			}
		})
	}
}

func TestExecuteTransportError(t *testing.T) {
	nc := openRouterNode(t, `{"api_key":"k","prompt":"hi"}`)
	_, err := runOpenRouter(nc, &fakeHTTP{err: errors.New("dial refused")}, 0, nil)
	var nodeErr *executor.NodeError
	if !errors.As(err, &nodeErr) || nodeErr.Reason != "openrouter" {
		t.Fatalf("error = %v, want NodeError reason openrouter", err)
	}
}
