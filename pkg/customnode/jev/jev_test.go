package jev_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/simpwf/workflow-engine/internal/workflow/executor"
	"github.com/simpwf/workflow-engine/internal/workflow/model"
	"github.com/simpwf/workflow-engine/pkg/customnode"
	"github.com/simpwf/workflow-engine/pkg/customnode/jev"
	_ "github.com/simpwf/workflow-engine/pkg/customnode/jev"
)

const fullConfig = `{
  "model": "typesafe/jev-1.13",
  "endpoint": "https://openrouter.ai/api/alpha/decisions",
  "api_key": "test-key",
  "state": "Help! My payouts have been failing for 3 days.",
  "questions": {
    "is_urgent": {"type": "noul", "instructions": "Does this message convey urgency?",
      "criteria": {"true": "Explicitly time-sensitive", "false": "No urgency expressed"}},
    "department": {"type": "choice", "instructions": "Which team?",
      "criteria": {"billing": "Payments, refunds", "technical": "Bugs, outages"}},
    "frustration": {"type": "score", "instructions": "How frustrated?",
      "criteria": ["Calm", "Frustrated", "Very angry"]}
  }
}`

type fakeHTTP struct {
	body    []byte
	status  int
	err     error
	gotURL  string
	gotAuth string
	gotBody []byte
}

func (f *fakeHTTP) Do(_ context.Context, _, target string, headers map[string]string, body []byte, _ time.Duration) ([]byte, int, http.Header, error) {
	f.gotURL = target
	f.gotAuth = headers["Authorization"]
	f.gotBody = body
	return f.body, f.status, http.Header{}, f.err
}

func jevNode(t *testing.T, config string) *model.NodeContent {
	t.Helper()
	raw := `{"type":"jev","config":` + config + `}`
	nc, err := model.ParseNodeContent([]byte(raw), model.NodeLimits{DefaultTimeout: 5 * time.Second, MaxTimeout: 60 * time.Second, ConditionTimeout: 2 * time.Second})
	if err != nil {
		t.Fatalf("ParseNodeContent() error = %v", err)
	}
	return nc
}

func runJevExec(nc *model.NodeContent, h *fakeHTTP) (*executor.Result, error) {
	ncCtx := map[string]any{}
	return jev.NewForTest(h, 0).Execute(context.Background(), executor.Request{Node: nc, Context: ncCtx})
}

func outMap(t *testing.T, res *executor.Result) map[string]any {
	t.Helper()
	out, ok := res.Output.(map[string]any)
	if !ok {
		t.Fatalf("Output = %#v, want map", res.Output)
	}
	return out
}

func TestRegistered(t *testing.T) {
	if _, ok := customnode.ByType("jev"); !ok {
		t.Fatal("ByType(jev) missing; want registered via blank import")
	}
}

func TestValidateAcceptFull(t *testing.T) {
	v, _ := customnode.ByType("jev")
	got, err := v.Validate(json.RawMessage(fullConfig))
	if err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
	cfg, ok := got.(jev.Config)
	if !ok {
		t.Fatalf("Validate() = %T, want jev.Config", got)
	}
	if cfg.Model != "typesafe/jev-1.13" || cfg.Endpoint != "https://openrouter.ai/api/alpha/decisions" {
		t.Fatalf("defaults not kept: %+v", cfg)
	}
	if len(cfg.Questions) != 3 {
		t.Fatalf("Questions = %d, want 3", len(cfg.Questions))
	}
}

func TestValidateDefaults(t *testing.T) {
	v, _ := customnode.ByType("jev")
	got, err := v.Validate(json.RawMessage(`{
	  "api_key": "k", "state": "s",
	  "questions": {"q": {"type": "noul", "instructions": "Is it?"}}
	}`))
	if err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
	cfg := got.(jev.Config)
	if cfg.Model != jev.DefaultModel {
		t.Errorf("Model = %q, want default %q", cfg.Model, jev.DefaultModel)
	}
	if cfg.Endpoint != jev.DefaultEndpoint {
		t.Errorf("Endpoint = %q, want default %q", cfg.Endpoint, jev.DefaultEndpoint)
	}
}

func TestValidateReject(t *testing.T) {
	v, _ := customnode.ByType("jev")
	choice256 := map[string]any{}
	for i := 0; i < 256; i++ {
		choice256[fmt.Sprintf("opt%d", i)] = "desc"
	}
	choice256Raw, _ := json.Marshal(map[string]any{
		"api_key": "k", "state": "s",
		"questions": map[string]any{
			"q": map[string]any{"type": "choice", "instructions": "Pick?", "criteria": choice256},
		},
	})
	score11 := make([]string, 11)
	for i := range score11 {
		score11[i] = fmt.Sprintf("level %d", i)
	}
	score11Raw, _ := json.Marshal(map[string]any{
		"api_key": "k", "state": "s",
		"questions": map[string]any{
			"q": map[string]any{"type": "score", "instructions": "Rate?", "criteria": score11},
		},
	})
	cases := map[string]string{
		"missing api_key":     `{"state": "s", "questions": {"q": {"type": "noul", "instructions": "Is it?"}}}`,
		"blank api_key":       `{"api_key": "  ", "state": "s", "questions": {"q": {"type": "noul", "instructions": "Is it?"}}}`,
		"missing state":       `{"api_key": "k", "questions": {"q": {"type": "noul", "instructions": "Is it?"}}}`,
		"null state":          `{"api_key": "k", "state": null, "questions": {"q": {"type": "noul", "instructions": "Is it?"}}}`,
		"numeric state":       `{"api_key": "k", "state": 42, "questions": {"q": {"type": "noul", "instructions": "Is it?"}}}`,
		"missing questions":   `{"api_key": "k", "state": "s"}`,
		"empty questions":     `{"api_key": "k", "state": "s", "questions": {}}`,
		"bad question type":   `{"api_key": "k", "state": "s", "questions": {"q": {"type": "maybe", "instructions": "Is it?"}}}`,
		"missing type":        `{"api_key": "k", "state": "s", "questions": {"q": {"instructions": "Is it?"}}}`,
		"blank instructions":  `{"api_key": "k", "state": "s", "questions": {"q": {"type": "noul", "instructions": "  "}}}`,
		"null instructions":   `{"api_key": "k", "state": "s", "questions": {"q": {"type": "noul", "instructions": null}}}`,
		"choice missing crit": `{"api_key": "k", "state": "s", "questions": {"q": {"type": "choice", "instructions": "Pick?"}}}`,
		"choice 1 option":     `{"api_key": "k", "state": "s", "questions": {"q": {"type": "choice", "instructions": "Pick?", "criteria": {"a": "A"}}}}`,
		"choice blank value":  `{"api_key": "k", "state": "s", "questions": {"q": {"type": "choice", "instructions": "Pick?", "criteria": {"a": "A", "b": " "}}}}`,
		"score missing crit":  `{"api_key": "k", "state": "s", "questions": {"q": {"type": "score", "instructions": "Rate?"}}}`,
		"score 1 level":       `{"api_key": "k", "state": "s", "questions": {"q": {"type": "score", "instructions": "Rate?", "criteria": ["only"]}}}`,
		"score blank level":   `{"api_key": "k", "state": "s", "questions": {"q": {"type": "score", "instructions": "Rate?", "criteria": ["a", " "]}}}`,
		"noul blank true":     `{"api_key": "k", "state": "s", "questions": {"q": {"type": "noul", "instructions": "Is it?", "criteria": {"true": " "}}}}`,
		"bad endpoint":        `{"api_key": "k", "state": "s", "endpoint": "ftp://x/y", "questions": {"q": {"type": "noul", "instructions": "Is it?"}}}`,
		"null config":         `null`,
		"not object":          `[1,2]`,
		"choice 256 options":  string(choice256Raw),
		"score 11 levels":     string(score11Raw),
	}
	for name, raw := range cases {
		if _, err := v.Validate(json.RawMessage(raw)); err == nil {
			t.Errorf("%s: Validate() = nil, want error", name)
		}
	}
}

func TestValidateTemplateShapesPass(t *testing.T) {
	v, _ := customnode.ByType("jev")
	got, err := v.Validate(json.RawMessage(`{
	  "api_key": "{{ env.SIMPWF_OPENROUTER_KEY }}",
	  "state": "{{ ticket }}",
	  "questions": {"q": {"type": "noul", "instructions": "{{ q_text }}"}}
	}`))
	if err != nil {
		t.Fatalf("Validate() error = %v, want accept for template shapes", err)
	}
	if _, ok := got.(jev.Config); !ok {
		t.Fatalf("Validate() = %T, want jev.Config", got)
	}
}

const cannedResponse = `{
  "model": "typesafe/jev-1.13-20260917",
  "answers": {
    "is_urgent": {"type": "noul", "noul": 0.96},
    "department": {"type": "choice", "choice": "billing", "confidence": 0.67,
      "probabilities": {"billing": 0.78, "technical": 0.22}},
    "frustration": {"type": "score", "score": 1.99, "confidence": 0.99,
      "probabilities": {"0": 0, "1": 0, "2": 1},
      "legend": {"0": "Calm", "1": "Frustrated", "2": "Very angry"}}
  },
  "usage": {"input_tokens": 476, "output_tokens": 70, "cost": 0.000019992}
}`

func TestExecuteSuccess(t *testing.T) {
	nc := jevNode(t, fullConfig)
	h := &fakeHTTP{body: []byte(cannedResponse), status: 200}
	res, err := runJevExec(nc, h)
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if h.gotURL != "https://openrouter.ai/api/alpha/decisions" {
		t.Errorf("URL = %q, want decisions endpoint", h.gotURL)
	}
	if !strings.HasPrefix(h.gotAuth, "Bearer ") || h.gotAuth == "Bearer " {
		t.Errorf("Authorization header missing Bearer key (redacted check)")
	}
	var sent map[string]any
	if err := json.Unmarshal(h.gotBody, &sent); err != nil {
		t.Fatalf("request body not JSON: %v", err)
	}
	if sent["model"] != "typesafe/jev-1.13" {
		t.Errorf("model = %v, want jev-1.13", sent["model"])
	}
	out := outMap(t, res)
	if out["model"] != "typesafe/jev-1.13-20260917" {
		t.Errorf("model = %v, want dated snapshot passthrough", out["model"])
	}
	answers, ok := out["answers"].(map[string]any)
	if !ok || len(answers) != 3 {
		t.Fatalf("answers = %#v, want 3 entries", out["answers"])
	}
	if n := answers["is_urgent"].(map[string]any)["noul"]; n != 0.96 {
		t.Errorf("is_urgent.noul = %v, want 0.96", n)
	}
	if c := answers["department"].(map[string]any)["choice"]; c != "billing" {
		t.Errorf("department.choice = %v, want billing", c)
	}
	if _, ok := out["usage"].(map[string]any); !ok {
		t.Errorf("usage = %#v, want map passthrough", out["usage"])
	}
}

func TestExecuteErrors(t *testing.T) {
	missingQid := strings.Replace(cannedResponse, `"frustration":`, `"other":`, 1)
	outOfRange := strings.Replace(cannedResponse, `"noul": 0.96`, `"noul": 1.5`, 1)
	for name, body := range map[string]string{
		"missing answer qid": missingQid,
		"noul out of range":  outOfRange,
		"not json":           `oops`,
	} {
		nc := jevNode(t, fullConfig)
		_, err := runJevExec(nc, &fakeHTTP{body: []byte(body), status: 200})
		if err == nil {
			t.Errorf("%s: Execute() = nil, want error", name)
		}
	}
}

func TestExecuteTransportError(t *testing.T) {
	nc := jevNode(t, fullConfig)
	_, err := runJevExec(nc, &fakeHTTP{err: errors.New("dial refused")})
	if err == nil {
		t.Fatal("Execute() = nil, want transport error")
	}
	var ne *executor.NodeError
	if !errors.As(err, &ne) || ne.Reason != "jev" {
		t.Errorf("err = %v, want NodeError reason jev", err)
	}
}

func TestExecuteStatusError(t *testing.T) {
	nc := jevNode(t, fullConfig)
	res, err := runJevExec(nc, &fakeHTTP{body: []byte(`oops`), status: 500})
	if err == nil {
		t.Fatal("Execute() = nil, want status error")
	}
	var ne *executor.NodeError
	if !errors.As(err, &ne) || ne.Reason != "jev-status" {
		t.Errorf("err = %v, want NodeError reason jev-status", err)
	}
	if res != nil {
		t.Errorf("res = %#v, want nil without on_failure", res)
	}
}

func TestExecuteStatusErrorWithOnFailure(t *testing.T) {
	nc := jevNode(t, fullConfig)
	nc.OnFailure = &model.FailureRoute{NextNode: "11111111-1111-7111-8111-111111111111", OutputProperty: "jev_err"}
	res, err := runJevExec(nc, &fakeHTTP{body: []byte(`oops`), status: 500})
	if err == nil {
		t.Fatal("Execute() = nil, want status error")
	}
	if res == nil || res.Output == nil {
		t.Fatal("Execute() result missing; want empty-map output for routeFailure")
	}
}

func TestExecuteBlankRenderedAPIKey(t *testing.T) {
	nc := jevNode(t, `{
	  "api_key": "{{ missing_key }}", "state": "s",
	  "questions": {"q": {"type": "noul", "instructions": "Is it?"}}
	}`)
	_, err := runJevExec(nc, &fakeHTTP{body: []byte(cannedResponse), status: 200})
	if err == nil {
		t.Fatal("Execute() = nil, want blank rendered api_key error")
	}
}

func TestExecuteUnresolvableState(t *testing.T) {
	nc := jevNode(t, `{
	  "api_key": "k", "state": "{{ nope.missing }}",
	  "questions": {"q": {"type": "noul", "instructions": "Is it?"}}
	}`)
	_, err := runJevExec(nc, &fakeHTTP{body: []byte(cannedResponse), status: 200})
	if err == nil {
		t.Fatal("Execute() = nil, want state render error")
	}
}

func TestParseNodeContent(t *testing.T) {
	nc := jevNode(t, `{
	  "api_key": "k", "state": "s",
	  "questions": {"q": {"type": "noul", "instructions": "Is it?"}}
	}`)
	cfg, ok := nc.Custom.(jev.Config)
	if !ok {
		t.Fatalf("Custom = %T, want jev.Config", nc.Custom)
	}
	if len(cfg.Questions) != 1 {
		t.Fatalf("Questions = %d, want 1", len(cfg.Questions))
	}
}
