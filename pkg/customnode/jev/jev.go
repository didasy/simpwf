// Package jev is a custom node calling the Jev 1.13 Decisions API on
// OpenRouter. It sends application state plus typed questions (noul,
// choice, score) and returns the raw probabilities verbatim — no
// thresholds live in this node; downstream conditions/script nodes gate.
//
// Configs address the server-side key as {{ env.SIMPWF_OPENROUTER_KEY }};
// the snapshot is per-instance at creation. The host openrouter.ai must
// be in the engine HTTP allowlist.
package jev

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/simpwf/workflow-engine/internal/workflow/executor"
	"github.com/simpwf/workflow-engine/pkg/contextpath"
	"github.com/simpwf/workflow-engine/pkg/customnode"
)

// Defaults for the Decisions API surface.
const (
	DefaultModel    = "typesafe/jev-1.13"
	DefaultEndpoint = "https://openrouter.ai/api/alpha/decisions"
)

// Question types supported by the Decisions API.
const (
	QuestionNoul   = "noul"
	QuestionChoice = "choice"
	QuestionScore  = "score"
)

// Bounds for criteria sizes (per plan).
const (
	MaxChoiceOptions = 255
	MinChoiceOptions = 2
	MinScoreLevels   = 2
	MaxScoreLevels   = 10
)

func init() {
	customnode.MustRegister(customnode.Definition{
		Type:     "jev",
		Validate: ValidateConfig,
		New: func(d customnode.Deps) (executor.Executor, error) {
			if d.HTTP == nil {
				return nil, fmt.Errorf("jev: shared HTTP client is not configured")
			}
			return &Executor{http: d.HTTP, maxBytes: d.Limits.MaxOutputBytes}, nil
		},
	})
}

// Question is one validated typed question sent to the Decisions API.
type Question struct {
	Type         string          `json:"type"`               // noul | choice | score
	Instructions json.RawMessage `json:"instructions"`       // string | object | array, required non-null
	Criteria     json.RawMessage `json:"criteria,omitempty"` // shape depends on type
	// choiceKeys holds the validated choice option keys for answer
	// range-checking (choice only; other types leave it nil).
	choiceKeys map[string]bool
}

// Config is the validated form of the jev node config object.
type Config struct {
	Model     string              `json:"model"`
	Endpoint  string              `json:"endpoint"`
	APIKey    string              `json:"api_key"` // templated, e.g. {{ env.SIMPWF_OPENROUTER_KEY }}
	State     json.RawMessage     `json:"state"`   // string | object | array, templated
	Questions map[string]Question `json:"questions"`
}

// rawQuestion mirrors the JSON shape for lenient per-type parsing.
type rawQuestion struct {
	Type         *string         `json:"type"`
	Instructions json.RawMessage `json:"instructions"`
	Criteria     json.RawMessage `json:"criteria"`
}

// rawConfig mirrors the JSON shape so missing vs blank stays visible.
type rawConfig struct {
	Model     *string                    `json:"model"`
	Endpoint  *string                    `json:"endpoint"`
	APIKey    *string                    `json:"api_key"`
	State     json.RawMessage            `json:"state"`
	Questions map[string]json.RawMessage `json:"questions"`
}

// ValidateConfig owns the jev schema. It checks raw presence only:
// templates are unresolved at parse, so values containing {{ }} shapes
// (e.g. {{ env.SIMPWF_OPENROUTER_KEY }}) are accepted here and resolved
// at runtime against the instance context. Unknown fields are ignored.
func ValidateConfig(raw json.RawMessage) (any, error) {
	if len(raw) == 0 || strings.TrimSpace(string(raw)) == "null" {
		return nil, fmt.Errorf("config is required")
	}
	var r rawConfig
	dec := json.NewDecoder(bytes.NewReader(raw))
	if err := dec.Decode(&r); err != nil {
		return nil, fmt.Errorf("config must be an object: %w", err)
	}
	c := Config{Questions: map[string]Question{}}
	if r.Model == nil || strings.TrimSpace(*r.Model) == "" {
		c.Model = DefaultModel
	} else {
		c.Model = strings.TrimSpace(*r.Model)
	}
	if r.Endpoint == nil || strings.TrimSpace(*r.Endpoint) == "" {
		c.Endpoint = DefaultEndpoint
	} else {
		ep := strings.TrimSpace(*r.Endpoint)
		u, err := url.Parse(ep)
		if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
			return nil, fmt.Errorf("config.endpoint %q must be an absolute http(s) url", ep)
		}
		c.Endpoint = ep
	}
	if r.APIKey == nil || strings.TrimSpace(*r.APIKey) == "" {
		return nil, fmt.Errorf("config.api_key is required")
	}
	c.APIKey = *r.APIKey
	if len(r.State) == 0 || strings.TrimSpace(string(r.State)) == "null" {
		return nil, fmt.Errorf("config.state is required")
	}
	var stateProbe any
	if err := json.Unmarshal(r.State, &stateProbe); err != nil {
		return nil, fmt.Errorf("config.state must be valid JSON: %w", err)
	}
	switch stateProbe.(type) {
	case string, map[string]any, []any:
	default:
		return nil, fmt.Errorf("config.state must be a string, object, or array")
	}
	c.State = append(json.RawMessage(nil), r.State...)
	if len(r.Questions) == 0 {
		return nil, fmt.Errorf("config.questions is required with at least one question")
	}
	for qid, qraw := range r.Questions {
		if strings.TrimSpace(qid) == "" {
			return nil, fmt.Errorf("question id must be non-empty")
		}
		q, err := parseQuestion(qid, qraw)
		if err != nil {
			return nil, err
		}
		c.Questions[qid] = q
	}
	return c, nil
}

// parseQuestion validates one question entry.
func parseQuestion(qid string, raw json.RawMessage) (Question, error) {
	var r rawQuestion
	if err := json.Unmarshal(raw, &r); err != nil {
		return Question{}, fmt.Errorf("questions[%q] must be an object: %w", qid, err)
	}
	if r.Type == nil || (*r.Type != QuestionNoul && *r.Type != QuestionChoice && *r.Type != QuestionScore) {
		return Question{}, fmt.Errorf("questions[%q].type must be noul, choice, or score", qid)
	}
	q := Question{Type: *r.Type}
	if len(r.Instructions) == 0 || strings.TrimSpace(string(r.Instructions)) == "null" {
		return Question{}, fmt.Errorf("questions[%q].instructions is required", qid)
	}
	var instrProbe any
	if err := json.Unmarshal(r.Instructions, &instrProbe); err != nil {
		return Question{}, fmt.Errorf("questions[%q].instructions must be valid JSON: %w", qid, err)
	}
	switch v := instrProbe.(type) {
	case string:
		if strings.TrimSpace(v) == "" {
			return Question{}, fmt.Errorf("questions[%q].instructions must be non-blank", qid)
		}
	case map[string]any, []any:
	default:
		return Question{}, fmt.Errorf("questions[%q].instructions must be a string, object, or array", qid)
	}
	q.Instructions = append(json.RawMessage(nil), r.Instructions...)
	switch q.Type {
	case QuestionNoul:
		if len(r.Criteria) == 0 || strings.TrimSpace(string(r.Criteria)) == "null" {
			break // optional
		}
		var crit map[string]json.RawMessage
		if err := json.Unmarshal(r.Criteria, &crit); err != nil {
			return Question{}, fmt.Errorf("questions[%q].criteria must be an object for noul", qid)
		}
		for _, key := range []string{"true", "false"} {
			rawVal, ok := crit[key]
			if !ok {
				continue
			}
			if strings.TrimSpace(string(rawVal)) == "null" {
				return Question{}, fmt.Errorf("questions[%q].criteria.%s must be a non-empty string", qid, key)
			}
			var s string
			if err := json.Unmarshal(rawVal, &s); err != nil || strings.TrimSpace(s) == "" {
				return Question{}, fmt.Errorf("questions[%q].criteria.%s must be a non-empty string", qid, key)
			}
		}
		q.Criteria = append(json.RawMessage(nil), r.Criteria...)
	case QuestionChoice:
		if len(r.Criteria) == 0 || strings.TrimSpace(string(r.Criteria)) == "null" {
			return Question{}, fmt.Errorf("questions[%q].criteria is required for choice", qid)
		}
		var crit map[string]json.RawMessage
		if err := json.Unmarshal(r.Criteria, &crit); err != nil {
			return Question{}, fmt.Errorf("questions[%q].criteria must be an object for choice", qid)
		}
		if len(crit) < MinChoiceOptions || len(crit) > MaxChoiceOptions {
			return Question{}, fmt.Errorf("questions[%q].criteria must have %d..%d entries", qid, MinChoiceOptions, MaxChoiceOptions)
		}
		q.choiceKeys = make(map[string]bool, len(crit))
		for key, val := range crit {
			if strings.TrimSpace(key) == "" {
				return Question{}, fmt.Errorf("questions[%q].criteria has an empty option key", qid)
			}
			if strings.TrimSpace(string(val)) == "null" {
				q.choiceKeys[key] = true
				continue
			}
			var probe any
			if err := json.Unmarshal(val, &probe); err != nil {
				return Question{}, fmt.Errorf("questions[%q].criteria[%q] must be a string, null, or object", qid, key)
			}
			switch v := probe.(type) {
			case string:
				if strings.TrimSpace(v) == "" {
					return Question{}, fmt.Errorf("questions[%q].criteria[%q] must be a non-empty string", qid, key)
				}
			case map[string]any:
			default:
				return Question{}, fmt.Errorf("questions[%q].criteria[%q] must be a string, null, or object", qid, key)
			}
			q.choiceKeys[key] = true
		}
		q.Criteria = append(json.RawMessage(nil), r.Criteria...)
	case QuestionScore:
		if len(r.Criteria) == 0 || strings.TrimSpace(string(r.Criteria)) == "null" {
			return Question{}, fmt.Errorf("questions[%q].criteria is required for score", qid)
		}
		var crit []json.RawMessage
		if err := json.Unmarshal(r.Criteria, &crit); err != nil {
			return Question{}, fmt.Errorf("questions[%q].criteria must be an array for score", qid)
		}
		if len(crit) < MinScoreLevels || len(crit) > MaxScoreLevels {
			return Question{}, fmt.Errorf("questions[%q].criteria must have %d..%d entries", qid, MinScoreLevels, MaxScoreLevels)
		}
		for i, item := range crit {
			if strings.TrimSpace(string(item)) == "null" {
				return Question{}, fmt.Errorf("questions[%q].criteria[%d] must be a non-empty string or object", qid, i)
			}
			var probe any
			if err := json.Unmarshal(item, &probe); err != nil {
				return Question{}, fmt.Errorf("questions[%q].criteria[%d] must be a non-empty string or object", qid, i)
			}
			switch v := probe.(type) {
			case string:
				if strings.TrimSpace(v) == "" {
					return Question{}, fmt.Errorf("questions[%q].criteria[%d] must be a non-empty string or object", qid, i)
				}
			case map[string]any:
			default:
				return Question{}, fmt.Errorf("questions[%q].criteria[%d] must be a non-empty string or object", qid, i)
			}
		}
		q.Criteria = append(json.RawMessage(nil), r.Criteria...)
	}
	return q, nil
}

// httpDo is the subset of HTTPExecutor the jev node needs. It is an
// interface (not *executor.HTTPExecutor) so unit tests can fake it.
type httpDo interface {
	Do(ctx context.Context, method, target string, headers map[string]string, body []byte, timeout time.Duration) ([]byte, int, http.Header, error)
}

// Executor calls the Decisions API via the shared HTTP client and
// returns raw probabilities verbatim.
type Executor struct {
	http     httpDo
	maxBytes int
}

// NewForTest builds an Executor with fakes. Tests only.
func NewForTest(h httpDo, maxBytes int) *Executor {
	return &Executor{http: h, maxBytes: maxBytes}
}

// renderAPIKey renders the api_key template and requires non-blank.
func renderAPIKey(tpl string, ctx map[string]any) (string, error) {
	v, err := contextpath.RenderTemplate(tpl, ctx)
	if err != nil {
		return "", err
	}
	s, ok := v.(string)
	if !ok || strings.TrimSpace(s) == "" {
		return "", fmt.Errorf("jev: rendered api_key is blank (want non-empty string)")
	}
	return s, nil
}

// fail wraps err as a NodeError, attaching an empty output map when
// on_failure is set (routeFailure path); otherwise the error stands
// alone. Never logs the key.
func fail(req executor.Request, reason string, err error) (*executor.Result, error) {
	nodeErr := &executor.NodeError{Node: req.Node, Reason: reason, Err: err}
	if req.Node != nil && req.Node.OnFailure != nil {
		return &executor.Result{Output: map[string]any{}}, nodeErr
	}
	return nil, nodeErr
}

// renderState renders the state field: strings via RenderTemplate,
// objects/arrays via RenderJSON.
func renderState(raw json.RawMessage, ctx map[string]any) (any, error) {
	var probe any
	if err := json.Unmarshal(raw, &probe); err != nil {
		return nil, err
	}
	switch probe.(type) {
	case string:
		var tpl string
		if err := json.Unmarshal(raw, &tpl); err != nil {
			return nil, err
		}
		return contextpath.RenderTemplate(tpl, ctx)
	default:
		rendered, err := contextpath.RenderJSON(raw, ctx)
		if err != nil {
			return nil, err
		}
		var out any
		dec := json.NewDecoder(bytes.NewReader(rendered))
		dec.UseNumber()
		if err := dec.Decode(&out); err != nil {
			return nil, err
		}
		return out, nil
	}
}

// decisionsResponse mirrors the Decisions API response envelope.
type decisionsResponse struct {
	Model   string                     `json:"model"`
	Answers map[string]json.RawMessage `json:"answers"`
	Usage   json.RawMessage            `json:"usage"`
}

// Execute renders the config against req.Context, POSTs to the Decisions
// API via the shared HTTP client (allowlist enforced), strictly validates
// the answers, and returns raw probabilities only.
func (e *Executor) Execute(ctx context.Context, req executor.Request) (*executor.Result, error) {
	if req.Node == nil {
		return nil, fmt.Errorf("jev: request has no node")
	}
	cfg, ok := req.Node.Custom.(Config)
	if !ok {
		return nil, fmt.Errorf("jev: node custom config has unexpected type %T", req.Node.Custom)
	}
	tplCtx := req.Context
	if tplCtx == nil {
		tplCtx = map[string]any{}
	}
	apiKey, err := renderAPIKey(cfg.APIKey, tplCtx)
	if err != nil {
		return fail(req, "jev", fmt.Errorf("api_key: %w", err))
	}
	state, err := renderState(cfg.State, tplCtx)
	if err != nil {
		return fail(req, "jev", fmt.Errorf("state: %w", err))
	}
	questions := make(map[string]any, len(cfg.Questions))
	for qid, q := range cfg.Questions {
		var instructions any
		dec := json.NewDecoder(bytes.NewReader(q.Instructions))
		dec.UseNumber()
		if err := dec.Decode(&instructions); err != nil {
			return fail(req, "jev", fmt.Errorf("questions[%q].instructions: %w", qid, err))
		}
		entry := map[string]any{"type": q.Type, "instructions": instructions}
		if len(q.Criteria) > 0 {
			var criteria any
			dec := json.NewDecoder(bytes.NewReader(q.Criteria))
			dec.UseNumber()
			if err := dec.Decode(&criteria); err != nil {
				return fail(req, "jev", fmt.Errorf("questions[%q].criteria: %w", qid, err))
			}
			entry["criteria"] = criteria
		}
		questions[qid] = entry
	}
	payload, err := json.Marshal(map[string]any{
		"model":     cfg.Model,
		"state":     state,
		"questions": questions,
	})
	if err != nil {
		return fail(req, "jev", fmt.Errorf("encode request: %w", err))
	}
	if e.http == nil {
		return fail(req, "jev", fmt.Errorf("shared HTTP client is not configured"))
	}
	headers := map[string]string{
		"Authorization": "Bearer " + apiKey,
		"Content-Type":  "application/json",
	}
	timeout := req.Node.Timeout
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	body, status, _, err := e.http.Do(ctx, "POST", cfg.Endpoint, headers, payload, timeout)
	if err != nil {
		return fail(req, "jev", err)
	}
	if status >= 300 {
		return fail(req, "jev-status", fmt.Errorf("decisions request failed with status %d", status))
	}
	if e.maxBytes > 0 && len(body) > e.maxBytes {
		return fail(req, "jev", fmt.Errorf("decisions response exceeds output cap (%d > %d bytes)", len(body), e.maxBytes))
	}
	var resp decisionsResponse
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.UseNumber()
	if err := dec.Decode(&resp); err != nil {
		return fail(req, "jev", fmt.Errorf("decode response: %w", err))
	}
	if resp.Answers == nil {
		return fail(req, "jev", fmt.Errorf("response has no answers"))
	}
	answers := make(map[string]any, len(cfg.Questions))
	for qid, q := range cfg.Questions {
		rawAns, ok := resp.Answers[qid]
		if !ok {
			return fail(req, "jev", fmt.Errorf("response missing answer for question %q", qid))
		}
		var ansMap map[string]any
		dec := json.NewDecoder(bytes.NewReader(rawAns))
		dec.UseNumber()
		if err := dec.Decode(&ansMap); err != nil {
			return fail(req, "jev", fmt.Errorf("answer[%q] must be an object: %w", qid, err))
		}
		if ansMap["type"] != q.Type {
			return fail(req, "jev", fmt.Errorf("answer[%q].type = %v, want %q", qid, ansMap["type"], q.Type))
		}
		if err := checkAnswer(qid, q, ansMap); err != nil {
			return fail(req, "jev", err)
		}
		answers[qid] = normalizeNumbers(ansMap)
	}
	var usage any
	if len(resp.Usage) > 0 && strings.TrimSpace(string(resp.Usage)) != "null" {
		dec := json.NewDecoder(bytes.NewReader(resp.Usage))
		dec.UseNumber()
		if err := dec.Decode(&usage); err != nil {
			return fail(req, "jev", fmt.Errorf("decode usage: %w", err))
		}
		usage = normalizeNumbers(usage)
	}
	return &executor.Result{Output: map[string]any{
		"model":   resp.Model,
		"answers": answers,
		"usage":   usage,
	}}, nil
}

// checkAnswer range-checks one raw answer against its question.
func checkAnswer(qid string, q Question, ans map[string]any) error {
	switch q.Type {
	case QuestionNoul:
		n, err := toFloat(ans["noul"])
		if err != nil {
			return fmt.Errorf("answer[%q].noul must be numeric 0..1", qid)
		}
		if n < 0 || n > 1 {
			return fmt.Errorf("answer[%q].noul = %v, want 0..1", qid, n)
		}
	case QuestionChoice:
		choice, ok := ans["choice"].(string)
		if !ok || !q.choiceKeys[choice] {
			return fmt.Errorf("answer[%q].choice %v is not one of the requested options", qid, ans["choice"])
		}
		probs, ok := ans["probabilities"].(map[string]any)
		if !ok {
			return fmt.Errorf("answer[%q].probabilities is required for choice", qid)
		}
		_ = probs
	case QuestionScore:
		if _, err := toFloat(ans["score"]); err != nil {
			return fmt.Errorf("answer[%q].score must be numeric", qid)
		}
		if _, ok := ans["probabilities"].(map[string]any); !ok {
			return fmt.Errorf("answer[%q].probabilities is required for score", qid)
		}
	}
	return nil
}

// toFloat coerces json.Number/float64/int to float64.
func toFloat(v any) (float64, error) {
	switch n := v.(type) {
	case json.Number:
		return n.Float64()
	case float64:
		return n, nil
	case int:
		return float64(n), nil
	case int64:
		return float64(n), nil
	default:
		return 0, fmt.Errorf("not numeric: %T", v)
	}
}

// normalizeNumbers converts json.Number leaves to float64 so outputs
// carry plain JSON numbers.
func normalizeNumbers(v any) any {
	switch t := v.(type) {
	case map[string]any:
		out := make(map[string]any, len(t))
		for k, item := range t {
			out[k] = normalizeNumbers(item)
		}
		return out
	case []any:
		out := make([]any, len(t))
		for i, item := range t {
			out[i] = normalizeNumbers(item)
		}
		return out
	case json.Number:
		if f, err := t.Float64(); err == nil {
			return f
		}
		return t.String()
	default:
		return v
	}
}
