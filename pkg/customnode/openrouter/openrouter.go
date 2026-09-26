// Package openrouter is a custom node calling OpenRouter text models through
// the shared HTTP executor. It supports both Chat Completions and Responses
// APIs. API keys are rendered from the instance environment snapshot and are
// never returned or logged.
package openrouter

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/simpwf/workflow-engine/internal/workflow/executor"
	"github.com/simpwf/workflow-engine/pkg/contextpath"
	"github.com/simpwf/workflow-engine/pkg/customnode"
)

const (
	APIChat      = "chat"
	APIResponses = "responses"

	DefaultModel             = "openai/gpt-4o-mini"
	DefaultChatEndpoint      = "https://openrouter.ai/api/v1/chat/completions"
	DefaultResponsesEndpoint = "https://openrouter.ai/api/v1/responses"
	DefaultTimeout           = 30 * time.Second
	MaxTemperature           = 2
	MaxTopP                  = 1
)

// configSchema describes the openrouter config object. It is documentation
// for frontend form rendering: ValidateConfig stays authoritative. Prompt
// and messages are mutually exclusive, mirroring the validator. Numeric
// fields accept a template instead of a literal, so they stay untyped
// (documented range only) rather than typed as numbers.
var configSchema = json.RawMessage(`{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "title": "openrouter config",
  "description": "Calls an OpenRouter text model through the shared HTTP executor.",
  "type": "object",
  "properties": {
    "api": { "type": "string", "enum": ["chat", "responses"], "default": "chat" },
    "model": { "type": "string", "default": "openai/gpt-4o-mini" },
    "endpoint": { "type": "string", "description": "Absolute http(s) url or a {{ path }} template; defaults per api." },
    "api_key": { "type": "string", "minLength": 1, "description": "Templated, e.g. {{ env.SIMPWF_OPENROUTER_KEY }}." },
    "prompt": { "type": "string", "minLength": 1, "description": "Single-turn shorthand. Mutually exclusive with messages; system requires prompt." },
    "system": { "type": "string", "minLength": 1, "description": "System instruction. Only allowed together with prompt." },
    "messages": {
      "type": "array",
      "minItems": 1,
      "description": "Explicit message list. Mutually exclusive with prompt.",
      "items": {
        "type": "object",
        "properties": {
          "role": { "type": "string", "enum": ["system", "user", "assistant", "developer"], "description": "developer is responses-only." },
          "content": { "description": "String, or an array of structured content parts." }
        },
        "required": ["role", "content"],
        "additionalProperties": false
      }
    },
    "temperature": { "description": "Number between 0 and 2, or a {{ path }} template." },
    "max_tokens": { "description": "Positive integer, or a {{ path }} template." },
    "top_p": { "description": "Number greater than 0 and at most 1, or a {{ path }} template." },
    "stop": { "type": "array", "minItems": 1, "items": { "type": "string", "minLength": 1 } },
    "timeout": { "description": "Positive Go duration string or a {{ path }} template." }
  },
  "required": ["api_key"],
  "oneOf": [
    { "required": ["prompt"] },
    { "required": ["messages"] }
  ]
}`)

func init() {
	customnode.MustRegister(customnode.Definition{
		Type:     "openrouter",
		Validate: ValidateConfig,
		Schema:   configSchema,
		New: func(d customnode.Deps) (executor.Executor, error) {
			if d.HTTP == nil {
				return nil, fmt.Errorf("openrouter: shared HTTP client is not configured")
			}
			return &Executor{http: d.HTTP, maxBytes: d.Limits.MaxOutputBytes}, nil
		},
	})
}

// Message is one explicit chat or response message. Content is kept raw so
// string and structured content parts can be rendered without lossy parsing.
type Message struct {
	Role    string          `json:"role"`
	Content json.RawMessage `json:"content"`
}

// Config is the validated form of the openrouter node config object.
type Config struct {
	API         string        `json:"api"`
	Model       string        `json:"model"`
	Endpoint    string        `json:"endpoint"`
	APIKey      string        `json:"api_key"`
	Prompt      string        `json:"prompt,omitempty"`
	System      string        `json:"system,omitempty"`
	Messages    []Message     `json:"messages,omitempty"`
	Temperature *float64      `json:"temperature,omitempty"`
	MaxTokens   *int          `json:"max_tokens,omitempty"`
	TopP        *float64      `json:"top_p,omitempty"`
	Stop        []string      `json:"stop,omitempty"`
	Timeout     time.Duration `json:"timeout,omitempty"`

	rawModel       *string
	rawEndpoint    *string
	rawTemperature json.RawMessage
	rawMaxTokens   json.RawMessage
	rawTopP        json.RawMessage
	rawTimeout     json.RawMessage
}

type rawMessage struct {
	Role    *string         `json:"role"`
	Content json.RawMessage `json:"content"`
}

type rawConfig struct {
	API         *string         `json:"api"`
	Model       *string         `json:"model"`
	Endpoint    *string         `json:"endpoint"`
	APIKey      *string         `json:"api_key"`
	Prompt      *string         `json:"prompt"`
	System      *string         `json:"system"`
	Messages    json.RawMessage `json:"messages"`
	Temperature json.RawMessage `json:"temperature"`
	MaxTokens   json.RawMessage `json:"max_tokens"`
	TopP        json.RawMessage `json:"top_p"`
	Stop        *[]string       `json:"stop"`
	Timeout     json.RawMessage `json:"timeout"`
}

// ValidateConfig owns the openrouter schema. It checks raw values only:
// templates are unresolved at parse time and are rendered at execution time.
func ValidateConfig(raw json.RawMessage) (any, error) {
	if len(raw) == 0 || strings.TrimSpace(string(raw)) == "null" {
		return nil, fmt.Errorf("config is required")
	}
	var r rawConfig
	dec := json.NewDecoder(bytes.NewReader(raw))
	if err := dec.Decode(&r); err != nil {
		return nil, fmt.Errorf("config must be an object: %w", err)
	}

	api := APIChat
	if r.API != nil {
		api = strings.TrimSpace(*r.API)
	}
	if api != APIChat && api != APIResponses {
		return nil, fmt.Errorf("config.api %q must be chat or responses", api)
	}

	model := DefaultModel
	if r.Model != nil && strings.TrimSpace(*r.Model) != "" {
		model = strings.TrimSpace(*r.Model)
	}

	endpoint := DefaultChatEndpoint
	if api == APIResponses {
		endpoint = DefaultResponsesEndpoint
	}
	if r.Endpoint != nil && strings.TrimSpace(*r.Endpoint) != "" {
		endpoint = strings.TrimSpace(*r.Endpoint)
		if !contextpath.HasTemplate(endpoint) {
			if err := validateEndpoint(endpoint); err != nil {
				return nil, err
			}
		}
	}

	if r.APIKey == nil || strings.TrimSpace(*r.APIKey) == "" {
		return nil, fmt.Errorf("config.api_key is required")
	}
	rawModel := r.Model
	if rawModel != nil && strings.TrimSpace(*rawModel) == "" {
		rawModel = nil
	}
	rawEndpoint := r.Endpoint
	if rawEndpoint != nil && strings.TrimSpace(*rawEndpoint) == "" {
		rawEndpoint = nil
	}
	cfg := Config{
		API: api, Model: model, Endpoint: endpoint, APIKey: *r.APIKey,
		rawModel: rawModel, rawEndpoint: rawEndpoint, rawTemperature: r.Temperature, rawMaxTokens: r.MaxTokens, rawTopP: r.TopP, rawTimeout: r.Timeout,
	}
	if r.Prompt != nil {
		if strings.TrimSpace(*r.Prompt) == "" {
			return nil, fmt.Errorf("config.prompt must be non-blank")
		}
		cfg.Prompt = *r.Prompt
	}
	if r.System != nil {
		if strings.TrimSpace(*r.System) == "" {
			return nil, fmt.Errorf("config.system must be non-blank when set")
		}
		cfg.System = *r.System
	}
	if cfg.Prompt == "" && (len(r.Messages) > 0 || strings.TrimSpace(string(r.Messages)) != "null") && r.System != nil {
		return nil, fmt.Errorf("config.system requires config.prompt")
	}
	if cfg.Prompt != "" || len(r.Messages) == 0 || strings.TrimSpace(string(r.Messages)) == "null" {
		if cfg.Prompt == "" {
			return nil, fmt.Errorf("config requires exactly one of prompt or messages")
		}
		if len(r.Messages) > 0 && strings.TrimSpace(string(r.Messages)) != "null" {
			return nil, fmt.Errorf("config.prompt and config.messages are mutually exclusive")
		}
		if r.System != nil && cfg.Prompt == "" {
			return nil, fmt.Errorf("config.system requires config.prompt")
		}
	}

	if len(r.Messages) > 0 && strings.TrimSpace(string(r.Messages)) != "null" {
		if cfg.Prompt != "" {
			return nil, fmt.Errorf("config.prompt and config.messages are mutually exclusive")
		}
		var messages []rawMessage
		if err := json.Unmarshal(r.Messages, &messages); err != nil {
			return nil, fmt.Errorf("config.messages must be an array: %w", err)
		}
		if len(messages) == 0 {
			return nil, fmt.Errorf("config.messages is required with at least one message")
		}
		for i, raw := range messages {
			if raw.Role == nil || strings.TrimSpace(*raw.Role) == "" {
				return nil, fmt.Errorf("config.messages[%d].role is required", i)
			}
			role := strings.TrimSpace(*raw.Role)
			if !contextpath.HasTemplate(role) && !validRole(role, api) {
				return nil, fmt.Errorf("config.messages[%d].role %q is not valid for %s", i, role, api)
			}
			if err := validateContent(raw.Content); err != nil {
				return nil, fmt.Errorf("config.messages[%d].content: %w", i, err)
			}
			cfg.Messages = append(cfg.Messages, Message{Role: role, Content: append(json.RawMessage(nil), raw.Content...)})
		}
	} else if r.System != nil && cfg.Prompt == "" {
		return nil, fmt.Errorf("config.system requires config.prompt")
	}

	if value, literal, err := decodeNumberField("temperature", r.Temperature); err != nil {
		return nil, err
	} else if literal {
		if value < 0 || value > MaxTemperature {
			return nil, fmt.Errorf("config.temperature must be between 0 and %d", MaxTemperature)
		}
		cfg.Temperature = &value
	}
	if value, literal, err := decodeIntField("max_tokens", r.MaxTokens); err != nil {
		return nil, err
	} else if literal {
		if value <= 0 {
			return nil, fmt.Errorf("config.max_tokens must be greater than 0")
		}
		cfg.MaxTokens = &value
	}
	if value, literal, err := decodeNumberField("top_p", r.TopP); err != nil {
		return nil, err
	} else if literal {
		if value <= 0 || value > MaxTopP {
			return nil, fmt.Errorf("config.top_p must be greater than 0 and at most %d", MaxTopP)
		}
		cfg.TopP = &value
	}
	if r.Stop != nil {
		if len(*r.Stop) == 0 {
			return nil, fmt.Errorf("config.stop must contain at least one string")
		}
		for i, stop := range *r.Stop {
			if strings.TrimSpace(stop) == "" {
				return nil, fmt.Errorf("config.stop[%d] must be non-blank", i)
			}
		}
		cfg.Stop = append([]string(nil), (*r.Stop)...)
	}
	if raw := r.Timeout; len(raw) > 0 && !isNull(raw) {
		rawString, ok, err := rawStringOrTemplate("timeout", raw)
		if err != nil {
			return nil, err
		}
		if ok {
			timeout, err := time.ParseDuration(rawString)
			if err != nil || timeout <= 0 {
				return nil, fmt.Errorf("config.timeout must be a positive duration")
			}
			cfg.Timeout = timeout
		}
	}
	return cfg, nil
}

func isNull(raw json.RawMessage) bool {
	return strings.TrimSpace(string(raw)) == "null"
}

func rawStringOrTemplate(field string, raw json.RawMessage) (string, bool, error) {
	var value string
	if err := json.Unmarshal(raw, &value); err == nil {
		if contextpath.HasTemplate(value) {
			return value, false, nil
		}
		return value, true, nil
	}
	var number json.Number
	if err := json.Unmarshal(raw, &number); err == nil {
		return number.String(), true, nil
	}
	if contextpath.HasTemplate(string(raw)) {
		return string(raw), false, nil
	}
	return "", false, fmt.Errorf("config.%s must be a number or template", field)
}

func decodeNumberField(field string, raw json.RawMessage) (float64, bool, error) {
	if len(raw) == 0 || isNull(raw) {
		return 0, false, nil
	}
	rawString, literal, err := rawStringOrTemplate(field, raw)
	if err != nil {
		return 0, false, err
	}
	if !literal {
		return 0, false, nil
	}
	value, err := strconv.ParseFloat(rawString, 64)
	if err != nil {
		return 0, false, fmt.Errorf("config.%s must be a number or template", field)
	}
	return value, true, nil
}

func decodeIntField(field string, raw json.RawMessage) (int, bool, error) {
	value, literal, err := decodeNumberField(field, raw)
	if err != nil || !literal {
		return 0, literal, err
	}
	integer := int(value)
	if float64(integer) != value {
		return 0, false, fmt.Errorf("config.%s must be an integer or template", field)
	}
	return integer, true, nil
}

func validateEndpoint(endpoint string) error {
	u, err := url.Parse(endpoint)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return fmt.Errorf("config.endpoint %q must be an absolute http(s) url", endpoint)
	}
	return nil
}

func validRole(role, api string) bool {
	switch role {
	case "system", "user", "assistant":
		return true
	case "developer":
		return api == APIResponses
	default:
		return false
	}
}

func validateContent(raw json.RawMessage) error {
	if len(raw) == 0 || strings.TrimSpace(string(raw)) == "null" {
		return fmt.Errorf("is required")
	}
	var content any
	if err := json.Unmarshal(raw, &content); err != nil {
		return fmt.Errorf("must be valid JSON: %w", err)
	}
	switch content.(type) {
	case string, []any:
		return nil
	default:
		return fmt.Errorf("must be a string or array")
	}
}

type httpDo interface {
	Do(ctx context.Context, method, target string, headers map[string]string, body []byte, timeout time.Duration) ([]byte, int, http.Header, error)
}

type Executor struct {
	http     httpDo
	maxBytes int
}

// NewForTest builds an Executor with a fake HTTP client. Tests only.
func NewForTest(h httpDo, maxBytes int) *Executor {
	return &Executor{http: h, maxBytes: maxBytes}
}

func renderAPIKey(tpl string, ctx map[string]any) (string, error) {
	v, err := contextpath.RenderTemplate(tpl, ctx)
	if err != nil {
		return "", err
	}
	s, ok := v.(string)
	if !ok || strings.TrimSpace(s) == "" {
		return "", fmt.Errorf("openrouter: rendered api_key is blank (want non-empty string)")
	}
	return s, nil
}

func renderNumber(field string, raw json.RawMessage, ctx map[string]any) (*float64, error) {
	if len(raw) == 0 || isNull(raw) {
		return nil, nil
	}
	rawString, literal, err := rawStringOrTemplate(field, raw)
	if err != nil {
		return nil, err
	}
	if !literal {
		rendered, renderErr := contextpath.RenderTemplate(rawString, ctx)
		if renderErr != nil {
			return nil, renderErr
		}
		var ok bool
		rawString, ok = rendered.(string)
		if !ok {
			rawString = fmt.Sprint(rendered)
		}
	}
	value, err := strconv.ParseFloat(rawString, 64)
	if err != nil {
		return nil, fmt.Errorf("%s must render to a number", field)
	}
	return &value, nil
}

func renderDuration(field string, raw json.RawMessage, ctx map[string]any) (time.Duration, error) {
	if len(raw) == 0 || isNull(raw) {
		return 0, nil
	}
	rawString, literal, err := rawStringOrTemplate(field, raw)
	if err != nil {
		return 0, err
	}
	if !literal {
		rendered, renderErr := contextpath.RenderTemplate(rawString, ctx)
		if renderErr != nil {
			return 0, renderErr
		}
		var ok bool
		rawString, ok = rendered.(string)
		if !ok {
			rawString = fmt.Sprint(rendered)
		}
	}
	value, err := time.ParseDuration(rawString)
	if err != nil || value <= 0 {
		return 0, fmt.Errorf("%s must render to a positive duration", field)
	}
	return value, nil
}

func renderString(tpl string, ctx map[string]any) (string, error) {
	v, err := contextpath.RenderTemplate(tpl, ctx)
	if err != nil {
		return "", err
	}
	s, ok := v.(string)
	if !ok {
		return "", fmt.Errorf("must render to a string")
	}
	if strings.TrimSpace(s) == "" {
		return "", fmt.Errorf("must render to a non-blank string")
	}
	return s, nil
}

func renderContent(raw json.RawMessage, ctx map[string]any) (any, error) {
	rendered, err := contextpath.RenderJSON(raw, ctx)
	if err != nil {
		return nil, err
	}
	var content any
	dec := json.NewDecoder(bytes.NewReader(rendered))
	dec.UseNumber()
	if err := dec.Decode(&content); err != nil {
		return nil, err
	}
	switch content.(type) {
	case string, []any:
		return content, nil
	default:
		return nil, fmt.Errorf("content must render to a string or array")
	}
}

func renderMessages(cfg Config, ctx map[string]any) ([]any, error) {
	if cfg.Prompt != "" {
		messages := make([]any, 0, 2)
		if cfg.System != "" {
			system, err := renderString(cfg.System, ctx)
			if err != nil {
				return nil, fmt.Errorf("system: %w", err)
			}
			messages = append(messages, map[string]any{"role": "system", "content": system})
		}
		prompt, err := renderString(cfg.Prompt, ctx)
		if err != nil {
			return nil, fmt.Errorf("prompt: %w", err)
		}
		messages = append(messages, map[string]any{"role": "user", "content": prompt})
		return messages, nil
	}
	messages := make([]any, 0, len(cfg.Messages))
	for i, message := range cfg.Messages {
		role, err := renderString(message.Role, ctx)
		if err != nil {
			return nil, fmt.Errorf("messages[%d].role: %w", i, err)
		}
		if !validRole(role, cfg.API) {
			return nil, fmt.Errorf("messages[%d].role %q is not valid for %s", i, role, cfg.API)
		}
		content, err := renderContent(message.Content, ctx)
		if err != nil {
			return nil, fmt.Errorf("messages[%d].content: %w", i, err)
		}
		messages = append(messages, map[string]any{"role": role, "content": content})
	}
	return messages, nil
}

func buildChatPayload(cfg Config, messages []any) map[string]any {
	payload := map[string]any{"model": cfg.Model, "messages": messages}
	addTuning(payload, cfg, "max_tokens")
	return payload
}

func buildResponsesPayload(cfg Config, input []any) map[string]any {
	payload := map[string]any{"model": cfg.Model, "input": input}
	addTuning(payload, cfg, "max_output_tokens")
	return payload
}

func addTuning(payload map[string]any, cfg Config, maxTokensKey string) {
	if cfg.Temperature != nil {
		payload["temperature"] = *cfg.Temperature
	}
	if cfg.MaxTokens != nil {
		payload[maxTokensKey] = *cfg.MaxTokens
	}
	if cfg.TopP != nil {
		payload["top_p"] = *cfg.TopP
	}
	if len(cfg.Stop) > 0 {
		payload["stop"] = cfg.Stop
	}
}

type responseEnvelope struct {
	Model      string          `json:"model"`
	Choices    []chatChoice    `json:"choices"`
	OutputText string          `json:"output_text"`
	Output     []responseItem  `json:"output"`
	Usage      json.RawMessage `json:"usage"`
	Status     string          `json:"status"`
}

type chatChoice struct {
	Message struct {
		Content json.RawMessage `json:"content"`
	} `json:"message"`
	FinishReason string `json:"finish_reason"`
}

type responseItem struct {
	Type    string          `json:"type"`
	Status  string          `json:"status"`
	Content json.RawMessage `json:"content"`
}

func extractText(api string, body []byte) (model, text, finishReason string, usage any, err error) {
	var resp responseEnvelope
	dec := json.NewDecoder(bytes.NewReader(body))
	if err := dec.Decode(&resp); err != nil {
		return "", "", "", nil, fmt.Errorf("decode response: %w", err)
	}
	model = resp.Model
	if api == APIChat {
		if len(resp.Choices) == 0 {
			return model, "", "", nil, fmt.Errorf("response has no choices")
		}
		text, err = rawText(resp.Choices[0].Message.Content)
		if err != nil {
			return model, "", "", nil, fmt.Errorf("response choices[0].message.content: %w", err)
		}
		finishReason = resp.Choices[0].FinishReason
	} else {
		if strings.TrimSpace(resp.OutputText) != "" {
			text = resp.OutputText
		} else {
			for _, item := range resp.Output {
				text, err = rawText(item.Content)
				if err == nil && strings.TrimSpace(text) != "" {
					break
				}
				text = ""
			}
		}
		if strings.TrimSpace(text) == "" {
			return model, "", "", nil, fmt.Errorf("response has no output text")
		}
		finishReason = resp.Status
		if finishReason == "" {
			for _, item := range resp.Output {
				if item.Status != "" {
					finishReason = item.Status
					break
				}
			}
		}
		if finishReason == "" {
			finishReason = "completed"
		}
	}
	if len(resp.Usage) > 0 && strings.TrimSpace(string(resp.Usage)) != "null" {
		var value any
		dec := json.NewDecoder(bytes.NewReader(resp.Usage))
		dec.UseNumber()
		if err := dec.Decode(&value); err != nil {
			return model, "", "", nil, fmt.Errorf("decode usage: %w", err)
		}
		usage = normalizeNumbers(value)
	}
	if strings.TrimSpace(model) == "" {
		return model, "", "", nil, fmt.Errorf("response has no model")
	}
	return model, text, finishReason, usage, nil
}

func rawText(raw json.RawMessage) (string, error) {
	if len(raw) == 0 || strings.TrimSpace(string(raw)) == "null" {
		return "", fmt.Errorf("is missing")
	}
	var text string
	if err := json.Unmarshal(raw, &text); err == nil {
		if strings.TrimSpace(text) == "" {
			return "", fmt.Errorf("is blank")
		}
		return text, nil
	}
	var parts []struct {
		Text string `json:"text"`
	}
	if err := json.Unmarshal(raw, &parts); err != nil {
		return "", fmt.Errorf("must be a string or array of content parts")
	}
	var builder strings.Builder
	for _, part := range parts {
		if part.Text != "" {
			builder.WriteString(part.Text)
		}
	}
	if strings.TrimSpace(builder.String()) == "" {
		return "", fmt.Errorf("is blank")
	}
	return builder.String(), nil
}

func fail(req executor.Request, reason string, err error) (*executor.Result, error) {
	nodeErr := &executor.NodeError{Node: req.Node, Reason: reason, Err: err}
	if req.Node != nil && req.Node.OnFailure != nil {
		return &executor.Result{Output: map[string]any{}}, nodeErr
	}
	return nil, nodeErr
}

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

func (e *Executor) Execute(ctx context.Context, req executor.Request) (*executor.Result, error) {
	if req.Node == nil {
		return nil, fmt.Errorf("openrouter: request has no node")
	}
	cfg, ok := req.Node.Custom.(Config)
	if !ok {
		return nil, fmt.Errorf("openrouter: node custom config has unexpected type %T", req.Node.Custom)
	}
	tplCtx := req.Context
	if tplCtx == nil {
		tplCtx = map[string]any{}
	}
	apiKey, err := renderAPIKey(cfg.APIKey, tplCtx)
	if err != nil {
		return fail(req, "openrouter", fmt.Errorf("api_key: %w", err))
	}
	if cfg.rawModel != nil {
		cfg.Model, err = renderString(*cfg.rawModel, tplCtx)
		if err != nil {
			return fail(req, "openrouter", fmt.Errorf("model: %w", err))
		}
	}
	if cfg.rawEndpoint != nil {
		cfg.Endpoint, err = renderString(*cfg.rawEndpoint, tplCtx)
		if err != nil {
			return fail(req, "openrouter", fmt.Errorf("endpoint: %w", err))
		}
		if err := validateEndpoint(cfg.Endpoint); err != nil {
			return fail(req, "openrouter", err)
		}
	}
	if value, err := renderNumber("temperature", cfg.rawTemperature, tplCtx); err != nil {
		return fail(req, "openrouter", err)
	} else if value != nil {
		if *value < 0 || *value > MaxTemperature {
			return fail(req, "openrouter", fmt.Errorf("temperature must be between 0 and %d", MaxTemperature))
		}
		cfg.Temperature = value
	}
	if value, err := renderNumber("max_tokens", cfg.rawMaxTokens, tplCtx); err != nil {
		return fail(req, "openrouter", err)
	} else if value != nil {
		integer := int(*value)
		if float64(integer) != *value || integer <= 0 {
			return fail(req, "openrouter", fmt.Errorf("max_tokens must render to a positive integer"))
		}
		cfg.MaxTokens = &integer
	}
	if value, err := renderNumber("top_p", cfg.rawTopP, tplCtx); err != nil {
		return fail(req, "openrouter", err)
	} else if value != nil {
		if *value <= 0 || *value > MaxTopP {
			return fail(req, "openrouter", fmt.Errorf("top_p must render to a value greater than 0 and at most %d", MaxTopP))
		}
		cfg.TopP = value
	}
	timeout, err := renderDuration("timeout", cfg.rawTimeout, tplCtx)
	if err != nil {
		return fail(req, "openrouter", err)
	}
	if timeout > 0 {
		cfg.Timeout = timeout
	}
	messages, err := renderMessages(cfg, tplCtx)
	if err != nil {
		return fail(req, "openrouter", err)
	}
	stop := make([]string, len(cfg.Stop))
	for i, value := range cfg.Stop {
		stop[i], err = renderString(value, tplCtx)
		if err != nil {
			return fail(req, "openrouter", fmt.Errorf("stop[%d]: %w", i, err))
		}
	}
	cfg.Stop = stop
	var payload map[string]any
	if cfg.API == APIChat {
		payload = buildChatPayload(cfg, messages)
	} else {
		payload = buildResponsesPayload(cfg, messages)
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return fail(req, "openrouter", fmt.Errorf("encode request: %w", err))
	}
	if e.http == nil {
		return fail(req, "openrouter", fmt.Errorf("shared HTTP client is not configured"))
	}
	headers := map[string]string{
		"Authorization": "Bearer " + apiKey,
		"Content-Type":  "application/json",
	}
	timeout = cfg.Timeout
	if timeout <= 0 {
		timeout = req.Node.Timeout
	}
	if timeout <= 0 {
		timeout = DefaultTimeout
	}
	responseBody, status, _, err := e.http.Do(ctx, "POST", cfg.Endpoint, headers, body, timeout)
	if err != nil {
		return fail(req, "openrouter", err)
	}
	if status >= 300 {
		return fail(req, "openrouter-status", fmt.Errorf("openrouter request failed with status %d", status))
	}
	if e.maxBytes > 0 && len(responseBody) > e.maxBytes {
		return fail(req, "openrouter", fmt.Errorf("openrouter response exceeds output cap (%d > %d bytes)", len(responseBody), e.maxBytes))
	}
	model, text, finishReason, usage, err := extractText(cfg.API, responseBody)
	if err != nil {
		return fail(req, "openrouter", err)
	}
	return &executor.Result{Output: map[string]any{
		"model": model, "api": cfg.API, "text": text, "usage": usage, "finish_reason": finishReason,
	}}, nil
}
