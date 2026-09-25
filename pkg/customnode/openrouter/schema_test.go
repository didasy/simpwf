package openrouter_test

import (
	"encoding/json"
	"testing"

	"github.com/santhosh-tekuri/jsonschema/v6"
	"github.com/simpwf/workflow-engine/internal/workflow/model"
)

// TestConfigSchemaIsRegistered proves the init registration carried the
// schema through the facade into the served node envelope.
func TestConfigSchemaIsRegistered(t *testing.T) {
	raw, ok := model.NodeSchema("openrouter")
	if !ok {
		t.Fatal("model.NodeSchema(openrouter) not found; the leaf did not register a schema")
	}
	var doc map[string]any
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("served schema is not valid JSON: %v", err)
	}
	props, _ := doc["properties"].(map[string]any)
	typeProp, _ := props["type"].(map[string]any)
	if typeProp["const"] != "openrouter" {
		t.Errorf("envelope properties.type.const = %v, want openrouter", typeProp["const"])
	}
	config, _ := props["config"].(map[string]any)
	if config["type"] != "object" {
		t.Fatalf("envelope properties.config.type = %v, want object", config["type"])
	}
	assertSameStrings(t, "config.required", stringOfSlice(t, config["required"]), []string{"api_key"})
}

// TestConfigSchemaAPIEnum pins the api enum.
func TestConfigSchemaAPIEnum(t *testing.T) {
	config := configOf(t)
	props, _ := config["properties"].(map[string]any)
	api, _ := props["api"].(map[string]any)
	assertSameStrings(t, "config.api.enum", stringOfSlice(t, api["enum"]), []string{"chat", "responses"})
}

// TestConfigSchemaPromptOrMessages pins the mutual exclusion: exactly one of
// prompt or messages, matching the validator.
func TestConfigSchemaPromptOrMessages(t *testing.T) {
	config := configOf(t)
	oneOf, _ := config["oneOf"].([]any)
	if len(oneOf) != 2 {
		t.Fatalf("config.oneOf has %d arms, want 2", len(oneOf))
	}
	got := map[string]bool{}
	for _, raw := range oneOf {
		arm, _ := raw.(map[string]any)
		names := stringOfSlice(t, arm["required"])
		if len(names) != 1 {
			t.Errorf("oneOf arm requires %v, want exactly one field", names)
			continue
		}
		got[names[0]] = true
	}
	if !got["prompt"] || !got["messages"] {
		t.Errorf("oneOf arms = %v, want prompt and messages", got)
	}
}

// TestConfigSchemaMessageRoles pins the role enum and the required fields.
func TestConfigSchemaMessageRoles(t *testing.T) {
	config := configOf(t)
	props, _ := config["properties"].(map[string]any)
	messages, _ := props["messages"].(map[string]any)
	if messages["type"] != "array" {
		t.Errorf("messages.type = %v, want array", messages["type"])
	}
	if messages["minItems"] != float64(1) {
		t.Errorf("messages.minItems = %v, want 1", messages["minItems"])
	}
	item, _ := messages["items"].(map[string]any)
	itemProps, _ := item["properties"].(map[string]any)
	role, _ := itemProps["role"].(map[string]any)
	assertSameStrings(t, "messages.items.role.enum", stringOfSlice(t, role["enum"]),
		[]string{"system", "user", "assistant", "developer"})
	assertSameStrings(t, "messages.items.required", stringOfSlice(t, item["required"]), []string{"role", "content"})
	if item["additionalProperties"] != false {
		t.Error("messages.items must forbid additional properties")
	}
}

// TestConfigSchemaNumericRanges pins the documented ranges on the
// unconstrained numeric fields, which accept a template instead of a literal.
func TestConfigSchemaNumericRanges(t *testing.T) {
	config := configOf(t)
	props, _ := config["properties"].(map[string]any)
	for _, field := range []string{"temperature", "max_tokens", "top_p"} {
		if _, present := props[field]; !present {
			t.Errorf("config is missing %s", field)
		}
	}
	// stop is a constrained array.
	stop, _ := props["stop"].(map[string]any)
	if stop["type"] != "array" || stop["minItems"] != float64(1) {
		t.Errorf("stop = %v, want an array with minItems 1", stop)
	}
}

// TestConfigSchemaAcceptsShippedExamples validates both seed definitions
// wrapped in their node envelope.
func TestConfigSchemaAcceptsShippedExamples(t *testing.T) {
	chat := wrapNode("openrouter", json.RawMessage(`{
      "api": "chat", "model": "openai/gpt-4o-mini",
      "api_key": "{{ env.SIMPWF_OPENROUTER_KEY }}",
      "system": "Answer briefly and accurately.",
      "prompt": "{{ question }}",
      "temperature": 0.2, "max_tokens": 256, "top_p": 0.9
    }`), json.RawMessage(`{"output_property":"chat_answer","timeout":"60s"}`))
	validate(t, chat)

	responses := wrapNode("openrouter", json.RawMessage(`{
      "api": "responses", "model": "openai/gpt-4o-mini",
      "api_key": "{{ env.SIMPWF_OPENROUTER_KEY }}",
      "messages": [
        {"role": "developer", "content": "Use the prior chat answer and produce a concise follow-up."},
        {"role": "user", "content": "Question: {{ question }}"}
      ],
      "temperature": 0.3, "max_tokens": 256
    }`), json.RawMessage(`{"output_property":"responses_answer","timeout":"60s"}`))
	validate(t, responses)
}

func TestConfigSchemaRejectsPromptAndMessagesTogether(t *testing.T) {
	validateFails(t, wrapNode("openrouter", json.RawMessage(`{
      "api_key": "k", "prompt": "hi", "messages": [{"role": "user", "content": "hi"}]
    }`), nil))
}

func TestConfigSchemaRejectsNeitherPromptNorMessages(t *testing.T) {
	validateFails(t, wrapNode("openrouter", json.RawMessage(`{"api_key": "k"}`), nil))
}

func TestConfigSchemaRejectsUnknownAPI(t *testing.T) {
	validateFails(t, wrapNode("openrouter", json.RawMessage(`{"api_key": "k", "api": "completions", "prompt": "hi"}`), nil))
}

func TestConfigSchemaRejectsUnknownRole(t *testing.T) {
	validateFails(t, wrapNode("openrouter", json.RawMessage(`{
      "api_key": "k", "messages": [{"role": "root", "content": "hi"}]
    }`), nil))
}

func configOf(t *testing.T) map[string]any {
	t.Helper()
	raw, ok := model.NodeSchema("openrouter")
	if !ok {
		t.Fatal("model.NodeSchema(openrouter) not found")
	}
	var doc map[string]any
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatal(err)
	}
	props, _ := doc["properties"].(map[string]any)
	config, _ := props["config"].(map[string]any)
	return config
}

func wrapNode(nodeType string, config, common json.RawMessage) json.RawMessage {
	instance := map[string]any{"type": nodeType, "config": json.RawMessage(config)}
	if len(common) > 0 {
		if err := json.Unmarshal(common, &instance); err != nil {
			panic(err)
		}
	}
	raw, err := json.Marshal(instance)
	if err != nil {
		panic(err)
	}
	return raw
}

func validate(t *testing.T, instance json.RawMessage) {
	t.Helper()
	if err := compileServed(t).Validate(anyOf(t, instance)); err != nil {
		t.Errorf("served schema rejected a valid openrouter node: %v", err)
	}
}

func validateFails(t *testing.T, instance json.RawMessage) {
	t.Helper()
	if err := compileServed(t).Validate(anyOf(t, instance)); err == nil {
		t.Error("served schema accepted an invalid openrouter config")
	}
}

func compileServed(t *testing.T) *jsonschema.Schema {
	t.Helper()
	raw, ok := model.NodeSchema("openrouter")
	if !ok {
		t.Fatal("model.NodeSchema(openrouter) not found")
	}
	var doc any
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatal(err)
	}
	c := jsonschema.NewCompiler()
	c.DefaultDraft(jsonschema.Draft2020)
	if err := c.AddResource("node.json", doc); err != nil {
		t.Fatal(err)
	}
	compiled, err := c.Compile("node.json")
	if err != nil {
		t.Fatalf("served schema does not compile: %v", err)
	}
	return compiled
}

func anyOf(t *testing.T, raw json.RawMessage) any {
	t.Helper()
	var value any
	if err := json.Unmarshal(raw, &value); err != nil {
		t.Fatal(err)
	}
	return value
}

func stringOfSlice(t *testing.T, v any) []string {
	t.Helper()
	raw, _ := v.([]any)
	out := make([]string, 0, len(raw))
	for _, item := range raw {
		s, _ := item.(string)
		out = append(out, s)
	}
	return out
}

func assertSameStrings(t *testing.T, label string, got, want []string) {
	t.Helper()
	if len(got) != len(want) {
		t.Errorf("%s = %v, want %v", label, got, want)
		return
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("%s = %v, want %v", label, got, want)
			return
		}
	}
}
