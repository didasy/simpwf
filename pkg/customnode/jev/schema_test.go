package jev_test

import (
	"encoding/json"
	"testing"

	"github.com/santhosh-tekuri/jsonschema/v6"
	"github.com/simpwf/workflow-engine/internal/workflow/model"
)

// TestConfigSchemaIsRegistered proves the init registration carried the
// schema through the facade into the served node envelope.
func TestConfigSchemaIsRegistered(t *testing.T) {
	raw, ok := model.NodeSchema("jev")
	if !ok {
		t.Fatal("model.NodeSchema(jev) not found; the leaf did not register a schema")
	}
	var doc map[string]any
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("served schema is not valid JSON: %v", err)
	}
	props, _ := doc["properties"].(map[string]any)
	typeProp, _ := props["type"].(map[string]any)
	if typeProp["const"] != "jev" {
		t.Errorf("envelope properties.type.const = %v, want jev", typeProp["const"])
	}
	config, _ := props["config"].(map[string]any)
	if config["type"] != "object" {
		t.Fatalf("envelope properties.config.type = %v, want object", config["type"])
	}
	got := stringOfSlice(t, config["required"])
	want := []string{"api_key", "state", "questions"}
	assertSameStrings(t, "config.required", got, want)
}

// TestConfigSchemaQuestionTypes pins the question type enum and the
// per-type criteria shapes the validator enforces.
func TestConfigSchemaQuestionTypes(t *testing.T) {
	config := configOf(t)
	questions, _ := config["properties"].(map[string]any)["questions"].(map[string]any)
	if questions["type"] != "object" {
		t.Fatalf("questions.type = %v, want object", questions["type"])
	}
	if questions["minProperties"] != float64(1) {
		t.Errorf("questions.minProperties = %v, want 1", questions["minProperties"])
	}
	// The question def must survive hoisting into the envelope under a
	// namespaced name, and the ref must point at it.
	question := defOf(t, config, "question")
	qType, _ := question["properties"].(map[string]any)["type"].(map[string]any)
	assertSameStrings(t, "question.type.enum", stringOfSlice(t, qType["enum"]), []string{"noul", "choice", "score"})
	assertSameStrings(t, "question.required", stringOfSlice(t, question["required"]), []string{"type", "instructions"})

	// Per-type criteria constraints.
	choice := criteriaInBranch(t, question, "choice")
	if choice["type"] != "object" {
		t.Errorf("choice criteria type = %v, want object", choice["type"])
	}
	if choice["minProperties"] != float64(2) || choice["maxProperties"] != float64(255) {
		t.Errorf("choice criteria bounds = %v..%v, want 2..255", choice["minProperties"], choice["maxProperties"])
	}
	score := criteriaInBranch(t, question, "score")
	if score["type"] != "array" {
		t.Errorf("score criteria type = %v, want array", score["type"])
	}
	if score["minItems"] != float64(2) || score["maxItems"] != float64(10) {
		t.Errorf("score criteria bounds = %v..%v, want 2..10", score["minItems"], score["maxItems"])
	}
}

// TestConfigSchemaAcceptsShippedExample validates the seed example wrapped
// in its node envelope, so the documented shape and the served schema agree.
func TestConfigSchemaAcceptsShippedExample(t *testing.T) {
	config := json.RawMessage(`{
      "api_key": "{{ env.SIMPWF_OPENROUTER_KEY }}",
      "state": "{{ ticket }}",
      "questions": {
        "is_urgent": {"type": "noul", "instructions": "Urgent?", "criteria": {"true": "Yes", "false": "No"}},
        "department": {"type": "choice", "instructions": "Which team?", "criteria": {"billing": "Payments", "technical": "Bugs"}},
        "frustration": {"type": "score", "instructions": "How frustrated?", "criteria": ["Calm", "Frustrated", "Very angry"]}
      }
    }`)
	validate(t, wrapNode("jev", config, json.RawMessage(`{"output_property":"jev","timeout":"60s"}`)))
}

// TestConfigSchemaRejectsMissingQuestions shows the served schema still
// carries the author's required list, not just the envelope's.
func TestConfigSchemaRejectsMissingQuestions(t *testing.T) {
	validateFails(t, wrapNode("jev", json.RawMessage(`{"api_key":"k","state":"s"}`), nil))
}

// TestConfigSchemaRejectsChoiceWithOneOption pins the choice bound.
func TestConfigSchemaRejectsChoiceWithOneOption(t *testing.T) {
	validateFails(t, wrapNode("jev", json.RawMessage(`{
      "api_key": "k", "state": "s",
      "questions": {"q": {"type": "choice", "instructions": "Which?", "criteria": {"only": "One"}}}
    }`), nil))
}

// TestConfigSchemaRejectsScoreWithOneLevel pins the score bound.
func TestConfigSchemaRejectsScoreWithOneLevel(t *testing.T) {
	validateFails(t, wrapNode("jev", json.RawMessage(`{
      "api_key": "k", "state": "s",
      "questions": {"q": {"type": "score", "instructions": "How much?", "criteria": ["Calm"]}}
    }`), nil))
}

// wrapNode assembles a full custom node instance from a config object and
// the optional common fields.
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

func configOf(t *testing.T) map[string]any {
	t.Helper()
	raw, ok := model.NodeSchema("jev")
	if !ok {
		t.Fatal("model.NodeSchema(jev) not found")
	}
	var doc map[string]any
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatal(err)
	}
	props, _ := doc["properties"].(map[string]any)
	config, _ := props["config"].(map[string]any)
	return config
}

// defOf resolves a $defs entry from the served envelope, following the
// namespaced name the core hoists author defs to.
func defOf(t *testing.T, config map[string]any, name string) map[string]any {
	t.Helper()
	raw, ok := model.NodeSchema("jev")
	if !ok {
		t.Fatal("model.NodeSchema(jev) not found")
	}
	var doc map[string]any
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatal(err)
	}
	defs, _ := doc["$defs"].(map[string]any)
	// The def lives under a namespaced key; find it by suffix.
	for key, value := range defs {
		if key == name || key == "config_"+name {
			out, _ := value.(map[string]any)
			return out
		}
	}
	t.Fatalf("$defs has no %q entry; got %v", name, defs)
	return nil
}

// criteriaInBranch returns the criteria subschema of the if/then branch
// selected by a question type.
func criteriaInBranch(t *testing.T, question map[string]any, typ string) map[string]any {
	t.Helper()
	branches, _ := question["allOf"].([]any)
	for _, raw := range branches {
		branch, _ := raw.(map[string]any)
		ifProp, _ := branch["if"].(map[string]any)["properties"].(map[string]any)
		typeProp, _ := ifProp["type"].(map[string]any)
		if typeProp["const"] != typ {
			continue
		}
		then, _ := branch["then"].(map[string]any)
		props, _ := then["properties"].(map[string]any)
		criteria, _ := props["criteria"].(map[string]any)
		return criteria
	}
	t.Fatalf("question schema has no if/then branch for type %q", typ)
	return nil
}

// validate compiles the jev envelope and validates an instance against it.
func validate(t *testing.T, instance json.RawMessage) {
	t.Helper()
	if err := compileServed(t).Validate(anyOf(t, instance)); err != nil {
		t.Errorf("served schema rejected a valid jev node: %v", err)
	}
}

// validateFails is the negative counterpart: the instance must not satisfy
// the served schema.
func validateFails(t *testing.T, instance json.RawMessage) {
	t.Helper()
	if err := compileServed(t).Validate(anyOf(t, instance)); err == nil {
		t.Error("served schema accepted an invalid jev config")
	}
}

func compileServed(t *testing.T) *jsonschema.Schema {
	t.Helper()
	raw, ok := model.NodeSchema("jev")
	if !ok {
		t.Fatal("model.NodeSchema(jev) not found")
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
