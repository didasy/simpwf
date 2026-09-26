package model_test

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/simpwf/workflow-engine/internal/workflow/model"
)

func testValidator(json.RawMessage) (any, error) {
	return map[string]any{"ok": true}, nil
}

func TestRegisterCustomTypeRoundTrip(t *testing.T) {
	const typ = "plantest1"
	if err := model.RegisterCustomType(typ, testValidator); err != nil {
		t.Fatalf("RegisterCustomType() error = %v", err)
	}
	if !model.ValidNodeType(typ) {
		t.Fatalf("ValidNodeType(%q) = false, want true", typ)
	}
	v, ok := model.LookupCustomType(typ)
	if !ok || v == nil {
		t.Fatalf("LookupCustomType(%q) = %v, %v; want validator", typ, v, ok)
	}
	if got, err := v(json.RawMessage(`{}`)); err != nil || got == nil {
		t.Fatalf("validator() = %v, %v; want value", got, err)
	}
	found := false
	for _, c := range model.CustomTypes() {
		if c == typ {
			found = true
		}
	}
	if !found {
		t.Fatalf("CustomTypes() missing %q", typ)
	}
}

func TestRegisterCustomTypeDuplicate(t *testing.T) {
	const typ = "plantest2"
	if err := model.RegisterCustomType(typ, testValidator); err != nil {
		t.Fatalf("first RegisterCustomType() error = %v", err)
	}
	if err := model.RegisterCustomType(typ, testValidator); err == nil {
		t.Fatal("second RegisterCustomType() = nil, want error")
	}
}

func TestRegisterCustomTypeRejectsBuiltin(t *testing.T) {
	for _, b := range []string{"script", "conditions", "input", "group", "external_call", "output", "poller"} {
		if err := model.RegisterCustomType(b, testValidator); err == nil {
			t.Errorf("RegisterCustomType(%q) = nil, want builtin-collision error", b)
		}
	}
}

func TestRegisterCustomTypeRejectsInvalidName(t *testing.T) {
	cases := map[string]string{
		"empty":      "",
		"uppercase":  "Weather",
		"leadingdig": "9lives",
		"space":      "my node",
		"dash":       "my-node",
		"dot":        "my.node",
		"toolong":    strings.Repeat("a", 65),
	}
	for name, typ := range cases {
		if err := model.RegisterCustomType(typ, testValidator); err == nil {
			t.Errorf("%s: RegisterCustomType(%q) = nil, want error", name, typ)
		}
	}
}

func TestRegisterCustomTypeRejectsNilValidator(t *testing.T) {
	if err := model.RegisterCustomType("plantest3", nil); err == nil {
		t.Fatal("RegisterCustomType(nil validator) = nil, want error")
	}
}

func TestValidNodeTypeUnknown(t *testing.T) {
	if model.ValidNodeType("plantest_missing") {
		t.Fatal("ValidNodeType(unknown) = true, want false")
	}
	if !model.ValidNodeType("script") {
		t.Fatal("ValidNodeType(script) = false, want true")
	}
}

func TestLookupCustomTypeMissing(t *testing.T) {
	if _, ok := model.LookupCustomType("plantest_missing"); ok {
		t.Fatal("LookupCustomType(unknown) ok = true, want false")
	}
}

// validConfigSchema is a minimal author sub-schema for registry tests.
var validConfigSchema = json.RawMessage(`{"type":"object","properties":{"a":{"type":"string"}},"required":["a"]}`)

func registerSchemaType(t *testing.T, typ string, schema json.RawMessage) {
	t.Helper()
	if err := model.RegisterCustomType(typ, testValidator); err != nil {
		t.Fatalf("RegisterCustomType(%q) error = %v", typ, err)
	}
	t.Cleanup(func() {
		model.UnregisterCustomType(typ)
		model.UnregisterCustomSchema(typ)
	})
	if err := model.RegisterCustomSchema(typ, schema); err != nil {
		t.Fatalf("RegisterCustomSchema(%q) error = %v", typ, err)
	}
}

func TestRegisterCustomSchemaRoundTrip(t *testing.T) {
	const typ = "schematest1"
	registerSchemaType(t, typ, validConfigSchema)
	raw, ok := model.LookupCustomSchema(typ)
	if !ok || raw == nil {
		t.Fatalf("LookupCustomSchema(%q) = %v, %v; want schema", typ, raw, ok)
	}
	if got := string(model.LookupCustomConfigSchema(typ)); got != string(validConfigSchema) {
		t.Errorf("config schema = %s, want %s", got, validConfigSchema)
	}
}

func TestRegisterCustomSchemaRejectsBadInput(t *testing.T) {
	cases := map[string]json.RawMessage{
		"empty":       nil,
		"emptyString": json.RawMessage(``),
		"array":       json.RawMessage(`[]`),
		"string":      json.RawMessage(`"a string"`),
		"number":      json.RawMessage(`42`),
		"notJSON":     json.RawMessage(`{`),
		"badKeyword":  json.RawMessage(`{"type":"not-a-json-type"}`),
		"badRef":      json.RawMessage(`{"type":"object","properties":{"a":{"$ref":"#/$defs/missing"}}}`),
	}
	for name, schema := range cases {
		typ := "schematest_" + name
		if err := model.RegisterCustomSchema(typ, schema); err == nil {
			model.UnregisterCustomSchema(typ)
			t.Errorf("%s: RegisterCustomSchema() = nil, want error", name)
		}
	}
}

func TestRegisterCustomSchemaRejectsBuiltinAndDuplicate(t *testing.T) {
	if err := model.RegisterCustomSchema("script", validConfigSchema); err == nil {
		t.Error("RegisterCustomSchema(builtin) = nil, want error")
	}
	const typ = "schematest2"
	registerSchemaType(t, typ, validConfigSchema)
	if err := model.RegisterCustomSchema(typ, validConfigSchema); err == nil {
		t.Error("second RegisterCustomSchema() = nil, want error")
	}
}

// TestCustomNodeSchemaEnvelope pins the wrapper contract: the author schema
// becomes the config property, the type is a const, and no builtin
// executable field survives.
func TestCustomNodeSchemaEnvelope(t *testing.T) {
	const typ = "schematest3"
	registerSchemaType(t, typ, validConfigSchema)
	raw, ok := model.NodeSchema(typ)
	if !ok {
		t.Fatalf("NodeSchema(%q) not found", typ)
	}
	var doc map[string]any
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatal(err)
	}
	if doc["$schema"] != "https://json-schema.org/draft/2020-12/schema" {
		t.Errorf("$schema = %v", doc["$schema"])
	}
	props, _ := doc["properties"].(map[string]any)
	typeProp, _ := props["type"].(map[string]any)
	if typeProp["const"] != typ {
		t.Errorf("properties.type.const = %v, want %q", typeProp["const"], typ)
	}
	config, _ := props["config"].(map[string]any)
	if config["type"] != "object" {
		t.Errorf("properties.config.type = %v, want object", config["type"])
	}
	if got := requiredStrings(config["required"]); len(got) != 1 || got[0] != "a" {
		t.Errorf("properties.config.required = %v, want [a]", got)
	}
	// The author schema is embedded verbatim apart from the hoisted $defs.
	if _, stillThere := config["$defs"]; stillThere {
		t.Error("config sub-schema kept its own $defs instead of being hoisted")
	}
	// Shared common fields are present; builtin executable fields are not.
	for _, common := range []string{"timeout", "output_property", "next_node", "on_failure", "pre_script", "post_script", "id", "name", "metadata", "retry_on_recovery", "input_data"} {
		if _, present := props[common]; !present {
			t.Errorf("envelope missing common field %q", common)
		}
	}
	for _, builtin := range []string{"script", "conditions", "channel", "http_config", "execution_config", "nodes", "form", "start_node_id", "context_path"} {
		if _, present := props[builtin]; present {
			t.Errorf("envelope must not allow builtin field %q", builtin)
		}
	}
}

// TestCustomNodeSchemaHoistsAuthorDefs covers an author schema that defines
// its own $defs: the refs must survive being nested in the envelope.
func TestCustomNodeSchemaHoistsAuthorDefs(t *testing.T) {
	const typ = "schematest4"
	author := json.RawMessage(`{
      "type": "object",
      "properties": {"item": {"$ref": "#/$defs/item"}},
      "$defs": {"item": {"type": "string", "minLength": 1}}
    }`)
	registerSchemaType(t, typ, author)
	raw, ok := model.NodeSchema(typ)
	if !ok {
		t.Fatalf("NodeSchema(%q) not found", typ)
	}
	var doc map[string]any
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatal(err)
	}
	defs, _ := doc["$defs"].(map[string]any)
	if _, present := defs["config_item"]; !present {
		t.Fatalf("envelope missing the hoisted def; got %v", defsKeys(defs))
	}
	props, _ := doc["properties"].(map[string]any)
	config, _ := props["config"].(map[string]any)
	inner, _ := config["properties"].(map[string]any)
	ref, _ := inner["item"].(map[string]any)["$ref"].(string)
	if ref != "#/$defs/config_item" {
		t.Errorf("config.properties.item.$ref = %v, want #/$defs/config_item", ref)
	}
}

func TestUnregisterCustomTypeDropsSchema(t *testing.T) {
	const typ = "schematest5"
	if err := model.RegisterCustomType(typ, testValidator); err != nil {
		t.Fatal(err)
	}
	if err := model.RegisterCustomSchema(typ, validConfigSchema); err != nil {
		t.Fatal(err)
	}
	model.UnregisterCustomType(typ)
	if _, ok := model.LookupCustomSchema(typ); ok {
		t.Error("UnregisterCustomType left the schema behind")
	}
	if model.ValidNodeType(typ) {
		t.Error("UnregisterCustomType left the validator behind")
	}
}

func requiredStrings(v any) []string {
	raw, _ := v.([]any)
	out := make([]string, 0, len(raw))
	for _, item := range raw {
		s, _ := item.(string)
		out = append(out, s)
	}
	return out
}

func defsKeys(defs map[string]any) []string {
	out := make([]string, 0, len(defs))
	for k := range defs {
		out = append(out, k)
	}
	return out
}
