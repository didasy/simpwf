package model_test

import (
	"encoding/json"
	"testing"

	"github.com/santhosh-tekuri/jsonschema/v6"
	"github.com/simpwf/workflow-engine/internal/workflow/model"
)

const draft2020 = "https://json-schema.org/draft/2020-12/schema"

var builtinTypes = []string{
	"script", "conditions", "input", "group", "external_call", "output", "poller",
}

// decodeSchema unmarshals a node schema into a generic document.
func decodeSchema(t *testing.T, raw json.RawMessage) map[string]any {
	t.Helper()
	var doc map[string]any
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("schema is not valid JSON: %v", err)
	}
	return doc
}

// compileSchema compiles raw as a draft 2020-12 schema, independent of the
// production compiler, so the test proves the served document is valid.
func compileSchema(t *testing.T, raw json.RawMessage) *jsonschema.Schema {
	t.Helper()
	var doc any
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("schema is not valid JSON: %v", err)
	}
	c := jsonschema.NewCompiler()
	c.DefaultDraft(jsonschema.Draft2020)
	if err := c.AddResource("node.json", doc); err != nil {
		t.Fatalf("AddResource() error = %v", err)
	}
	compiled, err := c.Compile("node.json")
	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}
	return compiled
}

func TestBuiltinNodeSchemasCompile(t *testing.T) {
	schemas := model.BuiltinNodeSchemas()
	if len(schemas) != len(builtinTypes) {
		t.Fatalf("BuiltinNodeSchemas() has %d entries, want %d", len(schemas), len(builtinTypes))
	}
	for _, nodeType := range builtinTypes {
		raw, ok := schemas[nodeType]
		if !ok {
			t.Errorf("BuiltinNodeSchemas() missing %q", nodeType)
			continue
		}
		compileSchema(t, raw)
	}
}

// TestBuiltinNodeSchemasAreCopies guards the cache: mutating the returned
// map must not corrupt the shared schemas.
func TestBuiltinNodeSchemasAreCopies(t *testing.T) {
	schemas := model.BuiltinNodeSchemas()
	delete(schemas, "script")
	if _, ok := model.BuiltinNodeSchemas()["script"]; !ok {
		t.Error("BuiltinNodeSchemas() returned the internal map, not a copy")
	}
}

func TestNodeSchemaBuiltinShape(t *testing.T) {
	for _, nodeType := range builtinTypes {
		raw, ok := model.NodeSchema(nodeType)
		if !ok {
			t.Errorf("NodeSchema(%q) not found", nodeType)
			continue
		}
		doc := decodeSchema(t, raw)
		if got := doc["$schema"]; got != draft2020 {
			t.Errorf("%s: $schema = %v, want %s", nodeType, got, draft2020)
		}
		if got := doc["type"]; got != "object" {
			t.Errorf("%s: type = %v, want object", nodeType, got)
		}
		props, _ := doc["properties"].(map[string]any)
		typeProp, _ := props["type"].(map[string]any)
		if got := typeProp["const"]; got != nodeType {
			t.Errorf("%s: properties.type.const = %v, want %q", nodeType, got, nodeType)
		}
	}
}

func TestNodeSchemaRequiredPerType(t *testing.T) {
	cases := map[string][]string{
		"script":        {"type", "script"},
		"conditions":    {"type", "conditions"},
		"input":         {"type", "channel"},
		"group":         {"type", "start_node_id", "nodes"},
		"external_call": {"type"},
		"output":        {"type", "channel", "context_path"},
		"poller":        {"type"},
	}
	for nodeType, want := range cases {
		raw, ok := model.NodeSchema(nodeType)
		if !ok {
			t.Errorf("NodeSchema(%q) not found", nodeType)
			continue
		}
		got := requiredOf(t, decodeSchema(t, raw))
		if len(got) != len(want) {
			t.Errorf("%s: required = %v, want %v", nodeType, got, want)
			continue
		}
		for i := range want {
			if got[i] != want[i] {
				t.Errorf("%s: required = %v, want %v", nodeType, got, want)
				break
			}
		}
	}
}

func TestNodeSchemaEnumsPerType(t *testing.T) {
	cases := []struct {
		nodeType string
		path     []string
		want     []string
	}{
		{"input", []string{"channel"}, []string{"http", "redis", "rabbitmq"}},
		{"output", []string{"channel"}, []string{"redis", "rabbitmq"}},
		{"poller", []string{"redis", "method"}, []string{"GET", "SUB"}},
	}
	for _, tc := range cases {
		raw, ok := model.NodeSchema(tc.nodeType)
		if !ok {
			t.Errorf("NodeSchema(%q) not found", tc.nodeType)
			continue
		}
		got := enumAt(t, decodeSchema(t, raw), tc.path)
		if len(got) != len(tc.want) {
			t.Errorf("%s %v: enum = %v, want %v", tc.nodeType, tc.path, got, tc.want)
			continue
		}
		for i := range tc.want {
			if got[i] != tc.want[i] {
				t.Errorf("%s %v: enum = %v, want %v", tc.nodeType, tc.path, got, tc.want)
				break
			}
		}
	}
}

func TestNodeSchemaConditionsConstraints(t *testing.T) {
	raw, _ := model.NodeSchema("conditions")
	doc := decodeSchema(t, raw)
	props := doc["properties"].(map[string]any)
	conditions := props["conditions"].(map[string]any)
	if got := conditions["minItems"]; got != float64(2) {
		t.Errorf("conditions.minItems = %v, want 2", got)
	}
	// The parser rejects both fields on a conditions node.
	for _, forbidden := range []string{"next_node", "output_property"} {
		if _, present := props[forbidden]; present {
			t.Errorf("conditions schema must not allow %q", forbidden)
		}
	}
}

func TestNodeSchemaOneOfPerType(t *testing.T) {
	cases := map[string][]string{
		"external_call": {"http_config", "execution_config"},
		"poller":        {"http", "redis", "rabbitmq"},
	}
	for nodeType, want := range cases {
		raw, _ := model.NodeSchema(nodeType)
		doc := decodeSchema(t, raw)
		oneOf, _ := doc["oneOf"].([]any)
		if len(oneOf) != len(want) {
			t.Errorf("%s: oneOf has %d arms, want %d", nodeType, len(oneOf), len(want))
			continue
		}
		for i, arm := range oneOf {
			got := requiredOf(t, arm.(map[string]any))
			if len(got) != 1 || got[0] != want[i] {
				t.Errorf("%s: oneOf[%d] requires %v, want [%s]", nodeType, i, got, want[i])
			}
		}
	}
}

// TestNodeSchemaOnFailureOnlyWhereParserAllows pins the field to the types
// parseRawNode accepts it on: external_call, poller, and custom.
func TestNodeSchemaOnFailureOnlyWhereParserAllows(t *testing.T) {
	allowed := map[string]bool{"external_call": true, "poller": true}
	for _, nodeType := range builtinTypes {
		raw, _ := model.NodeSchema(nodeType)
		props := decodeSchema(t, raw)["properties"].(map[string]any)
		_, present := props["on_failure"]
		if present != allowed[nodeType] {
			t.Errorf("%s: on_failure present = %v, want %v", nodeType, present, allowed[nodeType])
		}
	}
}

func TestNodeSchemaDurationsAndIDs(t *testing.T) {
	raw, _ := model.NodeSchema("script")
	doc := decodeSchema(t, raw)
	props := doc["properties"].(map[string]any)
	if got := props["timeout"]; got == nil {
		t.Error("script schema missing timeout")
	}
	defs, _ := doc["$defs"].(map[string]any)
	duration, _ := defs["duration"].(map[string]any)
	if got := duration["type"]; got != "string" {
		t.Errorf("duration def type = %v, want string", got)
	}
	nodeID, _ := defs["nodeId"].(map[string]any)
	if got := nodeID["format"]; got != "uuid" {
		t.Errorf("nodeId def format = %v, want uuid", got)
	}
}

// TestNodeSchemaGroupChildren covers the shallow recursion: the group
// schema references every builtin through $defs plus a generic custom arm.
func TestNodeSchemaGroupChildren(t *testing.T) {
	raw, _ := model.NodeSchema("group")
	doc := decodeSchema(t, raw)
	defs, _ := doc["$defs"].(map[string]any)
	child, _ := defs["childNode"].(map[string]any)
	arms, _ := child["anyOf"].([]any)
	if len(arms) != len(builtinTypes)+1 {
		t.Fatalf("childNode has %d arms, want %d", len(arms), len(builtinTypes)+1)
	}
	refs := map[string]bool{}
	for _, arm := range arms {
		ref, _ := arm.(map[string]any)["$ref"].(string)
		refs[ref] = true
	}
	for _, nodeType := range builtinTypes {
		want := "#/$defs/" + nodeType + "Node"
		if !refs[want] {
			t.Errorf("childNode missing arm %s", want)
		}
	}
	if !refs["#/$defs/customNode"] {
		t.Error("childNode missing the custom arm")
	}
	// Each arm must exist and compile through the group document alone.
	compileSchema(t, raw)
	for ref := range refs {
		name := ref[len("#/$defs/"):]
		if _, ok := defs[name]; !ok {
			t.Errorf("childNode ref %s has no $defs entry", ref)
		}
	}
}

// TestNodeSchemaAcceptsValidContent is the positive half of the docs-only
// contract: content the Go parser accepts also satisfies the schema.
func TestNodeSchemaAcceptsValidContent(t *testing.T) {
	cases := map[string]string{
		"script":        `{"type":"script","script":"return 1;","id":"aaaaaaaa-aaaa-7aaa-8aaa-aaaaaaaaaaaa","output_property":"total"}`,
		"conditions":    `{"type":"conditions","conditions":[{"condition":"return true;"},{"condition":"return false;"}]}`,
		"input":         `{"type":"input","channel":"http","output_property":"post"}`,
		"output":        `{"type":"output","channel":"redis","context_path":"post"}`,
		"external_call": `{"type":"external_call","http_config":{"url":"https://example.com","method":"GET"}}`,
		"poller":        `{"type":"poller","http":{"url":"https://example.com","until":"return response.status == 200;"}}`,
		"group":         `{"type":"group","start_node_id":"aaaaaaaa-aaaa-7aaa-8aaa-aaaaaaaaaaaa","nodes":[{"type":"script","script":"return 1;","id":"aaaaaaaa-aaaa-7aaa-8aaa-aaaaaaaaaaaa"}]}`,
	}
	for nodeType, content := range cases {
		raw, _ := model.NodeSchema(nodeType)
		compiled := compileSchema(t, raw)
		var instance any
		if err := json.Unmarshal([]byte(content), &instance); err != nil {
			t.Fatalf("%s: bad test content: %v", nodeType, err)
		}
		if err := compiled.Validate(instance); err != nil {
			t.Errorf("%s: schema rejected content the parser accepts: %v", nodeType, err)
		}
	}
}

// TestSchemasAreDocsOnlyNotGatekeeper is the negative half: content the Go
// parser rejects still parses against the schema without error, proving the
// schema documents the contract and does not enforce it.
func TestSchemasAreDocsOnlyNotGatekeeper(t *testing.T) {
	limits := testLimits
	// A script node with no script is rejected by the parser...
	invalid := `{"type":"script","id":"aaaaaaaa-aaaa-7aaa-8aaa-aaaaaaaaaaaa"}`
	if _, err := model.ParseNodeContent([]byte(invalid), limits); err == nil {
		t.Fatal("ParseNodeContent accepted a script node with no script; test premise broken")
	}
	// ...but the served schema also accepts it, because schemas are docs.
	raw, _ := model.NodeSchema("script")
	compiled := compileSchema(t, raw)
	var instance any
	if err := json.Unmarshal([]byte(invalid), &instance); err != nil {
		t.Fatal(err)
	}
	if err := compiled.Validate(instance); err == nil {
		t.Error("schema rejected the fixture; it no longer documents the parser gap")
	}
}

func TestNodeSchemaUnknownType(t *testing.T) {
	if raw, ok := model.NodeSchema("definitely_not_a_type"); ok {
		t.Errorf("NodeSchema(unknown) = %s, want not found", raw)
	}
}

func requiredOf(t *testing.T, doc map[string]any) []string {
	t.Helper()
	raw, _ := doc["required"].([]any)
	out := make([]string, 0, len(raw))
	for _, v := range raw {
		s, _ := v.(string)
		out = append(out, s)
	}
	return out
}

// enumAt walks a path of property names and returns the enum at its leaf.
func enumAt(t *testing.T, doc map[string]any, path []string) []string {
	t.Helper()
	node := doc
	for i, key := range path {
		props, ok := node["properties"].(map[string]any)
		if !ok {
			t.Fatalf("no properties at %v", path[:i])
		}
		child, ok := props[key].(map[string]any)
		if !ok {
			t.Fatalf("no property %q at %v", key, path[:i])
		}
		node = child
	}
	raw, _ := node["enum"].([]any)
	out := make([]string, 0, len(raw))
	for _, v := range raw {
		s, _ := v.(string)
		out = append(out, s)
	}
	return out
}
