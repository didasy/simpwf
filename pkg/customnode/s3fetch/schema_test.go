package s3fetch_test

import (
	"encoding/json"
	"testing"

	"github.com/santhosh-tekuri/jsonschema/v6"
	"github.com/simpwf/workflow-engine/internal/workflow/model"
)

// TestConfigSchemaIsRegistered proves the init registration carried the
// schema through the facade into the served node envelope.
func TestConfigSchemaIsRegistered(t *testing.T) {
	raw, ok := model.NodeSchema("s3fetch")
	if !ok {
		t.Fatal("model.NodeSchema(s3fetch) not found; the leaf did not register a schema")
	}
	var doc map[string]any
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("served schema is not valid JSON: %v", err)
	}
	props, _ := doc["properties"].(map[string]any)
	typeProp, _ := props["type"].(map[string]any)
	if typeProp["const"] != "s3fetch" {
		t.Errorf("envelope properties.type.const = %v, want s3fetch", typeProp["const"])
	}
	config, _ := props["config"].(map[string]any)
	if config["type"] != "object" {
		t.Fatalf("envelope properties.config.type = %v, want object", config["type"])
	}
	want := []string{"operation", "endpoint", "bucket", "key", "access_key", "secret_key"}
	assertSameStrings(t, "config.required", stringOfSlice(t, config["required"]), want)
}

// TestConfigSchemaOperationEnum pins the operation enum.
func TestConfigSchemaOperationEnum(t *testing.T) {
	config := configOf(t)
	props, _ := config["properties"].(map[string]any)
	operation, _ := props["operation"].(map[string]any)
	assertSameStrings(t, "config.operation.enum", stringOfSlice(t, operation["enum"]), []string{"pipe", "presign"})
}

// TestConfigSchemaExpiryRange pins the 1..604800 bound on expiry_seconds.
func TestConfigSchemaExpiryRange(t *testing.T) {
	config := configOf(t)
	props, _ := config["properties"].(map[string]any)
	expiry, _ := props["expiry_seconds"].(map[string]any)
	if expiry["type"] != "integer" {
		t.Errorf("expiry_seconds.type = %v, want integer", expiry["type"])
	}
	if expiry["minimum"] != float64(1) {
		t.Errorf("expiry_seconds.minimum = %v, want 1", expiry["minimum"])
	}
	if expiry["maximum"] != float64(604800) {
		t.Errorf("expiry_seconds.maximum = %v, want 604800", expiry["maximum"])
	}
}

// TestConfigSchemaIfThenOnSourceURL covers the pipe/presign rule: pipe
// requires source_url, presign forbids it.
func TestConfigSchemaIfThenOnSourceURL(t *testing.T) {
	config := configOf(t)
	branches, _ := config["allOf"].([]any)
	if len(branches) != 2 {
		t.Fatalf("config.allOf has %d branches, want 2", len(branches))
	}
	// pipe: then requires source_url.
	pipe := thenOf(t, branches, "pipe")
	assertSameStrings(t, "pipe.then.required", stringOfSlice(t, pipe["required"]), []string{"source_url"})
	// presign: then forbids source_url.
	presign := thenOf(t, branches, "presign")
	not, _ := presign["not"].(map[string]any)
	assertSameStrings(t, "presign.then.not.required", stringOfSlice(t, not["required"]), []string{"source_url"})
}

// TestConfigSchemaAcceptsShippedExamples validates both seed definitions
// wrapped in their node envelope.
func TestConfigSchemaAcceptsShippedExamples(t *testing.T) {
	pipe := wrapNode("s3fetch", json.RawMessage(`{
      "operation": "pipe",
      "endpoint": "{{ env.SIMPWF_S3_ENDPOINT }}",
      "region": "us-east-1",
      "bucket": "reports",
      "key": "seed/pipe-probe.pdf",
      "source_url": "https://example.com/report.pdf",
      "expiry_seconds": 3600,
      "use_ssl": false,
      "access_key": "{{ env.SIMPWF_S3_ACCESS_KEY }}",
      "secret_key": "{{ env.SIMPWF_S3_SECRET_KEY }}"
    }`), json.RawMessage(`{"output_property":"report","timeout":"120s"}`))
	validate(t, pipe)

	presign := wrapNode("s3fetch", json.RawMessage(`{
      "operation": "presign",
      "endpoint": "{{ env.SIMPWF_S3_ENDPOINT }}",
      "region": "us-east-1",
      "bucket": "reports",
      "key": "seed/pipe-probe.pdf",
      "expiry_seconds": 3600,
      "use_ssl": false,
      "access_key": "{{ env.SIMPWF_S3_ACCESS_KEY }}",
      "secret_key": "{{ env.SIMPWF_S3_SECRET_KEY }}"
    }`), json.RawMessage(`{"output_property":"report","timeout":"30s"}`))
	validate(t, presign)
}

func TestConfigSchemaRejectsPipeWithoutSourceURL(t *testing.T) {
	validateFails(t, wrapNode("s3fetch", json.RawMessage(`{
      "operation": "pipe", "endpoint": "e:9000", "bucket": "b", "key": "k",
      "access_key": "a", "secret_key": "s"
    }`), nil))
}

func TestConfigSchemaRejectsPresignWithSourceURL(t *testing.T) {
	validateFails(t, wrapNode("s3fetch", json.RawMessage(`{
      "operation": "presign", "endpoint": "e:9000", "bucket": "b", "key": "k",
      "source_url": "https://example.com/x", "access_key": "a", "secret_key": "s"
    }`), nil))
}

func TestConfigSchemaRejectsExpiryOutOfRange(t *testing.T) {
	for _, expiry := range []string{"0", "604801"} {
		validateFails(t, wrapNode("s3fetch", json.RawMessage(`{
          "operation": "presign", "endpoint": "e:9000", "bucket": "b", "key": "k",
          "expiry_seconds": `+expiry+`, "access_key": "a", "secret_key": "s"
        }`), nil))
	}
}

func TestConfigSchemaRejectsUnknownOperation(t *testing.T) {
	validateFails(t, wrapNode("s3fetch", json.RawMessage(`{
      "operation": "download", "endpoint": "e:9000", "bucket": "b", "key": "k",
      "access_key": "a", "secret_key": "s"
    }`), nil))
}

func configOf(t *testing.T) map[string]any {
	t.Helper()
	raw, ok := model.NodeSchema("s3fetch")
	if !ok {
		t.Fatal("model.NodeSchema(s3fetch) not found")
	}
	var doc map[string]any
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatal(err)
	}
	props, _ := doc["properties"].(map[string]any)
	config, _ := props["config"].(map[string]any)
	return config
}

// thenOf returns the then-branch guarded by if.properties.type.const.
func thenOf(t *testing.T, branches []any, operation string) map[string]any {
	t.Helper()
	for _, raw := range branches {
		branch, _ := raw.(map[string]any)
		ifProp, _ := branch["if"].(map[string]any)["properties"].(map[string]any)
		typeProp, _ := ifProp["operation"].(map[string]any)
		if typeProp["const"] != operation {
			continue
		}
		then, _ := branch["then"].(map[string]any)
		return then
	}
	t.Fatalf("config.allOf has no if/then branch for operation %q", operation)
	return nil
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
		t.Errorf("served schema rejected a valid s3fetch node: %v", err)
	}
}

func validateFails(t *testing.T, instance json.RawMessage) {
	t.Helper()
	if err := compileServed(t).Validate(anyOf(t, instance)); err == nil {
		t.Error("served schema accepted an invalid s3fetch config")
	}
}

func compileServed(t *testing.T) *jsonschema.Schema {
	t.Helper()
	raw, ok := model.NodeSchema("s3fetch")
	if !ok {
		t.Fatal("model.NodeSchema(s3fetch) not found")
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
