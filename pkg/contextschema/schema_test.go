package contextschema_test

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/simpwf/workflow-engine/pkg/contextschema"
)

func decode(t *testing.T, raw string) any {
	t.Helper()
	v, err := contextschema.DecodeSnapshot(json.RawMessage(raw))
	if err != nil {
		t.Fatalf("DecodeSnapshot(%s) error = %v", raw, err)
	}
	return v
}

func TestInferPrimitives(t *testing.T) {
	cases := []struct {
		raw  string
		want string
	}{
		{`{"s":"hi"}`, "{ s: string; }"},
		{`{"n":3}`, "{ n: number; }"},
		{`{"n":3.5}`, "{ n: number; }"},
		{`{"b":true}`, "{ b: boolean; }"},
		{`{"z":null}`, "{ z: null; }"},
	}
	for _, tc := range cases {
		got := contextschema.RenderTypeScript(decode(t, tc.raw))
		want := "declare const context: " + tc.want + ";"
		if got != want {
			t.Errorf("RenderTypeScript(%s) = %q, want %q", tc.raw, got, want)
		}
	}
}

func TestInferNestedObjectSortedKeys(t *testing.T) {
	v := decode(t, `{"order":{"total":10,"id":"x"},"a":1}`)
	got := contextschema.RenderTypeScript(v)
	want := "declare const context: { a: number; order: { id: string; total: number; }; };"
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
	schema := contextschema.InferJSONSchema(v)
	props, ok := schema["properties"].(map[string]any)
	if !ok {
		t.Fatalf("schema properties = %#v", schema["properties"])
	}
	order, ok := props["order"].(map[string]any)
	if !ok {
		t.Fatalf("order schema = %#v", props["order"])
	}
	orderProps, ok := order["properties"].(map[string]any)
	if !ok || orderProps["total"] == nil || orderProps["id"] == nil {
		t.Errorf("order properties = %#v", order["properties"])
	}
	if req, ok := schema["required"].([]string); !ok || len(req) != 2 || req[0] != "a" || req[1] != "order" {
		t.Errorf("required = %#v, want [a order]", schema["required"])
	}
}

func TestInferMixedArray(t *testing.T) {
	v := decode(t, `{"vals":[1,"two",true]}`)
	got := contextschema.RenderTypeScript(v)
	want := "declare const context: { vals: (boolean | number | string)[]; };"
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestInferEmptyArrayAndObject(t *testing.T) {
	v := decode(t, `{"arr":[],"obj":{}}`)
	got := contextschema.RenderTypeScript(v)
	want := "declare const context: { arr: unknown[]; obj: Record<string, unknown>; };"
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestInferInvalidTSKeyQuoted(t *testing.T) {
	v := decode(t, `{"foo-bar":1,"with space":2,"ok":3}`)
	got := contextschema.RenderTypeScript(v)
	if !strings.Contains(got, `"foo-bar": number;`) {
		t.Errorf("missing quoted foo-bar in %q", got)
	}
	if !strings.Contains(got, `"with space": number;`) {
		t.Errorf("missing quoted with-space in %q", got)
	}
	if !strings.Contains(got, `ok: number;`) {
		t.Errorf("missing bare ok in %q", got)
	}
}

func TestInferTopLevelNonObject(t *testing.T) {
	for _, raw := range []string{`[1,2]`, `"hi"`, `3`, `null`, `true`} {
		v := decode(t, raw)
		if got := contextschema.RenderTypeScript(v); got != "declare const context: unknown;" {
			t.Errorf("RenderTypeScript(%s) = %q, want unknown", raw, got)
		}
		if schema := contextschema.InferJSONSchema(v); len(schema) != 0 {
			t.Errorf("InferJSONSchema(%s) = %#v, want {}", raw, schema)
		}
	}
}

func TestDecodeSnapshotInvalid(t *testing.T) {
	if _, err := contextschema.DecodeSnapshot(json.RawMessage(`not json`)); err == nil {
		t.Error("DecodeSnapshot(bad) error = nil, want error")
	}
}

func TestRenderNeverEmitsLiteralsOrAny(t *testing.T) {
	v := decode(t, `{"a":"lit","n":42,"arr":[],"nested":{"x":null}}`)
	got := contextschema.RenderTypeScript(v)
	for _, banned := range []string{`"lit"`, `42`, `: any`, `any[]`} {
		if strings.Contains(got, banned) {
			t.Errorf("output contains %q: %q", banned, got)
		}
	}
}

func TestRenderCapsDepthAndSize(t *testing.T) {
	deep := `{"l1":{"l2":{"l3":{"l4":{"l5":{"l6":{"l7":{"l8":{"l9":{"l10":"x"}}}}}}}}}}`
	got := contextschema.RenderTypeScript(decode(t, deep))
	if strings.Contains(got, `"x"`) || strings.Contains(got, ": any") {
		t.Errorf("deep output leaks value or any: %q", got)
	}
	if !strings.Contains(got, "unknown") {
		t.Errorf("deep output should collapse to unknown: %q", got)
	}
	if len(got) > 32*1024 {
		t.Errorf("output length %d exceeds 32KB", len(got))
	}
}
