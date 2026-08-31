package contextpath_test

import (
	"encoding/json"
	"errors"
	"reflect"
	"testing"

	"github.com/simpwf/workflow-engine/pkg/contextpath"
)

func TestParse(t *testing.T) {
	cases := []struct {
		path string
		want contextpath.Path
	}{
		{"data1", []contextpath.Segment{{Key: "data1"}}},
		{"user.name", []contextpath.Segment{{Key: "user"}, {Key: "name"}}},
		{"items[0]", []contextpath.Segment{{Key: "items"}, {Key: "0", Index: intPtr(0)}}},
		{"items[0].name", []contextpath.Segment{{Key: "items"}, {Key: "0", Index: intPtr(0)}, {Key: "name"}}},
		{"a[2].b[1]", []contextpath.Segment{{Key: "a"}, {Key: "2", Index: intPtr(2)}, {Key: "b"}, {Key: "1", Index: intPtr(1)}}},
	}
	for _, tc := range cases {
		got, err := contextpath.Parse(tc.path)
		if err != nil {
			t.Errorf("Parse(%q) error = %v", tc.path, err)
			continue
		}
		if !reflect.DeepEqual(got, tc.want) {
			t.Errorf("Parse(%q) = %+v, want %+v", tc.path, got, tc.want)
		}
	}
}

func TestParseInvalid(t *testing.T) {
	bad := []string{"", ".", "a..b", "a.", ".a", "items[]", "items[x]", "items[1a]", "items[1]]", "[0]"}
	for _, p := range bad {
		if _, err := contextpath.Parse(p); err == nil {
			t.Errorf("Parse(%q) error = nil, want error", p)
		}
	}
}

func TestGet(t *testing.T) {
	ctx := map[string]any{
		"user":  map[string]any{"name": "Jono"},
		"items": []any{"a", "b"},
		"n":     3.14,
	}
	cases := []struct {
		path string
		want any
	}{
		{"user.name", "Jono"},
		{"items[1]", "b"},
		{"n", 3.14},
		{"user", map[string]any{"name": "Jono"}},
	}
	for _, tc := range cases {
		got, err := contextpath.Get(ctx, tc.path)
		if err != nil {
			t.Errorf("Get(%q) error = %v", tc.path, err)
			continue
		}
		if !reflect.DeepEqual(got, tc.want) {
			t.Errorf("Get(%q) = %v, want %v", tc.path, got, tc.want)
		}
	}
}

func TestGetMissing(t *testing.T) {
	ctx := map[string]any{"user": map[string]any{"name": "Jono"}}
	for _, p := range []string{"user.age", "missing", "user.name[0]", "items[0]"} {
		if _, err := contextpath.Get(ctx, p); !errors.Is(err, contextpath.ErrPathNotFound) {
			t.Errorf("Get(%q) error = %v, want ErrPathNotFound", p, err)
		}
	}
}

func TestSetCreatesAndOverwrites(t *testing.T) {
	ctx := map[string]any{}
	if err := contextpath.Set(ctx, "user.name", "Jono"); err != nil {
		t.Fatalf("Set() error = %v", err)
	}
	if err := contextpath.Set(ctx, "items[0]", 42); err != nil {
		t.Fatalf("Set(items[0]) error = %v", err)
	}
	if ctx["user"].(map[string]any)["name"] != "Jono" {
		t.Errorf("user.name = %v", ctx["user"])
	}
	if ctx["items"].([]any)[0] != 42 {
		t.Errorf("items = %v", ctx["items"])
	}
	// overwrite existing
	if err := contextpath.Set(ctx, "user.name", "Anna"); err != nil {
		t.Fatal(err)
	}
	if ctx["user"].(map[string]any)["name"] != "Anna" {
		t.Errorf("user.name after overwrite = %v", ctx["user"])
	}
}

func TestSetIndexOutOfRange(t *testing.T) {
	ctx := map[string]any{"items": []any{"a"}}
	if err := contextpath.Set(ctx, "items[2]", "x"); err == nil {
		t.Error("Set(items[2]) on length-1 array error = nil, want error")
	}
	// index on an existing map value
	ctx2 := map[string]any{"user": map[string]any{"name": "x"}}
	if err := contextpath.Set(ctx2, "user[0]", "x"); err == nil {
		t.Error("Set(user[0]) on map error = nil, want error")
	}
}

func TestRenderTemplateTyped(t *testing.T) {
	ctx := map[string]any{
		"user": map[string]any{"name": "Jono"},
		"n":    100,
	}
	got, err := contextpath.RenderTemplate("{{ user.name }}", ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got != "Jono" {
		t.Errorf("typed template = %v (%T), want string Jono", got, got)
	}

	got, err = contextpath.RenderTemplate("{{ n }}", ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got != 100 {
		t.Errorf("typed numeric template = %v (%T), want 100", got, got)
	}
}

func TestRenderTemplateInterpolation(t *testing.T) {
	ctx := map[string]any{"user": map[string]any{"name": "Jono"}, "n": 100}
	got, err := contextpath.RenderTemplate("hello {{ user.name }}, {{ n }}!", ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got != "hello Jono, 100!" {
		t.Errorf("interpolated = %q", got)
	}
}

func TestRenderTemplateNoPlaceholder(t *testing.T) {
	got, err := contextpath.RenderTemplate("plain text", nil)
	if err != nil {
		t.Fatal(err)
	}
	if got != "plain text" {
		t.Errorf("got %q", got)
	}
}

func TestRenderTemplateMissingPath(t *testing.T) {
	if _, err := contextpath.RenderTemplate("{{ missing.key }}", map[string]any{}); !errors.Is(err, contextpath.ErrPathNotFound) {
		t.Errorf("error = %v, want ErrPathNotFound", err)
	}
}

func TestRenderJSON(t *testing.T) {
	ctx := map[string]any{
		"user": map[string]any{"name": "Jono", "age": 30},
		"ok":   true,
	}
	raw := []byte(`{"name":"{{ user.name }}","age":"{{ user.age }}","flag":"{{ ok }}","plain":"x {{ user.name }}","nested":{"n":"{{ user.age }}"}}`)
	got, err := contextpath.RenderJSON(raw, ctx)
	if err != nil {
		t.Fatalf("RenderJSON() error = %v", err)
	}
	var m map[string]any
	if err := json.Unmarshal(got, &m); err != nil {
		t.Fatalf("unmarshal %s: %v", got, err)
	}
	if m["name"] != "Jono" {
		t.Errorf("name = %v", m["name"])
	}
	if v, ok := m["age"].(float64); !ok || v != 30 {
		t.Errorf("age = %v", m["age"])
	}
	if m["flag"] != true {
		t.Errorf("flag = %v", m["flag"])
	}
	if m["plain"] != "x Jono" {
		t.Errorf("plain = %v", m["plain"])
	}
	nested, ok := m["nested"].(map[string]any)
	if !ok {
		t.Fatal("nested missing")
	}
	if v, ok := nested["n"].(float64); !ok || v != 30 {
		t.Errorf("nested.n = %v", nested["n"])
	}
}

func TestRenderJSONNoPlaceholders(t *testing.T) {
	got, err := contextpath.RenderJSON([]byte(`{"a":1}`), map[string]any{})
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != `{"a":1}` {
		t.Errorf("got %s", got)
	}
}

func TestRenderJSONPreservesLargeInteger(t *testing.T) {
	got, err := contextpath.RenderJSON([]byte(`{"id":9007199254740993}`), map[string]any{})
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != `{"id":9007199254740993}` {
		t.Errorf("got %s", got)
	}
}

func TestRenderJSONRejectsTrailingJSON(t *testing.T) {
	if _, err := contextpath.RenderJSON([]byte(`{"id":1} {"id":2}`), map[string]any{}); err == nil {
		t.Fatal("RenderJSON() error = nil, want error")
	}
}

func TestHasTemplate(t *testing.T) {
	cases := []struct {
		in   string
		want bool
	}{
		{"{{ user.name }}", true},
		{"hello {{ user.name }}!", true},
		{"https://api.example.com/{{ path }}", true},
		{"plain text", false},
		{"", false},
		{"{{ }}", true}, // matches the placeholder regex; the empty path fails at render time
		{"{{}}", false},
	}
	for _, c := range cases {
		if got := contextpath.HasTemplate(c.in); got != c.want {
			t.Errorf("HasTemplate(%q) = %v, want %v", c.in, got, c.want)
		}
	}
}

func intPtr(i int) *int { return &i }
