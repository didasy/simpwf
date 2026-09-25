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
