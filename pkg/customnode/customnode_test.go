package customnode_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/simpwf/workflow-engine/internal/workflow/executor"
	"github.com/simpwf/workflow-engine/internal/workflow/model"
	"github.com/simpwf/workflow-engine/pkg/customnode"
)

type stubExecutor struct{}

func (stubExecutor) Execute(context.Context, executor.Request) (*executor.Result, error) {
	return &executor.Result{Output: "ok"}, nil
}

func validateOK(json.RawMessage) (any, error) { return map[string]any{}, nil }

// okConfigSchema is a minimal valid config sub-schema for registration
// tests; the customnode facade does not interpret its contents.
var okConfigSchema = json.RawMessage(`{"type":"object"}`)

func newOK(customnode.Deps) (executor.Executor, error) { return stubExecutor{}, nil }

var errBoom = errors.New("boom")

func TestRegisterRoundTrip(t *testing.T) {
	const typ = "cnreg1"
	customnode.MustRegister(customnode.Definition{Type: typ, Validate: validateOK, Schema: okConfigSchema, New: newOK})
	d, ok := customnode.ByType(typ)
	if !ok || d.Type != typ {
		t.Fatalf("ByType(%q) = %+v, %v; want definition", typ, d, ok)
	}
	if !model.ValidNodeType(typ) {
		t.Fatalf("ValidNodeType(%q) = false after register", typ)
	}
	if _, ok := model.LookupCustomType(typ); !ok {
		t.Fatalf("model.LookupCustomType(%q) missing", typ)
	}
	if _, ok := executor.LookupCustomFactory(typ); !ok {
		t.Fatalf("executor.LookupCustomFactory(%q) missing", typ)
	}
	found := false
	for _, c := range customnode.Types() {
		if c == typ {
			found = true
		}
	}
	if !found {
		t.Fatalf("Types() missing %q", typ)
	}
}

func TestRegisterDuplicate(t *testing.T) {
	const typ = "cnreg2"
	if err := customnode.Register(customnode.Definition{Type: typ, Validate: validateOK, Schema: okConfigSchema, New: newOK}); err != nil {
		t.Fatalf("first Register() error = %v", err)
	}
	if err := customnode.Register(customnode.Definition{Type: typ, Validate: validateOK, Schema: okConfigSchema, New: newOK}); err == nil {
		t.Fatal("second Register() = nil, want error")
	}
}

func TestRegisterRejectsBuiltinAndBadNames(t *testing.T) {
	for _, typ := range []string{"script", "poller", "BadName", "has space", "9x", ""} {
		if err := customnode.Register(customnode.Definition{Type: typ, Validate: validateOK, Schema: okConfigSchema, New: newOK}); err == nil {
			t.Errorf("Register(%q) = nil, want error", typ)
		}
	}
}

func TestRegisterRejectsNilHooks(t *testing.T) {
	if err := customnode.Register(customnode.Definition{Type: "cnreg3", New: newOK}); err == nil {
		t.Error("Register(nil Validate) = nil, want error")
	}
	if err := customnode.Register(customnode.Definition{Type: "cnreg4", Validate: validateOK}); err == nil {
		t.Error("Register(nil New) = nil, want error")
	}
}

func TestMustRegisterPanicsOnDuplicate(t *testing.T) {
	const typ = "cnreg5"
	customnode.MustRegister(customnode.Definition{Type: typ, Validate: validateOK, Schema: okConfigSchema, New: newOK})
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("MustRegister(duplicate) did not panic")
		}
	}()
	customnode.MustRegister(customnode.Definition{Type: typ, Validate: validateOK, Schema: okConfigSchema, New: newOK})
}

func TestBuildExecutors(t *testing.T) {
	const typ = "cnreg6"
	customnode.MustRegister(customnode.Definition{Type: typ, Validate: validateOK, Schema: okConfigSchema, New: newOK})
	m, err := customnode.BuildExecutors(customnode.Deps{})
	if err != nil {
		t.Fatalf("BuildExecutors() error = %v", err)
	}
	if _, ok := m[model.NodeType(typ)]; !ok {
		t.Fatalf("BuildExecutors() missing %q", typ)
	}
}

func TestBuildExecutorsFactoryError(t *testing.T) {
	const typ = "cnreg7"
	customnode.MustRegister(customnode.Definition{
		Type:     typ,
		Validate: validateOK, Schema: okConfigSchema,
		New: func(customnode.Deps) (executor.Executor, error) {
			return nil, errBoom
		},
	})
	_, err := customnode.BuildExecutors(customnode.Deps{})
	if err == nil || !strings.Contains(err.Error(), typ) {
		t.Fatalf("BuildExecutors() error = %v, want error naming %q", err, typ)
	}
}

func TestRegisterRollsBackModelOnExecutorConflict(t *testing.T) {
	const typ = "cnreg8"
	if err := executor.RegisterCustomFactory(typ, func(executor.CustomDeps) (executor.Executor, error) {
		return stubExecutor{}, nil
	}); err != nil {
		t.Fatalf("seed executor.RegisterCustomFactory() error = %v", err)
	}
	if err := customnode.Register(customnode.Definition{Type: typ, Validate: validateOK, Schema: okConfigSchema, New: newOK}); err == nil {
		t.Fatal("Register() with executor conflict = nil, want error")
	}
	if model.ValidNodeType(typ) {
		t.Fatalf("ValidNodeType(%q) = true after rolled-back register", typ)
	}
	if _, ok := model.LookupCustomType(typ); ok {
		t.Fatalf("model.LookupCustomType(%q) present after rollback", typ)
	}
	if _, ok := model.LookupCustomSchema(typ); ok {
		t.Fatalf("model.LookupCustomSchema(%q) present after rollback", typ)
	}
}

// TestRegisterRollsBackOnBadSchema covers the second rollback path: an
// unusable schema must not leave a validated-but-schemaless type behind.
func TestRegisterRollsBackOnBadSchema(t *testing.T) {
	const typ = "cnreg9"
	err := customnode.Register(customnode.Definition{
		Type: typ, Validate: validateOK, Schema: json.RawMessage(`[]`), New: newOK,
	})
	if err == nil {
		t.Fatal("Register(non-object Schema) = nil, want error")
	}
	if model.ValidNodeType(typ) {
		t.Errorf("ValidNodeType(%q) = true after schema rollback", typ)
	}
	if _, ok := model.LookupCustomType(typ); ok {
		t.Errorf("model.LookupCustomType(%q) present after schema rollback", typ)
	}
	// The name is still free, so a corrected registration succeeds.
	if err := customnode.Register(customnode.Definition{Type: typ, Validate: validateOK, Schema: okConfigSchema, New: newOK}); err != nil {
		t.Errorf("re-Register after rollback error = %v", err)
	}
}

func TestRegisterRejectsBadSchemas(t *testing.T) {
	cases := map[string]json.RawMessage{
		"nil":     nil,
		"array":   json.RawMessage(`[]`),
		"string":  json.RawMessage(`"config"`),
		"notJSON": json.RawMessage(`{oops`),
		"badRef":  json.RawMessage(`{"type":"object","properties":{"a":{"$ref":"#/$defs/nope"}}}`),
	}
	for name, schema := range cases {
		typ := "cnschema_" + name
		if err := customnode.Register(customnode.Definition{Type: typ, Validate: validateOK, Schema: schema, New: newOK}); err == nil {
			t.Errorf("%s: Register() = nil, want error", name)
		}
	}
}

func TestMustRegisterPanicsOnMissingSchema(t *testing.T) {
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("MustRegister(no Schema) did not panic")
		}
		if !strings.Contains(fmt.Sprint(r), "Schema") {
			t.Errorf("panic %v does not mention Schema", r)
		}
	}()
	customnode.MustRegister(customnode.Definition{Type: "cnreg10", Validate: validateOK, New: newOK})
}

// TestByTypeCarriesSchema pins that the round-trip view keeps the author
// sub-schema, not the wrapped envelope.
func TestByTypeCarriesSchema(t *testing.T) {
	const typ = "cnreg11"
	const author = `{"type":"object","properties":{"mode":{"type":"string"}},"required":["mode"]}`
	if err := customnode.Register(customnode.Definition{
		Type: typ, Validate: validateOK, Schema: json.RawMessage(author), New: newOK,
	}); err != nil {
		t.Fatal(err)
	}
	d, ok := customnode.ByType(typ)
	if !ok {
		t.Fatalf("ByType(%q) not found", typ)
	}
	if got := string(d.Schema); got != author {
		t.Errorf("ByType().Schema = %s, want %s", got, author)
	}
	// The served node schema is the wrapped envelope, keyed by the type.
	if _, ok := model.NodeSchema(typ); !ok {
		t.Errorf("model.NodeSchema(%q) not found after Register", typ)
	}
}
