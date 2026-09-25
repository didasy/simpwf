package customnode_test

import (
	"context"
	"encoding/json"
	"errors"
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

func newOK(customnode.Deps) (executor.Executor, error) { return stubExecutor{}, nil }

var errBoom = errors.New("boom")

func TestRegisterRoundTrip(t *testing.T) {
	const typ = "cnreg1"
	customnode.MustRegister(customnode.Definition{Type: typ, Validate: validateOK, New: newOK})
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
	if err := customnode.Register(customnode.Definition{Type: typ, Validate: validateOK, New: newOK}); err != nil {
		t.Fatalf("first Register() error = %v", err)
	}
	if err := customnode.Register(customnode.Definition{Type: typ, Validate: validateOK, New: newOK}); err == nil {
		t.Fatal("second Register() = nil, want error")
	}
}

func TestRegisterRejectsBuiltinAndBadNames(t *testing.T) {
	for _, typ := range []string{"script", "poller", "BadName", "has space", "9x", ""} {
		if err := customnode.Register(customnode.Definition{Type: typ, Validate: validateOK, New: newOK}); err == nil {
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
	customnode.MustRegister(customnode.Definition{Type: typ, Validate: validateOK, New: newOK})
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("MustRegister(duplicate) did not panic")
		}
	}()
	customnode.MustRegister(customnode.Definition{Type: typ, Validate: validateOK, New: newOK})
}

func TestBuildExecutors(t *testing.T) {
	const typ = "cnreg6"
	customnode.MustRegister(customnode.Definition{Type: typ, Validate: validateOK, New: newOK})
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
		Validate: validateOK,
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
	if err := customnode.Register(customnode.Definition{Type: typ, Validate: validateOK, New: newOK}); err == nil {
		t.Fatal("Register() with executor conflict = nil, want error")
	}
	if model.ValidNodeType(typ) {
		t.Fatalf("ValidNodeType(%q) = true after rolled-back register", typ)
	}
	if _, ok := model.LookupCustomType(typ); ok {
		t.Fatalf("model.LookupCustomType(%q) present after rollback", typ)
	}
}
