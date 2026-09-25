package executor_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/simpwf/workflow-engine/internal/workflow/executor"
	"github.com/simpwf/workflow-engine/internal/workflow/model"
)

type stubExecutor struct {
	out *executor.Result
	err error
}

func (s *stubExecutor) Execute(context.Context, executor.Request) (*executor.Result, error) {
	return s.out, s.err
}

func stubFactory(ex executor.Executor, err error) executor.CustomFactory {
	return func(executor.CustomDeps) (executor.Executor, error) {
		return ex, err
	}
}

func TestRegisterCustomFactoryRoundTrip(t *testing.T) {
	const typ = "execreg1"
	stub := &stubExecutor{out: &executor.Result{Output: "ok"}}
	if err := executor.RegisterCustomFactory(typ, stubFactory(stub, nil)); err != nil {
		t.Fatalf("RegisterCustomFactory() error = %v", err)
	}
	f, ok := executor.LookupCustomFactory(typ)
	if !ok || f == nil {
		t.Fatalf("LookupCustomFactory(%q) missing", typ)
	}
	got, err := f(executor.CustomDeps{})
	if err != nil || got != stub {
		t.Fatalf("factory() = %v, %v; want stub", got, err)
	}
	found := false
	for _, c := range executor.CustomFactoryTypes() {
		if c == typ {
			found = true
		}
	}
	if !found {
		t.Fatalf("CustomFactoryTypes() missing %q", typ)
	}
}

func TestRegisterCustomFactoryDuplicate(t *testing.T) {
	const typ = "execreg2"
	if err := executor.RegisterCustomFactory(typ, stubFactory(&stubExecutor{}, nil)); err != nil {
		t.Fatalf("first RegisterCustomFactory() error = %v", err)
	}
	if err := executor.RegisterCustomFactory(typ, stubFactory(&stubExecutor{}, nil)); err == nil {
		t.Fatal("second RegisterCustomFactory() = nil, want error")
	}
}

func TestRegisterCustomFactoryRejectsBuiltinAndEmpty(t *testing.T) {
	for _, typ := range []string{"", "script", "conditions", "input", "group", "external_call", "output", "poller"} {
		if err := executor.RegisterCustomFactory(typ, stubFactory(&stubExecutor{}, nil)); err == nil {
			t.Errorf("RegisterCustomFactory(%q) = nil, want error", typ)
		}
	}
	if err := executor.RegisterCustomFactory("execreg3", nil); err == nil {
		t.Error("RegisterCustomFactory(nil factory) = nil, want error")
	}
}

func TestBuildCustomExecutorsMergesRegistered(t *testing.T) {
	const typ = "execreg4"
	deps := executor.CustomDeps{Limits: executor.Limits{MaxOutputBytes: 7}}
	var gotDeps executor.CustomDeps
	if err := executor.RegisterCustomFactory(typ, func(d executor.CustomDeps) (executor.Executor, error) {
		gotDeps = d
		return &stubExecutor{}, nil
	}); err != nil {
		t.Fatalf("RegisterCustomFactory() error = %v", err)
	}
	t.Cleanup(func() { executor.UnregisterCustomFactory(typ) })
	m, err := executor.BuildCustomExecutorsExcept(deps, nil)
	if err != nil {
		t.Fatalf("BuildCustomExecutorsExcept() error = %v", err)
	}
	if _, ok := m[model.NodeType(typ)]; !ok {
		t.Fatalf("BuildCustomExecutorsExcept() missing %q", typ)
	}
	if gotDeps.Limits.MaxOutputBytes != 7 {
		t.Fatalf("factory deps = %+v, want limits passthrough", gotDeps)
	}
}

func TestBuildCustomExecutorsFactoryError(t *testing.T) {
	const typ = "execreg5"
	boom := errors.New("boom")
	if err := executor.RegisterCustomFactory(typ, stubFactory(nil, boom)); err != nil {
		t.Fatalf("RegisterCustomFactory() error = %v", err)
	}
	t.Cleanup(func() { executor.UnregisterCustomFactory(typ) })
	_, err := executor.BuildCustomExecutorsExcept(executor.CustomDeps{}, nil)
	if err == nil || !strings.Contains(err.Error(), typ) {
		t.Fatalf("BuildCustomExecutorsExcept() error = %v, want error naming %q", err, typ)
	}
}

func TestNewExecutorsMergesCustom(t *testing.T) {
	const typ = "execreg6"
	stub := &stubExecutor{out: &executor.Result{Output: "ok"}}
	if err := executor.RegisterCustomFactory(typ, stubFactory(stub, nil)); err != nil {
		t.Fatalf("RegisterCustomFactory() error = %v", err)
	}
	t.Cleanup(func() { executor.UnregisterCustomFactory(typ) })
	// Invert the exclude helper: build everything except nothing, then
	// assert our type is present. (Exclude is for skipping; here we want
	// the production merge path to include it.)
	m := executor.NewExecutors(executor.Limits{}, nil, executor.Dependencies{})
	if _, ok := m[model.NodeType(typ)]; !ok {
		t.Fatalf("NewExecutors() missing %q", typ)
	}
	if len(m) < 7 {
		t.Fatalf("NewExecutors() has %d entries, want >= 7 (6 builtin + custom)", len(m))
	}
}

func TestNewExecutorsPanicsOnFactoryError(t *testing.T) {
	const typ = "execreg7"
	boom := errors.New("boom")
	if err := executor.RegisterCustomFactory(typ, stubFactory(nil, boom)); err != nil {
		t.Fatalf("RegisterCustomFactory() error = %v", err)
	}
	defer func() {
		executor.UnregisterCustomFactory(typ)
		if r := recover(); r == nil {
			t.Fatal("NewExecutors() with failing factory did not panic")
		}
	}()
	_ = executor.NewExecutors(executor.Limits{}, nil, executor.Dependencies{})
}
