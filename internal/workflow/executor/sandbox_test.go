package executor_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/simpwf/workflow-engine/internal/workflow/executor"
	"github.com/simpwf/workflow-engine/pkg/jsfunc"
)

func run(t *testing.T, opts executor.ScriptOptions) (*executor.ScriptResult, error) {
	t.Helper()
	if opts.Timeout == 0 {
		opts.Timeout = time.Second
	}
	return executor.RunScript(context.Background(), opts)
}

func TestRunScriptReturnsValue(t *testing.T) {
	res, err := run(t, executor.ScriptOptions{
		Source:  "return context.a + 1;",
		Context: map[string]any{"a": 1},
	})
	if err != nil {
		t.Fatalf("RunScript() error = %v", err)
	}
	if fmt.Sprintf("%v", res.Value) != "2" {
		t.Errorf("value = %v (%T), want 2", res.Value, res.Value)
	}
}

func TestRunScriptMutationsPersist(t *testing.T) {
	res, err := run(t, executor.ScriptOptions{
		Source:  "context.user.name = 'X'; context.items.push(3); return true;",
		Context: map[string]any{"user": map[string]any{"name": "Jono"}, "items": []any{"a", "b"}},
	})
	if err != nil {
		t.Fatalf("RunScript() error = %v", err)
	}
	user, ok := res.Context["user"].(map[string]any)
	if !ok || user["name"] != "X" {
		t.Errorf("user.name = %v, want X (mutation must persist)", res.Context["user"])
	}
	items, ok := res.Context["items"].([]any)
	if !ok || len(items) != 3 || fmt.Sprintf("%v", items[2]) != "3" {
		t.Errorf("items = %v, want [a b 3]", res.Context["items"])
	}
}

func TestRunScriptFrozenContext(t *testing.T) {
	res, err := run(t, executor.ScriptOptions{
		Source:  "context.a = 99; context.user.name = 'Y'; return context.a;",
		Context: map[string]any{"a": 1, "user": map[string]any{"name": "Jono"}},
		Frozen:  true,
	})
	if err != nil {
		t.Fatalf("RunScript() error = %v", err)
	}
	if fmt.Sprintf("%v", res.Value) != "1" {
		t.Errorf("value = %v, want 1 (frozen context must reject mutation)", res.Value)
	}
	user, ok := res.Context["user"].(map[string]any)
	if !ok || user["name"] != "Jono" {
		t.Errorf("user.name = %v, want Jono (frozen)", res.Context["user"])
	}
}

func TestRunScriptTimeout(t *testing.T) {
	_, err := run(t, executor.ScriptOptions{
		Source:  "while (true) {}",
		Context: map[string]any{},
		Timeout: 50 * time.Millisecond,
	})
	if err == nil {
		t.Fatal("RunScript() error = nil, want timeout")
	}
	if !strings.Contains(err.Error(), "timeout") {
		t.Errorf("error = %v, want timeout mention", err)
	}
}

func TestRunScriptEvalDisabled(t *testing.T) {
	for _, src := range []string{"return eval('1+1');", "return new Function('return 1')();"} {
		if _, err := run(t, executor.ScriptOptions{Source: src, Context: map[string]any{}}); err == nil {
			t.Errorf("RunScript(%q) error = nil, want error (eval/Function disabled)", src)
		}
	}
}

func TestRunScriptGoFuncs(t *testing.T) {
	reg := jsfunc.New()
	if err := reg.Register("double", func(x int) int { return x * 2 }); err != nil {
		t.Fatal(err)
	}
	res, err := run(t, executor.ScriptOptions{
		Source:  "return go.double(21);",
		Context: map[string]any{},
		Funcs:   reg,
	})
	if err != nil {
		t.Fatalf("RunScript() error = %v", err)
	}
	if fmt.Sprintf("%v", res.Value) != "42" {
		t.Errorf("value = %v, want 42", res.Value)
	}
}

func TestRunScriptVars(t *testing.T) {
	res, err := run(t, executor.ScriptOptions{
		Source:  "return input.ok === true;",
		Context: map[string]any{},
		Vars:    map[string]any{"input": map[string]any{"ok": true}},
	})
	if err != nil {
		t.Fatalf("RunScript() error = %v", err)
	}
	if res.Value != true {
		t.Errorf("value = %v, want true", res.Value)
	}
}

func TestRunScriptFrozenVars(t *testing.T) {
	res, err := run(t, executor.ScriptOptions{
		Source: "response.body.status = 'hacked'; response.extra = 1; " +
			"return response.body.status === 'completed' && response.extra === undefined && " +
			"Object.isFrozen(response) && Object.isFrozen(response.body);",
		Context: map[string]any{},
		Vars: map[string]any{
			"response": map[string]any{
				"body":    map[string]any{"status": "completed"},
				"headers": map[string]any{"a": "b"},
			},
		},
		FrozenVars: []string{"response"},
	})
	if err != nil {
		t.Fatalf("RunScript() error = %v", err)
	}
	if res.Value != true {
		t.Errorf("value = %v, want true (frozen response must reject mutation)", res.Value)
	}
}

func TestRunScriptFrozenVarsMissing(t *testing.T) {
	_, err := run(t, executor.ScriptOptions{
		Source:     "return 1;",
		Context:    map[string]any{},
		FrozenVars: []string{"nope"},
	})
	if err == nil {
		t.Error("frozen var not provided error = nil, want error")
	}
}

func TestRunScriptNestedRead(t *testing.T) {
	res, err := run(t, executor.ScriptOptions{
		Source:  "return context.user.profile.age >= 18;",
		Context: map[string]any{"user": map[string]any{"profile": map[string]any{"age": 25}}},
	})
	if err != nil {
		t.Fatalf("RunScript() error = %v", err)
	}
	if res.Value != true {
		t.Errorf("value = %v, want true", res.Value)
	}
}

func TestRunScriptSyntaxError(t *testing.T) {
	if _, err := run(t, executor.ScriptOptions{Source: "if (", Context: map[string]any{}}); err == nil {
		t.Error("RunScript(syntax error) error = nil, want error")
	}
}

func TestRunScriptRequiresTimeout(t *testing.T) {
	if _, err := executor.RunScript(context.Background(), executor.ScriptOptions{
		Source:  "return 1;",
		Context: map[string]any{},
	}); err == nil {
		t.Error("RunScript without timeout error = nil, want error")
	}
}
