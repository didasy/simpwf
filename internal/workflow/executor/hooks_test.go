package executor_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/simpwf/workflow-engine/internal/workflow/executor"
	"github.com/simpwf/workflow-engine/internal/workflow/model"
)

func hookNode(pre, post *model.HookScript) *model.NodeContent {
	return &model.NodeContent{Type: model.NodeTypeScript, PreScript: pre, PostScript: post}
}

// hookNum normalizes a goja-exported number (int64 for integral values) to
// float64 for assertions.
func hookNum(t *testing.T, v any) float64 {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal value %v: %v", v, err)
	}
	var f float64
	if err := json.Unmarshal(b, &f); err != nil {
		t.Fatalf("value %v is not a number: %v", v, err)
	}
	return f
}

func TestHookRunnerPreMutatesContext(t *testing.T) {
	r := executor.NewHookRunner(nil)
	nc := hookNode(&model.HookScript{Script: "context.p = context.a + 1;", Timeout: time.Second}, nil)
	got, err := r.RunPre(context.Background(), nc, map[string]any{"a": 1})
	if err != nil {
		t.Fatalf("RunPre() error = %v", err)
	}
	if hookNum(t, got["p"]) != 2 {
		t.Errorf("context = %v, want p=2", got)
	}
	if hookNum(t, got["a"]) != 1 {
		t.Errorf("context = %v, want original a preserved", got)
	}
}

func TestHookRunnerPreReturnValueIgnored(t *testing.T) {
	r := executor.NewHookRunner(nil)
	nc := hookNode(&model.HookScript{Script: "context.p = 1; return 'ignored';", Timeout: time.Second}, nil)
	got, err := r.RunPre(context.Background(), nc, map[string]any{})
	if err != nil {
		t.Fatalf("RunPre() error = %v", err)
	}
	if hookNum(t, got["p"]) != 1 {
		t.Errorf("context = %v, want p=1", got)
	}
}

func TestHookRunnerPostReceivesFrozenOutput(t *testing.T) {
	r := executor.NewHookRunner(nil)
	nc := hookNode(nil, &model.HookScript{Script: "context.o = output.status; output.status = 999; context.s = output.status;", Timeout: time.Second})
	got, err := r.RunPost(context.Background(), nc, map[string]any{}, map[string]any{"status": 200})
	if err != nil {
		t.Fatalf("RunPost() error = %v", err)
	}
	if hookNum(t, got["o"]) != 200 {
		t.Errorf("context.o = %v, want 200", got["o"])
	}
	if hookNum(t, got["s"]) != 200 {
		t.Errorf("context.s = %v, want 200 (output must be frozen, mutation dropped)", got["s"])
	}
}

func TestHookRunnerPostReturnValueIgnored(t *testing.T) {
	r := executor.NewHookRunner(nil)
	nc := hookNode(nil, &model.HookScript{Script: "context.q = 1; return {ignored: true};", Timeout: time.Second})
	got, err := r.RunPost(context.Background(), nc, map[string]any{}, nil)
	if err != nil {
		t.Fatalf("RunPost() error = %v", err)
	}
	if hookNum(t, got["q"]) != 1 {
		t.Errorf("context = %v, want q=1", got)
	}
}

func TestHookRunnerNilHookPassthrough(t *testing.T) {
	r := executor.NewHookRunner(nil)
	ctxMap := map[string]any{"a": 1}
	pre, err := r.RunPre(context.Background(), hookNode(nil, nil), ctxMap)
	if err != nil {
		t.Fatalf("RunPre(nil hook) error = %v", err)
	}
	if pre["a"] != 1 {
		t.Errorf("pre context = %v", pre)
	}
	post, err := r.RunPost(context.Background(), hookNode(nil, nil), ctxMap, nil)
	if err != nil {
		t.Fatalf("RunPost(nil hook) error = %v", err)
	}
	if post["a"] != 1 {
		t.Errorf("post context = %v", post)
	}
}

func TestHookRunnerTimeout(t *testing.T) {
	r := executor.NewHookRunner(nil)
	nc := hookNode(&model.HookScript{Script: "while (true) {}", Timeout: 20 * time.Millisecond}, nil)
	if _, err := r.RunPre(context.Background(), nc, map[string]any{}); !errors.Is(err, executor.ErrScriptTimeout) {
		t.Errorf("RunPre() error = %v, want ErrScriptTimeout", err)
	}
}

func TestHookRunnerScriptError(t *testing.T) {
	r := executor.NewHookRunner(nil)
	nc := hookNode(&model.HookScript{Script: "return missing.value;", Timeout: time.Second}, nil)
	_, err := r.RunPre(context.Background(), nc, map[string]any{})
	if err == nil {
		t.Fatal("RunPre() error = nil, want script error")
	}
	if !strings.Contains(err.Error(), "pre-script") {
		t.Errorf("error = %q, want pre-script reason", err)
	}
}

func TestHookRunnerFailureLeavesInputContextUntouched(t *testing.T) {
	r := executor.NewHookRunner(nil)
	ctxMap := map[string]any{"a": 1}
	nc := hookNode(&model.HookScript{Script: "context.b = 2; throw new Error('boom');", Timeout: time.Second}, nil)
	if _, err := r.RunPre(context.Background(), nc, ctxMap); err == nil {
		t.Fatal("RunPre() error = nil, want script error")
	}
	if len(ctxMap) != 1 || ctxMap["a"] != 1 {
		t.Errorf("input context mutated on failure: %v", ctxMap)
	}
}
