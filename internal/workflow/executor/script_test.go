package executor_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/simpwf/workflow-engine/internal/workflow/executor"
	"github.com/simpwf/workflow-engine/internal/workflow/model"
)

func scriptNode(source string) *model.NodeContent {
	return &model.NodeContent{Type: model.NodeTypeScript, Script: source, Timeout: testTimeout}
}

func TestScriptExecutorReturnsValue(t *testing.T) {
	res, err := executor.NewExecutors(executor.Limits{}, nil, executor.Dependencies{})[model.NodeTypeScript].Execute(
		context.Background(),
		executor.Request{Node: scriptNode("return context.a + 1;"), Context: map[string]any{"a": 1}},
	)
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if fmt.Sprintf("%v", res.Output) != "2" {
		t.Errorf("output = %v, want 2", res.Output)
	}
}

func TestScriptExecutorMutatesContext(t *testing.T) {
	res, err := executor.NewExecutors(executor.Limits{}, nil, executor.Dependencies{})[model.NodeTypeScript].Execute(
		context.Background(),
		executor.Request{
			Node:    scriptNode("context.user.name = 'X'; return 1;"),
			Context: map[string]any{"user": map[string]any{"name": "Jono"}},
		},
	)
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	user, ok := res.Context["user"].(map[string]any)
	if !ok || user["name"] != "X" {
		t.Errorf("user.name = %v, want X", res.Context["user"])
	}
}

func TestScriptExecutorError(t *testing.T) {
	_, err := executor.NewExecutors(executor.Limits{}, nil, executor.Dependencies{})[model.NodeTypeScript].Execute(
		context.Background(),
		executor.Request{Node: scriptNode("return undefinedVar.x;"), Context: map[string]any{}},
	)
	if err == nil {
		t.Error("Execute() error = nil, want script error")
	}
}
