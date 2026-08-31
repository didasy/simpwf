package executor_test

import (
	"context"
	"testing"

	"github.com/simpwf/workflow-engine/internal/workflow/executor"
	"github.com/simpwf/workflow-engine/internal/workflow/model"
)

func inputNode(validation string) *model.NodeContent {
	var v *model.ValidationScript
	if validation != "" {
		v = &model.ValidationScript{Script: validation}
	}
	return &model.NodeContent{Type: model.NodeTypeInput, Channel: "http", ContextPath: "webhook", Validation: v, Timeout: testTimeout}
}

func TestInputValidationSuccess(t *testing.T) {
	ex := executor.NewExecutors(executor.Limits{}, nil, executor.Dependencies{})[model.NodeTypeInput]
	res, err := ex.Execute(context.Background(), executor.Request{
		Node:    inputNode("return;"),
		Context: map[string]any{},
		Payload: []byte(`{"ok":true}`),
	})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	vr := res.Output.(*executor.ValidationResult)
	if !vr.Valid {
		t.Errorf("result = %+v, want valid", vr)
	}
}

func TestInputValidationRejects(t *testing.T) {
	ex := executor.NewExecutors(executor.Limits{}, nil, executor.Dependencies{})[model.NodeTypeInput]
	res, err := ex.Execute(context.Background(), executor.Request{
		Node:    inputNode(`input = JSON.parse(input); if (!input.success) { return 'Webhook failed!'; };`),
		Context: map[string]any{},
		Payload: []byte(`{"success":false}`),
	})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	vr := res.Output.(*executor.ValidationResult)
	if vr.Valid || vr.Message != "Webhook failed!" {
		t.Errorf("result = %+v, want invalid with message", vr)
	}
}

func TestInputValidationNonStringReturnIsSuccess(t *testing.T) {
	ex := executor.NewExecutors(executor.Limits{}, nil, executor.Dependencies{})[model.NodeTypeInput]
	res, err := ex.Execute(context.Background(), executor.Request{
		Node:    inputNode("return true;"),
		Context: map[string]any{},
		Payload: []byte(`{"success":true}`),
	})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if !res.Output.(*executor.ValidationResult).Valid {
		t.Errorf("result = %+v, want valid", res.Output)
	}
}

func TestInputValidationScriptError(t *testing.T) {
	ex := executor.NewExecutors(executor.Limits{}, nil, executor.Dependencies{})[model.NodeTypeInput]
	_, err := ex.Execute(context.Background(), executor.Request{
		Node:    inputNode("return boom();"),
		Context: map[string]any{},
		Payload: []byte(`{}`),
	})
	if err == nil {
		t.Error("Execute() error = nil, want script error")
	}
}
