package executor_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/simpwf/workflow-engine/internal/workflow/executor"
	"github.com/simpwf/workflow-engine/internal/workflow/model"
)

const testTimeout = time.Second

func conditionsNode(conds ...model.Condition) *model.NodeContent {
	return &model.NodeContent{Type: model.NodeTypeConditions, Conditions: conds, Timeout: testTimeout}
}

func TestConditionsExactlyOneMatchRoutes(t *testing.T) {
	ex := executor.NewExecutors(executor.Limits{}, nil, executor.Dependencies{})[model.NodeTypeConditions]
	ctx := map[string]any{"user": map[string]any{"title": "Manager"}}
	res, err := ex.Execute(context.Background(), executor.Request{
		Node: conditionsNode(
			model.Condition{Key: "staff", Condition: "return context.user.title === 'Staff';"},
			model.Condition{Key: "manager", Condition: "return context.user.title === 'Manager';"},
			model.Condition{Key: "fallback", Condition: "return context.user.title === 'Other';"},
		),
		Context: ctx,
	})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if !res.Output.(*executor.ConditionResult).Matched {
		t.Fatal("expected a match")
	}
	cr := res.Output.(*executor.ConditionResult)
	if cr.Index != 1 || cr.Key != "manager" {
		t.Errorf("matched = index %d key %q, want 1/manager", cr.Index, cr.Key)
	}
}

func TestConditionsMultipleMatchesFail(t *testing.T) {
	ex := executor.NewExecutors(executor.Limits{}, nil, executor.Dependencies{})[model.NodeTypeConditions]
	_, err := ex.Execute(context.Background(), executor.Request{
		Node: &model.NodeContent{
			ID: "route-node", Type: model.NodeTypeConditions, Timeout: testTimeout,
			Conditions: []model.Condition{
				{Key: "all", Condition: "return true;"},
				{Key: "staff", Condition: "return context.user.title === 'Staff';"},
				{Key: "manager", Condition: "return context.user.title === 'Manager';"},
			},
		},
		Context: map[string]any{"user": map[string]any{"title": "Manager"}},
	})
	if err == nil {
		t.Fatal("Execute() error = nil, want multiple-match error")
	}
	for _, want := range []string{"route-node", `index 0 key "all"`, `index 2 key "manager"`} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error = %q, want contains %q", err.Error(), want)
		}
	}
}

func TestConditionsMultipleMatchesIncludeEmptyKey(t *testing.T) {
	ex := executor.NewExecutors(executor.Limits{}, nil, executor.Dependencies{})[model.NodeTypeConditions]
	_, err := ex.Execute(context.Background(), executor.Request{
		Node: conditionsNode(
			model.Condition{Condition: "return true;"},
			model.Condition{Key: "a", Condition: "return true;"},
		),
		Context: map[string]any{},
	})
	if err == nil {
		t.Fatal("Execute() error = nil, want multiple-match error")
	}
	if !strings.Contains(err.Error(), `key ""`) || !strings.Contains(err.Error(), `key "a"`) {
		t.Errorf("error = %q, want empty and named keys listed", err.Error())
	}
}

func TestConditionsReturnsMatchedKey(t *testing.T) {
	ex := executor.NewExecutors(executor.Limits{}, nil, executor.Dependencies{})[model.NodeTypeConditions]
	res, err := ex.Execute(context.Background(), executor.Request{
		Node:    conditionsNode(model.Condition{Key: "exit", Condition: "return context.exit === true;"}),
		Context: map[string]any{"exit": true},
	})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	cr := res.Output.(*executor.ConditionResult)
	if !cr.Matched || cr.Key != "exit" {
		t.Errorf("result = %+v, want matched exit key", cr)
	}
}

func TestConditionsReturnsEmptyKeyForScopeExit(t *testing.T) {
	ex := executor.NewExecutors(executor.Limits{}, nil, executor.Dependencies{})[model.NodeTypeConditions]
	res, err := ex.Execute(context.Background(), executor.Request{
		Node:    conditionsNode(model.Condition{Condition: "return true;"}),
		Context: map[string]any{},
	})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	cr := res.Output.(*executor.ConditionResult)
	if !cr.Matched || cr.Key != "" {
		t.Errorf("result = %+v, want matched empty exit key", cr)
	}
}

func TestConditionsNoMatch(t *testing.T) {
	ex := executor.NewExecutors(executor.Limits{}, nil, executor.Dependencies{})[model.NodeTypeConditions]
	res, err := ex.Execute(context.Background(), executor.Request{
		Node:    conditionsNode(model.Condition{Key: "never", Condition: "return false;"}),
		Context: map[string]any{},
	})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	cr := res.Output.(*executor.ConditionResult)
	if cr.Matched || cr.Index != -1 {
		t.Errorf("result = %+v, want no match", cr)
	}
}

func TestConditionsNonBooleanError(t *testing.T) {
	ex := executor.NewExecutors(executor.Limits{}, nil, executor.Dependencies{})[model.NodeTypeConditions]
	_, err := ex.Execute(context.Background(), executor.Request{
		Node:    conditionsNode(model.Condition{Key: "invalid", Condition: "return 'yes';"}),
		Context: map[string]any{},
	})
	if err == nil {
		t.Error("Execute() error = nil, want error for non-boolean condition")
	}
}

func TestConditionsContextFrozen(t *testing.T) {
	ex := executor.NewExecutors(executor.Limits{}, nil, executor.Dependencies{})[model.NodeTypeConditions]
	res, err := ex.Execute(context.Background(), executor.Request{
		Node:    conditionsNode(model.Condition{Key: "frozen", Condition: "context.a = 99; return true;"}),
		Context: map[string]any{"a": 1},
	})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	cr := res.Output.(*executor.ConditionResult)
	if !cr.Matched {
		t.Fatal("expected match")
	}
	if res.Context["a"] != 1 {
		t.Errorf("context.a = %v, want 1 (frozen)", res.Context["a"])
	}
}
