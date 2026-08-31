package executor_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/simpwf/workflow-engine/internal/workflow/executor"
	"github.com/simpwf/workflow-engine/internal/workflow/model"
)

func execNode(command []string, stdin string) *model.NodeContent {
	return &model.NodeContent{
		Type:      model.NodeTypeExternalCall,
		Execution: &model.ExecutionConfig{Command: command, Stdin: stdin},
		Timeout:   testTimeout,
	}
}

func TestCommandExecutorRuns(t *testing.T) {
	ex := executor.NewExecutors(executor.Limits{ExecAllowlist: []string{"echo"}, MaxOutputBytes: 4096}, nil, executor.Dependencies{})[model.NodeTypeExternalCall]
	res, err := ex.Execute(context.Background(), executor.Request{
		Node:    execNode([]string{"echo", "hello"}, ""),
		Context: map[string]any{},
	})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	cr := res.Output.(*executor.CommandResult)
	if cr.ExitCode != 0 {
		t.Errorf("exit code = %d, want 0", cr.ExitCode)
	}
	if strings.TrimSpace(cr.Stdout) != "hello" {
		t.Errorf("stdout = %q", cr.Stdout)
	}
}

func TestCommandExecutorRejectsNonAllowlisted(t *testing.T) {
	ex := executor.NewExecutors(executor.Limits{ExecAllowlist: []string{"echo"}, MaxOutputBytes: 4096}, nil, executor.Dependencies{})[model.NodeTypeExternalCall]
	_, err := ex.Execute(context.Background(), executor.Request{
		Node:    execNode([]string{"rm", "-rf", "/tmp/x"}, ""),
		Context: map[string]any{},
	})
	if err == nil {
		t.Error("Execute() error = nil, want allowlist rejection")
	}
}

func TestCommandExecutorExitCode(t *testing.T) {
	ex := executor.NewExecutors(executor.Limits{ExecAllowlist: []string{"false"}, MaxOutputBytes: 4096}, nil, executor.Dependencies{})[model.NodeTypeExternalCall]
	res, err := ex.Execute(context.Background(), executor.Request{
		Node:    execNode([]string{"false"}, ""),
		Context: map[string]any{},
	})
	if err == nil {
		t.Error("Execute(false) error = nil, want non-zero exit error")
	}
	if res == nil {
		t.Fatal("result is nil on non-zero exit")
	}
	cr := res.Output.(*executor.CommandResult)
	if cr.ExitCode != 1 {
		t.Errorf("exit code = %d, want 1", cr.ExitCode)
	}
}

func TestCommandExecutorStdin(t *testing.T) {
	ex := executor.NewExecutors(executor.Limits{ExecAllowlist: []string{"cat"}, MaxOutputBytes: 4096}, nil, executor.Dependencies{})[model.NodeTypeExternalCall]
	res, err := ex.Execute(context.Background(), executor.Request{
		Node:    execNode([]string{"cat"}, "{{ payload.text }}"),
		Context: map[string]any{"payload": map[string]any{"text": "streamed"}},
	})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if strings.TrimSpace(res.Output.(*executor.CommandResult).Stdout) != "streamed" {
		t.Errorf("stdout = %q", res.Output.(*executor.CommandResult).Stdout)
	}
}

func TestCommandExecutorOutputCap(t *testing.T) {
	ex := executor.NewExecutors(executor.Limits{ExecAllowlist: []string{"seq"}, MaxOutputBytes: 64}, nil, executor.Dependencies{})[model.NodeTypeExternalCall]
	res, err := ex.Execute(context.Background(), executor.Request{
		Node:    execNode([]string{"seq", "1", "100000"}, ""),
		Context: map[string]any{},
	})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	cr := res.Output.(*executor.CommandResult)
	if !cr.Truncated {
		t.Error("truncated = false, want true with 64-byte cap")
	}
	if len(cr.Stdout) > 64 {
		t.Errorf("stdout len = %d, exceeds cap", len(cr.Stdout))
	}
}

func TestCommandExecutorTimeout(t *testing.T) {
	ex := executor.NewExecutors(executor.Limits{ExecAllowlist: []string{"sleep"}, MaxOutputBytes: 4096}, nil, executor.Dependencies{})[model.NodeTypeExternalCall]
	start := time.Now()
	_, err := ex.Execute(context.Background(), executor.Request{
		Node: &model.NodeContent{
			Type:      model.NodeTypeExternalCall,
			Execution: &model.ExecutionConfig{Command: []string{"sleep", "30"}},
			Timeout:   100 * time.Millisecond,
		},
		Context: map[string]any{},
	})
	if err == nil {
		t.Fatal("Execute() error = nil, want timeout")
	}
	if !strings.Contains(err.Error(), "timed out") {
		t.Errorf("error = %v, want timeout mention", err)
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Errorf("timeout took %v, process group may not have been killed", elapsed)
	}
}
