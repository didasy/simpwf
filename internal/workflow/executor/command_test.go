package executor_test

import (
	"context"
	"os"
	"path/filepath"
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

// writeScript creates an executable shell script in dir and returns its
// absolute path, so tests never depend on system binaries or PATH.
func writeScript(t *testing.T, dir, name, body string) string {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte("#!/bin/sh\n"+body), 0o755); err != nil {
		t.Fatalf("write script: %v", err)
	}
	return path
}

func TestCommandExecutorRuns(t *testing.T) {
	exe := writeScript(t, t.TempDir(), "greet", "printf '%s\\n' \"$@\"\n")
	ex := executor.NewExecutors(executor.Limits{ExecAllowlist: []string{exe}, MaxOutputBytes: 4096}, nil, executor.Dependencies{})[model.NodeTypeExternalCall]
	res, err := ex.Execute(context.Background(), executor.Request{
		Node:    execNode([]string{exe, "hello"}, ""),
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
	dir := t.TempDir()
	allowed := writeScript(t, dir, "allowed", "exit 0\n")
	other := writeScript(t, dir, "other", "exit 0\n")
	ex := executor.NewExecutors(executor.Limits{ExecAllowlist: []string{allowed}, MaxOutputBytes: 4096}, nil, executor.Dependencies{})[model.NodeTypeExternalCall]
	_, err := ex.Execute(context.Background(), executor.Request{
		Node:    execNode([]string{other, "-rf", "/tmp/x"}, ""),
		Context: map[string]any{},
	})
	if err == nil || !strings.Contains(err.Error(), "not in allowlist") {
		t.Errorf("Execute() error = %v, want allowlist rejection", err)
	}
}

// TestCommandExecutorRejectsPathCollision pins the SEC-2 exploit: an
// executable with the same basename as an allowlisted entry, but at a
// different path, must be rejected without running.
func TestCommandExecutorRejectsPathCollision(t *testing.T) {
	allowedDir := t.TempDir()
	evilDir := t.TempDir()
	allowed := writeScript(t, allowedDir, "tool", "exit 0\n")
	marker := filepath.Join(evilDir, "pwned")
	evil := writeScript(t, evilDir, "tool", "touch "+marker+"\n")
	ex := executor.NewExecutors(executor.Limits{ExecAllowlist: []string{allowed}, MaxOutputBytes: 4096}, nil, executor.Dependencies{})[model.NodeTypeExternalCall]
	_, err := ex.Execute(context.Background(), executor.Request{
		Node:    execNode([]string{evil}, ""),
		Context: map[string]any{},
	})
	if err == nil || !strings.Contains(err.Error(), "not in allowlist") {
		t.Fatalf("Execute() error = %v, want allowlist rejection", err)
	}
	if _, statErr := os.Stat(marker); !os.IsNotExist(statErr) {
		t.Error("decoy executable ran: marker file exists")
	}
}

func TestCommandExecutorRejectsNonAbsoluteRequest(t *testing.T) {
	allowed := writeScript(t, t.TempDir(), "tool", "exit 0\n")
	ex := executor.NewExecutors(executor.Limits{ExecAllowlist: []string{allowed}, MaxOutputBytes: 4096}, nil, executor.Dependencies{})[model.NodeTypeExternalCall]
	for _, exe := range []string{"tool", "./tool", "subdir/tool", "../tool", ""} {
		_, err := ex.Execute(context.Background(), executor.Request{
			Node:    execNode([]string{exe}, ""),
			Context: map[string]any{},
		})
		if err == nil || !strings.Contains(err.Error(), "not in allowlist") {
			t.Errorf("Execute(%q) error = %v, want allowlist rejection", exe, err)
		}
	}
}

func TestCommandExecutorResolvesSymlinks(t *testing.T) {
	dir := t.TempDir()
	real := writeScript(t, dir, "real", "printf ok\n")
	outside := writeScript(t, t.TempDir(), "outside", "printf pwned\n")
	inLink := filepath.Join(dir, "alias")
	if err := os.Symlink(real, inLink); err != nil {
		t.Fatalf("symlink alias: %v", err)
	}
	outLink := filepath.Join(dir, "escape")
	if err := os.Symlink(outside, outLink); err != nil {
		t.Fatalf("symlink escape: %v", err)
	}
	ex := executor.NewExecutors(executor.Limits{ExecAllowlist: []string{real}, MaxOutputBytes: 4096}, nil, executor.Dependencies{})[model.NodeTypeExternalCall]

	res, err := ex.Execute(context.Background(), executor.Request{
		Node:    execNode([]string{inLink}, ""),
		Context: map[string]any{},
	})
	if err != nil {
		t.Fatalf("Execute(alias) error = %v, want symlink to allowlisted target accepted", err)
	}
	if got := res.Output.(*executor.CommandResult).Stdout; got != "ok" {
		t.Errorf("stdout = %q, want %q", got, "ok")
	}

	_, err = ex.Execute(context.Background(), executor.Request{
		Node:    execNode([]string{outLink}, ""),
		Context: map[string]any{},
	})
	if err == nil || !strings.Contains(err.Error(), "not in allowlist") {
		t.Errorf("Execute(escape) error = %v, want allowlist rejection", err)
	}
}

func TestCommandExecutorExitCode(t *testing.T) {
	exe := writeScript(t, t.TempDir(), "fail", "exit 1\n")
	ex := executor.NewExecutors(executor.Limits{ExecAllowlist: []string{exe}, MaxOutputBytes: 4096}, nil, executor.Dependencies{})[model.NodeTypeExternalCall]
	res, err := ex.Execute(context.Background(), executor.Request{
		Node:    execNode([]string{exe}, ""),
		Context: map[string]any{},
	})
	if err == nil {
		t.Error("Execute(fail) error = nil, want non-zero exit error")
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
	exe := writeScript(t, t.TempDir(), "relay", "cat\n")
	ex := executor.NewExecutors(executor.Limits{ExecAllowlist: []string{exe}, MaxOutputBytes: 4096}, nil, executor.Dependencies{})[model.NodeTypeExternalCall]
	res, err := ex.Execute(context.Background(), executor.Request{
		Node:    execNode([]string{exe}, "{{ payload.text }}"),
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
	exe := writeScript(t, t.TempDir(), "flood", "i=1; while [ \"$i\" -le 100 ]; do echo \"line $i\"; i=$((i+1)); done\n")
	ex := executor.NewExecutors(executor.Limits{ExecAllowlist: []string{exe}, MaxOutputBytes: 64}, nil, executor.Dependencies{})[model.NodeTypeExternalCall]
	res, err := ex.Execute(context.Background(), executor.Request{
		Node:    execNode([]string{exe}, ""),
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
	exe := writeScript(t, t.TempDir(), "nap", "sleep 30\n")
	ex := executor.NewExecutors(executor.Limits{ExecAllowlist: []string{exe}, MaxOutputBytes: 4096}, nil, executor.Dependencies{})[model.NodeTypeExternalCall]
	start := time.Now()
	_, err := ex.Execute(context.Background(), executor.Request{
		Node: &model.NodeContent{
			Type:      model.NodeTypeExternalCall,
			Execution: &model.ExecutionConfig{Command: []string{exe, "30"}},
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

func TestNewExecutorsPanicsOnBadAllowlist(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "missing")
	for _, entry := range []string{"echo", "./echo", "bin/echo", "", missing} {
		func() {
			defer func() {
				r := recover()
				if r == nil {
					t.Errorf("NewExecutors(%q) did not panic, want fail-fast", entry)
					return
				}
				msg, _ := r.(string)
				if !strings.Contains(msg, "engine.exec_allowlist") {
					t.Errorf("panic = %q, want engine.exec_allowlist mention", msg)
				}
			}()
			executor.NewExecutors(executor.Limits{ExecAllowlist: []string{entry}}, nil, executor.Dependencies{})
		}()
	}
}

func TestNewExecutorsAcceptsEmptyAllowlist(t *testing.T) {
	ex := executor.NewExecutors(executor.Limits{MaxOutputBytes: 4096}, nil, executor.Dependencies{})[model.NodeTypeExternalCall]
	exe := writeScript(t, t.TempDir(), "tool", "exit 0\n")
	_, err := ex.Execute(context.Background(), executor.Request{
		Node:    execNode([]string{exe}, ""),
		Context: map[string]any{},
	})
	if err == nil || !strings.Contains(err.Error(), "not in allowlist") {
		t.Errorf("Execute() error = %v, want allowlist rejection", err)
	}
}
