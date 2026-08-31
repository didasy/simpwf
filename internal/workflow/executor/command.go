package executor

import (
	"bytes"
	"context"
	"fmt"
	"os/exec"
	"strings"
	"syscall"

	"github.com/simpwf/workflow-engine/pkg/contextpath"
)

// CommandExecutor runs external commands with direct argv (never a shell)
// under an executable allowlist, a per-node timeout enforced by killing the
// whole process group, and capped output capture.
type CommandExecutor struct {
	allowlist []string
	maxOutput int
}

func (e *CommandExecutor) Execute(ctx context.Context, req Request) (*Result, error) {
	cfg := req.Node.Execution
	if len(cfg.Command) == 0 {
		return nil, &NodeError{Node: req.Node, Reason: "command", Err: fmt.Errorf("empty command")}
	}
	exe := cfg.Command[0]
	if !e.allowedExecutable(exe) {
		return nil, &NodeError{Node: req.Node, Reason: "command", Err: fmt.Errorf("executable %q not in allowlist", exe)}
	}
	stdin := ""
	if cfg.Stdin != "" {
		rendered, err := contextpath.RenderTemplate(cfg.Stdin, req.Context)
		if err != nil {
			return nil, &NodeError{Node: req.Node, Reason: "command", Err: fmt.Errorf("render stdin: %w", err)}
		}
		stdin = fmt.Sprintf("%v", rendered)
	}

	ctx, cancel := context.WithTimeout(ctx, nodeTimeout(req.Node))
	defer cancel()

	cmd := exec.Command(exe, cfg.Command[1:]...)
	cmd.Stdin = strings.NewReader(stdin)
	var stdout, stderr cappedBuffer
	stdout.max = e.maxOutput
	stderr.max = e.maxOutput
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	// Run the command in its own process group so the timeout can kill
	// children too, not just the direct child.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	if err := cmd.Start(); err != nil {
		return nil, &NodeError{Node: req.Node, Reason: "command", Err: err}
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()

	timedOut := false
	var waitErr error
	select {
	case <-ctx.Done():
		timedOut = true
		// Kill the whole process group.
		if cmd.Process != nil {
			_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		}
		waitErr = <-done
	case waitErr = <-done:
	}

	res := &Result{
		Output: &CommandResult{
			ExitCode:  exitCodeOf(cmd),
			Stdout:    stdout.String(),
			Stderr:    stderr.String(),
			TimedOut:  timedOut,
			Truncated: stdout.truncated || stderr.truncated,
		},
	}
	if timedOut {
		return res, &NodeError{Node: req.Node, Reason: "command", Err: fmt.Errorf("command timed out after %s", nodeTimeout(req.Node))}
	}
	if waitErr != nil {
		return res, &NodeError{Node: req.Node, Reason: "command", Err: fmt.Errorf("command failed: %w", waitErr)}
	}
	return res, nil
}

func (e *CommandExecutor) allowedExecutable(exe string) bool {
	base := exe
	if i := strings.LastIndexByte(exe, '/'); i >= 0 {
		base = exe[i+1:]
	}
	for _, entry := range e.allowlist {
		if entry == exe || entry == base {
			return true
		}
	}
	return false
}

func exitCodeOf(cmd *exec.Cmd) int {
	if cmd.ProcessState == nil {
		return -1
	}
	return cmd.ProcessState.ExitCode()
}

// cappedBuffer captures output up to max bytes, dropping the rest and
// flagging truncation.
type cappedBuffer struct {
	buf       bytes.Buffer
	max       int
	truncated bool
}

func (b *cappedBuffer) Write(p []byte) (int, error) {
	if b.max > 0 && b.buf.Len()+len(p) > b.max {
		remaining := b.max - b.buf.Len()
		if remaining > 0 {
			_, _ = b.buf.Write(p[:remaining])
		}
		b.truncated = true
		return len(p), nil
	}
	return b.buf.Write(p)
}

func (b *cappedBuffer) String() string { return b.buf.String() }
