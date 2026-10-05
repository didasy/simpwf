package executor

import (
	"bytes"
	"context"
	"fmt"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/simpwf/workflow-engine/pkg/contextpath"
)

// CommandExecutor runs external commands with direct argv (never a shell)
// under an executable allowlist, a per-node timeout enforced by killing the
// whole process group, and capped output capture. Both the allowlist and the
// requested executable must be absolute paths; matching is on the canonical
// path after symlink resolution, and the canonical path is what executes.
type CommandExecutor struct {
	allowlist map[string]struct{}
	maxOutput int
}

// NewCommandExecutor builds a CommandExecutor over the canonical form of
// allowlist. A non-absolute or unresolvable entry panics: bad allowlist
// config must fail fast at startup, never per-request.
func NewCommandExecutor(allowlist []string, maxOutput int) *CommandExecutor {
	canonical := make(map[string]struct{}, len(allowlist))
	for _, entry := range allowlist {
		c, err := canonicalizeExecutable(entry)
		if err != nil {
			panic(fmt.Sprintf("executor: engine.exec_allowlist entry %q must be an absolute path, e.g. %q: %v", entry, "/bin/echo", err))
		}
		canonical[c] = struct{}{}
	}
	return &CommandExecutor{allowlist: canonical, maxOutput: maxOutput}
}

// canonicalizeExecutable resolves exe to its canonical absolute path. It
// fails for non-absolute input and when the path cannot be resolved.
func canonicalizeExecutable(exe string) (string, error) {
	if !filepath.IsAbs(exe) {
		return "", fmt.Errorf("not an absolute path")
	}
	abs, err := filepath.Abs(exe)
	if err != nil {
		return "", err
	}
	return filepath.EvalSymlinks(abs)
}

func (e *CommandExecutor) Execute(ctx context.Context, req Request) (*Result, error) {
	cfg := req.Node.Execution
	if len(cfg.Command) == 0 {
		return nil, &NodeError{Node: req.Node, Reason: "command", Err: fmt.Errorf("empty command")}
	}
	exe := cfg.Command[0]
	canonical, ok := e.allowedExecutable(exe)
	if !ok {
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

	cmd := exec.Command(canonical, cfg.Command[1:]...)
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

// allowedExecutable reports whether exe is allowlisted, returning the
// canonical path to execute. Non-absolute input and resolution failures
// fail closed.
func (e *CommandExecutor) allowedExecutable(exe string) (string, bool) {
	canonical, err := canonicalizeExecutable(exe)
	if err != nil {
		return "", false
	}
	_, ok := e.allowlist[canonical]
	return canonical, ok
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
