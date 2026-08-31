// Package executor runs node executions: the Goja sandbox plus the script,
// conditions, input-validation, HTTP, and command executors.
package executor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/dop251/goja"
	"github.com/simpwf/workflow-engine/pkg/jsfunc"
)

// ErrScriptTimeout is returned when a script exceeds its budget.
var ErrScriptTimeout = errors.New("executor: script timeout")

// ScriptOptions configures one sandboxed script run.
type ScriptOptions struct {
	Source  string
	Context map[string]any
	Timeout time.Duration
	// Funcs is the pkg/jsfunc registry exposed as the "go" root object.
	Funcs *jsfunc.Registry
	// Frozen deep-freezes the context so scripts cannot mutate it
	// (conditions and input validation).
	Frozen bool
	// Vars are additional globals (e.g. "input" for validation scripts).
	Vars map[string]any
	// FrozenVars lists Vars names that are deep-cloned into pure JavaScript
	// objects and recursively frozen, so predicates cannot mutate injected
	// read-only values (e.g. the poller "response").
	FrozenVars []string
}

// ScriptResult carries the post-run exported context and the return value.
type ScriptResult struct {
	Context map[string]any
	Value   any
}

// deepFreezeSrc is a prelude that recursively freezes the context object.
const deepFreezeSrc = `(function () {
	if (typeof Object.freeze !== 'function') { return; }
	function deepFreeze(o) {
		if (o !== null && typeof o === 'object' && !Object.isFrozen(o)) {
			Object.freeze(o);
			Object.getOwnPropertyNames(o).forEach(function (k) { deepFreeze(o[k]); });
		}
	}
	deepFreeze(context);
})();`

// freezeVarSrc is a prelude that recursively freezes the named global (a
// frozen var from Vars, e.g. response).
const freezeVarSrc = `(function () {
	if (typeof Object.freeze !== 'function') { return; }
	function deepFreeze(o) {
		if (o !== null && typeof o === 'object' && !Object.isFrozen(o)) {
			Object.freeze(o);
			Object.getOwnPropertyNames(o).forEach(function (k) { deepFreeze(o[k]); });
		}
	}
	deepFreeze(%s);
})();`

// RunScript executes ES5.1-style JavaScript in the Goja sandbox:
//   - no eval / Function constructor;
//   - context injected as the "context" global (deep-frozen when Frozen);
//   - registered Go functions exposed under the "go" root object;
//   - hard timeout via runtime interrupt;
//   - the context is re-exported after the run so mutations persist.
func RunScript(ctx context.Context, opts ScriptOptions) (*ScriptResult, error) {
	if opts.Timeout <= 0 {
		return nil, errors.New("executor: script timeout must be > 0")
	}
	if opts.Context == nil {
		opts.Context = map[string]any{}
	}
	if ctx == nil {
		ctx = context.Background()
	}

	vm := goja.New()
	// eval and the Function constructor are banned.
	if err := vm.Set("eval", goja.Undefined()); err != nil {
		return nil, fmt.Errorf("executor: disable eval: %w", err)
	}
	if err := vm.Set("Function", goja.Undefined()); err != nil {
		return nil, fmt.Errorf("executor: disable Function: %w", err)
	}
	// The context is deep-copied into a pure JavaScript object: host
	// (Go map/slice-backed) objects cannot be frozen and do not reliably
	// propagate array mutations, so the script always works on a clone and
	// the exported result is the source of truth.
	raw, err := json.Marshal(opts.Context)
	if err != nil {
		return nil, fmt.Errorf("executor: encode context: %w", err)
	}
	obj, err := vm.RunString("(" + string(raw) + ")")
	if err != nil {
		return nil, fmt.Errorf("executor: load context: %w", err)
	}
	if err := vm.Set("context", obj); err != nil {
		return nil, fmt.Errorf("executor: set context: %w", err)
	}
	if opts.Funcs != nil {
		if err := vm.Set("go", opts.Funcs.All()); err != nil {
			return nil, fmt.Errorf("executor: set go funcs: %w", err)
		}
	}
	for name, value := range opts.Vars {
		if err := vm.Set(name, value); err != nil {
			return nil, fmt.Errorf("executor: set var %q: %w", name, err)
		}
	}
	// Frozen vars are re-injected as pure JavaScript clones and deep-frozen;
	// host objects cannot be frozen and mutations on them would not behave
	// like read-only data.
	for _, name := range opts.FrozenVars {
		value, ok := opts.Vars[name]
		if !ok {
			return nil, fmt.Errorf("executor: frozen var %q is not provided", name)
		}
		raw, err := json.Marshal(value)
		if err != nil {
			return nil, fmt.Errorf("executor: encode frozen var %q: %w", name, err)
		}
		obj, err := vm.RunString("(" + string(raw) + ")")
		if err != nil {
			return nil, fmt.Errorf("executor: load frozen var %q: %w", name, err)
		}
		if err := vm.Set(name, obj); err != nil {
			return nil, fmt.Errorf("executor: set frozen var %q: %w", name, err)
		}
		if _, err := vm.RunString(fmt.Sprintf(freezeVarSrc, name)); err != nil {
			return nil, fmt.Errorf("executor: freeze var %q: %w", name, err)
		}
	}
	if opts.Frozen {
		if _, err := vm.RunString(deepFreezeSrc); err != nil {
			return nil, fmt.Errorf("executor: freeze context: %w", err)
		}
	}

	done := make(chan struct{})
	defer close(done)
	timer := time.NewTimer(opts.Timeout)
	defer timer.Stop()
	go func() {
		select {
		case <-done:
		case <-ctx.Done():
			vm.Interrupt(ctx.Err())
		case <-timer.C:
			vm.Interrupt(ErrScriptTimeout)
		}
	}()

	// Scripts are written with a top-level "return" (see workflow.yaml), so
	// the source runs inside an immediately-invoked function body.
	val, err := vm.RunString("(function(){\n" + opts.Source + "\n})()")
	if err != nil {
		var interrupted *goja.InterruptedError
		if errors.As(err, &interrupted) {
			if errors.Is(fmt.Errorf("%v", interrupted.Value()), ErrScriptTimeout) || ctx.Err() != nil {
				return nil, fmt.Errorf("%w: %v", ErrScriptTimeout, interrupted.Value())
			}
			return nil, fmt.Errorf("executor: script interrupted: %w", err)
		}
		return nil, fmt.Errorf("executor: %w", err)
	}

	var out map[string]any
	if err := vm.ExportTo(vm.Get("context"), &out); err != nil {
		return nil, fmt.Errorf("executor: export context: %w", err)
	}
	return &ScriptResult{Context: out, Value: val.Export()}, nil
}
