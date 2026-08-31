package executor

import (
	"context"

	"github.com/simpwf/workflow-engine/internal/workflow/model"
	"github.com/simpwf/workflow-engine/pkg/jsfunc"
)

// HookRunner executes lifecycle hooks in the same Goja sandbox as node
// scripts. One instance is built per process and shared by the engine and
// the instance service, using the same registered Go functions as the node
// executors.
type HookRunner struct {
	funcs *jsfunc.Registry
}

// NewHookRunner builds a hook runner. funcs may be nil.
func NewHookRunner(funcs *jsfunc.Registry) *HookRunner {
	return &HookRunner{funcs: funcs}
}

// RunPre executes the node's pre_script against a clone of ctxMap and
// returns the transformed context. A nil hook returns ctxMap unchanged and
// never mutates it in place.
func (r *HookRunner) RunPre(ctx context.Context, nc *model.NodeContent, ctxMap map[string]any) (map[string]any, error) {
	if nc == nil || nc.PreScript == nil {
		return ctxMap, nil
	}
	return runHook(ctx, r.funcs, nc.PreScript, "pre-script", ctxMap, nil, nc)
}

// RunPost executes the node's post_script against a clone of ctxMap and
// returns the transformed context. output is the native node output exposed
// to the script as a deep-frozen "output" global for convenient reads; hook
// return values are always ignored. A nil hook returns ctxMap unchanged.
func (r *HookRunner) RunPost(ctx context.Context, nc *model.NodeContent, ctxMap map[string]any, output any) (map[string]any, error) {
	if nc == nil || nc.PostScript == nil {
		return ctxMap, nil
	}
	return runHook(ctx, r.funcs, nc.PostScript, "post-script", ctxMap, output, nc)
}

func runHook(ctx context.Context, funcs *jsfunc.Registry, h *model.HookScript, reason string, ctxMap map[string]any, output any, node *model.NodeContent) (map[string]any, error) {
	opts := ScriptOptions{
		Source:  h.Script,
		Context: ctxMap,
		Timeout: h.Timeout,
		Funcs:   funcs,
	}
	if output != nil {
		opts.Vars = map[string]any{"output": output}
		opts.FrozenVars = []string{"output"}
	}
	res, err := RunScript(ctx, opts)
	if err != nil {
		return nil, &NodeError{Node: node, Reason: reason, Err: err}
	}
	return res.Context, nil
}
