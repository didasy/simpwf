package kernel

import (
	"context"

	"github.com/simpwf/workflow-engine/internal/workflow/executor"
	"github.com/simpwf/workflow-engine/internal/workflow/model"
)

// ExitedGroupLookup resolves a group node by id for post-exit hooks.
type ExitedGroupLookup interface {
	Node(id string) (*model.NodeContent, error)
}

// RunExitedGroupPosts executes the post_script of each exited group
// innermost-first, threading the workflow context through the hooks in
// order. The returned context is the last successfully transformed one:
// when a hook fails it is the context before that hook, so the caller can
// persist the latest completed state.
func RunExitedGroupPosts(ctx context.Context, hooks *executor.HookRunner, lookup ExitedGroupLookup, exited []string, ctxMap map[string]any) (map[string]any, error) {
	curCtx := ctxMap
	for _, gid := range exited {
		gnc, err := lookup.Node(gid)
		if err != nil {
			return curCtx, err
		}
		next, err := hooks.RunPost(ctx, gnc, curCtx, nil)
		if err != nil {
			return curCtx, err
		}
		curCtx = next
	}
	return curCtx, nil
}
