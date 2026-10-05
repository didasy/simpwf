package engine_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/simpwf/workflow-engine/internal/workflow/engine"
	"github.com/simpwf/workflow-engine/internal/workflow/executor"
	"github.com/simpwf/workflow-engine/internal/workflow/kernel"
	"github.com/simpwf/workflow-engine/internal/workflow/model"
	"github.com/simpwf/workflow-engine/internal/workflow/repository"
)

// TestEngineKernelOnlyLoader proves the engine drives a def-ref workflow
// with zero service involvement: the loader resolves node_definition_id
// references through kernel.NewMaterializer exactly like cmd/app/main.go,
// and the materialized node executes.
func TestEngineKernelOnlyLoader(t *testing.T) {
	db := setupEngineDB(t)
	ctx := context.Background()

	defID := newEngineID()
	nd := model.NodeDefinition{
		ID:        defID,
		Name:      "kernel-shared-script",
		Version:   1,
		LineageID: newEngineID(),
		Type:      "script",
		Content:   json.RawMessage(`{"type":"script","script":"return 42;","output_property":"answer"}`),
		CreatedBy: sysUserID,
		UpdatedBy: sysUserID,
		CreatedAt: time.Now().UTC(),
		UpdatedAt: time.Now().UTC(),
	}
	if err := repository.NewNodeDefinitionRepository(db).Create(ctx, nd); err != nil {
		t.Fatalf("create node definition: %v", err)
	}

	wfID := createWorkflow(t, db, n1,
		`{"id":"`+n1+`","node_definition_id":"`+defID+`"}`,
	)

	mat := kernel.NewMaterializer(repository.NewNodeDefinitionRepository(db), testLimitsNode)
	loader := func(ctx context.Context, instanceID string) (*model.WorkflowContent, error) {
		inst, err := repository.NewInstanceRepository(db).GetByID(ctx, instanceID)
		if err != nil {
			return nil, err
		}
		def, err := repository.NewWorkflowDefinitionRepository(db).GetByID(ctx, inst.WorkflowDefinitionID)
		if err != nil {
			return nil, err
		}
		wc, err := model.ParseWorkflowContent(def.Content, testLimitsNode)
		if err != nil {
			return nil, err
		}
		return mat.Materialize(ctx, wc)
	}

	instances := repository.NewInstanceRepository(db)
	e := engine.NewEngine(instances, repository.NewParallelRepository(db),
		executor.NewExecutors(executor.Limits{}, nil, executor.Dependencies{}),
		executor.NewHookRunner(nil), model.DefaultLimits(), loader, sysUserID, model.LeanOptions{})

	instanceID := insertInstance(t, db, wfID, n1, map[string]any{})
	cur := runEngine(t, db, e, instanceID)
	if cur.Status != model.WorkflowFinished {
		t.Fatalf("status = %s, want finished (error %q)", cur.Status, cur.Error)
	}
	got := instanceContext(t, db, instanceID)
	if got["answer"] != float64(42) {
		t.Errorf("context = %v, want answer=42 from the referenced node definition", got)
	}
}
