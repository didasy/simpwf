package service_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/simpwf/workflow-engine/internal/workflow/model"
	"github.com/simpwf/workflow-engine/internal/workflow/repository"
	"github.com/simpwf/workflow-engine/internal/workflow/service"
)

func TestStatisticsServicePassthrough(t *testing.T) {
	db := setupSvcDB(t)
	ctx := context.Background()

	defID := svcCreateWorkflowRaw(t, db, `{"start_node_id":"n1","nodes":[{"id":"n1","type":"script","name":"n1","script":"return 1;"}]}`)

	repo := repository.NewInstanceRepository(db)
	svc := service.NewStatisticsService(repo)

	frame := model.NewFrame("n1")
	frameRaw, _ := frame.JSON()
	inst := model.WorkflowInstance{
		ID:                   "11111111-1111-7111-8111-111111111111",
		WorkflowDefinitionID: defID,
		Status:               model.WorkflowWaiting,
		Frame:                frameRaw,
		Context:              json.RawMessage(`{}`),
		Counters:             json.RawMessage(`{}`),
		CreatedBy:            svcSysUserID,
		UpdatedBy:            svcSysUserID,
		CreatedAt:            time.Now().UTC(),
		UpdatedAt:            time.Now().UTC(),
	}
	if err := repo.Insert(ctx, inst); err != nil {
		t.Fatalf("insert: %v", err)
	}

	sum, err := svc.Summary(ctx, repository.StatisticsQuery{})
	if err != nil {
		t.Fatalf("Summary() error = %v", err)
	}
	if sum.TotalRuns != 1 {
		t.Errorf("TotalRuns = %d, want 1", sum.TotalRuns)
	}
	if sum.SuccessRate != nil || sum.AverageDurationMS != nil {
		t.Errorf("non-terminal rate/avg = %v/%v, want nil/nil", sum.SuccessRate, sum.AverageDurationMS)
	}
}
