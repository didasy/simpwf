package service_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/simpwf/workflow-engine/internal/workflow/auth"
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

	sum, err := svc.Summary(ctx, repository.StatisticsQuery{}, auth.Principal{})
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

func TestStatisticsServiceOwnerScoping(t *testing.T) {
	db := setupSvcDB(t)
	ctx := context.Background()

	defID := svcCreateWorkflowRaw(t, db, `{"start_node_id":"n1","nodes":[{"id":"n1","type":"script","name":"n1","script":"return 1;"}]}`)

	repo := repository.NewInstanceRepository(db)
	svc := service.NewStatisticsService(repo)

	const adaID = "44444444-4444-7444-8444-444444444444"
	const mossID = "22222222-2222-7222-8222-222222222222"
	mk := func(id, owner string) model.WorkflowInstance {
		frame := model.NewFrame("n1")
		frameRaw, _ := frame.JSON()
		return model.WorkflowInstance{
			ID:                   id,
			WorkflowDefinitionID: defID,
			Status:               model.WorkflowWaiting,
			Frame:                frameRaw,
			Context:              json.RawMessage(`{}`),
			Counters:             json.RawMessage(`{}`),
			CreatedBy:            owner,
			UpdatedBy:            owner,
			CreatedAt:            time.Now().UTC(),
			UpdatedAt:            time.Now().UTC(),
		}
	}
	for _, w := range []model.WorkflowInstance{
		mk("61111111-1111-7111-8111-111111111111", adaID),
		mk("62222222-2222-7222-8222-222222222222", adaID),
		mk("63333333-3333-7333-8333-333333333333", mossID),
	} {
		if err := repo.Insert(ctx, w); err != nil {
			t.Fatalf("insert: %v", err)
		}
	}

	cases := []struct {
		name      string
		principal auth.Principal
		want      int64
	}{
		{"owner sees only own runs", auth.Principal{UserID: adaID, Roles: []string{"finance"}}, 2},
		{"other owner sees only own runs", auth.Principal{UserID: mossID, Roles: []string{"finance"}}, 1},
		{"system principal sees all runs", auth.SystemPrincipal(svcSysUserID), 3},
		{"admin role sees all runs", auth.Principal{UserID: adaID, Roles: []string{"admin"}}, 3},
		{"empty principal sees all runs", auth.Principal{}, 3},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sum, err := svc.Summary(ctx, repository.StatisticsQuery{}, tc.principal)
			if err != nil {
				t.Fatalf("Summary() error = %v", err)
			}
			if sum.TotalRuns != tc.want {
				t.Errorf("TotalRuns = %d, want %d", sum.TotalRuns, tc.want)
			}
		})
	}
}
