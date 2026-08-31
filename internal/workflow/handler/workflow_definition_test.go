package handler

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/simpwf/workflow-engine/internal/workflow/model"
	"github.com/simpwf/workflow-engine/internal/workflow/repository"
	"github.com/simpwf/workflow-engine/internal/workflow/service"
)

const workflowDefID = "11111111-1111-7111-8111-111111111111"

// fakeWorkflowSvc is an in-memory WorkflowDefinitionService for handler tests.
type fakeWorkflowSvc struct {
	createErr error
	getDef    model.WorkflowDefinition
	getErr    error
	items     []model.WorkflowDefinition
	total     int64
	listErr   error
	deleteErr error
}

func (f *fakeWorkflowSvc) Create(_ context.Context, _ service.CreateWorkflowDefinition) (model.WorkflowDefinition, error) {
	if f.createErr != nil {
		return model.WorkflowDefinition{}, f.createErr
	}
	now := time.Now().UTC()
	return model.WorkflowDefinition{
		ID: workflowDefID, Name: "flow", Version: 1, LineageID: nodeLineage,
		Content:   json.RawMessage(`{"start_node_id":"x"}`),
		CreatedBy: "actor", UpdatedBy: "actor", CreatedAt: now, UpdatedAt: now,
	}, nil
}

func (f *fakeWorkflowSvc) Get(_ context.Context, _ string) (model.WorkflowDefinition, error) {
	return f.getDef, f.getErr
}

func (f *fakeWorkflowSvc) List(_ context.Context, _ repository.DefinitionListQuery) ([]model.WorkflowDefinition, int64, error) {
	return f.items, f.total, f.listErr
}

func (f *fakeWorkflowSvc) Delete(_ context.Context, _ string) error { return f.deleteErr }

func (f *fakeWorkflowSvc) Materialize(_ context.Context, wc *model.WorkflowContent) (*model.WorkflowContent, error) {
	return wc, nil
}

func workflowRouter(f *fakeWorkflowSvc) *gin.Engine {
	return NewRouter(Deps{
		Health:              NewHealth(fakePinger{}),
		WorkflowDefinitions: f,
	})
}

func TestWorkflowDefinitionCreate(t *testing.T) {
	r := workflowRouter(&fakeWorkflowSvc{})
	body := `{"name":"flow","content":{"start_node_id":"aaaaaaaa-aaaa-7aaa-8aaa-aaaaaaaaaaaa","nodes":[{"id":"aaaaaaaa-aaaa-7aaa-8aaa-aaaaaaaaaaaa","type":"script","script":"return 1;"}]}}`
	w := performJSON(r, http.MethodPost, "/v1/workflow/definition", body, nil)
	if w.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201; body %s", w.Code, w.Body.String())
	}
	var resp WorkflowDefinitionResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if resp.ID != workflowDefID || resp.Name != "flow" || resp.Version != 1 {
		t.Errorf("response = %+v", resp)
	}
}

func TestWorkflowDefinitionCreateErrors(t *testing.T) {
	// malformed body -> 400
	r := workflowRouter(&fakeWorkflowSvc{})
	w := performJSON(r, http.MethodPost, "/v1/workflow/definition", `{nope`, nil)
	if w.Code != http.StatusBadRequest {
		t.Errorf("malformed status = %d, want 400", w.Code)
	}

	// missing name/content -> 422
	w = performJSON(r, http.MethodPost, "/v1/workflow/definition", `{"name":""}`, nil)
	if w.Code != http.StatusUnprocessableEntity {
		t.Errorf("missing fields status = %d, want 422", w.Code)
	}

	// invalid content -> 422
	r = workflowRouter(&fakeWorkflowSvc{createErr: model.ErrInvalid})
	w = performJSON(r, http.MethodPost, "/v1/workflow/definition",
		`{"name":"flow","content":{"nodes":[]}}`, nil)
	if w.Code != http.StatusUnprocessableEntity {
		t.Errorf("invalid content status = %d, want 422", w.Code)
	}

	// unknown previous version -> 404
	r = workflowRouter(&fakeWorkflowSvc{createErr: errors.Join(model.ErrNotFound, errors.New("nope"))})
	w = performJSON(r, http.MethodPost, "/v1/workflow/definition",
		`{"name":"flow","content":{"start_node_id":"aaaaaaaa-aaaa-7aaa-8aaa-aaaaaaaaaaaa","nodes":[{"id":"aaaaaaaa-aaaa-7aaa-8aaa-aaaaaaaaaaaa","type":"script","script":"return 1;"}]}}`, nil)
	if w.Code != http.StatusNotFound {
		t.Errorf("unknown prev status = %d, want 404", w.Code)
	}

	// version race -> 409
	r = workflowRouter(&fakeWorkflowSvc{createErr: model.ErrConflict})
	w = performJSON(r, http.MethodPost, "/v1/workflow/definition",
		`{"name":"flow","content":{"start_node_id":"aaaaaaaa-aaaa-7aaa-8aaa-aaaaaaaaaaaa","nodes":[{"id":"aaaaaaaa-aaaa-7aaa-8aaa-aaaaaaaaaaaa","type":"script","script":"return 1;"}]}}`, nil)
	if w.Code != http.StatusConflict {
		t.Errorf("race status = %d, want 409", w.Code)
	}
}

func TestWorkflowDefinitionList(t *testing.T) {
	r := workflowRouter(&fakeWorkflowSvc{items: []model.WorkflowDefinition{{ID: workflowDefID, Name: "flow"}}, total: 1})
	w := performJSON(r, http.MethodGet, "/v1/workflow/definition?name=flow", "", nil)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	var resp ListResponse[WorkflowDefinitionResponse]
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if resp.Total != 1 || len(resp.Items) != 1 || resp.TotalPages != 1 {
		t.Errorf("envelope = %+v", resp)
	}
}

func TestWorkflowDefinitionGet(t *testing.T) {
	r := workflowRouter(&fakeWorkflowSvc{getDef: model.WorkflowDefinition{ID: workflowDefID, Name: "flow", Version: 1}})
	w := performJSON(r, http.MethodGet, "/v1/workflow/definition/"+workflowDefID, "", nil)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
}

func TestWorkflowDefinitionGetNotFound(t *testing.T) {
	r := workflowRouter(&fakeWorkflowSvc{getErr: model.ErrNotFound})
	w := performJSON(r, http.MethodGet, "/v1/workflow/definition/"+workflowDefID, "", nil)
	if w.Code != http.StatusNotFound {
		t.Errorf("status = %d, want 404", w.Code)
	}
}

func TestWorkflowDefinitionDelete(t *testing.T) {
	r := workflowRouter(&fakeWorkflowSvc{})
	w := performJSON(r, http.MethodDelete, "/v1/workflow/definition/"+workflowDefID, "", nil)
	if w.Code != http.StatusNoContent {
		t.Errorf("status = %d, want 204", w.Code)
	}

	r = workflowRouter(&fakeWorkflowSvc{deleteErr: model.ErrConflict})
	w = performJSON(r, http.MethodDelete, "/v1/workflow/definition/"+workflowDefID, "", nil)
	if w.Code != http.StatusConflict {
		t.Errorf("conflict status = %d, want 409", w.Code)
	}
}
