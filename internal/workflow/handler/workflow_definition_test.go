package handler

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
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

func (f *fakeWorkflowSvc) Create(_ context.Context, req service.CreateWorkflowDefinition) (model.WorkflowDefinition, error) {
	if f.createErr != nil {
		return model.WorkflowDefinition{}, f.createErr
	}
	now := time.Now().UTC()
	content := req.Content
	if content == nil {
		content = json.RawMessage(`{"start_node_id":"x"}`)
	}
	return model.WorkflowDefinition{
		ID: workflowDefID, Name: "flow", Version: 1, LineageID: nodeLineage,
		Content:   content,
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

// Schemas mirrors the service degrade contract: content that cannot be
// parsed yields the types it declares.
func (f *fakeWorkflowSvc) Schemas(_ context.Context, content json.RawMessage) map[string]json.RawMessage {
	used := map[string]bool{}
	var walk func(raws []json.RawMessage)
	walk = func(raws []json.RawMessage) {
		for _, raw := range raws {
			var node struct {
				Type  string            `json:"type"`
				Nodes []json.RawMessage `json:"nodes"`
			}
			if err := json.Unmarshal(raw, &node); err != nil {
				continue
			}
			if node.Type != "" {
				used[node.Type] = true
			}
			walk(node.Nodes)
		}
	}
	var doc struct {
		Nodes []json.RawMessage `json:"nodes"`
	}
	if err := json.Unmarshal(content, &doc); err == nil {
		walk(doc.Nodes)
	}
	out := map[string]json.RawMessage{}
	for nodeType := range used {
		if schema, ok := model.NodeSchema(nodeType); ok {
			out[nodeType] = schema
		}
	}
	return out
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

const twoTypeWorkflowContent = `{"start_node_id":"aaaaaaaa-aaaa-7aaa-8aaa-aaaaaaaaaaaa","nodes":[
  {"id":"aaaaaaaa-aaaa-7aaa-8aaa-aaaaaaaaaaaa","type":"script","script":"return 1;"},
  {"id":"bbbbbbbb-bbbb-7bbb-8bbb-bbbbbbbbbbbb","type":"output","channel":"redis","context_path":"x"}]}`

// TestWorkflowDefinitionCarriesOnlyUsedSchemas covers all three read paths:
// POST response, GET one, and LIST items each carry a schemas map holding
// only the node types the definition actually uses.
func TestWorkflowDefinitionCarriesOnlyUsedSchemas(t *testing.T) {
	svc := &fakeWorkflowSvc{
		getDef: model.WorkflowDefinition{ID: workflowDefID, Name: "flow", Version: 1, Content: json.RawMessage(twoTypeWorkflowContent)},
		items: []model.WorkflowDefinition{
			{ID: workflowDefID, Name: "flow", Content: json.RawMessage(twoTypeWorkflowContent)},
		},
		total: 1,
	}
	r := workflowRouter(svc)

	post := performJSON(r, http.MethodPost, "/v1/workflow/definition",
		`{"name":"flow","content":`+twoTypeWorkflowContent+`}`, nil)
	if post.Code != http.StatusCreated {
		t.Fatalf("create status = %d, want 201; body %s", post.Code, post.Body.String())
	}
	assertUsedSchemas(t, decodeWorkflowResponse(t, post.Body.Bytes()))

	one := performJSON(r, http.MethodGet, "/v1/workflow/definition/"+workflowDefID, "", nil)
	if one.Code != http.StatusOK {
		t.Fatalf("get status = %d, want 200", one.Code)
	}
	assertUsedSchemas(t, decodeWorkflowResponse(t, one.Body.Bytes()))

	list := performJSON(r, http.MethodGet, "/v1/workflow/definition", "", nil)
	if list.Code != http.StatusOK {
		t.Fatalf("list status = %d, want 200", list.Code)
	}
	var listResp ListResponse[WorkflowDefinitionResponse]
	if err := json.Unmarshal(list.Body.Bytes(), &listResp); err != nil {
		t.Fatal(err)
	}
	if len(listResp.Items) != 1 {
		t.Fatalf("list has %d items, want 1", len(listResp.Items))
	}
	assertUsedSchemas(t, listResp.Items[0])
}

// TestWorkflowDefinitionDegradedReadNever500s covers a stored definition
// whose content no longer parses: the read still succeeds and reports the
// declared types.
func TestWorkflowDefinitionDegradedReadNever500s(t *testing.T) {
	r := workflowRouter(&fakeWorkflowSvc{
		getDef: model.WorkflowDefinition{
			ID: workflowDefID, Name: "flow", Version: 1,
			// A script node with no script: rejected by the parser.
			Content: json.RawMessage(`{"start_node_id":"aaaaaaaa-aaaa-7aaa-8aaa-aaaaaaaaaaaa","nodes":[{"id":"aaaaaaaa-aaaa-7aaa-8aaa-aaaaaaaaaaaa","type":"script"}]}`),
		},
	})
	w := performJSON(r, http.MethodGet, "/v1/workflow/definition/"+workflowDefID, "", nil)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body %s", w.Code, w.Body.String())
	}
	resp := decodeWorkflowResponse(t, w.Body.Bytes())
	if _, ok := resp.Schemas["script"]; !ok {
		t.Errorf("schemas = %v, want the declared type script", resp.Schemas)
	}
}

// TestWorkflowDefinitionUnparseableContentStillOK covers stored content
// that no longer satisfies the parser: the read still succeeds and reports
// the types the content declares. A fresh recorder is used because a
// problem response elsewhere on this router aborts the shared writer.
func TestWorkflowDefinitionUnparseableContentStillOK(t *testing.T) {
	// A script node with no script: valid JSON the parser rejects.
	stale := json.RawMessage(`{"start_node_id":"aaaaaaaa-aaaa-7aaa-8aaa-aaaaaaaaaaaa","nodes":[{"id":"aaaaaaaa-aaaa-7aaa-8aaa-aaaaaaaaaaaa","type":"script"}]}`)
	r := workflowRouter(&fakeWorkflowSvc{
		getDef: model.WorkflowDefinition{ID: workflowDefID, Name: "flow", Version: 1, Content: stale},
	})
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/v1/workflow/definition/"+workflowDefID, bytes.NewReader(nil)))
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	var resp WorkflowDefinitionResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("response is not valid JSON: %v", err)
	}
	if _, ok := resp.Schemas["script"]; !ok {
		t.Errorf("schemas = %v, want the declared type script", resp.Schemas)
	}
}

func decodeWorkflowResponse(t *testing.T, body []byte) WorkflowDefinitionResponse {
	t.Helper()
	var resp WorkflowDefinitionResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		t.Fatal(err)
	}
	return resp
}

func assertUsedSchemas(t *testing.T, resp WorkflowDefinitionResponse) {
	t.Helper()
	if len(resp.Schemas) != 2 {
		t.Fatalf("schemas has %d entries (%v), want 2", len(resp.Schemas), workflowSchemaKeys(resp.Schemas))
	}
	for _, want := range []string{"script", "output"} {
		raw, ok := resp.Schemas[want]
		if !ok {
			t.Errorf("schemas missing %q", want)
			continue
		}
		var doc map[string]any
		if err := json.Unmarshal(raw, &doc); err != nil {
			t.Errorf("schema %q is not valid JSON: %v", want, err)
			continue
		}
		props, _ := doc["properties"].(map[string]any)
		typeProp, _ := props["type"].(map[string]any)
		if typeProp["const"] != want {
			t.Errorf("schema %q properties.type.const = %v", want, typeProp["const"])
		}
	}
	// The workflow uses no custom types, so none are invented.
	for _, unwanted := range []string{"input", "poller", "group", "conditions", "external_call"} {
		if _, present := resp.Schemas[unwanted]; present {
			t.Errorf("schemas included unused type %q", unwanted)
		}
	}
}

func workflowSchemaKeys(m map[string]json.RawMessage) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}
