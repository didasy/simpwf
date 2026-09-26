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

const (
	nodeDefID   = "11111111-1111-7111-8111-111111111111"
	nodeLineage = "99999999-9999-7999-8999-999999999999"
)

// fakeNodeSvc is an in-memory NodeDefinitionService for handler tests.
type fakeNodeSvc struct {
	createErr error
	getDef    model.NodeDefinition
	getErr    error
	items     []model.NodeDefinition
	total     int64
	listErr   error
	deleteErr error
}

func (f *fakeNodeSvc) Create(_ context.Context, _ service.CreateNodeDefinition) (model.NodeDefinition, error) {
	if f.createErr != nil {
		return model.NodeDefinition{}, f.createErr
	}
	now := time.Now().UTC()
	return model.NodeDefinition{
		ID: nodeDefID, Name: "transform", Version: 1, LineageID: nodeLineage,
		Type: "script", Content: json.RawMessage(`{"type":"script"}`),
		CreatedBy: "actor", UpdatedBy: "actor", CreatedAt: now, UpdatedAt: now,
	}, nil
}

func (f *fakeNodeSvc) Get(_ context.Context, _ string) (model.NodeDefinition, error) {
	return f.getDef, f.getErr
}

func (f *fakeNodeSvc) List(_ context.Context, _ repository.DefinitionListQuery) ([]model.NodeDefinition, int64, error) {
	return f.items, f.total, f.listErr
}

func (f *fakeNodeSvc) Delete(_ context.Context, _ string) error { return f.deleteErr }

// Schema mirrors the service: known types get their schema, unknown types
// degrade to nil so the handler emits a null schema.
func (f *fakeNodeSvc) Schema(nodeType string) json.RawMessage {
	schema, ok := model.NodeSchema(nodeType)
	if !ok {
		return nil
	}
	return schema
}

func performJSON(r *gin.Engine, method, path, body string, headers map[string]string) *httptest.ResponseRecorder {
	w := httptest.NewRecorder()
	var reader *bytes.Reader
	if body == "" {
		reader = bytes.NewReader(nil)
	} else {
		reader = bytes.NewReader([]byte(body))
	}
	req := httptest.NewRequest(method, path, reader)
	req.Header.Set("Content-Type", "application/json")
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	r.ServeHTTP(w, req)
	return w
}

func nodeRouter(f *fakeNodeSvc) *gin.Engine {
	return NewRouter(Deps{
		Health:          NewHealth(fakePinger{}),
		NodeDefinitions: f,
	})
}

func TestNodeDefinitionCreate(t *testing.T) {
	r := nodeRouter(&fakeNodeSvc{})
	body := `{"name":"transform","type":"script","content":{"type":"script","script":"return 1;"}}`
	w := performJSON(r, http.MethodPost, "/v1/node/definition", body, nil)
	if w.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201; body %s", w.Code, w.Body.String())
	}
	var resp NodeDefinitionResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if resp.ID != nodeDefID || resp.Version != 1 || resp.Name != "transform" {
		t.Errorf("response = %+v", resp)
	}
	if resp.LineageID == "" || resp.CreatedAt.IsZero() {
		t.Errorf("response missing lineage/timestamps: %+v", resp)
	}
}

func TestNodeDefinitionCreateRejectsMalformedBody(t *testing.T) {
	r := nodeRouter(&fakeNodeSvc{})
	w := performJSON(r, http.MethodPost, "/v1/node/definition", `{not-json`, nil)
	if w.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", w.Code)
	}
}

func TestNodeDefinitionCreateUnprocessable(t *testing.T) {
	r := nodeRouter(&fakeNodeSvc{createErr: model.ErrInvalid})
	w := performJSON(r, http.MethodPost, "/v1/node/definition",
		`{"name":"x","type":"script","content":{"type":"script"}}`, nil)
	if w.Code != http.StatusUnprocessableEntity {
		t.Errorf("status = %d, want 422", w.Code)
	}
}

func TestNodeDefinitionCreateConflicts(t *testing.T) {
	// unknown previous version -> 404
	r := nodeRouter(&fakeNodeSvc{createErr: errors.Join(model.ErrNotFound, errors.New("nope"))})
	w := performJSON(r, http.MethodPost, "/v1/node/definition",
		`{"name":"x","type":"script","content":{"type":"script","script":"x"}}`, nil)
	if w.Code != http.StatusNotFound {
		t.Errorf("not found status = %d, want 404", w.Code)
	}

	// version race -> 409
	r = nodeRouter(&fakeNodeSvc{createErr: model.ErrConflict})
	w = performJSON(r, http.MethodPost, "/v1/node/definition",
		`{"name":"x","type":"script","content":{"type":"script","script":"x"}}`, nil)
	if w.Code != http.StatusConflict {
		t.Errorf("conflict status = %d, want 409", w.Code)
	}
}

func TestNodeDefinitionList(t *testing.T) {
	r := nodeRouter(&fakeNodeSvc{items: []model.NodeDefinition{{ID: nodeDefID, Name: "transform"}}, total: 1})
	w := performJSON(r, http.MethodGet, "/v1/node/definition", "", nil)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	var resp ListResponse[NodeDefinitionResponse]
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if resp.Page != 1 || resp.PerPage != 50 || resp.Total != 1 || resp.TotalPages != 1 || len(resp.Items) != 1 {
		t.Errorf("list envelope = %+v", resp)
	}
}

func TestNodeDefinitionListRejectsBadQuery(t *testing.T) {
	r := nodeRouter(&fakeNodeSvc{})
	w := performJSON(r, http.MethodGet, "/v1/node/definition?per_page=999", "", nil)
	if w.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", w.Code)
	}
}

func TestNodeDefinitionGet(t *testing.T) {
	r := nodeRouter(&fakeNodeSvc{
		getDef: model.NodeDefinition{ID: nodeDefID, Name: "transform", Version: 1, LineageID: nodeLineage},
	})
	w := performJSON(r, http.MethodGet, "/v1/node/definition/"+nodeDefID, "", nil)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	var resp NodeDefinitionResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if resp.ID != nodeDefID {
		t.Errorf("id = %q", resp.ID)
	}
}

func TestNodeDefinitionGetNotFoundAndBadID(t *testing.T) {
	r := nodeRouter(&fakeNodeSvc{getErr: model.ErrNotFound})
	w := performJSON(r, http.MethodGet, "/v1/node/definition/"+nodeDefID, "", nil)
	if w.Code != http.StatusNotFound {
		t.Errorf("not found status = %d, want 404", w.Code)
	}

	w = performJSON(r, http.MethodGet, "/v1/node/definition/not-a-uuid", "", nil)
	if w.Code != http.StatusBadRequest {
		t.Errorf("bad id status = %d, want 400", w.Code)
	}
}

func TestNodeDefinitionDelete(t *testing.T) {
	r := nodeRouter(&fakeNodeSvc{})
	w := performJSON(r, http.MethodDelete, "/v1/node/definition/"+nodeDefID, "", nil)
	if w.Code != http.StatusNoContent {
		t.Errorf("status = %d, want 204", w.Code)
	}
}

func TestNodeDefinitionDeleteConflict(t *testing.T) {
	r := nodeRouter(&fakeNodeSvc{deleteErr: model.ErrConflict})
	w := performJSON(r, http.MethodDelete, "/v1/node/definition/"+nodeDefID, "", nil)
	if w.Code != http.StatusConflict {
		t.Errorf("status = %d, want 409", w.Code)
	}
}

// TestNodeDefinitionCarriesSchema covers all three read paths: POST
// response, GET one, and LIST items each attach the type's node schema.
func TestNodeDefinitionCarriesSchema(t *testing.T) {
	svc := &fakeNodeSvc{
		getDef: model.NodeDefinition{ID: nodeDefID, Name: "transform", Version: 1, Type: "script"},
		items: []model.NodeDefinition{
			{ID: nodeDefID, Name: "transform", Type: "script"},
			{ID: nodeLineage, Name: "emit", Type: "output"},
		},
		total: 2,
	}
	r := nodeRouter(svc)

	post := performJSON(r, http.MethodPost, "/v1/node/definition",
		`{"name":"transform","type":"script","content":{"type":"script","script":"return 1;"}}`, nil)
	if post.Code != http.StatusCreated {
		t.Fatalf("create status = %d, want 201", post.Code)
	}
	assertScriptSchema(t, decodeNodeResponse(t, post.Body.Bytes()).Schema)

	one := performJSON(r, http.MethodGet, "/v1/node/definition/"+nodeDefID, "", nil)
	if one.Code != http.StatusOK {
		t.Fatalf("get status = %d, want 200", one.Code)
	}
	assertScriptSchema(t, decodeNodeResponse(t, one.Body.Bytes()).Schema)

	list := performJSON(r, http.MethodGet, "/v1/node/definition", "", nil)
	if list.Code != http.StatusOK {
		t.Fatalf("list status = %d, want 200", list.Code)
	}
	var listResp ListResponse[NodeDefinitionResponse]
	if err := json.Unmarshal(list.Body.Bytes(), &listResp); err != nil {
		t.Fatal(err)
	}
	if len(listResp.Items) != 2 {
		t.Fatalf("list has %d items, want 2", len(listResp.Items))
	}
	assertScriptSchema(t, listResp.Items[0].Schema)
	if len(listResp.Items[1].Schema) == 0 {
		t.Error("list item 1 missing schema for type output")
	}
}

// TestNodeDefinitionUnknownTypeSchemaIsNull covers the degraded read: an
// unknown type yields a null schema and still a 200.
func TestNodeDefinitionUnknownTypeSchemaIsNull(t *testing.T) {
	r := nodeRouter(&fakeNodeSvc{
		getDef: model.NodeDefinition{ID: nodeDefID, Name: "mystery", Type: "not_a_registered_type"},
	})
	w := performJSON(r, http.MethodGet, "/v1/node/definition/"+nodeDefID, "", nil)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	if !bytes.Contains(w.Body.Bytes(), []byte(`"schema":null`)) {
		t.Errorf("body does not carry a null schema: %s", w.Body.String())
	}
}

func decodeNodeResponse(t *testing.T, body []byte) NodeDefinitionResponse {
	t.Helper()
	var resp NodeDefinitionResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		t.Fatal(err)
	}
	return resp
}

// assertScriptSchema checks the served schema is the script node schema and
// not an empty object.
func assertScriptSchema(t *testing.T, raw json.RawMessage) {
	t.Helper()
	if len(raw) == 0 {
		t.Fatal("schema is empty, want the script node schema")
	}
	var doc map[string]any
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("schema is not valid JSON: %v", err)
	}
	props, _ := doc["properties"].(map[string]any)
	typeProp, _ := props["type"].(map[string]any)
	if typeProp["const"] != "script" {
		t.Errorf("schema properties.type.const = %v, want script", typeProp["const"])
	}
}
