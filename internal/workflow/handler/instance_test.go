package handler

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/simpwf/workflow-engine/internal/workflow/model"
	"github.com/simpwf/workflow-engine/internal/workflow/repository"
	"github.com/simpwf/workflow-engine/internal/workflow/service"
)

const (
	instanceID   = "11111111-1111-7111-8111-111111111111"
	wfDefID      = "22222222-2222-7222-8222-222222222222"
	occurrenceID = "33333333-3333-7333-8333-333333333333"
)

// fakeInstanceSvc is an in-memory InstanceService for handler tests.
type fakeInstanceSvc struct {
	createInst   model.WorkflowInstance
	createErr    error
	statusInst   *model.WorkflowInstance
	statusErr    error
	detail       *service.StatusDetail
	contextInst  *model.WorkflowInstance
	updateCtxReq *service.UpdateContext
	updateCtxRes *model.WorkflowInstance
	updateCtxErr error
	delivery     *model.InputDelivery
	deliveryErr  error
	nodeDebug    *service.NodeDebugDetail
	nodeDebugErr error
	pauseRes     *service.ControlResult
	pauseErr     error
	resumeRes    *service.ControlResult
	resumeErr    error
	stopRes      *service.ControlResult
	stopErr      error
	rollbackRes  *service.RollbackResult
	rollbackErr  error
	listQuery    repository.InstanceListQuery
	listItems    []model.WorkflowInstance
	listTotal    int64
	listErr      error
}

func (f *fakeInstanceSvc) Create(_ context.Context, _ service.CreateInstance) (model.WorkflowInstance, error) {
	return f.createInst, f.createErr
}
func (f *fakeInstanceSvc) GetStatus(_ context.Context, _ string) (*model.WorkflowInstance, error) {
	return f.statusInst, f.statusErr
}
func (f *fakeInstanceSvc) GetStatusDetail(_ context.Context, _ string) (*service.StatusDetail, error) {
	return f.detail, f.statusErr
}
func (f *fakeInstanceSvc) GetContext(_ context.Context, _ string) (*model.WorkflowInstance, error) {
	return f.contextInst, f.statusErr
}
func (f *fakeInstanceSvc) UpdateContext(_ context.Context, req service.UpdateContext) (*model.WorkflowInstance, error) {
	f.updateCtxReq = &req
	return f.updateCtxRes, f.updateCtxErr
}
func (f *fakeInstanceSvc) DeliverInput(_ context.Context, _ service.DeliverInput) (*model.InputDelivery, error) {
	return f.delivery, f.deliveryErr
}
func (f *fakeInstanceSvc) NodeDebug(_ context.Context, _, _ string, _ int) (*service.NodeDebugDetail, error) {
	return f.nodeDebug, f.nodeDebugErr
}
func (f *fakeInstanceSvc) Pause(_ context.Context, _ string) (*service.ControlResult, error) {
	return f.pauseRes, f.pauseErr
}
func (f *fakeInstanceSvc) Resume(_ context.Context, _ string) (*service.ControlResult, error) {
	return f.resumeRes, f.resumeErr
}
func (f *fakeInstanceSvc) Stop(_ context.Context, _, _ string) (*service.ControlResult, error) {
	return f.stopRes, f.stopErr
}
func (f *fakeInstanceSvc) Rollback(_ context.Context, _ service.RollbackRequest) (*service.RollbackResult, error) {
	return f.rollbackRes, f.rollbackErr
}
func (f *fakeInstanceSvc) List(_ context.Context, q repository.InstanceListQuery) ([]model.WorkflowInstance, int64, error) {
	f.listQuery = q
	return f.listItems, f.listTotal, f.listErr
}

func TestInstanceCreateAccepted(t *testing.T) {
	r := NewRouter(Deps{Health: NewHealth(fakePinger{}), Instances: &fakeInstanceSvc{
		createInst: model.WorkflowInstance{ID: instanceID, Status: model.WorkflowWaiting},
	}})
	w := performJSON(r, http.MethodPost, "/v1/workflow/instance",
		`{"workflow_definition_id":"`+wfDefID+`"}`, nil)
	if w.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202 (%s)", w.Code, w.Body.String())
	}
	var resp CreateInstanceResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if resp.ID != instanceID || resp.Status != "waiting" {
		t.Errorf("resp = %+v", resp)
	}
}

func TestInstanceCreateInvalidBody(t *testing.T) {
	r := NewRouter(Deps{Health: NewHealth(fakePinger{}), Instances: &fakeInstanceSvc{}})
	w := performJSON(r, http.MethodPost, "/v1/workflow/instance", `not json`, nil)
	if w.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", w.Code)
	}
}

func TestInstanceStatus(t *testing.T) {
	now := time.Now().UTC()
	inst := model.WorkflowInstance{
		ID: instanceID, WorkflowDefinitionID: wfDefID,
		Status: model.WorkflowWaiting, WaitingReason: model.WaitingReasonInput,
		CreatedBy: instanceID, UpdatedBy: instanceID,
		Frame:     json.RawMessage(`{"current_node_id":"x"}`),
		CreatedAt: now, UpdatedAt: now,
	}
	occ := occurrenceID
	detail := &service.StatusDetail{Instance: inst, CurrentNodeInstanceID: &occ, Attempt: 2}
	r := NewRouter(Deps{Health: NewHealth(fakePinger{}), Instances: &fakeInstanceSvc{detail: detail}})
	w := performJSON(r, http.MethodGet, "/v1/workflow/instance/"+instanceID+"/status", "", nil)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	var resp InstanceStatusResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if resp.Status != "waiting" || resp.WaitingReason == nil || *resp.WaitingReason != "input" {
		t.Errorf("resp = %+v", resp)
	}
	wantNodeInstance := instanceID + ":" + occurrenceID
	if resp.CurrentNodeInstanceID == nil || *resp.CurrentNodeInstanceID != wantNodeInstance {
		t.Errorf("current_node_instance_id = %v, want %s", resp.CurrentNodeInstanceID, wantNodeInstance)
	}
	if resp.Attempt != 2 {
		t.Errorf("attempt = %d, want 2", resp.Attempt)
	}
	if resp.CreatedBy != instanceID || resp.UpdatedBy != instanceID {
		t.Errorf("audit actors = %q/%q, want %q", resp.CreatedBy, resp.UpdatedBy, instanceID)
	}
}

func TestInstanceStatusNotFound(t *testing.T) {
	r := NewRouter(Deps{Health: NewHealth(fakePinger{}), Instances: &fakeInstanceSvc{
		statusErr: model.ErrNotFound,
	}})
	w := performJSON(r, http.MethodGet, "/v1/workflow/instance/"+instanceID+"/status", "", nil)
	if w.Code != http.StatusNotFound {
		t.Errorf("status = %d, want 404", w.Code)
	}
}

func TestInstanceStatusNodesMap(t *testing.T) {
	now := time.Now().UTC()
	inst := model.WorkflowInstance{
		ID: instanceID, WorkflowDefinitionID: wfDefID,
		Status:    model.WorkflowPaused,
		CreatedBy: instanceID, UpdatedBy: instanceID,
		Frame:     json.RawMessage(`{"current_node_id":"node-1"}`),
		CreatedAt: now, UpdatedAt: now,
	}
	occ := occurrenceID
	attempt := 1
	detail := &service.StatusDetail{
		Instance:              inst,
		CurrentNodeInstanceID: &occ,
		Attempt:               2,
		Nodes: map[string]service.NodeOccurrence{
			"node-1": {OccurrenceID: &occ, Status: "finished", Attempt: &attempt, Rollbackable: true},
			"node-2": {Status: "not_started", Rollbackable: false},
		},
	}
	r := NewRouter(Deps{Health: NewHealth(fakePinger{}), Instances: &fakeInstanceSvc{detail: detail}})
	w := performJSON(r, http.MethodGet, "/v1/workflow/instance/"+instanceID+"/status", "", nil)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (%s)", w.Code, w.Body.String())
	}
	var resp InstanceStatusResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	e1, ok := resp.Nodes["node-1"]
	if !ok {
		t.Fatalf("nodes missing node-1: %v", resp.Nodes)
	}
	if e1.OccurrenceID == nil || *e1.OccurrenceID != occurrenceID {
		t.Errorf("node-1 occurrence = %v, want %s", e1.OccurrenceID, occurrenceID)
	}
	if e1.Status != "finished" || e1.Attempt == nil || *e1.Attempt != 1 || !e1.Rollbackable {
		t.Errorf("node-1 = %+v, want finished/1/true", e1)
	}
	// not_started entries render occurrence_id and attempt as JSON null.
	var raw map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &raw); err != nil {
		t.Fatal(err)
	}
	nodes, ok := raw["nodes"].(map[string]any)
	if !ok {
		t.Fatalf("nodes missing in raw body: %v", raw)
	}
	n2, ok := nodes["node-2"].(map[string]any)
	if !ok {
		t.Fatalf("nodes.node-2 missing: %v", nodes)
	}
	if n2["occurrence_id"] != nil {
		t.Errorf("node-2 occurrence_id = %v, want null", n2["occurrence_id"])
	}
	if n2["attempt"] != nil {
		t.Errorf("node-2 attempt = %v, want null", n2["attempt"])
	}
	if n2["status"] != "not_started" || n2["rollbackable"] != false {
		t.Errorf("node-2 = %v, want not_started/false", n2)
	}
}

func TestInstanceContext(t *testing.T) {
	inst := model.WorkflowInstance{ID: instanceID, Context: json.RawMessage(`{"x":1}`)}
	r := NewRouter(Deps{Health: NewHealth(fakePinger{}), Instances: &fakeInstanceSvc{contextInst: &inst}})
	w := performJSON(r, http.MethodGet, "/v1/workflow/instance/"+instanceID+"/context", "", nil)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	var resp InstanceContextResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if resp.ID != instanceID || string(resp.Context) != `{"x":1}` {
		t.Errorf("resp = %+v", resp)
	}
}

func TestInstanceUpdateContext(t *testing.T) {
	inst := model.WorkflowInstance{ID: instanceID, Context: json.RawMessage(`{"new":2}`)}
	svc := &fakeInstanceSvc{updateCtxRes: &inst}
	r := NewRouter(Deps{Health: NewHealth(fakePinger{}), Instances: svc})
	w := performJSON(r, http.MethodPut, "/v1/workflow/instance/"+instanceID+"/context",
		`{"new":2}`, map[string]string{"X-Context-Update-Reason": "urgent fix"})
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (%s)", w.Code, w.Body.String())
	}
	var resp InstanceContextResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if resp.ID != instanceID || string(resp.Context) != `{"new":2}` {
		t.Errorf("resp = %+v", resp)
	}
	if svc.updateCtxReq == nil {
		t.Fatal("UpdateContext request not captured")
	}
	if svc.updateCtxReq.InstanceID != instanceID || string(svc.updateCtxReq.Context) != `{"new":2}` || svc.updateCtxReq.Reason != "urgent fix" {
		t.Errorf("req = %+v, want instance/body/reason forwarded", svc.updateCtxReq)
	}
}

func TestInstanceUpdateContextMalformedBody(t *testing.T) {
	r := NewRouter(Deps{Health: NewHealth(fakePinger{}), Instances: &fakeInstanceSvc{}})
	for _, body := range []string{"not json", ""} {
		w := performJSON(r, http.MethodPut, "/v1/workflow/instance/"+instanceID+"/context", body, nil)
		if w.Code != http.StatusBadRequest {
			t.Errorf("body %q: status = %d, want 400", body, w.Code)
		}
	}
}

func TestInstanceUpdateContextErrors(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want int
	}{
		{"invalid", model.ErrInvalid, http.StatusUnprocessableEntity},
		{"conflict", model.ErrConflict, http.StatusConflict},
		{"not found", model.ErrNotFound, http.StatusNotFound},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := NewRouter(Deps{Health: NewHealth(fakePinger{}), Instances: &fakeInstanceSvc{updateCtxErr: tc.err}})
			w := performJSON(r, http.MethodPut, "/v1/workflow/instance/"+instanceID+"/context", `{"x":1}`, nil)
			if w.Code != tc.want {
				t.Errorf("status = %d, want %d (%s)", w.Code, tc.want, w.Body.String())
			}
		})
	}
}

func TestInstanceInputAccepted(t *testing.T) {
	r := NewRouter(Deps{Health: NewHealth(fakePinger{}), Instances: &fakeInstanceSvc{
		delivery: &model.InputDelivery{Accepted: true},
	}})
	w := performJSON(r, http.MethodPut, "/v1/workflow/instance/"+instanceID+"/input",
		`{"ok":true}`, map[string]string{"Idempotency-Key": "key-1"})
	if w.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202 (%s)", w.Code, w.Body.String())
	}
	var resp InputDeliveryResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if !resp.Accepted {
		t.Errorf("resp = %+v, want accepted", resp)
	}
}

func TestInstanceInputRejected(t *testing.T) {
	msg := "Webhook failed!"
	r := NewRouter(Deps{Health: NewHealth(fakePinger{}), Instances: &fakeInstanceSvc{
		delivery: &model.InputDelivery{Accepted: false, Error: msg},
	}})
	w := performJSON(r, http.MethodPut, "/v1/workflow/instance/"+instanceID+"/input",
		`{"ok":false}`, map[string]string{"Idempotency-Key": "key-1"})
	if w.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want 422", w.Code)
	}
	var p Problem
	if err := json.Unmarshal(w.Body.Bytes(), &p); err != nil {
		t.Fatal(err)
	}
	if p.Detail != msg {
		t.Errorf("detail = %q, want %q", p.Detail, msg)
	}
}

func TestInstanceInputInvalidKey(t *testing.T) {
	r := NewRouter(Deps{Health: NewHealth(fakePinger{}), Instances: &fakeInstanceSvc{
		deliveryErr: model.ErrInvalid,
	}})
	w := performJSON(r, http.MethodPut, "/v1/workflow/instance/"+instanceID+"/input",
		`{}`, nil)
	if w.Code != http.StatusUnprocessableEntity {
		t.Errorf("status = %d, want 422", w.Code)
	}
}

func TestNodeDebugDetail(t *testing.T) {
	attempt := 1
	started := time.Now().UTC()
	finished := started.Add(50 * time.Millisecond)
	duration := int64(50)
	svc := &fakeInstanceSvc{nodeDebug: &service.NodeDebugDetail{
		OccurrenceID:           occurrenceID,
		SourceNodeDefinitionID: wfDefID,
		Name:                   "compute",
		Type:                   "script",
		SelectedAttempt:        &attempt,
		LatestAttempt:          &attempt,
		AttemptCount:           1,
		Status:                 "finished",
		ContextBefore:          json.RawMessage(`{"x":1}`),
		ContextAfter:           json.RawMessage(`{"x":1,"out":2}`),
		Input:                  json.RawMessage(`null`),
		Output:                 json.RawMessage(`2`),
		DurationMS:             &duration,
		StartedAt:              &started,
		FinishedAt:             &finished,
		CreatedAt:              started,
		UpdatedAt:              finished,
	}}
	r := NewRouter(Deps{Health: NewHealth(fakePinger{}), Instances: svc})
	w := performJSON(r, http.MethodGet, "/v1/workflow/instance/"+instanceID+"/status/node/"+occurrenceID+"?attempt=1", "", nil)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (%s)", w.Code, w.Body.String())
	}
	var resp NodeDebugResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if resp.OccurrenceID != occurrenceID || resp.Name != "compute" || resp.Type != "script" {
		t.Errorf("resp = %+v", resp)
	}
	if resp.SelectedAttempt == nil || *resp.SelectedAttempt != 1 || resp.AttemptCount != 1 {
		t.Errorf("resp = %+v, want selected 1, count 1", resp)
	}
	if resp.Status != "finished" || string(resp.Output) != `2` {
		t.Errorf("resp = %+v", resp)
	}
	if resp.DurationMS == nil || *resp.DurationMS != 50 {
		t.Errorf("resp = %+v, want duration 50", resp)
	}
}

func TestNodeDebugNotStarted(t *testing.T) {
	svc := &fakeInstanceSvc{nodeDebug: &service.NodeDebugDetail{
		OccurrenceID: wfDefID, Name: "later", Type: "script",
		Status: "not_started",
	}}
	r := NewRouter(Deps{Health: NewHealth(fakePinger{}), Instances: svc})
	w := performJSON(r, http.MethodGet, "/v1/workflow/instance/"+instanceID+"/status/node/"+wfDefID, "", nil)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	var resp NodeDebugResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if resp.Status != "not_started" || resp.AttemptCount != 0 {
		t.Errorf("resp = %+v", resp)
	}
}

func TestNodeDebugAttemptParamInvalid(t *testing.T) {
	r := NewRouter(Deps{Health: NewHealth(fakePinger{}), Instances: &fakeInstanceSvc{}})
	for _, q := range []string{"?attempt=0", "?attempt=-1", "?attempt=abc"} {
		w := performJSON(r, http.MethodGet, "/v1/workflow/instance/"+instanceID+"/status/node/"+occurrenceID+q, "", nil)
		if w.Code != http.StatusBadRequest {
			t.Errorf("attempt=%q status = %d, want 400", q, w.Code)
		}
	}
}

func TestNodeDebugNotFound(t *testing.T) {
	r := NewRouter(Deps{Health: NewHealth(fakePinger{}), Instances: &fakeInstanceSvc{
		nodeDebugErr: model.ErrNotFound,
	}})
	w := performJSON(r, http.MethodGet, "/v1/workflow/instance/"+instanceID+"/status/node/"+occurrenceID, "", nil)
	if w.Code != http.StatusNotFound {
		t.Errorf("status = %d, want 404", w.Code)
	}
}

func TestInstancePauseImmediate(t *testing.T) {
	svc := &fakeInstanceSvc{pauseRes: &service.ControlResult{Status: model.WorkflowPaused}}
	r := NewRouter(Deps{Health: NewHealth(fakePinger{}), Instances: svc})
	w := performJSON(r, http.MethodPost, "/v1/workflow/instance/"+instanceID+"/pause", "", nil)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (%s)", w.Code, w.Body.String())
	}
	var resp PauseResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if resp.Status != "paused" || resp.PauseRequested {
		t.Errorf("resp = %+v", resp)
	}
}

func TestInstancePauseDeferred(t *testing.T) {
	svc := &fakeInstanceSvc{pauseRes: &service.ControlResult{Status: model.WorkflowRunning, PauseRequested: true}}
	r := NewRouter(Deps{Health: NewHealth(fakePinger{}), Instances: svc})
	w := performJSON(r, http.MethodPost, "/v1/workflow/instance/"+instanceID+"/pause", "", nil)
	if w.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202", w.Code)
	}
	var resp PauseResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if !resp.PauseRequested {
		t.Errorf("resp = %+v, want pause_requested", resp)
	}
}

func TestInstanceResume(t *testing.T) {
	svc := &fakeInstanceSvc{resumeRes: &service.ControlResult{Status: model.WorkflowWaiting}}
	r := NewRouter(Deps{Health: NewHealth(fakePinger{}), Instances: svc})
	w := performJSON(r, http.MethodPost, "/v1/workflow/instance/"+instanceID+"/resume", "", nil)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	var resp ResumeResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if resp.Status != "waiting" {
		t.Errorf("resp = %+v", resp)
	}
}

func TestInstanceStop(t *testing.T) {
	svc := &fakeInstanceSvc{stopRes: &service.ControlResult{Status: model.WorkflowStopped, TerminationPending: true}}
	r := NewRouter(Deps{Health: NewHealth(fakePinger{}), Instances: svc})
	w := performJSON(r, http.MethodPost, "/v1/workflow/instance/"+instanceID+"/stop", "", nil)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	var resp StopResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if resp.Status != "stopped" || !resp.TerminationPending {
		t.Errorf("resp = %+v", resp)
	}
}

func TestInstanceControlsConflict(t *testing.T) {
	for _, path := range []string{
		"/v1/workflow/instance/" + instanceID + "/pause",
		"/v1/workflow/instance/" + instanceID + "/resume",
		"/v1/workflow/instance/" + instanceID + "/stop",
	} {
		svc := &fakeInstanceSvc{
			pauseErr: model.ErrConflict, resumeErr: model.ErrConflict, stopErr: model.ErrConflict,
		}
		r := NewRouter(Deps{Health: NewHealth(fakePinger{}), Instances: svc})
		w := performJSON(r, http.MethodPost, path, "", nil)
		if w.Code != http.StatusConflict {
			t.Errorf("%s status = %d, want 409", path, w.Code)
		}
	}
}

func TestInstanceControlsNotFound(t *testing.T) {
	for _, path := range []string{
		"/v1/workflow/instance/" + instanceID + "/pause",
		"/v1/workflow/instance/" + instanceID + "/resume",
		"/v1/workflow/instance/" + instanceID + "/stop",
	} {
		svc := &fakeInstanceSvc{
			pauseErr: model.ErrNotFound, resumeErr: model.ErrNotFound, stopErr: model.ErrNotFound,
		}
		r := NewRouter(Deps{Health: NewHealth(fakePinger{}), Instances: svc})
		w := performJSON(r, http.MethodPost, path, "", nil)
		if w.Code != http.StatusNotFound {
			t.Errorf("%s status = %d, want 404", path, w.Code)
		}
	}
}

func TestInstanceRollback(t *testing.T) {
	svc := &fakeInstanceSvc{rollbackRes: &service.RollbackResult{Status: model.WorkflowPaused, CurrentNodeID: occurrenceID}}
	r := NewRouter(Deps{Health: NewHealth(fakePinger{}), Instances: svc})
	w := performJSON(r, http.MethodPost, "/v1/workflow/instance/"+instanceID+"/rollback",
		`{"target_occurrence_id":"`+occurrenceID+`","reason":"retry"}`, nil)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (%s)", w.Code, w.Body.String())
	}
	var resp RollbackResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if resp.Status != "paused" || resp.CurrentNodeID != occurrenceID {
		t.Errorf("resp = %+v", resp)
	}
}

func TestInstanceRollbackBadBody(t *testing.T) {
	r := NewRouter(Deps{Health: NewHealth(fakePinger{}), Instances: &fakeInstanceSvc{}})
	for _, body := range []string{"not json", "", `{}`, `{"reason":"x"}`} {
		w := performJSON(r, http.MethodPost, "/v1/workflow/instance/"+instanceID+"/rollback", body, nil)
		if w.Code != http.StatusBadRequest {
			t.Errorf("body %q: status = %d, want 400", body, w.Code)
		}
	}
}

func TestInstanceRollbackErrors(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want int
	}{
		{"not found", model.ErrNotFound, http.StatusNotFound},
		{"conflict", model.ErrConflict, http.StatusConflict},
		{"invalid", model.ErrInvalid, http.StatusUnprocessableEntity},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := NewRouter(Deps{Health: NewHealth(fakePinger{}), Instances: &fakeInstanceSvc{rollbackErr: tc.err}})
			w := performJSON(r, http.MethodPost, "/v1/workflow/instance/"+instanceID+"/rollback",
				`{"target_occurrence_id":"`+occurrenceID+`"}`, nil)
			if w.Code != tc.want {
				t.Errorf("status = %d, want %d (%s)", w.Code, tc.want, w.Body.String())
			}
		})
	}
}

func TestInstanceRollbackSupersedeSucceeded(t *testing.T) {
	svc := &fakeInstanceSvc{rollbackRes: &service.RollbackResult{Status: model.WorkflowPaused, CurrentNodeID: occurrenceID}}
	r := NewRouter(Deps{Health: NewHealth(fakePinger{}), Instances: svc})
	w := performJSON(r, http.MethodPost, "/v1/workflow/instance/"+instanceID+"/rollback",
		`{"target_occurrence_id":"`+occurrenceID+`"}`, nil)
	if w.Code != http.StatusOK {
		t.Errorf("status = %d, want 200 (%s)", w.Code, w.Body.String())
	}
}

func TestInstanceListDelegatesParsedQuery(t *testing.T) {
	svc := &fakeInstanceSvc{listTotal: 1}
	r := NewRouter(Deps{Health: NewHealth(fakePinger{}), Instances: svc})
	w := performJSON(r, http.MethodGet, "/v1/workflow/instance?page=2&per_page=10&order=-status"+
		"&id="+instanceID+"&workflow_definition_id="+wfDefID+"&status=waiting&status=failed", "", nil)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (%s)", w.Code, w.Body.String())
	}
	q := svc.listQuery
	if q.Page != 2 || q.PerPage != 10 || q.Order != "-status" {
		t.Errorf("query page/per_page/order = %d/%d/%q, want 2/10/-status", q.Page, q.PerPage, q.Order)
	}
	if len(q.IDs) != 1 || q.IDs[0] != instanceID {
		t.Errorf("query IDs = %v, want [%s]", q.IDs, instanceID)
	}
	if q.WorkflowDefinitionID != wfDefID {
		t.Errorf("query workflow_definition_id = %q, want %q", q.WorkflowDefinitionID, wfDefID)
	}
	if len(q.Statuses) != 2 || q.Statuses[0] != "waiting" || q.Statuses[1] != "failed" {
		t.Errorf("query statuses = %v, want [waiting failed]", q.Statuses)
	}
}

func TestInstanceListPageMetadata(t *testing.T) {
	svc := &fakeInstanceSvc{
		listItems: []model.WorkflowInstance{{ID: instanceID, Status: model.WorkflowWaiting}},
		listTotal: 42,
	}
	r := NewRouter(Deps{Health: NewHealth(fakePinger{}), Instances: svc})
	w := performJSON(r, http.MethodGet, "/v1/workflow/instance?page=2&per_page=10", "", nil)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	var resp ListResponse[InstanceSummaryResponse]
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if resp.Page != 2 || resp.PerPage != 10 || resp.Total != 42 || resp.TotalPages != 5 {
		t.Errorf("envelope = page %d per_page %d total %d total_pages %d, want 2/10/42/5",
			resp.Page, resp.PerPage, resp.Total, resp.TotalPages)
	}
	if len(resp.Items) != 1 || resp.Items[0].ID != instanceID {
		t.Errorf("items = %+v", resp.Items)
	}
}

func TestInstanceListCompactSummary(t *testing.T) {
	now := time.Now().UTC()
	inst := model.WorkflowInstance{
		ID:                   instanceID,
		WorkflowDefinitionID: wfDefID,
		Status:               model.WorkflowPaused,
		WaitingReason:        model.WaitingReasonInput,
		PauseRequested:       true,
		TerminationPending:   true,
		Error:                "boom",
		StartedAt:            &now,
		FinishedAt:           &now,
		CreatedBy:            instanceID,
		UpdatedBy:            wfDefID,
		CreatedAt:            now,
		UpdatedAt:            now,
		Frame:                json.RawMessage(`{"current_node_id":"x"}`),
		Context:              json.RawMessage(`{"secret":1}`),
		Counters:             json.RawMessage(`{"finished":2}`),
		Revision:             7,
		LeasedBy:             "worker-1",
	}
	svc := &fakeInstanceSvc{listItems: []model.WorkflowInstance{inst}, listTotal: 1}
	r := NewRouter(Deps{Health: NewHealth(fakePinger{}), Instances: svc})
	w := performJSON(r, http.MethodGet, "/v1/workflow/instance", "", nil)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	var resp ListResponse[InstanceSummaryResponse]
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if len(resp.Items) != 1 {
		t.Fatalf("items = %d, want 1", len(resp.Items))
	}
	item := resp.Items[0]
	if item.ID != instanceID || item.WorkflowDefinitionID != wfDefID || item.Status != "paused" {
		t.Errorf("identity fields = %+v", item)
	}
	if item.WaitingReason == nil || *item.WaitingReason != "input" {
		t.Errorf("waiting_reason = %v, want input", item.WaitingReason)
	}
	if !item.PauseRequested || !item.TerminationPending {
		t.Errorf("flags = pause %v termination %v, want true/true", item.PauseRequested, item.TerminationPending)
	}
	if item.Error == nil || *item.Error != "boom" {
		t.Errorf("error = %v, want boom", item.Error)
	}
	if item.StartedAt == nil || item.FinishedAt == nil || item.CreatedBy != instanceID || item.UpdatedBy != wfDefID {
		t.Errorf("timestamps/audit = %+v", item)
	}

	// The summary must not leak runtime internals.
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(w.Body.Bytes(), &raw); err != nil {
		t.Fatal(err)
	}
	var items []map[string]json.RawMessage
	if err := json.Unmarshal(raw["items"], &items); err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{
		"frame", "context", "counters", "revision", "leased_by", "lease_expiry",
		"current_group_id", "current_node_id",
	} {
		if _, ok := items[0][forbidden]; ok {
			t.Errorf("summary leaks %q: %s", forbidden, w.Body.String())
		}
	}
}

func TestInstanceListEmptyResults(t *testing.T) {
	svc := &fakeInstanceSvc{listTotal: 0}
	r := NewRouter(Deps{Health: NewHealth(fakePinger{}), Instances: svc})
	w := performJSON(r, http.MethodGet, "/v1/workflow/instance?status=stopped", "", nil)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(w.Body.Bytes(), &raw); err != nil {
		t.Fatal(err)
	}
	if string(raw["items"]) != "[]" {
		t.Errorf("items = %s, want []", raw["items"])
	}
}

func TestInstanceListInvalidQuery(t *testing.T) {
	for _, suffix := range []string{"?page=0", "?per_page=201", "?order=frame", "?id=not-a-uuid", "?status=done"} {
		r := NewRouter(Deps{Health: NewHealth(fakePinger{}), Instances: &fakeInstanceSvc{}})
		w := performJSON(r, http.MethodGet, "/v1/workflow/instance"+suffix, "", nil)
		if w.Code != http.StatusBadRequest {
			t.Errorf("%s status = %d, want 400", suffix, w.Code)
		}
		if ct := w.Header().Get("Content-Type"); !strings.HasPrefix(ct, "application/problem+json") {
			t.Errorf("%s content-type = %q, want application/problem+json", suffix, ct)
		}
	}
}

func TestInstanceListServiceError(t *testing.T) {
	r := NewRouter(Deps{Health: NewHealth(fakePinger{}), Instances: &fakeInstanceSvc{
		listErr: errors.New("database unreachable"),
	}})
	w := performJSON(r, http.MethodGet, "/v1/workflow/instance", "", nil)
	if w.Code != http.StatusInternalServerError {
		t.Errorf("status = %d, want 500", w.Code)
	}
}
