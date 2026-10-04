package handler

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/simpwf/workflow-engine/internal/workflow/model"
	"github.com/simpwf/workflow-engine/internal/workflow/repository"
	"github.com/simpwf/workflow-engine/internal/workflow/service"
)

const scheduleID = "11111111-1111-7111-8111-111111111111"

// fakeScheduleSvc is an in-memory ScheduleService for handler tests.
type fakeScheduleSvc struct {
	createErr error
	getItem   model.CronSchedule
	getErr    error
	items     []model.CronSchedule
	total     int64
	listErr   error
	deleteErr error
	pauseErr  error
	resumeErr error
	lastPause service.ScheduleControl
}

func testSchedule() model.CronSchedule {
	now := time.Now().UTC()
	return model.CronSchedule{
		ID:                   scheduleID,
		WorkflowDefinitionID: "22222222-2222-7222-8222-222222222222",
		Crontab:              "*/5 * * * *",
		Timezone:             "UTC",
		Context:              json.RawMessage(`{"k":"v"}`),
		Enabled:              true,
		CreatedBy:            "actor",
		UpdatedBy:            "actor",
		CreatedAt:            now,
		UpdatedAt:            now,
	}
}

func (f *fakeScheduleSvc) Create(_ context.Context, req service.CreateSchedule) (model.CronSchedule, error) {
	if f.createErr != nil {
		return model.CronSchedule{}, f.createErr
	}
	s := testSchedule()
	s.WorkflowDefinitionID = req.WorkflowDefinitionID
	s.Crontab = req.Crontab
	if req.Timezone != "" {
		s.Timezone = req.Timezone
	}
	if len(req.Context) > 0 {
		s.Context = req.Context
	}
	if req.Enabled != nil {
		s.Enabled = *req.Enabled
	}
	return s, nil
}

func (f *fakeScheduleSvc) Get(_ context.Context, _ string) (model.CronSchedule, error) {
	if f.getErr != nil {
		return model.CronSchedule{}, f.getErr
	}
	if f.getItem.ID == "" {
		return testSchedule(), nil
	}
	return f.getItem, nil
}

func (f *fakeScheduleSvc) List(_ context.Context, _ repository.ScheduleListQuery) ([]model.CronSchedule, int64, error) {
	return f.items, f.total, f.listErr
}

func (f *fakeScheduleSvc) Delete(_ context.Context, _ string) error { return f.deleteErr }

func (f *fakeScheduleSvc) Pause(_ context.Context, req service.ScheduleControl) (model.CronSchedule, error) {
	if f.pauseErr != nil {
		return model.CronSchedule{}, f.pauseErr
	}
	f.lastPause = req
	s := testSchedule()
	s.Enabled = false
	return s, nil
}

func (f *fakeScheduleSvc) Resume(_ context.Context, req service.ScheduleControl) (model.CronSchedule, error) {
	if f.resumeErr != nil {
		return model.CronSchedule{}, f.resumeErr
	}
	return testSchedule(), nil
}

func scheduleRouter(f *fakeScheduleSvc) *gin.Engine {
	return NewRouter(Deps{
		Health:    NewHealth(fakePinger{}),
		Schedules: f,
	})
}

func TestScheduleCreate(t *testing.T) {
	r := scheduleRouter(&fakeScheduleSvc{})
	body := `{"workflow_definition_id":"22222222-2222-7222-8222-222222222222","crontab":"*/5 * * * *","context":{"k":"v"}}`
	w := performJSON(r, http.MethodPost, "/v1/workflow/schedules", body, nil)
	if w.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201; body %s", w.Code, w.Body.String())
	}
	var resp ScheduleResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if resp.ID != scheduleID || resp.Crontab != "*/5 * * * *" || resp.Timezone != "UTC" || !resp.Enabled {
		t.Errorf("response = %+v", resp)
	}
	if string(resp.Context) != `{"k":"v"}` {
		t.Errorf("context = %s", resp.Context)
	}
}

func TestScheduleCreateErrors(t *testing.T) {
	// malformed body -> 400
	r := scheduleRouter(&fakeScheduleSvc{})
	w := performJSON(r, http.MethodPost, "/v1/workflow/schedules", `{nope`, nil)
	if w.Code != http.StatusBadRequest {
		t.Errorf("malformed status = %d, want 400", w.Code)
	}

	// missing crontab/definition -> 422
	w = performJSON(r, http.MethodPost, "/v1/workflow/schedules", `{"crontab":""}`, nil)
	if w.Code != http.StatusUnprocessableEntity {
		t.Errorf("missing fields status = %d, want 422", w.Code)
	}

	// service invalid -> 422, unknown definition -> 404
	r = scheduleRouter(&fakeScheduleSvc{createErr: model.ErrInvalid})
	w = performJSON(r, http.MethodPost, "/v1/workflow/schedules", `{"workflow_definition_id":"22222222-2222-7222-8222-222222222222","crontab":"*/5 * * * *"}`, nil)
	if w.Code != http.StatusUnprocessableEntity {
		t.Errorf("invalid status = %d, want 422", w.Code)
	}
	r = scheduleRouter(&fakeScheduleSvc{createErr: model.ErrNotFound})
	w = performJSON(r, http.MethodPost, "/v1/workflow/schedules", `{"workflow_definition_id":"22222222-2222-7222-8222-222222222222","crontab":"*/5 * * * *"}`, nil)
	if w.Code != http.StatusNotFound {
		t.Errorf("unknown definition status = %d, want 404", w.Code)
	}
}

func TestScheduleList(t *testing.T) {
	r := scheduleRouter(&fakeScheduleSvc{items: []model.CronSchedule{testSchedule()}, total: 1})
	w := performJSON(r, http.MethodGet, "/v1/workflow/schedules?page=1&per_page=10", "", nil)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body %s", w.Code, w.Body.String())
	}
	var resp ListResponse[ScheduleResponse]
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if resp.Total != 1 || len(resp.Items) != 1 || resp.Items[0].ID != scheduleID {
		t.Errorf("response = %+v", resp)
	}

	w = performJSON(r, http.MethodGet, "/v1/workflow/schedules?page=0", "", nil)
	if w.Code != http.StatusBadRequest {
		t.Errorf("bad page status = %d, want 400", w.Code)
	}
}

func TestScheduleGet(t *testing.T) {
	r := scheduleRouter(&fakeScheduleSvc{})
	w := performJSON(r, http.MethodGet, "/v1/workflow/schedules/"+scheduleID, "", nil)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body %s", w.Code, w.Body.String())
	}
	var resp ScheduleResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if resp.ID != scheduleID {
		t.Errorf("id = %s, want %s", resp.ID, scheduleID)
	}

	w = performJSON(r, http.MethodGet, "/v1/workflow/schedules/not-a-uuid", "", nil)
	if w.Code != http.StatusBadRequest {
		t.Errorf("malformed id status = %d, want 400", w.Code)
	}

	r = scheduleRouter(&fakeScheduleSvc{getErr: model.ErrNotFound})
	w = performJSON(r, http.MethodGet, "/v1/workflow/schedules/"+scheduleID, "", nil)
	if w.Code != http.StatusNotFound {
		t.Errorf("missing status = %d, want 404", w.Code)
	}
}

func TestScheduleDelete(t *testing.T) {
	r := scheduleRouter(&fakeScheduleSvc{})
	w := performJSON(r, http.MethodDelete, "/v1/workflow/schedules/"+scheduleID, "", nil)
	if w.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want 204; body %s", w.Code, w.Body.String())
	}

	w = performJSON(r, http.MethodDelete, "/v1/workflow/schedules/not-a-uuid", "", nil)
	if w.Code != http.StatusBadRequest {
		t.Errorf("malformed id status = %d, want 400", w.Code)
	}

	r = scheduleRouter(&fakeScheduleSvc{deleteErr: model.ErrNotFound})
	w = performJSON(r, http.MethodDelete, "/v1/workflow/schedules/"+scheduleID, "", nil)
	if w.Code != http.StatusNotFound {
		t.Errorf("missing status = %d, want 404", w.Code)
	}
}

func TestSchedulePauseResume(t *testing.T) {
	f := &fakeScheduleSvc{}
	r := scheduleRouter(f)
	w := performJSON(r, http.MethodPost, "/v1/workflow/schedules/"+scheduleID+"/pause", "", nil)
	if w.Code != http.StatusOK {
		t.Fatalf("pause status = %d, want 200; body %s", w.Code, w.Body.String())
	}
	var paused ScheduleResponse
	if err := json.Unmarshal(w.Body.Bytes(), &paused); err != nil {
		t.Fatal(err)
	}
	if paused.Enabled {
		t.Errorf("enabled after pause = true, want false")
	}
	if f.lastPause.ID != scheduleID {
		t.Errorf("pause id = %s, want %s", f.lastPause.ID, scheduleID)
	}

	w = performJSON(r, http.MethodPost, "/v1/workflow/schedules/"+scheduleID+"/resume", "", nil)
	if w.Code != http.StatusOK {
		t.Fatalf("resume status = %d, want 200; body %s", w.Code, w.Body.String())
	}
	var resumed ScheduleResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resumed); err != nil {
		t.Fatal(err)
	}
	if !resumed.Enabled {
		t.Errorf("enabled after resume = false, want true")
	}

	w = performJSON(r, http.MethodPost, "/v1/workflow/schedules/not-a-uuid/pause", "", nil)
	if w.Code != http.StatusBadRequest {
		t.Errorf("malformed pause status = %d, want 400", w.Code)
	}

	r = scheduleRouter(&fakeScheduleSvc{pauseErr: model.ErrNotFound})
	w = performJSON(r, http.MethodPost, "/v1/workflow/schedules/"+scheduleID+"/pause", "", nil)
	if w.Code != http.StatusNotFound {
		t.Errorf("pause missing status = %d, want 404", w.Code)
	}
}
