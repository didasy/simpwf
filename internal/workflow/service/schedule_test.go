package service_test

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"testing"

	"github.com/simpwf/workflow-engine/internal/workflow/model"
	"github.com/simpwf/workflow-engine/internal/workflow/repository"
	"github.com/simpwf/workflow-engine/internal/workflow/service"
)

// fakeScheduleRepo is an in-memory ScheduleRepository.
type fakeScheduleRepo struct {
	schedules map[string]model.CronSchedule
}

func newFakeScheduleRepo() *fakeScheduleRepo {
	return &fakeScheduleRepo{schedules: map[string]model.CronSchedule{}}
}

func (f *fakeScheduleRepo) Create(_ context.Context, s model.CronSchedule) error {
	if _, ok := f.schedules[s.ID]; ok {
		return model.ErrConflict
	}
	f.schedules[s.ID] = s
	return nil
}

func (f *fakeScheduleRepo) GetByID(_ context.Context, id string) (model.CronSchedule, error) {
	s, ok := f.schedules[id]
	if !ok {
		return model.CronSchedule{}, errors.Join(model.ErrNotFound, errors.New("cron schedule "+id))
	}
	return s, nil
}

func (f *fakeScheduleRepo) List(_ context.Context, q repository.ScheduleListQuery) ([]model.CronSchedule, int64, error) {
	var items []model.CronSchedule
	for _, s := range f.schedules {
		items = append(items, s)
	}
	sort.Slice(items, func(i, j int) bool { return items[i].ID < items[j].ID })
	if q.Page < 1 {
		q.Page = 1
	}
	if q.PerPage < 1 {
		q.PerPage = 50
	}
	start := (q.Page - 1) * q.PerPage
	if start >= len(items) {
		return nil, int64(len(items)), nil
	}
	end := start + q.PerPage
	if end > len(items) {
		end = len(items)
	}
	return items[start:end], int64(len(items)), nil
}

func (f *fakeScheduleRepo) Delete(_ context.Context, id string) error {
	if _, ok := f.schedules[id]; !ok {
		return model.ErrNotFound
	}
	delete(f.schedules, id)
	return nil
}

func (f *fakeScheduleRepo) SetEnabled(_ context.Context, id string, enabled bool, actor string) (model.CronSchedule, error) {
	s, ok := f.schedules[id]
	if !ok {
		return model.CronSchedule{}, model.ErrNotFound
	}
	s.Enabled = enabled
	s.UpdatedBy = actor
	f.schedules[id] = s
	return s, nil
}

const scheduleDefID = "aaaaaaaa-aaaa-7aaa-8aaa-aaaaaaaaaaaa"

func newScheduleService(schedRepo *fakeScheduleRepo, calls *int) (service.ScheduleService, *fakeWorkflowRepo) {
	wfRepo := newFakeWorkflowRepo()
	wfRepo.defs[scheduleDefID] = model.WorkflowDefinition{ID: scheduleDefID, Name: "flow"}
	var onChange func()
	if calls != nil {
		onChange = func() { *calls++ }
	}
	return service.NewScheduleService(schedRepo, wfRepo, actorID, onChange), wfRepo
}

func TestCreateSchedule(t *testing.T) {
	var calls int
	svc, _ := newScheduleService(newFakeScheduleRepo(), &calls)
	s, err := svc.Create(context.Background(), service.CreateSchedule{
		WorkflowDefinitionID: scheduleDefID,
		Crontab:              "*/5 * * * *",
		Context:              json.RawMessage(`{"k":"v"}`),
	})
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	if s.ID == "" {
		t.Errorf("ID is empty")
	}
	if s.WorkflowDefinitionID != scheduleDefID || s.Crontab != "*/5 * * * *" {
		t.Errorf("fields mismatch: %+v", s)
	}
	if s.Timezone != "UTC" {
		t.Errorf("Timezone = %q, want UTC default", s.Timezone)
	}
	if !s.Enabled {
		t.Errorf("Enabled = false, want true default")
	}
	if string(s.Context) != `{"k":"v"}` {
		t.Errorf("Context = %s", s.Context)
	}
	if s.CreatedBy != actorID || s.UpdatedBy != actorID {
		t.Errorf("actor mismatch: %+v", s)
	}
	if calls != 1 {
		t.Errorf("onChange calls = %d, want 1", calls)
	}
}

func TestCreateScheduleInvalidCrontab(t *testing.T) {
	var calls int
	svc, _ := newScheduleService(newFakeScheduleRepo(), &calls)
	if _, err := svc.Create(context.Background(), service.CreateSchedule{
		WorkflowDefinitionID: scheduleDefID,
		Crontab:              "not a cron",
	}); !errors.Is(err, model.ErrInvalid) {
		t.Errorf("error = %v, want ErrInvalid", err)
	}
	if _, err := svc.Create(context.Background(), service.CreateSchedule{
		WorkflowDefinitionID: scheduleDefID,
		Crontab:              "* * * * *",
		Timezone:             "Mars/Olympus",
	}); !errors.Is(err, model.ErrInvalid) {
		t.Errorf("bad timezone error = %v, want ErrInvalid", err)
	}
	if calls != 0 {
		t.Errorf("onChange calls = %d, want 0", calls)
	}
}

func TestCreateScheduleUnknownDefinition(t *testing.T) {
	var calls int
	svc, _ := newScheduleService(newFakeScheduleRepo(), &calls)
	if _, err := svc.Create(context.Background(), service.CreateSchedule{
		WorkflowDefinitionID: "bbbbbbbb-bbbb-7bbb-8bbb-bbbbbbbbbbbb",
		Crontab:              "* * * * *",
	}); !errors.Is(err, model.ErrNotFound) {
		t.Errorf("error = %v, want ErrNotFound", err)
	}
	if _, err := svc.Create(context.Background(), service.CreateSchedule{
		Crontab: "* * * * *",
	}); !errors.Is(err, model.ErrInvalid) {
		t.Errorf("blank definition id error = %v, want ErrInvalid", err)
	}
	if calls != 0 {
		t.Errorf("onChange calls = %d, want 0", calls)
	}
}

func TestCreateScheduleContextMustBeObject(t *testing.T) {
	svc, _ := newScheduleService(newFakeScheduleRepo(), nil)
	if _, err := svc.Create(context.Background(), service.CreateSchedule{
		WorkflowDefinitionID: scheduleDefID,
		Crontab:              "* * * * *",
		Context:              json.RawMessage(`[1,2]`),
	}); !errors.Is(err, model.ErrInvalid) {
		t.Errorf("array context error = %v, want ErrInvalid", err)
	}
	// Empty and null contexts normalize to {}.
	for _, raw := range []string{"", "null"} {
		s, err := svc.Create(context.Background(), service.CreateSchedule{
			WorkflowDefinitionID: scheduleDefID,
			Crontab:              "* * * * *",
			Context:              json.RawMessage(raw),
		})
		if err != nil {
			t.Errorf("Create(context %q) error = %v", raw, err)
			continue
		}
		if string(s.Context) != "{}" {
			t.Errorf("Create(context %q) stored %s, want {}", raw, s.Context)
		}
	}
}

func TestCreateScheduleExplicitEnabledAndTimezone(t *testing.T) {
	svc, _ := newScheduleService(newFakeScheduleRepo(), nil)
	disabled := false
	s, err := svc.Create(context.Background(), service.CreateSchedule{
		WorkflowDefinitionID: scheduleDefID,
		Crontab:              "0 9 * * *",
		Timezone:             "America/New_York",
		Enabled:              &disabled,
		Actor:                "cccccccc-cccc-7ccc-8ccc-cccccccccccc",
	})
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	if s.Enabled {
		t.Errorf("Enabled = true, want explicit false")
	}
	if s.Timezone != "America/New_York" {
		t.Errorf("Timezone = %q", s.Timezone)
	}
	if s.CreatedBy != "cccccccc-cccc-7ccc-8ccc-cccccccccccc" {
		t.Errorf("CreatedBy = %s, want request actor", s.CreatedBy)
	}
}

func TestGetScheduleNotFound(t *testing.T) {
	svc, _ := newScheduleService(newFakeScheduleRepo(), nil)
	if _, err := svc.Get(context.Background(), "bbbbbbbb-bbbb-7bbb-8bbb-bbbbbbbbbbbb"); !errors.Is(err, model.ErrNotFound) {
		t.Errorf("error = %v, want ErrNotFound", err)
	}
}

func TestDeleteSchedule(t *testing.T) {
	repo := newFakeScheduleRepo()
	var calls int
	svc, _ := newScheduleService(repo, &calls)
	s, err := svc.Create(context.Background(), service.CreateSchedule{
		WorkflowDefinitionID: scheduleDefID,
		Crontab:              "* * * * *",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := svc.Delete(context.Background(), s.ID); err != nil {
		t.Fatalf("Delete() error = %v", err)
	}
	if _, err := svc.Get(context.Background(), s.ID); !errors.Is(err, model.ErrNotFound) {
		t.Errorf("Get after delete error = %v, want ErrNotFound", err)
	}
	if err := svc.Delete(context.Background(), s.ID); !errors.Is(err, model.ErrNotFound) {
		t.Errorf("second Delete error = %v, want ErrNotFound", err)
	}
	if calls != 2 {
		t.Errorf("onChange calls = %d, want 2 (create + delete)", calls)
	}
}

func TestPauseResumeSchedule(t *testing.T) {
	repo := newFakeScheduleRepo()
	var calls int
	svc, _ := newScheduleService(repo, &calls)
	s, err := svc.Create(context.Background(), service.CreateSchedule{
		WorkflowDefinitionID: scheduleDefID,
		Crontab:              "* * * * *",
	})
	if err != nil {
		t.Fatal(err)
	}
	paused, err := svc.Pause(context.Background(), service.ScheduleControl{ID: s.ID, Actor: actorID})
	if err != nil {
		t.Fatalf("Pause() error = %v", err)
	}
	if paused.Enabled {
		t.Errorf("Enabled after pause = true, want false")
	}
	resumed, err := svc.Resume(context.Background(), service.ScheduleControl{ID: s.ID, Actor: actorID})
	if err != nil {
		t.Fatalf("Resume() error = %v", err)
	}
	if !resumed.Enabled {
		t.Errorf("Enabled after resume = false, want true")
	}
	if _, err := svc.Pause(context.Background(), service.ScheduleControl{ID: "bbbbbbbb-bbbb-7bbb-8bbb-bbbbbbbbbbbb"}); !errors.Is(err, model.ErrNotFound) {
		t.Errorf("Pause missing error = %v, want ErrNotFound", err)
	}
	if calls != 3 {
		t.Errorf("onChange calls = %d, want 3 (create + pause + resume)", calls)
	}
}

func TestListSchedules(t *testing.T) {
	svc, _ := newScheduleService(newFakeScheduleRepo(), nil)
	for _, crontab := range []string{"* * * * *", "0 9 * * *"} {
		if _, err := svc.Create(context.Background(), service.CreateSchedule{
			WorkflowDefinitionID: scheduleDefID,
			Crontab:              crontab,
		}); err != nil {
			t.Fatal(err)
		}
	}
	items, total, err := svc.List(context.Background(), repository.ScheduleListQuery{Page: 1, PerPage: 50})
	if err != nil {
		t.Fatal(err)
	}
	if total != 2 || len(items) != 2 {
		t.Errorf("total = %d len = %d, want 2 and 2", total, len(items))
	}
}
