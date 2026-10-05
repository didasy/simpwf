package service

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/simpwf/workflow-engine/internal/workflow/model"
	"github.com/simpwf/workflow-engine/internal/workflow/repository"
)

// CreateSchedule is the input for creating a cron schedule.
type CreateSchedule struct {
	WorkflowDefinitionID string
	Crontab              string
	// Timezone is the IANA zone ticks are evaluated in. Empty defaults to
	// UTC. An inline TZ=/CRON_TZ= prefix in Crontab wins over this field.
	Timezone string
	// Context is the JSON object new instances start from. Empty normalizes
	// to {}.
	Context json.RawMessage
	// Enabled defaults to true when nil.
	Enabled *bool
	// Actor is the users.id uuid recorded as created_by/updated_by. Empty
	// falls back to the service default (the system user).
	Actor string
}

// ScheduleControl is the input for pausing or resuming a schedule.
type ScheduleControl struct {
	ID string
	// Actor is the users.id uuid recorded as updated_by. Empty falls back
	// to the service default.
	Actor string
}

// ScheduleService is the use-case boundary for cron schedules.
type ScheduleService interface {
	Create(ctx context.Context, req CreateSchedule) (model.CronSchedule, error)
	Get(ctx context.Context, id string) (model.CronSchedule, error)
	List(ctx context.Context, q repository.ScheduleListQuery) ([]model.CronSchedule, int64, error)
	Delete(ctx context.Context, id string) error
	// Pause disables a schedule: ticks stop, but already-running instances
	// are unaffected. Idempotent.
	Pause(ctx context.Context, req ScheduleControl) (model.CronSchedule, error)
	// Resume re-enables a paused schedule. Idempotent.
	Resume(ctx context.Context, req ScheduleControl) (model.CronSchedule, error)
}

type scheduleService struct {
	schedules repository.ScheduleRepository
	wfDefs    repository.WorkflowDefinitionRepository
	actor     string
	// onChange refreshes the scheduler's entries after a mutation. It may
	// be nil: tests and a deployment without the firing loop pass none.
	onChange func()
}

// NewScheduleService builds the service.
func NewScheduleService(
	schedules repository.ScheduleRepository,
	wfDefs repository.WorkflowDefinitionRepository,
	actor string,
	onChange func(),
) ScheduleService {
	return &scheduleService{schedules: schedules, wfDefs: wfDefs, actor: actor, onChange: onChange}
}

func (s *scheduleService) Create(ctx context.Context, req CreateSchedule) (model.CronSchedule, error) {
	if strings.TrimSpace(req.WorkflowDefinitionID) == "" {
		return model.CronSchedule{}, fmt.Errorf("%w: workflow_definition_id is required", model.ErrInvalid)
	}
	// Pure-local validation runs before any I/O.
	_, tz, err := model.ResolveCronSpec(req.Crontab, req.Timezone)
	if err != nil {
		return model.CronSchedule{}, err
	}
	contextRaw := req.Context
	if len(contextRaw) == 0 || string(contextRaw) == "null" {
		contextRaw = json.RawMessage("{}")
	}
	var obj map[string]any
	if err := json.Unmarshal(contextRaw, &obj); err != nil || obj == nil {
		return model.CronSchedule{}, fmt.Errorf("%w: context must be a JSON object", model.ErrInvalid)
	}
	if _, err := s.wfDefs.GetByID(ctx, req.WorkflowDefinitionID); err != nil {
		return model.CronSchedule{}, err
	}
	enabled := true
	if req.Enabled != nil {
		enabled = *req.Enabled
	}
	now := nowUTC()
	actor := resolveActor(req.Actor, s.actor)
	sched := model.CronSchedule{
		ID:                   mustNewID(),
		WorkflowDefinitionID: req.WorkflowDefinitionID,
		Crontab:              strings.TrimSpace(req.Crontab),
		Timezone:             tz,
		Context:              contextRaw,
		Enabled:              enabled,
		CreatedBy:            actor,
		UpdatedBy:            actor,
		CreatedAt:            now,
		UpdatedAt:            now,
	}
	if err := s.schedules.Create(ctx, sched); err != nil {
		return model.CronSchedule{}, err
	}
	s.notify()
	return sched, nil
}

func (s *scheduleService) Get(ctx context.Context, id string) (model.CronSchedule, error) {
	return s.schedules.GetByID(ctx, id)
}

func (s *scheduleService) List(ctx context.Context, q repository.ScheduleListQuery) ([]model.CronSchedule, int64, error) {
	return s.schedules.List(ctx, q)
}

func (s *scheduleService) Delete(ctx context.Context, id string) error {
	if err := s.schedules.Delete(ctx, id); err != nil {
		return err
	}
	s.notify()
	return nil
}

func (s *scheduleService) Pause(ctx context.Context, req ScheduleControl) (model.CronSchedule, error) {
	sched, err := s.schedules.SetEnabled(ctx, req.ID, false, resolveActor(req.Actor, s.actor))
	if err != nil {
		return model.CronSchedule{}, err
	}
	s.notify()
	return sched, nil
}

func (s *scheduleService) Resume(ctx context.Context, req ScheduleControl) (model.CronSchedule, error) {
	sched, err := s.schedules.SetEnabled(ctx, req.ID, true, resolveActor(req.Actor, s.actor))
	if err != nil {
		return model.CronSchedule{}, err
	}
	s.notify()
	return sched, nil
}

func (s *scheduleService) notify() {
	if s.onChange != nil {
		s.onChange()
	}
}
