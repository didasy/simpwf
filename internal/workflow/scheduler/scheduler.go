// Package scheduler fires cron schedules: on every tick of an enabled
// schedule it creates a workflow instance of the target definition. It is a
// single-replica runner in v1: a fleet must run exactly one
// scheduler-enabled replica, or every tick fires once per replica.
package scheduler

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/robfig/cron/v3"
	"github.com/simpwf/workflow-engine/internal/workflow/model"
	"github.com/simpwf/workflow-engine/internal/workflow/repository"
	"github.com/simpwf/workflow-engine/internal/workflow/service"
)

// fireTimeout bounds one tick's instance creation.
const fireTimeout = 30 * time.Second

// Scheduler keeps a cron registry in sync with the enabled rows of the
// cron_schedules table and creates an instance on every tick. It is inert
// until Run is called.
type Scheduler struct {
	schedules repository.ScheduleRepository
	instances service.InstanceService
	log       *slog.Logger

	inner  *cron.Cron
	ctx    context.Context
	cancel context.CancelFunc
	mu     sync.Mutex
}

// New builds a scheduler; it is inert until Run is called.
func New(ctx context.Context, schedules repository.ScheduleRepository, instances service.InstanceService) *Scheduler {
	log := slog.Default()
	adapter := slogAdapter{log: log}
	inner := cron.New(
		cron.WithLocation(time.UTC),
		cron.WithParser(model.CronParser()),
		cron.WithLogger(adapter),
		// Recover keeps one panicking tick from killing the loop;
		// SkipIfStillRunning skips a tick while its own job still runs.
		// The skip guard is per entry: each job gets its own gate even
		// though the wrapper is shared through the chain.
		cron.WithChain(cron.Recover(adapter), cron.SkipIfStillRunning(adapter)),
	)
	runCtx, cancel := context.WithCancel(ctx)
	return &Scheduler{
		schedules: schedules,
		instances: instances,
		log:       log,
		inner:     inner,
		ctx:       runCtx,
		cancel:    cancel,
	}
}

// Run loads the enabled schedules and starts the firing loop. It returns
// immediately; ticks fire asynchronously. A load failure is returned and
// the loop is not started.
func (s *Scheduler) Run() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.reload(); err != nil {
		return err
	}
	s.inner.Start()
	return nil
}

// Refresh reloads the enabled schedules, replacing every entry. It is safe
// to call while running. A load failure keeps the old entries and is
// logged, never returned, so a service mutation never fails on the refresh
// path.
func (s *Scheduler) Refresh() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.reload(); err != nil {
		s.log.Warn("scheduler refresh failed; keeping old entries", "error", err)
	}
}

// Shutdown stops the loop and waits for in-flight ticks to finish,
// honoring the caller's deadline.
func (s *Scheduler) Shutdown(ctx context.Context) error {
	stopped := s.inner.Stop()
	select {
	case <-stopped.Done():
		s.cancel()
		return nil
	case <-ctx.Done():
		s.cancel()
		return ctx.Err()
	}
}

// reload replaces every entry with the currently enabled rows. The rows
// load before any entry is touched, so a failure keeps the old registry.
// The caller holds s.mu: two concurrent reloads would otherwise interleave
// their clear-and-add and duplicate entries.
func (s *Scheduler) reload() error {
	rows, err := s.loadEnabled()
	if err != nil {
		return err
	}
	for _, e := range s.inner.Entries() {
		s.inner.Remove(e.ID)
	}
	for _, row := range rows {
		spec, _, err := model.ResolveCronSpec(row.Crontab, row.Timezone)
		if err != nil {
			// Validated at create; a row that no longer parses is
			// skipped, never fatal to the other entries.
			s.log.Warn("scheduler skipping unparsable row", "schedule", row.ID, "error", err)
			continue
		}
		job := &fireJob{instances: s.instances, log: s.log, schedule: row}
		if _, err := s.inner.AddJob(spec, job); err != nil {
			s.log.Warn("scheduler skipping row", "schedule", row.ID, "error", err)
			continue
		}
	}
	return nil
}

func (s *Scheduler) loadEnabled() ([]model.CronSchedule, error) {
	enabled := true
	var all []model.CronSchedule
	for page := 1; ; page++ {
		items, total, err := s.schedules.List(s.ctx, repository.ScheduleListQuery{Page: page, PerPage: 100, Enabled: &enabled})
		if err != nil {
			return nil, fmt.Errorf("scheduler load: %w", err)
		}
		all = append(all, items...)
		if int64(len(all)) >= total {
			return all, nil
		}
	}
}

// fireJob creates one workflow instance per tick.
type fireJob struct {
	instances service.InstanceService
	log       *slog.Logger
	schedule  model.CronSchedule
}

// Run implements cron.Job. The tick runs on a detached context so shutdown
// waits for it instead of cancelling it. A failed tick is logged, never
// retried: the next tick fires on schedule.
func (j *fireJob) Run() {
	ctx, cancel := context.WithTimeout(context.Background(), fireTimeout)
	defer cancel()
	inst, err := j.instances.Create(ctx, service.CreateInstance{
		WorkflowDefinitionID: j.schedule.WorkflowDefinitionID,
		Context:              j.schedule.Context,
		Actor:                j.schedule.CreatedBy,
	})
	if err != nil {
		j.log.Warn("schedule tick failed", "schedule", j.schedule.ID, "error", err)
		return
	}
	j.log.Info("schedule tick fired", "schedule", j.schedule.ID, "instance", inst.ID)
}

// slogAdapter plugs slog into cron's logger interface.
type slogAdapter struct {
	log *slog.Logger
}

func (l slogAdapter) Info(msg string, keysAndValues ...any) {
	l.log.Info(msg, keysAndValues...)
}

func (l slogAdapter) Error(err error, msg string, keysAndValues ...any) {
	l.log.Error(msg, append([]any{"error", err}, keysAndValues...)...)
}
