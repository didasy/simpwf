// Package scheduler fires cron schedules: on every tick of an enabled
// schedule it creates a workflow instance of the target definition. Every
// replica fires the same ticks, but the (schedule, fire-at) claim keeps
// each tick to one instance fleet-wide, so any number of
// scheduler-enabled replicas may run. A background refresh picks up
// schedule mutations made through other replicas.
package scheduler

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/robfig/cron/v3"
	"github.com/simpwf/workflow-engine/internal/workflow/model"
	"github.com/simpwf/workflow-engine/internal/workflow/repository"
	"github.com/simpwf/workflow-engine/internal/workflow/service"
)

// fireTimeout bounds one tick's instance creation.
const fireTimeout = 30 * time.Second

// fireClaimRetention bounds the fire-claim history: claims older than this
// are swept on every registry refresh.
const fireClaimRetention = 24 * time.Hour

// SchedulerOptions tunes the background loops. Zero values get defaults.
type SchedulerOptions struct {
	// RefreshInterval reloads the enabled schedules from the database so
	// mutations made on other replicas take effect locally.
	RefreshInterval time.Duration
}

// registeredEntry is one live cron entry: its registry id and the spec it
// was added with, so a refresh can tell a changed schedule apart from an
// untouched one.
type registeredEntry struct {
	id   cron.EntryID
	spec string
}

// Scheduler keeps a cron registry in sync with the enabled rows of the
// cron_schedules table and creates an instance on every tick. It is inert
// until Run is called.
type Scheduler struct {
	schedules repository.ScheduleRepository
	instances service.InstanceService
	log       *slog.Logger
	workerID  string

	inner           *cron.Cron
	ctx             context.Context
	cancel          context.CancelFunc
	refreshInterval time.Duration
	entries         map[string]registeredEntry
	mu              sync.Mutex
	wg              sync.WaitGroup
}

// New builds a scheduler; it is inert until Run is called. workerID
// identifies this replica in fire claims, like the dispatcher worker ids.
func New(ctx context.Context, schedules repository.ScheduleRepository, instances service.InstanceService, workerID string, opts SchedulerOptions) *Scheduler {
	if opts.RefreshInterval <= 0 {
		opts.RefreshInterval = 30 * time.Second
	}
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
		schedules:       schedules,
		instances:       instances,
		log:             log,
		workerID:        workerID,
		inner:           inner,
		ctx:             runCtx,
		cancel:          cancel,
		refreshInterval: opts.RefreshInterval,
		entries:         map[string]registeredEntry{},
	}
}

// Run loads the enabled schedules and starts the firing loop. It returns
// immediately; ticks fire asynchronously and a background refresh keeps the
// registry in sync across replicas. A load failure is returned and neither
// loop is started.
func (s *Scheduler) Run() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.sync(); err != nil {
		return err
	}
	s.inner.Start()
	s.wg.Add(1)
	go s.refreshLoop()
	return nil
}

// Refresh reloads the enabled schedules, converging the registry on the
// enabled rows. It is safe to call while running. A load failure keeps the
// old entries and is logged, never returned, so a service mutation never
// fails on the refresh path.
func (s *Scheduler) Refresh() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.sync(); err != nil {
		s.log.Warn("scheduler refresh failed; keeping old entries", "error", err)
	}
}

// Shutdown stops the loop and waits for in-flight ticks and the registry
// refresher to finish, honoring the caller's deadline.
func (s *Scheduler) Shutdown(ctx context.Context) error {
	stopped := s.inner.Stop()
	select {
	case <-stopped.Done():
	case <-ctx.Done():
		s.cancel()
		return ctx.Err()
	}
	s.cancel()
	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// refreshLoop reloads the registry on an interval so schedule mutations made
// through other replicas (create, delete, pause, resume) take effect
// locally, and sweeps expired fire claims.
func (s *Scheduler) refreshLoop() {
	defer s.wg.Done()
	ticker := time.NewTicker(s.refreshInterval)
	defer ticker.Stop()
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
		}
		s.Refresh()
		s.sweep()
	}
}

func (s *Scheduler) sweep() {
	n, err := s.schedules.SweepFires(s.ctx, time.Now().UTC().Add(-fireClaimRetention))
	if err != nil {
		if s.ctx.Err() == nil {
			s.log.Warn("scheduler sweep failed", "error", err)
		}
		return
	}
	if n > 0 {
		s.log.Debug("scheduler swept fire claims", "count", n)
	}
}

// alignedEvery is an @every interval anchored to wall-clock multiples of
// the interval instead of the registration time, so every replica agrees
// on fire times however staggered their refreshes are. Without it each
// replica's relative timer would fire its own phase and multiply the
// effective rate by the replica count.
type alignedEvery time.Duration

// Next implements cron.Schedule.
func (d alignedEvery) Next(t time.Time) time.Time {
	return t.Truncate(time.Duration(d)).Add(time.Duration(d))
}

// addAligned registers job with wall-clock-aligned ticks for @every specs
// and plain cron parsing for everything else. The spec carries a
// CRON_TZ=/TZ= prefix from ResolveCronSpec, so the descriptor is located
// past the prefix rather than at the start.
func (s *Scheduler) addAligned(spec string, job cron.Job) (cron.EntryID, error) {
	if _, rest, ok := strings.Cut(spec, "@every "); ok {
		if d, err := time.ParseDuration(strings.TrimSpace(rest)); err == nil && d > 0 {
			return s.inner.Schedule(alignedEvery(d), job), nil
		}
	}
	return s.inner.AddJob(spec, job)
}

// sync converges the registry on the currently enabled rows: entries for
// new schedules are added, entries for removed, disabled, or changed
// schedules are dropped and re-added, and untouched entries keep their
// timers. A clear-and-add reload would reset every @every timer on each
// refresh, starving any interval longer than the refresh ticker. The rows
// load before any entry is touched, so a failure keeps the old registry.
// The caller holds s.mu: two concurrent syncs would otherwise interleave
// their add-and-remove and duplicate entries.
func (s *Scheduler) sync() error {
	rows, err := s.loadEnabled()
	if err != nil {
		return err
	}
	want := make(map[string]model.CronSchedule, len(rows))
	for _, row := range rows {
		want[row.ID] = row
	}
	for id, ent := range s.entries {
		row, ok := want[id]
		if !ok {
			s.inner.Remove(ent.id)
			delete(s.entries, id)
			continue
		}
		spec, _, err := model.ResolveCronSpec(row.Crontab, row.Timezone)
		if err != nil || spec != ent.spec {
			// Unparsable or changed: drop; re-added below when parsable.
			s.inner.Remove(ent.id)
			delete(s.entries, id)
		}
	}
	for _, row := range rows {
		if _, ok := s.entries[row.ID]; ok {
			continue
		}
		spec, _, err := model.ResolveCronSpec(row.Crontab, row.Timezone)
		if err != nil {
			// Validated at create; a row that no longer parses is
			// skipped, never fatal to the other entries.
			s.log.Warn("scheduler skipping unparsable row", "schedule", row.ID, "error", err)
			continue
		}
		job := &fireJob{schedules: s.schedules, instances: s.instances, log: s.log, workerID: s.workerID, schedule: row}
		entryID, err := s.addAligned(spec, job)
		if err != nil {
			s.log.Warn("scheduler skipping row", "schedule", row.ID, "error", err)
			continue
		}
		s.entries[row.ID] = registeredEntry{id: entryID, spec: spec}
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
	schedules repository.ScheduleRepository
	instances service.InstanceService
	log       *slog.Logger
	workerID  string
	schedule  model.CronSchedule
}

// Run implements cron.Job. Every replica fires the same tick, but only the
// replica that wins the (schedule, fire-at) claim creates the instance;
// losers and claim errors drop the tick. The tick runs on a detached
// context so shutdown waits for it instead of cancelling it. A failed tick
// is logged, never retried: the next tick fires on schedule.
func (j *fireJob) Run() {
	fireAt := time.Now().UTC().Truncate(time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), fireTimeout)
	defer cancel()
	claimed, err := j.schedules.ClaimFire(ctx, j.schedule.ID, fireAt, j.workerID)
	if err != nil {
		j.log.Warn("schedule tick claim failed", "schedule", j.schedule.ID, "error", err)
		return
	}
	if !claimed {
		j.log.Debug("schedule tick already claimed", "schedule", j.schedule.ID, "fire_at", fireAt)
		return
	}
	inst, err := j.instances.Create(ctx, service.CreateInstance{
		WorkflowDefinitionID: j.schedule.WorkflowDefinitionID,
		Context:              j.schedule.Context,
		Actor:                j.schedule.CreatedBy,
	})
	if err != nil {
		j.log.Warn("schedule tick failed", "schedule", j.schedule.ID, "error", err)
		return
	}
	if err := j.schedules.RecordFireInstance(ctx, j.schedule.ID, fireAt, inst.ID); err != nil {
		j.log.Warn("schedule tick record failed", "schedule", j.schedule.ID, "instance", inst.ID, "error", err)
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
