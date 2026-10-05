package scheduler

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/simpwf/workflow-engine/internal/workflow/model"
	"github.com/simpwf/workflow-engine/internal/workflow/repository"
	"github.com/simpwf/workflow-engine/internal/workflow/service"
)

// memScheduleRepo is an in-memory ScheduleRepository with claim-failure
// injection. Only the claim path is exercised; schedule CRUD is stubbed.
type memScheduleRepo struct {
	mu        sync.Mutex
	schedules []model.CronSchedule
	fires     map[string]string // fire key -> winning worker
	claimErr  error
}

func (m *memScheduleRepo) Create(_ context.Context, _ model.CronSchedule) error {
	return nil
}

func (m *memScheduleRepo) GetByID(_ context.Context, _ string) (model.CronSchedule, error) {
	return model.CronSchedule{}, model.ErrNotFound
}

func (m *memScheduleRepo) List(_ context.Context, _ repository.ScheduleListQuery) ([]model.CronSchedule, int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.schedules, int64(len(m.schedules)), nil
}

func (m *memScheduleRepo) Delete(_ context.Context, _ string) error {
	return model.ErrNotFound
}

func (m *memScheduleRepo) SetEnabled(_ context.Context, _ string, _ bool, _ string) (model.CronSchedule, error) {
	return model.CronSchedule{}, model.ErrNotFound
}

func (m *memScheduleRepo) ClaimFire(_ context.Context, scheduleID string, fireAt time.Time, workerID string) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.claimErr != nil {
		return false, m.claimErr
	}
	key := scheduleID + "\x00" + fireAt.UTC().Format(time.RFC3339)
	if _, ok := m.fires[key]; ok {
		return false, nil
	}
	if m.fires == nil {
		m.fires = map[string]string{}
	}
	m.fires[key] = workerID
	return true, nil
}

func (m *memScheduleRepo) RecordFireInstance(_ context.Context, scheduleID string, fireAt time.Time, _ string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.fires[scheduleID+"\x00"+fireAt.UTC().Format(time.RFC3339)]; !ok {
		return model.ErrNotFound
	}
	return nil
}

func (m *memScheduleRepo) SweepFires(_ context.Context, _ time.Time) (int64, error) {
	return 0, nil
}

// memInstanceSvc records instance creations. Only Create is exercised; the
// embedded interface panics on any other call.
type memInstanceSvc struct {
	service.InstanceService
	mu      sync.Mutex
	created int
}

func (m *memInstanceSvc) Create(_ context.Context, _ service.CreateInstance) (model.WorkflowInstance, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.created++
	return model.WorkflowInstance{ID: fmt.Sprintf("inst-%d", m.created)}, nil
}

func (m *memInstanceSvc) count() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.created
}

func testFireSchedule() model.CronSchedule {
	return model.CronSchedule{
		ID:                   "sched-1",
		WorkflowDefinitionID: "def-1",
		CreatedBy:            "actor-1",
	}
}

// TestConcurrentFireSameTickCreatesOneInstance races N replicas on one tick:
// exactly one wins the claim and creates the instance.
func TestConcurrentFireSameTickCreatesOneInstance(t *testing.T) {
	repo := &memScheduleRepo{}
	inst := &memInstanceSvc{}
	sched := testFireSchedule()

	const racers = 8
	var wg sync.WaitGroup
	for i := 0; i < racers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			job := &fireJob{schedules: repo, instances: inst, log: slog.Default(), workerID: fmt.Sprintf("worker-%d", i), schedule: sched}
			job.Run()
		}(i)
	}
	wg.Wait()

	if got := inst.count(); got != 1 {
		t.Errorf("created %d instances for one tick, want 1", got)
	}
	repo.mu.Lock()
	defer repo.mu.Unlock()
	if len(repo.fires) != inst.created {
		t.Errorf("%d claims won but %d instances created, want equal", len(repo.fires), inst.created)
	}
}

// TestFireSkipsClaimedTick covers the losing replica: a tick another worker
// already claimed creates nothing.
func TestFireSkipsClaimedTick(t *testing.T) {
	repo := &memScheduleRepo{}
	inst := &memInstanceSvc{}
	if _, err := repo.ClaimFire(context.Background(), "sched-1", time.Now().UTC().Truncate(time.Second), "other-worker"); err != nil {
		t.Fatal(err)
	}

	job := &fireJob{schedules: repo, instances: inst, log: slog.Default(), workerID: "test-worker", schedule: testFireSchedule()}
	job.Run()

	if got := inst.count(); got != 0 {
		t.Errorf("created %d instances for a claimed tick, want 0", got)
	}
}

// TestFireSkipsTickOnClaimError covers a claim failure: the tick is dropped
// (logged, never retried) rather than fired blind.
func TestFireSkipsTickOnClaimError(t *testing.T) {
	repo := &memScheduleRepo{claimErr: fmt.Errorf("db down")}
	inst := &memInstanceSvc{}

	job := &fireJob{schedules: repo, instances: inst, log: slog.Default(), workerID: "test-worker", schedule: testFireSchedule()}
	job.Run()

	if got := inst.count(); got != 0 {
		t.Errorf("created %d instances on claim error, want 0", got)
	}
}

// TestRefreshKeepsUnchangedEntries proves a refresh converges the registry
// instead of rebuilding it: re-adding an @every entry would reset its
// timer and starve any interval longer than the refresh ticker.
func TestRefreshKeepsUnchangedEntries(t *testing.T) {
	repo := &memScheduleRepo{schedules: []model.CronSchedule{{
		ID: "sched-1", WorkflowDefinitionID: "def-1",
		Crontab: "@every 1h", Timezone: "UTC", Enabled: true,
	}}}
	s := New(context.Background(), repo, &memInstanceSvc{}, "test-worker", SchedulerOptions{})
	if err := s.Run(); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	defer func() {
		if err := s.Shutdown(ctx); err != nil {
			t.Fatalf("Shutdown() error = %v", err)
		}
	}()

	before, ok := s.entries["sched-1"]
	if !ok {
		t.Fatal("sched-1 not registered after Run")
	}
	s.Refresh()
	after, ok := s.entries["sched-1"]
	if !ok {
		t.Fatal("sched-1 missing after Refresh")
	}
	if before != after {
		t.Errorf("entry reset by refresh: %+v -> %+v, want unchanged", before, after)
	}
}

// TestAlignedEveryAgreesAcrossRegistrations proves @every ticks anchor to
// wall-clock boundaries: two replicas registering at different times still
// fire the same ticks, which is what the fire claim dedupes on.
func TestAlignedEveryAgreesAcrossRegistrations(t *testing.T) {
	d := alignedEvery(10 * time.Second)
	a := d.Next(time.Date(2026, 10, 4, 12, 0, 7, 0, time.UTC))
	b := d.Next(time.Date(2026, 10, 4, 12, 0, 9, 0, time.UTC))
	want := time.Date(2026, 10, 4, 12, 0, 10, 0, time.UTC)
	if !a.Equal(want) || !b.Equal(want) {
		t.Errorf("next = %v, %v, want both %v", a, b, want)
	}
}

// TestAddAlignedAnchorsEvery proves registration past the CRON_TZ prefix:
// @every specs get the aligned schedule, five-field specs keep plain cron
// parsing.
func TestAddAlignedAnchorsEvery(t *testing.T) {
	s := New(context.Background(), &memScheduleRepo{}, &memInstanceSvc{}, "test-worker", SchedulerOptions{})

	id, err := s.addAligned("CRON_TZ=UTC @every 10s", &fireJob{})
	if err != nil {
		t.Fatalf("addAligned(@every) error = %v", err)
	}
	if _, ok := s.inner.Entry(id).Schedule.(alignedEvery); !ok {
		t.Errorf("schedule type = %T, want alignedEvery", s.inner.Entry(id).Schedule)
	}

	id, err = s.addAligned("CRON_TZ=UTC */5 * * * *", &fireJob{})
	if err != nil {
		t.Fatalf("addAligned(cron) error = %v", err)
	}
	if _, ok := s.inner.Entry(id).Schedule.(alignedEvery); ok {
		t.Errorf("five-field spec got alignedEvery, want plain cron parsing")
	}
}
