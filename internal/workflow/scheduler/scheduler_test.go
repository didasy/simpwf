package scheduler_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/simpwf/workflow-engine/internal/workflow/model"
	"github.com/simpwf/workflow-engine/internal/workflow/repository"
	"github.com/simpwf/workflow-engine/internal/workflow/scheduler"
	"github.com/simpwf/workflow-engine/internal/workflow/service"
)

// fakeScheduleRepo is an in-memory ScheduleRepository with list-failure
// injection.
type fakeScheduleRepo struct {
	mu        sync.Mutex
	schedules map[string]model.CronSchedule
	fires     map[string]fakeFire
	listErr   error
}

// fakeFire is one claimed tick: when it fired, who won it, and the instance
// the winner created.
type fakeFire struct {
	at         time.Time
	workerID   string
	instanceID string
}

func fireKey(scheduleID string, fireAt time.Time) string {
	return scheduleID + "\x00" + fireAt.UTC().Format(time.RFC3339)
}

func newFakeScheduleRepo() *fakeScheduleRepo {
	return &fakeScheduleRepo{schedules: map[string]model.CronSchedule{}}
}

func (f *fakeScheduleRepo) Create(_ context.Context, s model.CronSchedule) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.schedules[s.ID] = s
	return nil
}

func (f *fakeScheduleRepo) GetByID(_ context.Context, id string) (model.CronSchedule, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	s, ok := f.schedules[id]
	if !ok {
		return model.CronSchedule{}, model.ErrNotFound
	}
	return s, nil
}

func (f *fakeScheduleRepo) List(_ context.Context, q repository.ScheduleListQuery) ([]model.CronSchedule, int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.listErr != nil {
		return nil, 0, f.listErr
	}
	var items []model.CronSchedule
	for _, s := range f.schedules {
		if q.Enabled != nil && s.Enabled != *q.Enabled {
			continue
		}
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
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.schedules[id]; !ok {
		return model.ErrNotFound
	}
	delete(f.schedules, id)
	return nil
}

func (f *fakeScheduleRepo) SetEnabled(_ context.Context, id string, enabled bool, actor string) (model.CronSchedule, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	s, ok := f.schedules[id]
	if !ok {
		return model.CronSchedule{}, model.ErrNotFound
	}
	s.Enabled = enabled
	s.UpdatedBy = actor
	f.schedules[id] = s
	return s, nil
}

func (f *fakeScheduleRepo) ClaimFire(_ context.Context, scheduleID string, fireAt time.Time, workerID string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	key := fireKey(scheduleID, fireAt)
	if _, ok := f.fires[key]; ok {
		return false, nil
	}
	if f.fires == nil {
		f.fires = map[string]fakeFire{}
	}
	f.fires[key] = fakeFire{at: fireAt, workerID: workerID}
	return true, nil
}

func (f *fakeScheduleRepo) RecordFireInstance(_ context.Context, scheduleID string, fireAt time.Time, instanceID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	key := fireKey(scheduleID, fireAt)
	fire, ok := f.fires[key]
	if !ok {
		return model.ErrNotFound
	}
	fire.instanceID = instanceID
	f.fires[key] = fire
	return nil
}

func (f *fakeScheduleRepo) SweepFires(_ context.Context, olderThan time.Time) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var swept int64
	for key, fire := range f.fires {
		if fire.at.Before(olderThan) {
			delete(f.fires, key)
			swept++
		}
	}
	return swept, nil
}

// fakeInstanceSvc records instance creations. Only Create is exercised; the
// embedded interface panics on any other call.
type fakeInstanceSvc struct {
	service.InstanceService
	mu      sync.Mutex
	created []service.CreateInstance
	started int
	err     error
	gate    chan struct{}
}

func (f *fakeInstanceSvc) Create(ctx context.Context, req service.CreateInstance) (model.WorkflowInstance, error) {
	f.mu.Lock()
	f.started++
	f.mu.Unlock()
	if f.gate != nil {
		select {
		case <-f.gate:
		case <-ctx.Done():
			return model.WorkflowInstance{}, ctx.Err()
		}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return model.WorkflowInstance{}, f.err
	}
	f.created = append(f.created, req)
	return model.WorkflowInstance{ID: fmt.Sprintf("inst-%d", len(f.created))}, nil
}

func (f *fakeInstanceSvc) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.created)
}

func (f *fakeInstanceSvc) startedCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.started
}

func (f *fakeInstanceSvc) first() service.CreateInstance {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.created[0]
}

func waitFor(t *testing.T, timeout time.Duration, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", msg)
}

const (
	schedDefID = "aaaaaaaa-aaaa-7aaa-8aaa-aaaaaaaaaaaa"
	schedActor = "bbbbbbbb-bbbb-7bbb-8bbb-bbbbbbbbbbbb"
)

func newTestSchedule(id, crontab string, enabled bool) model.CronSchedule {
	now := time.Now().UTC()
	return model.CronSchedule{
		ID:                   id,
		WorkflowDefinitionID: schedDefID,
		Crontab:              crontab,
		Timezone:             "UTC",
		Context:              json.RawMessage(`{"k":"v"}`),
		Enabled:              enabled,
		CreatedBy:            schedActor,
		UpdatedBy:            schedActor,
		CreatedAt:            now,
		UpdatedAt:            now,
	}
}

func shutdown(t *testing.T, s *scheduler.Scheduler) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := s.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown() error = %v", err)
	}
}

func TestTickCreatesInstance(t *testing.T) {
	repo := newFakeScheduleRepo()
	if err := repo.Create(context.Background(), newTestSchedule("sched-1", "@every 1s", true)); err != nil {
		t.Fatal(err)
	}
	inst := &fakeInstanceSvc{}
	s := scheduler.New(context.Background(), repo, inst, "test-worker", scheduler.SchedulerOptions{})
	if err := s.Run(); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	defer shutdown(t, s)

	waitFor(t, 5*time.Second, func() bool { return inst.count() >= 1 }, "first tick")
	got := inst.first()
	if got.WorkflowDefinitionID != schedDefID {
		t.Errorf("definition id = %s, want %s", got.WorkflowDefinitionID, schedDefID)
	}
	if string(got.Context) != `{"k":"v"}` {
		t.Errorf("context = %s, want stored schedule context", got.Context)
	}
	if got.Actor != schedActor {
		t.Errorf("actor = %s, want schedule creator %s", got.Actor, schedActor)
	}
}

func TestDisabledScheduleDoesNotFire(t *testing.T) {
	repo := newFakeScheduleRepo()
	if err := repo.Create(context.Background(), newTestSchedule("sched-1", "@every 1s", false)); err != nil {
		t.Fatal(err)
	}
	inst := &fakeInstanceSvc{}
	s := scheduler.New(context.Background(), repo, inst, "test-worker", scheduler.SchedulerOptions{})
	if err := s.Run(); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	defer shutdown(t, s)

	// Ticks land on whole seconds; the window must cover 2+ activations
	// to prove silence.
	time.Sleep(2500 * time.Millisecond)
	if n := inst.count(); n != 0 {
		t.Errorf("created %d instances, want 0", n)
	}
}

func TestRefreshAddsSchedule(t *testing.T) {
	repo := newFakeScheduleRepo()
	inst := &fakeInstanceSvc{}
	s := scheduler.New(context.Background(), repo, inst, "test-worker", scheduler.SchedulerOptions{})
	if err := s.Run(); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	defer shutdown(t, s)

	if err := repo.Create(context.Background(), newTestSchedule("sched-1", "@every 1s", true)); err != nil {
		t.Fatal(err)
	}
	s.Refresh()
	waitFor(t, 5*time.Second, func() bool { return inst.count() >= 1 }, "tick after refresh")
}

func TestRefreshRemovesDeletedSchedule(t *testing.T) {
	ctx := context.Background()
	repo := newFakeScheduleRepo()
	if err := repo.Create(ctx, newTestSchedule("sched-1", "@every 1s", true)); err != nil {
		t.Fatal(err)
	}
	inst := &fakeInstanceSvc{}
	s := scheduler.New(ctx, repo, inst, "test-worker", scheduler.SchedulerOptions{})
	if err := s.Run(); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	defer shutdown(t, s)

	waitFor(t, 5*time.Second, func() bool { return inst.count() >= 1 }, "first tick")
	if err := repo.Delete(ctx, "sched-1"); err != nil {
		t.Fatal(err)
	}
	s.Refresh()
	time.Sleep(150 * time.Millisecond) // let an in-flight tick land
	n := inst.count()
	time.Sleep(2500 * time.Millisecond) // 2+ activations at 1s if still registered
	if got := inst.count(); got != n {
		t.Errorf("created %d instances after delete, want %d (no new ticks)", got, n)
	}
}

func TestPauseStopsFiring(t *testing.T) {
	ctx := context.Background()
	repo := newFakeScheduleRepo()
	if err := repo.Create(ctx, newTestSchedule("sched-1", "@every 1s", true)); err != nil {
		t.Fatal(err)
	}
	inst := &fakeInstanceSvc{}
	s := scheduler.New(ctx, repo, inst, "test-worker", scheduler.SchedulerOptions{})
	if err := s.Run(); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	defer shutdown(t, s)

	waitFor(t, 5*time.Second, func() bool { return inst.count() >= 1 }, "first tick")
	if _, err := repo.SetEnabled(ctx, "sched-1", false, schedActor); err != nil {
		t.Fatal(err)
	}
	s.Refresh()
	time.Sleep(150 * time.Millisecond) // let an in-flight tick land
	n := inst.count()
	time.Sleep(2500 * time.Millisecond) // 2+ activations at 1s if still registered
	if got := inst.count(); got != n {
		t.Errorf("created %d instances after pause, want %d (no new ticks)", got, n)
	}
}

func TestRunFailsWhenLoadFails(t *testing.T) {
	repo := newFakeScheduleRepo()
	repo.listErr = errors.New("db down")
	s := scheduler.New(context.Background(), repo, &fakeInstanceSvc{}, "test-worker", scheduler.SchedulerOptions{})
	if err := s.Run(); err == nil {
		shutdown(t, s)
		t.Fatalf("Run() error = nil, want load failure")
	}
}

func TestRefreshKeepsEntriesOnLoadFailure(t *testing.T) {
	repo := newFakeScheduleRepo()
	if err := repo.Create(context.Background(), newTestSchedule("sched-1", "@every 1s", true)); err != nil {
		t.Fatal(err)
	}
	inst := &fakeInstanceSvc{}
	s := scheduler.New(context.Background(), repo, inst, "test-worker", scheduler.SchedulerOptions{})
	if err := s.Run(); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	defer shutdown(t, s)

	waitFor(t, 5*time.Second, func() bool { return inst.count() >= 1 }, "first tick")
	repo.mu.Lock()
	repo.listErr = errors.New("db down")
	repo.mu.Unlock()
	s.Refresh()
	repo.mu.Lock()
	repo.listErr = nil
	repo.mu.Unlock()
	waitFor(t, 5*time.Second, func() bool { return inst.count() >= 2 }, "tick after failed refresh")
}

func TestUnparsableRowIsSkipped(t *testing.T) {
	ctx := context.Background()
	repo := newFakeScheduleRepo()
	bad := newTestSchedule("sched-bad", "not a cron", true)
	if err := repo.Create(ctx, bad); err != nil {
		t.Fatal(err)
	}
	if err := repo.Create(ctx, newTestSchedule("sched-good", "@every 1s", true)); err != nil {
		t.Fatal(err)
	}
	inst := &fakeInstanceSvc{}
	s := scheduler.New(ctx, repo, inst, "test-worker", scheduler.SchedulerOptions{})
	if err := s.Run(); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	defer shutdown(t, s)

	waitFor(t, 5*time.Second, func() bool { return inst.count() >= 1 }, "tick from good row")
}

func TestShutdownHonorsDeadline(t *testing.T) {
	repo := newFakeScheduleRepo()
	if err := repo.Create(context.Background(), newTestSchedule("sched-1", "@every 1s", true)); err != nil {
		t.Fatal(err)
	}
	gate := make(chan struct{})
	inst := &fakeInstanceSvc{gate: gate}
	s := scheduler.New(context.Background(), repo, inst, "test-worker", scheduler.SchedulerOptions{})
	if err := s.Run(); err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	waitFor(t, 5*time.Second, func() bool { return inst.startedCount() >= 1 }, "blocked tick")
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if err := s.Shutdown(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("Shutdown() error = %v, want DeadlineExceeded", err)
	}
	close(gate)
	shutdown(t, s)
	if n := inst.count(); n < 1 {
		t.Errorf("created %d instances, want >= 1 after gate release", n)
	}
}

func TestTickRecordsFireClaim(t *testing.T) {
	repo := newFakeScheduleRepo()
	if err := repo.Create(context.Background(), newTestSchedule("sched-1", "@every 1s", true)); err != nil {
		t.Fatal(err)
	}
	inst := &fakeInstanceSvc{}
	s := scheduler.New(context.Background(), repo, inst, "test-worker", scheduler.SchedulerOptions{})
	if err := s.Run(); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	defer shutdown(t, s)

	waitFor(t, 5*time.Second, func() bool { return inst.count() >= 1 }, "first tick")
	waitFor(t, 5*time.Second, func() bool {
		repo.mu.Lock()
		defer repo.mu.Unlock()
		for _, fire := range repo.fires {
			if fire.instanceID != "" {
				return true
			}
		}
		return false
	}, "fire claim recorded")
	repo.mu.Lock()
	defer repo.mu.Unlock()
	if len(repo.fires) != 1 {
		t.Fatalf("fires = %d, want 1", len(repo.fires))
	}
	for _, fire := range repo.fires {
		if fire.workerID != "test-worker" {
			t.Errorf("winner = %q, want test-worker", fire.workerID)
		}
		if fire.instanceID != "inst-1" {
			t.Errorf("instance = %q, want inst-1", fire.instanceID)
		}
	}
}

func TestRefreshLoopPicksUpNewSchedule(t *testing.T) {
	repo := newFakeScheduleRepo()
	inst := &fakeInstanceSvc{}
	s := scheduler.New(context.Background(), repo, inst, "test-worker", scheduler.SchedulerOptions{RefreshInterval: 100 * time.Millisecond})
	if err := s.Run(); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	defer shutdown(t, s)

	if err := repo.Create(context.Background(), newTestSchedule("sched-1", "@every 1s", true)); err != nil {
		t.Fatal(err)
	}
	// No manual Refresh: the background loop must pick the schedule up.
	waitFor(t, 5*time.Second, func() bool { return inst.count() >= 1 }, "tick after background refresh")
}

func TestRefreshLoopSweepsOldClaims(t *testing.T) {
	repo := newFakeScheduleRepo()
	stale := time.Now().UTC().Add(-48 * time.Hour)
	repo.fires = map[string]fakeFire{
		fireKey("sched-gone", stale): {at: stale, workerID: "old-worker"},
	}
	inst := &fakeInstanceSvc{}
	s := scheduler.New(context.Background(), repo, inst, "test-worker", scheduler.SchedulerOptions{RefreshInterval: 100 * time.Millisecond})
	if err := s.Run(); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	defer shutdown(t, s)

	waitFor(t, 5*time.Second, func() bool {
		repo.mu.Lock()
		defer repo.mu.Unlock()
		return len(repo.fires) == 0
	}, "sweep of stale claim")
}
