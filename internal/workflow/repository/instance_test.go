package repository_test

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/simpwf/workflow-engine/internal/workflow/model"
	"github.com/simpwf/workflow-engine/internal/workflow/repository"
	"gorm.io/gorm"
)

const (
	fixtureUserID        = "bbbbbbbb-bbbb-7bbb-8bbb-bbbbbbbbbbbb"
	fixtureWorkflowDefID = "22222222-2222-7222-8222-222222222222"
	fixtureNodeDefID     = "88888888-8888-7888-8888-888888888888"
)

// seedInstanceFixture inserts the users/definitions rows the foreign keys of
// workflow_instances, node_instances, and events reference.
func seedInstanceFixture(t *testing.T, db *gorm.DB) {
	t.Helper()
	now := time.Now().UTC()
	u := repository.UserToModel(model.User{ID: fixtureUserID, Name: "fixture", Email: "fixture@localhost", CreatedAt: now, UpdatedAt: now})
	if err := db.Create(&u).Error; err != nil {
		t.Fatalf("seed user: %v", err)
	}
	wd := repository.WorkflowDefinitionToModel(model.WorkflowDefinition{
		ID: fixtureWorkflowDefID, Name: "fixture-flow", Version: 1,
		LineageID: fixtureWorkflowDefID, Content: json.RawMessage(`{}`),
		CreatedBy: fixtureUserID, UpdatedBy: fixtureUserID, CreatedAt: now, UpdatedAt: now,
	})
	if err := db.Create(&wd).Error; err != nil {
		t.Fatalf("seed workflow definition: %v", err)
	}
	nd := repository.NodeDefinitionToModel(model.NodeDefinition{
		ID: fixtureNodeDefID, Name: "fixture-node", Version: 1,
		LineageID: fixtureNodeDefID, Type: "script", Content: json.RawMessage(`{}`),
		CreatedBy: fixtureUserID, UpdatedBy: fixtureUserID, CreatedAt: now, UpdatedAt: now,
	})
	if err := db.Create(&nd).Error; err != nil {
		t.Fatalf("seed node definition: %v", err)
	}
}

func newTestInstance(id string, status model.WorkflowStatus, reason model.WaitingReason) model.WorkflowInstance {
	frame := model.NewFrame("node-1")
	frameRaw, _ := frame.JSON()
	return model.WorkflowInstance{
		ID:                   id,
		WorkflowDefinitionID: fixtureWorkflowDefID,
		Status:               status,
		WaitingReason:        reason,
		Frame:                frameRaw,
		Context:              json.RawMessage(`{}`),
		Counters:             json.RawMessage(`{}`),
		CreatedBy:            fixtureUserID,
		UpdatedBy:            fixtureUserID,
		CreatedAt:            time.Now().UTC(),
		UpdatedAt:            time.Now().UTC(),
	}
}

func insertInstance(t *testing.T, db *gorm.DB, w model.WorkflowInstance) {
	t.Helper()
	if err := repository.NewInstanceRepository(db).Insert(context.Background(), w); err != nil {
		t.Fatalf("insert instance: %v", err)
	}
}

func TestClaimNextClaimsOnlyRunnable(t *testing.T) {
	db := setupTestDB(t)
	seedInstanceFixture(t, db)
	ctx := context.Background()
	now := time.Now().UTC()

	insertInstance(t, db, newTestInstance("11111111-1111-7111-8111-111111111111", model.WorkflowWaiting, ""))
	insertInstance(t, db, newTestInstance("22222222-2222-7222-8222-222222222222", model.WorkflowWaiting, ""))
	// input-waiting instance must NOT be claimed
	insertInstance(t, db, newTestInstance("33333333-3333-7333-8333-333333333333", model.WorkflowWaiting, model.WaitingReasonInput))
	// running instance with an expired lease is recoverable
	expired := newTestInstance("44444444-4444-7444-8444-444444444444", model.WorkflowRunning, "")
	expired.LeasedBy = "dead-worker"
	expired.LeaseExpiry = now.Add(-time.Minute)
	insertInstance(t, db, expired)
	// running instance with an active lease must NOT be claimed
	active := newTestInstance("55555555-5555-7555-8555-555555555555", model.WorkflowRunning, "")
	active.LeasedBy = "other-worker"
	active.LeaseExpiry = now.Add(time.Minute)
	insertInstance(t, db, active)

	repo := repository.NewInstanceRepository(db)
	claimed, err := repo.ClaimNext(ctx, "worker-1", time.Minute, 10)
	if err != nil {
		t.Fatalf("ClaimNext() error = %v", err)
	}
	got := map[string]bool{}
	for _, w := range claimed {
		got[w.ID] = true
	}
	if !got["11111111-1111-7111-8111-111111111111"] || !got["22222222-2222-7222-8222-222222222222"] {
		t.Errorf("missing runnable claims: %v", got)
	}
	if got["33333333-3333-7333-8333-333333333333"] {
		t.Error("claimed input-waiting instance")
	}
	if !got["44444444-4444-7444-8444-444444444444"] {
		t.Error("did not reclaim expired-lease running instance")
	}
	if got["55555555-5555-7555-8555-555555555555"] {
		t.Error("claimed instance with active lease")
	}
	for _, w := range claimed {
		if w.Status != model.WorkflowRunning || w.LeasedBy != "worker-1" || w.LeaseExpiry.Before(now.Add(30*time.Second)) {
			t.Errorf("claim not fenced: %+v", w)
		}
	}
}

func TestClaimNextSetsStartedAtOnlyOnce(t *testing.T) {
	db := setupTestDB(t)
	seedInstanceFixture(t, db)
	ctx := context.Background()
	now := time.Now().UTC()

	freshID := "11111111-1111-7111-8111-111111111111"
	insertInstance(t, db, newTestInstance(freshID, model.WorkflowWaiting, ""))

	recoveredID := "22222222-2222-7222-8222-222222222222"
	started := now.Add(-time.Hour).Truncate(time.Microsecond)
	recovered := newTestInstance(recoveredID, model.WorkflowRunning, "")
	recovered.LeasedBy = "dead-worker"
	recovered.LeaseExpiry = now.Add(-time.Minute)
	recovered.StartedAt = &started
	insertInstance(t, db, recovered)

	beforeClaim := time.Now().UTC()
	claimed, err := repository.NewInstanceRepository(db).ClaimNext(ctx, "worker-1", time.Minute, 10)
	if err != nil {
		t.Fatalf("ClaimNext() error = %v", err)
	}
	afterClaim := time.Now().UTC()

	byID := make(map[string]model.WorkflowInstance, len(claimed))
	for _, w := range claimed {
		byID[w.ID] = w
	}
	fresh := byID[freshID]
	if fresh.StartedAt == nil || fresh.StartedAt.Before(beforeClaim) || fresh.StartedAt.After(afterClaim) {
		t.Errorf("fresh started_at = %v, want claim time in [%v, %v]", fresh.StartedAt, beforeClaim, afterClaim)
	}
	reclaimed := byID[recoveredID]
	if reclaimed.StartedAt == nil || !reclaimed.StartedAt.Equal(started) {
		t.Errorf("recovered started_at = %v, want preserved %v", reclaimed.StartedAt, started)
	}
}

func TestClaimNextSkipLockedConcurrent(t *testing.T) {
	db := setupTestDB(t)
	seedInstanceFixture(t, db)
	ctx := context.Background()
	ids := []string{
		"11111111-1111-7111-8111-111111111111",
		"22222222-2222-7222-8222-222222222222",
		"33333333-3333-7333-8333-333333333333",
		"44444444-4444-7444-8444-444444444444",
	}
	for _, id := range ids {
		insertInstance(t, db, newTestInstance(id, model.WorkflowWaiting, ""))
	}
	repo := repository.NewInstanceRepository(db)

	start := make(chan struct{})
	var a, b []model.WorkflowInstance
	var errA, errB error
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		<-start
		a, errA = repo.ClaimNext(ctx, "worker-a", time.Minute, 10)
	}()
	go func() {
		defer wg.Done()
		<-start
		b, errB = repo.ClaimNext(ctx, "worker-b", time.Minute, 10)
	}()
	close(start)
	wg.Wait()

	if errA != nil || errB != nil {
		t.Fatalf("claim errors: %v, %v", errA, errB)
	}
	if len(a)+len(b) != 4 {
		t.Errorf("total claims = %d, want 4", len(a)+len(b))
	}
	seen := map[string]bool{}
	for _, w := range append(a, b...) {
		if seen[w.ID] {
			t.Errorf("instance %s claimed by both workers", w.ID)
		}
		seen[w.ID] = true
	}
}

func TestCheckpointFencesAndPersists(t *testing.T) {
	db := setupTestDB(t)
	seedInstanceFixture(t, db)
	ctx := context.Background()
	insertInstance(t, db, newTestInstance("11111111-1111-7111-8111-111111111111", model.WorkflowWaiting, ""))

	repo := repository.NewInstanceRepository(db)
	claimed, err := repo.ClaimNext(ctx, "worker-1", time.Minute, 10)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("claim = %v, err %v", claimed, err)
	}
	w := claimed[0]

	frame := model.Frame{CurrentNodeID: "node-2", GroupStack: []string{"g1"}}
	cp := repository.Checkpoint{
		InstanceID:     w.ID,
		WorkerID:       "worker-1",
		Revision:       w.Revision,
		Status:         model.WorkflowWaiting,
		WaitingReason:  model.WaitingReasonRunnable,
		Frame:          frame,
		Counters:       model.Counters{Total: 1, Nodes: map[string]int{"node-1": 1}},
		Context:        json.RawMessage(`{"x":1}`),
		Error:          "",
		PauseRequested: false,
	}
	if err := repo.Checkpoint(ctx, cp); err != nil {
		t.Fatalf("Checkpoint() error = %v", err)
	}

	stored, err := repo.GetByID(ctx, w.ID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.Status != model.WorkflowWaiting || stored.WaitingReason != model.WaitingReasonRunnable {
		t.Errorf("status = %s/%s", stored.Status, stored.WaitingReason)
	}
	if stored.LeasedBy != "" || !stored.LeaseExpiry.IsZero() {
		t.Errorf("lease not released: %+v", stored)
	}
	gotFrame, err := model.ParseFrame(stored.Frame)
	if err != nil {
		t.Fatal(err)
	}
	if gotFrame.CurrentNodeID != "node-2" || len(gotFrame.GroupStack) != 1 {
		t.Errorf("frame = %+v", gotFrame)
	}
	gotCounters, err := model.ParseCounters(stored.Counters)
	if err != nil {
		t.Fatal(err)
	}
	if gotCounters.Total != 1 || gotCounters.Nodes["node-1"] != 1 {
		t.Errorf("counters = %+v", gotCounters)
	}
	if !jsonEqual(t, stored.Context, json.RawMessage(`{"x":1}`)) {
		t.Errorf("context = %s", stored.Context)
	}
	if stored.Revision != w.Revision+1 {
		t.Errorf("revision = %d, want %d", stored.Revision, w.Revision+1)
	}
}

func TestCheckpointRejectsWrongWorker(t *testing.T) {
	db := setupTestDB(t)
	seedInstanceFixture(t, db)
	ctx := context.Background()
	insertInstance(t, db, newTestInstance("11111111-1111-7111-8111-111111111111", model.WorkflowWaiting, ""))
	repo := repository.NewInstanceRepository(db)
	claimed, _ := repo.ClaimNext(ctx, "worker-1", time.Minute, 10)
	w := claimed[0]

	err := repo.Checkpoint(ctx, repository.Checkpoint{
		InstanceID: w.ID,
		WorkerID:   "worker-2",
		Revision:   w.Revision,
		Status:     model.WorkflowWaiting,
		Frame:      model.NewFrame("node-2"),
		Counters:   model.Counters{},
		Context:    json.RawMessage(`{}`),
	})
	if !errors.Is(err, repository.ErrLeaseLost) {
		t.Errorf("Checkpoint() error = %v, want ErrLeaseLost", err)
	}
}

func TestCheckpointRejectsStaleRevision(t *testing.T) {
	db := setupTestDB(t)
	seedInstanceFixture(t, db)
	ctx := context.Background()
	insertInstance(t, db, newTestInstance("11111111-1111-7111-8111-111111111111", model.WorkflowWaiting, ""))
	repo := repository.NewInstanceRepository(db)
	claimed, _ := repo.ClaimNext(ctx, "worker-1", time.Minute, 10)
	w := claimed[0]

	err := repo.Checkpoint(ctx, repository.Checkpoint{
		InstanceID: w.ID,
		WorkerID:   "worker-1",
		Revision:   w.Revision + 7,
		Status:     model.WorkflowWaiting,
		Frame:      model.NewFrame("node-2"),
		Counters:   model.Counters{},
		Context:    json.RawMessage(`{}`),
	})
	if !errors.Is(err, repository.ErrRevisionConflict) {
		t.Errorf("Checkpoint() error = %v, want ErrRevisionConflict", err)
	}
}

func TestCheckpointFencedAfterStop(t *testing.T) {
	db := setupTestDB(t)
	seedInstanceFixture(t, db)
	ctx := context.Background()
	insertInstance(t, db, newTestInstance("11111111-1111-7111-8111-111111111111", model.WorkflowWaiting, ""))
	repo := repository.NewInstanceRepository(db)
	claimed, _ := repo.ClaimNext(ctx, "worker-1", time.Minute, 10)
	w := claimed[0]

	if _, err := repo.Stop(ctx, w.ID, "operator"); err != nil {
		t.Fatalf("Stop() error = %v", err)
	}
	err := repo.Checkpoint(ctx, repository.Checkpoint{
		InstanceID: w.ID,
		WorkerID:   "worker-1",
		Revision:   w.Revision,
		Status:     model.WorkflowWaiting,
		Frame:      model.NewFrame("node-2"),
		Counters:   model.Counters{},
		Context:    json.RawMessage(`{}`),
	})
	if !errors.Is(err, repository.ErrLeaseLost) {
		t.Errorf("Checkpoint() error = %v, want ErrLeaseLost after stop", err)
	}
}

func TestCheckpointTerminalStatus(t *testing.T) {
	db := setupTestDB(t)
	seedInstanceFixture(t, db)
	ctx := context.Background()
	insertInstance(t, db, newTestInstance("11111111-1111-7111-8111-111111111111", model.WorkflowWaiting, ""))
	repo := repository.NewInstanceRepository(db)
	claimed, _ := repo.ClaimNext(ctx, "worker-1", time.Minute, 10)
	w := claimed[0]

	done := time.Now().UTC()
	cp := repository.Checkpoint{
		InstanceID:     w.ID,
		WorkerID:       "worker-1",
		Revision:       w.Revision,
		Status:         model.WorkflowFinished,
		Frame:          model.NewFrame(""),
		Counters:       model.Counters{Total: 1},
		Context:        json.RawMessage(`{"x":1}`),
		FinishedAt:     &done,
		PauseRequested: false,
	}
	if err := repo.Checkpoint(ctx, cp); err != nil {
		t.Fatalf("Checkpoint(finished) error = %v", err)
	}
	stored, _ := repo.GetByID(ctx, w.ID)
	if stored.Status != model.WorkflowFinished || stored.FinishedAt == nil || stored.LeasedBy != "" {
		t.Errorf("stored = %+v", stored)
	}
}

func TestPauseResumeStop(t *testing.T) {
	db := setupTestDB(t)
	seedInstanceFixture(t, db)
	ctx := context.Background()
	repo := repository.NewInstanceRepository(db)

	// immediate pause of a runnable waiting instance
	insertInstance(t, db, newTestInstance("11111111-1111-7111-8111-111111111111", model.WorkflowWaiting, ""))
	deferred, err := repo.Pause(ctx, "11111111-1111-7111-8111-111111111111")
	if err != nil {
		t.Fatal(err)
	}
	if deferred {
		t.Error("pause of waiting instance reported deferred")
	}
	stored, _ := repo.GetByID(ctx, "11111111-1111-7111-8111-111111111111")
	if stored.Status != model.WorkflowPaused {
		t.Errorf("status = %s, want paused", stored.Status)
	}
	// idempotent pause while paused
	if _, err := repo.Pause(ctx, "11111111-1111-7111-8111-111111111111"); err != nil {
		t.Errorf("second pause error = %v, want nil", err)
	}

	// resume -> runnable again
	if err := repo.Resume(ctx, "11111111-1111-7111-8111-111111111111"); err != nil {
		t.Fatal(err)
	}
	stored, _ = repo.GetByID(ctx, "11111111-1111-7111-8111-111111111111")
	if stored.Status != model.WorkflowWaiting {
		t.Errorf("status = %s, want waiting", stored.Status)
	}

	// deferred pause of a running instance sets pause_requested
	insertInstance(t, db, newTestInstance("22222222-2222-7222-8222-222222222222", model.WorkflowRunning, ""))
	deferred, err = repo.Pause(ctx, "22222222-2222-7222-8222-222222222222")
	if err != nil || !deferred {
		t.Fatalf("pause of running = deferred %v, err %v", deferred, err)
	}
	stored, _ = repo.GetByID(ctx, "22222222-2222-7222-8222-222222222222")
	if !stored.PauseRequested || stored.Status != model.WorkflowRunning {
		t.Errorf("deferred pause not recorded: %+v", stored)
	}

	// stop from paused is terminal
	if _, err := repo.Stop(ctx, "11111111-1111-7111-8111-111111111111", "operator"); err != nil {
		t.Fatal(err)
	}
	stored, _ = repo.GetByID(ctx, "11111111-1111-7111-8111-111111111111")
	if stored.Status != model.WorkflowStopped {
		t.Errorf("status = %s, want stopped", stored.Status)
	}
	// stop is idempotent
	if _, err := repo.Stop(ctx, "11111111-1111-7111-8111-111111111111", "operator"); err != nil {
		t.Errorf("second stop error = %v, want nil", err)
	}
	// terminal instances reject further transitions
	if err := repo.Resume(ctx, "11111111-1111-7111-8111-111111111111"); !errors.Is(err, repository.ErrStatusConflict) {
		t.Errorf("resume stopped = %v, want ErrStatusConflict", err)
	}
	if _, err := repo.Pause(ctx, "11111111-1111-7111-8111-111111111111"); !errors.Is(err, repository.ErrStatusConflict) {
		t.Errorf("pause stopped = %v, want ErrStatusConflict", err)
	}
}

func TestPausePreservesInputWaiting(t *testing.T) {
	db := setupTestDB(t)
	seedInstanceFixture(t, db)
	ctx := context.Background()
	insertInstance(t, db, newTestInstance("11111111-1111-7111-8111-111111111111", model.WorkflowWaiting, model.WaitingReasonInput))
	repo := repository.NewInstanceRepository(db)
	if _, err := repo.Pause(ctx, "11111111-1111-7111-8111-111111111111"); err != nil {
		t.Fatal(err)
	}
	stored, _ := repo.GetByID(ctx, "11111111-1111-7111-8111-111111111111")
	if stored.Status != model.WorkflowPaused || stored.WaitingReason != model.WaitingReasonInput {
		t.Errorf("stored = %+v, want paused with input reason preserved", stored)
	}
	if err := repo.Resume(ctx, "11111111-1111-7111-8111-111111111111"); err != nil {
		t.Fatal(err)
	}
	stored, _ = repo.GetByID(ctx, "11111111-1111-7111-8111-111111111111")
	if stored.Status != model.WorkflowWaiting || stored.WaitingReason != model.WaitingReasonInput {
		t.Errorf("stored = %+v, want waiting with input reason preserved", stored)
	}
}

func TestRenewLease(t *testing.T) {
	db := setupTestDB(t)
	seedInstanceFixture(t, db)
	ctx := context.Background()
	insertInstance(t, db, newTestInstance("11111111-1111-7111-8111-111111111111", model.WorkflowWaiting, ""))
	repo := repository.NewInstanceRepository(db)
	claimed, _ := repo.ClaimNext(ctx, "worker-1", time.Minute, 10)
	w := claimed[0]

	if err := repo.RenewLease(ctx, w.ID, "worker-1", 2*time.Minute); err != nil {
		t.Fatalf("RenewLease() error = %v", err)
	}
	stored, _ := repo.GetByID(ctx, w.ID)
	if !stored.LeaseExpiry.After(time.Now().Add(90 * time.Second)) {
		t.Errorf("lease not extended: %+v", stored.LeaseExpiry)
	}
	if err := repo.RenewLease(ctx, w.ID, "intruder", time.Minute); !errors.Is(err, repository.ErrLeaseLost) {
		t.Errorf("RenewLease(intruder) error = %v, want ErrLeaseLost", err)
	}
}

func TestRenewLeasesHeartbeat(t *testing.T) {
	db := setupTestDB(t)
	seedInstanceFixture(t, db)
	ctx := context.Background()
	repo := repository.NewInstanceRepository(db)
	insertInstance(t, db, newTestInstance("11111111-1111-7111-8111-111111111111", model.WorkflowWaiting, ""))
	insertInstance(t, db, newTestInstance("22222222-2222-7222-8222-222222222222", model.WorkflowWaiting, ""))
	claimed, err := repo.ClaimNext(ctx, "worker-1", time.Minute, 10)
	if err != nil || len(claimed) != 2 {
		t.Fatalf("claim = %d, err %v", len(claimed), err)
	}
	if err := repo.RenewLeases(ctx, "worker-1", 5*time.Minute); err != nil {
		t.Fatalf("RenewLeases() error = %v", err)
	}
	for _, w := range claimed {
		stored, err := repo.GetByID(ctx, w.ID)
		if err != nil {
			t.Fatal(err)
		}
		if !stored.LeaseExpiry.After(time.Now().Add(4 * time.Minute)) {
			t.Errorf("lease of %s not extended: %v", w.ID, stored.LeaseExpiry)
		}
	}
}

func TestNodeInstanceCRUDAndEvents(t *testing.T) {
	db := setupTestDB(t)
	seedInstanceFixture(t, db)
	ctx := context.Background()
	insertInstance(t, db, newTestInstance("11111111-1111-7111-8111-111111111111", model.WorkflowWaiting, ""))
	repo := repository.NewInstanceRepository(db)

	now := time.Now().UTC()
	n := model.NodeInstance{
		ID:                 "99999999-9999-7999-8999-999999999999",
		WorkflowInstanceID: "11111111-1111-7111-8111-111111111111",
		NodeID:             "77777777-7777-7777-8777-777777777777",
		NodeDefinitionID:   fixtureNodeDefID,
		Name:               "transform",
		Type:               string(model.NodeTypeScript),
		Attempt:            1,
		Status:             model.NodeRunning,
		Input:              json.RawMessage(`null`),
		Output:             json.RawMessage(`null`),
		ContextBefore:      json.RawMessage(`{"a":1}`),
		ContextAfter:       json.RawMessage(`null`),
		StartedAt:          &now,
		CreatedAt:          now,
		UpdatedAt:          now,
	}
	if err := repo.InsertNodeInstance(ctx, n); err != nil {
		t.Fatalf("InsertNodeInstance() error = %v", err)
	}

	got, err := repo.GetNodeInstance(ctx, n.WorkflowInstanceID, n.ID)
	if err != nil {
		t.Fatalf("GetNodeInstance() error = %v", err)
	}
	if got.Name != "transform" || got.Attempt != 1 || got.Status != model.NodeRunning || got.NodeID != n.NodeID {
		t.Errorf("got = %+v", got)
	}

	byNode, err := repo.GetNodeInstanceByNode(ctx, n.WorkflowInstanceID, n.NodeID)
	if err != nil || byNode.ID != n.ID {
		t.Fatalf("GetNodeInstanceByNode() = %v, err %v", byNode, err)
	}
	running, err := repo.GetRunningNodeInstance(ctx, n.WorkflowInstanceID)
	if err != nil || running.ID != n.ID {
		t.Fatalf("GetRunningNodeInstance() = %v, err %v", running, err)
	}

	n.Status = model.NodeFinished
	n.Output = json.RawMessage(`{"ok":true}`)
	n.ContextAfter = json.RawMessage(`{"a":2}`)
	finished := now.Add(time.Minute)
	n.FinishedAt = &finished
	if err := repo.UpdateNodeInstance(ctx, n); err != nil {
		t.Fatalf("UpdateNodeInstance() error = %v", err)
	}
	got, _ = repo.GetNodeInstance(ctx, n.WorkflowInstanceID, n.ID)
	if got.Status != model.NodeFinished || !jsonEqual(t, got.Output, json.RawMessage(`{"ok":true}`)) {
		t.Errorf("after update: %+v", got)
	}

	list, err := repo.ListNodeInstances(ctx, n.WorkflowInstanceID)
	if err != nil || len(list) != 1 {
		t.Fatalf("ListNodeInstances() = %d, err %v", len(list), err)
	}

	e := model.WorkflowInstanceEvent{
		ID:                 "aaaaaaaa-aaaa-7aaa-8aaa-aaaaaaaaaaaa",
		WorkflowInstanceID: n.WorkflowInstanceID,
		Type:               "node_finished",
		Data:               json.RawMessage(`{"node":"x"}`),
		CreatedBy:          fixtureUserID,
		CreatedAt:          now,
	}
	if err := repo.AppendEvent(ctx, e); err != nil {
		t.Fatalf("AppendEvent() error = %v", err)
	}
	events, err := repo.ListEvents(ctx, n.WorkflowInstanceID)
	if err != nil || len(events) != 1 || events[0].Type != "node_finished" {
		t.Fatalf("ListEvents() = %v, err %v", events, err)
	}
}

func TestStopRunningSetsTerminationPending(t *testing.T) {
	db := setupTestDB(t)
	seedInstanceFixture(t, db)
	ctx := context.Background()
	insertInstance(t, db, newTestInstance("11111111-1111-7111-8111-111111111111", model.WorkflowRunning, ""))
	repo := repository.NewInstanceRepository(db)

	pending, err := repo.Stop(ctx, "11111111-1111-7111-8111-111111111111", "operator")
	if err != nil {
		t.Fatalf("Stop() error = %v", err)
	}
	if !pending {
		t.Error("Stop(running) pending = false, want true")
	}
	stored, _ := repo.GetByID(ctx, "11111111-1111-7111-8111-111111111111")
	if stored.Status != model.WorkflowStopped || !stored.TerminationPending {
		t.Errorf("stored = %+v, want stopped with termination pending", stored)
	}
	if stored.LeasedBy != "" || !stored.LeaseExpiry.IsZero() {
		t.Errorf("stored = %+v, want lease fenced", stored)
	}
}

func TestStopIdempotentPreservesPending(t *testing.T) {
	db := setupTestDB(t)
	seedInstanceFixture(t, db)
	ctx := context.Background()
	insertInstance(t, db, newTestInstance("11111111-1111-7111-8111-111111111111", model.WorkflowRunning, ""))
	repo := repository.NewInstanceRepository(db)

	if _, err := repo.Stop(ctx, "11111111-1111-7111-8111-111111111111", "operator"); err != nil {
		t.Fatal(err)
	}
	again, err := repo.Stop(ctx, "11111111-1111-7111-8111-111111111111", "operator")
	if err != nil {
		t.Errorf("second Stop() error = %v, want nil", err)
	}
	if !again {
		t.Error("second Stop() pending = false, want current pending true")
	}
}

func TestStopWaitingHasNoPending(t *testing.T) {
	db := setupTestDB(t)
	seedInstanceFixture(t, db)
	ctx := context.Background()
	insertInstance(t, db, newTestInstance("11111111-1111-7111-8111-111111111111", model.WorkflowWaiting, ""))
	repo := repository.NewInstanceRepository(db)

	pending, err := repo.Stop(ctx, "11111111-1111-7111-8111-111111111111", "operator")
	if err != nil || pending {
		t.Fatalf("Stop(waiting) = pending %v, err %v, want false/nil", pending, err)
	}
}

func TestListTerminationPendingAndResolve(t *testing.T) {
	db := setupTestDB(t)
	seedInstanceFixture(t, db)
	ctx := context.Background()
	insertInstance(t, db, newTestInstance("11111111-1111-7111-8111-111111111111", model.WorkflowRunning, ""))
	insertInstance(t, db, newTestInstance("22222222-2222-7222-8222-222222222222", model.WorkflowWaiting, ""))
	repo := repository.NewInstanceRepository(db)

	if _, err := repo.Stop(ctx, "11111111-1111-7111-8111-111111111111", "operator"); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.Stop(ctx, "22222222-2222-7222-8222-222222222222", "operator"); err != nil {
		t.Fatal(err)
	}

	pending, err := repo.ListTerminationPending(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 1 || pending[0] != "11111111-1111-7111-8111-111111111111" {
		t.Errorf("pending = %v, want only the running-stop instance", pending)
	}

	if err := repo.ResolveTermination(ctx, "11111111-1111-7111-8111-111111111111"); err != nil {
		t.Fatal(err)
	}
	stored, _ := repo.GetByID(ctx, "11111111-1111-7111-8111-111111111111")
	if stored.TerminationPending {
		t.Errorf("stored = %+v, want pending cleared", stored)
	}
	if pending, _ := repo.ListTerminationPending(ctx); len(pending) != 0 {
		t.Errorf("pending after resolve = %v, want empty", pending)
	}
}

func TestSweepTerminationClearsFinishedCleanups(t *testing.T) {
	db := setupTestDB(t)
	seedInstanceFixture(t, db)
	ctx := context.Background()
	insertInstance(t, db, newTestInstance("11111111-1111-7111-8111-111111111111", model.WorkflowRunning, ""))
	repo := repository.NewInstanceRepository(db)

	if _, err := repo.Stop(ctx, "11111111-1111-7111-8111-111111111111", "operator"); err != nil {
		t.Fatal(err)
	}
	// No running node attempt exists: nothing is left to cancel, so the
	// sweep clears the termination flag.
	if err := repo.SweepTermination(ctx); err != nil {
		t.Fatal(err)
	}
	stored, _ := repo.GetByID(ctx, "11111111-1111-7111-8111-111111111111")
	if stored.TerminationPending {
		t.Errorf("stored = %+v, want sweep cleared pending", stored)
	}
}

func TestSweepKeepsPendingWhileNodeRuns(t *testing.T) {
	db := setupTestDB(t)
	seedInstanceFixture(t, db)
	ctx := context.Background()
	insertInstance(t, db, newTestInstance("11111111-1111-7111-8111-111111111111", model.WorkflowRunning, ""))
	repo := repository.NewInstanceRepository(db)
	if _, err := repo.Stop(ctx, "11111111-1111-7111-8111-111111111111", "operator"); err != nil {
		t.Fatal(err)
	}
	// A running node attempt means the worker still needs cancellation.
	n := model.NodeInstance{
		ID:                 "33333333-3333-7333-8333-333333333333",
		WorkflowInstanceID: "11111111-1111-7111-8111-111111111111",
		NodeID:             "33333333-3333-7333-8333-333333333333",
		Name:               "spin",
		Type:               "script",
		Attempt:            1,
		Status:             model.NodeRunning,
		CreatedAt:          time.Now().UTC(),
		UpdatedAt:          time.Now().UTC(),
	}
	if err := repo.InsertNodeInstance(ctx, n); err != nil {
		t.Fatal(err)
	}
	if err := repo.SweepTermination(ctx); err != nil {
		t.Fatal(err)
	}
	stored, _ := repo.GetByID(ctx, "11111111-1111-7111-8111-111111111111")
	if !stored.TerminationPending {
		t.Errorf("stored = %+v, want pending kept while node runs", stored)
	}
}

func TestReplaceContextPausedReplacesFullContext(t *testing.T) {
	db := setupTestDB(t)
	seedInstanceFixture(t, db)
	ctx := context.Background()
	repo := repository.NewInstanceRepository(db)

	w := newTestInstance("11111111-1111-7111-8111-111111111111", model.WorkflowPaused, "")
	w.Context = json.RawMessage(`{"old":1}`)
	w.UpdatedAt = time.Now().UTC().Add(-time.Hour)
	insertInstance(t, db, w)

	got, err := repo.ReplaceContext(ctx, repository.ContextUpdate{
		InstanceID: w.ID,
		Context:    json.RawMessage(`{"new":2}`),
		Actor:      fixtureUserID,
		Reason:     "debugging",
	})
	if err != nil {
		t.Fatalf("ReplaceContext() error = %v", err)
	}
	if got.ID != w.ID || !jsonEqual(t, got.Context, json.RawMessage(`{"new":2}`)) {
		t.Errorf("got = %+v, want replaced context", got)
	}

	stored, err := repo.GetByID(ctx, w.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !jsonEqual(t, stored.Context, json.RawMessage(`{"new":2}`)) {
		t.Errorf("context = %s, want full replacement", stored.Context)
	}
	var m map[string]any
	_ = json.Unmarshal(stored.Context, &m)
	if _, ok := m["old"]; ok {
		t.Errorf("context = %s, old key survived replacement", stored.Context)
	}
	if stored.Revision != w.Revision+1 {
		t.Errorf("revision = %d, want %d", stored.Revision, w.Revision+1)
	}
	if !stored.UpdatedAt.After(w.UpdatedAt) {
		t.Errorf("updated_at = %v, want after %v", stored.UpdatedAt, w.UpdatedAt)
	}
}

func TestReplaceContextAppendsAuditEvent(t *testing.T) {
	db := setupTestDB(t)
	seedInstanceFixture(t, db)
	ctx := context.Background()
	repo := repository.NewInstanceRepository(db)
	insertInstance(t, db, newTestInstance("11111111-1111-7111-8111-111111111111", model.WorkflowPaused, ""))

	if _, err := repo.ReplaceContext(ctx, repository.ContextUpdate{
		InstanceID: "11111111-1111-7111-8111-111111111111",
		Context:    json.RawMessage(`{"x":1}`),
		Actor:      fixtureUserID,
		Reason:     "urgent fix",
	}); err != nil {
		t.Fatal(err)
	}
	events, err := repo.ListEvents(ctx, "11111111-1111-7111-8111-111111111111")
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 || events[0].Type != "context_updated" || events[0].CreatedBy != fixtureUserID {
		t.Fatalf("events = %+v, want single context_updated by actor", events)
	}
	var data map[string]any
	if err := json.Unmarshal(events[0].Data, &data); err != nil {
		t.Fatal(err)
	}
	if data["reason"] != "urgent fix" {
		t.Errorf("event data = %v, want reason recorded", data)
	}
	if _, ok := data["context"]; ok {
		t.Errorf("event data = %v, context must not be duplicated into audit event", data)
	}
}

func TestReplaceContextAppendsEventWithoutReason(t *testing.T) {
	db := setupTestDB(t)
	seedInstanceFixture(t, db)
	ctx := context.Background()
	repo := repository.NewInstanceRepository(db)
	insertInstance(t, db, newTestInstance("11111111-1111-7111-8111-111111111111", model.WorkflowPaused, ""))

	if _, err := repo.ReplaceContext(ctx, repository.ContextUpdate{
		InstanceID: "11111111-1111-7111-8111-111111111111",
		Context:    json.RawMessage(`{"x":1}`),
		Actor:      fixtureUserID,
	}); err != nil {
		t.Fatal(err)
	}
	events, err := repo.ListEvents(ctx, "11111111-1111-7111-8111-111111111111")
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 {
		t.Fatalf("events = %+v, want exactly one", events)
	}
	var data map[string]any
	if err := json.Unmarshal(events[0].Data, &data); err != nil {
		t.Fatal(err)
	}
	if len(data) != 0 {
		t.Errorf("event data = %v, want empty when no reason", data)
	}
}

func TestReplaceContextRejectsNonPaused(t *testing.T) {
	db := setupTestDB(t)
	seedInstanceFixture(t, db)
	ctx := context.Background()
	repo := repository.NewInstanceRepository(db)

	cases := []struct {
		id     string
		status model.WorkflowStatus
	}{
		{"11111111-1111-7111-8111-111111111111", model.WorkflowWaiting},
		{"22222222-2222-7222-8222-222222222222", model.WorkflowRunning},
		{"33333333-3333-7333-8333-333333333333", model.WorkflowFinished},
		{"44444444-4444-7444-8444-444444444444", model.WorkflowFailed},
		{"55555555-5555-7555-8555-555555555555", model.WorkflowStopped},
	}
	for _, tc := range cases {
		insertInstance(t, db, newTestInstance(tc.id, tc.status, ""))
		_, err := repo.ReplaceContext(ctx, repository.ContextUpdate{
			InstanceID: tc.id,
			Context:    json.RawMessage(`{"x":1}`),
			Actor:      fixtureUserID,
		})
		if !errors.Is(err, repository.ErrStatusConflict) {
			t.Errorf("ReplaceContext(%s) error = %v, want ErrStatusConflict", tc.status, err)
		}
		stored, err := repo.GetByID(ctx, tc.id)
		if err != nil {
			t.Fatal(err)
		}
		if !jsonEqual(t, stored.Context, json.RawMessage(`{}`)) {
			t.Errorf("context of %s = %s, want unchanged", tc.status, stored.Context)
		}
		if stored.Revision != 0 {
			t.Errorf("revision of %s = %d, want unchanged", tc.status, stored.Revision)
		}
	}
}

func TestRollbackInstancePausedMovesCursorAndRestoresContext(t *testing.T) {
	db := setupTestDB(t)
	seedInstanceFixture(t, db)
	ctx := context.Background()
	repo := repository.NewInstanceRepository(db)

	id := "11111111-1111-7111-8111-111111111111"
	w := newTestInstance(id, model.WorkflowPaused, "")
	w.Context = json.RawMessage(`{"after":true}`)
	w.PauseRequested = true
	insertInstance(t, db, w)

	frame := model.Frame{
		CurrentNodeID: "22222222-2222-7222-8222-222222222222",
		GroupStack:    []string{"33333333-3333-7333-8333-333333333333"},
	}
	got, err := repo.RollbackInstance(ctx, repository.RollbackUpdate{
		InstanceID:    id,
		Frame:         frame,
		Context:       json.RawMessage(`{"before":1}`),
		Actor:         fixtureUserID,
		Reason:        "retry from earlier node",
		FromNode:      "44444444-4444-7444-8444-444444444444",
		ToNode:        "22222222-2222-7222-8222-222222222222",
		ToOccurrence:  "55555555-5555-7555-8555-555555555555",
		WaitingReason: model.WaitingReasonRunnable,
	})
	if err != nil {
		t.Fatalf("RollbackInstance() error = %v", err)
	}
	if got.Status != model.WorkflowPaused || got.WaitingReason != model.WaitingReasonRunnable {
		t.Errorf("got status = %s/%q, want paused/runnable", got.Status, got.WaitingReason)
	}
	if got.PauseRequested {
		t.Error("pause_requested = true, want false")
	}
	gotFrame, err := model.ParseFrame(got.Frame)
	if err != nil {
		t.Fatal(err)
	}
	if gotFrame.CurrentNodeID != "22222222-2222-7222-8222-222222222222" ||
		len(gotFrame.GroupStack) != 1 || gotFrame.GroupStack[0] != "33333333-3333-7333-8333-333333333333" {
		t.Errorf("frame = %+v, want target cursor", gotFrame)
	}
	if !jsonEqual(t, got.Context, json.RawMessage(`{"before":1}`)) {
		t.Errorf("context = %s, want restored", got.Context)
	}
	if got.Revision != w.Revision+1 {
		t.Errorf("revision = %d, want %d", got.Revision, w.Revision+1)
	}

	events, err := repo.ListEvents(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 || events[0].Type != "rollback" {
		t.Fatalf("events = %+v, want one rollback event", events)
	}
	var data map[string]any
	if err := json.Unmarshal(events[0].Data, &data); err != nil {
		t.Fatal(err)
	}
	want := map[string]any{
		"from_node":     "44444444-4444-7444-8444-444444444444",
		"to_node":       "22222222-2222-7222-8222-222222222222",
		"to_occurrence": "55555555-5555-7555-8555-555555555555",
		"context_mode":  "restore",
		"reason":        "retry from earlier node",
	}
	if !reflect.DeepEqual(data, want) {
		t.Errorf("event data = %v, want %v", data, want)
	}
}

func TestRollbackInstanceFailedClearsErrorAndFinishedAt(t *testing.T) {
	db := setupTestDB(t)
	seedInstanceFixture(t, db)
	ctx := context.Background()
	repo := repository.NewInstanceRepository(db)

	id := "11111111-1111-7111-8111-111111111111"
	w := newTestInstance(id, model.WorkflowFailed, "")
	w.Context = json.RawMessage(`{"after":true}`)
	w.Error = "node boom"
	finished := time.Now().UTC().Add(-time.Minute).Truncate(time.Microsecond)
	w.FinishedAt = &finished
	started := finished.Add(-time.Minute)
	w.StartedAt = &started
	w.LeasedBy = "dead-worker"
	insertInstance(t, db, w)

	frame := model.Frame{CurrentNodeID: "22222222-2222-7222-8222-222222222222"}
	got, err := repo.RollbackInstance(ctx, repository.RollbackUpdate{
		InstanceID:    id,
		Frame:         frame,
		Context:       json.RawMessage(`{}`),
		Actor:         fixtureUserID,
		FromNode:      "44444444-4444-7444-8444-444444444444",
		ToNode:        "22222222-2222-7222-8222-222222222222",
		ToOccurrence:  "55555555-5555-7555-8555-555555555555",
		WaitingReason: model.WaitingReasonRunnable,
	})
	if err != nil {
		t.Fatalf("RollbackInstance() error = %v", err)
	}
	if got.Status != model.WorkflowPaused {
		t.Errorf("status = %s, want paused", got.Status)
	}
	if got.Error != "" {
		t.Errorf("error = %q, want cleared", got.Error)
	}
	if got.FinishedAt != nil {
		t.Errorf("finished_at = %v, want nil", got.FinishedAt)
	}
	if got.StartedAt == nil || !got.StartedAt.Equal(started) {
		t.Errorf("started_at = %v, want preserved %v", got.StartedAt, started)
	}
	if got.LeasedBy != "dead-worker" {
		t.Errorf("leased_by = %q, want untouched", got.LeasedBy)
	}
	if got.Revision != w.Revision+1 {
		t.Errorf("revision = %d, want %d", got.Revision, w.Revision+1)
	}

	events, err := repo.ListEvents(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 || events[0].Type != "rollback" {
		t.Fatalf("events = %+v, want one rollback event", events)
	}
	var data map[string]any
	if err := json.Unmarshal(events[0].Data, &data); err != nil {
		t.Fatal(err)
	}
	if data["reason"] != "" || data["context_mode"] != "restore" {
		t.Errorf("event data = %v, want empty reason and restore mode", data)
	}
}

func TestRollbackInstanceRejectsWrongState(t *testing.T) {
	db := setupTestDB(t)
	seedInstanceFixture(t, db)
	ctx := context.Background()
	repo := repository.NewInstanceRepository(db)

	cases := []struct {
		id     string
		status model.WorkflowStatus
	}{
		{"11111111-1111-7111-8111-111111111111", model.WorkflowWaiting},
		{"22222222-2222-7222-8222-222222222222", model.WorkflowRunning},
		{"33333333-3333-7333-8333-333333333333", model.WorkflowFinished},
		{"44444444-4444-7444-8444-444444444444", model.WorkflowStopped},
	}
	for _, tc := range cases {
		insertInstance(t, db, newTestInstance(tc.id, tc.status, ""))
		_, err := repo.RollbackInstance(ctx, repository.RollbackUpdate{
			InstanceID:    tc.id,
			Frame:         model.Frame{CurrentNodeID: "22222222-2222-7222-8222-222222222222"},
			Context:       json.RawMessage(`{}`),
			Actor:         fixtureUserID,
			ToNode:        "22222222-2222-7222-8222-222222222222",
			ToOccurrence:  "55555555-5555-7555-8555-555555555555",
			WaitingReason: model.WaitingReasonRunnable,
		})
		if !errors.Is(err, repository.ErrStatusConflict) {
			t.Errorf("RollbackInstance(%s) error = %v, want ErrStatusConflict", tc.status, err)
		}
		stored, err := repo.GetByID(ctx, tc.id)
		if err != nil {
			t.Fatal(err)
		}
		if stored.Revision != 0 {
			t.Errorf("revision of %s = %d, want unchanged", tc.status, stored.Revision)
		}
		if events, _ := repo.ListEvents(ctx, tc.id); len(events) != 0 {
			t.Errorf("events of %s = %d, want none", tc.status, len(events))
		}
	}
}

func TestRollbackInstanceRejectsTerminationPending(t *testing.T) {
	db := setupTestDB(t)
	seedInstanceFixture(t, db)
	ctx := context.Background()
	repo := repository.NewInstanceRepository(db)

	id := "11111111-1111-7111-8111-111111111111"
	w := newTestInstance(id, model.WorkflowPaused, "")
	w.TerminationPending = true
	insertInstance(t, db, w)

	_, err := repo.RollbackInstance(ctx, repository.RollbackUpdate{
		InstanceID:    id,
		Frame:         model.Frame{CurrentNodeID: "22222222-2222-7222-8222-222222222222"},
		Context:       json.RawMessage(`{}`),
		Actor:         fixtureUserID,
		ToNode:        "22222222-2222-7222-8222-222222222222",
		ToOccurrence:  "55555555-5555-7555-8555-555555555555",
		WaitingReason: model.WaitingReasonRunnable,
	})
	if !errors.Is(err, repository.ErrStatusConflict) {
		t.Errorf("RollbackInstance() error = %v, want ErrStatusConflict", err)
	}
}

func TestRollbackInstanceMissingInstance(t *testing.T) {
	db := setupTestDB(t)
	seedInstanceFixture(t, db)
	ctx := context.Background()
	repo := repository.NewInstanceRepository(db)

	_, err := repo.RollbackInstance(ctx, repository.RollbackUpdate{
		InstanceID:    "99999999-9999-7999-8999-999999999999",
		Frame:         model.Frame{CurrentNodeID: "22222222-2222-7222-8222-222222222222"},
		Context:       json.RawMessage(`{}`),
		Actor:         fixtureUserID,
		ToNode:        "22222222-2222-7222-8222-222222222222",
		ToOccurrence:  "55555555-5555-7555-8555-555555555555",
		WaitingReason: model.WaitingReasonRunnable,
	})
	if !errors.Is(err, repository.ErrInstanceNotFound) {
		t.Errorf("RollbackInstance() error = %v, want ErrInstanceNotFound", err)
	}
}

func TestRollbackInstanceEnqueuesNoStatusUpdate(t *testing.T) {
	db := setupTestDB(t)
	seedInstanceFixture(t, db)
	ctx := context.Background()
	repo := repository.NewInstanceRepository(db)

	id := "11111111-1111-7111-8111-111111111111"
	insertInstance(t, db, newTestInstance(id, model.WorkflowPaused, ""))

	_, err := repo.RollbackInstance(ctx, repository.RollbackUpdate{
		InstanceID:    id,
		Frame:         model.Frame{CurrentNodeID: "22222222-2222-7222-8222-222222222222"},
		Context:       json.RawMessage(`{}`),
		Actor:         fixtureUserID,
		ToNode:        "22222222-2222-7222-8222-222222222222",
		ToOccurrence:  "55555555-5555-7555-8555-555555555555",
		WaitingReason: model.WaitingReasonRunnable,
	})
	if err != nil {
		t.Fatalf("RollbackInstance() error = %v", err)
	}
	var count int64
	if err := db.Model(&repository.StatusUpdateOutboxModel{}).Count(&count).Error; err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Errorf("status_update_outbox rows = %d, want 0", count)
	}
}

func TestRollbackInstancePreservesInputParkReason(t *testing.T) {
	db := setupTestDB(t)
	seedInstanceFixture(t, db)
	ctx := context.Background()
	repo := repository.NewInstanceRepository(db)

	id := "11111111-1111-7111-8111-111111111111"
	w := newTestInstance(id, model.WorkflowPaused, model.WaitingReasonInput)
	insertInstance(t, db, w)

	got, err := repo.RollbackInstance(ctx, repository.RollbackUpdate{
		InstanceID:    id,
		Frame:         model.Frame{CurrentNodeID: "22222222-2222-7222-8222-222222222222"},
		Context:       json.RawMessage(`{}`),
		Actor:         fixtureUserID,
		ToNode:        "22222222-2222-7222-8222-222222222222",
		ToOccurrence:  "55555555-5555-7555-8555-555555555555",
		WaitingReason: model.WaitingReasonInput,
	})
	if err != nil {
		t.Fatalf("RollbackInstance() error = %v", err)
	}
	if got.Status != model.WorkflowPaused || got.WaitingReason != model.WaitingReasonInput {
		t.Errorf("got status = %s/%q, want paused/input", got.Status, got.WaitingReason)
	}
	stored, err := repo.GetByID(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if stored.WaitingReason != model.WaitingReasonInput {
		t.Errorf("stored waiting_reason = %q, want input", stored.WaitingReason)
	}
}

func TestRollbackInstanceRearmsFinishedInputOccurrence(t *testing.T) {
	db := setupTestDB(t)
	seedInstanceFixture(t, db)
	ctx := context.Background()
	repo := repository.NewInstanceRepository(db)

	id := "11111111-1111-7111-8111-111111111111"
	w := newTestInstance(id, model.WorkflowPaused, "")
	insertInstance(t, db, w)
	occID := "55555555-5555-7555-8555-555555555555"
	if err := repo.InsertNodeInstance(ctx, model.NodeInstance{
		ID:                 occID,
		WorkflowInstanceID: id,
		NodeID:             "22222222-2222-7222-8222-222222222222",
		Name:               "ask",
		Type:               string(model.NodeTypeInput),
		Attempt:            1,
		Status:             model.NodeFinished,
		ContextBefore:      json.RawMessage(`{}`),
		CreatedAt:          time.Now().UTC(),
		UpdatedAt:          time.Now().UTC(),
	}); err != nil {
		t.Fatalf("InsertNodeInstance() error = %v", err)
	}

	_, err := repo.RollbackInstance(ctx, repository.RollbackUpdate{
		InstanceID:           id,
		Frame:                model.Frame{CurrentNodeID: "22222222-2222-7222-8222-222222222222"},
		Context:              json.RawMessage(`{}`),
		Actor:                fixtureUserID,
		ToNode:               "22222222-2222-7222-8222-222222222222",
		ToOccurrence:         occID,
		WaitingReason:        model.WaitingReasonInput,
		RearmInputOccurrence: true,
	})
	if err != nil {
		t.Fatalf("RollbackInstance() error = %v", err)
	}
	occ, err := repo.GetNodeInstance(ctx, id, occID)
	if err != nil {
		t.Fatal(err)
	}
	if occ.Status != model.NodeRunning {
		t.Errorf("occurrence status = %s, want running (re-armed)", occ.Status)
	}
	if occ.Attempt != 2 {
		t.Errorf("attempt = %d, want 2 (fresh attempt of same row)", occ.Attempt)
	}
}

func TestRollbackInstanceRearmSkipsNonInputOccurrence(t *testing.T) {
	db := setupTestDB(t)
	seedInstanceFixture(t, db)
	ctx := context.Background()
	repo := repository.NewInstanceRepository(db)

	id := "11111111-1111-7111-8111-111111111111"
	insertInstance(t, db, newTestInstance(id, model.WorkflowPaused, ""))
	occID := "55555555-5555-7555-8555-555555555555"
	if err := repo.InsertNodeInstance(ctx, model.NodeInstance{
		ID:                 occID,
		WorkflowInstanceID: id,
		NodeID:             "22222222-2222-7222-8222-222222222222",
		Name:               "compute",
		Type:               string(model.NodeTypeScript),
		Attempt:            1,
		Status:             model.NodeFinished,
		ContextBefore:      json.RawMessage(`{}`),
		CreatedAt:          time.Now().UTC(),
		UpdatedAt:          time.Now().UTC(),
	}); err != nil {
		t.Fatalf("InsertNodeInstance() error = %v", err)
	}

	_, err := repo.RollbackInstance(ctx, repository.RollbackUpdate{
		InstanceID:    id,
		Frame:         model.Frame{CurrentNodeID: "22222222-2222-7222-8222-222222222222"},
		Context:       json.RawMessage(`{}`),
		Actor:         fixtureUserID,
		ToNode:        "22222222-2222-7222-8222-222222222222",
		ToOccurrence:  occID,
		WaitingReason: model.WaitingReasonRunnable,
	})
	if err != nil {
		t.Fatalf("RollbackInstance() error = %v", err)
	}
	occ, err := repo.GetNodeInstance(ctx, id, occID)
	if err != nil {
		t.Fatal(err)
	}
	if occ.Status != model.NodeFinished {
		t.Errorf("occurrence status = %s, want untouched finished", occ.Status)
	}
	if occ.Attempt != 1 {
		t.Errorf("attempt = %d, want unchanged 1", occ.Attempt)
	}
}

func TestRollbackInstanceRearmFlagOffLeavesInputFinished(t *testing.T) {
	db := setupTestDB(t)
	seedInstanceFixture(t, db)
	ctx := context.Background()
	repo := repository.NewInstanceRepository(db)

	id := "11111111-1111-7111-8111-111111111111"
	insertInstance(t, db, newTestInstance(id, model.WorkflowPaused, ""))
	occID := "55555555-5555-7555-8555-555555555555"
	if err := repo.InsertNodeInstance(ctx, model.NodeInstance{
		ID:                 occID,
		WorkflowInstanceID: id,
		NodeID:             "22222222-2222-7222-8222-222222222222",
		Name:               "ask",
		Type:               string(model.NodeTypeInput),
		Attempt:            1,
		Status:             model.NodeFinished,
		ContextBefore:      json.RawMessage(`{}`),
		CreatedAt:          time.Now().UTC(),
		UpdatedAt:          time.Now().UTC(),
	}); err != nil {
		t.Fatalf("InsertNodeInstance() error = %v", err)
	}

	// Without the re-arm flag an input occurrence stays finished even
	// when the park reason is input.
	_, err := repo.RollbackInstance(ctx, repository.RollbackUpdate{
		InstanceID:    id,
		Frame:         model.Frame{CurrentNodeID: "22222222-2222-7222-8222-222222222222"},
		Context:       json.RawMessage(`{}`),
		Actor:         fixtureUserID,
		ToNode:        "22222222-2222-7222-8222-222222222222",
		ToOccurrence:  occID,
		WaitingReason: model.WaitingReasonInput,
	})
	if err != nil {
		t.Fatalf("RollbackInstance() error = %v", err)
	}
	occ, err := repo.GetNodeInstance(ctx, id, occID)
	if err != nil {
		t.Fatal(err)
	}
	if occ.Status != model.NodeFinished || occ.Attempt != 1 {
		t.Errorf("occurrence = %s/%d, want finished/1", occ.Status, occ.Attempt)
	}
}

func TestReplaceContextMissingInstance(t *testing.T) {
	db := setupTestDB(t)
	seedInstanceFixture(t, db)
	ctx := context.Background()
	repo := repository.NewInstanceRepository(db)

	_, err := repo.ReplaceContext(ctx, repository.ContextUpdate{
		InstanceID: "99999999-9999-7999-8999-999999999999",
		Context:    json.RawMessage(`{}`),
		Actor:      fixtureUserID,
	})
	if !errors.Is(err, repository.ErrInstanceNotFound) {
		t.Errorf("ReplaceContext() error = %v, want ErrInstanceNotFound", err)
	}
}

func instanceWithTime(id string, status model.WorkflowStatus, wfID string, at time.Time) model.WorkflowInstance {
	w := newTestInstance(id, status, "")
	w.WorkflowDefinitionID = wfID
	w.CreatedAt = at
	w.UpdatedAt = at
	return w
}

func instanceIDs(items []model.WorkflowInstance) []string {
	out := make([]string, len(items))
	for i, w := range items {
		out[i] = w.ID
	}
	return out
}

func seedSecondWorkflowDefinition(t *testing.T, db *gorm.DB) string {
	t.Helper()
	now := time.Now().UTC()
	id := "99999999-9999-7999-8999-999999999999"
	wd := repository.WorkflowDefinitionToModel(model.WorkflowDefinition{
		ID: id, Name: "fixture-flow-2", Version: 1,
		LineageID: id, Content: json.RawMessage(`{}`),
		CreatedBy: fixtureUserID, UpdatedBy: fixtureUserID, CreatedAt: now, UpdatedAt: now,
	})
	if err := db.Create(&wd).Error; err != nil {
		t.Fatalf("seed second workflow definition: %v", err)
	}
	return id
}

func TestInstanceListIDFilter(t *testing.T) {
	db := setupTestDB(t)
	seedInstanceFixture(t, db)
	ctx := context.Background()
	base := time.Now().UTC()

	ids := []string{
		"11111111-1111-7111-8111-111111111111",
		"22222222-2222-7222-8222-222222222222",
		"33333333-3333-7333-8333-333333333333",
	}
	statuses := []model.WorkflowStatus{model.WorkflowWaiting, model.WorkflowFinished, model.WorkflowFailed}
	for i, id := range ids {
		insertInstance(t, db, instanceWithTime(id, statuses[i], fixtureWorkflowDefID, base.Add(time.Duration(i)*time.Second)))
	}

	repo := repository.NewInstanceRepository(db)
	items, total, err := repo.List(ctx, repository.InstanceListQuery{
		Page: 1, PerPage: 50, Order: "created_at",
		IDs: ids[1:],
	})
	if err != nil {
		t.Fatalf("List() error = %v", err)
	}
	if total != 2 {
		t.Errorf("total = %d, want 2", total)
	}
	if got := instanceIDs(items); len(got) != 2 || got[0] != ids[1] || got[1] != ids[2] {
		t.Errorf("items = %v, want [%s %s]", got, ids[1], ids[2])
	}
}

func TestInstanceListWorkflowDefinitionFilter(t *testing.T) {
	db := setupTestDB(t)
	seedInstanceFixture(t, db)
	ctx := context.Background()
	base := time.Now().UTC()
	wd2 := seedSecondWorkflowDefinition(t, db)

	insertInstance(t, db, instanceWithTime("11111111-1111-7111-8111-111111111111", model.WorkflowWaiting, fixtureWorkflowDefID, base))
	insertInstance(t, db, instanceWithTime("22222222-2222-7222-8222-222222222222", model.WorkflowFinished, wd2, base.Add(time.Second)))
	insertInstance(t, db, instanceWithTime("33333333-3333-7333-8333-333333333333", model.WorkflowFailed, fixtureWorkflowDefID, base.Add(2*time.Second)))

	repo := repository.NewInstanceRepository(db)
	items, total, err := repo.List(ctx, repository.InstanceListQuery{
		Page: 1, PerPage: 50, Order: "created_at", WorkflowDefinitionID: wd2,
	})
	if err != nil {
		t.Fatalf("List() error = %v", err)
	}
	if total != 1 || len(items) != 1 || items[0].ID != "22222222-2222-7222-8222-222222222222" {
		t.Errorf("total = %d items = %v, want only instance of %s", total, instanceIDs(items), wd2)
	}
}

func TestInstanceListStatusFilter(t *testing.T) {
	db := setupTestDB(t)
	seedInstanceFixture(t, db)
	ctx := context.Background()
	base := time.Now().UTC()

	insertInstance(t, db, instanceWithTime("11111111-1111-7111-8111-111111111111", model.WorkflowWaiting, fixtureWorkflowDefID, base))
	insertInstance(t, db, instanceWithTime("22222222-2222-7222-8222-222222222222", model.WorkflowFinished, fixtureWorkflowDefID, base.Add(time.Second)))
	insertInstance(t, db, instanceWithTime("33333333-3333-7333-8333-333333333333", model.WorkflowFailed, fixtureWorkflowDefID, base.Add(2*time.Second)))

	repo := repository.NewInstanceRepository(db)
	items, total, err := repo.List(ctx, repository.InstanceListQuery{
		Page: 1, PerPage: 50, Order: "created_at",
		Statuses: []string{"waiting", "failed"},
	})
	if err != nil {
		t.Fatalf("List() error = %v", err)
	}
	if total != 2 {
		t.Errorf("total = %d, want 2", total)
	}
	if got := instanceIDs(items); len(got) != 2 || got[0] != "11111111-1111-7111-8111-111111111111" || got[1] != "33333333-3333-7333-8333-333333333333" {
		t.Errorf("items = %v, want waiting and failed instances", got)
	}
}

func TestInstanceListNoMatches(t *testing.T) {
	db := setupTestDB(t)
	seedInstanceFixture(t, db)
	ctx := context.Background()
	base := time.Now().UTC()
	insertInstance(t, db, instanceWithTime("11111111-1111-7111-8111-111111111111", model.WorkflowWaiting, fixtureWorkflowDefID, base))

	repo := repository.NewInstanceRepository(db)
	items, total, err := repo.List(ctx, repository.InstanceListQuery{
		Page: 1, PerPage: 50, Order: "created_at",
		Statuses: []string{"stopped"},
	})
	if err != nil {
		t.Fatalf("List() error = %v", err)
	}
	if total != 0 || len(items) != 0 {
		t.Errorf("total = %d items = %v, want no matches", total, instanceIDs(items))
	}
}

func TestInstanceListDeterministicOrdering(t *testing.T) {
	db := setupTestDB(t)
	seedInstanceFixture(t, db)
	ctx := context.Background()
	base := time.Now().UTC().Truncate(time.Second) // shared timestamp exercises the id tie-breaker

	insertInstance(t, db, instanceWithTime("11111111-1111-7111-8111-111111111111", model.WorkflowWaiting, fixtureWorkflowDefID, base))
	insertInstance(t, db, instanceWithTime("22222222-2222-7222-8222-222222222222", model.WorkflowWaiting, fixtureWorkflowDefID, base))
	insertInstance(t, db, instanceWithTime("33333333-3333-7333-8333-333333333333", model.WorkflowWaiting, fixtureWorkflowDefID, base.Add(time.Hour)))

	repo := repository.NewInstanceRepository(db)

	items, _, err := repo.List(ctx, repository.InstanceListQuery{Page: 1, PerPage: 50, Order: "created_at"})
	if err != nil {
		t.Fatalf("List(created_at) error = %v", err)
	}
	if got := instanceIDs(items); len(got) != 3 || got[0] != "11111111-1111-7111-8111-111111111111" || got[1] != "22222222-2222-7222-8222-222222222222" || got[2] != "33333333-3333-7333-8333-333333333333" {
		t.Errorf("asc items = %v, want 1111 2222 3333 (id tie-breaker)", got)
	}

	items, _, err = repo.List(ctx, repository.InstanceListQuery{Page: 1, PerPage: 50, Order: "-created_at"})
	if err != nil {
		t.Fatalf("List(-created_at) error = %v", err)
	}
	if got := instanceIDs(items); len(got) != 3 || got[0] != "33333333-3333-7333-8333-333333333333" || got[1] != "11111111-1111-7111-8111-111111111111" || got[2] != "22222222-2222-7222-8222-222222222222" {
		t.Errorf("desc items = %v, want 3333 1111 2222 (id tie-breaker)", got)
	}
}

func TestInstanceListPagination(t *testing.T) {
	db := setupTestDB(t)
	seedInstanceFixture(t, db)
	ctx := context.Background()
	base := time.Now().UTC()
	ids := []string{
		"11111111-1111-7111-8111-111111111111",
		"22222222-2222-7222-8222-222222222222",
		"33333333-3333-7333-8333-333333333333",
		"44444444-4444-7444-8444-444444444444",
		"55555555-5555-7555-8555-555555555555",
	}
	for i, id := range ids {
		insertInstance(t, db, instanceWithTime(id, model.WorkflowWaiting, fixtureWorkflowDefID, base.Add(time.Duration(i)*time.Second)))
	}

	repo := repository.NewInstanceRepository(db)
	for _, tc := range []struct {
		page int
		want []string
	}{
		{1, ids[:2]},
		{2, ids[2:4]},
		{3, ids[4:]},
	} {
		items, total, err := repo.List(ctx, repository.InstanceListQuery{Page: tc.page, PerPage: 2, Order: "created_at"})
		if err != nil {
			t.Fatalf("List(page %d) error = %v", tc.page, err)
		}
		if total != 5 {
			t.Errorf("page %d total = %d, want 5", tc.page, total)
		}
		if got := instanceIDs(items); len(got) != len(tc.want) || !reflect.DeepEqual(got, tc.want) {
			t.Errorf("page %d items = %v, want %v", tc.page, got, tc.want)
		}
	}
}
