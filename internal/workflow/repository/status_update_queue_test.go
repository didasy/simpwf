package repository_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/simpwf/workflow-engine/internal/workflow/model"
	"github.com/simpwf/workflow-engine/internal/workflow/repository"
	"gorm.io/datatypes"
	"gorm.io/gorm"
)

// seedOutbox inserts a pending outbox row for a fixture instance.
func seedOutbox(t *testing.T, db *gorm.DB, id, instanceID string, revision int64, eventIndex int, next time.Time) {
	t.Helper()
	now := time.Now().UTC()
	m := repository.StatusUpdateOutboxModel{
		ID: id, WorkflowInstanceID: instanceID, WorkflowDefinitionID: fixtureWorkflowDefID,
		Revision: revision, EventIndex: eventIndex, Transport: model.StatusUpdateTransportHTTP,
		Payload: datatypes.JSON(`{"event":"x"}`), NextAttemptAt: next,
		CreatedAt: now, UpdatedAt: now,
	}
	if err := db.Create(&m).Error; err != nil {
		t.Fatalf("seed outbox: %v", err)
	}
}

func readyNow() time.Time { return time.Now().UTC().Add(-time.Minute) }

func TestClaimNextStatusUpdatesClaimsOldestPerInstance(t *testing.T) {
	db := setupTestDB(t)
	seedInstanceFixture(t, db)
	ctx := context.Background()
	insertInstance(t, db, newTestInstance("11111111-1111-7111-8111-111111111111", model.WorkflowWaiting, ""))
	insertInstance(t, db, newTestInstance("22222222-2222-7222-8222-222222222222", model.WorkflowWaiting, ""))

	now := readyNow()
	// Instance A: two pending events; the newer one must not claim before
	// the older is resolved.
	seedOutbox(t, db, "aaaaaaaa-aaaa-1aaa-8aaa-aaaaaaaaaaaa", "11111111-1111-7111-8111-111111111111", 1, 0, now)
	seedOutbox(t, db, "bbbbbbbb-bbbb-1bbb-8bbb-bbbbbbbbbbbb", "11111111-1111-7111-8111-111111111111", 2, 0, now)
	// Instance B: one pending event.
	seedOutbox(t, db, "cccccccc-cccc-1ccc-8ccc-cccccccccccc", "22222222-2222-7222-8222-222222222222", 1, 0, now)

	repo := repository.NewStatusUpdateRepository(db)
	claimed, err := repo.ClaimNextStatusUpdates(ctx, "worker-1", time.Minute, 10)
	if err != nil {
		t.Fatalf("ClaimNextStatusUpdates() error = %v", err)
	}
	if len(claimed) != 2 {
		t.Fatalf("claimed = %d, want 2 (one per instance)", len(claimed))
	}
	got := map[string]bool{}
	for _, e := range claimed {
		got[e.ID] = true
	}
	if !got["aaaaaaaa-aaaa-1aaa-8aaa-aaaaaaaaaaaa"] || !got["cccccccc-cccc-1ccc-8ccc-cccccccccccc"] {
		t.Errorf("claimed ids = %v, want the oldest event of each instance", got)
	}

	// The newer event of A stays blocked by the claimed older one.
	claimed, err = repo.ClaimNextStatusUpdates(ctx, "worker-1", time.Minute, 10)
	if err != nil {
		t.Fatalf("second ClaimNextStatusUpdates() error = %v", err)
	}
	if len(claimed) != 0 {
		t.Errorf("second claim = %d events, want 0", len(claimed))
	}
}

func TestClaimNextStatusUpdatesOrdersByNextAttemptAt(t *testing.T) {
	db := setupTestDB(t)
	seedInstanceFixture(t, db)
	ctx := context.Background()
	insertInstance(t, db, newTestInstance("11111111-1111-7111-8111-111111111111", model.WorkflowWaiting, ""))
	insertInstance(t, db, newTestInstance("22222222-2222-7222-8222-222222222222", model.WorkflowWaiting, ""))

	seedOutbox(t, db, "aaaaaaaa-aaaa-1aaa-8aaa-aaaaaaaaaaaa", "11111111-1111-7111-8111-111111111111", 1, 0, time.Now().UTC().Add(time.Hour))
	seedOutbox(t, db, "bbbbbbbb-bbbb-1bbb-8bbb-bbbbbbbbbbbb", "22222222-2222-7222-8222-222222222222", 1, 0, readyNow())

	repo := repository.NewStatusUpdateRepository(db)
	claimed, err := repo.ClaimNextStatusUpdates(ctx, "worker-1", time.Minute, 1)
	if err != nil {
		t.Fatalf("ClaimNextStatusUpdates() error = %v", err)
	}
	if len(claimed) != 1 || claimed[0].ID != "bbbbbbbb-bbbb-1bbb-8bbb-bbbbbbbbbbbb" {
		t.Errorf("claimed = %+v, want the earliest ready event", claimed)
	}
}

func TestClaimNextStatusUpdatesBlocksLaterWhileOlderInBackoff(t *testing.T) {
	db := setupTestDB(t)
	seedInstanceFixture(t, db)
	ctx := context.Background()
	insertInstance(t, db, newTestInstance("11111111-1111-7111-8111-111111111111", model.WorkflowWaiting, ""))

	seedOutbox(t, db, "aaaaaaaa-aaaa-1aaa-8aaa-aaaaaaaaaaaa", "11111111-1111-7111-8111-111111111111", 1, 0, time.Now().UTC().Add(time.Hour))
	seedOutbox(t, db, "bbbbbbbb-bbbb-1bbb-8bbb-bbbbbbbbbbbb", "11111111-1111-7111-8111-111111111111", 2, 0, readyNow())

	repo := repository.NewStatusUpdateRepository(db)
	claimed, err := repo.ClaimNextStatusUpdates(ctx, "worker-1", time.Minute, 10)
	if err != nil {
		t.Fatalf("ClaimNextStatusUpdates() error = %v", err)
	}
	if len(claimed) != 0 {
		t.Errorf("claimed = %d events, want 0 (older event in backoff blocks newer)", len(claimed))
	}
}

func TestClaimNextStatusUpdatesRecoversExpiredClaim(t *testing.T) {
	db := setupTestDB(t)
	seedInstanceFixture(t, db)
	ctx := context.Background()
	insertInstance(t, db, newTestInstance("11111111-1111-7111-8111-111111111111", model.WorkflowWaiting, ""))

	now := time.Now().UTC()
	m := repository.StatusUpdateOutboxModel{
		ID: "aaaaaaaa-aaaa-1aaa-8aaa-aaaaaaaaaaaa", WorkflowInstanceID: "11111111-1111-7111-8111-111111111111",
		WorkflowDefinitionID: fixtureWorkflowDefID, Revision: 1, EventIndex: 0,
		Transport: model.StatusUpdateTransportHTTP, Payload: datatypes.JSON(`{}`),
		NextAttemptAt: now.Add(-time.Minute), ClaimedBy: "dead-worker",
		ClaimExpiry: timePtr(now.Add(-time.Minute)), CreatedAt: now, UpdatedAt: now,
	}
	if err := db.Create(&m).Error; err != nil {
		t.Fatal(err)
	}

	repo := repository.NewStatusUpdateRepository(db)
	claimed, err := repo.ClaimNextStatusUpdates(ctx, "worker-1", time.Minute, 10)
	if err != nil {
		t.Fatalf("ClaimNextStatusUpdates() error = %v", err)
	}
	if len(claimed) != 1 || claimed[0].ID != "aaaaaaaa-aaaa-1aaa-8aaa-aaaaaaaaaaaa" {
		t.Fatalf("claimed = %+v, want recovered expired claim", claimed)
	}
	var stored repository.StatusUpdateOutboxModel
	if err := db.Where("id = ?", "aaaaaaaa-aaaa-1aaa-8aaa-aaaaaaaaaaaa").First(&stored).Error; err != nil {
		t.Fatal(err)
	}
	if stored.ClaimedBy != "worker-1" || stored.ClaimExpiry == nil {
		t.Errorf("claim not fenced to worker-1: %+v", stored)
	}
}

func TestClaimNextStatusUpdatesConcurrentNoDoubleClaim(t *testing.T) {
	db := setupTestDB(t)
	seedInstanceFixture(t, db)
	ctx := context.Background()
	instances := []string{
		"11111111-1111-7111-8111-111111111111",
		"22222222-2222-7222-8222-222222222222",
		"33333333-3333-7333-8333-333333333333",
		"44444444-4444-7444-8444-444444444444",
	}
	eventIDs := []string{
		"aaaaaaaa-aaaa-1aaa-8aaa-aaaaaaaaaa01",
		"aaaaaaaa-aaaa-1aaa-8aaa-aaaaaaaaaa02",
		"aaaaaaaa-aaaa-1aaa-8aaa-aaaaaaaaaa03",
		"aaaaaaaa-aaaa-1aaa-8aaa-aaaaaaaaaa04",
	}
	for i, id := range instances {
		insertInstance(t, db, newTestInstance(id, model.WorkflowWaiting, ""))
		seedOutbox(t, db, eventIDs[i], id, 1, 0, readyNow())
	}
	repo := repository.NewStatusUpdateRepository(db)

	start := make(chan struct{})
	var a, b []repository.PendingStatusUpdate
	var errA, errB error
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		<-start
		a, errA = repo.ClaimNextStatusUpdates(ctx, "worker-a", time.Minute, 10)
	}()
	go func() {
		defer wg.Done()
		<-start
		b, errB = repo.ClaimNextStatusUpdates(ctx, "worker-b", time.Minute, 10)
	}()
	close(start)
	wg.Wait()
	if errA != nil || errB != nil {
		t.Fatalf("claim errors: %v, %v", errA, errB)
	}
	union := map[string]string{}
	for _, e := range a {
		union[e.ID] = "a"
	}
	for _, e := range b {
		if prev, dup := union[e.ID]; dup {
			t.Fatalf("event %s claimed by both workers (first %s)", e.ID, prev)
		}
		union[e.ID] = "b"
	}
	if len(union) != 4 {
		t.Errorf("distinct claimed events = %d, want 4", len(union))
	}
}

func TestMarkStatusUpdateDelivered(t *testing.T) {
	db := setupTestDB(t)
	seedInstanceFixture(t, db)
	ctx := context.Background()
	insertInstance(t, db, newTestInstance("11111111-1111-7111-8111-111111111111", model.WorkflowWaiting, ""))
	seedOutbox(t, db, "aaaaaaaa-aaaa-1aaa-8aaa-aaaaaaaaaaaa", "11111111-1111-7111-8111-111111111111", 1, 0, readyNow())

	repo := repository.NewStatusUpdateRepository(db)
	claimed, err := repo.ClaimNextStatusUpdates(ctx, "worker-1", time.Minute, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(claimed) != 1 {
		t.Fatalf("claimed = %d, want 1", len(claimed))
	}
	if err := repo.MarkStatusUpdateDelivered(ctx, claimed[0].ID, "worker-1"); err != nil {
		t.Fatalf("MarkStatusUpdateDelivered() error = %v", err)
	}
	var stored repository.StatusUpdateOutboxModel
	if err := db.Where("id = ?", claimed[0].ID).First(&stored).Error; err != nil {
		t.Fatal(err)
	}
	if stored.DeliveredAt == nil || stored.ClaimedBy != "" {
		t.Errorf("delivered row not final: %+v", stored)
	}

	// Delivering again (or by another worker) is a claim loss.
	if err := repo.MarkStatusUpdateDelivered(ctx, claimed[0].ID, "worker-1"); !errors.Is(err, repository.ErrStatusUpdateClaimLost) {
		t.Errorf("second deliver error = %v, want ErrStatusUpdateClaimLost", err)
	}
}

func TestFailStatusUpdateRetriesThenDeadLetters(t *testing.T) {
	db := setupTestDB(t)
	seedInstanceFixture(t, db)
	ctx := context.Background()
	insertInstance(t, db, newTestInstance("11111111-1111-7111-8111-111111111111", model.WorkflowWaiting, ""))
	seedOutbox(t, db, "aaaaaaaa-aaaa-1aaa-8aaa-aaaaaaaaaaaa", "11111111-1111-7111-8111-111111111111", 1, 0, readyNow())

	repo := repository.NewStatusUpdateRepository(db)
	for attempt := 1; attempt <= 3; attempt++ {
		claimed, err := repo.ClaimNextStatusUpdates(ctx, "worker-1", time.Minute, 10)
		if err != nil {
			t.Fatal(err)
		}
		if len(claimed) != 1 {
			t.Fatalf("claim %d = %d events, want 1", attempt, len(claimed))
		}
		if err := repo.FailStatusUpdate(ctx, claimed[0].ID, "worker-1", 5*time.Second, 3, "boom"); err != nil {
			t.Fatalf("FailStatusUpdate() error = %v", err)
		}
		var stored repository.StatusUpdateOutboxModel
		if err := db.Where("id = ?", claimed[0].ID).First(&stored).Error; err != nil {
			t.Fatal(err)
		}
		if stored.Attempts != attempt {
			t.Errorf("attempts = %d, want %d", stored.Attempts, attempt)
		}
		if stored.DeadAt != nil || stored.LastError != "boom" {
			t.Errorf("row after failure %d: %+v", attempt, stored)
		}
		if stored.NextAttemptAt.Before(time.Now()) {
			t.Errorf("next_attempt_at = %v, want scheduled after retry_delay", stored.NextAttemptAt)
		}
		// Make the retry ready immediately.
		if err := db.Model(&repository.StatusUpdateOutboxModel{}).Where("id = ?", stored.ID).Update("next_attempt_at", readyNow()).Error; err != nil {
			t.Fatal(err)
		}
	}

	// The 4th failure (1 initial + 3 retries) dead-letters the event.
	claimed, err := repo.ClaimNextStatusUpdates(ctx, "worker-1", time.Minute, 10)
	if err != nil {
		t.Fatal(err)
	}
	if err := repo.FailStatusUpdate(ctx, claimed[0].ID, "worker-1", 5*time.Second, 3, "boom"); err != nil {
		t.Fatal(err)
	}
	var stored repository.StatusUpdateOutboxModel
	if err := db.Where("id = ?", claimed[0].ID).First(&stored).Error; err != nil {
		t.Fatal(err)
	}
	if stored.Attempts != 4 || stored.DeadAt == nil {
		t.Errorf("row after 4th failure: %+v, want dead-lettered", stored)
	}
	claimed, err = repo.ClaimNextStatusUpdates(ctx, "worker-1", time.Minute, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(claimed) != 0 {
		t.Errorf("claim after dead-letter = %d events, want 0", len(claimed))
	}
}

func TestFailStatusUpdateClaimLost(t *testing.T) {
	db := setupTestDB(t)
	seedInstanceFixture(t, db)
	ctx := context.Background()
	insertInstance(t, db, newTestInstance("11111111-1111-7111-8111-111111111111", model.WorkflowWaiting, ""))
	seedOutbox(t, db, "aaaaaaaa-aaaa-1aaa-8aaa-aaaaaaaaaaaa", "11111111-1111-7111-8111-111111111111", 1, 0, readyNow())

	repo := repository.NewStatusUpdateRepository(db)
	if err := repo.FailStatusUpdate(ctx, "aaaaaaaa-aaaa-1aaa-8aaa-aaaaaaaaaaaa", "worker-1", time.Second, 3, "x"); !errors.Is(err, repository.ErrStatusUpdateClaimLost) {
		t.Errorf("FailStatusUpdate() error = %v, want ErrStatusUpdateClaimLost", err)
	}
}

func timePtr(t time.Time) *time.Time { return &t }
