// Internal test: the enqueue cache is asserted through white-box stats,
// and the instanceRepo wiring through the public Checkpoint path.
package repository

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/simpwf/workflow-engine/internal/workflow/defcache"
	"github.com/simpwf/workflow-engine/internal/workflow/model"
	"github.com/simpwf/workflow-engine/internal/workflow/testdb"
	"gorm.io/gorm"
)

const (
	statusCacheUserID  = "aaaaaaaa-aaaa-7aaa-8aaa-000000000001"
	statusCacheDefID   = "aaaaaaaa-aaaa-7aaa-8aaa-000000000002"
	statusCachePlainID = "aaaaaaaa-aaaa-7aaa-8aaa-000000000003"
	statusCacheInstID  = "aaaaaaaa-aaaa-7aaa-8aaa-000000000004"
)

const statusCacheContent = `{
	"start_node_id": "aaaaaaaa-aaaa-7aaa-8aaa-aaaaaaaaaaaa",
	"status_update": {"http": {"url": "https://hooks.example.com/wf"}},
	"nodes": [
		{"id": "aaaaaaaa-aaaa-7aaa-8aaa-aaaaaaaaaaaa", "type": "script", "script": "return 1;"}
	]
}`

// setupStatusCacheDB opens a scratch database with only the tables the
// enqueue-cache tests touch. Rows use suite-unique IDs and every other
// test truncates first, so no reset is needed here.
func setupStatusCacheDB(t *testing.T) *gorm.DB {
	t.Helper()
	db := testdb.Open(t, "repository")
	if err := db.AutoMigrate(
		&UserModel{},
		&WorkflowDefinitionModel{},
		&WorkflowInstanceModel{},
		&StatusUpdateOutboxModel{},
	); err != nil {
		t.Fatalf("AutoMigrate() error = %v", err)
	}
	// Suite tests run sequentially, so resetting here is safe and keeps
	// row counts exact across tests and re-runs.
	if err := db.Exec(`TRUNCATE TABLE users, workflow_definitions, workflow_instances, status_update_outbox CASCADE`).Error; err != nil {
		t.Fatalf("truncate tables: %v", err)
	}
	now := time.Now().UTC()
	u := UserToModel(model.User{ID: statusCacheUserID, Name: "status-cache", Email: "status-cache@localhost", CreatedAt: now, UpdatedAt: now})
	if err := db.FirstOrCreate(&u, "id = ?", statusCacheUserID).Error; err != nil {
		t.Fatalf("seed user: %v", err)
	}
	return db
}

func seedStatusCacheDef(t *testing.T, db *gorm.DB, id, content string) {
	t.Helper()
	now := time.Now().UTC()
	wd := WorkflowDefinitionToModel(model.WorkflowDefinition{
		ID: id, Name: "status-cache-flow", Version: 1, LineageID: id,
		Content:   json.RawMessage(content),
		CreatedBy: statusCacheUserID, UpdatedBy: statusCacheUserID, CreatedAt: now, UpdatedAt: now,
	})
	if err := db.FirstOrCreate(&wd, "id = ?", id).Error; err != nil {
		t.Fatalf("seed definition: %v", err)
	}
}

func enqueueOnce(t *testing.T, db *gorm.DB, cache *defcache.Cache[string, *model.StatusUpdateConfig], defID string, rev int64) error {
	t.Helper()
	return db.Transaction(func(tx *gorm.DB) error {
		from := statusWithReason{status: model.WorkflowRunning}
		to := statusWithReason{status: model.WorkflowWaiting, waitingReason: model.WaitingReasonInput}
		return enqueueStatusUpdateTx(context.Background(), tx, statusCacheInstID, defID, rev,
			from, to, []string{model.StatusUpdateEventWaitingForInput}, "", json.RawMessage(`{}`), time.Now().UTC(), cache)
	})
}

func statusCacheOutboxCount(t *testing.T, db *gorm.DB) int64 {
	t.Helper()
	var n int64
	if err := db.Model(&StatusUpdateOutboxModel{}).Where("workflow_instance_id = ?", statusCacheInstID).Count(&n).Error; err != nil {
		t.Fatal(err)
	}
	return n
}

func TestEnqueueStatusUpdateCachesConfig(t *testing.T) {
	db := setupStatusCacheDB(t)
	seedStatusCacheDef(t, db, statusCacheDefID, statusCacheContent)
	cache := defcache.New[string, *model.StatusUpdateConfig](16)

	for rev := int64(1); rev <= 2; rev++ {
		if err := enqueueOnce(t, db, cache, statusCacheDefID, rev); err != nil {
			t.Fatalf("enqueueStatusUpdateTx() error = %v", err)
		}
	}
	if got := cache.Hits(); got != 1 {
		t.Errorf("cache Hits() = %d, want 1", got)
	}
	if got := statusCacheOutboxCount(t, db); got != 2 {
		t.Errorf("outbox rows = %d, want 2 (both enqueues wrote)", got)
	}
}

func TestEnqueueStatusUpdateCachesNegative(t *testing.T) {
	db := setupStatusCacheDB(t)
	seedStatusCacheDef(t, db, statusCachePlainID, `{"nodes": []}`)
	cache := defcache.New[string, *model.StatusUpdateConfig](16)

	for rev := int64(1); rev <= 2; rev++ {
		if err := enqueueOnce(t, db, cache, statusCachePlainID, rev); err != nil {
			t.Fatalf("enqueueStatusUpdateTx() error = %v", err)
		}
	}
	if got := cache.Hits(); got != 1 {
		t.Errorf("cache Hits() = %d, want 1 (negative cached)", got)
	}
	if got := statusCacheOutboxCount(t, db); got != 0 {
		t.Errorf("outbox rows = %d, want 0 (not configured)", got)
	}
}

func TestEnqueueStatusUpdateErrorNotCached(t *testing.T) {
	db := setupStatusCacheDB(t)
	cache := defcache.New[string, *model.StatusUpdateConfig](16)

	for rev := int64(1); rev <= 2; rev++ {
		if err := enqueueOnce(t, db, cache, "00000000-0000-7000-8000-000000000000", rev); err == nil {
			t.Fatal("enqueueStatusUpdateTx() error = nil, want missing-definition error")
		}
	}
	if got := cache.Hits(); got != 0 {
		t.Errorf("cache Hits() = %d, want 0", got)
	}
	if got := cache.Misses(); got != 2 {
		t.Errorf("cache Misses() = %d, want 2 (failures retried)", got)
	}
}

func TestInstanceCheckpointReusesStatusUpdateCache(t *testing.T) {
	db := setupStatusCacheDB(t)
	seedStatusCacheDef(t, db, statusCacheDefID, statusCacheContent)
	ctx := context.Background()

	frame := model.NewFrame("node-1")
	frameRaw, _ := frame.JSON()
	now := time.Now().UTC()
	repo := NewInstanceRepository(db).(*instanceRepo)
	w := model.WorkflowInstance{
		ID: statusCacheInstID, WorkflowDefinitionID: statusCacheDefID,
		Status: model.WorkflowRunning, Frame: frameRaw,
		Context: json.RawMessage(`{}`), Counters: json.RawMessage(`{}`),
		LeasedBy:  "worker-1",
		CreatedBy: statusCacheUserID, UpdatedBy: statusCacheUserID, CreatedAt: now, UpdatedAt: now,
	}
	if err := repo.Insert(ctx, w); err != nil {
		t.Fatalf("Insert() error = %v", err)
	}
	for rev := int64(0); rev <= 1; rev++ {
		// A checkpoint releases the lease; re-claim before the next one.
		if rev > 0 {
			if err := db.Exec(`UPDATE workflow_instances SET leased_by = 'worker-1' WHERE id = ?`, statusCacheInstID).Error; err != nil {
				t.Fatalf("re-lease: %v", err)
			}
		}
		from := model.WorkflowRunning
		if rev == 1 {
			from = model.WorkflowWaiting
		}
		err := repo.Checkpoint(ctx, Checkpoint{
			InstanceID: statusCacheInstID, WorkerID: "worker-1", Revision: rev,
			WorkflowDefinitionID: statusCacheDefID,
			FromStatus:           from, FromWaitingReason: model.WaitingReasonInput,
			Status:        model.WorkflowWaiting,
			WaitingReason: model.WaitingReasonInput,
			Frame:         frame, Counters: model.Counters{},
			Context: json.RawMessage(`{}`),
		})
		if err != nil {
			t.Fatalf("Checkpoint() rev %d error = %v", rev, err)
		}
	}
	if got := repo.statusUpdateCache.Hits(); got != 1 {
		t.Errorf("cache Hits() = %d, want 1 (second checkpoint hit)", got)
	}
	if got := statusCacheOutboxCount(t, db); got != 2 {
		t.Errorf("outbox rows = %d, want 2", got)
	}
}

func TestEnqueueStatusUpdateConcurrentHammer(t *testing.T) {
	db := setupStatusCacheDB(t)
	seedStatusCacheDef(t, db, statusCacheDefID, statusCacheContent)
	cache := defcache.New[string, *model.StatusUpdateConfig](16)

	if err := enqueueOnce(t, db, cache, statusCacheDefID, 1); err != nil {
		t.Fatalf("warmup error = %v", err)
	}
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 25; i++ {
				if err := enqueueOnce(t, db, cache, statusCacheDefID, int64(100+g*25+i)); err != nil {
					t.Error(err)
					return
				}
			}
		}(g)
	}
	wg.Wait()
	if got := cache.Misses(); got != 1 {
		t.Errorf("cache Misses() = %d, want 1 (warmup only)", got)
	}
}
