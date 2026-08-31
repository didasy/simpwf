package statusupdate_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/simpwf/workflow-engine/internal/workflow/executor"
	"github.com/simpwf/workflow-engine/internal/workflow/model"
	"github.com/simpwf/workflow-engine/internal/workflow/repository"
	"github.com/simpwf/workflow-engine/internal/workflow/statusupdate"
	"github.com/simpwf/workflow-engine/pkg/database"
	"gorm.io/datatypes"
	"gorm.io/gorm"
)

const (
	dispUserID = "bbbbbbbb-bbbb-7bbb-8bbb-bbbbbbbbbbbb"
	dispDefID  = "22222222-2222-7222-8222-222222222222"
	dispInstID = "11111111-1111-7111-8111-111111111111"
)

func setupTestDB(t *testing.T) *gorm.DB {
	t.Helper()
	ctx := context.Background()
	dsn := os.Getenv("TEST_DATABASE_DSN_STATUSUPDATE")
	if dsn == "" {
		dsn = os.Getenv("TEST_DATABASE_DSN")
	}
	if dsn == "" {
		t.Skip("TEST_DATABASE_DSN not set; skipping live database test")
	}
	opts := database.DefaultOptions()
	opts.DSN = dsn
	db, err := database.New(opts)
	if err != nil {
		t.Fatalf("database.New() error = %v", err)
	}
	t.Cleanup(func() {
		sqlDB, err := db.DB()
		if err == nil {
			_ = sqlDB.Close()
		}
	})
	if err := db.AutoMigrate(
		&repository.UserModel{},
		&repository.NodeDefinitionModel{},
		&repository.WorkflowDefinitionModel{},
		&repository.WorkflowDefinitionNodeRefModel{},
		&repository.WorkflowRequestModel{},
		&repository.WorkflowInstanceModel{},
		&repository.NodeInstanceModel{},
		&repository.WorkflowInstanceEventModel{},
		&repository.InputDeliveryModel{},
		&repository.StatusUpdateOutboxModel{},
	); err != nil {
		t.Fatalf("AutoMigrate() error = %v", err)
	}
	if err := db.Exec(`TRUNCATE TABLE
		status_update_outbox, input_deliveries, workflow_instance_events, node_instances,
		workflow_instances, workflow_requests, workflow_definition_node_refs,
		workflow_definitions, node_definitions, users RESTART IDENTITY`).Error; err != nil {
		t.Fatalf("truncate tables: %v", err)
	}
	now := time.Now().UTC()
	system := model.User{ID: dispUserID, Name: "fixture", Email: "fixture@localhost", CreatedAt: now, UpdatedAt: now}
	if err := repository.UpsertSystemUser(ctx, db, system); err != nil {
		t.Fatalf("seed system user: %v", err)
	}
	return db
}

// seedDef inserts a workflow definition whose content carries the given
// status_update http url.
func seedDef(t *testing.T, db *gorm.DB, url string) {
	t.Helper()
	now := time.Now().UTC()
	content, _ := json.Marshal(map[string]any{
		"status_update": map[string]any{
			"http": map[string]any{"url": url, "max_retry": 1, "retry_delay": "10ms"},
		},
	})
	wd := repository.WorkflowDefinitionToModel(model.WorkflowDefinition{
		ID: dispDefID, Name: "su-flow", Version: 1, LineageID: dispDefID,
		Content:   content,
		CreatedBy: dispUserID, UpdatedBy: dispUserID, CreatedAt: now, UpdatedAt: now,
	})
	if err := db.Create(&wd).Error; err != nil {
		t.Fatalf("seed definition: %v", err)
	}
}

func seedInstance(t *testing.T, db *gorm.DB, id string) {
	t.Helper()
	now := time.Now().UTC()
	w := model.WorkflowInstance{
		ID: id, WorkflowDefinitionID: dispDefID,
		Status: model.WorkflowWaiting, WaitingReason: model.WaitingReasonRunnable,
		Frame: json.RawMessage(`{}`), Context: json.RawMessage(`{}`), Counters: json.RawMessage(`{}`),
		CreatedBy: dispUserID, UpdatedBy: dispUserID, CreatedAt: now, UpdatedAt: now,
	}
	if err := repository.NewInstanceRepository(db).Insert(context.Background(), w); err != nil {
		t.Fatalf("insert instance: %v", err)
	}
}

func seedOutbox(t *testing.T, db *gorm.DB, id, instanceID string, revision int64) {
	t.Helper()
	now := time.Now().UTC().Add(-time.Minute)
	m := repository.StatusUpdateOutboxModel{
		ID: id, WorkflowInstanceID: instanceID, WorkflowDefinitionID: dispDefID,
		Revision: revision, EventIndex: 0, Transport: model.StatusUpdateTransportHTTP,
		// The payload id is the logical event id delivered to receivers.
		Payload:       datatypes.JSON(`{"event":"x","id":"` + id + `"}`),
		NextAttemptAt: now, CreatedAt: now, UpdatedAt: now,
	}
	if err := db.Create(&m).Error; err != nil {
		t.Fatalf("seed outbox: %v", err)
	}
}

// webhookRecorder records delivered event ids and can fail the first n
// requests to exercise retries.
type webhookRecorder struct {
	mu    sync.Mutex
	ids   []string
	fails int
}

func (w *webhookRecorder) handler() http.HandlerFunc {
	return func(rw http.ResponseWriter, r *http.Request) {
		w.mu.Lock()
		id := r.Header.Get("X-SimpWF-Event-ID")
		w.ids = append(w.ids, id)
		fail := w.fails > 0
		if fail {
			w.fails--
		}
		w.mu.Unlock()
		if fail {
			rw.WriteHeader(http.StatusInternalServerError)
			return
		}
		rw.WriteHeader(http.StatusOK)
	}
}

func (w *webhookRecorder) events() []string {
	w.mu.Lock()
	defer w.mu.Unlock()
	out := make([]string, len(w.ids))
	copy(out, w.ids)
	return out
}

func waitFor(t *testing.T, cond func() bool, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("condition not met within", timeout)
}

func newTestDispatcher(t *testing.T, db *gorm.DB, webhookURL string, opts statusupdate.DispatcherOptions) *statusupdate.Dispatcher {
	t.Helper()
	client := executor.NewHTTPExecutor(executor.Limits{
		HTTPAllowlist: []string{strings.TrimPrefix(webhookURL, "http://")},
		MaxRedirects:  5,
	})
	publisher := statusupdate.NewHTTPPublisher(client, 5*time.Second)
	loader := func(ctx context.Context, defID string) (*model.StatusUpdateConfig, error) {
		var def repository.WorkflowDefinitionModel
		if err := db.WithContext(ctx).Where("id = ?", defID).First(&def).Error; err != nil {
			return nil, err
		}
		return model.ParseStatusUpdate(json.RawMessage(def.Content))
	}
	d, err := statusupdate.NewDispatcher(context.Background(),
		repository.NewStatusUpdateRepository(db), loader,
		map[string]statusupdate.Publisher{model.StatusUpdateTransportHTTP: publisher},
		"test-worker", opts)
	if err != nil {
		t.Fatalf("NewDispatcher() error = %v", err)
	}
	return d
}

func TestDispatcherDeliversInOrder(t *testing.T) {
	db := setupTestDB(t)
	rec := &webhookRecorder{}
	srv := httptest.NewServer(rec.handler())
	defer srv.Close()

	seedDef(t, db, srv.URL)
	seedInstance(t, db, dispInstID)
	seedOutbox(t, db, "aaaaaaaa-aaaa-1aaa-8aaa-aaaaaaaaaa01", dispInstID, 1)
	seedOutbox(t, db, "aaaaaaaa-aaaa-1aaa-8aaa-aaaaaaaaaa02", dispInstID, 2)
	seedOutbox(t, db, "aaaaaaaa-aaaa-1aaa-8aaa-aaaaaaaaaa03", dispInstID, 3)

	d := newTestDispatcher(t, db, srv.URL, statusupdate.DispatcherOptions{
		PollInterval: 20 * time.Millisecond, Lease: time.Minute, BatchSize: 10, PoolSize: 4,
	})
	d.Run()
	defer func() { _ = d.Shutdown(context.Background()) }()

	want := []string{
		"aaaaaaaa-aaaa-1aaa-8aaa-aaaaaaaaaa01",
		"aaaaaaaa-aaaa-1aaa-8aaa-aaaaaaaaaa02",
		"aaaaaaaa-aaaa-1aaa-8aaa-aaaaaaaaaa03",
	}
	waitFor(t, func() bool { return len(rec.events()) >= 3 }, 15*time.Second)
	got := rec.events()
	for i, id := range want {
		if got[i] != id {
			t.Fatalf("delivery order = %v, want %v", got, want)
		}
	}
}

func TestDispatcherRetriesThenDeadLettersUnblocksNext(t *testing.T) {
	db := setupTestDB(t)
	rec := &webhookRecorder{fails: 2} // first two deliveries fail
	srv := httptest.NewServer(rec.handler())
	defer srv.Close()

	seedDef(t, db, srv.URL)
	seedInstance(t, db, dispInstID)
	e1 := "aaaaaaaa-aaaa-1aaa-8aaa-aaaaaaaaaa01"
	e2 := "aaaaaaaa-aaaa-1aaa-8aaa-aaaaaaaaaa02"
	seedOutbox(t, db, e1, dispInstID, 1)
	seedOutbox(t, db, e2, dispInstID, 2)

	d := newTestDispatcher(t, db, srv.URL, statusupdate.DispatcherOptions{
		PollInterval: 20 * time.Millisecond, Lease: time.Minute, BatchSize: 10, PoolSize: 4,
	})
	d.Run()
	defer func() { _ = d.Shutdown(context.Background()) }()

	// e1: initial + 1 retry (max_retry=1) then dead; e2 succeeds on its
	// first attempt. Order must stay per-instance strict.
	waitFor(t, func() bool { return len(rec.events()) >= 3 }, 15*time.Second)
	want := []string{e1, e1, e2}
	got := rec.events()
	for i, id := range want {
		if got[i] != id {
			t.Fatalf("delivery order = %v, want %v", got, want)
		}
	}

	var row1, row2 repository.StatusUpdateOutboxModel
	if err := db.Where("id = ?", e1).First(&row1).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Where("id = ?", e2).First(&row2).Error; err != nil {
		t.Fatal(err)
	}
	if row1.DeadAt == nil || row1.DeliveredAt != nil {
		t.Errorf("e1 = %+v, want dead-lettered", row1)
	}
	if row2.DeliveredAt == nil || row2.DeadAt != nil {
		t.Errorf("e2 = %+v, want delivered", row2)
	}
}

func TestDispatcherDeliversAcrossInstances(t *testing.T) {
	db := setupTestDB(t)
	rec := &webhookRecorder{}
	srv := httptest.NewServer(rec.handler())
	defer srv.Close()

	seedDef(t, db, srv.URL)
	seedInstance(t, db, "11111111-1111-7111-8111-111111111111")
	seedInstance(t, db, "22222222-2222-7222-8222-222222222222")
	seedOutbox(t, db, "aaaaaaaa-aaaa-1aaa-8aaa-aaaaaaaaaa01", "11111111-1111-7111-8111-111111111111", 1)
	seedOutbox(t, db, "aaaaaaaa-aaaa-1aaa-8aaa-aaaaaaaaaa02", "22222222-2222-7222-8222-222222222222", 1)

	d := newTestDispatcher(t, db, srv.URL, statusupdate.DispatcherOptions{
		PollInterval: 20 * time.Millisecond, Lease: time.Minute, BatchSize: 10, PoolSize: 4,
	})
	d.Run()
	defer func() { _ = d.Shutdown(context.Background()) }()

	waitFor(t, func() bool { return len(rec.events()) >= 2 }, 15*time.Second)
	got := rec.events()
	if len(got) != 2 {
		t.Fatalf("deliveries = %v, want 2 (one per instance)", got)
	}
}

func TestDispatcherShutdown(t *testing.T) {
	db := setupTestDB(t)
	rec := &webhookRecorder{}
	srv := httptest.NewServer(rec.handler())
	defer srv.Close()

	seedDef(t, db, srv.URL)
	d := newTestDispatcher(t, db, srv.URL, statusupdate.DispatcherOptions{
		PollInterval: 20 * time.Millisecond, Lease: time.Minute, BatchSize: 10, PoolSize: 4,
	})
	d.Run()
	time.Sleep(50 * time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := d.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown() error = %v", err)
	}
}
