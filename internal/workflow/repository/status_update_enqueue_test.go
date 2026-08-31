package repository_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/simpwf/workflow-engine/internal/workflow/model"
	"github.com/simpwf/workflow-engine/internal/workflow/repository"
	"gorm.io/gorm"
)

const (
	suDefID      = "44444444-4444-7444-8444-444444444444"
	suInstanceID = "11111111-1111-7111-8111-111111111111"
	suAttemptID  = "aaaaaaaa-aaaa-7aaa-8aaa-aaaaaaaaaaaa"
)

const suContent = `{
	"start_node_id": "aaaaaaaa-aaaa-7aaa-8aaa-aaaaaaaaaaaa",
	"status_update": {"http": {"url": "https://hooks.example.com/wf"}},
	"nodes": [
		{"id": "aaaaaaaa-aaaa-7aaa-8aaa-aaaaaaaaaaaa", "type": "script", "script": "return 1;"}
	]
}`

// seedStatusUpdateDef inserts a workflow definition whose content carries the
// given status_update block.
func seedStatusUpdateDef(t *testing.T, db *gorm.DB, id, content string) {
	t.Helper()
	now := time.Now().UTC()
	wd := repository.WorkflowDefinitionToModel(model.WorkflowDefinition{
		ID: id, Name: "su-flow", Version: 1, LineageID: id,
		Content:   json.RawMessage(content),
		CreatedBy: fixtureUserID, UpdatedBy: fixtureUserID, CreatedAt: now, UpdatedAt: now,
	})
	if err := db.Create(&wd).Error; err != nil {
		t.Fatalf("seed status update definition: %v", err)
	}
}

// newSUInstance is a test instance referencing the status-update definition.
func newSUInstance(id string, status model.WorkflowStatus, reason model.WaitingReason) model.WorkflowInstance {
	w := newTestInstance(id, status, reason)
	w.WorkflowDefinitionID = suDefID
	return w
}

// outboxRows loads the outbox rows of an instance in delivery order.
func outboxRows(t *testing.T, db *gorm.DB, instanceID string) []repository.StatusUpdateOutboxModel {
	t.Helper()
	var rows []repository.StatusUpdateOutboxModel
	if err := db.Where("workflow_instance_id = ?", instanceID).Order("revision, event_index").Find(&rows).Error; err != nil {
		t.Fatal(err)
	}
	return rows
}

// mustPayload unmarshals an outbox row's immutable payload.
func mustPayload(t *testing.T, m repository.StatusUpdateOutboxModel) model.StatusUpdateEventPayload {
	t.Helper()
	var p model.StatusUpdateEventPayload
	if err := json.Unmarshal(m.Payload, &p); err != nil {
		t.Fatalf("parse outbox payload: %v", err)
	}
	return p
}

func TestCheckpointEnqueuesWaitingForInput(t *testing.T) {
	db := setupTestDB(t)
	seedInstanceFixture(t, db)
	seedStatusUpdateDef(t, db, suDefID, suContent)
	ctx := context.Background()

	w := newSUInstance(suInstanceID, model.WorkflowRunning, "")
	w.LeasedBy = "worker-1"
	insertInstance(t, db, w)

	repo := repository.NewInstanceRepository(db)
	frame := model.NewFrame("node-1")
	err := repo.Checkpoint(ctx, repository.Checkpoint{
		InstanceID:           w.ID,
		WorkerID:             "worker-1",
		Revision:             w.Revision,
		WorkflowDefinitionID: suDefID,
		FromStatus:           model.WorkflowRunning,
		Status:               model.WorkflowWaiting,
		WaitingReason:        model.WaitingReasonInput,
		Frame:                frame,
		Counters:             model.Counters{},
		Context:              json.RawMessage(`{}`),
	})
	if err != nil {
		t.Fatalf("Checkpoint() error = %v", err)
	}

	rows := outboxRows(t, db, w.ID)
	if len(rows) != 1 {
		t.Fatalf("outbox rows = %d, want 1", len(rows))
	}
	p := mustPayload(t, rows[0])
	if p.Event != model.StatusUpdateEventWaitingForInput {
		t.Errorf("event = %q, want %q", p.Event, model.StatusUpdateEventWaitingForInput)
	}
	if p.Type != model.StatusUpdateEventType {
		t.Errorf("type = %q, want %q", p.Type, model.StatusUpdateEventType)
	}
	if p.FromStatus != "running" || p.ToStatus != "waiting" || p.ToWaitingReason != "input" {
		t.Errorf("status fields = from %q/%q to %q/%q", p.FromStatus, p.FromWaitingReason, p.ToStatus, p.ToWaitingReason)
	}
	if p.Revision != 1 {
		t.Errorf("revision = %d, want 1", p.Revision)
	}
	if p.WorkflowInstanceID != w.ID || p.WorkflowDefinitionID != suDefID {
		t.Errorf("ids mismatch: %+v", p)
	}
	if p.ID == "" {
		t.Error("payload id is empty")
	}
	if p.ID == rows[0].ID {
		t.Error("payload id equals row id; with per-transport rows the row id must be distinct from the shared logical event id")
	}
	if rows[0].Transport != model.StatusUpdateTransportHTTP {
		t.Errorf("transport = %q, want %q", rows[0].Transport, model.StatusUpdateTransportHTTP)
	}
}

const suFanoutContent = `{
	"start_node_id": "aaaaaaaa-aaaa-7aaa-8aaa-aaaaaaaaaaaa",
	"status_update": {
		"http": {"url": "https://hooks.example.com/wf"},
		"redis": {"max_retry": 2, "retry_delay": "3s"},
		"rabbitmq": {"max_retry": 1}
	},
	"nodes": [
		{"id": "aaaaaaaa-aaaa-7aaa-8aaa-aaaaaaaaaaaa", "type": "script", "script": "return 1;"}
	]
}`

func TestCheckpointFansOutAcrossTransports(t *testing.T) {
	db := setupTestDB(t)
	seedInstanceFixture(t, db)
	seedStatusUpdateDef(t, db, suDefID, suFanoutContent)
	ctx := context.Background()

	w := newSUInstance(suInstanceID, model.WorkflowRunning, "")
	w.LeasedBy = "worker-1"
	insertInstance(t, db, w)

	repo := repository.NewInstanceRepository(db)
	err := repo.Checkpoint(ctx, repository.Checkpoint{
		InstanceID: w.ID, WorkerID: "worker-1", Revision: w.Revision,
		WorkflowDefinitionID: suDefID, FromStatus: model.WorkflowRunning,
		Status: model.WorkflowFinished, Frame: model.NewFrame("node-1"),
		Counters: model.Counters{}, Context: json.RawMessage(`{}`),
	})
	if err != nil {
		t.Fatalf("Checkpoint() error = %v", err)
	}

	rows := outboxRows(t, db, w.ID)
	if len(rows) != 3 {
		t.Fatalf("outbox rows = %d, want 3 (one per transport)", len(rows))
	}
	wantTransports := []string{"http", "redis", "rabbitmq"}
	logicalIDs := map[string]bool{}
	rowIDs := map[string]bool{}
	for i, row := range rows {
		if row.Transport != wantTransports[i] {
			t.Errorf("rows[%d] transport = %q, want %q", i, row.Transport, wantTransports[i])
		}
		if row.Revision != 1 || row.EventIndex != 0 {
			t.Errorf("rows[%d] = rev %d idx %d, want 1/0", i, row.Revision, row.EventIndex)
		}
		rowIDs[row.ID] = true
		p := mustPayload(t, row)
		logicalIDs[p.ID] = true
		if row.ID == p.ID {
			t.Errorf("rows[%d] row id equals payload id; want distinct per-transport row id", i)
		}
	}
	if len(rowIDs) != 3 {
		t.Errorf("row ids = %d distinct, want 3", len(rowIDs))
	}
	if len(logicalIDs) != 1 {
		t.Errorf("logical event ids shared across transports = %d, want 1", len(logicalIDs))
	}
}

func TestTransportsProgressIndependently(t *testing.T) {
	db := setupTestDB(t)
	seedInstanceFixture(t, db)
	seedStatusUpdateDef(t, db, suDefID, `{
		"start_node_id": "aaaaaaaa-aaaa-7aaa-8aaa-aaaaaaaaaaaa",
		"status_update": {"http": {"url": "https://hooks.example.com/wf"}, "redis": {}},
		"nodes": [
			{"id": "aaaaaaaa-aaaa-7aaa-8aaa-aaaaaaaaaaaa", "type": "script", "script": "return 1;"}
		]
	}`)
	ctx := context.Background()

	w := newSUInstance(suInstanceID, model.WorkflowRunning, "")
	w.LeasedBy = "worker-1"
	insertInstance(t, db, w)
	repo := repository.NewInstanceRepository(db)
	if err := repo.Checkpoint(ctx, repository.Checkpoint{
		InstanceID: w.ID, WorkerID: "worker-1", Revision: w.Revision,
		WorkflowDefinitionID: suDefID, FromStatus: model.WorkflowRunning,
		Status: model.WorkflowFinished, Frame: model.NewFrame("node-1"),
		Counters: model.Counters{}, Context: json.RawMessage(`{}`),
	}); err != nil {
		t.Fatalf("Checkpoint() error = %v", err)
	}

	rows := outboxRows(t, db, w.ID)
	if len(rows) != 2 {
		t.Fatalf("outbox rows = %d, want 2", len(rows))
	}
	var httpRow, redisRow repository.StatusUpdateOutboxModel
	for _, row := range rows {
		switch row.Transport {
		case model.StatusUpdateTransportHTTP:
			httpRow = row
		case model.StatusUpdateTransportRedis:
			redisRow = row
		}
	}
	if httpRow.ID == "" || redisRow.ID == "" {
		t.Fatalf("missing transport rows: %+v", rows)
	}

	// Both rows of the same instance are claimable: predecessor blocking is
	// scoped per transport, so the transports do not block each other.
	out := repository.NewStatusUpdateRepository(db)
	claimed, err := out.ClaimNextStatusUpdates(ctx, "worker-1", time.Minute, 10)
	if err != nil {
		t.Fatalf("ClaimNextStatusUpdates() error = %v", err)
	}
	if len(claimed) != 2 {
		t.Fatalf("claimed = %d, want 2 (one per transport)", len(claimed))
	}
	claimedTransports := map[string]bool{}
	for _, ev := range claimed {
		claimedTransports[ev.Transport] = true
	}
	if !claimedTransports[model.StatusUpdateTransportHTTP] || !claimedTransports[model.StatusUpdateTransportRedis] {
		t.Errorf("claimed transports = %v, want http and redis", claimedTransports)
	}

	// Delivering only the http row leaves the redis row pending; failing the
	// redis row (releases its claim, schedules a retry) must not touch the
	// delivered http row, and the next claim returns just the redis row.
	if err := out.MarkStatusUpdateDelivered(ctx, httpRow.ID, "worker-1"); err != nil {
		t.Fatalf("MarkStatusUpdateDelivered() error = %v", err)
	}
	if err := out.FailStatusUpdate(ctx, redisRow.ID, "worker-1", 0, 3, "boom"); err != nil {
		t.Fatalf("FailStatusUpdate() error = %v", err)
	}
	next, err := out.ClaimNextStatusUpdates(ctx, "worker-2", time.Minute, 10)
	if err != nil {
		t.Fatalf("second ClaimNextStatusUpdates() error = %v", err)
	}
	if len(next) != 1 || next[0].Transport != model.StatusUpdateTransportRedis || next[0].ID != redisRow.ID {
		t.Fatalf("second claim = %+v, want only the redis row", next)
	}
	// The redis row carries the same logical event id as the http row.
	if next[0].LogicalID == "" || next[0].LogicalID != mustPayload(t, httpRow).ID {
		t.Errorf("redis logical id = %q, want shared logical id %q", next[0].LogicalID, mustPayload(t, httpRow).ID)
	}
	var httpAfter repository.StatusUpdateOutboxModel
	if err := db.Where("id = ?", httpRow.ID).First(&httpAfter).Error; err != nil {
		t.Fatal(err)
	}
	if httpAfter.DeliveredAt == nil || httpAfter.Attempts != 0 {
		t.Errorf("http row after redis failure = %+v, want delivered with 0 attempts", httpAfter)
	}
}

func TestCheckpointSuppressesSchedulerChurn(t *testing.T) {
	db := setupTestDB(t)
	seedInstanceFixture(t, db)
	seedStatusUpdateDef(t, db, suDefID, suContent)
	ctx := context.Background()

	w := newSUInstance(suInstanceID, model.WorkflowRunning, "")
	w.LeasedBy = "worker-1"
	insertInstance(t, db, w)

	repo := repository.NewInstanceRepository(db)
	err := repo.Checkpoint(ctx, repository.Checkpoint{
		InstanceID: w.ID, WorkerID: "worker-1", Revision: w.Revision,
		WorkflowDefinitionID: suDefID, FromStatus: model.WorkflowRunning,
		Status: model.WorkflowWaiting, WaitingReason: model.WaitingReasonRunnable,
		Frame: model.NewFrame("node-1"), Counters: model.Counters{}, Context: json.RawMessage(`{}`),
	})
	if err != nil {
		t.Fatalf("Checkpoint() error = %v", err)
	}
	if rows := outboxRows(t, db, w.ID); len(rows) != 0 {
		t.Errorf("outbox rows = %d, want 0 (scheduler churn suppressed)", len(rows))
	}
}

func TestCheckpointEnqueuesTerminalStates(t *testing.T) {
	db := setupTestDB(t)
	seedInstanceFixture(t, db)
	seedStatusUpdateDef(t, db, suDefID, suContent)
	ctx := context.Background()

	for _, tc := range []struct {
		name       string
		status     model.WorkflowStatus
		errMsg     string
		wantEvent  string
		wantReason string
	}{
		{"finished", model.WorkflowFinished, "", model.StatusUpdateEventFinished, ""},
		{"failed", model.WorkflowFailed, "boom", model.StatusUpdateEventFailed, ""},
		{"stopped", model.WorkflowStopped, "cancel", model.StatusUpdateEventStopped, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := newSUInstance(suInstanceID, model.WorkflowRunning, "")
			w.LeasedBy = "worker-1"
			insertInstance(t, db, w)
			defer func() {
				db.Exec(`DELETE FROM status_update_outbox WHERE workflow_instance_id = ?`, w.ID)
				db.Exec(`DELETE FROM workflow_instances WHERE id = ?`, w.ID)
			}()
			repo := repository.NewInstanceRepository(db)
			err := repo.Checkpoint(ctx, repository.Checkpoint{
				InstanceID: w.ID, WorkerID: "worker-1", Revision: w.Revision,
				WorkflowDefinitionID: suDefID, FromStatus: model.WorkflowRunning,
				Status: tc.status, Frame: model.NewFrame("node-1"),
				Counters: model.Counters{}, Context: json.RawMessage(`{}`), Error: tc.errMsg,
			})
			if err != nil {
				t.Fatalf("Checkpoint() error = %v", err)
			}
			rows := outboxRows(t, db, w.ID)
			if len(rows) != 1 {
				t.Fatalf("outbox rows = %d, want 1", len(rows))
			}
			p := mustPayload(t, rows[0])
			if p.Event != tc.wantEvent {
				t.Errorf("event = %q, want %q", p.Event, tc.wantEvent)
			}
			if p.ToStatus != string(tc.status) {
				t.Errorf("to_status = %q, want %q", p.ToStatus, tc.status)
			}
			if p.Error != tc.errMsg {
				t.Errorf("error = %q, want %q", p.Error, tc.errMsg)
			}
			if tc.name == "stopped" && p.ToWaitingReason != tc.wantReason {
				t.Errorf("to_waiting_reason = %q, want %q", p.ToWaitingReason, tc.wantReason)
			}
		})
	}
}

func TestCheckpointWithoutStatusUpdateConfigEnqueuesNothing(t *testing.T) {
	db := setupTestDB(t)
	seedInstanceFixture(t, db) // fixture def content has no status_update
	ctx := context.Background()

	w := newTestInstance(suInstanceID, model.WorkflowRunning, "")
	w.LeasedBy = "worker-1"
	insertInstance(t, db, w)

	repo := repository.NewInstanceRepository(db)
	err := repo.Checkpoint(ctx, repository.Checkpoint{
		InstanceID: w.ID, WorkerID: "worker-1", Revision: w.Revision,
		WorkflowDefinitionID: fixtureWorkflowDefID, FromStatus: model.WorkflowRunning,
		Status: model.WorkflowFinished, Frame: model.NewFrame("node-1"),
		Counters: model.Counters{}, Context: json.RawMessage(`{}`),
	})
	if err != nil {
		t.Fatalf("Checkpoint() error = %v", err)
	}
	if rows := outboxRows(t, db, w.ID); len(rows) != 0 {
		t.Errorf("outbox rows = %d, want 0 without status_update config", len(rows))
	}
}

func TestPauseEnqueuesPausedIdempotently(t *testing.T) {
	db := setupTestDB(t)
	seedInstanceFixture(t, db)
	seedStatusUpdateDef(t, db, suDefID, suContent)
	ctx := context.Background()

	insertInstance(t, db, newSUInstance(suInstanceID, model.WorkflowWaiting, ""))
	repo := repository.NewInstanceRepository(db)

	deferred, err := repo.Pause(ctx, suInstanceID)
	if err != nil {
		t.Fatalf("Pause() error = %v", err)
	}
	if deferred {
		t.Error("Pause() deferred = true, want immediate")
	}
	rows := outboxRows(t, db, suInstanceID)
	if len(rows) != 1 {
		t.Fatalf("outbox rows = %d, want 1", len(rows))
	}
	p := mustPayload(t, rows[0])
	if p.Event != model.StatusUpdateEventPaused {
		t.Errorf("event = %q, want %q", p.Event, model.StatusUpdateEventPaused)
	}
	if p.FromStatus != "waiting" || p.ToStatus != "paused" {
		t.Errorf("status fields = from %q to %q", p.FromStatus, p.ToStatus)
	}

	// Idempotent second pause must not duplicate the event.
	if _, err := repo.Pause(ctx, suInstanceID); err != nil {
		t.Fatalf("second Pause() error = %v", err)
	}
	if rows := outboxRows(t, db, suInstanceID); len(rows) != 1 {
		t.Errorf("outbox rows after idempotent pause = %d, want 1", len(rows))
	}
}

func TestPauseDeferredRunningNoEvent(t *testing.T) {
	db := setupTestDB(t)
	seedInstanceFixture(t, db)
	seedStatusUpdateDef(t, db, suDefID, suContent)
	ctx := context.Background()

	w := newSUInstance(suInstanceID, model.WorkflowRunning, "")
	w.LeasedBy = "worker-1"
	insertInstance(t, db, w)

	repo := repository.NewInstanceRepository(db)
	deferred, err := repo.Pause(ctx, suInstanceID)
	if err != nil {
		t.Fatalf("Pause() error = %v", err)
	}
	if !deferred {
		t.Error("Pause() deferred = false, want true for running instance")
	}
	if rows := outboxRows(t, db, suInstanceID); len(rows) != 0 {
		t.Errorf("outbox rows = %d, want 0 (pause_requested flag only)", len(rows))
	}
}

func TestResumeEnqueuesResumedPreservingReason(t *testing.T) {
	db := setupTestDB(t)
	seedInstanceFixture(t, db)
	seedStatusUpdateDef(t, db, suDefID, suContent)
	ctx := context.Background()

	insertInstance(t, db, newSUInstance(suInstanceID, model.WorkflowPaused, model.WaitingReasonInput))
	repo := repository.NewInstanceRepository(db)

	if err := repo.Resume(ctx, suInstanceID); err != nil {
		t.Fatalf("Resume() error = %v", err)
	}
	rows := outboxRows(t, db, suInstanceID)
	if len(rows) != 1 {
		t.Fatalf("outbox rows = %d, want 1", len(rows))
	}
	p := mustPayload(t, rows[0])
	if p.Event != model.StatusUpdateEventResumed {
		t.Errorf("event = %q, want %q", p.Event, model.StatusUpdateEventResumed)
	}
	if p.FromStatus != "paused" || p.ToStatus != "waiting" || p.ToWaitingReason != "input" {
		t.Errorf("status fields = from %q/%q to %q/%q", p.FromStatus, p.FromWaitingReason, p.ToStatus, p.ToWaitingReason)
	}

	// Idempotent second resume must not duplicate.
	if err := repo.Resume(ctx, suInstanceID); err != nil {
		t.Fatalf("second Resume() error = %v", err)
	}
	if rows := outboxRows(t, db, suInstanceID); len(rows) != 1 {
		t.Errorf("outbox rows after idempotent resume = %d, want 1", len(rows))
	}
}

func TestStopEnqueuesStopped(t *testing.T) {
	db := setupTestDB(t)
	seedInstanceFixture(t, db)
	seedStatusUpdateDef(t, db, suDefID, suContent)
	ctx := context.Background()

	insertInstance(t, db, newSUInstance(suInstanceID, model.WorkflowWaiting, ""))
	repo := repository.NewInstanceRepository(db)

	if _, err := repo.Stop(ctx, suInstanceID, "user cancelled"); err != nil {
		t.Fatalf("Stop() error = %v", err)
	}
	rows := outboxRows(t, db, suInstanceID)
	if len(rows) != 1 {
		t.Fatalf("outbox rows = %d, want 1", len(rows))
	}
	p := mustPayload(t, rows[0])
	if p.Event != model.StatusUpdateEventStopped || p.Error != "user cancelled" {
		t.Errorf("payload = %+v", p)
	}
	if p.FromStatus != "waiting" || p.ToStatus != "stopped" {
		t.Errorf("status fields = from %q to %q", p.FromStatus, p.ToStatus)
	}
}

func TestDeliverInputEnqueuesInputReceived(t *testing.T) {
	db := setupTestDB(t)
	seedInstanceFixture(t, db)
	seedStatusUpdateDef(t, db, suDefID, suContent)
	ctx := context.Background()

	insertInstance(t, db, newSUInstance(suInstanceID, model.WorkflowWaiting, model.WaitingReasonInput))
	now := time.Now().UTC()
	attempt := model.NodeInstance{
		ID: suAttemptID, WorkflowInstanceID: suInstanceID,
		NodeID: suAttemptID, NodeDefinitionID: fixtureNodeDefID, Name: "input", Type: string(model.NodeTypeInput),
		Attempt: 1, Status: model.NodeRunning, CreatedAt: now, UpdatedAt: now,
	}
	repo := repository.NewInstanceRepository(db)
	if err := repo.InsertNodeInstance(ctx, attempt); err != nil {
		t.Fatalf("InsertNodeInstance() error = %v", err)
	}

	frame := model.NewFrame("node-1")
	if _, err := repo.DeliverInput(ctx, repository.InputCompletion{
		InstanceID: suInstanceID, NodeInstanceID: suAttemptID, IdempotencyKey: "key-1",
		Payload: json.RawMessage(`{"ok":true}`), Accepted: true,
		NewFrame: frame, NewContext: json.RawMessage(`{"x":1}`),
		Status: model.WorkflowWaiting, CreatedBy: fixtureUserID,
	}); err != nil {
		t.Fatalf("DeliverInput() error = %v", err)
	}

	rows := outboxRows(t, db, suInstanceID)
	if len(rows) != 1 {
		t.Fatalf("outbox rows = %d, want 1", len(rows))
	}
	p := mustPayload(t, rows[0])
	if p.Event != model.StatusUpdateEventInputReceived {
		t.Errorf("event = %q, want %q", p.Event, model.StatusUpdateEventInputReceived)
	}
	if p.FromStatus != "waiting" || p.FromWaitingReason != "input" {
		t.Errorf("from fields = %q/%q, want waiting/input", p.FromStatus, p.FromWaitingReason)
	}
	// WaitingReasonRunnable is the empty string.
	if p.ToStatus != "waiting" || p.ToWaitingReason != "" {
		t.Errorf("to fields = %q/%q, want waiting/runnable", p.ToStatus, p.ToWaitingReason)
	}
}

func TestDeliverInputTerminalEnqueuesInputReceivedThenFinished(t *testing.T) {
	db := setupTestDB(t)
	seedInstanceFixture(t, db)
	seedStatusUpdateDef(t, db, suDefID, suContent)
	ctx := context.Background()

	insertInstance(t, db, newSUInstance(suInstanceID, model.WorkflowWaiting, model.WaitingReasonInput))
	now := time.Now().UTC()
	attempt := model.NodeInstance{
		ID: suAttemptID, WorkflowInstanceID: suInstanceID,
		NodeID: suAttemptID, NodeDefinitionID: fixtureNodeDefID, Name: "input", Type: string(model.NodeTypeInput),
		Attempt: 1, Status: model.NodeRunning, CreatedAt: now, UpdatedAt: now,
	}
	repo := repository.NewInstanceRepository(db)
	if err := repo.InsertNodeInstance(ctx, attempt); err != nil {
		t.Fatalf("InsertNodeInstance() error = %v", err)
	}

	frame := model.NewFrame("node-1")
	if _, err := repo.DeliverInput(ctx, repository.InputCompletion{
		InstanceID: suInstanceID, NodeInstanceID: suAttemptID, IdempotencyKey: "key-1",
		Payload: json.RawMessage(`{"ok":true}`), Accepted: true,
		NewFrame: frame, NewContext: json.RawMessage(`{"x":1}`),
		Status: model.WorkflowFinished, CreatedBy: fixtureUserID,
	}); err != nil {
		t.Fatalf("DeliverInput() error = %v", err)
	}

	rows := outboxRows(t, db, suInstanceID)
	if len(rows) != 2 {
		t.Fatalf("outbox rows = %d, want 2", len(rows))
	}
	p0 := mustPayload(t, rows[0])
	p1 := mustPayload(t, rows[1])
	if p0.Event != model.StatusUpdateEventInputReceived {
		t.Errorf("first event = %q, want %q", p0.Event, model.StatusUpdateEventInputReceived)
	}
	if p1.Event != model.StatusUpdateEventFinished {
		t.Errorf("second event = %q, want %q", p1.Event, model.StatusUpdateEventFinished)
	}
	if p0.Revision != p1.Revision || rows[0].Revision != rows[1].Revision {
		t.Errorf("revisions differ: %d vs %d", rows[0].Revision, rows[1].Revision)
	}
	if rows[0].EventIndex != 0 || rows[1].EventIndex != 1 {
		t.Errorf("event indexes = %d, %d; want 0, 1", rows[0].EventIndex, rows[1].EventIndex)
	}
}

func TestPauseRollsBackStatusUpdateOutbox(t *testing.T) {
	db := setupTestDB(t)
	seedInstanceFixture(t, db)
	seedStatusUpdateDef(t, db, suDefID, `{
		"start_node_id": "aaaaaaaa-aaaa-7aaa-8aaa-aaaaaaaaaaaa",
		"status_update": {"redis": {"max_retry": -1}},
		"nodes": [
			{"id": "aaaaaaaa-aaaa-7aaa-8aaa-aaaaaaaaaaaa", "type": "script", "script": "return 1;"}
		]
	}`)
	ctx := context.Background()

	insertInstance(t, db, newSUInstance(suInstanceID, model.WorkflowWaiting, ""))
	repo := repository.NewInstanceRepository(db)

	if _, err := repo.Pause(ctx, suInstanceID); err == nil {
		t.Fatal("Pause() error = nil, want error from invalid status_update config")
	}
	inst, err := repo.GetByID(ctx, suInstanceID)
	if err != nil {
		t.Fatal(err)
	}
	if inst.Status != model.WorkflowWaiting {
		t.Errorf("instance status = %s, want waiting (rollback)", inst.Status)
	}
	if rows := outboxRows(t, db, suInstanceID); len(rows) != 0 {
		t.Errorf("outbox rows = %d, want 0 (rollback)", len(rows))
	}
}
