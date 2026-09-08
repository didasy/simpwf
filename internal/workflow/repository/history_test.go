package repository_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/simpwf/workflow-engine/internal/workflow/model"
	"github.com/simpwf/workflow-engine/internal/workflow/repository"
)

const historyInstanceID = "aaaaaaaa-aaaa-7aaa-8aaa-aaaaaaaaaaaa"

func historyRow(id string, createdAt time.Time, anchor bool, snapshot, diff string) model.NodeContextHistory {
	return model.NodeContextHistory{
		ID:                 id,
		WorkflowInstanceID: historyInstanceID,
		OccurrenceID:       "occ-" + id[0:4],
		NodeID:             "bbbbbbbb-bbbb-7bbb-8bbb-bbbbbbbbbbbb",
		Attempt:            1,
		IsAnchor:           anchor,
		Snapshot:           json.RawMessage(snapshot),
		Diff:               json.RawMessage(diff),
		CreatedAt:          createdAt,
	}
}

func TestHistoryCursorOrdersTimestampThenID(t *testing.T) {
	now := time.Date(2026, 9, 7, 12, 0, 0, 0, time.UTC)
	low := model.HistoryCursor{CreatedAt: now, ID: "a"}
	high := model.HistoryCursor{CreatedAt: now, ID: "b"}
	if !low.Before(high) || !low.AtOrBefore(high) || !high.AtOrBefore(high) || high.Before(low) {
		t.Fatalf("cursor comparison did not order equal timestamps by id")
	}
	if !low.AtOrBefore(low) || high.Before(low) {
		t.Fatalf("cursor comparison equality/order incorrect")
	}
}

func TestHistoryAppendLoadOrdersByCursor(t *testing.T) {
	db := setupTestDB(t)
	seedInstanceFixture(t, db)
	insertInstance(t, db, newTestInstance(historyInstanceID, model.WorkflowWaiting, ""))
	repo := repository.NewInstanceRepository(db)
	ctx := context.Background()
	base := time.Date(2026, 9, 7, 12, 0, 0, 0, time.UTC)

	rows := []model.NodeContextHistory{
		historyRow("cccccccc-cccc-7ccc-8ccc-cccccccccc02", base.Add(time.Second), false, "null", `{"set":{},"unset":[]}`),
		historyRow("cccccccc-cccc-7ccc-8ccc-cccccccccc01", base, true, `{"value":1}`, `{"set":{},"unset":[]}`),
		historyRow("cccccccc-cccc-7ccc-8ccc-cccccccccc03", base.Add(time.Second), false, "null", `{"set":{},"unset":[]}`),
	}
	for _, row := range []int{0, 2, 1} {
		if err := repo.AppendHistory(ctx, rows[row]); err != nil {
			t.Fatalf("AppendHistory() error = %v", err)
		}
	}

	got, err := repo.LoadHistory(ctx, historyInstanceID)
	if err != nil {
		t.Fatalf("LoadHistory() error = %v", err)
	}
	if len(got) != len(rows) {
		t.Fatalf("LoadHistory() returned %d rows, want %d", len(got), len(rows))
	}
	wantIDs := []string{
		"cccccccc-cccc-7ccc-8ccc-cccccccccc01",
		"cccccccc-cccc-7ccc-8ccc-cccccccccc02",
		"cccccccc-cccc-7ccc-8ccc-cccccccccc03",
	}
	for i, want := range wantIDs {
		if got[i].ID != want {
			t.Errorf("row %d id = %q, want %q", i, got[i].ID, want)
		}
	}
}

func TestHistoryReconstructBeforeReplaysAnchorAndDiffs(t *testing.T) {
	db := setupTestDB(t)
	seedInstanceFixture(t, db)
	insertInstance(t, db, newTestInstance(historyInstanceID, model.WorkflowWaiting, ""))
	repo := repository.NewInstanceRepository(db)
	ctx := context.Background()
	base := time.Date(2026, 9, 7, 12, 0, 0, 0, time.UTC)
	anchor := historyRow("cccccccc-cccc-7ccc-8ccc-cccccccccc11", base, true, `{"count":1,"nested":{"keep":true}}`, `{"set":{},"unset":[]}`)
	diff := historyRow("cccccccc-cccc-7ccc-8ccc-cccccccccc12", base.Add(time.Second), false, "null", `{"set":{"count":2,"nested.value":"changed"},"unset":[]}`)
	target := historyRow("cccccccc-cccc-7ccc-8ccc-cccccccccc13", base.Add(2*time.Second), false, "null", `{"set":{"count":3},"unset":[]}`)
	for _, row := range []model.NodeContextHistory{anchor, diff, target} {
		if err := repo.AppendHistory(ctx, row); err != nil {
			t.Fatalf("AppendHistory() error = %v", err)
		}
	}

	got, err := repo.ReconstructBefore(ctx, historyInstanceID, target.Cursor(), json.RawMessage(`{"initial":true}`))
	if err != nil {
		t.Fatalf("ReconstructBefore() error = %v", err)
	}
	want := `{"count":2,"nested":{"keep":true,"value":"changed"}}`
	if string(got) != want {
		t.Errorf("reconstructed context = %s, want %s", got, want)
	}
}

func TestHistoryReconstructBeforeUsesInitialContextWithoutAnchor(t *testing.T) {
	db := setupTestDB(t)
	seedInstanceFixture(t, db)
	insertInstance(t, db, newTestInstance(historyInstanceID, model.WorkflowWaiting, ""))
	repo := repository.NewInstanceRepository(db)
	ctx := context.Background()
	base := time.Date(2026, 9, 7, 12, 0, 0, 0, time.UTC)
	diff := historyRow("cccccccc-cccc-7ccc-8ccc-cccccccccc21", base, false, "null", `{"set":{"count":2},"unset":["gone"]}`)
	target := historyRow("cccccccc-cccc-7ccc-8ccc-cccccccccc22", base.Add(time.Second), false, "null", `{"set":{},"unset":[]}`)
	for _, row := range []model.NodeContextHistory{diff, target} {
		if err := repo.AppendHistory(ctx, row); err != nil {
			t.Fatalf("AppendHistory() error = %v", err)
		}
	}

	got, err := repo.ReconstructBefore(ctx, historyInstanceID, target.Cursor(), json.RawMessage(`{"count":1,"gone":true}`))
	if err != nil {
		t.Fatalf("ReconstructBefore() error = %v", err)
	}
	if string(got) != `{"count":2}` {
		t.Errorf("reconstructed context = %s, want %s", got, `{"count":2}`)
	}
}

func TestHistoryReconstructBeforeFailsClosedOnGap(t *testing.T) {
	db := setupTestDB(t)
	seedInstanceFixture(t, db)
	insertInstance(t, db, newTestInstance(historyInstanceID, model.WorkflowWaiting, ""))
	repo := repository.NewInstanceRepository(db)
	ctx := context.Background()
	row := historyRow(
		"cccccccc-cccc-7ccc-8ccc-cccccccccc31",
		time.Date(2026, 9, 7, 12, 0, 0, 0, time.UTC),
		false,
		"null",
		`{"not":"a diff"}`,
	)
	if err := repo.AppendHistory(ctx, row); err != nil {
		t.Fatalf("AppendHistory() error = %v", err)
	}

	_, err := repo.ReconstructBefore(ctx, historyInstanceID, model.HistoryCursor{
		CreatedAt: row.CreatedAt.Add(time.Second),
		ID:        "cccccccc-cccc-7ccc-8ccc-cccccccccc32",
	}, json.RawMessage(`{"count":1}`))
	if !errors.Is(err, repository.ErrHistoryGap) {
		t.Errorf("ReconstructBefore() error = %v, want ErrHistoryGap", err)
	}
}

func TestHistoryReconstructBeforeEnforcesDepth(t *testing.T) {
	db := setupTestDB(t)
	seedInstanceFixture(t, db)
	insertInstance(t, db, newTestInstance(historyInstanceID, model.WorkflowWaiting, ""))
	repo := repository.NewInstanceRepositoryWithOptions(db, model.LeanOptions{ReplayMax: 2})
	ctx := context.Background()
	base := time.Date(2026, 9, 7, 12, 0, 0, 0, time.UTC)
	rows := []model.NodeContextHistory{
		historyRow("cccccccc-cccc-7ccc-8ccc-cccccccccc41", base, true, `{}`, `{"set":{},"unset":[]}`),
		historyRow("cccccccc-cccc-7ccc-8ccc-cccccccccc42", base.Add(time.Second), false, "null", `{"set":{"a":1},"unset":[]}`),
		historyRow("cccccccc-cccc-7ccc-8ccc-cccccccccc43", base.Add(2*time.Second), false, "null", `{"set":{"b":2},"unset":[]}`),
		historyRow("cccccccc-cccc-7ccc-8ccc-cccccccccc44", base.Add(3*time.Second), false, "null", `{"set":{"c":3},"unset":[]}`),
	}
	for _, row := range rows {
		if err := repo.AppendHistory(ctx, row); err != nil {
			t.Fatalf("AppendHistory() error = %v", err)
		}
	}

	_, err := repo.ReconstructBefore(ctx, historyInstanceID, model.HistoryCursor{
		CreatedAt: base.Add(4 * time.Second),
		ID:        "cccccccc-cccc-7ccc-8ccc-cccccccccc45",
	}, json.RawMessage(`{}`))
	if !errors.Is(err, repository.ErrHistoryTooDeep) {
		t.Errorf("ReconstructBefore() error = %v, want ErrHistoryTooDeep", err)
	}
}

func TestHistorySupersedeAfterMarksOnlyLaterRows(t *testing.T) {
	db := setupTestDB(t)
	seedInstanceFixture(t, db)
	insertInstance(t, db, newTestInstance(historyInstanceID, model.WorkflowWaiting, ""))
	repo := repository.NewInstanceRepository(db)
	ctx := context.Background()
	base := time.Date(2026, 9, 7, 12, 0, 0, 0, time.UTC)
	rows := []model.NodeContextHistory{
		historyRow("cccccccc-cccc-7ccc-8ccc-cccccccccc51", base, true, `{}`, `{"set":{},"unset":[]}`),
		historyRow("cccccccc-cccc-7ccc-8ccc-cccccccccc52", base.Add(time.Second), false, "null", `{"set":{},"unset":[]}`),
		historyRow("cccccccc-cccc-7ccc-8ccc-cccccccccc53", base.Add(2*time.Second), false, "null", `{"set":{},"unset":[]}`),
	}
	for _, row := range rows {
		if err := repo.AppendHistory(ctx, row); err != nil {
			t.Fatalf("AppendHistory() error = %v", err)
		}
	}

	if err := repo.SupersedeAfter(ctx, historyInstanceID, rows[1].Cursor()); err != nil {
		t.Fatalf("SupersedeAfter() error = %v", err)
	}
	got, err := repo.LoadHistory(ctx, historyInstanceID)
	if err != nil {
		t.Fatalf("LoadHistory() error = %v", err)
	}
	if got[0].Superseded || !got[1].Superseded || !got[2].Superseded {
		t.Errorf("superseded flags = %v, %v, %v; want false, true, true",
			got[0].Superseded, got[1].Superseded, got[2].Superseded)
	}
}

func TestCheckpointAppendsHistoryAtomically(t *testing.T) {
	db := setupTestDB(t)
	seedInstanceFixture(t, db)
	ctx := context.Background()
	instanceID := "aaaaaaaa-aaaa-7aaa-8aaa-aaaaaaaaaa61"
	insertInstance(t, db, newTestInstance(instanceID, model.WorkflowWaiting, ""))
	repo := repository.NewInstanceRepository(db)
	claimed, err := repo.ClaimNext(ctx, "worker-1", time.Minute, 1)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("ClaimNext() = %v, err %v", claimed, err)
	}

	history := historyRow(
		"cccccccc-cccc-7ccc-8ccc-cccccccccc61",
		time.Time{},
		true,
		`{"before":true}`,
		`{"set":{},"unset":[]}`,
	)
	history.WorkflowInstanceID = instanceID
	if err := repo.Checkpoint(ctx, repository.Checkpoint{
		InstanceID:    instanceID,
		WorkerID:      "worker-1",
		Revision:      claimed[0].Revision,
		Status:        model.WorkflowWaiting,
		WaitingReason: model.WaitingReasonRunnable,
		Frame:         model.NewFrame("next"),
		Counters:      model.Counters{},
		Context:       json.RawMessage(`{"after":true}`),
		History:       &history,
	}); err != nil {
		t.Fatalf("Checkpoint() error = %v", err)
	}

	rows, err := repo.LoadHistory(ctx, instanceID)
	if err != nil {
		t.Fatalf("LoadHistory() error = %v", err)
	}
	if len(rows) != 1 || !rows[0].IsAnchor || !jsonEqual(t, rows[0].Snapshot, json.RawMessage(`{"before":true}`)) {
		t.Fatalf("history = %+v, want one checkpoint anchor", rows)
	}
	stored, err := repo.GetByID(ctx, instanceID)
	if err != nil {
		t.Fatal(err)
	}
	if !jsonEqual(t, stored.Context, json.RawMessage(`{"after":true}`)) {
		t.Errorf("context = %s, want checkpoint context", stored.Context)
	}
}

func TestCheckpointHistoryFailureRollsBackInstanceUpdate(t *testing.T) {
	db := setupTestDB(t)
	seedInstanceFixture(t, db)
	ctx := context.Background()
	instanceID := "aaaaaaaa-aaaa-7aaa-8aaa-aaaaaaaaaa62"
	insertInstance(t, db, newTestInstance(instanceID, model.WorkflowWaiting, ""))
	repo := repository.NewInstanceRepository(db)
	claimed, err := repo.ClaimNext(ctx, "worker-1", time.Minute, 1)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("ClaimNext() = %v, err %v", claimed, err)
	}

	history := historyRow(
		"cccccccc-cccc-7ccc-8ccc-cccccccccc62",
		time.Time{},
		true,
		`{"before":true}`,
		`{"set":{},"unset":[]}`,
	)
	history.WorkflowInstanceID = "aaaaaaaa-aaaa-7aaa-8aaa-aaaaaaaaaa63"
	if err := repo.Checkpoint(ctx, repository.Checkpoint{
		InstanceID:    instanceID,
		WorkerID:      "worker-1",
		Revision:      claimed[0].Revision,
		Status:        model.WorkflowWaiting,
		WaitingReason: model.WaitingReasonRunnable,
		Frame:         model.NewFrame("next"),
		Counters:      model.Counters{},
		Context:       json.RawMessage(`{"after":true}`),
		History:       &history,
	}); err == nil {
		t.Fatal("Checkpoint() error = nil, want history append failure")
	}

	stored, err := repo.GetByID(ctx, instanceID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.Revision != claimed[0].Revision || stored.Status != model.WorkflowRunning ||
		string(stored.Context) != `{}` {
		t.Errorf("instance after rollback = %+v, want checkpoint update rolled back", stored)
	}
	rows, err := repo.LoadHistory(ctx, instanceID)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 0 {
		t.Errorf("history rows = %+v, want none after rollback", rows)
	}
}

func TestDeliverInputAppendsHistoryAtomically(t *testing.T) {
	db := setupTestDB(t)
	seedInstanceFixture(t, db)
	ctx := context.Background()
	instanceID := "aaaaaaaa-aaaa-7aaa-8aaa-aaaaaaaaaa64"
	nodeID := "bbbbbbbb-bbbb-7bbb-8bbb-bbbbbbbbbbb1"
	insertInstance(t, db, newTestInstance(instanceID, model.WorkflowWaiting, model.WaitingReasonInput))
	repo := repository.NewInstanceRepository(db)
	now := time.Now().UTC()
	if err := repo.InsertNodeInstance(ctx, model.NodeInstance{
		ID:                 "cccccccc-cccc-7ccc-8ccc-cccccccccc64",
		WorkflowInstanceID: instanceID,
		NodeID:             nodeID,
		Type:               string(model.NodeTypeInput),
		Status:             model.NodeRunning,
		CreatedAt:          now,
		UpdatedAt:          now,
	}); err != nil {
		t.Fatalf("InsertNodeInstance() error = %v", err)
	}
	history := historyRow(
		"dddddddd-dddd-7ddd-8ddd-dddddddddd64",
		time.Time{},
		false,
		"null",
		`{"set":{"answer":true},"unset":[]}`,
	)
	history.WorkflowInstanceID = instanceID
	history.OccurrenceID = "cccccccc-cccc-7ccc-8ccc-cccccccccc64"
	history.NodeID = nodeID
	if _, err := repo.DeliverInput(ctx, repository.InputCompletion{
		InstanceID:     instanceID,
		NodeInstanceID: history.OccurrenceID,
		IdempotencyKey: "input-64",
		Payload:        json.RawMessage(`{"answer":true}`),
		Accepted:       true,
		CreatedBy:      fixtureUserID,
		NewFrame:       model.NewFrame("next"),
		NewContext:     json.RawMessage(`{"answer":true}`),
		Status:         model.WorkflowWaiting,
		History:        &history,
	}); err != nil {
		t.Fatalf("DeliverInput() error = %v", err)
	}

	rows, err := repo.LoadHistory(ctx, instanceID)
	if err != nil {
		t.Fatalf("LoadHistory() error = %v", err)
	}
	if len(rows) != 1 || rows[0].IsAnchor || !jsonEqual(t, rows[0].Diff, json.RawMessage(`{"set":{"answer":true},"unset":[]}`)) {
		t.Fatalf("history = %+v, want one input diff", rows)
	}
}

func TestFailedInputAppendsHistoryAtomically(t *testing.T) {
	db := setupTestDB(t)
	seedInstanceFixture(t, db)
	ctx := context.Background()
	instanceID := "aaaaaaaa-aaaa-7aaa-8aaa-aaaaaaaaaa65"
	nodeID := "bbbbbbbb-bbbb-7bbb-8bbb-bbbbbbbbbbb2"
	insertInstance(t, db, newTestInstance(instanceID, model.WorkflowWaiting, model.WaitingReasonInput))
	repo := repository.NewInstanceRepository(db)
	now := time.Now().UTC()
	nodeInstanceID := "cccccccc-cccc-7ccc-8ccc-cccccccccc65"
	if err := repo.InsertNodeInstance(ctx, model.NodeInstance{
		ID:                 nodeInstanceID,
		WorkflowInstanceID: instanceID,
		NodeID:             nodeID,
		Type:               string(model.NodeTypeInput),
		Status:             model.NodeRunning,
		CreatedAt:          now,
		UpdatedAt:          now,
	}); err != nil {
		t.Fatalf("InsertNodeInstance() error = %v", err)
	}
	history := historyRow(
		"dddddddd-dddd-7ddd-8ddd-dddddddddd65",
		time.Time{},
		false,
		"null",
		`{"set":{"answer":false},"unset":[]}`,
	)
	history.WorkflowInstanceID = instanceID
	history.OccurrenceID = nodeInstanceID
	history.NodeID = nodeID
	if _, err := repo.DeliverInput(ctx, repository.InputCompletion{
		InstanceID:     instanceID,
		NodeInstanceID: nodeInstanceID,
		IdempotencyKey: "input-65",
		Payload:        json.RawMessage(`{"answer":false}`),
		Accepted:       true,
		CreatedBy:      fixtureUserID,
		PostFailure:    true,
		NodeStatus:     model.NodeFailed,
		Error:          "input hook failed",
		NewContext:     json.RawMessage(`{"answer":false}`),
		History:        &history,
	}); err != nil {
		t.Fatalf("DeliverInput() error = %v", err)
	}

	rows, err := repo.LoadHistory(ctx, instanceID)
	if err != nil {
		t.Fatalf("LoadHistory() error = %v", err)
	}
	if len(rows) != 1 || !jsonEqual(t, rows[0].Diff, json.RawMessage(`{"set":{"answer":false},"unset":[]}`)) {
		t.Fatalf("history = %+v, want one failed-input diff", rows)
	}
}
