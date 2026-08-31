package handler

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

type fakePinger struct{ err error }

func (f fakePinger) PingContext(context.Context) error { return f.err }

func TestHealthLive(t *testing.T) {
	h := &Health{db: fakePinger{err: errors.New("db down")}}
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodGet, "/health/live", nil)

	h.Live(c)

	if w.Code != http.StatusOK {
		t.Errorf("status = %d, want 200", w.Code)
	}
	var body map[string]string
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body["status"] != "ok" {
		t.Errorf("body = %v, want status ok", body)
	}
}

func TestHealthReady(t *testing.T) {
	// database reachable -> 200
	h := &Health{db: fakePinger{err: nil}}
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodGet, "/health/ready", nil)
	h.Ready(c)
	if w.Code != http.StatusOK {
		t.Errorf("ready (db ok) status = %d, want 200", w.Code)
	}

	// database unreachable -> 503
	h = &Health{db: fakePinger{err: errors.New("connection refused")}}
	w = httptest.NewRecorder()
	c, _ = gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodGet, "/health/ready", nil)
	h.Ready(c)
	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("ready (db down) status = %d, want 503", w.Code)
	}
}

func TestInstanceStatusResponseMarshals(t *testing.T) {
	now := time.Date(2026, 8, 5, 1, 0, 0, 0, time.UTC)
	resp := InstanceStatusResponse{
		ID:                   "11111111-1111-7111-8111-111111111111",
		WorkflowDefinitionID: "22222222-2222-7222-8222-222222222222",
		Status:               "running",
		PauseRequested:       true,
		Counters:             json.RawMessage(`{"finished":2}`),
		CreatedAt:            now,
		UpdatedAt:            now,
	}
	b, err := json.Marshal(resp)
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"id", "workflow_definition_id", "status", "waiting_reason", "pause_requested", "termination_pending", "current_node_instance_id", "counters", "error", "created_by", "updated_by", "created_at"} {
		if _, ok := m[key]; !ok {
			t.Errorf("missing key %q in %v", key, m)
		}
	}
}

func TestNodeDebugResponseMarshals(t *testing.T) {
	resp := NodeDebugResponse{
		OccurrenceID:           "11111111-1111-7111-8111-111111111111",
		SourceNodeDefinitionID: "22222222-2222-7222-8222-222222222222",
		Name:                   "transform",
		Type:                   "script",
		AttemptCount:           2,
		Status:                 "finished",
		ContextBefore:          json.RawMessage(`{"a":1}`),
		ContextAfter:           json.RawMessage(`{"a":2}`),
	}
	b, err := json.Marshal(resp)
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"occurrence_id", "source_node_definition_id", "selected_attempt", "attempt_count", "context_before", "context_after", "duration_ms", "recovery_policy", "cancelled"} {
		if _, ok := m[key]; !ok {
			t.Errorf("missing key %q in %v", key, m)
		}
	}
}

func TestListResponseMarshals(t *testing.T) {
	resp := ListResponse[string]{Items: []string{"a"}, Page: 1, PerPage: 50, Total: 1, TotalPages: 1}
	b, err := json.Marshal(resp)
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"items", "page", "per_page", "total", "total_pages"} {
		if _, ok := m[key]; !ok {
			t.Errorf("missing key %q in %v", key, m)
		}
	}
}
