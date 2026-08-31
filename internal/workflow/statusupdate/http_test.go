package statusupdate_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/simpwf/workflow-engine/internal/workflow/executor"
	"github.com/simpwf/workflow-engine/internal/workflow/model"
	"github.com/simpwf/workflow-engine/internal/workflow/repository"
	"github.com/simpwf/workflow-engine/internal/workflow/statusupdate"
)

func testPublisher(t *testing.T, allowlist []string) *statusupdate.HTTPPublisher {
	t.Helper()
	client := executor.NewHTTPExecutor(executor.Limits{
		HTTPAllowlist:  allowlist,
		MaxRedirects:   5,
		MaxOutputBytes: 1024 * 1024,
	})
	return statusupdate.NewHTTPPublisher(client, 5*time.Second)
}

func testEvent(id string) repository.PendingStatusUpdate {
	return repository.PendingStatusUpdate{
		ID:                   id,
		LogicalID:            id,
		WorkflowInstanceID:   "11111111-1111-7111-8111-111111111111",
		WorkflowDefinitionID: "22222222-2222-7222-8222-222222222222",
		Transport:            model.StatusUpdateTransportHTTP,
		Payload:              json.RawMessage(`{"event":"finished"}`),
	}
}

// httpCfg wraps an HTTP status_update block for the Publisher interface.
func httpCfg(url, method string, headers map[string]string) *model.StatusUpdateConfig {
	return &model.StatusUpdateConfig{HTTP: &model.HTTPStatusUpdateConfig{
		URL: url, Method: method, Headers: headers,
	}}
}

func TestHTTPPublisherSendsEvent(t *testing.T) {
	var gotMethod, gotAuth, gotID string
	var gotBody []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotAuth = r.Header.Get("Authorization")
		gotID = r.Header.Get("X-SimpWF-Event-ID")
		if r.Header.Get("Idempotency-Key") != gotID {
			t.Errorf("Idempotency-Key = %q, want event id %q", r.Header.Get("Idempotency-Key"), gotID)
		}
		if ct := r.Header.Get("Content-Type"); ct != "application/json" {
			t.Errorf("Content-Type = %q, want application/json", ct)
		}
		gotBody, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	ev := testEvent("event-123")
	p := testPublisher(t, []string{allowlistHost(srv.URL)})
	err := p.Publish(context.Background(), httpCfg(srv.URL+"/hooks", "PUT",
		map[string]string{"Authorization": "Bearer token"}), ev)
	if err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	if gotMethod != "PUT" {
		t.Errorf("method = %q, want PUT", gotMethod)
	}
	if gotAuth != "Bearer token" {
		t.Errorf("authorization = %q", gotAuth)
	}
	if gotID != "event-123" {
		t.Errorf("X-SimpWF-Event-ID = %q, want event-123", gotID)
	}
	if string(gotBody) != `{"event":"finished"}` {
		t.Errorf("body = %q", gotBody)
	}
}

func TestHTTPPublisherDefaultsToPOST(t *testing.T) {
	var gotMethod string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	ev := testEvent("event-123")
	p := testPublisher(t, []string{allowlistHost(srv.URL)})
	if err := p.Publish(context.Background(), httpCfg(srv.URL, "", nil), ev); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	if gotMethod != "POST" {
		t.Errorf("method = %q, want POST", gotMethod)
	}
}

func TestHTTPPublisherFailsOnNon2xx(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	ev := testEvent("event-123")
	p := testPublisher(t, []string{allowlistHost(srv.URL)})
	if err := p.Publish(context.Background(), httpCfg(srv.URL, "", nil), ev); err == nil {
		t.Error("Publish() error = nil, want failure on 500")
	}
}

func TestHTTPPublisherRejectsNonAllowlistedTarget(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	ev := testEvent("event-123")
	p := testPublisher(t, []string{"allowed.example.com"})
	if err := p.Publish(context.Background(), httpCfg(srv.URL, "", nil), ev); err == nil {
		t.Error("Publish() error = nil, want allowlist rejection")
	}
}

func TestHTTPPublisherFailsWithoutHTTPBlock(t *testing.T) {
	ev := testEvent("event-123")
	p := testPublisher(t, []string{"allowed.example.com"})
	// Only a redis block configured: the http publisher must fail.
	cfg := &model.StatusUpdateConfig{Redis: &model.RedisStatusUpdateConfig{}}
	if err := p.Publish(context.Background(), cfg, ev); err == nil {
		t.Error("Publish() error = nil, want missing-http-block failure")
	}
}

func TestHTTPPublisherValidatesRedirects(t *testing.T) {
	blocked := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer blocked.Close()

	redirector := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, blocked.URL, http.StatusFound)
	}))
	defer redirector.Close()

	ev := testEvent("event-123")
	// Only the redirector host:port is allowed; the redirect target is not.
	p := testPublisher(t, []string{allowlistHost(redirector.URL)})
	if err := p.Publish(context.Background(), httpCfg(redirector.URL, "", nil), ev); err == nil {
		t.Error("Publish() error = nil, want redirect rejection")
	}

	// With both hosts allowed the redirect succeeds.
	p = testPublisher(t, []string{allowlistHost(redirector.URL), allowlistHost(blocked.URL)})
	if err := p.Publish(context.Background(), httpCfg(redirector.URL, "", nil), ev); err != nil {
		t.Errorf("Publish() with allowed redirect error = %v", err)
	}
}

func TestHTTPPublisherSendsCustomHeadersOverridingDefaults(t *testing.T) {
	var gotCT string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotCT = r.Header.Get("Content-Type")
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	ev := testEvent("event-123")
	p := testPublisher(t, []string{allowlistHost(srv.URL)})
	err := p.Publish(context.Background(), httpCfg(srv.URL, "", map[string]string{
		"Content-Type": "application/vnd.simpwf+json",
	}), ev)
	if err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	if gotCT != "application/vnd.simpwf+json" {
		t.Errorf("Content-Type = %q, want custom override", gotCT)
	}
}

// allowlistHost extracts the host:port of a test server URL for the engine
// allowlist.
func allowlistHost(raw string) string {
	return strings.TrimPrefix(raw, "http://")
}
