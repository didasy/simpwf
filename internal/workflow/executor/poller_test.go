package executor_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/simpwf/workflow-engine/internal/workflow/executor"
	"github.com/simpwf/workflow-engine/internal/workflow/model"
	"github.com/simpwf/workflow-engine/internal/workflow/transport"
)

func TestNormalizePollerBody(t *testing.T) {
	cases := []struct {
		name string
		raw  string
		want string // fmt.Sprintf("%v") of the expected value
	}{
		{"object", `{"status":"completed"}`, "map[status:completed]"},
		{"array", `[1,2,3]`, "[1 2 3]"},
		{"string", `"hello"`, "hello"},
		{"number", `42`, "42"},
		{"bool", `true`, "true"},
		{"null", `null`, "<nil>"},
		{"invalid", `not-json`, "not-json"},
		{"empty", ``, ""},
	}
	for _, c := range cases {
		got := executor.NormalizePollerBody([]byte(c.raw))
		if fmt.Sprintf("%v", got) != c.want {
			t.Errorf("%s: NormalizePollerBody(%q) = %v (%T), want %s", c.name, c.raw, got, got, c.want)
		}
	}
}

func TestPollerResponseHTTP(t *testing.T) {
	resp := executor.NewHTTPPollerResponse(200, map[string][]string{"X-A": {"1", "2"}}, map[string]any{"status": "completed"})
	raw, err := json.Marshal(resp)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var m map[string]json.RawMessage
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatalf("unmarshal %s: %v", raw, err)
	}
	for _, key := range []string{"body", "headers", "status"} {
		if _, ok := m[key]; !ok {
			t.Errorf("HTTP response missing key %q in %s", key, raw)
		}
	}
	var headers map[string][]string
	if err := json.Unmarshal(m["headers"], &headers); err != nil {
		t.Fatalf("headers not map[string][]string: %v", err)
	}
	if len(headers["X-A"]) != 2 || headers["X-A"][0] != "1" || headers["X-A"][1] != "2" {
		t.Errorf("headers = %v, want X-A: [1 2]", headers)
	}
	var status int
	if err := json.Unmarshal(m["status"], &status); err != nil || status != 200 {
		t.Errorf("status = %v (err %v), want 200", status, err)
	}
	var body map[string]any
	if err := json.Unmarshal(m["body"], &body); err != nil || body["status"] != "completed" {
		t.Errorf("body = %v (err %v)", body, err)
	}
}

func TestPollerResponseRedis(t *testing.T) {
	resp := executor.NewRedisPollerResponse(map[string]any{"status": "completed"})
	raw, err := json.Marshal(resp)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var m map[string]json.RawMessage
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatalf("unmarshal %s: %v", raw, err)
	}
	if len(m) != 1 {
		t.Errorf("redis response keys = %v, want only body", m)
	}
	if _, ok := m["body"]; !ok {
		t.Errorf("redis response missing body in %s", raw)
	}

	// A missing key is a normal response with body null, not an omitted field.
	respNil := executor.NewRedisPollerResponse(nil)
	rawNil, err := json.Marshal(respNil)
	if err != nil {
		t.Fatalf("marshal nil body: %v", err)
	}
	var mNil map[string]json.RawMessage
	if err := json.Unmarshal(rawNil, &mNil); err != nil {
		t.Fatalf("unmarshal %s: %v", rawNil, err)
	}
	if string(mNil["body"]) != "null" {
		t.Errorf("nil body = %s, want null", mNil["body"])
	}
}

func TestPollerResponseRabbit(t *testing.T) {
	resp := executor.NewRabbitPollerResponse(map[string]string{"k": "v", "n": "42"}, "payload")
	raw, err := json.Marshal(resp)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var m map[string]json.RawMessage
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatalf("unmarshal %s: %v", raw, err)
	}
	if _, ok := m["body"]; !ok {
		t.Errorf("rabbit response missing body in %s", raw)
	}
	var headers map[string]string
	if err := json.Unmarshal(m["headers"], &headers); err != nil {
		t.Fatalf("headers not map[string]string: %v (raw %s)", err, m["headers"])
	}
	if headers["k"] != "v" || headers["n"] != "42" {
		t.Errorf("headers = %v, want k:v n:42", headers)
	}
}

func TestPollerPredicate(t *testing.T) {
	ev := executor.NewPollerPredicateEvaluator(nil)
	wfCtx := map[string]any{"job": map[string]any{"id": "j1"}}
	resp := executor.NewHTTPPollerResponse(200, nil, map[string]any{"status": "completed"})
	eval := func(src string, timeout time.Duration) (bool, error) {
		return ev.Evaluate(context.Background(), src, timeout, wfCtx, resp)
	}

	if ok, err := eval("return response.body.status === 'completed';", time.Second); err != nil || !ok {
		t.Errorf("true predicate: ok = %v, err = %v", ok, err)
	}
	if ok, err := eval("return response.body.status === 'running';", time.Second); err != nil || ok {
		t.Errorf("false predicate: ok = %v, err = %v", ok, err)
	}
	if ok, err := eval("return response.status === 200 && response.body.status === 'completed';", time.Second); err != nil || !ok {
		t.Errorf("response shape predicate: ok = %v, err = %v", ok, err)
	}
	// Mutating the frozen response must not persist.
	if ok, err := eval("response.body.status = 'hacked'; return response.body.status === 'completed';", time.Second); err != nil || !ok {
		t.Errorf("frozen response mutation: ok = %v, err = %v", ok, err)
	}
	// The frozen workflow context stays readable.
	if ok, err := eval("return context.job.id === 'j1';", time.Second); err != nil || !ok {
		t.Errorf("frozen context access: ok = %v, err = %v", ok, err)
	}
	if _, err := eval("return 1;", time.Second); err == nil {
		t.Error("non-boolean predicate error = nil, want error")
	}
	if _, err := eval("return undefinedVar.x;", time.Second); err == nil {
		t.Error("script failure error = nil, want error")
	}
	if _, err := eval("while (true) {}", 50*time.Millisecond); err == nil {
		t.Error("timeout error = nil, want error")
	}
}

func newPollerExecutor(t *testing.T) *executor.PollerExecutor {
	t.Helper()
	httpEx := executor.NewHTTPExecutor(executor.Limits{
		HTTPAllowlist:  []string{"127.0.0.1"},
		MaxOutputBytes: 1 << 20,
		MaxRedirects:   5,
	})
	return executor.NewPollerExecutor(httpEx, nil)
}

func pollerHTTPNode(cfg *model.PollerHTTPConfig) *model.NodeContent {
	return &model.NodeContent{Type: model.NodeTypePoller, PollerHTTP: cfg, PredicateTimeout: time.Second}
}

func TestHTTPPollerFirstCallImmediate(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("X-First", "yes")
		w.WriteHeader(201)
		_, _ = io.WriteString(w, `{"status":"completed"}`)
	}))
	defer srv.Close()

	cfg := &model.PollerHTTPConfig{
		URL: srv.URL, Method: "GET", Delay: time.Hour, RequestTimeout: 2 * time.Second, MaxAttempts: 1,
		Until: "return response.status === 201 && response.body.status === 'completed';",
	}
	ex := newPollerExecutor(t)
	start := time.Now()
	res, err := ex.Execute(context.Background(), executor.Request{
		Node:    pollerHTTPNode(cfg),
		Context: map[string]any{},
	})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Errorf("first request delayed by %v, want immediate", elapsed)
	}
	if calls.Load() != 1 {
		t.Errorf("calls = %d, want 1", calls.Load())
	}
	out, ok := res.Output.(*executor.PollerResponse)
	if !ok {
		t.Fatalf("output = %T, want *PollerResponse", res.Output)
	}
	if out.Status != 201 {
		t.Errorf("status = %d, want 201", out.Status)
	}
	body, _ := out.Body.(map[string]any)
	if body["status"] != "completed" {
		t.Errorf("body = %v, want status completed", out.Body)
	}
	hdr, _ := out.Headers.(map[string][]string)
	if len(hdr["X-First"]) == 0 || hdr["X-First"][0] != "yes" {
		t.Errorf("headers = %v, want X-First: yes", out.Headers)
	}
}

func TestHTTPPollerFalseThenTrue(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			_, _ = io.WriteString(w, `{"status":"running"}`)
			return
		}
		_, _ = io.WriteString(w, `{"status":"completed"}`)
	}))
	defer srv.Close()

	cfg := &model.PollerHTTPConfig{
		URL: srv.URL, Method: "GET", Delay: 5 * time.Millisecond, RequestTimeout: time.Second, MaxAttempts: 5,
		Until: "return response.body.status === 'completed';",
	}
	ex := newPollerExecutor(t)
	res, err := ex.Execute(context.Background(), executor.Request{Node: pollerHTTPNode(cfg), Context: map[string]any{}})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if calls.Load() != 2 {
		t.Errorf("calls = %d, want 2", calls.Load())
	}
	out := res.Output.(*executor.PollerResponse)
	body, _ := out.Body.(map[string]any)
	if body["status"] != "completed" {
		t.Errorf("body = %v, want completed", out.Body)
	}
}

func TestHTTPPollerTemplates(t *testing.T) {
	var gotMethod, gotPath, gotAuth, gotBody string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotPath = r.URL.Path
		gotAuth = r.Header.Get("Authorization")
		b, _ := io.ReadAll(r.Body)
		gotBody = string(b)
		_, _ = io.WriteString(w, `{"ok":true}`)
	}))
	defer srv.Close()

	cfg := &model.PollerHTTPConfig{
		URL:            srv.URL + "/jobs/{{ job.id }}",
		Method:         "{{ job.method }}",
		Headers:        map[string]string{"Authorization": "Bearer {{ auth.token }}", "X-Static": "yes"},
		Body:           json.RawMessage(`{"job_id":"{{ job.id }}"}`),
		Delay:          time.Millisecond,
		RequestTimeout: time.Second,
		MaxAttempts:    1,
		Until:          "return response.body.ok === true;",
	}
	ex := newPollerExecutor(t)
	_, err := ex.Execute(context.Background(), executor.Request{
		Node: pollerHTTPNode(cfg),
		Context: map[string]any{
			"job":  map[string]any{"id": "j1", "method": "post"},
			"auth": map[string]any{"token": "t"},
		},
	})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if gotMethod != "POST" {
		t.Errorf("method = %q, want POST", gotMethod)
	}
	if gotPath != "/jobs/j1" {
		t.Errorf("path = %q, want /jobs/j1", gotPath)
	}
	if gotAuth != "Bearer t" {
		t.Errorf("authorization = %q, want Bearer t", gotAuth)
	}
	if gotBody != `{"job_id":"j1"}` {
		t.Errorf("body = %q, want rendered JSON", gotBody)
	}
}

func TestHTTPPollerIdempotencyKeyStable(t *testing.T) {
	var keys []string
	var mu sync.Mutex
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		keys = append(keys, r.Header.Get("Idempotency-Key"))
		mu.Unlock()
		_, _ = io.WriteString(w, `{"status":"running"}`)
	}))
	defer srv.Close()

	cfg := &model.PollerHTTPConfig{
		URL: srv.URL, Method: "GET", Delay: time.Millisecond, RequestTimeout: time.Second, MaxAttempts: 3,
		Until: "return response.body.status === 'completed';",
	}
	ex := newPollerExecutor(t)
	_, err := ex.Execute(context.Background(), executor.Request{
		Node:           pollerHTTPNode(cfg),
		Context:        map[string]any{},
		IdempotencyKey: "inst:node:1",
	})
	if err == nil {
		t.Fatal("Execute() error = nil, want exhaustion")
	}
	mu.Lock()
	defer mu.Unlock()
	if len(keys) != 3 {
		t.Fatalf("keys len = %d, want 3", len(keys))
	}
	for i, key := range keys {
		if key != "inst:node:1" {
			t.Errorf("attempt %d Idempotency-Key = %q, want stable", i, key)
		}
	}
}

func TestHTTPPollerNon2xxEvaluatedNormally(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(404)
		_, _ = io.WriteString(w, `{"error":"nope"}`)
	}))
	defer srv.Close()

	cfg := &model.PollerHTTPConfig{
		URL: srv.URL, Method: "GET", RequestTimeout: time.Second, MaxAttempts: 1,
		Until: "return response.status === 404 && response.body.error === 'nope';",
	}
	ex := newPollerExecutor(t)
	res, err := ex.Execute(context.Background(), executor.Request{Node: pollerHTTPNode(cfg), Context: map[string]any{}})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if out := res.Output.(*executor.PollerResponse); out.Status != 404 {
		t.Errorf("status = %d, want 404", out.Status)
	}
}

func TestHTTPPollerTransportErrorsRetry(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			hj, ok := w.(http.Hijacker)
			if !ok {
				t.Error("handler is not a hijacker")
				return
			}
			conn, _, err := hj.Hijack()
			if err == nil {
				_ = conn.Close()
			}
			return
		}
		_, _ = io.WriteString(w, `{"ok":true}`)
	}))
	defer srv.Close()

	cfg := &model.PollerHTTPConfig{
		URL: srv.URL, Method: "GET", Delay: 5 * time.Millisecond, RequestTimeout: time.Second, MaxAttempts: 3,
		Until: "return response.body.ok === true;",
	}
	ex := newPollerExecutor(t)
	res, err := ex.Execute(context.Background(), executor.Request{Node: pollerHTTPNode(cfg), Context: map[string]any{}})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if calls.Load() != 2 {
		t.Errorf("calls = %d, want 2 (first transport error must retry)", calls.Load())
	}
	body, _ := res.Output.(*executor.PollerResponse).Body.(map[string]any)
	if body["ok"] != true {
		t.Errorf("body = %v, want ok true", res.Output.(*executor.PollerResponse).Body)
	}
}

func TestHTTPPollerExhaustsAttempts(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		_, _ = io.WriteString(w, `{"status":"running"}`)
	}))
	defer srv.Close()

	cfg := &model.PollerHTTPConfig{
		URL: srv.URL, Method: "GET", Delay: time.Millisecond, RequestTimeout: time.Second, MaxAttempts: 3,
		Until: "return response.body.status === 'completed';",
	}
	ex := newPollerExecutor(t)
	_, err := ex.Execute(context.Background(), executor.Request{Node: pollerHTTPNode(cfg), Context: map[string]any{}})
	if err == nil {
		t.Fatal("Execute() error = nil, want exhaustion error")
	}
	if !strings.Contains(err.Error(), "exhausted") {
		t.Errorf("error = %q, want mention of exhausted", err)
	}
	if calls.Load() != 3 {
		t.Errorf("calls = %d, want exactly max_attempts 3", calls.Load())
	}
}

func TestHTTPPollerCancellationInterruptsRequest(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-time.After(5 * time.Second):
		}
		_, _ = io.WriteString(w, `{}`)
	}))
	defer srv.Close()

	cfg := &model.PollerHTTPConfig{
		URL: srv.URL, Method: "GET", RequestTimeout: 30 * time.Second, MaxAttempts: 3,
		Until: "return response.status === 200;",
	}
	ex := newPollerExecutor(t)
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()
	start := time.Now()
	_, err := ex.Execute(ctx, executor.Request{Node: pollerHTTPNode(cfg), Context: map[string]any{}})
	if err == nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("Execute() error = %v, want context.Canceled", err)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Errorf("cancellation took %v, want fast", elapsed)
	}
}

func TestHTTPPollerCancellationInterruptsDelay(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, `{"status":"running"}`)
	}))
	defer srv.Close()

	cfg := &model.PollerHTTPConfig{
		URL: srv.URL, Method: "GET", Delay: time.Hour, RequestTimeout: time.Second, MaxAttempts: 5,
		Until: "return response.body.status === 'completed';",
	}
	ex := newPollerExecutor(t)
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()
	start := time.Now()
	_, err := ex.Execute(ctx, executor.Request{Node: pollerHTTPNode(cfg), Context: map[string]any{}})
	if err == nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("Execute() error = %v, want context.Canceled", err)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Errorf("cancellation took %v, want fast", elapsed)
	}
}

// fakeRedisPoller implements executor.RedisPollerClient for redis poller
// tests. Get reads values with optional miss-first / error queues; Subscribe
// delivers queued messages synchronously then blocks until ctx is done.
type fakeRedisPoller struct {
	mu         sync.Mutex
	values     map[string]string
	missFirst  int
	getErrs    []error
	gets       int
	lastKey    string
	blockGet   chan struct{}
	msgs       []string
	subErr     error
	subChannel string
}

func (f *fakeRedisPoller) Get(ctx context.Context, key string) ([]byte, bool, error) {
	f.mu.Lock()
	f.gets++
	f.lastKey = key
	var err error
	if len(f.getErrs) > 0 {
		err = f.getErrs[0]
		f.getErrs = f.getErrs[1:]
	}
	miss := f.missFirst > 0
	if miss {
		f.missFirst--
	}
	val, ok := f.values[key]
	block := f.blockGet
	f.mu.Unlock()
	if block != nil {
		select {
		case <-ctx.Done():
			return nil, false, ctx.Err()
		case <-block:
		}
	}
	if err != nil {
		return nil, false, err
	}
	if !ok || miss {
		return nil, false, nil
	}
	return []byte(val), true, nil
}

func (f *fakeRedisPoller) Subscribe(ctx context.Context, channel string, handler func(ctx context.Context, channel string, payload []byte) error) error {
	f.mu.Lock()
	f.subChannel = channel
	msgs := f.msgs
	subErr := f.subErr
	f.mu.Unlock()
	if subErr != nil {
		return subErr
	}
	for _, m := range msgs {
		if err := handler(ctx, channel, []byte(m)); err != nil {
			return err
		}
	}
	<-ctx.Done()
	return nil
}

func (f *fakeRedisPoller) getCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.gets
}

func pollerRedisGETNode(cfg *model.PollerRedisConfig) *model.NodeContent {
	return &model.NodeContent{Type: model.NodeTypePoller, PollerRedis: cfg, PredicateTimeout: time.Second}
}

func TestRedisPollerGETImmediateFirstCall(t *testing.T) {
	fake := &fakeRedisPoller{values: map[string]string{"k": `{"status":"completed"}`}}
	ex := newPollerExecutor(t)
	ex.SetRedisClient(fake)
	cfg := &model.PollerRedisConfig{
		Method: "GET", Key: "k", Delay: time.Hour, RequestTimeout: time.Second, MaxAttempts: 1,
		Until: "return response.body.status === 'completed';",
	}
	start := time.Now()
	res, err := ex.Execute(context.Background(), executor.Request{Node: pollerRedisGETNode(cfg), Context: map[string]any{}})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if time.Since(start) > time.Second {
		t.Error("first GET delayed by retry delay")
	}
	if fake.getCount() != 1 {
		t.Errorf("gets = %d, want 1", fake.getCount())
	}
	body, _ := res.Output.(*executor.PollerResponse).Body.(map[string]any)
	if body["status"] != "completed" {
		t.Errorf("body = %v, want completed", res.Output.(*executor.PollerResponse).Body)
	}
}

func TestRedisPollerGETFalseThenTrue(t *testing.T) {
	fake := &fakeRedisPoller{missFirst: 1, values: map[string]string{"k": `{"status":"completed"}`}}
	ex := newPollerExecutor(t)
	ex.SetRedisClient(fake)
	cfg := &model.PollerRedisConfig{
		Method: "GET", Key: "k", Delay: 5 * time.Millisecond, RequestTimeout: time.Second, MaxAttempts: 5,
		Until: "return response.body !== null && response.body.status === 'completed';",
	}
	res, err := ex.Execute(context.Background(), executor.Request{Node: pollerRedisGETNode(cfg), Context: map[string]any{}})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if fake.getCount() != 2 {
		t.Errorf("gets = %d, want 2", fake.getCount())
	}
	body, _ := res.Output.(*executor.PollerResponse).Body.(map[string]any)
	if body["status"] != "completed" {
		t.Errorf("body = %v, want completed", res.Output.(*executor.PollerResponse).Body)
	}
}

func TestRedisPollerGETMissingKeyBodyNull(t *testing.T) {
	fake := &fakeRedisPoller{values: map[string]string{}}
	ex := newPollerExecutor(t)
	ex.SetRedisClient(fake)
	cfg := &model.PollerRedisConfig{
		Method: "GET", Key: "absent", RequestTimeout: time.Second, MaxAttempts: 1,
		Until: "return response.body === null;",
	}
	res, err := ex.Execute(context.Background(), executor.Request{Node: pollerRedisGETNode(cfg), Context: map[string]any{}})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if out := res.Output.(*executor.PollerResponse); out.Body != nil {
		t.Errorf("body = %v, want null for missing key", out.Body)
	}
}

func TestRedisPollerGETTemplatedKey(t *testing.T) {
	fake := &fakeRedisPoller{values: map[string]string{"jobs:j1": `{"status":"completed"}`}}
	ex := newPollerExecutor(t)
	ex.SetRedisClient(fake)
	cfg := &model.PollerRedisConfig{
		Method: "GET", Key: "jobs:{{ job.id }}", RequestTimeout: time.Second, MaxAttempts: 1,
		Until: "return response.body.status === 'completed';",
	}
	_, err := ex.Execute(context.Background(), executor.Request{
		Node:    pollerRedisGETNode(cfg),
		Context: map[string]any{"job": map[string]any{"id": "j1"}},
	})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if fake.lastKey != "jobs:j1" {
		t.Errorf("key = %q, want rendered jobs:j1", fake.lastKey)
	}
}

func TestRedisPollerGETTransportErrorsRetry(t *testing.T) {
	fake := &fakeRedisPoller{getErrs: []error{errors.New("connection reset")}, values: map[string]string{"k": `{"ok":true}`}}
	ex := newPollerExecutor(t)
	ex.SetRedisClient(fake)
	cfg := &model.PollerRedisConfig{
		Method: "GET", Key: "k", Delay: 5 * time.Millisecond, RequestTimeout: time.Second, MaxAttempts: 3,
		Until: "return response.body.ok === true;",
	}
	res, err := ex.Execute(context.Background(), executor.Request{Node: pollerRedisGETNode(cfg), Context: map[string]any{}})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if fake.getCount() != 2 {
		t.Errorf("gets = %d, want 2 (error must consume an attempt)", fake.getCount())
	}
	body, _ := res.Output.(*executor.PollerResponse).Body.(map[string]any)
	if body["ok"] != true {
		t.Errorf("body = %v, want ok true", res.Output.(*executor.PollerResponse).Body)
	}
}

func TestRedisPollerGETPerCallTimeout(t *testing.T) {
	fake := &fakeRedisPoller{blockGet: make(chan struct{}), values: map[string]string{"k": "x"}}
	ex := newPollerExecutor(t)
	ex.SetRedisClient(fake)
	cfg := &model.PollerRedisConfig{
		Method: "GET", Key: "k", Delay: time.Millisecond, RequestTimeout: 30 * time.Millisecond, MaxAttempts: 2,
		Until: "return true;",
	}
	_, err := ex.Execute(context.Background(), executor.Request{Node: pollerRedisGETNode(cfg), Context: map[string]any{}})
	if err == nil {
		t.Fatal("Execute() error = nil, want exhaustion after per-call timeouts")
	}
	if fake.getCount() != 2 {
		t.Errorf("gets = %d, want 2", fake.getCount())
	}
}

func TestRedisPollerGETExhaustsAttempts(t *testing.T) {
	fake := &fakeRedisPoller{values: map[string]string{"k": `{"status":"running"}`}}
	ex := newPollerExecutor(t)
	ex.SetRedisClient(fake)
	cfg := &model.PollerRedisConfig{
		Method: "GET", Key: "k", Delay: time.Millisecond, RequestTimeout: time.Second, MaxAttempts: 3,
		Until: "return response.body.status === 'completed';",
	}
	_, err := ex.Execute(context.Background(), executor.Request{Node: pollerRedisGETNode(cfg), Context: map[string]any{}})
	if err == nil || !strings.Contains(err.Error(), "exhausted") {
		t.Fatalf("Execute() error = %v, want exhaustion", err)
	}
	if fake.getCount() != 3 {
		t.Errorf("gets = %d, want exactly 3", fake.getCount())
	}
}

func TestRedisPollerGETCancellationInterruptsDelay(t *testing.T) {
	fake := &fakeRedisPoller{values: map[string]string{"k": `{"status":"running"}`}}
	ex := newPollerExecutor(t)
	ex.SetRedisClient(fake)
	cfg := &model.PollerRedisConfig{
		Method: "GET", Key: "k", Delay: time.Hour, RequestTimeout: time.Second, MaxAttempts: 5,
		Until: "return response.body.status === 'completed';",
	}
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()
	start := time.Now()
	_, err := ex.Execute(ctx, executor.Request{Node: pollerRedisGETNode(cfg), Context: map[string]any{}})
	if err == nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("Execute() error = %v, want context.Canceled", err)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Errorf("cancellation took %v, want fast", elapsed)
	}
	if fake.getCount() != 1 {
		t.Errorf("gets = %d, want 1", fake.getCount())
	}
}

func TestRedisPollerGETMissingTransport(t *testing.T) {
	ex := newPollerExecutor(t)
	cfg := &model.PollerRedisConfig{
		Method: "GET", Key: "k", RequestTimeout: time.Second, MaxAttempts: 1,
		Until: "return true;",
	}
	_, err := ex.Execute(context.Background(), executor.Request{Node: pollerRedisGETNode(cfg), Context: map[string]any{}})
	if err == nil || !strings.Contains(err.Error(), "redis") {
		t.Fatalf("Execute() error = %v, want missing redis transport", err)
	}
}

func TestRedisPollerSUBDiscardsNonmatches(t *testing.T) {
	fake := &fakeRedisPoller{msgs: []string{`{"status":"running"}`, `{"status":"completed"}`}}
	ex := newPollerExecutor(t)
	ex.SetRedisClient(fake)
	cfg := &model.PollerRedisConfig{
		Method: "SUB", Channel: "jobs:{{ job.id }}", MaxWaitTime: time.Second,
		Until: "return response.body.status === 'completed';",
	}
	res, err := ex.Execute(context.Background(), executor.Request{
		Node:    pollerRedisGETNode(cfg),
		Context: map[string]any{"job": map[string]any{"id": "j1"}},
	})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	body, _ := res.Output.(*executor.PollerResponse).Body.(map[string]any)
	if body["status"] != "completed" {
		t.Errorf("body = %v, want completed", res.Output.(*executor.PollerResponse).Body)
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if fake.subChannel != "jobs:j1" {
		t.Errorf("channel = %q, want rendered jobs:j1", fake.subChannel)
	}
}

func TestRedisPollerSUBTimeout(t *testing.T) {
	fake := &fakeRedisPoller{}
	ex := newPollerExecutor(t)
	ex.SetRedisClient(fake)
	cfg := &model.PollerRedisConfig{
		Method: "SUB", Channel: "c", MaxWaitTime: 50 * time.Millisecond,
		Until: "return true;",
	}
	_, err := ex.Execute(context.Background(), executor.Request{Node: pollerRedisGETNode(cfg), Context: map[string]any{}})
	if err == nil || !strings.Contains(err.Error(), "without a match") {
		t.Fatalf("Execute() error = %v, want wait timeout", err)
	}
}

func TestRedisPollerSUBPredicateError(t *testing.T) {
	fake := &fakeRedisPoller{msgs: []string{`{"status":"completed"}`}}
	ex := newPollerExecutor(t)
	ex.SetRedisClient(fake)
	cfg := &model.PollerRedisConfig{
		Method: "SUB", Channel: "c", MaxWaitTime: time.Second,
		Until: "return undefinedVar.x;",
	}
	_, err := ex.Execute(context.Background(), executor.Request{Node: pollerRedisGETNode(cfg), Context: map[string]any{}})
	if err == nil {
		t.Fatal("Execute() error = nil, want predicate failure")
	}
}

func TestRedisPollerSUBMissingTransport(t *testing.T) {
	ex := newPollerExecutor(t)
	cfg := &model.PollerRedisConfig{
		Method: "SUB", Channel: "c", MaxWaitTime: time.Second,
		Until: "return true;",
	}
	_, err := ex.Execute(context.Background(), executor.Request{Node: pollerRedisGETNode(cfg), Context: map[string]any{}})
	if err == nil || !strings.Contains(err.Error(), "redis") {
		t.Fatalf("Execute() error = %v, want missing redis transport", err)
	}
}

// fakeRabbitPoller implements executor.RabbitPollerClient for rabbitmq
// poller tests. It records the rendered queue, consumer tag, and every
// settlement, then blocks until ctx is done.
type fakeRabbitPoller struct {
	mu         sync.Mutex
	queue      string
	tag        string
	msgs       []transport.RabbitPollerMessage
	settles    []transport.RabbitPollerSettlement
	consumeErr error
}

func (f *fakeRabbitPoller) ConsumeQueue(ctx context.Context, queue, consumerTag string, handler func(msg transport.RabbitPollerMessage) (transport.RabbitPollerSettlement, error)) error {
	f.mu.Lock()
	f.queue = queue
	f.tag = consumerTag
	msgs := f.msgs
	consumeErr := f.consumeErr
	f.mu.Unlock()
	if consumeErr != nil {
		return consumeErr
	}
	for _, m := range msgs {
		settle, err := handler(m)
		f.mu.Lock()
		f.settles = append(f.settles, settle)
		f.mu.Unlock()
		if err != nil {
			return err
		}
	}
	<-ctx.Done()
	return nil
}

func (f *fakeRabbitPoller) settlementCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.settles)
}

func pollerRabbitNode(cfg *model.PollerRabbitMQConfig) *model.NodeContent {
	return &model.NodeContent{Type: model.NodeTypePoller, PollerRabbitMQ: cfg, PredicateTimeout: time.Second}
}

func TestRabbitPollerQueueRendering(t *testing.T) {
	fake := &fakeRabbitPoller{}
	ex := newPollerExecutor(t)
	ex.SetRabbitClient(fake)
	cfg := &model.PollerRabbitMQConfig{
		Queue: "jobs.{{ workflow_instance_id }}", MaxWaitTime: 50 * time.Millisecond, Until: "return true;",
	}
	_, err := ex.Execute(context.Background(), executor.Request{
		Node:       pollerRabbitNode(cfg),
		Context:    map[string]any{"workflow_instance_id": "user-value"},
		InstanceID: "w1",
	})
	if err == nil || !strings.Contains(err.Error(), "without a match") {
		t.Fatalf("Execute() error = %v, want wait timeout", err)
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if fake.queue != "jobs.w1" {
		t.Errorf("queue = %q, want rendered jobs.w1 (reserved id wins)", fake.queue)
	}
	if fake.tag == "" {
		t.Error("consumer tag is empty, want unique tag")
	}
}

func TestRabbitPollerFalseAcksAndContinues(t *testing.T) {
	fake := &fakeRabbitPoller{msgs: []transport.RabbitPollerMessage{{Body: []byte(`{"status":"running"}`)}}}
	ex := newPollerExecutor(t)
	ex.SetRabbitClient(fake)
	cfg := &model.PollerRabbitMQConfig{
		Queue: "jobs", MaxWaitTime: 50 * time.Millisecond, Until: "return response.body.status === 'completed';",
	}
	_, err := ex.Execute(context.Background(), executor.Request{Node: pollerRabbitNode(cfg), Context: map[string]any{}})
	if err == nil || !strings.Contains(err.Error(), "without a match") {
		t.Fatalf("Execute() error = %v, want wait timeout after false message", err)
	}
	if fake.settlementCount() != 1 {
		t.Fatalf("settlements = %d, want 1", fake.settlementCount())
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if fake.settles[0] != transport.RabbitPollerAck {
		t.Errorf("settlement = %v, want Ack (false messages are discarded)", fake.settles[0])
	}
}

func TestRabbitPollerTrueAcksAndReturns(t *testing.T) {
	fake := &fakeRabbitPoller{msgs: []transport.RabbitPollerMessage{
		{Body: []byte(`{"status":"running"}`)},
		{Body: []byte(`{"status":"completed"}`), Headers: map[string]string{"X-Job": "j1"}},
	}}
	ex := newPollerExecutor(t)
	ex.SetRabbitClient(fake)
	cfg := &model.PollerRabbitMQConfig{
		Queue: "jobs", MaxWaitTime: time.Second, Until: "return response.body.status === 'completed' && response.headers['X-Job'] === 'j1';",
	}
	res, err := ex.Execute(context.Background(), executor.Request{Node: pollerRabbitNode(cfg), Context: map[string]any{}})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	body, _ := res.Output.(*executor.PollerResponse).Body.(map[string]any)
	if body["status"] != "completed" {
		t.Errorf("body = %v, want completed", res.Output.(*executor.PollerResponse).Body)
	}
	if fake.settlementCount() != 2 {
		t.Fatalf("settlements = %d, want 2", fake.settlementCount())
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	for i, s := range fake.settles {
		if s != transport.RabbitPollerAck {
			t.Errorf("settlement %d = %v, want Ack", i, s)
		}
	}
}

func TestRabbitPollerPredicateErrorAcksThenFails(t *testing.T) {
	fake := &fakeRabbitPoller{msgs: []transport.RabbitPollerMessage{{Body: []byte(`{"status":"completed"}`)}}}
	ex := newPollerExecutor(t)
	ex.SetRabbitClient(fake)
	cfg := &model.PollerRabbitMQConfig{
		Queue: "jobs", MaxWaitTime: time.Second, Until: "return undefinedVar.x;",
	}
	_, err := ex.Execute(context.Background(), executor.Request{Node: pollerRabbitNode(cfg), Context: map[string]any{}})
	if err == nil {
		t.Fatal("Execute() error = nil, want predicate failure")
	}
	if fake.settlementCount() != 1 {
		t.Fatalf("settlements = %d, want 1", fake.settlementCount())
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if fake.settles[0] != transport.RabbitPollerAck {
		t.Errorf("settlement = %v, want Ack before failing the node", fake.settles[0])
	}
}

func TestRabbitPollerNonbooleanAcksThenFails(t *testing.T) {
	fake := &fakeRabbitPoller{msgs: []transport.RabbitPollerMessage{{Body: []byte(`{"status":"completed"}`)}}}
	ex := newPollerExecutor(t)
	ex.SetRabbitClient(fake)
	cfg := &model.PollerRabbitMQConfig{
		Queue: "jobs", MaxWaitTime: time.Second, Until: "return 1;",
	}
	_, err := ex.Execute(context.Background(), executor.Request{Node: pollerRabbitNode(cfg), Context: map[string]any{}})
	if err == nil {
		t.Fatal("Execute() error = nil, want non-boolean failure")
	}
	if fake.settlementCount() != 1 {
		t.Fatalf("settlements = %d, want 1", fake.settlementCount())
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if fake.settles[0] != transport.RabbitPollerAck {
		t.Errorf("settlement = %v, want Ack before failing the node", fake.settles[0])
	}
}

func TestRabbitPollerTimeout(t *testing.T) {
	fake := &fakeRabbitPoller{}
	ex := newPollerExecutor(t)
	ex.SetRabbitClient(fake)
	cfg := &model.PollerRabbitMQConfig{
		Queue: "jobs", MaxWaitTime: 50 * time.Millisecond, Until: "return true;",
	}
	_, err := ex.Execute(context.Background(), executor.Request{Node: pollerRabbitNode(cfg), Context: map[string]any{}})
	if err == nil || !strings.Contains(err.Error(), "without a match") {
		t.Fatalf("Execute() error = %v, want wait timeout", err)
	}
}

func TestRabbitPollerCancellation(t *testing.T) {
	fake := &fakeRabbitPoller{}
	ex := newPollerExecutor(t)
	ex.SetRabbitClient(fake)
	cfg := &model.PollerRabbitMQConfig{
		Queue: "jobs", MaxWaitTime: time.Hour, Until: "return true;",
	}
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()
	start := time.Now()
	_, err := ex.Execute(ctx, executor.Request{Node: pollerRabbitNode(cfg), Context: map[string]any{}})
	if err == nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("Execute() error = %v, want context.Canceled", err)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Errorf("cancellation took %v, want fast", elapsed)
	}
}

func TestRabbitPollerConsumeError(t *testing.T) {
	fake := &fakeRabbitPoller{consumeErr: errors.New("queue jobs is missing")}
	ex := newPollerExecutor(t)
	ex.SetRabbitClient(fake)
	cfg := &model.PollerRabbitMQConfig{
		Queue: "jobs", MaxWaitTime: time.Second, Until: "return true;",
	}
	_, err := ex.Execute(context.Background(), executor.Request{Node: pollerRabbitNode(cfg), Context: map[string]any{}})
	if err == nil || !strings.Contains(err.Error(), "queue jobs is missing") {
		t.Fatalf("Execute() error = %v, want consume failure", err)
	}
}

func TestRabbitPollerMissingTransport(t *testing.T) {
	ex := newPollerExecutor(t)
	cfg := &model.PollerRabbitMQConfig{
		Queue: "jobs", MaxWaitTime: time.Second, Until: "return true;",
	}
	_, err := ex.Execute(context.Background(), executor.Request{Node: pollerRabbitNode(cfg), Context: map[string]any{}})
	if err == nil || !strings.Contains(err.Error(), "rabbitmq") {
		t.Fatalf("Execute() error = %v, want missing rabbitmq transport", err)
	}
}

func TestNewExecutorsRegistersPoller(t *testing.T) {
	reg := executor.NewExecutors(executor.Limits{}, nil, executor.Dependencies{})
	ex, ok := reg[model.NodeTypePoller]
	if !ok {
		t.Fatal("poller executor not registered")
	}
	cfg := &model.PollerRedisConfig{
		Method: "GET", Key: "k", RequestTimeout: time.Second, MaxAttempts: 1,
		Until: "return true;",
	}
	_, err := ex.Execute(context.Background(), executor.Request{Node: pollerRedisGETNode(cfg), Context: map[string]any{}})
	if err == nil || !strings.Contains(err.Error(), "redis") {
		t.Fatalf("missing redis dependency error = %v, want redis mention", err)
	}
}

func TestNewExecutorsPollerWithRedisDependency(t *testing.T) {
	fake := &fakeRedisPoller{values: map[string]string{"k": `{"ok":true}`}}
	reg := executor.NewExecutors(executor.Limits{}, nil, executor.Dependencies{RedisPoller: fake})
	cfg := &model.PollerRedisConfig{
		Method: "GET", Key: "k", RequestTimeout: time.Second, MaxAttempts: 1,
		Until: "return response.body.ok === true;",
	}
	res, err := reg[model.NodeTypePoller].Execute(context.Background(), executor.Request{Node: pollerRedisGETNode(cfg), Context: map[string]any{}})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	body, _ := res.Output.(*executor.PollerResponse).Body.(map[string]any)
	if body["ok"] != true {
		t.Errorf("body = %v, want ok true", res.Output.(*executor.PollerResponse).Body)
	}
}
