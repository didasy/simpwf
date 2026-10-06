package executor_test

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/simpwf/workflow-engine/internal/workflow/executor"
	"github.com/simpwf/workflow-engine/internal/workflow/model"
)

// inflight tracks concurrent handler entries and the max observed.
type inflight struct {
	cur atomic.Int64
	max atomic.Int64
}

func (t *inflight) enter() func() {
	n := t.cur.Add(1)
	for {
		m := t.max.Load()
		if n <= m || t.max.CompareAndSwap(m, n) {
			break
		}
	}
	return func() { t.cur.Add(-1) }
}

func bulkheadTestExecutor(b *executor.HTTPBulkhead) *executor.HTTPExecutor {
	return executor.NewHTTPExecutor(executor.Limits{
		HTTPAllowlist: []string{"*"},
		MaxRedirects:  5,
		HTTPBulkhead:  b,
	})
}

func TestDoPerHostCap(t *testing.T) {
	var tr inflight
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer tr.enter()()
		time.Sleep(50 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`ok`))
	}))
	defer srv.Close()

	ex := bulkheadTestExecutor(nil) // private defaults: 128 global, 16 per host
	const n = 200
	var wg sync.WaitGroup
	errCh := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, status, _, err := ex.Do(context.Background(), "GET", srv.URL, nil, nil, 30*time.Second)
			if err != nil {
				errCh <- err
				return
			}
			if status != http.StatusOK {
				errCh <- fmt.Errorf("status = %d, want 200", status)
			}
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatalf("hammer call: %v", err)
	}
	if got := tr.max.Load(); got > 16 {
		t.Fatalf("max per-host in-flight = %d, want <= 16", got)
	} else if got < 2 {
		t.Fatalf("max per-host in-flight = %d, want parallelism > 1", got)
	}
}

func TestDoGlobalCap(t *testing.T) {
	var tr inflight
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer tr.enter()()
		time.Sleep(50 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`ok`))
	})
	const hosts = 8
	var urls []string
	for i := 0; i < hosts; i++ {
		srv := httptest.NewServer(handler)
		defer srv.Close()
		urls = append(urls, srv.URL)
	}

	// Per-host headroom (16 x 8 = 128) far exceeds the global cap, so the
	// global semaphore is what binds.
	ex := bulkheadTestExecutor(executor.NewHTTPBulkhead(32, 16))
	const perHost = 10
	var wg sync.WaitGroup
	errCh := make(chan error, hosts*perHost)
	for _, u := range urls {
		for i := 0; i < perHost; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				_, status, _, err := ex.Do(context.Background(), "GET", u, nil, nil, 30*time.Second)
				if err != nil {
					errCh <- err
					return
				}
				if status != http.StatusOK {
					errCh <- fmt.Errorf("status = %d, want 200", status)
				}
			}()
		}
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatalf("hammer call: %v", err)
	}
	if got := tr.max.Load(); got > 32 {
		t.Fatalf("max total in-flight = %d, want <= 32", got)
	}
}

func TestDoShedsWithOverload(t *testing.T) {
	releaseHook := make(chan struct{})
	// release unblocks the handler; deferred before srv.Close() because
	// Close waits for active requests. sync.Once keeps the mid-test
	// release and the deferred safety release from double-closing.
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseHook) }) }
	var inHandler atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		inHandler.Add(1)
		defer inHandler.Add(-1)
		<-releaseHook
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`ok`))
	}))
	defer srv.Close()
	defer release()

	shared := executor.NewHTTPBulkhead(1, 1)
	ex := bulkheadTestExecutor(shared)

	// Saturate the only slot with a call that stays in flight.
	done := make(chan error, 1)
	go func() {
		_, _, _, err := ex.Do(context.Background(), "GET", srv.URL, nil, nil, 10*time.Second)
		done <- err
	}()
	deadline := time.Now().Add(5 * time.Second)
	for inHandler.Load() < 1 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if inHandler.Load() < 1 {
		t.Fatal("saturating call never reached the handler")
	}

	start := time.Now()
	_, _, _, err := ex.Do(context.Background(), "GET", srv.URL, nil, nil, 200*time.Millisecond)
	elapsed := time.Since(start)
	if !errors.Is(err, executor.ErrHTTPOverloaded) {
		t.Fatalf("saturated Do err = %v, want ErrHTTPOverloaded", err)
	}
	if elapsed > 5*time.Second {
		t.Fatalf("saturated Do took %v, want fast shed", elapsed)
	}

	release()
	if err := <-done; err != nil {
		t.Fatalf("saturating call err = %v, want nil", err)
	}

	// Permits unleaked: a post-saturation call succeeds.
	if _, status, _, err := ex.Do(context.Background(), "GET", srv.URL, nil, nil, 5*time.Second); err != nil || status != http.StatusOK {
		t.Fatalf("post-saturation Do = (%d, %v), want (200, nil)", status, err)
	}
}

func TestExecuteMapsOverloadReason(t *testing.T) {
	releaseHook := make(chan struct{})
	started := make(chan struct{})
	var once sync.Once
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		once.Do(func() { close(started) })
		<-releaseHook
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`ok`))
	}))
	defer srv.Close()
	// Close the hook before the server: Close blocks until the gated
	// active request completes.
	defer close(releaseHook)

	shared := executor.NewHTTPBulkhead(1, 1)
	ex := bulkheadTestExecutor(shared)
	go func() {
		_, _, _, _ = ex.Do(context.Background(), "GET", srv.URL, nil, nil, 10*time.Second)
	}()
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("saturating call never reached the handler")
	}

	_, err := ex.Execute(context.Background(), executor.Request{
		Node: &model.NodeContent{
			Name:    "call",
			Type:    model.NodeTypeExternalCall,
			Timeout: 200 * time.Millisecond,
			HTTP:    &model.HTTPConfig{URL: srv.URL, Method: "GET"},
		},
		Context: map[string]any{},
	})
	var nodeErr *executor.NodeError
	if !errors.As(err, &nodeErr) {
		t.Fatalf("Execute err type = %T, want *NodeError", err)
	}
	if nodeErr.Reason != "http-overload" {
		t.Fatalf("Execute reason = %q, want http-overload", nodeErr.Reason)
	}
}

func TestDoResponseIndependence(t *testing.T) {
	var hits atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := hits.Add(1)
		time.Sleep(10 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
		_, _ = fmt.Fprintf(w, `{"hit":%d}`, n)
	}))
	defer srv.Close()

	ex := bulkheadTestExecutor(nil)
	const n = 20
	var wg sync.WaitGroup
	bodies := make([]string, n)
	errCh := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			body, status, _, err := ex.Do(context.Background(), "GET", srv.URL+"/same", nil, nil, 10*time.Second)
			if err != nil {
				errCh <- err
				return
			}
			if status != http.StatusOK {
				errCh <- fmt.Errorf("status = %d, want 200", status)
				return
			}
			bodies[i] = string(body)
		}(i)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatalf("call: %v", err)
	}
	if got := hits.Load(); got != n {
		t.Fatalf("upstream hits = %d, want one per caller (%d)", got, n)
	}
	seen := make(map[string]bool, n)
	for _, b := range bodies {
		if seen[b] {
			t.Fatalf("duplicate body %q: responses must stay independent", b)
		}
		seen[b] = true
	}
}

func TestDoRetryAfterHonored(t *testing.T) {
	var hits atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if hits.Add(1) == 1 {
			w.Header().Set("Retry-After", "1")
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = w.Write([]byte(`slow down`))
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`ok`))
	}))
	defer srv.Close()

	ex := bulkheadTestExecutor(nil)
	start := time.Now()
	body, status, _, err := ex.Do(context.Background(), "GET", srv.URL, nil, nil, 10*time.Second)
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("Do err = %v, want nil", err)
	}
	if status != http.StatusOK || string(body) != "ok" {
		t.Fatalf("Do = (%d, %q), want (200, ok)", status, body)
	}
	if got := hits.Load(); got != 2 {
		t.Fatalf("upstream hits = %d, want 2", got)
	}
	if elapsed < 900*time.Millisecond {
		t.Fatalf("elapsed = %v, want >= Retry-After 1s", elapsed)
	}
}

func TestDoRetryWithoutHeaderBounded(t *testing.T) {
	var hits atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = w.Write([]byte(`slow down`))
	}))
	defer srv.Close()

	ex := bulkheadTestExecutor(nil)
	body, status, _, err := ex.Do(context.Background(), "GET", srv.URL, nil, nil, 15*time.Second)
	if err != nil {
		t.Fatalf("Do err = %v, want nil (429 surfaced, not error)", err)
	}
	if status != http.StatusTooManyRequests {
		t.Fatalf("status = %d, want 429", status)
	}
	if string(body) != "slow down" {
		t.Fatalf("body = %q", body)
	}
	if got := hits.Load(); got != 3 {
		t.Fatalf("upstream hits = %d, want 3 (initial + 2 retries)", got)
	}
}

func TestDoPostNeverRetries(t *testing.T) {
	var hits atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.Header().Set("Retry-After", "1")
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = w.Write([]byte(`slow down`))
	}))
	defer srv.Close()

	ex := bulkheadTestExecutor(nil)
	start := time.Now()
	_, status, _, err := ex.Do(context.Background(), "POST", srv.URL, nil, []byte(`{}`), 10*time.Second)
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("Do err = %v, want nil (429 surfaced, not error)", err)
	}
	if status != http.StatusTooManyRequests {
		t.Fatalf("status = %d, want 429", status)
	}
	if got := hits.Load(); got != 1 {
		t.Fatalf("upstream hits = %d, want 1 (POST never retries)", got)
	}
	if elapsed > 5*time.Second {
		t.Fatalf("elapsed = %v, want prompt return without backoff", elapsed)
	}
}

func TestDoOversizedRetryAfterReturnsPromptly(t *testing.T) {
	var hits atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.Header().Set("Retry-After", "60")
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(`try later`))
	}))
	defer srv.Close()

	ex := bulkheadTestExecutor(nil)
	start := time.Now()
	_, status, _, err := ex.Do(context.Background(), "GET", srv.URL, nil, nil, 300*time.Millisecond)
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("Do err = %v, want nil (503 surfaced, not error)", err)
	}
	if status != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", status)
	}
	if got := hits.Load(); got != 1 {
		t.Fatalf("upstream hits = %d, want 1 (no retry past budget)", got)
	}
	if elapsed > 5*time.Second {
		t.Fatalf("elapsed = %v, want prompt return, not the 60s Retry-After", elapsed)
	}
}

func TestDoChaosRecovery(t *testing.T) {
	var mode atomic.Int64 // 0 = hang, 1 = 500s, 2 = ok
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch mode.Load() {
		case 0:
			time.Sleep(500 * time.Millisecond)
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`late`))
		case 1:
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(`boom`))
		default:
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`ok`))
		}
	}))
	defer srv.Close()

	ex := bulkheadTestExecutor(nil)
	ctx := context.Background()

	// Hung endpoint: in-flight calls time out with transport errors.
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _, _, err := ex.Do(ctx, "GET", srv.URL, nil, nil, 100*time.Millisecond)
			if err == nil {
				t.Error("hung call succeeded, want timeout error")
			}
		}()
	}
	wg.Wait()

	// 500s are surfaced, never retried.
	mode.Store(1)
	for i := 0; i < 10; i++ {
		_, status, _, err := ex.Do(ctx, "GET", srv.URL, nil, nil, 5*time.Second)
		if err != nil {
			t.Fatalf("500-phase call err = %v, want nil", err)
		}
		if status != http.StatusInternalServerError {
			t.Fatalf("500-phase status = %d, want 500", status)
		}
	}

	// Recovery: a burst succeeds with no wedged permits.
	mode.Store(2)
	const n = 50
	errCh := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			body, status, _, err := ex.Do(ctx, "GET", srv.URL, nil, nil, 5*time.Second)
			if err != nil {
				errCh <- err
				return
			}
			if status != http.StatusOK || string(body) != "ok" {
				errCh <- fmt.Errorf("recovery call = (%d, %q), want (200, ok)", status, body)
			}
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatalf("recovery: %v", err)
	}
}
