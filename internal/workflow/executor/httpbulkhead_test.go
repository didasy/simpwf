package executor

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"testing"
	"time"
)

func TestHTTPBulkheadCapEnforcement(t *testing.T) {
	b := NewHTTPBulkhead(2, 2)
	r1, err := b.Acquire(context.Background(), "a.example")
	if err != nil {
		t.Fatalf("acquire 1: %v", err)
	}
	defer r1()
	r2, err := b.Acquire(context.Background(), "a.example")
	if err != nil {
		t.Fatalf("acquire 2: %v", err)
	}
	defer r2()

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if _, err := b.Acquire(ctx, "a.example"); !errors.Is(err, ErrHTTPOverloaded) {
		t.Fatalf("acquire 3 over cap: err = %v, want ErrHTTPOverloaded", err)
	}

	r2()
	if r3, err := b.Acquire(context.Background(), "a.example"); err != nil {
		t.Fatalf("acquire after release: %v", err)
	} else {
		defer r3()
	}
}

func TestHTTPBulkheadHostIsolation(t *testing.T) {
	b := NewHTTPBulkhead(4, 1)
	releaseA, err := b.Acquire(context.Background(), "a.example")
	if err != nil {
		t.Fatalf("acquire A: %v", err)
	}
	defer releaseA()

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if _, err := b.Acquire(ctx, "a.example"); !errors.Is(err, ErrHTTPOverloaded) {
		t.Fatalf("second acquire on saturated host A: err = %v, want ErrHTTPOverloaded", err)
	}
	releaseB, err := b.Acquire(context.Background(), "b.example")
	if err != nil {
		t.Fatalf("acquire on idle host B while A saturated: %v", err)
	}
	releaseB()
}

func TestHTTPBulkheadOverflowValve(t *testing.T) {
	b := NewHTTPBulkhead(100000, 4)
	ctx := context.Background()
	for i := 0; i < 5000; i++ {
		release, err := b.Acquire(ctx, fmt.Sprintf("h%d.example", i))
		if err != nil {
			t.Fatalf("sequential acquire %d: %v", i, err)
		}
		release()
	}
	b.mu.Lock()
	n := len(b.hosts)
	b.mu.Unlock()
	if n > maxHTTPHostBuckets {
		t.Fatalf("host buckets = %d, want <= %d", n, maxHTTPHostBuckets)
	}

	// Buckets are full, so fresh hosts share the overflow bucket and stay
	// capped at the per-host limit.
	var held []func()
	for i := 0; i < 4; i++ {
		release, err := b.Acquire(ctx, fmt.Sprintf("overflow-%d.example", i))
		if err != nil {
			t.Fatalf("overflow acquire %d: %v", i, err)
		}
		held = append(held, release)
	}
	defer func() {
		for _, r := range held {
			r()
		}
	}()
	short, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	if _, err := b.Acquire(short, "overflow-extra.example"); !errors.Is(err, ErrHTTPOverloaded) {
		t.Fatalf("overflow over cap: err = %v, want ErrHTTPOverloaded", err)
	}
}

func TestParseRetryAfter(t *testing.T) {
	future := time.Now().Add(90 * time.Second).UTC().Format(http.TimeFormat)
	past := time.Now().Add(-90 * time.Second).UTC().Format(http.TimeFormat)
	cases := []struct {
		name   string
		header string
		want   time.Duration
		// wantApprox, when positive, accepts any delay in (0, wantApprox].
		wantApprox time.Duration
		ok         bool
	}{
		{name: "seconds", header: "120", want: 120 * time.Second, ok: true},
		{name: "zero", header: "0", want: 0, ok: true},
		{name: "padded", header: "  7 ", want: 7 * time.Second, ok: true},
		{name: "empty", header: "", ok: false},
		{name: "blank", header: "   ", ok: false},
		{name: "negative", header: "-5", ok: false},
		{name: "garbage", header: "soon", ok: false},
		{name: "fractional", header: "1.5", ok: false},
		{name: "future date", header: future, wantApprox: 90 * time.Second, ok: true},
		{name: "past date", header: past, want: 0, ok: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := parseRetryAfter(tc.header)
			if ok != tc.ok {
				t.Fatalf("parseRetryAfter(%q) ok = %v, want %v", tc.header, ok, tc.ok)
			}
			if !ok {
				return
			}
			if tc.wantApprox > 0 {
				if got <= 0 || got > tc.wantApprox {
					t.Fatalf("parseRetryAfter(%q) = %v, want in (0, %v]", tc.header, got, tc.wantApprox)
				}
				return
			}
			if got != tc.want {
				t.Fatalf("parseRetryAfter(%q) = %v, want %v", tc.header, got, tc.want)
			}
		})
	}
}

func TestHTTPBulkheadReleaseIdempotent(t *testing.T) {
	b := NewHTTPBulkhead(1, 1)
	release, err := b.Acquire(context.Background(), "a.example")
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	release()
	done := make(chan struct{})
	go func() { release(); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("second release blocked, want prompt idempotent return")
	}
	release2, err := b.Acquire(context.Background(), "a.example")
	if err != nil {
		t.Fatalf("acquire after releases: %v", err)
	}
	release2()
}

func TestHTTPBulkheadCancelPrompt(t *testing.T) {
	b := NewHTTPBulkhead(1, 1)
	hold, err := b.Acquire(context.Background(), "a.example")
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	defer hold()

	ctx, cancel := context.WithCancel(context.Background())
	type result struct {
		release func()
		err     error
	}
	resCh := make(chan result, 1)
	go func() {
		release, err := b.Acquire(ctx, "a.example")
		resCh <- result{release: release, err: err}
	}()
	time.Sleep(20 * time.Millisecond)
	cancel()
	select {
	case res := <-resCh:
		if !errors.Is(res.err, ErrHTTPOverloaded) {
			t.Fatalf("cancelled acquire err = %v, want ErrHTTPOverloaded", res.err)
		}
		if res.release != nil {
			res.release()
		}
	case <-time.After(2 * time.Second):
		t.Fatal("cancelled acquire did not return promptly")
	}
}

func TestHTTPBulkheadConcurrentSmoke(t *testing.T) {
	b := NewHTTPBulkhead(8, 4)
	var wg sync.WaitGroup
	errCh := make(chan error, 200)
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 4; j++ {
				host := fmt.Sprintf("h%d.example", (i+j)%5)
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				release, err := b.Acquire(ctx, host)
				cancel()
				if err != nil {
					errCh <- err
					return
				}
				time.Sleep(time.Millisecond)
				release()
			}
		}(i)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatalf("concurrent acquire: %v", err)
	}
}
