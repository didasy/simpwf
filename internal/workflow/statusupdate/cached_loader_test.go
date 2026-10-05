package statusupdate_test

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/simpwf/workflow-engine/internal/workflow/model"
	"github.com/simpwf/workflow-engine/internal/workflow/statusupdate"
)

type countingConfigLoader struct {
	mu    sync.Mutex
	calls int
	cfg   *model.StatusUpdateConfig
	err   error
}

func (l *countingConfigLoader) load(_ context.Context, _ string) (*model.StatusUpdateConfig, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.calls++
	return l.cfg, l.err
}

func (l *countingConfigLoader) getCalls() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.calls
}

func testStatusConfig() *model.StatusUpdateConfig {
	return &model.StatusUpdateConfig{HTTP: &model.HTTPStatusUpdateConfig{URL: "https://hooks.example.com/wf"}}
}

func TestCachedLoaderLoadsOnce(t *testing.T) {
	inner := &countingConfigLoader{cfg: testStatusConfig()}
	loader := statusupdate.CachedLoader(inner.load, 16)
	ctx := context.Background()

	first, err := loader(ctx, "def-1")
	if err != nil {
		t.Fatalf("loader() error = %v", err)
	}
	second, err := loader(ctx, "def-1")
	if err != nil {
		t.Fatalf("loader() error = %v", err)
	}
	if second != first {
		t.Error("second load returned a different pointer, want the cached config")
	}
	if got := inner.getCalls(); got != 1 {
		t.Errorf("inner calls = %d, want 1", got)
	}
	if _, err := loader(ctx, "def-2"); err != nil {
		t.Fatalf("loader() error = %v", err)
	}
	if got := inner.getCalls(); got != 2 {
		t.Errorf("inner calls = %d, want 2 (once per id)", got)
	}
}

func TestCachedLoaderCachesNegative(t *testing.T) {
	inner := &countingConfigLoader{cfg: nil}
	loader := statusupdate.CachedLoader(inner.load, 16)
	ctx := context.Background()

	for i := 0; i < 2; i++ {
		cfg, err := loader(ctx, "unconfigured")
		if err != nil {
			t.Fatalf("loader() error = %v", err)
		}
		if cfg != nil {
			t.Fatalf("loader() = %+v, want nil (not configured)", cfg)
		}
	}
	if got := inner.getCalls(); got != 1 {
		t.Errorf("inner calls = %d, want 1 (negative cached)", got)
	}
}

func TestCachedLoaderRetriesErrors(t *testing.T) {
	inner := &countingConfigLoader{err: errors.New("store exploded")}
	loader := statusupdate.CachedLoader(inner.load, 16)
	ctx := context.Background()

	for i := 0; i < 2; i++ {
		if _, err := loader(ctx, "def-1"); err == nil {
			t.Fatal("loader() error = nil, want store error")
		}
	}
	if got := inner.getCalls(); got != 2 {
		t.Errorf("inner calls = %d, want 2 (errors not cached)", got)
	}
	inner.err = nil
	inner.cfg = testStatusConfig()
	if _, err := loader(ctx, "def-1"); err != nil {
		t.Fatalf("loader() after recovery error = %v", err)
	}
}

func TestCachedLoaderConcurrentHammer(t *testing.T) {
	inner := &countingConfigLoader{cfg: testStatusConfig()}
	loader := statusupdate.CachedLoader(inner.load, 64)
	ctx := context.Background()

	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				if _, err := loader(ctx, "def-1"); err != nil {
					t.Error(err)
					return
				}
			}
		}()
	}
	wg.Wait()
	if got := inner.getCalls(); got != 1 {
		t.Errorf("inner calls = %d, want 1", got)
	}
}
