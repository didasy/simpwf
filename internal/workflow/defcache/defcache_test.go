package defcache_test

import (
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/simpwf/workflow-engine/internal/workflow/defcache"
)

func TestGetOrComputeComputesOnce(t *testing.T) {
	c := defcache.New[string, string](16)
	calls := 0
	compute := func() (string, error) { calls++; return "v", nil }
	for i := 0; i < 3; i++ {
		v, err := c.GetOrCompute("k", compute)
		if err != nil {
			t.Fatalf("GetOrCompute() error = %v", err)
		}
		if v != "v" {
			t.Fatalf("GetOrCompute() = %q, want %q", v, "v")
		}
	}
	if calls != 1 {
		t.Errorf("compute calls = %d, want 1", calls)
	}
	if got := c.Hits(); got != 2 {
		t.Errorf("Hits() = %d, want 2", got)
	}
	if got := c.Misses(); got != 1 {
		t.Errorf("Misses() = %d, want 1", got)
	}
	if got := c.Len(); got != 1 {
		t.Errorf("Len() = %d, want 1", got)
	}
}

func TestGetOrComputeDoesNotCacheErrors(t *testing.T) {
	c := defcache.New[string, string](16)
	calls := 0
	fail := func() (string, error) { calls++; return "", errors.New("boom") }
	for i := 0; i < 2; i++ {
		if _, err := c.GetOrCompute("k", fail); err == nil {
			t.Fatal("GetOrCompute() error = nil, want boom")
		}
	}
	if calls != 2 {
		t.Errorf("compute calls = %d, want 2 (failures retried)", calls)
	}
	if got := c.Len(); got != 0 {
		t.Errorf("Len() = %d, want 0 after failures", got)
	}
	v, err := c.GetOrCompute("k", func() (string, error) { return "ok", nil })
	if err != nil || v != "ok" {
		t.Fatalf("GetOrCompute() = %q, %v; want %q, nil", v, err, "ok")
	}
}

func TestEvictsOldestAtCap(t *testing.T) {
	c := defcache.New[string, int](2)
	computes := map[string]int{}
	compute := func(k string) func() (int, error) {
		return func() (int, error) { computes[k]++; return len(computes), nil }
	}
	if _, err := c.GetOrCompute("a", compute("a")); err != nil {
		t.Fatal(err)
	}
	if _, err := c.GetOrCompute("b", compute("b")); err != nil {
		t.Fatal(err)
	}
	if _, err := c.GetOrCompute("c", compute("c")); err != nil {
		t.Fatal(err)
	}
	// Cache now holds [b c]: survivors are hits, the evicted "a" recomputes.
	// Check the survivors first: re-inserting "a" must evict again.
	if _, err := c.GetOrCompute("b", compute("b")); err != nil {
		t.Fatal(err)
	}
	if _, err := c.GetOrCompute("c", compute("c")); err != nil {
		t.Fatal(err)
	}
	if computes["b"] != 1 || computes["c"] != 1 {
		t.Errorf("computes = %v, want b:1 c:1 (survivors are hits)", computes)
	}
	if _, err := c.GetOrCompute("a", compute("a")); err != nil {
		t.Fatal(err)
	}
	if computes["a"] != 2 {
		t.Errorf("computes[a] = %d, want 2 (evicted, recomputed)", computes["a"])
	}
	if got := c.Len(); got != 2 {
		t.Errorf("Len() = %d, want 2 (bounded at cap)", got)
	}
}

func TestZeroAndNegativeCapStayBounded(t *testing.T) {
	for _, cap := range []int{0, -5} {
		c := defcache.New[string, string](cap)
		for i := 0; i < 10; i++ {
			k := fmt.Sprintf("k%d", i)
			if _, err := c.GetOrCompute(k, func() (string, error) { return k, nil }); err != nil {
				t.Fatal(err)
			}
		}
		if got := c.Len(); got > 1 {
			t.Errorf("cap %d: Len() = %d, want <= 1", cap, got)
		}
		if _, err := c.GetOrCompute("k9", func() (string, error) { return "recomputed", nil }); err != nil {
			t.Fatal(err)
		}
	}
}

func TestConcurrentHammer(t *testing.T) {
	c := defcache.New[string, int](64)
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				// Shared keys across goroutines plus goroutine-local keys.
				keys := []string{fmt.Sprintf("shared-%d", i%16), fmt.Sprintf("g%d-%d", g, i)}
				for _, k := range keys {
					k := k
					if _, err := c.GetOrCompute(k, func() (int, error) { return len(k), nil }); err != nil {
						t.Error(err)
						return
					}
				}
			}
		}(g)
	}
	wg.Wait()
	if got := c.Len(); got > 64 {
		t.Errorf("Len() = %d, want <= 64", got)
	}
}
