package inputtransport

import (
	"context"
	"sync"
	"testing"
	"time"
)

// manualClock drives CappedOptions.Now without waiting.
type manualClock struct {
	mu sync.Mutex
	at time.Time
}

func (c *manualClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.at
}

func (c *manualClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.at = c.at.Add(d)
}

// sleepRecorder stands in for CappedOptions.Sleep: it records durations and
// never waits.
type sleepRecorder struct {
	mu     sync.Mutex
	delays []time.Duration
}

func (s *sleepRecorder) sleep(_ context.Context, d time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.delays = append(s.delays, d)
}

func (s *sleepRecorder) snapshot() []time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]time.Duration(nil), s.delays...)
}

// waitFor polls cond until true or the test times out.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

// inBounds reports whether d is within [base, base + 25% jitter].
func inBounds(d, base time.Duration) bool {
	return d >= base && d <= base+base/4
}

// Fail twice, then block until shutdown: the leeway path. Both failures are
// counted, the backoff stays bounded, and the consumer ends healthy because
// the budget never exhausts.
func TestSuperviseCappedRecoversWithinBudget(t *testing.T) {
	clock := &manualClock{at: time.Now()}
	rec := &sleepRecorder{}
	status := NewConsumerStatus("redis")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	calls := 0
	run := func(ctx context.Context) error {
		calls++
		if calls <= 2 {
			return context.DeadlineExceeded
		}
		<-ctx.Done()
		return ctx.Err()
	}
	done := make(chan struct{})
	go func() {
		SuperviseCapped(ctx, status, run, CappedOptions{
			MaxAttempts:    3,
			InitialBackoff: time.Second,
			MaxBackoff:     30 * time.Second,
			MinHealthyRun:  30 * time.Second,
			Sleep:          rec.sleep,
			Now:            clock.now,
		})
		close(done)
	}()

	waitFor(t, "2 failures", func() bool { return status.Attempts() == 2 })
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("supervisor did not return after ctx cancel")
	}

	if !status.Healthy() {
		t.Error("Healthy() = false, want true (budget never exhausted)")
	}
	if got := status.Attempts(); got != 2 {
		t.Errorf("Attempts() = %d, want 2", got)
	}
	delays := rec.snapshot()
	if len(delays) != 2 {
		t.Fatalf("sleeps = %d, want 2", len(delays))
	}
	if !inBounds(delays[0], time.Second) {
		t.Errorf("sleep[0] = %v, want within [1s, 1.25s]", delays[0])
	}
	if !inBounds(delays[1], 2*time.Second) {
		t.Errorf("sleep[1] = %v, want within [2s, 2.5s]", delays[1])
	}
}

// Fail MaxAttempts times in a row: the consumer goes sticky-down and the
// supervisor stops without sleeping after the final failure.
func TestSuperviseCappedMarksDownPastBudget(t *testing.T) {
	clock := &manualClock{at: time.Now()}
	rec := &sleepRecorder{}
	status := NewConsumerStatus("rabbitmq")
	calls := 0
	SuperviseCapped(context.Background(), status, func(context.Context) error {
		calls++
		return context.DeadlineExceeded
	}, CappedOptions{
		MaxAttempts:    3,
		InitialBackoff: time.Second,
		MaxBackoff:     30 * time.Second,
		MinHealthyRun:  30 * time.Second,
		Sleep:          rec.sleep,
		Now:            clock.now,
	})

	if status.Healthy() {
		t.Error("Healthy() = true, want false (budget exhausted)")
	}
	if got := status.Attempts(); got != 3 {
		t.Errorf("Attempts() = %d, want 3", got)
	}
	if calls != 3 {
		t.Errorf("run calls = %d, want 3 (no attempts past the budget)", calls)
	}
	if got := len(rec.snapshot()); got != 2 {
		t.Errorf("sleeps = %d, want 2 (none after the final failure)", got)
	}
	// Sticky: it stays down.
	if status.Healthy() {
		t.Error("Healthy() = true after mark-down, want sticky false")
	}
}

// A nil return with a live context is a failure: this is the Redis
// channel-closed path, which returns nil instead of an error.
func TestSuperviseCappedNilReturnCountsAsFailure(t *testing.T) {
	clock := &manualClock{at: time.Now()}
	rec := &sleepRecorder{}
	status := NewConsumerStatus("redis")
	SuperviseCapped(context.Background(), status, func(context.Context) error {
		return nil
	}, CappedOptions{
		MaxAttempts:    2,
		InitialBackoff: time.Second,
		MaxBackoff:     30 * time.Second,
		MinHealthyRun:  30 * time.Second,
		Sleep:          rec.sleep,
		Now:            clock.now,
	})

	if status.Healthy() {
		t.Error("Healthy() = true, want false (nil returns exhaust the budget)")
	}
	if got := status.Attempts(); got != 2 {
		t.Errorf("Attempts() = %d, want 2", got)
	}
}

// Cancel during a run: clean shutdown, no failure recorded.
func TestSuperviseCappedCancelDuringRunStaysHealthy(t *testing.T) {
	clock := &manualClock{at: time.Now()}
	rec := &sleepRecorder{}
	status := NewConsumerStatus("redis")
	ctx, cancel := context.WithCancel(context.Background())

	started := make(chan struct{})
	var once sync.Once
	done := make(chan struct{})
	go func() {
		SuperviseCapped(ctx, status, func(ctx context.Context) error {
			once.Do(func() { close(started) })
			<-ctx.Done()
			return ctx.Err()
		}, CappedOptions{
			MaxAttempts:    3,
			InitialBackoff: time.Second,
			MaxBackoff:     30 * time.Second,
			MinHealthyRun:  30 * time.Second,
			Sleep:          rec.sleep,
			Now:            clock.now,
		})
		close(done)
	}()

	<-started
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("supervisor did not return after ctx cancel")
	}
	if !status.Healthy() {
		t.Error("Healthy() = false, want true (cancel is a clean shutdown)")
	}
	if got := status.Attempts(); got != 0 {
		t.Errorf("Attempts() = %d, want 0", got)
	}
	if got := len(rec.snapshot()); got != 0 {
		t.Errorf("sleeps = %d, want 0", got)
	}
}

// Cancel during the backoff sleep: the supervisor returns promptly without
// marking down, since the budget never exhausted.
func TestSuperviseCappedCancelDuringSleepReturnsPromptly(t *testing.T) {
	clock := &manualClock{at: time.Now()}
	status := NewConsumerStatus("rabbitmq")
	ctx, cancel := context.WithCancel(context.Background())

	sleeping := make(chan struct{})
	var once sync.Once
	done := make(chan struct{})
	go func() {
		SuperviseCapped(ctx, status, func(context.Context) error {
			return context.DeadlineExceeded
		}, CappedOptions{
			MaxAttempts:    3,
			InitialBackoff: time.Second,
			MaxBackoff:     30 * time.Second,
			MinHealthyRun:  30 * time.Second,
			Sleep: func(ctx context.Context, _ time.Duration) {
				once.Do(func() { close(sleeping) })
				<-ctx.Done()
			},
			Now: clock.now,
		})
		close(done)
	}()

	<-sleeping
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("supervisor did not return promptly after ctx cancel during sleep")
	}
	if !status.Healthy() {
		t.Error("Healthy() = false, want true (1 failure is within budget)")
	}
	if got := status.Attempts(); got != 1 {
		t.Errorf("Attempts() = %d, want 1", got)
	}
}

// An attempt that lives past MinHealthyRun resets the consecutive-failure
// count: with MaxAttempts 2, the second fast failure alone would mark down,
// but a long-lived attempt between them keeps the consumer up until the
// third death.
func TestSuperviseCappedLongRunResetsBudget(t *testing.T) {
	clock := &manualClock{at: time.Now()}
	rec := &sleepRecorder{}
	status := NewConsumerStatus("redis")
	calls := 0
	SuperviseCapped(context.Background(), status, func(context.Context) error {
		calls++
		if calls == 2 {
			clock.advance(31 * time.Second)
		}
		return context.DeadlineExceeded
	}, CappedOptions{
		MaxAttempts:    2,
		InitialBackoff: time.Second,
		MaxBackoff:     30 * time.Second,
		MinHealthyRun:  30 * time.Second,
		Sleep:          rec.sleep,
		Now:            clock.now,
	})

	if calls != 3 {
		t.Fatalf("run calls = %d, want 3 (the long run must reset the count)", calls)
	}
	if status.Healthy() {
		t.Error("Healthy() = true, want false (two fast deaths after the reset)")
	}
	if got := status.Attempts(); got != 3 {
		t.Errorf("Attempts() = %d, want 3 total failures", got)
	}
}

// Backoff doubles per consecutive failure and caps at MaxBackoff.
func TestSuperviseCappedBackoffDoublesAndCaps(t *testing.T) {
	clock := &manualClock{at: time.Now()}
	rec := &sleepRecorder{}
	status := NewConsumerStatus("rabbitmq")
	SuperviseCapped(context.Background(), status, func(context.Context) error {
		return context.DeadlineExceeded
	}, CappedOptions{
		MaxAttempts:    4,
		InitialBackoff: time.Second,
		MaxBackoff:     2 * time.Second,
		MinHealthyRun:  30 * time.Second,
		Sleep:          rec.sleep,
		Now:            clock.now,
	})

	delays := rec.snapshot()
	want := []time.Duration{time.Second, 2 * time.Second, 2 * time.Second}
	if len(delays) != len(want) {
		t.Fatalf("sleeps = %d, want %d", len(delays), len(want))
	}
	for i, base := range want {
		if !inBounds(delays[i], base) {
			t.Errorf("sleep[%d] = %v, want within [%v, %v]", i, delays[i], base, base+base/4)
		}
	}
}

// Concurrent Healthy/Attempts readers while the supervisor writes: clean
// under -race.
func TestConsumerStatusConcurrentReaders(t *testing.T) {
	clock := &manualClock{at: time.Now()}
	rec := &sleepRecorder{}
	status := NewConsumerStatus("redis")

	done := make(chan struct{})
	go func() {
		SuperviseCapped(context.Background(), status, func(context.Context) error {
			return context.DeadlineExceeded
		}, CappedOptions{
			MaxAttempts:    200,
			InitialBackoff: time.Second,
			MaxBackoff:     30 * time.Second,
			MinHealthyRun:  30 * time.Second,
			Sleep:          rec.sleep,
			Now:            clock.now,
		})
		close(done)
	}()

	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 500 {
				_ = status.Healthy()
				_ = status.Attempts()
			}
		}()
	}
	wg.Wait()
	<-done
	if status.Healthy() {
		t.Error("Healthy() = true, want false (200 failures exhaust any budget)")
	}
}

// Nil Sleep/Now fall back to the production timer and wall clock. With
// MaxAttempts 1 no sleep is ever reached, so this stays instant.
func TestSuperviseCappedNilHooksUseDefaults(t *testing.T) {
	status := NewConsumerStatus("redis")
	SuperviseCapped(context.Background(), status, func(context.Context) error {
		return context.DeadlineExceeded
	}, CappedOptions{
		MaxAttempts:    1,
		InitialBackoff: time.Second,
		MaxBackoff:     30 * time.Second,
		MinHealthyRun:  time.Minute,
	})

	if status.Healthy() {
		t.Error("Healthy() = true, want false")
	}
	if got := status.Attempts(); got != 1 {
		t.Errorf("Attempts() = %d, want 1", got)
	}
}
