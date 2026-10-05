// Package inputtransport consumes broker input for waiting workflow
// instances and delivers it through the instance service with the source
// channel matching the transport.
package inputtransport

import (
	"context"
	"log/slog"
	"math/rand/v2"
	"sync"
	"time"
)

// ConsumerStatus tracks whether one broker input consumer is healthy. It
// starts healthy; once the supervisor marks it down it stays down for the
// life of the process, until a restart with healthy brokers.
type ConsumerStatus struct {
	mu       sync.Mutex
	name     string
	down     bool
	attempts int
}

// NewConsumerStatus builds the status for one named consumer.
func NewConsumerStatus(name string) *ConsumerStatus {
	return &ConsumerStatus{name: name}
}

// Healthy reports whether the consumer is up. Safe for concurrent use by the
// /ready handler while the supervisor writes.
func (s *ConsumerStatus) Healthy() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return !s.down
}

// Attempts reports the total failed attempts so far. Logs only; the retry
// budget tracks consecutive failures internally.
func (s *ConsumerStatus) Attempts() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.attempts
}

func (s *ConsumerStatus) recordFailure() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.attempts++
}

func (s *ConsumerStatus) markDown() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.down = true
}

// CappedOptions tunes SuperviseCapped. MaxAttempts, InitialBackoff,
// MaxBackoff, and MinHealthyRun come from engine.consumer_retry.*; Sleep and
// Now are injectable so tests record durations and drive a manual clock
// instead of waiting. A nil Sleep waits on a real timer interruptible by
// ctx; a nil Now reads the wall clock.
type CappedOptions struct {
	MaxAttempts    int
	InitialBackoff time.Duration
	MaxBackoff     time.Duration
	MinHealthyRun  time.Duration
	Sleep          func(ctx context.Context, d time.Duration)
	Now            func() time.Time
}

// SleepInterruptible is the production CappedOptions.Sleep: it waits out d
// but returns early when ctx is done, so shutdown never waits out the timer.
func SleepInterruptible(ctx context.Context, d time.Duration) {
	if d <= 0 {
		return
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}

// SuperviseCapped runs run, retrying it with doubling backoff until it either
// stays up or fails MaxAttempts times in a row, at which point the status is
// marked down (sticky until process restart) and /ready reports 503.
//
// Any return with a live context counts as a failure, including a nil
// return: the Redis consumer's channel-closed path returns nil. A return
// after ctx is cancelled is a clean shutdown: no failure is recorded and the
// status stays healthy. An attempt that lives past MinHealthyRun resets the
// consecutive-failure count, so slow flapping over hours never exhausts the
// budget — only back-to-back deaths do.
func SuperviseCapped(ctx context.Context, status *ConsumerStatus, run func(context.Context) error, opts CappedOptions) {
	sleep := opts.Sleep
	if sleep == nil {
		sleep = SleepInterruptible
	}
	now := opts.Now
	if now == nil {
		now = time.Now
	}
	failures := 0
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		start := now()
		// The error text is deliberately not logged: dial errors can
		// embed DSN credentials. Name + attempt is enough to correlate.
		_ = run(ctx)
		if ctx.Err() != nil {
			return
		}
		if now().Sub(start) >= opts.MinHealthyRun {
			failures = 0
		}
		failures++
		status.recordFailure()
		if failures >= opts.MaxAttempts {
			status.markDown()
			slog.Warn("broker input consumer down, /ready unavailable until restart",
				"consumer", status.name, "attempts", status.Attempts())
			return
		}
		delay := cappedBackoff(opts.InitialBackoff, opts.MaxBackoff, failures)
		slog.Warn("broker input consumer attempt failed, retrying",
			"consumer", status.name, "attempt", status.Attempts(), "retry_in", delay)
		sleep(ctx, delay)
	}
}

// cappedBackoff doubles InitialBackoff per consecutive failure up to
// MaxBackoff, then adds up to 25% jitter so the redis and rabbitmq consumers
// do not redial in lockstep. failures is >= 1.
func cappedBackoff(initial, max time.Duration, failures int) time.Duration {
	d := initial
	for i := 1; i < failures && d < max; i++ {
		d *= 2
		if d > max {
			d = max
		}
	}
	if d > max {
		d = max
	}
	if d <= 0 {
		return 0
	}
	return d + time.Duration(rand.Int64N(int64(d)/4+1))
}
