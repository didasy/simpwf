package executor

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

// ErrHTTPOverloaded is returned when an outbound HTTP call cannot obtain a
// bulkhead slot before its context expires. It sheds load with a routable
// error instead of hanging to the node timeout.
var ErrHTTPOverloaded = errors.New("executor: http bulkhead saturated")

const (
	// DefaultHTTPMaxInFlight caps total process-wide outbound HTTP
	// in-flight. Fleet math: 10 replicas x 16/host = 160/host fleet-wide;
	// the global backstop binds the 1000-worker default pools.
	DefaultHTTPMaxInFlight = 128
	// DefaultHTTPMaxInFlightPerHost caps outbound HTTP in-flight per host.
	DefaultHTTPMaxInFlightPerHost = 16
	// maxHTTPHostBuckets bounds the lazy per-host bucket map. Beyond this
	// many distinct hosts, callers share one overflow bucket, so memory
	// stays bounded even under allowlist "*".
	maxHTTPHostBuckets = 4096
)

// HTTPBulkhead bounds outbound HTTP concurrency with a process-wide global
// semaphore plus one semaphore per host. The zero value is not usable;
// build one with NewHTTPBulkhead or DefaultHTTPBulkhead.
type HTTPBulkhead struct {
	global  chan struct{}
	perHost int
	mu      sync.Mutex
	hosts   map[string]chan struct{}
	// overflow is the shared per-host bucket used once hosts reaches
	// maxHTTPHostBuckets.
	overflow chan struct{}
}

// NewHTTPBulkhead builds a bulkhead with the given caps. Non-positive caps
// fall back to the defaults.
func NewHTTPBulkhead(global, perHost int) *HTTPBulkhead {
	if global <= 0 {
		global = DefaultHTTPMaxInFlight
	}
	if perHost <= 0 {
		perHost = DefaultHTTPMaxInFlightPerHost
	}
	return &HTTPBulkhead{
		global:   make(chan struct{}, global),
		perHost:  perHost,
		hosts:    make(map[string]chan struct{}),
		overflow: make(chan struct{}, perHost),
	}
}

// DefaultHTTPBulkhead builds a bulkhead with the default caps (128 global,
// 16 per host).
func DefaultHTTPBulkhead() *HTTPBulkhead {
	return NewHTTPBulkhead(DefaultHTTPMaxInFlight, DefaultHTTPMaxInFlightPerHost)
}

// Acquire takes one global slot and one per-host slot for host, in that
// fixed order, waiting until ctx expires. Host buckets are created lazily;
// past maxHTTPHostBuckets distinct hosts, callers share the overflow bucket.
// On success it returns an idempotent release func safe to defer. On
// ctx-expiry it returns an error matching ErrHTTPOverloaded via errors.Is.
func (b *HTTPBulkhead) Acquire(ctx context.Context, host string) (func(), error) {
	select {
	case b.global <- struct{}{}:
	case <-ctx.Done():
		return nil, fmt.Errorf("%w: %s", ErrHTTPOverloaded, ctx.Err())
	}
	bucket := b.bucketFor(host)
	select {
	case bucket <- struct{}{}:
	case <-ctx.Done():
		<-b.global
		return nil, fmt.Errorf("%w: %s", ErrHTTPOverloaded, ctx.Err())
	}
	var once sync.Once
	return func() {
		once.Do(func() {
			<-bucket
			<-b.global
		})
	}, nil
}

// bucketFor returns the per-host bucket for host, creating it lazily. Once
// the map holds maxHTTPHostBuckets entries, every further host shares the
// overflow bucket.
func (b *HTTPBulkhead) bucketFor(host string) chan struct{} {
	host = strings.ToLower(host)
	b.mu.Lock()
	defer b.mu.Unlock()
	if ch, ok := b.hosts[host]; ok {
		return ch
	}
	if len(b.hosts) >= maxHTTPHostBuckets {
		return b.overflow
	}
	ch := make(chan struct{}, b.perHost)
	b.hosts[host] = ch
	return ch
}

// parseRetryAfter parses a Retry-After header value: either delay-seconds
// or an HTTP date. It reports false for empty, negative, and unparseable
// values. A date in the past yields a zero delay with true.
func parseRetryAfter(header string) (time.Duration, bool) {
	s := strings.TrimSpace(header)
	if s == "" {
		return 0, false
	}
	if secs, err := strconv.Atoi(s); err == nil {
		if secs < 0 {
			return 0, false
		}
		return time.Duration(secs) * time.Second, true
	}
	if t, err := http.ParseTime(s); err == nil {
		d := time.Until(t)
		if d < 0 {
			d = 0
		}
		return d, true
	}
	return 0, false
}
