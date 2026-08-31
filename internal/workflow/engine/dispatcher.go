package engine

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/panjf2000/ants/v2"
	"github.com/simpwf/workflow-engine/internal/workflow/repository"
)

// DispatcherOptions tunes the claim loop. Zero values get defaults.
type DispatcherOptions struct {
	PollInterval time.Duration
	Lease        time.Duration
	BatchSize    int
	PoolSize     int
	Heartbeat    time.Duration
}

// Dispatcher leases runnable instances with FOR UPDATE SKIP LOCKED claims
// and executes one node transition per claim through an ants pool, renewing
// its leases on a heartbeat while it lives.
type Dispatcher struct {
	engine    *Engine
	instances repository.InstanceRepository
	workerID  string
	pool      *ants.Pool

	pollInterval time.Duration
	lease        time.Duration
	batchSize    int
	heartbeat    time.Duration

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewDispatcher builds a dispatcher; it is inert until Run is called.
func NewDispatcher(ctx context.Context, e *Engine, instances repository.InstanceRepository, workerID string, opts DispatcherOptions) (*Dispatcher, error) {
	if opts.PollInterval <= 0 {
		opts.PollInterval = 200 * time.Millisecond
	}
	if opts.Lease <= 0 {
		opts.Lease = 30 * time.Second
	}
	if opts.BatchSize <= 0 {
		opts.BatchSize = 10
	}
	if opts.PoolSize <= 0 {
		opts.PoolSize = 20
	}
	if opts.Heartbeat <= 0 {
		opts.Heartbeat = 5 * time.Second
	}
	pool, err := ants.NewPool(opts.PoolSize, ants.WithNonblocking(false))
	if err != nil {
		return nil, err
	}
	runCtx, cancel := context.WithCancel(ctx)
	return &Dispatcher{
		engine:       e,
		instances:    instances,
		workerID:     workerID,
		pool:         pool,
		pollInterval: opts.PollInterval,
		lease:        opts.Lease,
		batchSize:    opts.BatchSize,
		heartbeat:    opts.Heartbeat,
		ctx:          runCtx,
		cancel:       cancel,
	}, nil
}

// Run starts the claim and heartbeat loops. It returns immediately.
func (d *Dispatcher) Run() {
	d.wg.Add(2)
	go d.claimLoop()
	go d.heartbeatLoop()
}

func (d *Dispatcher) claimLoop() {
	defer d.wg.Done()
	ticker := time.NewTicker(d.pollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-d.ctx.Done():
			return
		case <-ticker.C:
		}
		claimed, err := d.instances.ClaimNext(d.ctx, d.workerID, d.lease, d.batchSize)
		if err != nil {
			if d.ctx.Err() == nil {
				slog.Warn("dispatcher claim failed", "worker", d.workerID, "error", err)
			}
			continue
		}
		for _, w := range claimed {
			w := w
			err := d.pool.Submit(func() {
				if err := d.engine.Process(d.ctx, w); err != nil && d.ctx.Err() == nil {
					slog.Warn("dispatcher process failed", "worker", d.workerID, "instance", w.ID, "error", err)
				}
			})
			if err != nil {
				// Pool closed (shutdown): run the transition inline rather
				// than dropping the claim.
				if err := d.engine.Process(d.ctx, w); err != nil && d.ctx.Err() == nil {
					slog.Warn("dispatcher inline process failed", "worker", d.workerID, "instance", w.ID, "error", err)
				}
			}
		}
	}
}

func (d *Dispatcher) heartbeatLoop() {
	defer d.wg.Done()
	ticker := time.NewTicker(d.heartbeat)
	defer ticker.Stop()
	for {
		select {
		case <-d.ctx.Done():
			return
		case <-ticker.C:
		}
		if err := d.instances.RenewLeases(d.ctx, d.workerID, d.lease); err != nil && d.ctx.Err() == nil {
			slog.Warn("dispatcher heartbeat failed", "worker", d.workerID, "error", err)
		}
		d.pollTermination()
	}
}

// pollTermination propagates stops across replicas: any instance marked
// termination-pending is cancelled locally (no-op when it runs elsewhere),
// and pending flags whose node attempts already finished are swept.
func (d *Dispatcher) pollTermination() {
	pending, err := d.instances.ListTerminationPending(d.ctx)
	if err != nil {
		if d.ctx.Err() == nil {
			slog.Warn("dispatcher termination poll failed", "worker", d.workerID, "error", err)
		}
		return
	}
	for _, id := range pending {
		d.engine.Cancel(id)
	}
	if err := d.instances.SweepTermination(d.ctx); err != nil && d.ctx.Err() == nil {
		slog.Warn("dispatcher termination sweep failed", "worker", d.workerID, "error", err)
	}
}

// Shutdown stops claiming and heartbeat, then waits for in-flight
// transitions to finish, honoring the caller's deadline.
func (d *Dispatcher) Shutdown(ctx context.Context) error {
	d.cancel()
	done := make(chan struct{})
	go func() {
		d.wg.Wait()
		d.pool.Release()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
