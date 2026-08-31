package statusupdate

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/panjf2000/ants/v2"
	"github.com/simpwf/workflow-engine/internal/workflow/model"
	"github.com/simpwf/workflow-engine/internal/workflow/repository"
)

// ConfigLoader resolves the immutable status_update configuration of a
// workflow definition.
type ConfigLoader func(ctx context.Context, definitionID string) (*model.StatusUpdateConfig, error)

// Publisher delivers a claimed event to its transport. Implementations must
// return an error on any non-successful delivery so the dispatcher can
// schedule a retry or dead-letter the event; the transport's retry policy
// comes from the matching block of the status_update configuration.
type Publisher interface {
	Publish(ctx context.Context, cfg *model.StatusUpdateConfig, ev repository.PendingStatusUpdate) error
}

// DispatcherOptions tunes the outbox claim loop. Zero values get defaults.
type DispatcherOptions struct {
	PollInterval time.Duration
	Lease        time.Duration
	BatchSize    int
	PoolSize     int
}

// Dispatcher claims ready status-update events from the transactional outbox
// and delivers them through the publisher of the event's transport.
// Ordering is strict per workflow instance and transport: the claim query
// only returns the oldest unresolved event of an instance/transport pair, so
// a later event never overtakes an earlier one on the same transport.
type Dispatcher struct {
	repo       repository.StatusUpdateRepository
	loader     ConfigLoader
	publishers map[string]Publisher
	workerID   string
	pool       *ants.Pool

	pollInterval time.Duration
	lease        time.Duration
	batchSize    int

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewDispatcher builds a dispatcher; it is inert until Run is called.
// publishers maps a transport name ("http", "redis", "rabbitmq") to the
// publisher that delivers that transport's events.
func NewDispatcher(ctx context.Context, repo repository.StatusUpdateRepository, loader ConfigLoader, publishers map[string]Publisher, workerID string, opts DispatcherOptions) (*Dispatcher, error) {
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
	pool, err := ants.NewPool(opts.PoolSize, ants.WithNonblocking(false))
	if err != nil {
		return nil, err
	}
	runCtx, cancel := context.WithCancel(ctx)
	return &Dispatcher{
		repo:         repo,
		loader:       loader,
		publishers:   publishers,
		workerID:     workerID,
		pool:         pool,
		pollInterval: opts.PollInterval,
		lease:        opts.Lease,
		batchSize:    opts.BatchSize,
		ctx:          runCtx,
		cancel:       cancel,
	}, nil
}

// Run starts the claim loop. It returns immediately.
func (d *Dispatcher) Run() {
	d.wg.Add(1)
	go d.claimLoop()
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
		claimed, err := d.repo.ClaimNextStatusUpdates(d.ctx, d.workerID, d.lease, d.batchSize)
		if err != nil {
			if d.ctx.Err() == nil {
				slog.Warn("status update claim failed", "worker", d.workerID, "error", err)
			}
			continue
		}
		for _, ev := range claimed {
			ev := ev
			err := d.pool.Submit(func() {
				if err := d.deliver(ev); err != nil && d.ctx.Err() == nil {
					slog.Warn("status update delivery failed", "worker", d.workerID, "event", ev.ID, "error", err)
				}
			})
			if err != nil {
				// Pool closed (shutdown): deliver inline rather than
				// dropping the claim.
				if err := d.deliver(ev); err != nil && d.ctx.Err() == nil {
					slog.Warn("status update inline delivery failed", "worker", d.workerID, "event", ev.ID, "error", err)
				}
			}
		}
	}
}

// deliver publishes a claimed event through its transport's publisher and
// resolves the outbox row: delivered on success, retried (or dead-lettered
// past max_retry) on failure using the per-transport retry policy.
func (d *Dispatcher) deliver(ev repository.PendingStatusUpdate) error {
	cfg, err := d.loader(d.ctx, ev.WorkflowDefinitionID)
	if err != nil {
		// The definition is missing or unreadable; it will never recover,
		// so dead-letter immediately to unblock later events.
		return d.repo.FailStatusUpdate(d.ctx, ev.ID, d.workerID, 0, 0, "load status_update config: "+err.Error())
	}
	if cfg == nil {
		return d.repo.FailStatusUpdate(d.ctx, ev.ID, d.workerID, 0, 0, "status_update not configured")
	}
	publisher, ok := d.publishers[ev.Transport]
	if !ok {
		return d.repo.FailStatusUpdate(d.ctx, ev.ID, d.workerID, 0, 0, "unknown status transport "+ev.Transport)
	}
	maxRetry, retryDelay, ok := cfg.RetryPolicy(ev.Transport)
	if !ok {
		return d.repo.FailStatusUpdate(d.ctx, ev.ID, d.workerID, 0, 0, "status_update transport "+ev.Transport+" not configured")
	}
	if err := publisher.Publish(d.ctx, cfg, ev); err != nil {
		return d.repo.FailStatusUpdate(d.ctx, ev.ID, d.workerID, retryDelay, maxRetry, err.Error())
	}
	return d.repo.MarkStatusUpdateDelivered(d.ctx, ev.ID, d.workerID)
}

// Shutdown stops claiming and waits for in-flight deliveries, honoring the
// caller's deadline.
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
