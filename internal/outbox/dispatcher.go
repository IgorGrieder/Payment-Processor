package outbox

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/nats-io/nuid"
	"go.uber.org/zap"

	"payment-processor/internal/database"
	"payment-processor/internal/messaging"
)

const idlePollInterval = 100 * time.Millisecond
const failureRetryInterval = time.Second

// Dispatcher claims durable outbox control rows and publishes their Payment
// references to JetStream. It deliberately uses concrete infrastructure at the
// PostgreSQL and JetStream seams.
type Dispatcher struct {
	store          *database.Store
	publisher      messaging.WorkPublisher
	workers        int
	instanceID     string
	claimExpiry    time.Duration
	publishTimeout time.Duration
	logger         *zap.Logger
}

func NewDispatcher(store *database.Store, publisher messaging.WorkPublisher, workers int, instanceID string, claimExpiry, publishTimeout time.Duration, logger *zap.Logger) *Dispatcher {
	return &Dispatcher{
		store:          store,
		publisher:      publisher,
		workers:        workers,
		instanceID:     instanceID,
		claimExpiry:    claimExpiry,
		publishTimeout: publishTimeout,
		logger:         logger,
	}
}

// Run starts a bounded number of dispatcher workers and returns when ctx is
// cancelled. Each worker has a unique persisted claim owner, so a stale worker
// cannot mark a lease reclaimed by another worker as dispatched.
func (d *Dispatcher) Run(ctx context.Context) {
	var workers sync.WaitGroup
	workers.Add(d.workers)
	for worker := range d.workers {
		go func(worker int) {
			defer workers.Done()
			d.runWorker(ctx, fmt.Sprintf("%s-%d-%s", d.instanceID, worker, nuid.Next()))
		}(worker)
	}
	workers.Wait()
}

func (d *Dispatcher) runWorker(ctx context.Context, claimOwner string) {
	for {
		correlationID, claimed, err := d.store.ClaimOutbox(ctx, claimOwner, d.claimExpiry)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			d.logger.Error("claim outbox Payment", zap.Error(err))
			if !wait(ctx, failureRetryInterval) {
				return
			}
			continue
		}
		if !claimed {
			if !wait(ctx, idlePollInterval) {
				return
			}
			continue
		}

		publishCtx, cancel := context.WithTimeout(ctx, d.publishTimeout)
		err = d.publisher.Publish(publishCtx, correlationID)
		cancel()
		if err != nil {
			d.logger.Warn("publish outbox Payment", zap.Error(err), zap.String("correlation_id", correlationID.String()))
			if releaseErr := d.store.ReleaseOutbox(ctx, correlationID, claimOwner); releaseErr != nil && ctx.Err() == nil {
				d.logger.Error("release unpublished outbox Payment", zap.Error(releaseErr), zap.String("correlation_id", correlationID.String()))
			}
			continue
		}

		dispatched, err := d.store.MarkOutboxDispatched(ctx, correlationID, claimOwner)
		if err != nil {
			d.logger.Error("record dispatched outbox Payment", zap.Error(err), zap.String("correlation_id", correlationID.String()))
			continue
		}
		if !dispatched {
			d.logger.Warn("outbox Payment claim expired before dispatch record", zap.String("correlation_id", correlationID.String()))
		}
	}
}

func wait(ctx context.Context, duration time.Duration) bool {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
