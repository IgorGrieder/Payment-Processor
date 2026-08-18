// Package processing owns the bounded lifecycle from a JetStream work
// reference through durable Payment completion.
package processing

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/nats-io/nuid"
	"go.uber.org/zap"

	"payment-processor/internal/database"
	"payment-processor/internal/domain"
	"payment-processor/internal/messaging"
	"payment-processor/internal/processor"
)

const failureRetryInterval = time.Second

// WorkerPool processes Payment work with a fixed number of workers.
type WorkerPool struct {
	store       *database.Store
	consumer    messaging.WorkConsumer
	processor   *processor.Default
	workers     int
	instanceID  string
	claimExpiry time.Duration
	timeout     time.Duration
	logger      *zap.Logger
}

func NewWorkerPool(store *database.Store, consumer messaging.WorkConsumer, processor *processor.Default, workers int, instanceID string, claimExpiry, timeout time.Duration, logger *zap.Logger) *WorkerPool {
	return &WorkerPool{
		store:       store,
		consumer:    consumer,
		processor:   processor,
		workers:     workers,
		instanceID:  instanceID,
		claimExpiry: claimExpiry,
		timeout:     timeout,
		logger:      logger,
	}
}

// Run starts the fixed worker pool and returns after all workers stop.
func (p *WorkerPool) Run(ctx context.Context) {
	var workers sync.WaitGroup
	workers.Add(p.workers)
	for worker := range p.workers {
		go func(worker int) {
			defer workers.Done()
			p.runWorker(ctx, fmt.Sprintf("%s-processing-%d-%s", p.instanceID, worker, nuid.Next()))
		}(worker)
	}
	workers.Wait()
}

func (p *WorkerPool) runWorker(ctx context.Context, claimOwner string) {
	for {
		delivery, err := p.consumer.Next(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			if errors.Is(err, context.DeadlineExceeded) {
				continue
			}
			p.logger.Error("receive Payment work", zap.Error(err))
			if !wait(ctx, failureRetryInterval) {
				return
			}
			continue
		}

		correlationID, err := delivery.CorrelationID()
		if err != nil {
			p.logger.Error("read Payment work reference", zap.Error(err))
			continue
		}

		claim, err := p.store.ClaimPayment(ctx, correlationID, claimOwner, p.claimExpiry)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			p.logger.Error("claim Payment", zap.Error(err), zap.String("correlation_id", correlationID.String()))
			continue
		}
		switch claim.Status {
		case domain.PaymentCompleted:
			p.ack(ctx, delivery, correlationID)
			continue
		case domain.PaymentNotClaimable:
			// Lease-expiry recovery is intentionally deferred to Slice 3.
			p.logger.Debug("Payment is not claimable", zap.String("correlation_id", correlationID.String()))
			continue
		}

		p.logger.Debug("claimed Payment", zap.String("correlation_id", correlationID.String()))
		processorCtx, cancel := context.WithTimeout(ctx, p.timeout)
		err = p.processor.Process(processorCtx, claim.Payment)
		cancel()
		if err != nil {
			p.logger.Error("process Payment with Default Processor", zap.Error(err), zap.String("correlation_id", correlationID.String()))
			continue
		}

		completed, err := p.store.CompletePayment(ctx, correlationID, claimOwner, p.instanceID)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			p.logger.Error("record completed Payment", zap.Error(err), zap.String("correlation_id", correlationID.String()))
			continue
		}
		if !completed {
			p.logger.Warn("Payment claim was lost before completion", zap.String("correlation_id", correlationID.String()))
			continue
		}

		p.logger.Debug("completed Payment", zap.String("correlation_id", correlationID.String()))
		p.ack(ctx, delivery, correlationID)
	}
}

func (p *WorkerPool) ack(ctx context.Context, delivery messaging.WorkDelivery, correlationID uuid.UUID) {
	if err := delivery.Ack(ctx); err != nil && ctx.Err() == nil {
		p.logger.Error("acknowledge completed Payment work", zap.Error(err), zap.String("correlation_id", correlationID.String()))
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
