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

	"payment-processor/internal/availability"
	"payment-processor/internal/database"
	"payment-processor/internal/domain"
	"payment-processor/internal/messaging"
	"payment-processor/internal/processor"
)

const (
	failureRetryInterval = time.Second
	processingRetryDelay = 6 * time.Second
)

// WorkerPool processes Payment work with a fixed number of workers.
type WorkerPool struct {
	store        *database.Store
	consumer     messaging.WorkConsumer
	availability *availability.Service
	processors   map[domain.ProcessorService]*processor.Client
	workers      int
	instanceID   string
	claimExpiry  time.Duration
	logger       *zap.Logger
}

func NewWorkerPool(store *database.Store, consumer messaging.WorkConsumer, availability *availability.Service, processors map[domain.ProcessorService]*processor.Client, workers int, instanceID string, claimExpiry time.Duration, logger *zap.Logger) *WorkerPool {
	return &WorkerPool{
		store:        store,
		consumer:     consumer,
		availability: availability,
		processors:   processors,
		workers:      workers,
		instanceID:   instanceID,
		claimExpiry:  claimExpiry,
		logger:       logger,
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
			p.nak(ctx, delivery, uuid.Nil)
			continue
		}

		selected, selectable := p.availability.Select()
		var selectedService *domain.ProcessorService
		if selectable {
			selectedService = &selected
		}
		claim, err := p.store.ClaimPayment(ctx, correlationID, claimOwner, p.claimExpiry, selectedService)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			p.logger.Error("claim Payment", zap.Error(err), zap.String("correlation_id", correlationID.String()))
			p.nak(ctx, delivery, correlationID)
			continue
		}
		switch claim.Status {
		case domain.PaymentCompleted:
			p.ack(ctx, delivery, correlationID)
			continue
		case domain.PaymentUnassigned:
			p.logger.Debug("no Processor is selectable for unassigned Payment", zap.String("correlation_id", correlationID.String()))
			p.nak(ctx, delivery, correlationID)
			continue
		case domain.PaymentNotClaimable:
			p.logger.Debug("Payment is not claimable", zap.String("correlation_id", correlationID.String()))
			p.nak(ctx, delivery, correlationID)
			continue
		}

		client := p.processors[claim.Payment.ProcessorAssignment]
		if client == nil {
			p.logger.Error("Payment has no configured assigned Processor", zap.String("correlation_id", correlationID.String()), zap.String("service", string(claim.Payment.ProcessorAssignment)))
			p.releaseAndNak(ctx, delivery, correlationID, claimOwner)
			continue
		}

		p.logger.Debug("claimed Payment", zap.String("correlation_id", correlationID.String()), zap.String("service", string(claim.Payment.ProcessorAssignment)))
		result, err := client.Process(ctx, claim.Payment)
		if result == processor.ProcessUnavailable {
			if availabilityErr := p.availability.RecordPassiveFailure(ctx, claim.Payment.ProcessorAssignment); availabilityErr != nil && ctx.Err() == nil {
				p.logger.Error("record passive Processor failure", zap.Error(availabilityErr), zap.String("service", string(claim.Payment.ProcessorAssignment)))
			}
		}
		if err != nil || result != processor.ProcessConfirmed {
			if err != nil {
				p.logger.Error("process Payment", zap.Error(err), zap.String("correlation_id", correlationID.String()), zap.String("service", string(claim.Payment.ProcessorAssignment)))
			} else {
				p.logger.Error("Processor returned an unconfirmed result", zap.String("correlation_id", correlationID.String()), zap.String("service", string(claim.Payment.ProcessorAssignment)))
			}
			p.releaseAndNak(ctx, delivery, correlationID, claimOwner)
			continue
		}

		completed, err := p.store.CompletePayment(ctx, correlationID, claimOwner, p.instanceID, claim.Payment.ProcessorAssignment)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			p.logger.Error("record completed Payment", zap.Error(err), zap.String("correlation_id", correlationID.String()))
			p.nak(ctx, delivery, correlationID)
			continue
		}
		if !completed {
			p.logger.Warn("Payment claim was lost before completion", zap.String("correlation_id", correlationID.String()))
			p.nak(ctx, delivery, correlationID)
			continue
		}

		p.logger.Debug("completed Payment", zap.String("correlation_id", correlationID.String()), zap.String("service", string(claim.Payment.ProcessorAssignment)))
		p.ack(ctx, delivery, correlationID)
	}
}

func (p *WorkerPool) releaseAndNak(ctx context.Context, delivery messaging.WorkDelivery, correlationID uuid.UUID, claimOwner string) {
	released, err := p.store.ReleasePayment(ctx, correlationID, claimOwner)
	if err != nil {
		if ctx.Err() != nil {
			return
		}
		p.logger.Error("release retryable Payment claim", zap.Error(err), zap.String("correlation_id", correlationID.String()))
	} else if !released {
		p.logger.Warn("Payment claim was lost before release", zap.String("correlation_id", correlationID.String()))
	}
	p.nak(ctx, delivery, correlationID)
}

func (p *WorkerPool) ack(ctx context.Context, delivery messaging.WorkDelivery, correlationID uuid.UUID) {
	if err := delivery.Ack(ctx); err != nil && ctx.Err() == nil {
		p.logger.Error("acknowledge completed Payment work", zap.Error(err), zap.String("correlation_id", correlationID.String()))
	}
}

func (p *WorkerPool) nak(ctx context.Context, delivery messaging.WorkDelivery, correlationID uuid.UUID) {
	if ctx.Err() != nil {
		return
	}
	if err := delivery.Nak(processingRetryDelay); err != nil && ctx.Err() == nil {
		p.logger.Error("delay Payment work retry", zap.Error(err), zap.String("correlation_id", correlationID.String()))
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
