package availability

import (
	"context"
	"time"

	"go.uber.org/zap"

	"payment-processor/internal/database"
	"payment-processor/internal/domain"
	"payment-processor/internal/processor"
)

// Election runs the one active Processor Availability poller elected by a
// PostgreSQL lifetime advisory lock.
type Election struct {
	store         *database.Store
	availability  *Service
	processors    map[domain.ProcessorService]*processor.Client
	pollInterval  time.Duration
	retryInterval time.Duration
	logger        *zap.Logger
}

func NewElection(store *database.Store, availability *Service, processors map[domain.ProcessorService]*processor.Client, pollInterval, retryInterval time.Duration, logger *zap.Logger) *Election {
	return &Election{
		store:         store,
		availability:  availability,
		processors:    processors,
		pollInterval:  pollInterval,
		retryInterval: retryInterval,
		logger:        logger,
	}
}

// Run repeatedly tries to become the sole poller. A new lock holder polls
// immediately; a non-holder retries only at the configured interval.
func (e *Election) Run(ctx context.Context) {
	for {
		leadership, acquired, err := e.store.TryHealthLeadership(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			e.logger.Error("acquire Processor Availability leadership", zap.Error(err))
			if !wait(ctx, e.retryInterval) {
				return
			}
			continue
		}
		if !acquired {
			if !wait(ctx, e.retryInterval) {
				return
			}
			continue
		}

		e.logger.Info("acquired Processor Availability leadership")
		e.runLeader(ctx, leadership)
		closeLeadership(leadership)
		if ctx.Err() != nil {
			return
		}
		e.logger.Warn("relinquished Processor Availability leadership")
		if !wait(ctx, e.retryInterval) {
			return
		}
	}
}

func (e *Election) runLeader(ctx context.Context, leadership *database.HealthLeadership) {
	for {
		if !e.poll(ctx, leadership) {
			return
		}
		if !wait(ctx, e.pollInterval) {
			return
		}
	}
}

// poll gives each Processor an independently recorded database start time.
// Failed requests intentionally preserve the previous persisted assessment.
func (e *Election) poll(ctx context.Context, leadership *database.HealthLeadership) bool {
	for _, service := range []domain.ProcessorService{domain.DefaultProcessor, domain.FallbackProcessor} {
		if !e.hasLeadership(ctx, leadership) {
			return false
		}
		e.pollProcessor(ctx, service)
		if ctx.Err() != nil {
			return false
		}
	}
	return true
}

func (e *Election) hasLeadership(ctx context.Context, leadership *database.HealthLeadership) bool {
	if err := leadership.Alive(ctx); err != nil {
		if ctx.Err() == nil {
			e.logger.Warn("lost Processor Availability leadership", zap.Error(err))
		}
		return false
	}
	return true
}

func (e *Election) pollProcessor(ctx context.Context, service domain.ProcessorService) {
	client := e.processors[service]
	if client == nil {
		e.logger.Error("configured Processor client is missing", zap.String("service", string(service)))
		return
	}

	startedAt, err := e.store.AvailabilityPollStartedAt(ctx)
	if err != nil {
		e.logPollError(ctx, "record Processor Availability poll start", service, err)
		return
	}
	available, err := client.Availability(ctx)
	if err != nil {
		if ctx.Err() == nil {
			e.logger.Warn("poll Processor Availability", zap.String("service", string(service)), zap.Error(err))
		}
		return
	}
	accepted, err := e.availability.RecordPollObservation(ctx, service, available, startedAt)
	if err != nil {
		e.logPollError(ctx, "record Processor Availability", service, err)
		return
	}
	if !accepted {
		e.logger.Debug("ignored stale Processor Availability recovery", zap.String("service", string(service)))
	}
}

func (e *Election) logPollError(ctx context.Context, message string, service domain.ProcessorService, err error) {
	if ctx.Err() == nil {
		e.logger.Error(message, zap.String("service", string(service)), zap.Error(err))
	}
}

func closeLeadership(leadership *database.HealthLeadership) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	leadership.Close(ctx)
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
