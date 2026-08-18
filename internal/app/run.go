package app

import (
	"context"
	"fmt"

	"go.uber.org/zap"

	"payment-processor/internal/acceptance"
	"payment-processor/internal/availability"
	"payment-processor/internal/config"
	"payment-processor/internal/database"
	"payment-processor/internal/domain"
	"payment-processor/internal/httpserver"
	"payment-processor/internal/messaging"
	"payment-processor/internal/outbox"
	"payment-processor/internal/processing"
	"payment-processor/internal/processor"
)

// Run composes the process dependencies and runs its HTTP server.
func Run(ctx context.Context, logger *zap.Logger) error {
	cfg, err := config.FromEnv()
	if err != nil {
		return fmt.Errorf("load configuration: %w", err)
	}

	pool, err := database.Open(ctx, cfg.DatabaseURL, cfg.DatabaseMaxConns)
	if err != nil {
		return fmt.Errorf("connect PostgreSQL: %w", err)
	}
	defer pool.Close()

	natsConn, err := messaging.Connect(cfg.NATSURL)
	if err != nil {
		return fmt.Errorf("connect NATS: %w", err)
	}
	defer natsConn.Drain()

	publisher, err := messaging.ProvisionWorkStream(natsConn, cfg.JetStreamStream, cfg.JetStreamSubject, cfg.JetStreamDuplicateWindow)
	if err != nil {
		return fmt.Errorf("provision JetStream work stream: %w", err)
	}
	consumer, err := messaging.ProvisionWorkConsumer(natsConn, cfg.JetStreamStream, cfg.JetStreamSubject, cfg.JetStreamAckWait, cfg.JetStreamMaxAckPending)
	if err != nil {
		return fmt.Errorf("provision JetStream work consumer: %w", err)
	}

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	store := database.NewStore(pool)
	availabilityState := availability.New(store, availability.Broadcaster(messaging.NewProcessorAvailabilityPublisher(natsConn, cfg.ProcessorAvailabilitySubject)))
	unsubscribeAvailability, err := messaging.SubscribeProcessorAvailability(natsConn, cfg.ProcessorAvailabilitySubject, availabilityState.Apply)
	if err != nil {
		return fmt.Errorf("subscribe to Processor Availability updates: %w", err)
	}
	if err := availabilityState.Hydrate(runCtx); err != nil {
		unsubscribeAvailability()
		return fmt.Errorf("hydrate Processor Availability: %w", err)
	}
	defer unsubscribeAvailability()

	processors := map[domain.ProcessorService]*processor.Client{
		domain.DefaultProcessor:  processor.New(domain.DefaultProcessor, cfg.ProcessorDefaultURL, cfg.ProcessorTimeout),
		domain.FallbackProcessor: processor.New(domain.FallbackProcessor, cfg.ProcessorFallbackURL, cfg.ProcessorTimeout),
	}
	acceptor := acceptance.New(store)
	dispatcher := outbox.NewDispatcher(store, publisher, cfg.OutboxWorkers, cfg.InstanceID, cfg.OutboxClaimExpiry, cfg.NATSPublishTimeout, logger)
	workers := processing.NewWorkerPool(
		store,
		consumer,
		availabilityState,
		processors,
		cfg.ProcessingWorkers,
		cfg.InstanceID,
		cfg.PaymentClaimExpiry,
		logger,
	)
	election := availability.NewElection(store, availabilityState, processors, cfg.ProcessorPollInterval, cfg.HealthElectionRetryInterval, logger)
	dispatcherDone := make(chan struct{})
	workersDone := make(chan struct{})
	electionDone := make(chan struct{})
	go func() {
		dispatcher.Run(runCtx)
		close(dispatcherDone)
	}()
	go func() {
		workers.Run(runCtx)
		close(workersDone)
	}()
	go func() {
		election.Run(runCtx)
		close(electionDone)
	}()
	defer func() {
		cancel()
		<-dispatcherDone
		<-workersDone
		<-electionDone
	}()

	return httpserver.New(cfg.HTTPAddr, acceptor, logger).Run(runCtx)
}
