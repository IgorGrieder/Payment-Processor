package app

import (
	"context"
	"fmt"

	"go.uber.org/zap"

	"payment-processor/internal/acceptance"
	"payment-processor/internal/config"
	"payment-processor/internal/database"
	"payment-processor/internal/httpserver"
	"payment-processor/internal/messaging"
	"payment-processor/internal/outbox"
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

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	store := database.NewStore(pool)
	acceptor := acceptance.New(store)
	dispatcher := outbox.NewDispatcher(store, publisher, cfg.OutboxWorkers, cfg.InstanceID, cfg.OutboxClaimExpiry, cfg.NATSPublishTimeout, logger)
	dispatcherDone := make(chan struct{})
	go func() {
		dispatcher.Run(runCtx)
		close(dispatcherDone)
	}()
	defer func() {
		cancel()
		<-dispatcherDone
	}()

	return httpserver.New(cfg.HTTPAddr, acceptor, logger).Run(runCtx)
}
