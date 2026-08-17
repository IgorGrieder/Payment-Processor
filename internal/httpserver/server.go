package httpserver

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"go.uber.org/zap"

	"payment-processor/internal/intake"
)

const shutdownTimeout = 10 * time.Second

// Server owns the HTTP routes and HTTP-server lifecycle.
type Server struct {
	server *http.Server
	logger *zap.Logger
}

func New(addr string, acceptor intake.PaymentAccepter, logger *zap.Logger) *Server {
	mux := http.NewServeMux()
	mux.Handle("POST /payments", intake.NewHandler(acceptor, logger))
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	return &Server{
		server: &http.Server{Addr: addr, Handler: mux},
		logger: logger,
	}
}

// Run serves requests until ctx is cancelled or the server stops unexpectedly.
func (s *Server) Run(ctx context.Context) error {
	serverErrors := make(chan error, 1)
	go func() {
		s.logger.Info("payment-processor listening", zap.String("address", s.server.Addr))
		serverErrors <- s.server.ListenAndServe()
	}()

	select {
	case err := <-serverErrors:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return fmt.Errorf("serve HTTP: %w", err)
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
		defer cancel()
		if err := s.server.Shutdown(shutdownCtx); err != nil {
			return fmt.Errorf("shutdown HTTP server: %w", err)
		}
		if err := <-serverErrors; !errors.Is(err, http.ErrServerClosed) {
			return fmt.Errorf("serve HTTP during shutdown: %w", err)
		}
		return nil
	}
}
