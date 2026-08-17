package acceptance

import (
	"context"

	"github.com/google/uuid"

	"payment-processor/internal/database"
)

// Service accepts Payments durably before any asynchronous dispatch.
type Service struct {
	store *database.Store
}

func New(store *database.Store) *Service {
	return &Service{store: store}
}

func (s *Service) Accept(ctx context.Context, correlationID uuid.UUID, amountCents int64) error {
	return s.store.Accept(ctx, correlationID, amountCents)
}
