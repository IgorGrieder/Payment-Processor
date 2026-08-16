package domain

import (
	"time"

	"github.com/google/uuid"
)

type Payment struct {
	CorrelationID uuid.UUID
	Amount        int64 // amount in cents
	RequestedAt   time.Time
}
