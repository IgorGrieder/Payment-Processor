package domain

import (
	"time"

	"github.com/google/uuid"
)

// Payment is the stored data sent to an assigned Payment Processor.
type Payment struct {
	CorrelationID uuid.UUID
	Amount        int64 // amount in cents
	RequestedAt   time.Time
}

type PaymentClaimStatus uint8

const (
	PaymentClaimed PaymentClaimStatus = iota
	PaymentCompleted
	PaymentNotClaimable
)

type PaymentClaim struct {
	Status  PaymentClaimStatus
	Payment Payment
}
