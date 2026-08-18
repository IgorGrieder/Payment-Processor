package domain

import (
	"time"

	"github.com/google/uuid"
)

// ProcessorService identifies one configured Payment Processor.
type ProcessorService string

const (
	DefaultProcessor  ProcessorService = "default"
	FallbackProcessor ProcessorService = "fallback"
)

func (s ProcessorService) Valid() bool {
	return s == DefaultProcessor || s == FallbackProcessor
}

// Payment is the stored data sent to its assigned Payment Processor.
type Payment struct {
	CorrelationID       uuid.UUID
	Amount              int64 // amount in cents
	RequestedAt         time.Time
	ProcessorAssignment ProcessorService
}

// ProcessorAvailability is the versioned shared assessment used only for new
// Payment assignments. A nil PassiveFailureAt means no passive failure has
// been recorded since this Processor's availability row was created.
type ProcessorAvailability struct {
	Service          ProcessorService
	Available        bool
	Version          int64
	PassiveFailureAt *time.Time
}

type PaymentClaimStatus uint8

const (
	PaymentClaimed PaymentClaimStatus = iota
	PaymentCompleted
	PaymentUnassigned
	PaymentNotClaimable
)

type PaymentClaim struct {
	Status  PaymentClaimStatus
	Payment Payment
}
