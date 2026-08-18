package database

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"payment-processor/internal/domain"
)

// Store owns PostgreSQL persistence for accepted Payments and outbox control.
type Store struct {
	pool *pgxpool.Pool
}

func Open(ctx context.Context, connectionURL string, maxConns int) (*pgxpool.Pool, error) {
	config, err := pgxpool.ParseConfig(connectionURL)
	if err != nil {
		return nil, fmt.Errorf("parse database configuration: %w", err)
	}
	config.MaxConns = int32(maxConns)

	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, err
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return pool, nil
}

func NewStore(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}

// Accept atomically records a new Payment and its pending outbox row. A
// correlation-ID conflict inserts neither row and is still successful.
func (s *Store) Accept(ctx context.Context, correlationID uuid.UUID, amountCents int64) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin payment acceptance: %w", err)
	}
	defer tx.Rollback(ctx)

	_, err = tx.Exec(ctx, `
		WITH inserted_payment AS (
			INSERT INTO payments (correlation_id, amount)
			VALUES ($1, $2)
			ON CONFLICT (correlation_id) DO NOTHING
			RETURNING correlation_id
		)
		INSERT INTO payment_outbox (correlation_id, state)
		SELECT correlation_id, 'pending' FROM inserted_payment`, correlationID, amountCents)
	if err != nil {
		return fmt.Errorf("insert accepted payment: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit payment acceptance: %w", err)
	}
	return nil
}

// ClaimOutbox claims one pending or expired outbox row and commits that claim
// before the caller publishes it.
func (s *Store) ClaimOutbox(ctx context.Context, claimedBy string, expiry time.Duration) (uuid.UUID, bool, error) {
	var correlationID uuid.UUID
	err := s.pool.QueryRow(ctx, `
		WITH candidate AS (
			SELECT correlation_id
			FROM payment_outbox
			WHERE state = 'pending'
				OR (state = 'dispatching' AND claim_expires_at <= now())
			ORDER BY correlation_id
			FOR UPDATE SKIP LOCKED
			LIMIT 1
		)
		UPDATE payment_outbox AS outbox
		SET state = 'dispatching',
			claimed_by = $1,
			claim_expires_at = now() + ($2 * interval '1 microsecond')
		FROM candidate
		WHERE outbox.correlation_id = candidate.correlation_id
		RETURNING outbox.correlation_id`, claimedBy, expiry.Microseconds()).Scan(&correlationID)
	if errors.Is(err, pgx.ErrNoRows) {
		return uuid.Nil, false, nil
	}
	if err != nil {
		return uuid.Nil, false, fmt.Errorf("claim outbox row: %w", err)
	}
	return correlationID, true, nil
}

// ReleaseOutbox makes a claimed row available after publication fails.
func (s *Store) ReleaseOutbox(ctx context.Context, correlationID uuid.UUID, claimedBy string) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE payment_outbox
		SET state = 'pending', claimed_by = NULL, claim_expires_at = NULL
		WHERE correlation_id = $1 AND state = 'dispatching' AND claimed_by = $2`, correlationID, claimedBy)
	if err != nil {
		return fmt.Errorf("release outbox row: %w", err)
	}
	return nil
}

// ClaimPayment leases a pending Payment or reclaims an expired processing
// lease. A prior Processor Assignment is retained so an ambiguous attempt
// cannot be routed differently on retry. Completed Payments are identified so
// their JetStream messages can be acknowledged without another Processor call.
func (s *Store) ClaimPayment(ctx context.Context, correlationID uuid.UUID, claimOwner string, expiry time.Duration) (domain.PaymentClaim, error) {
	var status string
	var payment domain.Payment
	err := s.pool.QueryRow(ctx, `
		WITH candidate AS (
			SELECT correlation_id, amount, requested_at, processing_state,
				processing_claim_expires_at
			FROM payments
			WHERE correlation_id = $1
			FOR UPDATE
		), claimed AS (
			UPDATE payments AS payment
			SET processing_state = 'processing',
				processor_assignment = COALESCE(payment.processor_assignment, 'default'),
				processing_claimed_by = $2,
				processing_claim_expires_at = now() + ($3 * interval '1 microsecond')
			FROM candidate
			WHERE payment.correlation_id = candidate.correlation_id
				AND (
					candidate.processing_state = 'pending'
					OR (
						candidate.processing_state = 'processing'
						AND candidate.processing_claim_expires_at <= now()
					)
				)
			RETURNING payment.correlation_id, payment.amount, payment.requested_at
		)
		SELECT 'claimed', correlation_id, amount, requested_at
		FROM claimed
		UNION ALL
		SELECT CASE processing_state
			WHEN 'completed' THEN 'completed'
			ELSE 'not_claimable'
		END, correlation_id, amount, requested_at
		FROM candidate
		WHERE NOT EXISTS (SELECT 1 FROM claimed)`, correlationID, claimOwner, expiry.Microseconds()).Scan(
		&status, &payment.CorrelationID, &payment.Amount, &payment.RequestedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.PaymentClaim{Status: domain.PaymentNotClaimable}, nil
	}
	if err != nil {
		return domain.PaymentClaim{}, fmt.Errorf("claim Payment: %w", err)
	}

	switch status {
	case "claimed":
		return domain.PaymentClaim{Status: domain.PaymentClaimed, Payment: payment}, nil
	case "completed":
		return domain.PaymentClaim{Status: domain.PaymentCompleted}, nil
	default:
		return domain.PaymentClaim{Status: domain.PaymentNotClaimable}, nil
	}
}

// ReleasePayment returns a current worker's retryable claim to pending while
// retaining its Processor Assignment. A changed claim owner prevents an
// expired worker from releasing a newer lease.
func (s *Store) ReleasePayment(ctx context.Context, correlationID uuid.UUID, claimOwner string) (bool, error) {
	result, err := s.pool.Exec(ctx, `
		UPDATE payments
		SET processing_state = 'pending',
			processing_claimed_by = NULL,
			processing_claim_expires_at = NULL
		WHERE correlation_id = $1
			AND processing_state = 'processing'
			AND processing_claimed_by = $2`, correlationID, claimOwner)
	if err != nil {
		return false, fmt.Errorf("release Payment: %w", err)
	}
	return result.RowsAffected() == 1, nil
}

// CompletePayment atomically records Default Processor confirmation only for
// the active owner of a Payment claim.
func (s *Store) CompletePayment(ctx context.Context, correlationID uuid.UUID, claimOwner, completedByInstance string) (bool, error) {
	result, err := s.pool.Exec(ctx, `
		UPDATE payments
		SET processing_state = 'completed',
			processed_by_service = 'default',
			completed_by_instance = $3,
			completed_at = now(),
			processing_claimed_by = NULL,
			processing_claim_expires_at = NULL
		WHERE correlation_id = $1
			AND processing_state = 'processing'
			AND processing_claimed_by = $2`, correlationID, claimOwner, completedByInstance)
	if err != nil {
		return false, fmt.Errorf("complete Payment: %w", err)
	}
	return result.RowsAffected() == 1, nil
}

// MarkOutboxDispatched records a confirmed JetStream publication. The claim
// owner condition prevents an expired worker from completing a newer claim.
func (s *Store) MarkOutboxDispatched(ctx context.Context, correlationID uuid.UUID, claimedBy string) (bool, error) {
	result, err := s.pool.Exec(ctx, `
		UPDATE payment_outbox
		SET state = 'dispatched',
			claimed_by = NULL,
			claim_expires_at = NULL,
			dispatched_at = now()
		WHERE correlation_id = $1 AND state = 'dispatching' AND claimed_by = $2`, correlationID, claimedBy)
	if err != nil {
		return false, fmt.Errorf("mark outbox row dispatched: %w", err)
	}
	return result.RowsAffected() == 1, nil
}
