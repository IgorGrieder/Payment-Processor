package database

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
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
