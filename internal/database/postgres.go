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
// lease. It records selected only for an unassigned Payment. A prior Processor
// Assignment is retained so an ambiguous attempt cannot be routed differently
// on retry; without either one, an unassigned Payment remains unclaimed.
func (s *Store) ClaimPayment(ctx context.Context, correlationID uuid.UUID, claimOwner string, expiry time.Duration, selected *domain.ProcessorService) (domain.PaymentClaim, error) {
	var selectedService *string
	if selected != nil {
		selectedService = new(string)
		*selectedService = string(*selected)
	}

	var status string
	var assignment *string
	var payment domain.Payment
	err := s.pool.QueryRow(ctx, `
		WITH candidate AS (
			SELECT correlation_id, amount, requested_at, processing_state,
				processing_claim_expires_at, processor_assignment
			FROM payments
			WHERE correlation_id = $1
			FOR UPDATE
		), claimed AS (
			UPDATE payments AS payment
			SET processing_state = 'processing',
				processor_assignment = COALESCE(payment.processor_assignment, $4::text),
				processing_claimed_by = $2,
				processing_claim_expires_at = now() + ($3 * interval '1 microsecond')
			FROM candidate
			WHERE payment.correlation_id = candidate.correlation_id
				AND (candidate.processor_assignment IS NOT NULL OR $4::text IS NOT NULL)
				AND (
					candidate.processing_state = 'pending'
					OR (
						candidate.processing_state = 'processing'
						AND candidate.processing_claim_expires_at <= now()
					)
				)
			RETURNING payment.correlation_id, payment.amount, payment.requested_at,
				payment.processor_assignment
		)
		SELECT 'claimed', correlation_id, amount, requested_at, processor_assignment
		FROM claimed
		UNION ALL
		SELECT CASE
			WHEN processing_state = 'completed' THEN 'completed'
			WHEN processor_assignment IS NULL AND $4::text IS NULL THEN 'unassigned'
			ELSE 'not_claimable'
		END, correlation_id, amount, requested_at, processor_assignment
		FROM candidate
		WHERE NOT EXISTS (SELECT 1 FROM claimed)`, correlationID, claimOwner, expiry.Microseconds(), selectedService).Scan(
		&status, &payment.CorrelationID, &payment.Amount, &payment.RequestedAt, &assignment,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.PaymentClaim{Status: domain.PaymentNotClaimable}, nil
	}
	if err != nil {
		return domain.PaymentClaim{}, fmt.Errorf("claim Payment: %w", err)
	}
	if assignment != nil {
		payment.ProcessorAssignment = domain.ProcessorService(*assignment)
	}

	switch status {
	case "claimed":
		return domain.PaymentClaim{Status: domain.PaymentClaimed, Payment: payment}, nil
	case "completed":
		return domain.PaymentClaim{Status: domain.PaymentCompleted}, nil
	case "unassigned":
		return domain.PaymentClaim{Status: domain.PaymentUnassigned}, nil
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

// CompletePayment atomically records confirmation by the assigned Processor
// only for the active owner of a Payment claim.
func (s *Store) CompletePayment(ctx context.Context, correlationID uuid.UUID, claimOwner, completedByInstance string, service domain.ProcessorService) (bool, error) {
	result, err := s.pool.Exec(ctx, `
		UPDATE payments
		SET processing_state = 'completed',
			processed_by_service = $4,
			completed_by_instance = $3,
			completed_at = now(),
			processing_claimed_by = NULL,
			processing_claim_expires_at = NULL
		WHERE correlation_id = $1
			AND processing_state = 'processing'
			AND processing_claimed_by = $2
			AND processor_assignment = $4`, correlationID, claimOwner, completedByInstance, string(service))
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

// PaymentSummary reads completed Payments by their acceptance timestamps. The
// Processor contract treats a missing bound as an unfiltered request, so a
// timestamp predicate is used only when both bounds are present.
func (s *Store) PaymentSummary(ctx context.Context, from, to *time.Time) (domain.PaymentSummary, error) {
	query := `
		SELECT processed_by_service, COUNT(*), COALESCE(SUM(amount), 0)
		FROM payments
		WHERE processing_state = 'completed'`
	var arguments []any
	if from != nil && to != nil {
		query += `
			AND requested_at >= $1 AND requested_at <= $2`
		arguments = []any{*from, *to}
	}
	query += `
		GROUP BY processed_by_service`

	rows, err := s.pool.Query(ctx, query, arguments...)
	if err != nil {
		return domain.PaymentSummary{}, fmt.Errorf("query Payment Summary: %w", err)
	}
	defer rows.Close()

	var summary domain.PaymentSummary
	for rows.Next() {
		var service string
		var total domain.ProcessorTotal
		if err := rows.Scan(&service, &total.TotalRequests, &total.TotalAmountCents); err != nil {
			return domain.PaymentSummary{}, fmt.Errorf("scan Payment Summary: %w", err)
		}

		switch domain.ProcessorService(service) {
		case domain.DefaultProcessor:
			summary.Default = total
		case domain.FallbackProcessor:
			summary.Fallback = total
		default:
			return domain.PaymentSummary{}, fmt.Errorf("read invalid Processor service %q", service)
		}
	}
	if err := rows.Err(); err != nil {
		return domain.PaymentSummary{}, fmt.Errorf("iterate Payment Summary: %w", err)
	}
	return summary, nil
}

// ReadProcessorAvailability returns the authoritative startup state for every
// Processor that has a persisted observation.
func (s *Store) ReadProcessorAvailability(ctx context.Context) ([]domain.ProcessorAvailability, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT service, available, version, passive_failure_at
		FROM processor_availability`)
	if err != nil {
		return nil, fmt.Errorf("read Processor Availability: %w", err)
	}
	defer rows.Close()

	var availability []domain.ProcessorAvailability
	for rows.Next() {
		var state domain.ProcessorAvailability
		var service string
		if err := rows.Scan(&service, &state.Available, &state.Version, &state.PassiveFailureAt); err != nil {
			return nil, fmt.Errorf("scan Processor Availability: %w", err)
		}
		state.Service = domain.ProcessorService(service)
		if !state.Service.Valid() {
			return nil, fmt.Errorf("read invalid Processor service %q", service)
		}
		availability = append(availability, state)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate Processor Availability: %w", err)
	}
	return availability, nil
}

// AvailabilityPollStartedAt obtains the database time immediately before a
// Processor Availability HTTP request begins. It orders recovery against a
// concurrent passive failure without trusting instance clocks.
func (s *Store) AvailabilityPollStartedAt(ctx context.Context) (time.Time, error) {
	var startedAt time.Time
	if err := s.pool.QueryRow(ctx, `SELECT now()`).Scan(&startedAt); err != nil {
		return time.Time{}, fmt.Errorf("record Processor Availability poll start: %w", err)
	}
	return startedAt, nil
}

// RecordPassiveFailure atomically marks a Processor unavailable and records
// the database timestamp that later healthy polls must follow.
func (s *Store) RecordPassiveFailure(ctx context.Context, service domain.ProcessorService) (domain.ProcessorAvailability, error) {
	return s.upsertProcessorAvailability(ctx, `
		INSERT INTO processor_availability (service, available, version, passive_failure_at)
		VALUES ($1, FALSE, 1, now())
		ON CONFLICT (service) DO UPDATE
		SET available = FALSE,
			version = processor_availability.version + 1,
			passive_failure_at = now()
		RETURNING service, available, version, passive_failure_at`, service)
}

// RecordPollObservation records a successful health response. A healthy
// observation that began no later than the latest passive failure is rejected,
// so an older in-flight poll cannot restore new work routing.
func (s *Store) RecordPollObservation(ctx context.Context, service domain.ProcessorService, available bool, startedAt time.Time) (domain.ProcessorAvailability, bool, error) {
	state, err := s.upsertProcessorAvailability(ctx, `
		INSERT INTO processor_availability (service, available, version)
		VALUES ($1, $2, 1)
		ON CONFLICT (service) DO UPDATE
		SET available = EXCLUDED.available,
			version = processor_availability.version + 1
		WHERE NOT EXCLUDED.available
			OR processor_availability.passive_failure_at IS NULL
			OR processor_availability.passive_failure_at < $3
		RETURNING service, available, version, passive_failure_at`, service, available, startedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.ProcessorAvailability{}, false, nil
	}
	if err != nil {
		return domain.ProcessorAvailability{}, false, fmt.Errorf("record Processor Availability poll observation: %w", err)
	}
	return state, true, nil
}

func (s *Store) upsertProcessorAvailability(ctx context.Context, query string, arguments ...any) (domain.ProcessorAvailability, error) {
	var state domain.ProcessorAvailability
	var service string
	if err := s.pool.QueryRow(ctx, query, arguments...).Scan(&service, &state.Available, &state.Version, &state.PassiveFailureAt); err != nil {
		return domain.ProcessorAvailability{}, err
	}
	state.Service = domain.ProcessorService(service)
	if !state.Service.Valid() {
		return domain.ProcessorAvailability{}, fmt.Errorf("read invalid Processor service %q", service)
	}
	return state, nil
}

const healthAdvisoryLockKey int64 = 202504

// HealthLeadership holds the dedicated PostgreSQL session containing this
// instance's lifetime advisory lock. It must be closed rather than returned to
// the pool while still locked, because advisory locks are session-scoped.
type HealthLeadership struct {
	conn *pgxpool.Conn
}

// TryHealthLeadership tries to acquire the shared health-poller lock on a
// dedicated pool connection. The returned connection is not used for state
// reads or writes, which remain short-lived pool operations.
func (s *Store) TryHealthLeadership(ctx context.Context) (*HealthLeadership, bool, error) {
	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return nil, false, fmt.Errorf("acquire health leadership connection: %w", err)
	}

	var acquired bool
	if err := conn.QueryRow(ctx, `SELECT pg_try_advisory_lock($1)`, healthAdvisoryLockKey).Scan(&acquired); err != nil {
		conn.Release()
		return nil, false, fmt.Errorf("try health advisory lock: %w", err)
	}
	if !acquired {
		conn.Release()
		return nil, false, nil
	}
	return &HealthLeadership{conn: conn}, true, nil
}

// Alive verifies the lock session remains connected before a scheduled poll.
func (l *HealthLeadership) Alive(ctx context.Context) error {
	if err := l.conn.QueryRow(ctx, `SELECT 1`).Scan(new(int)); err != nil {
		return fmt.Errorf("check health leadership connection: %w", err)
	}
	return nil
}

// Close relinquishes leadership. If unlock cannot be confirmed, closing the
// physical session guarantees PostgreSQL releases its advisory lock.
func (l *HealthLeadership) Close(ctx context.Context) {
	if l == nil || l.conn == nil {
		return
	}
	var unlocked bool
	err := l.conn.QueryRow(ctx, `SELECT pg_advisory_unlock($1)`, healthAdvisoryLockKey).Scan(&unlocked)
	if err != nil || !unlocked {
		_ = l.conn.Conn().Close(ctx)
	}
	l.conn.Release()
	l.conn = nil
}
