CREATE TABLE IF NOT EXISTS payments (
    correlation_id UUID PRIMARY KEY,
    amount BIGINT NOT NULL CHECK (amount > 0),
    requested_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS payments_requested_at ON payments (requested_at);

CREATE TABLE IF NOT EXISTS payment_outbox (
    correlation_id UUID PRIMARY KEY REFERENCES payments (correlation_id),
    state TEXT NOT NULL CHECK (state IN ('pending', 'dispatching', 'dispatched')),
    claimed_by TEXT,
    claim_expires_at TIMESTAMPTZ,
    dispatched_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS payment_outbox_dispatchable
    ON payment_outbox (state, claim_expires_at, correlation_id)
    WHERE state IN ('pending', 'dispatching');
