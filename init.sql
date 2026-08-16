CREATE TABLE IF NOT EXISTS payments (
    correlation_id UUID PRIMARY KEY,
    amount BIGINT NOT NULL,
    requested_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS payments_requested_at ON payments (requested_at);
