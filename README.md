# payment-processor

Initial Go scaffold for the payment processor challenge.

## Dependencies

- PostgreSQL via `github.com/jackc/pgx/v5`
- NATS via `github.com/nats-io/nats.go`

## Run

```sh
go run ./cmd/api
go test ./...
```

Configuration is read from `HTTP_ADDR`, `DATABASE_URL`, and `NATS_URL`.
