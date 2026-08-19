# payment-processor

Initial Go scaffold for the payment processor challenge.

## Dependencies

- PostgreSQL via `github.com/jackc/pgx/v5`
- NATS via `github.com/nats-io/nats.go`

## Run

For local process development:

```sh
go run ./cmd/api
go test ./...
```

For the final Compose deployment, start the Payment Processor stack first. It creates the external Docker network named `payment-processor`, which the two API instances use to reach the processors. Then start this backend:

```sh
docker compose up --build
```

Nginx is the only public service and exposes the API at `http://localhost:9999`. PostgreSQL, NATS, and both API instances have no host port mappings. Configuration is read from environment variables; the Compose file sets the final instance, pool, worker, timeout, and processor URL values.
