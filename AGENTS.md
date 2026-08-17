# Collaboration instructions

This repository is being built collaboratively for the Rinha de Backend 2025 payment-processor challenge.

- The user owns every product, architectural, operational, and implementation decision.
- Act as a teacher: explain the problem, constraints, trade-offs, and consequences clearly enough for the user to make each decision.
- Do not make assumptions or proactively choose a stack, design, optimization, behavior, dependency, configuration, or implementation detail.
- Do not write or change application code unless the user has explicitly instructed that specific work.
- Before implementing a proposed next step, state what decision it depends on and wait for the user's instruction when it has not been made.
- Keep changes narrowly limited to the work explicitly requested.
- Treat the challenge instructions as the source of truth. Call out hard requirements and distinguish them from optional strategies.

## Implementation design

- Organize application code as a layered architecture, with each layer depending only on the layer beneath it.
- Prefer the fewest abstractions that clearly serve the current slice. Avoid speculative abstractions and overengineering.
- Define an interface only at a real seam where behavior varies; keep every interface minimal, focused, and caller-oriented.
- Prefer functional composition: explicit dependencies, data, and functions over object-oriented hierarchies or stateful objects.

## Agreed architecture

Do not change these decisions without an explicit user instruction.

### Acceptance and audit

- `POST /payments` validates input, durably accepts valid payments, and returns `202 Accepted` with an empty body.
- Invalid JSON, an invalid/missing `correlationId`, or an invalid/non-positive amount returns `400 Bad Request` and creates no data.
- In one PostgreSQL transaction, insert the payment and its outbox row. `correlation_id` is the unique payment primary key; duplicate incoming requests create no additional work but also return `202`.
- PostgreSQL assigns `requested_at` at acceptance with `now()`. Persist and forward that exact timestamp on every processor retry.
- Store money as `BIGINT` cents. Never use `float64` for money; format cents as decimals only at JSON boundaries.
- The payment row is the audit source of truth. Record terminal completion atomically with `processed_by_service`, `completed_by_instance`, and completion time.
- The payment summary queries completed payment rows directly; do not maintain aggregate counters. Filter by the original `requested_at`, using the processor's effective behavior: when either `from` or `to` is absent, return all records; when both are present, use inclusive bounds.
- Use a partial covering index for completed-payment summary queries, keyed by `requested_at` and covering processor and amount fields. Verify query plans with `EXPLAIN (ANALYZE, BUFFERS)` later.
- No separate attempt-history table and no dispatched-outbox cleanup are needed for this challenge; use terminal logging for operational events.

### Outbox and JetStream

- The outbox has a foreign key to its payment and stores dispatch control state, not a duplicated payment payload.
- Outbox dispatch state is `pending -> dispatching -> dispatched`, with persisted `claimed_by` and expiring claims.
- Claim a row and commit before publishing. Publish to JetStream and wait for PubAck; only then mark it dispatched. Never mark it dispatched before PubAck.
- A publish failure returns the row to pending. A dispatcher crash lets its persisted claim expire and permits reclaim. Duplicate publication is expected and safe.
- Publish with `Nats-Msg-Id = correlationId` and configure a JetStream duplicate window as an additional, non-authoritative guard.
- JetStream uses file-backed storage on a named Docker volume and work-queue retention: messages remain until ACK and are deleted after ACK.
- Use a shared durable pull consumer with explicit acknowledgements, unlimited delivery attempts, no dead-letter queue, and `MaxAckPending` equal to total processing-worker capacity.
- Initial values are configurable: `OUTBOX_WORKERS=1` per API instance, a 2-second NATS publish timeout, and a 5-second outbox claim expiry.

### Worker processing and routing

- Both API instances run the outbox dispatcher, processing-worker pool, and health-election goroutines at startup.
- Initial processing concurrency is `PROCESSING_WORKERS=6` per instance, for 12 total active payments. Use a bounded pool; never spawn unbounded goroutines per message.
- A worker atomically claims a pending payment before an external call. Claims expire after 20 seconds so crashes can be recovered.
- A completed payment is ACKed without another processor call. On confirmed 200 or the processor's duplicate `422`, atomically mark it completed in PostgreSQL before ACKing JetStream.
- Processor calls use configurable `PROCESSOR_TIMEOUT=10s`. JetStream `AckWait` is 15 seconds. If both processors are unavailable, explicitly NAK with an initial 6-second delay; do not sleep while holding a worker.
- The default processor is always selected while healthy; ignore `minResponseTime` initially because fee minimization takes precedence over latency.
- If default is known failing and fallback is healthy, assign new work to fallback. If both are failing, retry later through JetStream.
- Persist the assigned processor before the first call. After an ambiguous failure (timeout/connection failure), retry that same processor rather than immediately crossing to fallback, preventing a payment from being processed by both services.
- A real timeout or 5xx immediately marks that processor unavailable in shared health state. It remains unavailable for new assignments until the next scheduled health poll reports it healthy.

### Shared health state and deployment

- Each instance attempts to hold one PostgreSQL advisory lock for its lifetime. The lock holder is the only health poller and polls each processor every 5.5 seconds; PostgreSQL releases the lock if that process dies.
- Store the latest health state in PostgreSQL and broadcast updates through NATS. Workers route using their local in-memory cache, not a PostgreSQL read per payment.
- Nginx is the only public entrypoint on port `9999`; it uses minimal round-robin routing to `api-1` and `api-2`, without sticky sessions or POST retries.
- Only `api-1` and `api-2` join the external `payment-processor` bridge network. Configure processor URLs with `PROCESSOR_DEFAULT_URL=http://payment-processor-default:8080` and `PROCESSOR_FALLBACK_URL=http://payment-processor-fallback:8080`.
- Use PostgreSQL connection pools bounded by `DATABASE_MAX_CONNS=12` per API instance. Do not hold a database connection while making a processor HTTP call.
- Initial total resource budget: Nginx 0.05 CPU/16MB; each API 0.30 CPU/64MB; NATS 0.15 CPU/48MB; PostgreSQL 0.70 CPU/158MB. Tune only through later challenge-provided tests while remaining within the 1.5 CPU/350MB limit.
- Use Uber Zap for terminal logging. Do not add unit or integration tests until the user explicitly requests them; use the challenge-provided tests after the base implementation.
