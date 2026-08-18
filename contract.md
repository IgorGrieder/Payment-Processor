# HTTP Contract

This document records the HTTP routes relevant to this backend. The challenge specification is the external-contract source of truth; repository decisions that deliberately narrow our own public behavior are identified separately.

**Sources**

- Challenge instructions: [`INSTRUCOES.md`](https://github.com/zanfranceschi/rinha-de-backend-2025/blob/main/INSTRUCOES.md), accessed 2026-08-17.
- Local architectural decisions: [`AGENTS.md`](AGENTS.md).

## This backend's public API

These routes are exposed through Nginx on `http://localhost:9999` in the final deployment.

### `POST /payments`

Durably accepts a Payment for asynchronous processing.

**Request body**

```json
{
  "correlationId": "4a7901b8-7d26-4d9d-aa19-4dc1c7cf60b3",
  "amount": 19.90
}
```

| Field | Required | Type | Rules |
| --- | --- | --- | --- |
| `correlationId` | yes | UUID string | Unique Payment identity. |
| `amount` | yes | decimal number | Must be positive. |

**Response required by this backend's architecture**

| Condition | Response |
| --- | --- |
| Valid new Payment | `202 Accepted`, empty body, after the Payment and its outbox record commit atomically. |
| Valid duplicate `correlationId` | `202 Accepted`, empty body; creates neither another Payment nor more work. |
| Invalid JSON, missing/invalid `correlationId`, invalid/non-positive `amount` | `400 Bad Request`; creates no data. |

The challenge accepts any 2XX response and does not validate its body. The stricter `202` behavior above is a local agreed decision.

### `GET /payments-summary`

Returns the audit summary of Completed Payments, grouped by confirmed processor.

**Query parameters**

| Parameter | Required | Type | Meaning |
| --- | --- | --- | --- |
| `from` | no | ISO-8601 UTC timestamp | Lower timestamp bound. |
| `to` | no | ISO-8601 UTC timestamp | Upper timestamp bound. |

**Successful response**: `200 OK`

```json
{
  "default": {
    "totalRequests": 43236,
    "totalAmount": 415542345.98
  },
  "fallback": {
    "totalRequests": 423545,
    "totalAmount": 329347.34
  }
}
```

All four nested fields are required. `totalRequests` is an integer; both amounts are decimal JSON numbers. Per `AGENTS.md`, the query uses the original acceptance timestamp (`requested_at`): if either `from` or `to` is absent, it returns all Completed Payments; if both are present, both inclusive bounds apply.

## Payment Processor API

The Default and Fallback Payment Processors have identical APIs. In the final deployment, only `api-1` and `api-2` join their Docker network.

| Processor | Container-network base URL | Host-development base URL |
| --- | --- | --- |
| Default | `http://payment-processor-default:8080` | `http://localhost:8001` |
| Fallback | `http://payment-processor-fallback:8080` | `http://localhost:8002` |

### `POST /payments`

Processes a Payment at the selected external Payment Processor.

**Request body**

```json
{
  "correlationId": "4a7901b8-7d26-4d9d-aa19-4dc1c7cf60b3",
  "amount": 19.90,
  "requestedAt": "2025-07-15T12:34:56.000Z"
}
```

| Field | Required | Type | Rules |
| --- | --- | --- | --- |
| `correlationId` | yes | UUID string | Payment identity. |
| `amount` | yes | decimal number | Payment amount. |
| `requestedAt` | yes | ISO-8601 UTC timestamp | The exact timestamp assigned by PostgreSQL when this backend accepted the Payment. |

**Confirmed success**: `200 OK`

```json
{
  "message": "payment processed successfully"
}
```

The challenge specifies the `200` response. `AGENTS.md` additionally defines `422` as confirmation of an already-processed duplicate: it must be durably completed before its JetStream message is acknowledged.

### `GET /payments/service-health`

Reports whether the Processor may receive new Payments.

**Successful response**: `200 OK`

```json
{
  "failing": false,
  "minResponseTime": 100
}
```

| Field | Type | Meaning |
| --- | --- | --- |
| `failing` | boolean | `true` means the Processor's `POST /payments` will return 5XX errors. |
| `minResponseTime` | integer | Minimum possible response time for `POST /payments`, in milliseconds. |

The Processor permits at most one health request every five seconds; excess requests receive `429 Too Many Requests`. Our agreed architecture polls each Processor every 5.5 seconds and initially prioritizes Default's lower fee over `minResponseTime`.

### `GET /payments/{id}`

Diagnostic Payment lookup. It is not required for the final integration.

| Path parameter | Type | Meaning |
| --- | --- | --- |
| `id` | UUID | Payment Correlation ID. |

**Successful response**: `200 OK`

```json
{
  "correlationId": "4a7901b8-7d26-4d9d-aa19-4dc1c7cf60b3",
  "amount": 19.90,
  "requestedAt": "2025-07-15T12:34:56.000Z"
}
```

## Payment Processor administrative API

These routes exist on each Processor for test control and local troubleshooting. They require the `X-Rinha-Token` request header. The initial local token is `123`. The challenge explicitly says the final backend must not integrate with these routes; the test script uses them.

### `GET /admin/payments-summary`

Optional `from` and `to` ISO-8601 UTC timestamp query parameters. Returns `200 OK`:

```json
{
  "totalRequests": 43236,
  "totalAmount": 415542345.98,
  "totalFee": 415542.98,
  "feePerTransaction": 0.01
}
```

### `PUT /admin/configurations/token`

```json
{
  "token": "a password"
}
```

Requires a text `token`; returns `204 No Content`. Changing this token in a final submission aborts challenge testing.

### `PUT /admin/configurations/delay`

```json
{
  "delay": 235
}
```

Requires integer `delay` in milliseconds; returns `204 No Content`. It introduces response delay on processor `POST /payments`.

### `PUT /admin/configurations/failure`

```json
{
  "failure": true
}
```

Requires boolean `failure`; returns `204 No Content`. It configures processor `POST /payments` to fail.

### `POST /admin/purge-payments`

No request body. Returns `200 OK`:

```json
{
  "message": "All payments purged."
}
```

Deletes all Payments from that Processor and is for local development only.
