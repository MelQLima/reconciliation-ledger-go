# conciliacao-ledger-go

Pix payment **ledger and reconciliation service** written in **Go**. It consumes payment events from **Kafka**, records them in a **double-entry ledger** on **PostgreSQL** with **exactly-once effects** (idempotent consumer + database constraints + Redis fast path), and reconciles the ledger against the bank/PSP settlement statement with a **concurrent worker pool**.

Built as a portfolio project to show production-style backend engineering in Go: event-driven architecture, consistency under at-least-once delivery, observability and cloud-native deployment.

## Highlights

| Area | What is implemented |
|---|---|
| Domain | Double-entry bookkeeping in integer cents (no floats), balanced-transaction invariant, Pix settlement and refund |
| Messaging | Kafka consumer (`segmentio/kafka-go`, consumer groups), manual offset commit after processing, retries with **exponential backoff**, **dead-letter topic** with error headers |
| Idempotency | Redis `SET NX EX` guard + PostgreSQL unique keys (`transactions.id`, `(end_to_end_id, type)`) → duplicate webhooks/events never double count |
| Persistence | PostgreSQL with `pgx/v5` connection pool, single DB transaction per event, batched inserts, materialized balances via `UPSERT`, embedded SQL migrations |
| Concurrency | Reconciliation fan-out with `errgroup` + bounded worker pool, context cancellation, `sync.Mutex`, race-detector clean |
| API | REST with `chi` router, RFC 7807 `application/problem+json` errors, request IDs, timeouts, body size limits |
| Observability | Structured JSON logs (`log/slog`), **Prometheus** metrics (`/metrics`), **OpenTelemetry** tracing (OTLP → Jaeger/Tempo) |
| Operations | Graceful shutdown (`signal.NotifyContext`), `/healthz` + `/readyz` probes, 12-factor config via env vars |
| Delivery | Multi-stage Docker build on distroless (non-root), Docker Compose (Postgres, Redis, Redpanda, Prometheus, Jaeger), Kubernetes manifests (Deployment, Service, HPA, ConfigMap), GitHub Actions CI |
| Tests | Table-driven unit tests, fakes via interfaces, `httptest`, concurrency tests, **Testcontainers** integration test against real PostgreSQL |

## Architecture

```
 PSP webhooks / payment services
            │  payments.events (Kafka)
            ▼
 ┌──────────────────────┐   dup?   ┌───────┐
 │  consumer (retries,  │ ───────▶ │ Redis │  SET NX EX
 │  backoff, DLQ)       │          └───────┘
 └──────────┬───────────┘
            ▼
 ┌──────────────────────┐  1 tx/event  ┌────────────┐
 │ ledger.Service       │ ───────────▶ │ PostgreSQL │ transactions / entries / balances
 └──────────┬───────────┘              └────────────┘
            ▲
 ┌──────────┴───────────┐
 │ HTTP API (chi)       │  POST /v1/events · GET /v1/accounts/{id}/balance
 │                      │  POST /v1/reconciliations?date=YYYY-MM-DD (CSV)
 └──────────────────────┘
```

Packages follow a ports-and-adapters layout: `internal/ledger` holds the domain and the `Store`/`Guard` interfaces; `internal/store` (PostgreSQL, in-memory), `internal/idempotency` (Redis, in-memory), `internal/consumer` (Kafka) and `internal/httpapi` are adapters.

## Run locally

```bash
docker compose up --build        # API on :8080, Prometheus :9090, Jaeger :16686
# or, with no dependencies (in-memory store):
go run ./cmd/api
```

```bash
curl -X POST localhost:8080/v1/events -H 'Content-Type: application/json' -d '{
  "event_id":"evt-1","type":"pix.settled","end_to_end_id":"E2E1",
  "payer_account":"customer-1","payee_account":"merchant-1",
  "amount_cents":15990,"occurred_at":"2026-11-16T10:00:00Z"}'
# 201 {"event_id":"evt-1","result":"recorded"}  — sending it again returns 200 "duplicate"

curl localhost:8080/v1/accounts/merchant-1/balance

curl -X POST 'localhost:8080/v1/reconciliations?date=2026-11-16' \
  -H 'Content-Type: text/csv' --data-binary @testdata/statement-2026-11-16.csv
```

## Tests

```bash
make test               # unit tests with -race
make test-integration   # + Testcontainers (requires Docker)
```

## Configuration

| Variable | Default | Description |
|---|---|---|
| `HTTP_ADDR` | `:8080` | listen address |
| `DATABASE_URL` | – | PostgreSQL URL; in-memory store when empty |
| `REDIS_ADDR` | – | enables the Redis idempotency guard |
| `KAFKA_BROKERS` | – | comma-separated brokers; enables the consumer |
| `KAFKA_TOPIC` / `KAFKA_DLQ_TOPIC` / `KAFKA_GROUP_ID` | `payments.events` / `payments.events.dlq` / `ledger` | Kafka settings |
| `RECONCILE_WORKERS` | `8` | reconciliation worker pool size |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | – | enables OpenTelemetry tracing |
| `LOG_LEVEL` | `info` | `debug`, `info`, `warn`, `error` |

## Design notes

- **Why cents as `int64`?** Floating point cannot represent money exactly.
- **Effectively-once processing:** Kafka gives at-least-once delivery; the consumer commits offsets only after the event is persisted, and duplicates are absorbed by the Redis guard and, authoritatively, by PostgreSQL unique constraints. If Redis is down the service keeps working using the database alone.
- **Poison messages** (invalid JSON or business rule violations) go straight to the DLQ and are committed so the partition is never blocked; transient errors are retried with exponential backoff first.
