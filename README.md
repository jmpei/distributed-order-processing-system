# distributed-order-processing

Distributed order processing system demonstrating the **Saga Orchestration** pattern for distributed transactions across three independent microservices.

## Architecture

```
POST /orders
     │
     ▼
┌─────────────────┐   saga.commands (direct)   ┌──────────────────────┐
│  Order Service  │ ─────inventory.reserve────► │  Inventory Service   │
│  :8081          │ ─────inventory.release────► │  :8082               │
│                 │                             │                      │
│  SagaOrchest-   │ ◄────inventory.reserved───  │                      │
│  rator          │ ◄────inventory.released───  │                      │
│                 │   saga.events (topic)       └──────────────────────┘
│                 │
│  RecoverIn-     │   saga.commands (direct)   ┌──────────────────────┐
│  ProgressSagas  │ ──────payment.process─────► │  Payment Service     │
│  (every 30s)    │ ◄─────payment.processed───  │  :8083               │
└─────────────────┘   saga.events (topic)       └──────────────────────┘
```

**Happy path:** `POST /orders` → Order `PENDING` → Reserve Inventory → Process Payment → Order `CONFIRMED`

**Compensation path:** Payment fails → Order `FAILED` → Release Inventory → Order `COMPENSATED`

## Tech Stack

| Layer | Technology |
|---|---|
| Language | Go 1.25 |
| HTTP | Gin |
| ORM | GORM |
| Database | MySQL 8 (one DB per service) |
| Messaging | RabbitMQ 3 (amqp091-go) |
| Infrastructure | Docker Compose |

## Services

| Service | Port | DB | Tables |
|---|---|---|---|
| order-service | 8081 | orders_db | orders, saga_states, processed_events |
| inventory-service | 8082 | inventory_db | inventories, inventory_logs |
| payment-service | 8083 | payments_db | payments, processed_events |

## RabbitMQ Topology

| Exchange | Type | Purpose |
|---|---|---|
| `saga.commands` | direct | Orchestrator → worker commands |
| `saga.events` | topic | Worker replies → orchestrator |

Each queue has a corresponding DLQ (e.g. `inventory.commands.dlq`).

## Saga State Machine

Persisted in `order-service`'s `saga_states` table. Status × current_step:

```
                   ┌── reserve fails ──────────────────► FAILED
                   │
START (IN_PROGRESS)┼── reserved ─► (PROCESSING_PAYMENT) ─┬── paid ─────► COMPLETED
                   │                                     │
                   │                                     └── pay fails ─► COMPENSATING (RELEASING_INVENTORY)
                   │                                                          │
                   │                                                          ├── released ───► COMPENSATED
                   │                                                          │
                   │                                                          └── release fails ► FAILED
```

| Failure | Order final | Saga final |
|---|---|---|
| Reservation rejected (insufficient stock / lock exhaustion) | `FAILED` | `FAILED` |
| Payment rejected | `COMPENSATED` | `COMPENSATED` |
| Compensation (release) errors | `FAILED` | `FAILED` (manual intervention) |

## Crash Recovery

`order-service` runs `RecoverInProgressSagas` at startup and every 30s. It re-publishes the step-appropriate command for any saga in `IN_PROGRESS` or `COMPENSATING`. Combined with RabbitMQ durable queues + idempotent consumers (`(order_id, action)` for inventory; `processed_events` for payment & order), this means:

- A consumer can crash mid-saga; the broker holds the message until it returns.
- A consumer can be down when the command is published; it picks up the queued message on restart.
- The recovery loop covers the case where the publish itself was lost (e.g., publish-after-commit failed in a previous run).

## Admin Endpoints

Read-only, served by `order-service`:

```
GET /admin/sagas[?status=IN_PROGRESS|COMPENSATING|COMPLETED|COMPENSATED|FAILED]
GET /admin/sagas/:saga_id        # saga + matching order
```

## Configuration

Per-service env vars (set in `deploy/docker-compose.yml`, override at `up` time):

| Var | Service | Default | Purpose |
|---|---|---|---|
| `PAYMENT_FAILURE_RATE` | payment | `0.0` | `rand.Float64()` threshold for emitting `success:false` (testing only) |
| `INVENTORY_RESERVE_MAX_RETRIES` | inventory | `50` | Optimistic-lock retry budget per reserve cmd; bump if hot-SKU contention exceeds this |

## Running Locally

```bash
cd deploy
docker-compose up --build
```

Services start after their MySQL and RabbitMQ dependencies pass healthchecks.

**Seed inventory** (required before placing orders):

```bash
curl -X POST http://localhost:8082/inventory \
  -H 'Content-Type: application/json' \
  -d '{"product_id": 1001, "available_qty": 100}'
```

**Place an order:**

```bash
curl -X POST http://localhost:8081/orders \
  -H 'Content-Type: application/json' \
  -d '{"user_id": 42, "product_id": 1001, "quantity": 2, "total_amount": 199.98}'
```

**Run the test scripts** (each prints `PASS` or `FAIL` and exits non-zero on failure):

| Script | What it verifies |
|---|---|
| `scripts/test-happy-path.sh` | Order → `CONFIRMED`, inventory decremented, payment `SUCCESS` |
| `scripts/test-insufficient-stock.sh` | qty > stock → order `FAILED`, no compensation |
| `scripts/test-payment-failure.sh` | `PAYMENT_FAILURE_RATE=1.0` → order `COMPENSATED`, inventory restored |
| `scripts/test-crash-recovery.sh` | `inventory-service` stopped, queued reserve cmd processed on restart → `CONFIRMED` |
| `scripts/test-concurrent-orders.sh` | 50 concurrent orders on stock=10 → exactly 10 `CONFIRMED` + 40 `FAILED`, `available_qty=0` |

## Testing

### Unit tests

Each service ships a mock-based unit-test suite (`testify/mock`) under
`internal/service/`. Repositories and the AMQP publisher are wired through
Go interfaces so handlers can be tested against in-memory fakes — no MySQL or
RabbitMQ needed.

| Service | Tests | Coverage (`internal/service`) |
|---|---|---|
| order-service | 6 (saga recovery, payment-failure compensation, dispatch, CRUD, …) | **62.0%** |
| inventory-service | 5 (reserve success, **optimistic-lock retry**, idempotent re-delivery, release, dispatch) | **65.7%** |
| payment-service | 2 (idempotent re-process, CRUD) | **65.4%** |

Run from each service directory:

```bash
go test -cover ./internal/service/
```

### Integration tests (testcontainers-go)

`tests/integration/` spins up **one MySQL container (three schemas)** plus
**one RabbitMQ container** via `testcontainers-go`, compiles the three
service binaries once in `TestMain`, then launches them as subprocesses
pointed at the test infrastructure. Two end-to-end scenarios:

| Test | What it asserts |
|---|---|
| `TestE2E_PaymentFailure_CompensatesInventory` | With `PAYMENT_FAILURE_RATE=1.0`, the saga reaches `COMPENSATED`, inventory is fully refunded (`available_qty=10, reserved_qty=0`), and `saga_state.status=COMPENSATED`. |
| `TestE2E_ConcurrentOrders_NoOversell` | 50 concurrent orders against `stock=10` end with **exactly 10 `CONFIRMED` + 40 `FAILED`** and `available_qty=0` — never negative. |

```bash
cd tests/integration
go test -tags=integration -timeout=10m -v ./...
```

Total runtime including container startup: ~40 seconds.

### One-shot runner

`scripts/run-all-tests.sh` chains every suite (unit + integration) and exits
non-zero on the first failure.

```bash
bash scripts/run-all-tests.sh
```

### Load test (Locust)

`scripts/loadtest/` holds a Locust scenario that hammers `POST /orders` at a
constant 50 concurrent users for 5 minutes against a pool of 10 pre-seeded
products (100,000 units each). Setup and usage are documented in
[`scripts/loadtest/README.md`](scripts/loadtest/README.md).

```bash
make up                                    # bring the full stack up
bash scripts/loadtest/seed.sh              # seed 10 products × 100k units
locust -f scripts/loadtest/locustfile.py \
       --host http://localhost:8081 \
       --headless --print-stats \
       --html scripts/loadtest/report.html
```

#### Measured results — 50 users × 5 minutes

Run on Apple Silicon (MacBook Pro 16", Docker Desktop, all services local).

| Metric | Value |
|---|---|
| Total requests | **565,827** |
| Failures | **0 (0.00%)** |
| Sustained throughput | **~1,884 req/s** (avg over full run) |
| Latency p50 | 21 ms |
| Latency p95 | **56 ms** |
| Latency p99 | 94 ms |
| Latency p99.9 | 160 ms |
| Latency max | 310 ms |
| Inventory consumed (product 8001) | 3,445 / 100,000 (saga happy path completes fully under load) |

Each request returns the moment the order row + saga state are persisted and
the first command is published; saga completion is asynchronous over
RabbitMQ. The 0% failure rate combined with `available_qty + reserved_qty`
remaining conserved confirms the optimistic-lock + idempotency invariants
hold under sustained pressure.

## Key Design Decisions

- **Database-per-service**: no cross-service foreign keys; services share only `order_id` / `product_id` as logical references.
- **Optimistic locking** on `inventories.version` for hot-SKU reserves; retry budget configurable via `INVENTORY_RESERVE_MAX_RETRIES` (default 50).
- **Idempotency at every consumer**: `processed_events` (order, payment) keyed on event `message_id`; `inventory_logs` unique on `(order_id, action)` so reserve/release is safe to re-deliver.
- **Saga state persisted** in `saga_states` (`current_step` + `status`), updated atomically with the side-effect inside one DB transaction so the orchestrator never disagrees with itself across crashes.
- **Compensation** is an explicit saga branch, not error handling: payment failure transitions saga to `COMPENSATING` and emits `inventory.release`; only after the released event lands does the saga become terminal.
- **Recovery via re-publish**: `RecoverInProgressSagas` periodically re-sends the current step's command. Idempotency at consumers makes re-sends free.
