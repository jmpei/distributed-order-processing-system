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

#### Measured results on AWS — 50 users × 5 minutes

Same Locust scenario, run against the live ALB endpoint of the AWS deployment
described below. Stack: ECS Fargate (3 tasks × 0.25 vCPU / 512 MB) → RDS
`db.t4g.micro` → Amazon MQ for RabbitMQ `mq.m7g.medium` over AMQPS/TLS.
Locust on a laptop in Vancouver, ALB in `us-west-2`.

| Metric | Value | vs local |
|---|---|---|
| Total requests | **71,081** | — |
| Failures | **0 (0.00%)** | identical |
| Sustained throughput | **~237 req/s** | ~8× lower (smaller compute, real network) |
| Latency p50 | 200 ms | +9.5× (laptop ↔ ALB roundtrip) |
| Latency p95 | **310 ms** | +5.5× |
| Latency p99 | 500 ms | +5.3× |
| Latency p99.9 | 630 ms | +3.9× |
| Latency max | 1300 ms | +4.2× |
| Inventory conservation (3 sampled products) | `available + reserved = 100,000` exact | invariant holds |
| Sagas in `FAILED` / `COMPENSATED` | 0 | invariant holds |

Failure-rate of 0% and exact inventory conservation at the end of the run
confirm the saga state machine, optimistic-lock, and consumer-side idempotency
all behave correctly under real cross-AZ network + AMQPS/TLS overhead, not
just on a single Docker host.

## Deploy to AWS

Infrastructure-as-code under [`deploy/terraform/`](deploy/terraform/) brings up
the full stack on AWS:

| Layer | AWS resource |
|---|---|
| Network | VPC (10.0.0.0/16), 2 public + 2 private subnets across 2 AZs, 1 NAT GW |
| Edge | Application Load Balancer, path-based routing to 3 target groups |
| Compute | ECS Fargate cluster, 3 services (0.25 vCPU / 512 MB each) |
| Data | RDS MySQL 8 (`db.t4g.micro`, 3 schemas in one instance) |
| Messaging | Amazon MQ for RabbitMQ (`mq.m7g.medium`, single instance, AMQPS/TLS) |
| Secrets | DB + MQ passwords generated by Terraform → Secrets Manager → injected into Fargate tasks |
| Registry | 3 ECR repos (`order-service`, `inventory-service`, `payment-service`) |
| Identity | ECS task execution role with scoped Secrets Manager read |

> HTTPS is intentionally omitted in dev. `deploy/terraform/alb.tf` has the
> hook documented for a production-ready cert + redirect.

### Prereqs

- AWS account + IAM user with `AdministratorAccess` (Phase 6 only — tighten later)
- `aws configure` done (region = `us-west-2`)
- `aws --version` (≥2.x), `terraform version` (≥1.5), `docker --version` (≥20)
- Docker daemon running — `deploy.sh` uses `mysql:8` in a container to create the per-service schemas (Homebrew's `mysql` 9.x dropped the `mysql_native_password` plugin RDS 8.0 still uses)
- AWS Budget alert set in console (recommended: $30/month threshold)

### One-time setup

```bash
cd deploy/terraform
cp terraform.tfvars.example terraform.tfvars

# put your laptop's public IP into admin_cidr — required so deploy.sh can
# reach RDS on 3306 to create the inventory_db / payments_db schemas
echo "admin_cidr = \"$(curl -s https://checkip.amazonaws.com)/32\"" > terraform.tfvars
cat terraform.tfvars
```

### Deploy (≈ 15 minutes wall time)

```bash
# 1. Build + push the three images to ECR
bash deploy/scripts/build-and-push.sh

# 2. terraform apply in three stages (RDS → schemas → MQ + ECS)
bash deploy/scripts/deploy.sh

# 3. Wait ~2-3 min for ECS tasks to register healthy with the ALB
cd deploy/terraform
aws ecs describe-services \
  --cluster $(terraform output -raw ecs_cluster_name) \
  --services dop-dev-order dop-dev-inventory dop-dev-payment \
  --query 'services[].{name:serviceName,running:runningCount,desired:desiredCount}'
```

> First-time ordering note: `build-and-push.sh` needs the ECR repos to
> exist. If you run it before `deploy.sh`, run `deploy.sh` first (it's
> safe — Fargate tasks crashloop until images arrive, then settle).

### Verify happy path against the ALB

```bash
ALB=$(cd deploy/terraform && terraform output -raw alb_url)

# Seed a product
curl -s -X POST "$ALB/inventory" \
  -H 'Content-Type: application/json' \
  -d '{"product_id": 1001, "available_qty": 100}'

# Place an order (saga: PENDING → CONFIRMED in ~200 ms)
curl -s -X POST "$ALB/orders" \
  -H 'Content-Type: application/json' \
  -d '{"user_id": 42, "product_id": 1001, "quantity": 2, "total_amount": 199.98}'

# Check saga state
curl -s "$ALB/admin/sagas" | jq .
```

### Run Locust against the ALB

```bash
INVENTORY_URL="$ALB" bash scripts/loadtest/seed.sh   # seed against ALB instead of localhost
locust -f scripts/loadtest/locustfile.py --host "$ALB" \
       --headless --users 50 --spawn-rate 5 --run-time 5m \
       --html scripts/loadtest/report.html
```

### Cost estimate (us-west-2)

| Component | Monthly | Daily |
|---|---|---|
| Fargate (3 tasks × 0.25 vCPU / 512 MB, always-on) | ~$9 | ~$0.30 |
| Application Load Balancer | ~$16 | ~$0.55 |
| RDS `db.t4g.micro` + 20 GB gp3 | ~$13.50 | ~$0.45 |
| Amazon MQ `mq.m7g.medium`, single instance (smallest available for RabbitMQ — `mq.t3.micro` was removed) | ~$54 | ~$1.80 |
| NAT Gateway × 1 | ~$32 | ~$1.05 |
| Secrets Manager (2 secrets) | ~$0.80 | $0.03 |
| ECR (< 500 MB free), CloudWatch logs (7 d retention) | trivial | trivial |
| **Total** | **~$125** | **~$4.20** |

> ⚠ Forgetting to `destroy.sh` after a demo is the #1 way this project
> turns into a $100+ surprise. Set the AWS Budget alert.

> Verified end-to-end on real infra on 2026-05-12 (us-west-2): full deploy
> + happy path + 5-min Locust + destroy completed in ~1 hour for **≈ $0.20**
> in actual billing.

### Tear down

```bash
bash deploy/scripts/destroy.sh
# Requires typing 'destroy' to confirm.
# Takes 5-10 minutes (MQ broker deletion dominates).
```

## Key Design Decisions

- **Database-per-service**: no cross-service foreign keys; services share only `order_id` / `product_id` as logical references.
- **Optimistic locking** on `inventories.version` for hot-SKU reserves; retry budget configurable via `INVENTORY_RESERVE_MAX_RETRIES` (default 50).
- **Idempotency at every consumer**: `processed_events` (order, payment) keyed on event `message_id`; `inventory_logs` unique on `(order_id, action)` so reserve/release is safe to re-deliver.
- **Saga state persisted** in `saga_states` (`current_step` + `status`), updated atomically with the side-effect inside one DB transaction so the orchestrator never disagrees with itself across crashes.
- **Compensation** is an explicit saga branch, not error handling: payment failure transitions saga to `COMPENSATING` and emits `inventory.release`; only after the released event lands does the saga become terminal.
- **Recovery via re-publish**: `RecoverInProgressSagas` periodically re-sends the current step's command. Idempotency at consumers makes re-sends free.
