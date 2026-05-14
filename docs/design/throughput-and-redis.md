# Throughput and the Redis Question

A recurring interview prompt on this project:

> "MySQL only handles ~1K QPS. For high-concurrency order placement, shouldn't you put Redis in front to do pre-deduction?"

This document is the long answer.

## TL;DR

- The MySQL-only path measured **1,884 req/s** end-to-end (50 users × 5 min, 0 failures, p95 = 56 ms) on a single laptop. The "MySQL ≈ 1K QPS" framing is a heuristic, not a hard ceiling — it depends on schema, transaction shape, and contention.
- For **this workload** (order placement: write `orders` row + write `saga_states` row + publish one AMQP command), the bottleneck is not MySQL throughput; it's the durable write path itself. Putting Redis in front does not remove those writes.
- Redis is the right tool for **two specific scenarios** this project doesn't have: flash-sale-style burst contention on a single SKU, and read-heavy hot-key serving. For both, the design would be deliberate (Lua-script atomic decrement, eventual write-back), not a generic "speed up MySQL" layer.

## 1. Measured throughput

From `README.md` → Load test (Locust, 50 users × 5 min):

| Metric | Local Docker | AWS Fargate (0.25 vCPU × 3) |
|---|---|---|
| Sustained throughput | **1,884 req/s** | 237 req/s |
| Failures | 0 (0.00%) | 0 (0.00%) |
| p50 / p95 / p99 | 21 / 56 / 94 ms | 200 / 310 / 500 ms |
| Inventory invariant | `available + reserved` conserved | conserved |

Two observations:

1. **The numbers exceed the "1K QPS" framing.** A modern MySQL on a developer laptop handles short, indexed write transactions far above 1K/s. The "1K QPS" rule of thumb usually refers to wide, contended OLTP — not narrow PK + version-column updates.
2. **The 8× drop on Fargate is not MySQL.** It is compute size (0.25 vCPU per task), cross-AZ network, and AMQPS/TLS overhead. Adding Redis would not change the ALB → ECS → MySQL latency floor.

## 2. Why MySQL alone is sufficient *here*

The design already absorbs concurrency in three places that aren't "raise MySQL QPS":

### 2.1 Optimistic locking on `inventories.version`

Reserve is not a pessimistic `SELECT … FOR UPDATE`. It is:

```sql
UPDATE inventories
SET available_qty  = available_qty  - ?,
    reserved_qty   = reserved_qty   + ?,
    version        = version        + 1
WHERE product_id = ?
  AND version    = ?
  AND available_qty >= ?
```

If two reserves race, one wins on `version`, the other re-reads and retries. Retry budget = `INVENTORY_RESERVE_MAX_RETRIES` (default 50). The integration test `TestE2E_ConcurrentOrders_NoOversell` proves this under 50 concurrent reserves on `stock = 10`: exactly 10 succeed, 40 fail cleanly, `available_qty` lands at 0 — never negative.

This is the same invariant a Redis-Lua admission gate gives you, just paid for at the DB row level instead of in a separate system.

### 2.2 Async fan-out via RabbitMQ

`POST /orders` returns the moment the order + saga rows are committed and the first AMQP command is published. Inventory reserve, payment processing, and compensation happen asynchronously. The HTTP request path is therefore **short** — and short transactions are exactly what MySQL is good at.

If the workload required "client must see `CONFIRMED` in the HTTP response," the throughput story would be different. It does not.

### 2.3 Idempotency makes retries free

- `inventory_logs` unique on `(order_id, action)` → re-delivered reserves no-op.
- `processed_events` keyed on event `message_id` in payment + order consumers.
- `RecoverInProgressSagas` re-publishes step-appropriate commands every 30s.

Load spikes are absorbed by *queuing*: RabbitMQ holds work, consumers drain at the rate MySQL can sustain, no order is lost.

### 2.4 What actually limits throughput

Per request in the 1,884 req/s run, the dominant costs are:

- Go HTTP handler + JSON parsing
- `INSERT INTO orders`
- `INSERT INTO saga_states`
- AMQP publish round-trip

Two DB writes + one AMQP publish. Redis can shave the inventory check (which already runs on the consumer side, not the HTTP path), but it cannot eliminate the order/saga writes — those are the durability story.

## 3. When Redis *would* matter

Two scenarios, neither currently in scope:

### 3.1 Flash-sale burst contention (秒杀)

Setup: `stock = 10`, 100,000 buyers arrive in 1 second.

With pure optimistic-lock MySQL, ~99.99% of requests hit a `version` mismatch, retry until they observe `available_qty = 0`, then fail. Correctness holds, but the DB sees many multiples of the request volume in retries, and the connection pool / CPU get pinned.

The Redis pattern: pre-load `INV:1001 = 10` at the start of the sale. Each request runs a Lua script:

```lua
local stock = tonumber(redis.call('GET', KEYS[1]))
if stock and stock >= tonumber(ARGV[1]) then
  return redis.call('DECRBY', KEYS[1], ARGV[1])
else
  return -1
end
```

Atomic, in-memory, returns `-1` to 99,990 buyers instantly without touching MySQL. The 10 winners get a token and continue through the normal saga to persist.

**This project does not have flash sales.** The load test is broad (10 SKUs × 100k stock, random selection) — contention is per-row but never extreme. If a flash-sale requirement appeared, the right shape would be:

1. Front-load Redis with the SKU's count.
2. Lua atomic decrement is the admission gate.
3. Winners enqueue a "confirmed reserve" message.
4. A background worker writes the reserves to MySQL — the durability and audit source of truth — at the rate MySQL handles.

Crucially: **MySQL stays the source of truth.** Redis is an admission gate, not a replacement.

### 3.2 Read-heavy hot keys

`GET /inventory/:product_id` on an e-commerce homepage can hit millions of QPS. A Redis read-through cache is the standard answer.

This project's reads are admin / saga-internal — low volume. Not applicable.

## 4. Why Redis is not added as a resume bullet

Adding Redis half-heartedly creates more interview risk than it removes:

- **Two sources of truth.** Cache–DB consistency questions immediately follow (write-through? write-behind? what happens on Redis crash mid-decrement?).
- **New failure modes.** Redis down → degrade to MySQL (and the OL retry storm hits)? Reject the request? Queue and retry?
- **Compensation gets harder.** If inventory was pre-deducted in Redis and payment fails, the release path must roll back Redis *and* any eventual MySQL write.
- **Out of scope for the workload.** Measurements do not indicate MySQL is the bottleneck.

The defensible position: **measure first, scale the specific bottleneck.** Measurements exist. They don't point at MySQL. So no Redis.

If the workload changes (flash sale, hot reads), §3 is the playbook.

## 5. Interview talking points

| Prompt | Response |
|---|---|
| "MySQL only does 1K QPS." | "My single-laptop test sustained 1,884 req/s with 0 failures. 1K is a heuristic for wide, contended OLTP — not narrow optimistic-lock updates like this project's." |
| "Why no Redis?" | "Profiling shows order + saga writes dominate, not inventory contention. Redis would mask correctness behind a cache I'd have to keep consistent. The right place for Redis here is a flash-sale admission gate — designed but not implemented because no workload justifies the operational cost yet." |
| "What if 100k people hit one SKU at once?" | §3.1: Lua atomic decrement, winners enqueue to MySQL, MySQL stays source of truth. |
| "Cache–DB consistency?" | "I picked single-source-of-truth on purpose. If I added Redis, I'd use it as an admission gate with eventual write-back, not as the inventory store — that keeps the consistency question scoped to 'gate may reject extra winners,' not 'two systems disagree about stock.'" |
