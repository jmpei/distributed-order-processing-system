# Round 2 — MySQL CPU diagnosis

Round 2 of the pool-tuning experiment raised order-service's MySQL driver pool from `MaxOpen=25 / MaxIdle=5` to `50 / 25`. RPS lifted +9% (739 → 803) and p95 dropped 21% (140 → 110 ms), but **mysql-order's container CPU jumped from ~60% steady-state to 130-160%**. This document figures out *why* the DB suddenly went hot — query execution? row lock contention? something else?

## Setup

- Stack: local docker-compose, fresh re-seed (10 products × 100k).
- Load: same Locust scenario (50 users × 5 min, no think time), kept running through all three sampling rounds.
- Order-service `DB_MAX_OPEN_CONNS=50`, `DB_MAX_IDLE_CONNS=25`.
- Diagnostic shell ran in a separate `mysql` client session (no `EXPLAIN`, no plan-cache mutation).
- 60s warmup before first sample. Three rounds, 10s apart.

Raw output: `/tmp/locust-r2/mysql-diagnosis.txt`.

## Per-round samples

### Q1 — processlist state distribution (non-Sleep)

| Round | "waiting for handler commit" | "update" (Execute) | "executing" (Query) | "statistics" (Execute) | Daemon |
|---|---|---|---|---|---|
| **R1** (08:54:06) | **38** (Query) | 6 | 1 | 2 | 1 |
| **R2** (08:54:16) | **46** (Query) | 4 | 1 | 0 | 1 |
| **R3** (08:54:27) | **4** (Query) | 0 | 1 | 0 | 1 |

**Dominant state across all three rounds: `waiting for handler commit`** — concentrated 38–46 sessions in R1+R2, transient dip in R3 (caught mid-flush). Almost zero sessions in classic "Sending data" / "executing" / "Sorting result".

### Q2 — InnoDB transactions section (summary)

| Round | Trx id range | ACTIVE transactions | Typical state | Row locks held |
|---|---|---|---|---|
| R1 | 5015106–5015156 | ~30 | `ACTIVE (PREPARED) 0 sec` with next stmt = `COMMIT` | mostly **0**, a few txns hold 2 (their own inserted rows) |
| R2 | 5050666–5050707 | ~30 | same; one `ACTIVE 0 sec updating or deleting` on `saga_states` | mostly 0, a few hold 2 |
| R3 | 5082844–5082867 | ~20 | mixed: many `ACTIVE 0 sec` (post-prepare), some still pending `COMMIT` | same pattern |

Every active transaction across all three rounds is in the **`ACTIVE (PREPARED)`** half of 2PC, sitting on `COMMIT`. The example `saga_states` update in R2 (`UPDATE saga_states SET current_step='PROCESSING_PAYMENT' ...`) is a normal saga progress write — caught mid-flight, not blocked.

**No `LOCK WAIT` entries. No "waiting for table metadata lock". No "Waiting for row lock".**

### Q3 — Lock + thread counters

| Counter | R1 | R2 | R3 |
|---|---|---|---|
| `Innodb_row_lock_current_waits` | **0** | **0** | **0** |
| `Innodb_row_lock_waits` (cumulative) | **0** | **0** | **0** |
| `Innodb_row_lock_time` (cumulative ms) | **0** | **0** | **0** |
| `Slow_queries` | 0 | 0 | 0 |
| `Threads_running` | 46 | 48 | 35 |

`Threads_running` 35–48 ≈ matches the in-use connections (driver pool `in_use` ~ 32–50 in the sampler). **All three lock counters stayed at zero through the entire 30s window** — row-lock contention is not a factor.

### Q4 — Top-10 active SQL

R1 + R2 + R3 — every one of the 30 sampled active queries is `COMMIT`. (No `INSERT`, no `UPDATE`, no `SELECT` in flight at the sample moment; those are too quick to catch.)

## Cross-round pattern

| Signal | What we saw |
|---|---|
| Where time is spent on MySQL | Almost entirely in `waiting for handler commit` — i.e. the 2PC commit pipeline |
| What every active SQL is | `COMMIT` |
| Row-lock contention | **None** — all three counters at 0 |
| Slow queries | None |
| `Threads_running` | 35–48 — matches app pool in-use |
| Active transactions | All in `ACTIVE (PREPARED)` half of 2PC, awaiting commit |

## Verdict — neither [A] nor [B] from the rubric

The user's rubric was:

- **[A] CPU bound on query execution** — needs `state="Sending data"`/`"executing"`/`"Sorting result"`. We see *almost none* of those.
- **[B] InnoDB row lock contention** — needs `Innodb_row_lock_current_waits > 5` and row-lock counters growing. We see **all three counters frozen at 0**.
- **[C] Mixed / inconclusive** — both patterns coexist. We see basically none of (A)'s patterns and none of (B)'s.

The real pattern is **(D): commit-pipeline serialization — fsync-bound on binlog / redo log group commit**.

Mechanism: every transaction that hits the order-service path writes `orders` + `saga_states` + (sometimes) `processed_events`, then commits. With InnoDB's durable defaults (`innodb_flush_log_at_trx_commit=1`, `sync_binlog=1`) every `COMMIT` triggers an `fsync()` on the redo log and (if binlog is on) the binary log. Group commit batches some of these, but at this concurrency (40-50 in-flight commits) most threads pile up in `waiting for handler commit` while the disk syncs. That's exactly what we see in the processlist.

**Implications:**

1. **Bumping the app pool further (50 → 100) will not help much.** More threads will just queue deeper at the commit step.
2. **Adding more order-service instances doesn't help either** — they all hit the same DB commit pipeline.
3. **Real levers** (in increasing severity / cost):
   - **Relax durability**: `innodb_flush_log_at_trx_commit=2` (durable per-second, not per-commit). Big throughput win, very small data-loss window risk on crash.
   - **Group commit tuning**: `binlog_group_commit_sync_delay=100` (microseconds) — wait briefly so more commits batch together. Modest win, no durability tradeoff.
   - **Faster disk**: gp3 with provisioned IOPS on RDS (we're not running on RDS yet — on local docker-on-Mac, `fsync` over VirtioFS is unusually slow, see caveat below).
   - **Shorter transactions** in app code: e.g. consolidate `orders` insert + `saga_states` insert into one transaction (already done) — verify there isn't an extra round-trip.

## Important caveat — Docker Desktop on macOS

Docker Desktop on macOS runs containers in a Linux VM with file I/O over **VirtioFS**, whose `fsync()` latency is significantly higher than bare-metal Linux or AWS EBS gp3. The "commit pipeline saturation" pattern we observed locally is *exaggerated* on this stack — on real RDS gp3 with multi-MB/s sustained write throughput, the same workload may not even hit the commit ceiling at 50 connections.

**Don't generalize this finding to AWS without re-measuring there.** On AWS RDS the bottleneck mix could look different — possibly the order-service container CPU becomes the next binder, or possibly RDS connection limits (`max_connections = 60` for `db.t4g.micro`) bite first.

## Falsification test — fsync hypothesis confirmed

To confirm the fsync interpretation wasn't just a plausible story, ran a pre-registered A/B inside a single Locust run:

- Pre-registered: **>+30% RPS** when fsync is relaxed → fsync was the real binder
- Pre-registered: **~0% RPS change** → commit serialization had a non-fsync cause (refute hypothesis)

Protocol inside one 5-min Locust window:

1. Warmup 60s.
2. Baseline window (30s) — read `http_requests_total{POST /orders, 201}` counter delta, durable defaults `innodb_flush_log_at_trx_commit=1, sync_binlog=1`.
3. Flip: `SET GLOBAL innodb_flush_log_at_trx_commit=2; SET GLOBAL sync_binlog=0;`
4. Settle 30s.
5. Relaxed window (30s) — same counter-delta measurement.
6. Restore: `SET GLOBAL innodb_flush_log_at_trx_commit=1; SET GLOBAL sync_binlog=1;` (immediately — this is not a production change).

### Results

| Phase | RPS | Δreqs in 30s | order app CPU | mysql-order CPU | `Threads_running` | order pool `in_use` |
|---|---|---|---|---|---|---|
| Pre-baseline diag | — | — | 118% | **147%** | 44 | 31 |
| Post-baseline (30s) | **854.5** | 25,636 | 71% | 94% | 51 | 50 |
| Start of relaxed | — | — | 134% | **229%** | 9 | 49 |
| End of relaxed (30s) | **1,150.9** | 37,981 | 203% | **231%** | 17 | 25 |

**Δ = +34.7% RPS** — over the pre-registered 30% threshold. Hypothesis confirmed.

Three corroborating signals:

1. **`Threads_running` collapsed** from 44–51 → 9–17. The "waiting for handler commit" pile-up disappeared the instant fsync stopped serializing.
2. **mysql-order CPU went *up*, not down**, from ~95–147% → ~229–231%. With fsync no longer the bottleneck, MySQL spent its time actually executing work rather than blocking — the CPU number rose because MySQL was *doing more useful work per second*, not waiting on disk.
3. **order-service app CPU climbed** 71% → 203% — the app was now processing 35% more orders/sec, naturally pushing more CPU on its side too.

This is the cleanest possible refutation of "MySQL was CPU-bound on query plan / execution." If MySQL had been query-bound, relaxing fsync would have changed nothing — the same SQL would still need the same compute. Instead, mysql-order CPU *climbed* because the disk barrier moved out of the way and the CPU got more chances to run real work per unit time.

### What this means

- **Verdict locked: fsync-bound commit pipeline** is the binder at pool=50, on this Docker-on-Mac stack.
- **Pool=25→50 was a real win** at the prior step (pool was a separate bottleneck behind commits). Order matters: removing pool ceiling first then exposed the commit ceiling. Both observations are valid; they're sequential bottlenecks.
- The +34.7% is a **synthetic experimental signal**, not a production recommendation. The relaxed knobs were restored immediately after the experiment. Default `1/1` durability is back in `/var/log` flush behavior.

## Recommended next experiment

1. Re-run the same experiment on AWS (cheapest path: existing `db.t4g.micro`, default Fargate sizing, `DB_MAX_OPEN_CONNS=50`). Re-check whether the bottleneck signature is the same or different.
2. If AWS shows the same commit-pipeline pattern: try `innodb_flush_log_at_trx_commit=2` via a custom parameter group, re-measure. Compare durability + throughput.
3. If AWS shows a different bottleneck (CPU on app side, or hits the 60-connection cap): tune that layer instead.

The cleanest claim for the resume bullet is **what we actually proved**, not what we hoped to prove:

> "Instrumented MySQL driver pool metrics via Prometheus (`go_sql_*`) and confirmed pool exhaustion in order-service under load (`in_use` pinned at `MaxOpenConns=25`, ~580k cumulative waits in 5 min). Raising pool 25→50 + idle 5→25 cut p95 latency 21% (140→110 ms) and total wait time 12×. Diagnosed the next bottleneck via `information_schema.processlist` + `SHOW ENGINE INNODB STATUS` — not row-lock contention (0 waits) but **commit-pipeline serialization** (40+ sessions in `waiting for handler commit`), pointing at storage `fsync` latency rather than the database CPU or query plan."

That's defensible end-to-end and shows the methodology, not just one tuning lever.
