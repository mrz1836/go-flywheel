# Benchmarks

The runtime's measured baseline: enqueue, claim, finalize, and sweep against real PostgreSQL at
100,000 jobs, plus a full-index versus correctness-index-only comparison — and the characterization at
1,000,000 rows that found the claim never reached its index, together with the fix that took claim p50
from 38.7 ms to 0.96 ms — and the worker pool that took slot utilization on a mixed-speed drain from
24.4 % to 95.4 %.

Every number here was produced by a committed command line against a committed JSON report under
[`docs/benchmarks/`](benchmarks/). Nothing is estimated, and every caveat a run discovered travels
with the numbers it qualifies.

<br/>

## Environment

| | |
|---|---|
| **Machine** | Apple M4, 10 cores, 16 GB, macOS 26.5.2 (arm64) |
| **Go** | go1.26.5 darwin/arm64 |
| **PostgreSQL** | 17.10 (Homebrew), local loopback, same machine as the harness |
| **Commit** | `1ecb5e9` (baseline, index comparison) · `c7dba1b` (heartbeat cost, sigkill and slow-job chaos) · `5ba4a08` (mass-expiry chaos, re-run after the fault's retry fix) · `218fee9` (claim characterization at 1M, and the claim before/after) · `047cdf2` → `f24eec1` (the worker pool and poll-ladder before/after, both halves on the same machine in one session) |
| **Seed** | `1` (every run) |
| **Workload digest** | `c111170f7509817c…` — identical across all three 100k runs |

**Non-default PostgreSQL parameters.** Everything else is the 17.10 default.

```
shared_buffers = 128MB      max_wal_size = 1GB          min_wal_size = 80MB
max_connections = 100       dynamic_shared_memory_type = posix
```

Defaults that matter to the numbers below: `work_mem = 4MB`, `synchronous_commit = on`, `fsync = on`,
`autovacuum = on`, `effective_cache_size = 4GB`, `checkpoint_timeout = 5min`, `wal_level = replica`.

**This is a developer laptop, not a server.** The client and the database share ten cores and one page
cache, so absolute throughput is a floor rather than a ceiling. The *relative* results — the index
comparison, the plan evidence, the barrier finding — do not depend on the hardware.

<br/>

## How to reproduce

```bash
export FLYWHEEL_LOADTEST_DATABASE_URL="postgres://localhost:5432/flywheel_test?sslmode=disable"

# 100k drain, full production index set.
go run -tags=loadtest ./loadtest/cmd/scenario -jobs 100000 -runners 4 -workers 8 \
  -mix drain -indexes full -out docs/benchmarks/baseline-100k.json

# The same run with only the correctness-bearing indexes.
go run -tags=loadtest ./loadtest/cmd/scenario -jobs 100000 -runners 4 -workers 8 \
  -mix drain -indexes correctness-only -out docs/benchmarks/baseline-100k-noperf.json

# 100k inserts through the producer API.
go run -tags=loadtest ./loadtest/cmd/scenario -jobs 100000 -mix enqueue \
  -out docs/benchmarks/baseline-100k-enqueue.json

# The claim-predicate matrix: every claim shape against every candidate index, at 1M rows.
go run -tags=loadtest ./loadtest/cmd/explain -jobs 1000000 -queues 3 \
  -out docs/benchmarks/claim-plans-1m-after.txt

# The worker pool, on the mix where the barrier cost the most: 10% of jobs at 20x.
go run -tags=loadtest ./loadtest/cmd/scenario -jobs 100000 -mix mixed-speed \
  -runners 4 -workers 8 -work 10ms -out docs/benchmarks/pool-mixed-8-after.json

# The poll-error ladder, against a 60-second database outage at 50% drained.
go run -tags=loadtest ./loadtest/cmd/scenario -jobs 10000 -mix drain \
  -fault pause-database:60s -out docs/benchmarks/poll-backoff-after.json

# The retry-backoff cap, against a simulated dependency outage. The worker fails
# transiently while an injected clock — advanced between drain rounds — compresses
# the ladder into seconds. The number is attempt volume per cohort.
for cap in 30m 1m; do
  [ "$cap" = 30m ] && out=backoff-outage.json || out=backoff-outage-1m.json
  go run -tags=loadtest ./loadtest/cmd/scenario -jobs 10000 -mix drain \
    -fault downstream-outage:4h -backoff-cap "$cap" -max-attempts 8 \
    -out "docs/benchmarks/$out"
done
# The same at a budget sized to ride the outage out: the cap decides survival.
for cap in 30m 1m; do
  [ "$cap" = 30m ] && out=backoff-survive-30m.json || out=backoff-survive-1m.json
  go run -tags=loadtest ./loadtest/cmd/scenario -jobs 10000 -mix drain \
    -fault downstream-outage:4h -backoff-cap "$cap" -max-attempts 25 \
    -out "docs/benchmarks/$out"
done

# Bounded maintenance at depth: a 1M drain under mass lease expiry.
go run -tags=loadtest ./loadtest/cmd/scenario -jobs 1000000 -mix drain \
  -work 2ms -lease 1s -fault mass-lease-expiry -timeout 60m -sample-interval 5s \
  -out docs/benchmarks/sweep-1m.json

# Retention against a seeded backlog plus live churn.
go run -tags=loadtest ./loadtest/cmd/scenario -jobs 200000 -mix steady \
  -duration 20m -timeout 90m -work 2ms \
  -retention 2m -retention-interval 30s -retention-batch 1000 \
  -terminal-seed 500000 -sample-interval 10s \
  -out docs/benchmarks/retention-under-load.json

# Storage parameters, A/B at a 1M working set. Same command twice.
for arm in untuned tuned; do
  [ "$arm" = tuned ] && tune=-storage-tuning || tune=
  go run -tags=loadtest ./loadtest/cmd/scenario -jobs 1000000 -mix steady \
    -duration 30m -timeout 120m -work 2ms \
    -retention 2m -retention-interval 30s -sample-interval 10s $tune \
    -out "docs/benchmarks/bloat-$arm.json"
done

# Replay a 30k-failure cohort and assert it re-converges. 35,000 children under one
# parent, 85.7% failing transiently, replayed with a restored budget over a 10m window.
go run -tags=loadtest ./loadtest/cmd/scenario -mix fan-out -children 35000 \
  -fail-fraction 0.857 -replay -replay-stagger 10m \
  -out docs/benchmarks/replay-30k.json

# Admission control: the same 50/s budget, enforced pre-claim vs. claim-then-snooze.
go run -tags=loadtest ./loadtest/cmd/scenario -jobs 2000 -mix drain \
  -limiter token-bucket -rate 50 -out docs/benchmarks/gate-budget.json
go run -tags=loadtest ./loadtest/cmd/scenario -jobs 2000 -mix drain \
  -worker-snooze 50 -out docs/benchmarks/gate-snooze-baseline.json
```

Three things about those four commands are load-bearing, and each was found by getting it wrong first.

- **`-timeout` must exceed `-duration`.** `Run` wraps the whole run in `Timeout`, which defaults to 30
  minutes, so a `-duration 2h` against an unset `-timeout` dies partway with a non-zero exit and a
  truncated artifact — after spending the thirty minutes. `validate()` now rejects the combination up
  front rather than leaving it to be discovered.
- **The retention window is 2 minutes, not an hour.** Retention deletes jobs finalized *longer ago
  than* the window. Nothing in a 20-minute run is an hour old, so an hour-long window prunes exactly
  zero and produces an artifact that looks like a working sweep against an empty backlog.
  `-terminal-seed` supplies a real starting backlog on top of the live churn, because the bulk seed
  path writes no `finalized_at` and no `job_runs` at all.
- **Both bloat arms carry retention.** The closed loop bounds the *working set*, not the table:
  retired jobs stay as terminal rows, so without retention the table grows by accumulation and churn
  at once and neither is separable. Retention is identical across the two arms, so the delta stays
  attributable to the storage parameters.

A bloat arm that ends below three autovacuum cycles is unfinished, and no slope should be quoted from
it. `AutovacuumCount` is sampled per tick.

The two `-after` runs above have committed `-before` halves taken by checking out `047cdf2` into a
worktree and cherry-picking only the harness's measurement commit onto it — `SlotUtilization` and
`BlockedClaims` did not exist before this release, and a before-half that could not report them would
have nothing to compare.

The two drain runs generate a byte-identical workload — same seed, same digest — so the only variable
between them is the index set. Concurrency does not reach the workload either: generation is
single-threaded and completes before any runner starts, so a run at `-runners 2 -workers 4` and one at
`-runners 8 -workers 8` produce the same digest. That is verified rather than asserted:

```bash
go run -tags=loadtest ./loadtest/cmd/scenario -jobs 5000 -runners 2 -workers 4 -quiet -out a.json
go run -tags=loadtest ./loadtest/cmd/scenario -jobs 5000 -runners 8 -workers 8 -quiet -out b.json
diff <(jq -r .workload_digest a.json) <(jq -r .workload_digest b.json)   # no output
```

<br/>

## Baseline — 100,000 jobs

`-runners 4 -workers 8`, no simulated per-job work, so the numbers are the database path rather than a
measurement of how long the harness chose to sleep.

| | Full indexes | Correctness only |
|---|---|---|
| **Drain throughput** | 797 jobs/s | 821 jobs/s |
| **Wall time** | 2 m 05 s | 2 m 02 s |
| **Claim p50 / p99** | 38.7 ms / 61.3 ms | 38.7 ms / 61.3 ms |
| **Finalize p50 / p99** | 0.96 ms / 12.5 ms | 0.78 ms / 3.13 ms |
| **Sweep p50 / p99** | **0.78 ms / 2.42 ms** | **12.5 ms / 19.4 ms** |
| `jobs` table | 89.3 MB | 80.7 MB |
| `jobs` indexes | 22.1 MB | 11.8 MB |
| WAL generated | 232 MB | 233 MB |
| Peak client RSS | 201 MB | 199 MB |
| Retried / discarded / reclaimed | 0 / 0 / 0 | 0 / 0 / 0 |
| Concurrent executions | 0 | 0 |

**Enqueue through the producer API:** 9,897 jobs/s (100,000 single-row inserts, one `Enqueue` call
each, nothing draining). The drain runs' enqueue figure is the harness's batch-insert rate and is not
comparable — the reports label it as such.

Percentiles carry a **±15.5 %** relative error from the histogram's bucketing, published in each
report's `histogram` object. Count, min, max, and mean are exact.

<br/>

## Bulk enqueue: `InsertMany` vs single-row

`InsertMany` writes N jobs in bounded, dialect-aware chunks — one multi-row `INSERT` per chunk under
`ON CONFLICT DO NOTHING` — where `Enqueue` writes one row per call. At 100,000 jobs with nothing
draining, the producer path is the whole measurement.

| | Before (single-row `Enqueue`) | After (`InsertMany`, chunk 1,000) | |
|---|---|---|---|
| **Enqueue throughput** | 9,897 jobs/s | **52,230 jobs/s** | **5.3×** |
| Statements | 100,000 | 100 | the chunk factor |

The before is the single-row baseline above ([`baseline-100k-enqueue.json`](benchmarks/baseline-100k-enqueue.json)),
reconfirmed at 8.5–9.8k jobs/s in the same session as the after. The after is `BenchmarkEnqueueBatched100k`
at `-count=5`, median of five ([`enqueue-batched-100k.txt`](benchmarks/enqueue-batched-100k.txt)).

### The chunk-size sweep

The default chunk size — 1,000 on PostgreSQL — is where the round-trip savings have flattened while the
statement stays far inside the 65,535 bind-parameter ceiling (1,000 × 22 columns = 22,000 parameters).

| Chunk size | Throughput | vs single-row | Statements |
|---|---|---|---|
| 250 | 30,798 jobs/s | 3.1× | 400 |
| 500 | 46,928 jobs/s | 4.7× | 200 |
| **1,000** (default) | **52,230 jobs/s** | **5.3×** | 100 |
| 2,000 | 52,960 jobs/s | 5.4× | 50 |

Doubling the chunk from 1,000 to 2,000 buys 1.4 % — by then the round trips are no longer the
bottleneck — so the default sits at 1,000 where the curve knees, roughly a third of the ceiling so a
future column addition cannot push a default-configured caller over it.

**The landed-row read-back is skipped when no row carries a unique key.** A row can only be dropped by
the conflict clause if it has a `unique_key` or `unique_active_key`; its id is a freshly minted value
that never pre-exists, so the primary key never collides. A chunk without unique keys therefore lands
whole and needs no `SELECT`-back to attribute — which is the difference between the numbers above and an
earlier form that paid an id-`IN` read-back per chunk, where the read-back dominated the insert by an
order of magnitude because a bulk load leaves the table's planner statistics stale. A uniquely-keyed bulk
load still pays that read-back per chunk.

<br/>

## The index comparison

The headline is not what the shape of the question suggests.

### The finding: the claim path never reached its index

> **Fixed in v0.7.1.** This subsection is what the baseline measured and why; the characterization that
> chose the fix, and the fix's own before/after, follow it. The baseline table above is the pre-fix
> state and is left as measured.

Claim p50 and p99 are *identical* with and without the performance indexes. That is not noise — it is
the planner declining `jobs_ready` in both cases, and the scan counters in the reports say so
directly: `jobs` records ~11,200 sequential scans against ~12,500 claims with the full index set.

`EXPLAIN (ANALYZE)` on the claim's inner select, over 100,000 claimable rows:

| Query | Plan | Time |
|---|---|---|
| As the runtime emits it, with `jobs_ready` absent | Seq Scan → external merge Sort (3.9 MB to disk) | 21.1 ms |
| As the runtime emits it, with `jobs_ready` present | Seq Scan → external merge Sort (3.9 MB to disk) | 21.2 ms |
| Same, with a single `executor_class = ?` equality | **Index Scan using `jobs_ready`** | **0.165 ms** |

`jobs_ready` is a good index — it is worth **128×** on this query. The claim simply cannot reach it,
in either of its two modes:

- **Routed mode** emits `(executor_class = ? OR executor_class = '')`. The `OR` sits on the index's
  second column, so no single ordered index scan can satisfy `ORDER BY priority, scheduled_at`, and
  the planner falls back to a full scan and a sort.
- **Wildcard mode** (`ClaimAnyClass`) omits the `executor_class` predicate entirely, leaving a gap in
  the index's leading columns — `(queue, ·, priority, scheduled_at)` — which is equally unusable for
  the ordering.

Every claim therefore sorts the whole claimable set and spills 3.9 MB to disk, and that sort is
essentially all of the 38.7 ms claim latency — the single largest lever the baseline exposed.

### Characterizing it: the claim predicate at 1,000,000 rows

The 100k plans above were captured by hand in `psql`, which is fine for a finding and not enough to
choose a fix by. `loadtest/cmd/explain` does it reproducibly and at depth. It does not contain a copy
of the claim SQL — the statement lives once, built by `fmt.Sprintf` inside an unexported driver method,
and a re-typed copy would drift from the runtime silently. Instead the tool **captures what the driver
emits**: it opens a pool with a recording `gorm/logger.Interface`, calls `Dequeue` inside a transaction,
takes the SQL back out of `Trace` with its bind values already interpolated, rolls the transaction back,
and `EXPLAIN (ANALYZE, BUFFERS)`-es that string. Every candidate spelling is derived from the captured
statement by one documented `strings.Replace`, so everything except the clause under test is
byte-identical.

```bash
go run -tags=loadtest ./loadtest/cmd/explain \
  -jobs 1000000 -queues 3 -out docs/benchmarks/claim-plans-1m-before.txt
```

One million rows, all claimable, spread evenly across three queues and four executor classes — so a
routed claim matches half the table, and a seed in which every row matched would flatter any candidate
that drops the class from the key. `LIMIT` is 8, a runner's `Concurrency` at the baseline's settings.
**A `Sort` over more than `LIMIT` rows is the failure signature.**

Six claim shapes:

| | Shape |
|---|---|
| A | 1 queue, routed by `executor_class` |
| B | 3 queues, routed |
| C | 1 queue, `ClaimAnyClass` |
| D | 3 queues, `ClaimAnyClass` |
| E | **idle** queue, routed — the empty poll |
| F | **idle** queue, `ClaimAnyClass` — the empty poll |

E and F are additions to the plan of record, and they turned out to decide the outcome. A runner polls
on a fixed interval whether or not there is work, so on any quiet queue the overwhelming majority of
claims a deployment ever issues return nothing. An index that answers a hit quickly and a miss slowly
is a worse deployment than the one it replaced.

Five index definitions, every one installed under the name `jobs_ready` so the plans are comparable.
V0 is pulled from `flywheel.IndexSet("postgres")` rather than re-typed:

| | Definition |
|---|---|
| V− | absent |
| V0 | as shipped: `(queue, executor_class, priority, scheduled_at)` |
| V1 | V0 + `AND deleted_at IS NULL` in the predicate |
| V2 | `(priority, scheduled_at, queue, executor_class)` + V1's predicate |
| V3 | `(queue, priority, scheduled_at)` + V1's predicate — class dropped from the key |
| V4 | V1 + `INCLUDE (id)`, under A only |

Execution time in milliseconds, `P0` = the predicate as the driver emits it:

| Cell | V− | V0 | V1 | V2 | V3 | V4 |
|---|---|---|---|---|---|---|
| A — 1 queue, routed | 170.9 | 153.6 | 144.7 | **0.19** | **0.19** | 144.3 |
| B — 3 queues, routed | 321.6 | 317.2 | 317.1 | **0.13** | 338.1 | — |
| C — 1 queue, any class | 221.1 | 219.6 | 217.0 | **0.12** | **0.13** | — |
| D — 3 queues, any class | 553.9 | 560.6 | 566.5 | **0.13** | 566.0 | — |
| E — idle queue, routed | 100.9 | 0.019 | 0.020 | **21.4** | 0.014 | — |
| F — idle queue, any class | 93.5 | 0.012 | 0.008 | **16.6** | 0.007 | — |

Every cell in the V−, V0, V1 and V4 columns sorts above its scan. No cell in the V2 or V3 columns does.
The full plans, both predicate spellings, and the buffer counts are in
[`claim-plans-1m-before.txt`](benchmarks/claim-plans-1m-before.txt).

Four things fall out of it:

- **The `IN` spelling changes nothing.** `executor_class IN (?, '')` — what the SQLite driver already
  writes — produces the same plan as the `OR` in every one of the 47 cells. PostgreSQL 17's btree
  `ScalarArrayOp` execution does not preserve the index ordering here. There is no one-line driver fix.
- **`deleted_at IS NULL` in the predicate is not a plan change.** V1 matches V0's shape everywhere; it
  buys about 6% on A's bitmap path and nothing elsewhere.
- **`INCLUDE (id)` cannot help.** V4 is within noise of V1 (144.3 vs 144.7 ms). The claim is
  `FOR UPDATE`, so it must visit the heap tuple to lock it no matter what the index carries. This is
  measured, not argued from the manual.
- **No single index wins every shape**, and the two that win anything win opposite halves.

The reason is one irreconcilable tension. To answer a selective or empty queue quickly, `queue` has to
lead the key. To satisfy `ORDER BY priority, scheduled_at` across *several* queues without a sort, the
ordering columns have to lead. V2 takes the second option and pays for it: on an empty poll it reads
**5,749 buffers — 45 MB of index — to return zero rows**, because `queue = 'q-idle'` can only be
applied as a non-leading index condition and the scan walks the whole thing.

### The remedy: `jobs_ready` keyed on `(queue, priority, scheduled_at)`

V3 ships, with `deleted_at IS NULL` folded into the predicate. Its case is that it **never loses**:
there is no cell in the matrix where it is worse than what shipped before it, and it is three orders of
magnitude better in the shapes it can reach.

```sql
-- before
CREATE INDEX jobs_ready ON jobs (queue, executor_class, priority, scheduled_at)
  WHERE state IN ('available', 'retryable', 'scheduled');
-- after
CREATE INDEX jobs_ready ON jobs (queue, priority, scheduled_at)
  WHERE state IN ('available', 'retryable', 'scheduled') AND deleted_at IS NULL;
```

Dropping `executor_class` from the key is what makes both routing modes work at once: there is no
longer a gap in the leading columns for `ClaimAnyClass` to fall into, and a routed claim gets its
ordered scan with the class applied as a heap filter. The filter is cheap at the measured selectivity —
A returns 8 rows having discarded 7, reading 12 buffers. `queue` still leads, which is what keeps the
empty poll a probe rather than a scan.

**The claim, per shape**, from [`claim-plans-1m-before.txt`](benchmarks/claim-plans-1m-before.txt) and
[`claim-plans-1m-after.txt`](benchmarks/claim-plans-1m-after.txt) — same seed, same 1M rows, the index
definition the only variable:

| Shape | Before | After | |
|---|---|---|---|
| A — 1 queue, routed | 153.6 ms, Bitmap + Sort | **0.179 ms, Index Scan** | **858×** |
| C — 1 queue, `ClaimAnyClass` | 219.6 ms, Bitmap + Sort | **0.135 ms, Index Scan** | **1,627×** |
| E — idle queue, routed | 0.019 ms, Index Scan | **0.014 ms, Index Scan** | unchanged |
| F — idle queue, `ClaimAnyClass` | 0.012 ms, Index Scan | **0.013 ms, Index Scan** | unchanged |
| B — 3 queues, routed | 317.2 ms | 334.5 ms | unchanged (both scan and sort) |
| D — 3 queues, `ClaimAnyClass` | 560.6 ms | 583.5 ms | unchanged (both Seq Scan) |

**End to end**, the 100k drain at `-runners 4 -workers 8`, run at the same commit before and after:

| | Before | After | |
|---|---|---|---|
| **Claim p50** | 38.75 ms | **0.96 ms** | **40× faster** |
| **Claim p99** | 61.26 ms | **1.91 ms** | 32× faster |
| **Drain throughput** | 735 jobs/s | **9,474 jobs/s** | **12.9×** |
| **Wall time** | 2 m 16 s | **10.6 s** | 12.9× |
| Finalize p50 / p99 | 1.56 ms / 12.5 ms | 1.21 ms / 2.42 ms | |
| **`jobs` sequential scans** | **10,804** | **53** | against 12,682 claims |
| `jobs` index bytes | 21.9 MB | 19.5 MB | the new key is one column shorter |
| Workload digest | `c111170f7509817c…` | `c111170f7509817c…` | identical |
| Retried / discarded / superseded | 0 / 0 / 0 | 0 / 0 / 0 | |

The scan counters are the mechanism stated without inference. The baseline recorded ~11,200 sequential
scans of `jobs` against ~12,500 claims — one full scan per claim. After the change there are **53**, and
the claim count is unchanged. Nothing else about the run moved.

Reports: [`claim-100k-before.json`](benchmarks/claim-100k-before.json),
[`claim-100k-after.json`](benchmarks/claim-100k-after.json). The digests match, so the two runs drained
a byte-identical workload.

One number moved the *wrong* way and is an artifact rather than a regression: the `jobs` table ends at
104.5 MB against 89.1 MB. The run finished in 10.6 s instead of 2 m 16 s, so autovacuum had a ninth of
the wall clock in which to reclaim the dead tuples the drain produced. The index footprint, which
autovacuum timing does not explain, went *down*.

**Why not V2, which won B and D as well.** Trading a 1,100× regression on the operation a deployment
issues most often — the empty poll, 0.019 ms against 21.4 ms — for a win on the one it issues least is
not a trade worth making.
A claim that scans while draining a backlog is at least doing useful work; an empty poll that reads
45 MB of index to return nothing is not, and it recurs on every poll interval forever.

**What is still slow, and the supported answer.** A claim naming more than one queue (B, D) remains a
Seq Scan and a sort. One index cannot serve it — `queue` cannot both lead the key and be absent from
it. The fix is a runner per queue, which is already how the runtime is meant to be deployed and costs
nothing to adopt:

```go
// Instead of one runner over three queues...
flywheel.NewRunner(flywheel.RunnerConfig{Queues: []string{"a", "b", "c"}, /* ... */})
// ...run three, one per queue. Each one gets the ordered index scan.
flywheel.NewRunner(flywheel.RunnerConfig{Queues: []string{"a"}, /* ... */})
```

**The one residual risk**, stated because it is the mirror image of what sank V2: with the class out of
the key, a routed claim scans a queue's ready set in priority order and filters by class at the heap, so
its cost scales as `LIMIT ÷ (fraction of the queue's ready rows the class predicate matches)`. At the
measured 50% that is 15 index entries for 8 rows. It degenerates only where a single queue carries many
executor classes, one of them starved, *and* no job uses the empty-wildcard class — because the claim's
predicate is `class OR ''`, so any wildcard row is a match. Unlike V2's, this failure needs three
conditions to hold at once rather than one, and it is bounded by a single queue rather than by the
table. A queue per executor class removes it entirely.

**Upgrading an existing database.** `InstallIndexes` and `Migrate` both use `CREATE INDEX IF NOT EXISTS`,
which matches on **name only** — so a database that already carries an index named `jobs_ready` keeps
the old definition, and the install step still reports success. Fresh installs and new test schemas get
the new definition automatically. An existing database needs the drop done once:

```sql
DROP INDEX jobs_ready;   -- then InstallIndexes / Migrate, or your own migration, recreates it
```

A host with hand-written migrations adds a `DROP INDEX` + `CREATE INDEX` pair instead. There is no
version check that can do this for you today: the installer compares names, not definitions.

### Where the performance indexes do pay: the sweep

| | Full indexes | Correctness only | Delta |
|---|---|---|---|
| Sweep p50 | 0.78 ms | 12.5 ms | **16× slower** |
| Sweep p99 | 2.42 ms | 19.4 ms | **8× slower** |

`jobs_running_leased` turns "find expired leases" from a full scan of 100,000 rows into an index probe
that finds nothing. This is the steady-state cost a scheduler pays on every interval for the entire
life of a deployment, and it is measured here on a sweep that reclaims **zero** jobs — which is the
normal case, not a degenerate one.

### What the performance indexes cost

10.3 MB of index on 100,000 rows — 22.1 MB against 11.8 MB, so they are 47 % of the index footprint —
plus their maintenance on every insert and every state transition. WAL generation is unchanged within
noise (232 MB against 233 MB).

That pair predates the claim fix, which shortened `jobs_ready` by a column and narrowed its predicate:
the same 100k drain now ends with 19.5 MB of `jobs` index rather than 21.9 MB, so the performance
indexes cost about 2.4 MB less than the figure above.

<br/>

## Bloat and WAL trajectory

Sampled once per second across the drain. The row *count* never changes — 100,000 jobs are seeded
before the clock starts and none are added — so everything below is the cost of updating them.

| | Start | End | Change |
|---|---|---|---|
| `jobs` table | 61.7 MB | 89.3 MB | **+45 %** |
| `jobs` indexes | 15.7 MB | 22.1 MB | +41 % |
| `job_runs` table | 0.5 MB | 39.0 MB | (one audit row per attempt) |
| Dead tuples in `jobs` | 31 | **98,377** | ≈ 1 per live row |
| Autovacuum passes on `jobs` | — | 2 | in 2 m 05 s |
| WAL | — | 232 MB | **2,322 bytes/job** |

Each job takes two updates on its `jobs` row — the claim sets `state`, `attempt`, and `leased_until`;
the finalize sets `state` and `finalized_at` — and PostgreSQL's MVCC makes each one a new tuple
version. Two hundred thousand updates over 100,000 rows leaves the table half again as large as it
started, with roughly as many dead tuples as live rows still uncollected when the run ends. Autovacuum
ran twice and did not catch up.

The correctness-only run is worse on both counts, ending at 121,444 dead tuples and +55 % table
growth. Its index footprint nearly doubles (6.0 MB → 11.8 MB) — a smaller index set bloats by a larger
*fraction*, while remaining about half the absolute size.

This is a finding rather than a tuning result: nothing here was tuned. It says the runtime's
steady-state storage cost is dominated by update churn on `jobs`, not by row growth, and that the
default autovacuum settings do not keep pace with a 100k drain at this rate. *The storage-parameter
comparison below is what that finding turned into.*

<br/>

## Bounded maintenance

Both maintenance operations — the lease sweep and the retention prune — work in bounded batches, one
transaction per batch. This section is why.

### The unbounded versions did not degrade at depth. They stopped working.

Measured against the pre-change code, because these numbers are unobtainable afterwards. Both paths
built a single `IN (?)` list and bound one parameter per row, and PostgreSQL's extended protocol
rejects a statement carrying more than **65,535** of them.

| Path | Failing statement | Exact ceiling |
|---|---|---|
| lease sweep | `UPDATE jobs … WHERE id IN (?)` | **65,531 rows** |
| retention | `DELETE FROM job_runs WHERE job_id IN (?)` | **65,535 rows** |

The four-row difference is the sweep's `SET` clause — `state`, `leased_until`, `lease_token`,
`updated_at` are four bind parameters of their own. Verified by bisection: 65,531 reclaims in 1.769 s,
65,532 fails outright with `extended protocol limited to 65535 parameters`.

Past the ceiling it is a hard error, not a slow path. An executor pool that died holding more than
~65k leases left a backlog the sweep could **never** clear — every subsequent attempt failed on the
same statement — and the sweep is the only recovery path for work lost to a crashed process. Retention
had the same cliff, and a first prune against a long-lived database is exactly where it is met.

Below the ceiling, transaction age was linear in backlog at ≈26 µs/row, holding a row lock on every
reclaimed job throughout:

| Backlog | Sweep wall time | Peak server-side transaction age |
|---|---|---|
| 10,000 | 230 ms | 0.202 s |
| 25,000 | 604 ms | 0.602 s |
| 50,000 | 1.263 s | 1.252 s |
| 65,531 | 1.769 s | 1.752 s |
| 65,532 | — | **fails** |

### After: 1,000,000 jobs, and the longest transaction is 76 ms

`docs/benchmarks/sweep-1m.json` — a 1M-job drain under the mass-lease-expiry fault.

| | |
|---|---|
| Jobs drained | 1,000,000 |
| Peak **client-backend** transaction age, whole run | **0.076 s** |
| Sweep p50 / p99 | 9.7 ms / 1.96 s |
| Leases reclaimed / attempts superseded | 44 / 44 |
| Concurrent executions | **0** |
| Peak ungranted locks / longest wait | 1 / 1.1 ms |

The 76 ms is the number the rewrite exists to produce: across a million jobs, no client transaction
held a snapshot longer than that. Reclaiming 44 leases is not a weak test — it is the ceiling. Only a
*held* lease can expire, and the in-flight set is bounded by the connection budget, so no live
scenario reaches a deep backlog. The 200k-backlog case is covered by an integration test that writes
the rows directly, which is the only way to reach it.

Two numbers in that table measure different things and are easy to conflate. The **server-side** age
comes from `pg_stat_activity` on a sampling interval; the **client-side** `Sweep.max` of 7.25 s is one
`Sweep` call end to end, including connection acquisition and Go scheduling on a saturated box. The
gap between 0.076 s and 7.25 s is that overhead, not a transaction. See the sampling caveat below for
which to quote when.

### Retention under concurrent load

`docs/benchmarks/retention-under-load.json` — 20 minutes of concurrent enqueue and drain at a 200k
working set, against a 500k seeded terminal backlog with a 2-minute window.

The first pass removed all 500,000 in bounded batches. Subsequent passes tracked live churn at
80k–190k per 30-second tick, with zero concurrent executions throughout.

> **`Drained` is a residual, not a count.** It is read from the table after the run: how many rows
> were still in a terminal state at the end. With retention deleting continuously, that is what had
> *not yet been pruned* — this run reports `drained: 357,912` while actually draining about
> **3,376,000** jobs. The event-based figure is `DrainThroughput × Duration`. Newer runs emit a note
> carrying both; these artifacts predate it.

<br/>

## Storage parameters: measured, then adopted

The 100k finding above said update churn dominates and the autovacuum defaults do not keep pace. This
is the A/B that turned it into the settings `Migrate` and `InstallStorageParameters` now apply.

Two 33-minute runs at a **1,000,000-job working set**, identical in every respect except the storage
parameters on `jobs`. Both carry the same retention settings, so the terminal tail stays bounded and
growth is attributable to churn rather than accumulation; retention being identical across the two
keeps the delta attributable to the storage parameters alone.

`docs/benchmarks/bloat-untuned.json` · `docs/benchmarks/bloat-tuned.json`

| | Default | Tuned |
|---|---|---|
| **Table growth, final third** | **+19.3 MB/min** | **−4.8 MB/min** |
| Index growth, final third | +19.2 MB/min | +4.6 MB/min |
| Dead tuples, final | 4,716,750 | **793,208** |
| Dead tuples, peak | 7,010,561 | 1,920,226 |
| `jobs` table, final | 2,809 MB | 2,165 MB |
| Autovacuum cycles | 5 | **29** |
| Drain throughput | 2,596 jobs/s | **3,947 jobs/s** |
| **WAL per job** | **7.0 KB** | **10.8 KB** |

The default arm grows monotonically. The tuned arm *shrinks* over its final third — autovacuum
reclaiming faster than churn adds. Both ran well past three autovacuum cycles, below which a slope
means nothing: table size under autovacuum is a saw-tooth, and a first-versus-last comparison across
a partial cycle measures where the run happened to stop.

**The cost is real.** WAL rises 54 % per job. Vacuuming more often writes more WAL, and a fillfactor
below 100 puts fewer tuples on a page. A deployment paying for replication bandwidth or backup storage
by the byte should weigh that against the bloat and throughput it buys.

**Three honest qualifications.**

- **The condition bundles both settings**, so this does not attribute the win between them. The
  5-to-29 cycle count points hard at `autovacuum_vacuum_scale_factor`, which is also the only one of
  the two with a mechanism that plainly applies here.
- **`fillfactor` does not enable HOT updates on this table**, and the common claim that it does is
  false here. A HOT update requires that no index-relevant column change, and `state` sits in the
  *predicate* of `jobs_ready`, `jobs_unique_active_key`, `jobs_running_leased`, and `jobs_state`. A
  predicate column is index-relevant, so every state transition is non-HOT whatever the fillfactor
  is — before these settings, and before `jobs_state` existed. What fillfactor buys is same-page
  placement for the new tuple version.
- **The two arms drained different amounts of work** (2,596 against 3,947 jobs/s), so absolute sizes
  are not directly comparable between them. The slope over the final third is the comparison that is,
  which is why it is the one reported.

`job_runs` and `job_periodics` get no parameters: neither has update churn, and a lower fillfactor on
an append-only table reserves free space on every page for updates that never come.

<br/>

## The queue-state telemetry index

`jobs_state` is `(state) WHERE deleted_at IS NULL`. It serves three readers, not the two the
counts-by-state reads are usually described as: `SampleQueueHealth`, `Overview`, and `CountActiveJobs`
— the last of which the harness itself polls every 250 ms during a run, which puts it inside the
window every number in this document is measured in.

Its plan is gated with `SET LOCAL enable_seqscan = off`, and that is deliberate rather than
convenient. An index-only scan consults the visibility map per heap page, and this table churns hard
enough that `relallvisible` sits near zero under load — so the planner **correctly** prices a
sequential scan lower, and a gate asserting the unforced choice would fail whenever autovacuum was
behind, for a reason that is not a defect. What is gated instead is what a schema change can silently
break: the index's shape against the query's, read back from `pg_indexes.indexdef` rather than probed
by name. Probing by name is exactly what let a stale `jobs_ready` definition pass a green suite here
before.

Forced, the counts read reaches `Index Only Scan using jobs_state`. On SQLite the plan degrades
without it from `SCAN jobs USING INDEX jobs_state` to `SCAN jobs` plus `USE TEMP B-TREE FOR GROUP BY`
— verified by dropping the index, because an assertion never seen to fail is not yet a gate.

An honest ceiling on what to expect: at 1M rows `jobs` is roughly 890 MB against a ~30 MB partial
index, so the IO ceiling is about 30×. Parallelism and a cold visibility map erode it to roughly
**3–10×** on a quiet queue and closer to **1×** during a heavy drain. `Overview` with a `Kind` filter
cannot use it — that needs `(kind, state)` — and is a documented limitation rather than a defect: it
is an inspection query a human runs, not a scrape path something polls.

<br/>

## Throughput was barrier-bound, not database-bound — and the barrier is gone

This section is the history of the single most misleading number in this document, kept because every
figure above it was measured under the barrier and cannot be compared against a later one without it.

**What the barrier was.** `Concurrency` was a single knob doing two jobs: the claim batch size *and*
the in-batch parallelism. A poll claimed N jobs, dispatched them across N goroutines, and waited for
all of them before claiming again. The next claim could not start until the slowest member of the
current batch had finished.

At the original baseline that meant a runner spent one claim latency (38.7 ms) for every 8 jobs:
4 runners × 8 jobs / 38.7 ms ≈ 827 jobs/s, the measured throughput almost exactly. The drain was paying
for the claim, and the claim was paying for the sort.

**That prediction was tested twice, and held twice.** The claim fix took claim p50 from 38.7 ms to
0.96 ms and throughput from 735 to 9,474 jobs/s — but not by the 40× the claim itself improved by,
because the barrier reassigned the wait rather than removing it. A batch then cost one claim (≈1 ms)
plus the slowest of its eight finalizes (≈2 ms), and 4 runners × 8 jobs / ≈3 ms lands near the measured
9,474 jobs/s.

### The removal, measured

`Concurrency` is now the pool size: the runner claims to fill its free slots and dispatches each job
independently, so a slot frees the moment its own job finalizes.

The mix matters enormously here, and it is the whole reason this is reported twice.

**On a uniform workload the change is worth nothing measurable**, which is the arithmetic above
predicting its own irrelevance: the slowest of eight identical finalizes is the median of eight
identical finalizes, so there is no straggler to wait for. `BenchmarkClaim100k` at `-count=5`, taken on
the pre-change tree and the branch on the same machine in the same session:

```
             │ baseline.txt │            new.txt            │
             │    jobs/s    │    jobs/s      vs base        │
Claim100k-10   9.975k ± ∞ ¹   9.970k ± ∞ ¹  ~ (p=0.310 n=5)
```

Claim p99 moved one histogram bucket the wrong way — 1.91 ms to 2.42 ms, adjacent bucket edges — which
is what the bucketing's published relative error permits and is not a finding at five samples. The
pool claims far more often in smaller batches, so that direction is expected.

**On the mixed-speed mix the change is worth 4.2×.** 10 % of jobs at 20× the base work duration, which
is the shape `loadtest/workload.go` chose so that 1 − 0.9⁸ ≈ 57 % of eight-job batches contain a
straggler:

| | Before (barrier) | After (pool) | |
|---|---|---|---|
| **Slot utilization** | **24.4 %** | **95.4 %** | **3.9×** |
| **Drain throughput** | 247 jobs/s | **1,036 jobs/s** | **4.2×** |
| **Wall time** | 6 m 44.5 s | **1 m 36.6 s** | 4.2× |
| Claim p50 / p99 | 1.56 ms / 30.6 ms | **0.48 ms / 1.56 ms** | |
| Finalize p50 / p99 | 1.56 ms / 100.0 ms | **0.61 ms / 1.91 ms** | |
| Claims issued | 12,587 | 91,571 | smaller batches, far more often |
| Retried / discarded / superseded | 0 / 0 / 0 | 0 / 0 / 0 | |
| Workload digest | `ba1a209fd097f8ec…` | `ba1a209fd097f8ec…` | identical |

Reports: [`pool-mixed-8-before.json`](benchmarks/pool-mixed-8-before.json),
[`pool-mixed-8-after.json`](benchmarks/pool-mixed-8-after.json). The digests match, so both runs drained
a byte-identical workload.

**Read `SlotUtilization` as a floor.** It is the summed duration of every finished and superseded
attempt over `Runners × Workers × Duration`, and an attempt's duration runs from its run stub to its
finalize — so the claim, stub, and finalize round trips around each job are capacity the pool was using
that this does not count. The before/after at one configuration is what carries the claim, not the
absolute figure.

The p99 columns are the barrier stated from a second direction. A finalize p99 of 100 ms on a workload
whose slowest job is a 200 ms sleep is not the database being slow: it is finalizes queueing behind the
barrier. Under the pool the same finalize p99 is 1.91 ms, and the *only* thing that changed is when
`dispatch` is called.

What still holds from the old arithmetic: **finalize is the honest per-job number.** It is per-job and
unbatched, and its p50 of 0.61–1.56 ms is a real per-operation cost that does not depend on the
dispatch shape above it.

<br/>

## A database outage: the poll-error ladder

Consecutive poll failures now climb an exponential ladder from `PollInterval` to `MaxPollBackoff`
(30 s by default) with jitter, resetting on the first success. Before it, a failing database was polled
at the empty-queue rate forever, with an error log line per attempt.

The harness gates every runner's driver for 60 seconds at 50 % drained and counts the claims the gate
refused — which is the only measurement available, because a gated call deliberately records no latency
observation:

| | Before | After | |
|---|---|---|---|
| **Claims refused during the 60 s outage** | **42,676** | **56** | **762× fewer** |
| Per runner | 10,669 | 14 | |
| Error log lines | one per refused claim | one per refused claim | the ladder is what bounds both |
| Drain throughput (whole run) | 163 jobs/s | 160 jobs/s | unchanged — the outage dominates |
| Wall time | 1 m 01.3 s | 1 m 02.5 s | |
| Retried / discarded / superseded | 0 / 0 / 0 | 0 / 0 / 0 | |

Reports: [`poll-backoff-before.json`](benchmarks/poll-backoff-before.json),
[`poll-backoff-after.json`](benchmarks/poll-backoff-after.json).

**14 per runner is the ladder, not a coincidence.** The harness polls at 5 ms, so the rungs are
5 ms, 10, 20, … doubling to the 30 s ceiling, and the first thirteen sum to 41.0 s with the fourteenth
reaching past the 60 s window. Before the ladder, 60 s ÷ 5 ms ≈ 12,000 attempts per runner is the
number, and 10,669 measured is that with the gate's own overhead taken out.

Two caveats that travel with these numbers:

- **The run got 1.2 s longer, and that is the shipped default showing through.** A runner that has
  climbed to a 20 s rung sleeps out the rest of it after the gate reopens. The fault fires on drain
  fraction rather than wall time, so at the 30 s ceiling the overshoot can be up to 30 s. That is
  recorded rather than tuned away, so the published number reflects what a deployment actually gets.
- **Throughput is unchanged because the outage is the whole story.** Both runs spend 60 of their
  ~61 seconds gated. The ladder buys a database that is not hammered while it recovers and a log that
  does not fill a disk; it does not buy throughput, and it is not published as though it did.

<br/>

## A dependency outage: the retry-backoff cap

The poll ladder above governs how a *runner* retries a database it cannot reach. A separate ladder
governs how a *job* retries a dependency that fails its work, and it has its own ceiling —
`MaxRetryBackoff`, one minute by default. When a dependency's outages are measured in hours, the
one-minute ceiling spends a job's whole attempt budget in minutes; raising the ceiling spreads the same
budget across the outage.

The harness fails every worker transiently for a simulated outage and lets the runners' own backoff
ladder reschedule the cohort. Time is compressed: an advanceable clock is injected into the runners, and
the harness steps it forward between drain rounds once each retry generation has quiesced, so a
multi-hour ladder runs in seconds of wall time without shortening a single rung. Every run is 10,000
jobs draining a no-op worker; the number is the attempt volume — one `job_runs` row per attempt.

**A short budget: the cap is free.** With `-max-attempts 8`, eight attempts fit below both ceilings
(the ladder reaches 64 s before it would clamp), so the cohort is discarded either way for the same
attempt volume:

| `-backoff-cap` | Attempts | Per job | Outcome | Sim time the ladder rode out |
|---|---|---|---|---|
| `1m` | **80,000** | 8.0 | 10,000 discarded | 3 m 35 s |
| `30m` | **80,000** | 8.0 | 10,000 discarded | 3 m 49 s |

Reports: [`backoff-outage-1m.json`](benchmarks/backoff-outage-1m.json),
[`backoff-outage.json`](benchmarks/backoff-outage.json). The exponential ladder makes attempt volume
nearly cap-independent: raising the ceiling costs nothing in attempts or audit rows.

**A budget sized for the outage: the cap decides survival.** The point of the cap is a budget large
enough to ride out a long outage. At `-max-attempts 25` against a four-hour outage, the two ceilings
diverge completely:

| `-backoff-cap` | Attempts | Per job | Outcome | Sim time the ladder rode out |
|---|---|---|---|---|
| `1m` | 250,000 | 25.0 | **10,000 discarded** | 24 m 55 s |
| `30m` | **180,000** | 18.0 | **10,000 drained** | **4 h 27 m** |

Reports: [`backoff-survive-1m.json`](benchmarks/backoff-survive-1m.json),
[`backoff-survive-30m.json`](benchmarks/backoff-survive-30m.json).

The one-minute cap saturates at 60 s after seven rungs, so twenty-five attempts span under twenty-five
minutes — the budget is spent while the four-hour outage is still going, and every job is discarded
having burned all 25 attempts against a dependency that could not answer. The thirty-minute cap keeps
climbing (`1s → 2s → … → 17m → 30m`), so the same twenty-five-attempt budget spans four and a half
hours; the outage lifts around attempt eighteen and the whole cohort *survives* — drained, not
discarded, on **fewer** attempts (18 vs 25 per job). The cap is not a throughput knob and does not buy
one; it buys the difference between riding out an outage and failing every job partway through it.

Two caveats travel with these numbers, both from the time compression:

- **Simulated time, wall-clock measurement.** The four-and-a-half-hour ladder ran in 3 m 38 s of wall
  time. The compressed clock governs only when the runtime considers a retry due; `StartedAt`,
  `Duration`, and the throughput figures are real time, as everywhere else in this document.
- **Attempt volume is the deliverable, not survival timing.** The harness sizes the outage far past the
  ladder and steps the clock only once a generation has quiesced, so every scheduled retry is claimed
  before its rung is skipped — which makes the attempt counts above exact (`8×`, `25×`, and the `18×`
  where the outage lifts mid-ladder) rather than a function of wall-clock luck.

<br/>

## Measurement caveats

Every one of these is recorded in the `notes` array of the reports themselves, so they travel with the
numbers rather than living only here.

- **RSS is the harness client process, not the PostgreSQL server.** On this machine it was collected
  from `getrusage(RUSAGE_SELF).ru_maxrss`; macOS has no current-RSS source in the standard library, so
  the sampled series is a monotone high-water envelope rather than a live reading. On Linux the
  harness reads `/proc/self/statm` instead.
- **`pgstattuple` was not installed**, so free-space and dead-tuple percentages are absent from these
  reports rather than reported as zero. `CREATE EXTENSION pgstattuple` collects them.
- **Lock waits are a count *and* a duration.** `LockWaits` samples `pg_locks` instantaneously, and
  real lock waits are far shorter than the sampling interval, so a run of zeroes there means the
  sampler did not catch one rather than that there was no contention. `LongestLockWait` is the
  companion that survives being sampled: it measures how long the longest blocked backend has been
  waiting, which stays non-zero for the duration of a wait instead of only at the instant it is
  observed. Reports predating this pair carry only the count.
- **`MaxXactAge` has a one-sided guarantee, and it decides which number to quote.** A sampler at
  interval *I* is *guaranteed* to observe any transaction living longer than *I*, at an age of at
  least *L − I*; anything shorter may be missed entirely. That makes the server-side age the right
  instrument for an unbounded maintenance transaction and the wrong one for a bounded batch — the
  bounded sweep's batches finish in single-digit milliseconds and are invisible to it by design. For
  bounded maintenance the figure to quote is the client-side `Sweep.Max`, which is exact rather than
  bucketed and covers exactly one transaction per call, at the cost of including pool acquisition.
- **The contention numbers are database-scoped, not schema-scoped.** The harness isolates a run by
  schema *within* one database, so a concurrent run against the same database appears in
  `MaxXactAge`, `LockWaits`, and `LongestLockWait`. Publish contention figures only from a run that
  had the database to itself.
- **WAL is cluster-wide.** `pg_stat_wal` has no per-database breakdown, so any other activity on the
  server is included. These runs had a quiet server.
- **Superseded is a measurement, and was not always.** Before v0.7.0 the runtime computed the supersede
  signal during finalize and discarded it, so it was unobservable through both the driver and the
  observer interface, and the harness inferred it from audit-table residue. It now counts
  `OnSupersede` events. The residue query survives as an independent cross-check: the two count
  different things — the residue misses a superseded attempt whose job later succeeded on a retry —
  and a report notes a disagreement rather than asserting one.
- **The sweep numbers come from a real `Scheduler`.** The runner does not sweep; nothing in its
  dispatch loop calls `Sweep`. The harness therefore runs the runtime's own `Scheduler` on a
  one-second sweep interval, with the harness's timing driver injected — which is what an injected
  `Driver` is for, and what makes these numbers describe the loop a deployment runs rather than a
  hand-rolled imitation of it.

  The scheduler gets its own connection pool, sized to its concurrent activity count. Sharing the
  runners' pool would let a sweep queue behind a saturated work pool, and that wait would land inside
  the reported sweep latency — measuring the harness's pool sizing rather than the runtime.

<br/>

## Chaos: worker kill mid-drain

A runner is killed 40 % of the way through a 2,000-job drain by gating its driver — leaving its claims
claimed, its run stubs reading `started`, and its finalizes never issued, which is the row shape a
`SIGKILL`ed executor leaves behind.

Result: every job reaches a terminal state, every stale stub is marked `crashed` with a `finished_at`,
and no job runs twice — checked three independent ways (one success row per job, no two attempts on a
job overlapping in time, and an in-process execution tracker). It runs in CI on every pull request.

```bash
go test -tags=loadtest -run TestWorkerKillMidDrainReclaimsEveryJob ./loadtest/
```

<br/>

## Chaos: the lease, the heartbeat, and the fence

Three scenarios, each committed as a report under `docs/benchmarks/`. Every one of them ends with
`concurrent_executions: 0` and an empty `errors` array.

| Scenario | Report | Jobs | Shape | Reclaimed | Superseded | Drained |
|---|---|---|---|---|---|---|
| Runner killed at 40 % | `chaos-sigkill.json` | 100,000 | drain, 5 s lease | 8 | 0 | 100,000 |
| Jobs that outlive their lease | `chaos-slow-job.json` | 10,000 | mixed-speed, 4 s jobs vs. 2 s lease | **0** | **0** | 10,000 |
| Every lease expired at once | `chaos-mass-expiry.json` | 50,000 | drain, 25 ms jobs, 1 s lease | 16 | 16 | 50,000 |

```bash
export FLYWHEEL_LOADTEST_DATABASE_URL="postgres://localhost:5432/flywheel_test?sslmode=disable"

go run -tags=loadtest ./loadtest/cmd/scenario -jobs 100000 -mix drain \
  -fault kill-worker@0.4 -out docs/benchmarks/chaos-sigkill.json
go run -tags=loadtest ./loadtest/cmd/scenario -jobs 10000 -mix mixed-speed \
  -work 200ms -lease 2s -out docs/benchmarks/chaos-slow-job.json
go run -tags=loadtest ./loadtest/cmd/scenario -jobs 50000 -mix drain \
  -work 25ms -lease 1s -fault mass-lease-expiry -out docs/benchmarks/chaos-mass-expiry.json
```

**The slow-job run is the one to read first, and its interesting number is a zero.** The mixed-speed
mix gives a tenth of its jobs twenty times the base work duration, so at `-work 200ms` the slow decile
runs for four seconds — against a **two-second lease**. Before renewal every one of those thousand
jobs would have been reclaimed mid-flight and handed to a second runner. Measured: `reclaimed: 0`,
`superseded: 0`, ten thousand jobs drained, nothing run twice. The heartbeat held every lease across
two full lease-lengths of work.

Its throughput — 13 jobs/s — is not a heartbeat cost. It is the batch barrier meeting a bimodal
workload: a poll waits for its whole claimed batch, and at eight workers most batches contain one of
the four-second jobs. That is the effect the mix exists to expose; see *Throughput is barrier-bound*
above.

**The mass-expiry run is the fence under load.** Half way through the drain every held lease is pushed
into the past and swept immediately, so sixteen in-flight jobs are reclaimed and re-dispatched under
fresh tokens while their original attempts are still running. All sixteen originals then finalized
into a claim they no longer held: `superseded: 16`, and not one of them advanced a job's state,
enqueued a follow-up, or extended a lease. Every one of the fifty thousand jobs still reached
`succeeded`.

Two details of that scenario are worth recording, because both were measurement bugs first.

- **The fault sweeps as part of its own injection** rather than leaving the reclaim to the harness's
  one-second sweeper. An attempt is in flight only for the length of one job, so the chance the
  sweeper ticks inside that window is the job duration over the sweep interval — 2.5 % at a 25 ms job.
- **It retries until it catches a non-empty in-flight set.** Drain progress is counted from
  `OnFinish`, which is exactly the event that empties the running set, so the instant progress crosses
  the threshold is an instant when nothing is running. Fired once, the fault expired **zero** leases on
  three consecutive 50,000-job runs while a concurrent poll of the same table showed 32 rows running
  throughout.

<br/>

## What the heartbeat costs

Renewal is an `UPDATE` per running job per interval, against an indexed table. That is real write
amplification and it deserves a number rather than a reassurance.

**The arithmetic first, because it is the part that generalizes.** An attempt pays roughly
`duration / interval` extra `UPDATE`s, where the interval is `max(1s, LeaseDuration/3)`:

| Job duration | Lease | Interval | Extra `UPDATE`s |
|---|---|---|---|
| 25 ms | 1 s | 1 s | 0.025 — *an attempt usually ends before an interval elapses* |
| 30 s | 30 s (default) | 10 s | 3 |
| 10 min | 30 s (default) | 10 s | 60 |
| 10 min | 5 min | 100 s | 6 |

A workload whose jobs are shorter than the interval pays essentially nothing. A workload whose jobs
are much longer pays proportionally — and a longer lease reduces the bill linearly, which is the knob
to reach for if the write volume matters more than how fast a crash is noticed.

**The measurement.** A same-binary A/B over a 100,000-job drain at 25 ms per job against a one-second
lease, `-count=5`, differing only in `Config.Heartbeat`:

| | Median throughput | Median wall clock |
|---|---|---|
| Renewal disabled | 486.6 jobs/s | 205.5 s |
| Renewal enabled (default) | 481.3 jobs/s | 207.8 s |

**Read that as "no resolvable difference", not as "1 % slower".** The run-to-run spread within the
disabled set alone is 421.5–492.6 jobs/s — 17 % — so a 1 % gap between the two medians is well inside
the noise, and `benchstat` declines to put a confidence interval on five samples at all. What the
table actually shows is the first row of the arithmetic above: at 25 ms per job against a 1 s
interval, only about one attempt in forty lives long enough to renew even once, so there is almost no
extra write to measure.

This is the honest shape of the cost, and it is why the A/B is published alongside the arithmetic
rather than instead of it. **A workload where the heartbeat is expensive is a workload where it is
also load-bearing** — jobs that run for minutes are exactly the ones that were being reclaimed and
re-executed before it existed. The `UPDATE`s buy a correctness guarantee that was not previously
available at any price.

```bash
go test -tags=loadtest -run='^$' -bench=BenchmarkDrainWithHeartbeat -benchtime=1x -count=5 ./loadtest/
```

<br/>

## Replaying a failed cohort: re-convergence, and the one-more-try defect it replaces

"Replay the failures" is the common operator action after an incident, and the runtime used to support
it badly. A plain retry cleared a job's finalization and returned it to `available` but left `attempt`
and `max_attempts` untouched, so a job discarded at `attempt == max_attempts` was re-claimed exactly
once, failed on that single attempt, and was discarded again. An operator replaying 30,000 jobs watched
30,000 immediate re-failures with no signal why. The fix restores the retry budget as headroom
(`max_attempts = attempt + budget`) rather than rewinding the counter, so a replayed job gets a real
second life and its `job_runs` audit sequence stays continuous.

This run measures the whole path at scale. It seeds 35,000 children under one parent, fails 85.7% of
them transiently, drains to a mix of discarded and succeeded, replays the discarded cohort under its
parent with a restored budget of three spread across a ten-minute window, and awaits a second drain.

| Measurement | Value |
|---|---|
| Children seeded (one parent) | 35,000 |
| First drain — discarded / succeeded | 30,031 / 4,969 |
| Replayed (`ReplayByParent`, budget 3) | 30,031 |
| Succeeded children left untouched | 4,969 (before == after) |
| Re-converged discarded after replay | 30,031 |
| Jobs run past their restored budget | 0 |
| Concurrent executions | 0 |
| Retry transitions across both drains | 120,124 |
| Claim p50 / finalize p50 | 1.21 ms / 0.96 ms |

Every child re-converges to a terminal state. The 4,969 that succeeded on the first drain are never
re-run — the replay targets discarded jobs only — and the 30,031 that failed are replayed, run their
three restored attempts, and re-discard. `max_runs_over_budget` is zero: no job runs more times than
its restored budget allows, which is the invariant the restored budget exists to hold.

The stagger is why the run's wall clock is ten minutes and its drain rate reads 106 jobs/s: the replayed
cohort's `scheduled_at` is spread across the window *by construction*, so that rate is a governor
setting, not a ceiling — stagger shapes arrival, it does not cap the claim rate. The `retried` count is
the fix in one number: 120,124 retry transitions, four per failed child (attempts 1→2 and 2→3 on the
first drain, then 4→5 and 5→6 on the replay). A one-more-try retry could never have produced them — it
would have granted a single attempt and discarded.

`docs/benchmarks/replay-30k.json` is the committed artifact.

<br/>

## Admission control: the pre-claim gate vs. claim-then-snooze

Two runs drain the same 2,000 jobs against the same 50-per-second budget. One installs a pre-claim
`Limiter` — the runners consult it before every claim and take only what it grants. The other has no
gate and enforces the budget the only way the runtime offered before: the worker is dispatched,
discovers the downstream is at budget, and returns `Result{Snooze}`. A snooze costs no retry attempt,
but it is not free — it spent a poll, a claim, a `job_runs` stub, and a finalize to discover the job
could not run.

| | Pre-claim gate | Claim-then-snooze |
|---|---|---|
| Report | `gate-budget.json` | `gate-snooze-baseline.json` |
| Command | `-limiter token-bucket -rate 50` | `-worker-snooze 50` |
| Jobs drained | 2,000 | 2,000 |
| Claims issued | **1,978** | 213,274 |
| **`job_runs` rows written** | **2,000** | **442,495** |
| `job_runs` per drained job | **1.0** | **221** |

The headline is the `run_rows` field: the gate wrote one audit row per drained job, and the baseline
wrote **221**. Under sustained backpressure the queue's dominant work becomes jobs claiming and
re-scheduling themselves, and `job_runs` grows at the *snooze* rate — 213,274 claims and 442,495
finalizes to drain two thousand jobs — rather than the completion rate. The gate never claims work it
cannot run, so that growth drops to zero: the audit table tracks jobs done.

Both runs retire the same 2,000 jobs, so the gate is not throttling throughput below the budget — it is
removing the wasted claims that reach the budget the expensive way. Its `drain_throughput_per_sec`
reads ~51, tracking the 50/s budget rather than the poll rate; the baseline's reads in the thousands
because a snooze counts as a decided attempt, which is the poll rate the gate exists to stop chasing.

<br/>

## Running the harness yourself

The harness lives behind its own build tag and is excluded from the default test matrix, so it never
joins an ordinary `go test ./...`:

```bash
# Unit tests for the harness itself; everything database-bound skips without a DSN.
go test -tags=loadtest ./loadtest/...

# The Go benchmark entry points, for benchstat comparison across a change.
go test -tags=loadtest -run='^$' -bench='BenchmarkClaim100k|BenchmarkEnqueue100k' -benchtime=1x ./loadtest/
```
