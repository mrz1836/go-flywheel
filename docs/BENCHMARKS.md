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
```

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
default autovacuum settings do not keep pace with a 100k drain at this rate.

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

## Measurement caveats

Every one of these is recorded in the `notes` array of the reports themselves, so they travel with the
numbers rather than living only here.

- **RSS is the harness client process, not the PostgreSQL server.** On this machine it was collected
  from `getrusage(RUSAGE_SELF).ru_maxrss`; macOS has no current-RSS source in the standard library, so
  the sampled series is a monotone high-water envelope rather than a live reading. On Linux the
  harness reads `/proc/self/statm` instead.
- **`pgstattuple` was not installed**, so free-space and dead-tuple percentages are absent from these
  reports rather than reported as zero. `CREATE EXTENSION pgstattuple` collects them.
- **Lock waits read zero throughout.** `pg_locks` is sampled instantaneously and real lock waits are
  far shorter than the sampling interval, so a run of zeroes means the sampler did not catch one — not
  that there was no contention.
- **WAL is cluster-wide.** `pg_stat_wal` has no per-database breakdown, so any other activity on the
  server is included. These runs had a quiet server.
- **Superseded is a measurement, and was not always.** Before v0.7.0 the runtime computed the supersede
  signal during finalize and discarded it, so it was unobservable through both the driver and the
  observer interface, and the harness inferred it from audit-table residue. It now counts
  `OnSupersede` events. The residue query survives as an independent cross-check: the two count
  different things — the residue misses a superseded attempt whose job later succeeded on a retry —
  and a report notes a disagreement rather than asserting one.
- **The sweep numbers come from the harness's own sweeper.** The runner does not sweep; nothing in its
  dispatch loop calls `Sweep`. The harness runs a sweeper on a one-second interval, which is what a
  scheduler would do.

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

## Running the harness yourself

The harness lives behind its own build tag and is excluded from the default test matrix, so it never
joins an ordinary `go test ./...`:

```bash
# Unit tests for the harness itself; everything database-bound skips without a DSN.
go test -tags=loadtest ./loadtest/...

# The Go benchmark entry points, for benchstat comparison across a change.
go test -tags=loadtest -run='^$' -bench='BenchmarkClaim100k|BenchmarkEnqueue100k' -benchtime=1x ./loadtest/
```
