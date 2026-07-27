# Benchmarks

The runtime's first measured baseline: enqueue, claim, finalize, and sweep against real PostgreSQL at
100,000 jobs, plus a full-index versus correctness-index-only comparison.

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
| **Commit** | `1ecb5e9` (baseline, index comparison) · `c7dba1b` (heartbeat cost, sigkill and slow-job chaos) · `5ba4a08` (mass-expiry chaos, re-run after the fault's retry fix) |
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
```

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

### The claim path is unchanged, because the index is never used

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
essentially all of the 38.7 ms claim latency. This is a finding, recorded here and not fixed here; it
is the single largest lever the baseline exposes.

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

## Throughput is barrier-bound, not database-bound

This is the most important thing to know before comparing any future number against these.

`Concurrency` is a single knob doing two jobs: it is the claim batch size *and* the in-batch
parallelism. A poll claims N jobs, dispatches them across N goroutines, and waits for all of them
before claiming again. The next claim cannot start until the slowest member of the current batch has
finished.

At `-workers 8` that means a runner spends one claim latency (38.7 ms) for every 8 jobs, and 4 runners
× 8 jobs / 38.7 ms ≈ 827 jobs/s — which is the measured throughput almost exactly. The drain is
paying for the claim, and the claim is paying for the sort.

Two consequences for anyone reading these numbers later:

1. **Throughput here is `runners × workers / claim latency`.** It will move when the claim plan is
   fixed, and it will move again if the batch and the parallelism are ever separated. A change to
   either invalidates a direct comparison against this table.
2. **The finalize numbers are the honest ones.** Finalize is per-job, unbatched, and its p50 of
   0.78–0.96 ms is a real per-operation cost that does not depend on the batching above it.

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
