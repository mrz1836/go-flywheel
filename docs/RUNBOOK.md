# Operational runbook

How to run the runtime in production and what to do when a signal moves. Every symptom below names the
metric or column that shows it and the specific lever that answers it. For the exactly-once guarantees
these procedures rest on, see [`CONTRACT.md`](CONTRACT.md); for sizing the knobs, [`TUNING.md`](TUNING.md).

<br>

## Reading the metrics

Wire `observers.NewMetrics(rec)` into the runner (and pass the same observer to the scheduler, so sweep
timing lands in one recorder), then serve `observers.MetricsHandler(rec, sample)`. The scrape exposes:

| Series | Type | The question it answers |
|---|---|---|
| `flywheel_claim_duration_seconds` | histogram | How long does a claim round trip take? (contention) |
| `flywheel_finalize_duration_seconds` | histogram | How long does persisting an outcome take? (database) |
| `flywheel_job_duration_seconds` | histogram | How long does a worker body run? (downstream) |
| `flywheel_sweep_duration_seconds` | histogram | How long does one lease-reclaim pass take? |
| `flywheel_queue_ready` | gauge | How many jobs are claimable right now? |
| `flywheel_queue_inflight` | gauge | How many jobs are running? |
| `flywheel_queue_oldest_ready_seconds` | gauge | How long has the oldest ready job waited? (lag) |
| `flywheel_queue_jobs{state}` | gauge | How many jobs sit in each state? |
| `flywheel_jobs_claimed_total` | counter | Total jobs claimed. |
| `flywheel_jobs_finished_total{outcome}` | counter | State-advancing finalizations by outcome. |
| `flywheel_jobs_retried_total` | counter | Scheduled retries. |
| `flywheel_jobs_superseded_total` | counter | Attempts executed and discarded — the double-execution signal. |
| `flywheel_metrics_dropped_series_total` | counter | Series dropped to the cardinality ceiling. |

The gauges are sampled fresh on every scrape from `SampleQueueHealth`; the histograms and counters
accumulate over the process lifetime. Percentiles come from the `_bucket` series via
`histogram_quantile`, so their accuracy is bounded by the recorder's buckets (`DefaultLatencyBuckets`).

**Without a scrape.** A deployment that ships logs rather than scraping can still watch the lag. With
`SchedulerConfig.HealthSampleInterval` set, every `jobs: queue health` line carries
`oldest_ready_seconds`, the same value as `flywheel_queue_oldest_ready_seconds`, as a number (zero when
nothing is ready). Point a log-metric filter at that field and you have the lag signal the symptoms
below read, ready to alarm on.

<br>

## Symptom: the queue is deep

Two different failures both show as a backlog, and they call for opposite responses. The metrics tell
them apart.

**Rising `flywheel_queue_oldest_ready_seconds` (lag) with claim latency flat → under-capacity.** Work is
arriving faster than the runners drain it. The claim is fast; there simply are not enough workers. The
lever is capacity:

- Raise `Concurrency` (the worker-pool size) on the existing runners, up to what the connection pool
  supports — a runner needs roughly one connection per concurrent worker plus its claim.
- Add runners, or add processes. A runner per queue is the deployment the claim index is built for; see
  the multi-queue note below.

**Rising claim p99 (`flywheel_claim_duration_seconds`) with lag flat → contention.** The runners are
keeping up, but each claim is getting slower — executors are contending for the same rows, or the
database is loaded. Adding workers here makes it *worse*. The lever is to reduce contention:

- Split one runner over many queues into one runner per queue. A claim that names more than one queue
  cannot use an ordered index scan and falls back to a sequential scan and a sort; a single-queue claim
  is an index probe.
- Check `flywheel_sweep_duration_seconds` and any other database load in the same window — a rising
  claim p99 that tracks a rising sweep p99 is the database, not the queue.

**A queue that is deep but `flywheel_queue_ready` is low, with `flywheel_queue_jobs{state="scheduled"}`
high → the work is not due yet.** Scheduled-ahead jobs are not lag. Nothing is wrong; the jobs become
claimable at their `scheduled_at`.

<br>

## Symptom: leases keep expiring

A lease bounds a claim: if the worker holding it dies, the lease expires and the stuck-lease sweep
returns the job to the pool. This is the runtime's only recovery path for work lost to a crashed
process, and it is working as designed when it runs occasionally. It is a problem when it runs
*constantly*.

**How the sweep works.** The scheduler reclaims jobs in state `running` whose `leased_until` is in the
past, in bounded batches, on its sweep cadence (30 seconds by default). Each reclaimed job's stale run
stub — the `job_runs` row committed before the worker body ran — is marked with outcome `crashed`, so a
crash leaves an audit record rather than a hole.

**`crashed` in `job_runs.outcome`** means the attempt's process died between claiming the job and
finalizing it: the run stub was written, the worker never reported an outcome, and the sweep found the
lease expired. Find jobs whose lease keeps expiring:

```sql
-- jobs reclaimed more than once: their attempts keep dying mid-flight
SELECT job_id, count(*) AS crashes
FROM job_runs
WHERE outcome = 'crashed'
GROUP BY job_id
HAVING count(*) > 1
ORDER BY crashes DESC;
```

**Why a heartbeating worker should never appear here.** While a worker runs, the runtime renews its
lease on a heartbeat, so `leased_until` stays ahead of the sweep's cutoff for as long as the worker is
alive — even for a job that legitimately runs for many minutes. A live, heartbeating worker is therefore
never reclaimed. If a job *is* being reclaimed while its worker is still running, the cause is one of:

- **The lease is too short for the work.** The worker's runtime exceeds the lease and the heartbeat
  interval is not holding it. Lengthen `LeaseDuration` to comfortably exceed the p99 worker duration, or
  confirm the heartbeat is enabled. See the lease/heartbeat sizing section of [`TUNING.md`](TUNING.md).
- **The heartbeat is failing.** The renewal is a database write; if the database is unavailable the
  renewal fails silently and the lease lapses.

A nonzero `flywheel_jobs_superseded_total` is the same failure seen from the other side: a reclaimed job
is re-dispatched, both attempts run, and the original attempt's finalize is fenced out (its outcome
discarded) when it finally returns. Every increment is work that ran twice. Alert on it.

<br>

## Bounded retention

Terminal jobs (and their `job_runs`) accumulate forever unless retention is enabled. Without it the
tables grow without bound.

**Enabling it.** Set `SchedulerConfig.RetentionMaxAge` to the age past which a *finalized* job may be
deleted. Zero (the default) disables retention entirely — no surprise deletes for a consumer that never
asked for them. The prune runs on `RetentionInterval` (one hour by default) in bounded batches.

**Choosing the window.** Retention deletes jobs finalized *longer ago than* the window — not jobs
created that long ago. The window is a function of how long you need the audit trail (`job_runs`) and
the terminal job rows for debugging, replay, or compliance, not of throughput. A 30-day window keeps a
month of history; a 2-minute window (used in load tests) keeps almost nothing.

**Enabling it safely on an old database.** The *first* prune against a long-lived database that has
never had retention deletes its entire backlog of aged terminal jobs. The batching bounds each
transaction, so it will not take a table-wide lock or exhaust the bind-parameter ceiling — but it can
run for a long time and generate significant WAL clearing a large backlog. Enable it during a low-load
window the first time, and consider a tighter `RetentionBatchSize` and a `RetentionMaxBatches` ceiling
to bound the work each pass does. A pass that ends on its `RetentionMaxBatches` ceiling every tick is
logged, so "retention can never keep up with its window" is visible rather than silent.

**The host-FK caveat.** The runtime declares no foreign keys of its own, so retention deleting a
terminal job and its `job_runs` is safe. But if *your* schema declares a foreign key onto `job_runs.id`
(the natural key for correlating a side effect to the attempt that produced it — see
[`COOKBOOK.md`](COOKBOOK.md)), retention deleting that `job_runs` row will violate or cascade through
your constraint. Either exclude those rows from retention by keeping the window longer than your
correlation records live, or make your FK `ON DELETE SET NULL`. On SQLite specifically, a host FK is
enforced only when `foreign_keys` is on — which the pragma check warns about (see below).

<br>

## The scheduler is a singleton

The sweep, the retention prune, and the periodic tick must run on **exactly one process**. The scheduler
is a singleton by deployment, not by election — the runtime holds no lock to enforce it. Run one, and
only one.

**What running two looks like.** It is not catastrophic, but it is wasteful and, for maintenance,
doubled:

- **Periodic ticks collapse harmlessly.** Two schedulers enqueuing the same due periodic compute the
  same bucketed unique key for the same time bucket, so the second insert collides on the unique index
  and is a successful no-op. You get the right number of periodic jobs regardless of how many schedulers
  tick — this is the one part that is safe to duplicate.
- **Sweeps and retention double their load.** Two schedulers each run the full sweep and the full
  retention prune on their own cadence, so the database does twice the maintenance work for no benefit.
  Under load that doubled scan and delete traffic is exactly the contention you do not want.
- **The stats rollup stays correct, and doubles its work.** Every pass replaces whole hours inside one
  transaction, serialized per hour by an advisory lock on PostgreSQL, so two schedulers rolling the same
  hour leave exactly the rows one would.
- **Declared periodics are re-created once.** Two schedulers re-creating the same missing declared
  definition write one row — the insert does nothing when the slug exists — and only the one whose insert
  landed logs it. Two schedulers applying the same declarations at start end where one would.

**Deployment patterns for a single scheduler.**

- **One process runs the scheduler; the rest are runners only.** Give one deployment a
  `SchedulerConfig` and leave it off the others. This is the clearest pattern for a horizontally-scaled
  fleet.
- **A leader-elected singleton.** If your platform offers leader election (a Kubernetes `Lease`, a
  singleton `Deployment`/`StatefulSet` with one replica), run the scheduler only on the leader.
- **A single all-in-one process.** For a small deployment, one process running both runners and the
  scheduler is correct and simplest.

<br>

## Declared periodics come back

A scheduler with `SchedulerConfig.Periodics` set — `flywheel serve` declares its `schedules:` list this
way — re-creates a declared definition whose row goes missing, within `ReconcileInterval`
(`runtime.schedule_reconcile` in the CLI; one minute by default), and says so at warn:

```
jobs: declared periodic was missing and has been re-created slug=nightly-report kind=send_report next_run_at=…
```

That line means something removed a schedule out from under a running node: a restore from an older
snapshot, a database rebuilt under it, or a `DeletePeriodic` (`flywheel schedule rm`) of a slug that is
still declared. The re-created definition fires next at its first fire time after the re-creation; the
runs it missed while gone are not backfilled.

**Retiring a declared schedule.** Order matters:

1. Remove it from the declarations — `Periodics`, or the `schedules:` list — and deploy that to every
   node that runs a scheduler. During a rolling deploy, a node still running the old declarations
   re-creates the row.
2. Then delete the row with `DeletePeriodic` or `flywheel schedule rm`. `flywheel serve` also disables a
   schedule its file no longer names on its next start, which keeps the row and its history.

`flywheel schedule rm` warns when the slug is still declared in the config it loaded.

**Pausing a declared schedule.** `SetPeriodicActive(false)` (`flywheel schedule disable`) holds while the
scheduler runs — the reconcile only inserts, and never re-activates — but the next start re-applies the
declaration, `Active` included. To keep a schedule off across restarts, declare it with `Active: false`,
or remove it from the CLI's file.

**`flywheel doctor`** compares the file with the database and reports, without failing the check:

```
  schedules:    4 declared (1 missing, 1 inactive, 1 differs)
    - nightly              exec  0 2 * * *
    - ping                 http  every 1m0s  [not in database: serve creates it]
    - sync                 shell every 24h0m0s  [inactive: serve re-activates it on start]
    - deps                 mage  every 24h0m0s  [differs (schedule): serve updates it on start]
  undeclared:   1 active (legacy): serve disables it on start
```

**Log lines.**

| Line | Level | Meaning |
|---|---|---|
| `jobs: declared periodic was missing and has been re-created` | warn | a declared definition's row was gone and this scheduler re-created it — fields `slug`, `kind`, `next_run_at` |
| `jobs: declared periodic apply failed` | error | `Run` could not apply some declarations at start (`pending` says how many); they are retried every `retry_in` until they land — the reconcile cadence, or a minute with the pass off. Meanwhile the reconcile keeps every other declared definition present |
| `jobs: declared periodics applied` | info | the retry landed every declaration |
| `jobs: periodic reconcile failed` | error | a reconcile pass hit a database error; the next pass retries |

**A host-owned schema without `idx_job_periodics_slug`.** The re-creation inserts once because the slug
is unique. Without the index, two schedulers re-creating the same slug at the same moment can both
insert; their fires still collapse on the bucketed unique key, but the extra row stays. `InspectIndexes`
reports the missing index.

<br>

## Stats and the upgrade that brings them

The analytics surface — the `job_runs` columns, `job_run_finishes`, `job_stats_hourly`, `jobs_finished` —
is documented for consumers in [`INTEGRATING.md`](INTEGRATING.md). Operating it comes down to a handful of
signals.

**Deploying a release that changes the schema.** Migrate first, then deploy: `flywheel migrate
--concurrently --lock-timeout 5s` (library-owned), or your migration tool plus `InstallIndexesWithOptions`
with `Concurrently` (host-owned) — the runbook is
[Upgrading an existing database](INTEGRATING.md#upgrading-an-existing-database). A binary deployed before
the migration does not run half-working: it stops at startup with

```
jobs: runner schema check: flywheel: database schema is older than this binary: missing job_runs.kind, …;
migrate before deploying this binary: …
```

and claims nothing. Migrate and restart it. A binary of the previous release is fine against the migrated
schema, so the rollback is a redeploy.

**`flywheel doctor` after an upgrade** reports what the migration added, index parity, and the rollup's
lag:

```
  schema:       upgraded (added job_runs.kind, …, job_stats_progress (table))
  indexes:      in sync
  stats:        rolled up through 2026-10-03T14:00:00Z (lag 1h2m)
```

A lag of about an hour is normal — an hour closes, then waits out the grace. A lag growing past a few
hours means the rollup is not running (no Scheduler with `StatsRollupInterval`, or `runtime.stats_rollup`
negative) or is catching up.

**Log lines.**

| Line | Level | Meaning |
|---|---|---|
| `jobs: stats rollup catching up` | info | a pass stopped on `StatsMaxHoursPerPass` with closed hours left — normal on a first pass over history; a long `Stats` window answers `ErrStatsNotRolledUp` until it finishes |
| `jobs: stats rollup failed` | error | the pass hit a database error; the next tick retries it |
| `jobs: stats anomaly` | warn | a kind's duration regressed or its discards spiked, at its onset (`StatsAnomalyLog`) — fields `kind`, `signal`, `metric`, `recent`, `baseline`, `ratio` |
| `jobs: run is far slower than its kind's baseline` | warn | a running attempt passed `max(3 × p99, p99 + 1m)` for its kind, once per attempt; the heartbeat's `slow_running` counts them |

**`ErrStatsNotRolledUp`** from `Stats`/`StatsSeries` means the window would aggregate more than
`MaxRawSpan` (7 days) of raw runs: the rollup is off, or still catching up. Turn it on, wait, or
`flywheel stats rebuild --from … --to …` to catch up at once — a rebuild of only the recent hours also
rolls the ones between the rollup's progress and them, so it never leaves a gap. When the message names
`StatsRetention` instead, the window reaches back past the oldest hour the rollups keep: narrow it, or keep
rollups longer.

**Stats look short after an import or a skewed clock.** A run that finished in an hour after the rollup
closed it — a `SeedRun` import of history, or a node whose clock ran more than `StatsRollupGrace` behind —
is in the raw data but not that hour's rollup. `flywheel stats rebuild` over the affected range recounts
it; it refuses to shrink an hour retention has pruned (`--force` overrides). Runs an older release
finalized during a rolling deploy need nothing: the backfill that logs them re-rolls their hours.

**`flywheel prune` deleted fewer jobs than expected.** With the stats rollup on in its config it holds,
like the daemon's retention, for runs the rollup has not counted yet, and says so (`held for the stats
rollup`). Let the rollup catch up, or pass `--ignore-stats-rollup` if those hours' history does not
matter.

<br>

## Embedder SQLite checklist

SQLite is a supported dialect, but the connection must be opened correctly or the serialized claim can
deadlock under load. The library verifies the pragmas at construction (`NewSQLiteDriver` logs a failure;
`NewSQLiteDriverWithOptions` returns `ErrSQLitePragma`), but it cannot set them for you — they are DSN
parameters on the connection you open.

**The exact DSN.** Open the connection with every one of these:

```
file:/path/to/app.db?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)&_pragma=synchronous(NORMAL)&_pragma=foreign_keys(1)&_txlock=immediate
```

| Pragma | Why it is required |
|---|---|
| `journal_mode(WAL)` | Readers do not block the claim's writer. **Required for a file database.** An in-memory database cannot use WAL and is exempt from this one requirement. |
| `busy_timeout(5000)` | Absorbs the brief claim lock instead of failing immediately with `SQLITE_BUSY`. **Required.** |
| `synchronous(NORMAL)` | A committed outcome survives a crash. `OFF` risks losing committed job state on power loss and is **rejected**; `NORMAL` or `FULL` are safe. |
| `foreign_keys(1)` | A host foreign key onto `job_runs` is unenforced without it. The runtime declares none, so this is **warned about, not required** — set it if your schema has such an FK. |
| `_txlock=immediate` | The claim's write lock is taken up front. Without it the transaction starts deferred and can hit `SQLITE_BUSY` upgrading to a write mid-claim, even against a single writer. It is a DSN parameter `PRAGMA` cannot report, so the check **warns** when the DSN lacks it rather than failing. |

**The single-writer constraint.** SQLite serializes writers and has no `SKIP LOCKED`, so the runtime's
SQLite driver is correct only at `Concurrency: 1` — `NewRunner` enforces this with
`ErrSQLiteConcurrency`. Cap the connection pool to a single writer as well
(`sqlDB.SetMaxOpenConns(1)`): a second connection racing the claim is the exact contention `_txlock` and
the busy timeout exist to survive, and one writer removes it entirely. SQLite is for embedded,
single-process deployments; use PostgreSQL when you need concurrent claiming across processes.
