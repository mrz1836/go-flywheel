# Building dashboards and external platforms on flywheel

This guide is for a service that imports flywheel to *read* what a flywheel deployment is doing — a
dashboard, an admin console, an alerting job — from the same database the runtime writes. It covers what
the runtime records, every read API with its cost and the index behind it, the recipes a dashboard is
made of, the SQL a non-Go consumer can run directly, and the upgrade that brings an existing database to
the schema all of this needs.

If you are upgrading a database that already runs flywheel, start with
[Upgrading an existing database](#upgrading-an-existing-database): it is short, and nothing else here works
until it is done.

<br>

## Contents

- [At a glance](#at-a-glance)
- [Upgrading an existing database](#upgrading-an-existing-database)
- [Turning the stats on](#turning-the-stats-on)
- [The data contract](#the-data-contract)
- [The read APIs](#the-read-apis)
- [Polling intervals and their cost](#polling-intervals-and-their-cost)
- [Recipes](#recipes)
- [Direct SQL](#direct-sql)
- [Prometheus or database stats?](#prometheus-or-database-stats)
- [Retention and the rollup](#retention-and-the-rollup)
- [What the anomaly detector catches](#what-the-anomaly-detector-catches)
- [Measured cost](#measured-cost)

<br>

## At a glance

| Question | API | Served by | Median at 1M runs |
|---|---|---|---|
| What is running now, for how long, is it stuck? | `ListRunning` | `jobs_running_leased` + `job_runs` PK | 2.5 ms |
| How deep is each queue, how far behind? | `QueueDepths` | `jobs_ready`, `jobs_state` | 2.2 ms |
| What is in flight, per kind? | `CountActiveByKind` | `jobs_state` | 2.1 ms |
| What just finished, and how? | `ListFinished`, `RecentFailures` | `jobs_finished` | 1–2.5 ms |
| How did each kind do today? | `Stats` (24 h) | `job_stats_hourly` + the raw tail | 12 ms |
| How did each kind do this month? | `Stats` (30 d) | `job_stats_hourly` + the raw tail | 79 ms |
| How is it trending over weeks? | `StatsSeries` (30 d, hourly or daily) | `job_stats_hourly` | 20 ms |
| What is normal for kind X? | `Baselines` | `job_stats_hourly` | sub-ms, memoized |
| Is anything regressing? | `Anomalies` | `job_stats_hourly` | 21 ms |
| What were the slowest runs today? | `SlowRuns` | `job_run_finishes` + `job_runs` PK | 69 ms |
| One job's attempts | `ListRuns` | `job_runs_job_attempt` | sub-ms |

The [measured latencies](#measured-cost) are at the end. Everything above is bounded — by a window, a
`LIMIT`, or both — so no read's cost grows with the history the database holds.

<br>

## Upgrading an existing database

This release adds to the schema; it changes and removes nothing. Every addition is either a new table or a
column that is nullable or carries a default, so:

- **a database migrated for this release keeps working with the previous release's binaries**, which never
  read or write the new columns — the safe direction of a rolling deploy;
- **this release's binaries refuse to start against a database that has not been migrated.** A Runner,
  a Scheduler, and a Node each stop at startup with `ErrSchemaOutdated`, naming every missing column and
  table and the fix — rather than claiming work whose audit row they cannot write, or sweeping into a
  finish log that is not there.

**So the order is always: migrate the database, then deploy the binaries.**

### What the release adds

| Object | Kind | Purpose |
|---|---|---|
| `job_runs.kind`, `job_runs.queue` | column, `text NOT NULL DEFAULT ''` | per-kind and per-queue stats without a join |
| `job_runs.queue_wait_ms` | column, nullable | how long each attempt waited for a worker |
| `job_runs.job_state` | column, nullable | the state each attempt's finalize applied — splits a retried failure from a final one |
| `job_runs.superseded` | column, `boolean NOT NULL DEFAULT false` | a durable record that an attempt finished after its claim was gone |
| `job_run_finishes` | table | the finish log: `(finished_at, run_id)`, which makes runs searchable by finish time |
| `job_stats_hourly` | table | the hourly rollups trends and baselines read |
| `job_stats_progress` | table | one row: the hours the rollups cover, so a missing hour is known to be empty |
| `jobs_finished` | index on `jobs` | "recently finished", newest first, per terminal state |

None of it is backfilled at migration time: old rows keep the column defaults, and the read path resolves
what it needs from `jobs`. The finish log is backfilled by the runtime itself, after the deploy — see
[After the deploy](#after-the-deploy).

### Library-owned install (`Migrate`, `flywheel migrate`)

If the runtime installs its own schema — `flywheel.Migrate(db)` at startup, or `flywheel serve` — the
upgrade is the same call. Against a loaded PostgreSQL database, run it once *before* the deploy, with the
live-database options:

```bash
flywheel migrate --concurrently --lock-timeout 5s
```

```go
err := flywheel.MigrateWithOptions(db, flywheel.MigrateOpts{
	Concurrently: true,            // CREATE INDEX CONCURRENTLY: never blocks the queue's writers
	LockTimeout:  5 * time.Second, // fail fast instead of queueing every query behind a long transaction
})
```

`flywheel migrate` prints what it added (`added: job_runs.kind, …, job_stats_progress (table)`), and a
re-run prints nothing new — an up-to-date schema issues no DDL at all. Both options are no-ops on
SQLite. Each step commits on its own, so a lock timeout fails only the statement that could not get its
lock: everything before it is applied, and a re-run completes the rest. Two migrations of one schema run
one after the other.

`flywheel serve` and `flywheel doctor` migrate too, with concurrent index builds, so either can be the
first thing to touch a live database without blocking the queue's writers; both also repair an index an
interrupted `migrate --concurrently` left invalid. Running `flywheel migrate --concurrently --lock-timeout
5s` first is still the better order: it is the step you can watch and retry.

### Host-owned install (your migration tool)

If your migration tool owns the schema — Atlas over `flywheel.Models()`, or hand-written SQL — the upgrade
is a migration you add, then the same deploy step you already run.

**1. Add the migration.** With Atlas (or any loader that reads `Models()`), regenerate: the diff is
exactly the statements below. To write it by hand, use these — they are the statements GORM emits from
`Models()`, and a test runs this file's blocks against a pre-upgrade schema to prove they produce one with
no drift.

PostgreSQL — the `ADD COLUMN`s are catalog-only changes on PostgreSQL 11+ (a constant default rewrites no
rows), but each takes a brief `ACCESS EXCLUSIVE` lock on `job_runs`, so run them with a lock timeout:

<!-- upgrade-ddl:postgres -->
```sql
SET lock_timeout = '5s';
ALTER TABLE job_runs ADD COLUMN IF NOT EXISTS kind text NOT NULL DEFAULT '';
ALTER TABLE job_runs ADD COLUMN IF NOT EXISTS queue text NOT NULL DEFAULT '';
ALTER TABLE job_runs ADD COLUMN IF NOT EXISTS queue_wait_ms bigint;
ALTER TABLE job_runs ADD COLUMN IF NOT EXISTS job_state text;
ALTER TABLE job_runs ADD COLUMN IF NOT EXISTS superseded boolean NOT NULL DEFAULT false;
CREATE TABLE IF NOT EXISTS job_run_finishes (
  finished_at timestamptz NOT NULL,
  run_id      text        NOT NULL,
  PRIMARY KEY (finished_at, run_id)
);
CREATE TABLE IF NOT EXISTS job_stats_hourly (
  bucket_start_unix bigint      NOT NULL,
  kind              text        NOT NULL,
  queue             text        NOT NULL,
  attempts          bigint      NOT NULL,
  success           bigint      NOT NULL,
  error             bigint      NOT NULL,
  timeout           bigint      NOT NULL,
  snooze            bigint      NOT NULL,
  cancelled         bigint      NOT NULL,
  crashed           bigint      NOT NULL,
  superseded        bigint      NOT NULL,
  discarded         bigint      NOT NULL,
  dur_count         bigint      NOT NULL,
  dur_sum_ms        bigint      NOT NULL,
  dur_max_ms        bigint      NOT NULL,
  dur_p50_ms        bigint      NOT NULL,
  dur_p95_ms        bigint      NOT NULL,
  dur_p99_ms        bigint      NOT NULL,
  dur_hist          jsonb       NOT NULL,
  slowest_run_id    text,
  wait_count        bigint      NOT NULL,
  wait_sum_ms       bigint      NOT NULL,
  wait_max_ms       bigint      NOT NULL,
  wait_p95_ms       bigint      NOT NULL,
  wait_hist         jsonb       NOT NULL,
  cost_micros_sum   bigint      NOT NULL,
  hist_version      bigint      NOT NULL,
  rolled_at         timestamptz NOT NULL,
  PRIMARY KEY (bucket_start_unix, kind, queue)
);
CREATE TABLE IF NOT EXISTS job_stats_progress (
  id                bigint      NOT NULL,
  covered_from_unix bigint      NOT NULL,
  covered_to_unix   bigint      NOT NULL,
  retain_from_unix  bigint      NOT NULL,
  version           bigint      NOT NULL,
  updated_at        timestamptz NOT NULL,
  PRIMARY KEY (id)
);
```

If your schema declares `job_runs.id` as `uuid` rather than `text`, declare `job_run_finishes.run_id` as
`uuid` too: the stats reads join the two.

SQLite — `ADD COLUMN` has no `IF NOT EXISTS` there, so this block is for a migration tool that runs it once:

<!-- upgrade-ddl:sqlite -->
```sql
ALTER TABLE job_runs ADD COLUMN kind text NOT NULL DEFAULT '';
ALTER TABLE job_runs ADD COLUMN queue text NOT NULL DEFAULT '';
ALTER TABLE job_runs ADD COLUMN queue_wait_ms integer;
ALTER TABLE job_runs ADD COLUMN job_state text;
ALTER TABLE job_runs ADD COLUMN superseded numeric NOT NULL DEFAULT false;
CREATE TABLE IF NOT EXISTS job_run_finishes (
  finished_at datetime NOT NULL,
  run_id      text     NOT NULL,
  PRIMARY KEY (finished_at, run_id)
);
CREATE TABLE IF NOT EXISTS job_stats_hourly (
  bucket_start_unix integer  NOT NULL,
  kind              text     NOT NULL,
  queue             text     NOT NULL,
  attempts          integer  NOT NULL,
  success           integer  NOT NULL,
  error             integer  NOT NULL,
  timeout           integer  NOT NULL,
  snooze            integer  NOT NULL,
  cancelled         integer  NOT NULL,
  crashed           integer  NOT NULL,
  superseded        integer  NOT NULL,
  discarded         integer  NOT NULL,
  dur_count         integer  NOT NULL,
  dur_sum_ms        integer  NOT NULL,
  dur_max_ms        integer  NOT NULL,
  dur_p50_ms        integer  NOT NULL,
  dur_p95_ms        integer  NOT NULL,
  dur_p99_ms        integer  NOT NULL,
  dur_hist          JSON     NOT NULL,
  slowest_run_id    text,
  wait_count        integer  NOT NULL,
  wait_sum_ms       integer  NOT NULL,
  wait_max_ms       integer  NOT NULL,
  wait_p95_ms       integer  NOT NULL,
  wait_hist         JSON     NOT NULL,
  cost_micros_sum   integer  NOT NULL,
  hist_version      integer  NOT NULL,
  rolled_at         datetime NOT NULL,
  PRIMARY KEY (bucket_start_unix, kind, queue)
);
CREATE TABLE IF NOT EXISTS job_stats_progress (
  id                integer  NOT NULL,
  covered_from_unix integer  NOT NULL,
  covered_to_unix   integer  NOT NULL,
  retain_from_unix  integer  NOT NULL,
  version           integer  NOT NULL,
  updated_at        datetime NOT NULL,
  PRIMARY KEY (id)
);
```

**2. Install the index at deploy, the way you already install the others.** The one new index,
`jobs_finished`, is on `jobs` — the hottest table — so on a live PostgreSQL database build it concurrently:

```go
err := flywheel.InstallIndexesWithOptions(ctx, db, flywheel.IndexOpts{
	Concurrently: true,            // no lock on jobs' writers while it builds
	LockTimeout:  5 * time.Second, // bounds the plain (non-concurrent) path if you leave Concurrently off
})
```

A concurrent build waits for the transactions already open when it starts — it blocks no one, but it
cannot finish before a long-running transaction does. If a concurrent build is interrupted it leaves an
*invalid* index behind; `InspectIndexes` reports it (`IndexDrift.Invalid`), and re-running
`InstallIndexesWithOptions` with `Concurrently` replaces it. `InstallStorageParameters` has nothing new to
apply this release.

**3. Keep the parity check in CI.** Both reports are empty for a schema that matches this release:

```go
schemaGaps, _ := flywheel.InspectSchema(ctx, db)  // missing tables and columns
indexDrift, _ := flywheel.InspectIndexes(ctx, db) // missing, drifted, or invalid indexes
```

### After the deploy

The deploy itself needs nothing more. What remains, the runtime does on its own once a Scheduler with
`StatsRollupInterval` set (or `flywheel serve`) is running:

1. **Its first rollup pass backfills the finish log** for every run the previous release finalized
   (`BackfillRunFinishes`) — one pass over `job_runs` in primary-key batches, writing an entry only for the
   pre-upgrade rows. On SQLite it first re-stamps those rows' timestamps in UTC
   (`NormalizeRunTimestamps`), because the previous release wrote them in the clock's local zone. Every
   later backfill starts with one statement that asks whether any run still lacks its entry, and stops
   there when none does — so once the upgrade is done, a restart does not walk `job_runs` again.
2. **The rollup then works through history** a bounded number of hours per pass (`StatsMaxHoursPerPass`,
   24 by default), from the oldest finished run — but no further back than `StatsRetention` (400 days).
   While it catches up it logs `jobs: stats rollup catching up`, and a long `Stats` window answers
   `ErrStatsNotRolledUp`. To catch up at once instead, run `flywheel stats rebuild --from <oldest> --to
   <now>` (it backfills, then rebuilds), or `BackfillRunFinishes` followed by `RebuildStats`. A month at
   a million runs rolls up from scratch in under ten seconds. A rebuild of only the recent hours is safe
   too: the rollups never cover a range with a gap in it, so it rolls the hours between the rollup's
   progress and the ones you asked for as well (only those with runs cost a recompute).
3. **Old rows are read through `jobs`.** A run the previous release wrote has `kind = ''`; the stats
   resolve its kind, queue, and — for a failed attempt — whether it discarded its job, through the job row.
   Nothing rewrites history.

### Rolling deploys

During a rolling deploy both releases run against the migrated database. The previous release's runners
write runs without the new columns and without finish-log entries; this release's Scheduler backfills
those on its first pass and once more ten minutes later, which covers a deploy that overlaps for minutes.
A backfilled run whose hour the rollup already closed is counted by re-rolling that hour. If your deploy
overlaps for longer, run `flywheel stats rebuild` over the deploy window afterwards — a rebuild backfills
before it recomputes.

Right after the migration, a previous-release process on PostgreSQL may fail one statement on `job_runs`
with `cached plan must not change result type` (SQLSTATE `0A000`): its driver cached a `SELECT *` plan
before the columns were added. The driver drops the plan and the retry succeeds; the error is a one-off
per connection, not a sign the migration went wrong.

### Verifying

```bash
flywheel doctor    # schema: up to date · indexes: in sync · stats: rolled up through … (lag …)
flywheel stats     # per-kind stats for the last 24 hours, and how they were served
```

### Rolling back

Deploy the previous release's binaries: they run unchanged against the migrated schema. Nothing needs to
be dropped. If you want the schema back as it was, drop `job_stats_progress`, `job_stats_hourly`,
`job_run_finishes`, the `jobs_finished` index, and the five `job_runs` columns — but only after no binary
of this release is running, since it refuses to start without them.

<br>

## Turning the stats on

The live reads — `ListRunning`, `QueueDepths`, `ListFinished`, `SlowRuns` — need nothing turned on. The
history reads need the hourly rollup, which a Scheduler runs when `StatsRollupInterval` is set:

```go
node, err := flywheel.NewNode(flywheel.NodeConfig{
	Runners: runners,
	Scheduler: &flywheel.SchedulerConfig{
		DB: db, Client: client, Driver: driver,
		StatsRollupInterval:  time.Minute, // off when zero, like HealthSampleInterval
		StatsAnomalyLog:      true,        // warn once when a kind regresses or starts failing
		HealthSampleInterval: time.Minute, // the heartbeat gains slow_running
	},
})
```

| Field | Default | What it does |
|---|---|---|
| `StatsRollupInterval` | 0 (off) | cadence of the rollup; a pass with nothing to do is two indexed reads |
| `StatsRollupGrace` | 5m | how long past an hour's end before it is closed — absorbs clock skew between nodes |
| `StatsMaxHoursPerPass` | 24 | bounds one pass's work during a catch-up |
| `StatsRetention` | 400 days | how long rollup rows are kept |
| `StatsAnomalyLog` | false | log each anomaly's onset once, at warn |

`flywheel serve` turns the rollup on at one minute (`runtime.stats_rollup`; a negative value disables it)
and the anomaly log with it. Run the rollup on **one Scheduler per database** — a duplicate is harmless
(every pass replaces whole hours, idempotently, serialized per hour) but wasted. A host with its own
maintenance loop can call `Scheduler.RollupStats(ctx)` instead of the activity.

<br>

## The data contract

### Stability tiers

1. **The Go API is stable.** Every function, type, and field this guide names is covered by the module's
   compatibility promise.
2. **The columns documented below are stable for direct SQL.** Their names, types, and meaning will not
   change without a major version.
3. **Everything else is internal**: other columns, index names and shapes, the histogram encoding beyond
   what `HistogramBounds` documents, and `job_stats_progress` — read the hours the rollups cover through
   `StatsCoverage` (or `RebuildResult`) instead.

### Tables and columns

**`jobs`** — one row per job. Stable for SQL: `id`, `kind`, `queue`, `state`, `attempt`, `max_attempts`,
`priority`, `executor_class`, `parent_job_id`, `created_at` (enqueue time), `scheduled_at` (claimable at;
moves on retry and snooze), `finalized_at` (set once, on reaching a terminal state), `updated_at` (moves on
every transition *and every lease renewal* — a liveness signal, not a start time), `deleted_at`.

**`job_runs`** — one row per attempt: inserted with `outcome = 'started'` when the attempt begins, updated
once when it ends (by its finalize, or by the lease sweep if its process died).

| Column | Meaning |
|---|---|
| `id`, `job_id`, `attempt` | the attempt; `(job_id, attempt)` is unique |
| `kind`, `queue` | the job's, recorded on the attempt; `''` on a row an older release wrote |
| `outcome` | `started`, `success`, `error`, `timeout`, `snooze`, `cancelled`, `crashed` |
| `executor_class`, `executor_id` | which pool and which process ran it |
| `started_at`, `finished_at` | in UTC; `finished_at` is NULL while running and written once |
| `duration_ms` | start to finalize; NULL while running and for `crashed` |
| `queue_wait_ms` | claimable to started; NULL on older rows |
| `job_state` | the state the finalize applied (`succeeded`, `retryable`, `discarded`, `scheduled`, `cancelled`, or `available` for a crash the lease sweep reclaimed); NULL while running, when superseded, and on a row an older release or `SeedRun` wrote |
| `superseded` | true when the attempt finished after its claim was gone — its outcome was recorded but never applied, so the work may have run twice. An attempt the lease sweep crashed that then finished late keeps `job_state = 'available'` and its sweep-time `finished_at` beside the late outcome |
| `error_class`, `error_message` | for a failed attempt |
| `cost_micros`, `enqueued_children`, `output` | what the worker reported |

**`job_run_finishes`** — the finish log: one `(finished_at, run_id)` per finished run, written in the same
transaction that sets `job_runs.finished_at`. It exists because `job_runs.finished_at` is deliberately not
indexed (an index there would cost the finalize its HOT update; see [Measured cost](#measured-cost)).
Join it to `job_runs` on `run_id = id` to find runs by finish time.

**`job_stats_hourly`** — the hourly rollups, keyed by `bucket_start_unix` (epoch seconds of the UTC hour),
`kind`, and `queue`. Every rolled hour holds three classes of row, each a complete sum of the same runs:

| Rows | `kind` | `queue` | Holds |
|---|---|---|---|
| group | the kind | the queue | one (kind, queue) |
| per-kind total | the kind | `''` | that kind across every queue |
| hour total | `''` | `''` | every kind and queue |

**Read exactly one class**, or you count every run two or three times. A per-kind chart reads
`queue = ''`; an overview reads `kind = '' AND queue = ''`; only a per-queue view needs the groups
(`kind <> '' AND queue <> ''`). An hour with no runs has no rows: within the hours the rollups cover, a
missing hour means nothing ran (which hours those are, `Stats` reports in its `Coverage`). Runs whose kind
or queue cannot be resolved — an old row whose job is gone — group under `(unknown)`.

| Column | Meaning |
|---|---|
| `attempts` | finished, non-superseded attempts |
| `success`, `error`, `timeout`, `snooze`, `cancelled`, `crashed` | attempts by outcome (superseded excluded) |
| `superseded` | attempts that finished after losing their claim — to a cancel; an attempt the lease sweep reclaimed counts as `crashed`, even if it finished late |
| `discarded` | attempts that discarded their job — job-level failures |
| `dur_count`, `dur_sum_ms`, `dur_max_ms` | exact, over **successful** attempts only |
| `dur_p50_ms`, `dur_p95_ms`, `dur_p99_ms` | estimated within a few percent, never above the max |
| `wait_count`, `wait_sum_ms`, `wait_max_ms`, `wait_p95_ms` | queue wait, over non-superseded attempts |
| `cost_micros_sum` | summed `Result.CostMicros` |
| `slowest_run_id` | the slowest successful attempt of the hour |
| `dur_hist`, `wait_hist`, `hist_version` | sparse `[[bucket, count], …]` histograms; `HistogramBounds(hist_version)` gives the bounds |

Derived numbers: jobs **succeeded** = `success`; jobs **failed** = `discarded`; **retries** =
`error + timeout − discarded + crashed`; **success rate** = `success / (success + discarded)`. A failed
attempt that does not record the state it applied — one an older release wrote, or one `SeedRun`
imported — counts as the discard when its job is discarded at that attempt, and as a retry otherwise.

### Time and zones

`job_runs` and `job_run_finishes` timestamps are stored in UTC. On PostgreSQL every timestamp column is
`timestamptz`, so this is invisible. On SQLite timestamps are text and compared as text, which is only a
time comparison within one zone: pass `job_runs` bounds in UTC (the Go API does it for you). `jobs`
timestamps keep the zone of the clock that wrote them, as they always have; compare them in that zone.

Rollup hours are UTC hours. `StatsSeries` sums them into calendar days in any zone whose offset is a whole
number of hours (a day across a DST change is 23 or 25 hours); a half-hour zone such as `Asia/Kolkata` is
refused for day buckets.

<br>

## The read APIs

Every read takes a `context.Context` and the `*gorm.DB` to read through — a replica is fine — and returns
JSON-tagged values. None writes.

### Live: what is happening now

**`ListRunning(ctx, db, ListRunningParams{Kind, Queue, Limit, WithBaseline})`** — the jobs running now,
longest-running first, each with its attempt, executor, `Elapsed`, and whether its lease has expired (the
process stopped renewing — usually because it died; the next lease sweep will reclaim it). A job claimed a
moment ago whose attempt row has not landed yet is reported as `Starting` rather than dropped. With
`WithBaseline`, each run is compared to its kind's baseline p99 and flagged `Slow` past
`max(3 × p99, p99 + 1m)` — only for a kind with at least 200 baseline samples. Served by
`jobs_running_leased`, joined to each attempt by the `job_runs_job_attempt` unique index.

**`QueueDepths(ctx, db)`** — per queue: ready now, scheduled ahead, running, paused, and the oldest ready
job's age (the lag). The same access paths as `SampleQueueHealth`.

**`CountActiveByKind(ctx, db)`** — in-flight jobs per (kind, queue, state), non-terminal states only, on
`jobs_state`. This, not a `GROUP BY kind` over all of `jobs`, is the per-kind backlog read.

### Recent: what just happened

**`ListFinished(ctx, db, ListFinishedParams{States, Kind, Queue, Since, Before, Limit})`** — the most
recently finished jobs, newest first by `(finalized_at, id)`. One `LIMIT n` range scan per requested
terminal state on `jobs_finished`, merged in Go. Page with `Before: &FinishedCursor{FinalizedAt, ID}` from
the previous page's last job. A state listed twice is read once.

**`RecentFailures(ctx, db, RecentFailuresParams{Since, Limit})`** — discarded jobs with the error of the
attempt that discarded them; also on `jobs_finished`.

**`ListJobs(ctx, db, ListJobsParams{State, Kind, Queue, BeforeID, Limit})`** — jobs newest first *by id*.
Ids are UUIDv7, so id order is insertion order and the primary key serves the page; `BeforeID` pages it.

**`ListRuns(ctx, db, jobID, ListRunsParams{BeforeAttempt, Before, Limit})`** — one job's attempts with
every column above, newest attempt first. Page with `BeforeAttempt` set to the previous page's last
`Attempt`; `Before` (a `created_at` cursor) also works, and is zone-proof on SQLite.

**`SlowRuns(ctx, db, SlowRunsParams{Kind, Since, MinDuration, Limit})`** — the slowest finished runs in a
short window (24 hours by default), any outcome — a timeout is a slow run. A range of `job_run_finishes`;
for slow runs further back, read the hourly `SlowestRunID` from `Stats`.

### History: how it has been going

**`Stats(ctx, db, StatsParams{From, To, Kind, Queue, MaxRawSpan})`** — per-kind `KindStats` for a window:
attempts by outcome, jobs succeeded and discarded, retries, superseded attempts, success rate,
`Duration` (successful attempts) and `QueueWait` as `{Count, Avg, P50, P95, P99, Max}`, cost, and the
slowest run — plus a `Total` and a `Coverage` saying how it was served.

It is a hybrid read. Whole hours the rollups cover — `[Coverage.RolledFrom, Coverage.RolledThrough)`, the
range `job_stats_progress` records — come from `job_stats_hourly`; the rest — the hours past the rollup's
watermark, a partial hour at either edge, and any hours older than the rollups reach (`StatsRetention`)
— is aggregated from raw runs by the same function the rollup runs. Both halves count with the same
histograms, so the answer is identical whether an hour came from a rollup or from raw runs; a randomized
oracle test holds this to equality. With the rollup off the whole window is read raw — correct, only
slower — and a window that would read more than `MaxRawSpan` (7 days by default) of raw runs fails with
`ErrStatsNotRolledUp`, naming the fix. The cap counts raw *runs*, not the clock: each raw part is measured
from the first run in it, so a "last 90 days" window on a database with two weeks of history, or one
reaching back past the rollups into hours retention has emptied, is served.

**`StatsSeries(ctx, db, SeriesParams{From, To, Kind, Queue, Interval, Location})`** — one point per hour or
per local day, empty buckets included, each a `KindStats` for the bucket. Served exactly like `Stats`.

**`Baselines(ctx, db, window)`** — each kind's p50/p95/p99 success duration, sample count, and discard
rate over the window of rolled hours ending at the watermark (7 days by default). It is memoized per
database and window until the rollup next writes — in any process — so polling it costs one primary-key
read.

**`Anomalies(ctx, db, AnomalyParams{Hour, BaselineWindow, Thresholds})`** — the kinds behaving unlike
their own baseline at an hour; see [What the anomaly detector catches](#what-the-anomaly-detector-catches).

### Maintenance

| Function | Use |
|---|---|
| `RebuildStats(ctx, db, RebuildOpts{From, To, Force})` | recompute closed hours from raw runs — after a `SeedRun` import, clock skew beyond the grace, or to catch up at once; extends the covered range without leaving a gap (it rolls the hours between too); refuses to shrink an hour whose raw runs retention already pruned unless `Force` |
| `BackfillRunFinishes(ctx, db)` | write finish-log entries for runs an older release finalized, and re-roll any covered hour they land in; the rollup runs it for you, and `flywheel stats rebuild` runs it before rebuilding |
| `NormalizeRunTimestamps(ctx, db)` | SQLite only: re-stamp pre-upgrade `job_runs` timestamps in UTC; the backfill runs it for you |
| `HistogramBounds(version)` | the bucket bounds behind a stored histogram |
| `InspectSchema`, `InspectIndexes` | the CI parity checks |

<br>

## Polling intervals and their cost

| Panel | API | Suggested interval | Database cost per poll |
|---|---|---|---|
| Running now | `ListRunning(WithBaseline)` | 5–10 s | one indexed read of the running set + one rollup read |
| Queue depth | `QueueDepths` | 10–30 s | three grouped index reads + one ordered read per queue with ready work |
| Recently finished | `ListFinished` | 10–30 s | three `LIMIT n` index range scans |
| Today's stats | `Stats` (24 h) | 30–60 s | 24 × kinds rollup rows by primary key + the open hour raw |
| Trends | `StatsSeries` (30 d) | 5–15 min | 720 rollup rows (one per hour) + the open hour raw |
| Anomalies | `Anomalies` | hourly, after the rollup | ~190 × kinds rollup rows |

The rollups only change once an hour closes, so a trend chart polled more often than every few minutes is
re-reading the same rows; cache it. For anything finer than an hour, use Prometheus.

<br>

## Recipes

### A "running now" panel

```go
running, err := flywheel.ListRunning(ctx, db, flywheel.ListRunningParams{WithBaseline: true, Limit: 100})
for _, r := range running {
	switch {
	case r.Starting:
		// claimed, attempt not started yet
	case r.LeaseExpired:
		// its process stopped renewing — likely dead; the sweep will reclaim it
	case r.Slow:
		// far past its kind's p99 (r.BaselineP99)
	}
}
```

### A kind's detail page

```go
now := time.Now()
day, _ := flywheel.Stats(ctx, db, flywheel.StatsParams{From: now.Add(-24 * time.Hour), To: now, Kind: kind})
week, _ := flywheel.StatsSeries(ctx, db, flywheel.SeriesParams{From: now.Add(-7 * 24 * time.Hour), To: now, Kind: kind})
slow, _ := flywheel.SlowRuns(ctx, db, flywheel.SlowRunsParams{Kind: kind, Limit: 10})
failed, _ := flywheel.ListFinished(ctx, db, flywheel.ListFinishedParams{
	Kind: kind, States: []flywheel.JobState{flywheel.StateDiscarded}, Limit: 20,
})
// Drill from a failure or a slow run into its attempts:
runs, _ := flywheel.ListRuns(ctx, db, failed[0].ID, flywheel.ListRunsParams{})
```

### Trend charts

```go
ny, _ := time.LoadLocation("America/New_York")
points, err := flywheel.StatsSeries(ctx, db, flywheel.SeriesParams{
	From: now.Add(-90 * 24 * time.Hour), To: now, Interval: flywheel.IntervalDay, Location: ny,
})
for _, p := range points {
	// p.Start, p.Stats.Attempts, p.Stats.SuccessRate, p.Stats.Duration.P95, p.Stats.QueueWait.P95 …
}
```

### Regression alerts

Run once an hour, a few minutes after the hour closes, and alert on onsets only — a sustained regression
is reported every hour it lasts, with `Onset` true only the first time:

```go
found, err := flywheel.Anomalies(ctx, db, flywheel.AnomalyParams{})
for _, a := range found {
	if a.Onset {
		alert(a.Kind, a.Signal, a.Metric, a.Recent, a.Baseline, a.Ratio)
	}
}
```

Or set `StatsAnomalyLog` and alert on the `jobs: stats anomaly` log line, which fires once per onset.

### An embeddable JSON API

[`examples/dashboard-api`](../examples/dashboard-api/main.go) is a complete read-only `net/http` handler
over these reads — running, queues, finished, stats, series, anomalies, and a job's runs — with the error
mapping a client needs (`ErrValidation` and `ErrStatsNotRolledUp` are the caller's to fix, so 400). Mount
`NewHandler(db)` under your own mux, behind your own authentication: error messages and job metadata are
not for the open internet.

<br>

## Direct SQL

A consumer outside Go can read the same tables. Keep to the [stable columns](#tables-and-columns), and
read history from `job_stats_hourly`.

**Daily trend for one kind, PostgreSQL** — its per-kind total rows (`queue = ''`):

```sql
SELECT date_trunc('day', to_timestamp(bucket_start_unix) AT TIME ZONE 'UTC') AS day,
       sum(attempts) AS attempts, sum(success) AS succeeded, sum(discarded) AS failed,
       sum(dur_sum_ms)::float8 / nullif(sum(dur_count), 0) AS avg_ms,
       max(dur_p95_ms) AS worst_hourly_p95_ms
FROM job_stats_hourly
WHERE kind = 'email.send' AND queue = ''
  AND bucket_start_unix >= extract(epoch FROM now() - interval '30 days')
GROUP BY 1 ORDER BY 1;
```

**Hourly throughput across everything, SQLite** — the hour totals (`kind = '' AND queue = ''`):

```sql
SELECT datetime(bucket_start_unix, 'unixepoch') AS hour,
       attempts, success, discarded,
       1.0 * dur_sum_ms / nullif(dur_count, 0) AS avg_ms, dur_p95_ms
FROM job_stats_hourly
WHERE kind = '' AND queue = '' AND bucket_start_unix >= unixepoch('now', '-7 days')
ORDER BY bucket_start_unix;
```

Percentiles do not add: a day's p95 is not the max, sum, or average of its hours' p95s. The `dur_p*_ms`
columns are right for per-hour charts; for a percentile over many hours, merge the histograms (the Go API
does) or show the worst hour.

**Running now, PostgreSQL:**

```sql
SELECT j.id, j.kind, j.queue, j.attempt, r.started_at, now() - r.started_at AS elapsed,
       j.leased_until < now() AS lease_expired
FROM jobs j LEFT JOIN job_runs r ON r.job_id = j.id AND r.attempt = j.attempt
WHERE j.state = 'running' AND j.deleted_at IS NULL
ORDER BY r.started_at NULLS LAST LIMIT 100;
```

**Runs that finished in a window:** go through the finish log —

```sql
SELECT r.* FROM job_run_finishes f JOIN job_runs r ON r.id = f.run_id
WHERE f.finished_at >= $1 AND f.finished_at < $2;
```

### Anti-patterns

- **Summing every row of `job_stats_hourly`.** Each hour stores its groups, per-kind totals, and an hour
  total; summing across classes counts every run two or three times. Pick one class.
- **`GROUP BY kind` over `jobs`.** No index serves it; it reads every job ever retained. Per-kind history is
  `job_stats_hourly`; per-kind backlog is `CountActiveByKind`.
- **`ORDER BY created_at` on `jobs`.** No index carries it. Order by `id` (insertion order) or, for
  finished jobs, by `finalized_at` within a terminal `state` (`jobs_finished`).
- **Filtering `job_runs` by `finished_at` directly.** It is not indexed — on purpose. Use
  `job_run_finishes`.
- **Raw `job_runs` windows longer than a day.** Read `job_stats_hourly` for anything past the last few
  hours.
- **Treating `updated_at` as a start time.** Lease renewals move it.

<br>

## Prometheus or database stats?

They answer different questions, and a deployment usually wants both.

| | Prometheus (`observers.NewMetrics`, `/metrics`) | Database stats (this guide) |
|---|---|---|
| Resolution | seconds | an hour (a partial hour at the edges) |
| History | your Prometheus retention | 400 days of rollups, past job retention |
| Scope | the processes you scrape | every process that writes the database |
| Survives restarts | counters reset | yes |
| Per-job drill-down | no | yes — run ids, errors, outputs |
| Best for | alerting on what is happening this minute | dashboards, trends, "what happened to job X" |

Alert on the queue's live health (lag, failure counters) from Prometheus; build trend, capacity, and
regression views from the rollups. Minute-resolution history is deliberately not stored in the database.

<br>

## Retention and the rollup

Retention deletes old jobs with their runs and finish-log entries; the rollups outlive them, which is what
makes month-over-month trends possible on a database that keeps a week of jobs. The two are coupled so the
rollup never loses data:

- **Retention waits for the rollup.** With the rollup on, the retention cutoff is the earlier of
  `now − RetentionMaxAge` and the rollup watermark, and nothing is pruned until the rollup has run once.
  While the rollup is still working back through history it holds at the oldest hour the rollup keeps, so
  nothing it has yet to count is taken. `flywheel prune` holds the same way whenever its config has the
  rollup on (`--ignore-stats-rollup` prunes past it); any other caller sets
  `RetentionOpts.HoldForStatsRollup`.
- **A retention window shorter than one hour plus the grace is refused** (`ErrValidation`) when the rollup
  is on: no run would live long enough to be counted. `flywheel serve` instead turns its default rollup
  off, with a warning, so a config written before this release keeps starting.
- **Rebuilding pruned hours is refused** unless forced: `RebuildStats` will not replace a complete rollup
  with a recount of the runs retention left.
- **Rollups have their own retention**, `StatsRetention` (400 days).

<br>

## What the anomaly detector catches

`Anomalies` compares each kind's recent hours to its own baseline over the previous week, and is built to
page a person rarely and rightly:

- **`duration_regression`** — an hour's p50 (or p95, from 100 samples) is at least **2×** the baseline's
  **and** at least **250 ms** above it, with 20 samples in the hour and 200 in the baseline. A low-volume
  kind pools up to 6 hours into each sample, and its samples never overlap.
- **`failure_spike`** — an hour has at least **5** discards at a rate at least **3×** the baseline's and
  **5 percentage points** above it, against a baseline of at least 200 ended jobs.
- Either holds only when **2 of the last 3** hours (samples) breach, and `Onset` marks the first hour it
  holds.

Durations are successful runs only, so a burst of fast failures never reads as a speed-up, and the hours
under evaluation are never part of their own baseline. Every threshold is an `AnomalyThresholds` field.

**The envelope it was tested in.** The noise suite synthesizes a fortnight of hourly history for five
kinds — high, medium, and low volume, a bursty one with 6× volume spikes, and one brand new two days before
the end — with lognormal durations, ±30% hourly jitter, and a 1.5× diurnal swing. Over 60 seeds (50,400
kind-hours evaluated) the defaults raised **zero** false anomalies at a duration spread of σ ≤ 0.7, and
flagged an injected 3× slowdown and an injected failure spike **exactly once each, every seed**. With every
kind's spread raised to σ = 1.0 — individual runs routinely 3× their median — false onsets appeared in
about 2.5% of seed-weeks, all in the pooled low-volume and bursty kinds. A kind like that wants a higher
`DurationRatio`.

The slow-run warning (`slow_running` on the heartbeat, and one `warn` per run) uses the
[`ListRunning`](#live-what-is-happening-now) rule: past `max(3 × p99, p99 + 1m)`, with 200 baseline
samples.

<br>

## Measured cost

### Reads

Measured on PostgreSQL 17 at 1,000,000 runs over 30 days across 20 kinds, plus one hour of 100,000 runs,
with every closed hour rolled up and the open hour read raw. Each figure is the median wall time of the
whole API call; the plans behind them are in
[`benchmarks/stats-plans-1m.txt`](benchmarks/stats-plans-1m.txt).

<!-- measured-reads -->
| Read | Median |
|---|---|
| `Stats`, last 24 h, every kind | 12.3 ms |
| `Stats`, last 24 h, one kind | 9.4 ms |
| `Stats`, last 30 d, every kind | 79.4 ms |
| `StatsSeries`, 30 d hourly | 20.5 ms |
| `StatsSeries`, 30 d daily | 19.0 ms |
| `Baselines`, 7 d (memoized; first call ~20 ms) | 0.08 ms |
| `Anomalies`, latest hour | 21.1 ms |
| `ListRunning` with baselines | 2.5 ms |
| `ListFinished` | 2.5 ms |
| `RecentFailures` | 1.0 ms |
| `SlowRuns`, 24 h | 69.1 ms |
| `QueueDepths` | 2.2 ms |
| `CountActiveByKind` | 2.1 ms |
| `ListJobs` | 1.2 ms |
| Rolling up one hour of ~1.4k runs | 15.2 ms |
| Rolling up one hour of 100k runs | 263 ms |
| Rolling up all 30 days from scratch (720 hours, 58k rollup rows) | 8.8 s |

On SQLite, the same reads over a 100k-run month (`go test -bench SQLite ./internal/core`) are all under
15 ms; `Stats` for the last day is about 1 ms.

### Writes

The analytics add work to every job's finalize: the five `job_runs` columns, the finish-log entry (folded
into the finalize's `job_runs` UPDATE on PostgreSQL, so no extra round trip), and the `jobs_finished`
entry when a job reaches a terminal state. The `job_runs` finalize UPDATE stays HOT-eligible, as before.
Measured against the previous release on the same PostgreSQL 17:

- **About 0.43 KB of WAL per job** in total — roughly 0.27 KB the finish log, 0.18 KB `jobs_finished`, and
  0.14 KB the wider run rows. At a 1M-job soak that is **+9.6% WAL per job with no throughput loss**; on a
  100k drain of zero-work jobs, whose per-job WAL is only about 2 KB to begin with, it is +22% WAL and
  −3% throughput (within noise).
- **An index on `job_runs.finished_at`** — the obvious design — **was measured and rejected**: it makes
  every finalize a non-HOT update, which cost about 10% of drain throughput, 21% more WAL per job, and 38%
  more `job_runs` table at 1M jobs.

The full A/B is in [BENCHMARKS.md](BENCHMARKS.md#job-analytics).
