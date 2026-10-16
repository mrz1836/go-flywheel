<div align="center">

# 🎡&nbsp;&nbsp;go-flywheel

**Durable, Postgres - and SQLite-backed job runtime for Go**

<br/>

<a href="https://github.com/mrz1836/go-flywheel/releases"><img src="https://img.shields.io/github/release-pre/mrz1836/go-flywheel?include_prereleases&style=flat-square&logo=github&color=black" alt="Release"></a>
<a href="https://golang.org/"><img src="https://img.shields.io/github/go-mod/go-version/mrz1836/go-flywheel?style=flat-square&logo=go&color=00ADD8" alt="Go Version"></a>
<a href="https://github.com/mrz1836/go-flywheel/blob/master/LICENSE"><img src="https://img.shields.io/github/license/mrz1836/go-flywheel?style=flat-square&color=blue" alt="License"></a>

<br/>

<table align="center" border="0">
  <tr>
    <td align="right">
       <code>CI / CD</code> &nbsp;&nbsp;
    </td>
    <td align="left">
       <a href="https://github.com/mrz1836/go-flywheel/actions"><img src="https://img.shields.io/github/actions/workflow/status/mrz1836/go-flywheel/fortress.yml?branch=master&label=build&logo=github&style=flat-square" alt="Build"></a>
       <a href="https://github.com/mrz1836/go-flywheel/actions"><img src="https://img.shields.io/github/last-commit/mrz1836/go-flywheel?style=flat-square&logo=git&logoColor=white&label=last%20update" alt="Last Commit"></a>
    </td>
    <td align="right">
       &nbsp;&nbsp;&nbsp;&nbsp; <code>Quality</code> &nbsp;&nbsp;
    </td>
    <td align="left">
       <a href="https://codecov.io/gh/mrz1836/go-flywheel"><img src="https://codecov.io/gh/mrz1836/go-flywheel/branch/master/graph/badge.svg?style=flat-square" alt="Coverage"></a>
    </td>
  </tr>

  <tr>
    <td align="right">
       <code>Security</code> &nbsp;&nbsp;
    </td>
    <td align="left">
       <a href="https://scorecard.dev/viewer/?uri=github.com/mrz1836/go-flywheel"><img src="https://api.scorecard.dev/projects/github.com/mrz1836/go-flywheel/badge?style=flat-square" alt="Scorecard"></a>
       <a href=".github/SECURITY.md"><img src="https://img.shields.io/badge/policy-active-success?style=flat-square&logo=security&logoColor=white" alt="Security"></a>
    </td>
    <td align="right">
       &nbsp;&nbsp;&nbsp;&nbsp; <code>Community</code> &nbsp;&nbsp;
    </td>
    <td align="left">
       <a href="https://github.com/mrz1836/go-flywheel/graphs/contributors"><img src="https://img.shields.io/github/contributors/mrz1836/go-flywheel?style=flat-square&color=orange" alt="Contributors"></a>
       <a href="https://mrz1818.com/"><img src="https://img.shields.io/badge/donate-bitcoin-ff9900?style=flat-square&logo=bitcoin" alt="Bitcoin"></a>
    </td>
  </tr>
</table>

</div>

<br/>
<br/>

<div align="center">

### <code>Project Navigation</code>

</div>

<table align="center">
  <tr>
    <td align="center" width="33%">
       📦&nbsp;<a href="#-installation"><code>Installation</code></a>
    </td>
    <td align="center" width="33%">
       🚀&nbsp;<a href="#-quick-start"><code>Quick&nbsp;start</code></a>
    </td>
    <td align="center" width="33%">
       📖&nbsp;<a href="#-guides"><code>Guides</code></a>
    </td>
  </tr>
  <tr>
    <td align="center">
       🧪&nbsp;<a href="#-examples--tests"><code>Examples&nbsp;&&nbsp;Tests</code></a>
    </td>
    <td align="center">
       📚&nbsp;<a href="#-documentation"><code>Documentation</code></a>
    </td>
    <td align="center">
       ⚡&nbsp;<a href="#-benchmarks"><code>Benchmarks</code></a>
    </td>
  </tr>
  <tr>
    <td align="center">
       🤝&nbsp;<a href="#-contributing"><code>Contributing</code></a>
    </td>
    <td align="center">
      🛠️&nbsp;<a href="#-code-standards"><code>Code&nbsp;Standards</code></a>
    </td>
    <td align="center">
      🤖&nbsp;<a href="#-ai-usage--assistant-guidelines"><code>AI&nbsp;Usage</code></a>
    </td>
  </tr>
  <tr>
    <td align="center">
       👥&nbsp;<a href="#-maintainers"><code>Maintainers</code></a>
    </td>
    <td align="center">
       ⚖️&nbsp;<a href="#-license"><code>License</code></a>
    </td>
    <td align="center">
    </td>
  </tr>
</table>
<br/>

## 🧩 About

**go-flywheel** is a durable, database-backed **job runtime** for Go. It turns an ordinary
PostgreSQL or SQLite database into a reliable work queue with typed workers, a periodic
scheduler, automatic retries, and a complete per-run audit trail — no Redis, no broker, no
external job server to operate. Your jobs live in the same database as your data, so enqueuing
work can be transactional with the rest of your application.

Use it two ways: **embed** it in your app (define `Worker[A]` types, wire a `Node`, and let it
run the runner + scheduler + health server in ~10 lines), or **run it locally** as a daemon with
the [`flywheel` CLI](cmd/flywheel/README.md) — a drop-in cron replacement that runs your shell
scripts, Python scripts, magex/mage build tasks, and HTTP calls durably, with retries, backfill,
and a full audit trail.

The runtime is built from focused, composable pieces:

- **Typed workers** — generic `Worker[A]` interface, registered by `Kind()` ([registry.go](internal/core/registry.go))
- **One-call lifecycle** — `Node` runs N runners + the scheduler + an optional health/metrics server and drains cleanly on shutdown ([node.go](internal/node/node.go))
- **Bounded concurrency** — a runner keeps up to `Concurrency` jobs in flight, each slot refilling independently so one slow job never idles the rest; explicit `Stop`/`Drain` with an in-flight count ([runner.go](internal/core/runner.go))
- **Scheduler** — periodic / cron job enqueuing plus stuck-lease recovery; declare schedules in code with `SchedulerConfig.Periodics` (applied on start, re-created if deleted) or `UpsertPeriodic` ([scheduler.go](internal/core/scheduler.go), [schedule.go](internal/core/schedule.go))
- **Retries with backoff** — exponential backoff with jitter, overridable per worker; consecutive poll failures climb their own ladder so a failing database is not hammered ([runner.go](internal/core/runner.go))
- **Lease-based recovery** — orphaned, crashed jobs reclaimed via `leased_until` sweeps ([scheduler.go](internal/core/scheduler.go))
- **Worker timeouts** — per-job or per-kind execution deadlines that classify as a retryable timeout ([runner.go](internal/core/runner.go))
- **Per-run audit** — the `job_runs` table records every attempt: outcome, timing, queue wait, cost, and whether its claim was lost ([read.go](internal/core/read.go))
- **Job analytics for dashboards** — what is running now, what just finished, per-kind stats over any window, hourly and daily trends from rollups that outlive retention, and low-noise regression alerts ([docs/INTEGRATING.md](docs/INTEGRATING.md))
- **Idempotent enqueue** — `jobs_unique_key` partial unique index dedupes work, and a collision names the job holding the key ([client.go](internal/core/client.go))
- **Waiting on a job** — `WaitForJob` blocks until a job reaches a terminal state, polling with backoff under your deadline; joined with the job a collision names, a unit of work runs once for every caller that asks while it is in flight ([wait.go](internal/core/wait.go))
- **Outbox pattern** — enqueue on the caller's own `*gorm.DB` transaction for exactly-once side effects ([client.go](internal/core/client.go))
- **Follow-up jobs (DAG)** — workers return child jobs that are enqueued atomically ([types.go](internal/core/types.go))
- **Bulk enqueue** — `InsertMany` writes N jobs in bounded, dialect-aware chunks, honoring the outbox transaction and per-row idempotency ([batch.go](internal/core/batch.go))
- **Free-form routing** — a `ExecutorClass` label routes jobs to executor pools; empty is the wildcard ([types.go](internal/core/types.go))
- **Pre-claim admission** — an optional `Limiter` gates a runner before it claims, keyed on an arbitrary downstream resource; a job that cannot run yet is never claimed, leased, or audited. Ships an in-process token bucket and a shared database-backed limiter ([limiter.go](internal/core/limiter.go), [limiter_db.go](internal/core/limiter_db.go))
- **Observability built in** — a dependency-free `Observer` seam with ready-made metrics, slog, and Prometheus adapters, queue-health/lag inspection, a `/metrics` endpoint, and `flywheel status` ([observer.go](internal/core/observer.go), [observers/](observers), [health.go](internal/core/health.go))
- **Postgres + SQLite** — `FOR UPDATE SKIP LOCKED` and `BEGIN IMMEDIATE` drivers ([driver_postgres.go](internal/core/driver_postgres.go), [driver_sqlite.go](internal/core/driver_sqlite.go))
- **Generic workers** — ready-made `ExecWorker`, `ShellWorker`, `PythonWorker`, `MageWorker` (magex/mage), and `HTTPWorker` so local scripts and build tasks need no custom Go ([workers/](workers))

<br/>

## 📦 Installation

**go-flywheel** requires a [supported release of Go](https://golang.org/doc/devel/release.html#policy).
```shell script
go get -u github.com/mrz1836/go-flywheel
```

Get the [MAGE-X](https://github.com/mrz1836/mage-x) build tool for development:
```shell script
go install github.com/mrz1836/mage-x/cmd/magex@latest
```

<br/>

## 🚀 Quick start

A job runtime earns its keep when work is **slow, flaky, costly, or must-not-be-lost** —
which in 2026 describes almost every LLM and third-party API call your app makes. Blocking
a web request on a 30-second model call that might rate-limit is fragile; *enqueuing* that
call and letting flywheel run it in the background is durable. Each job is **retried** on
failure, **recovered** if the process crashes mid-run, and **audited** down to its
per-attempt cost.

It's three moving parts — **① define the work, ② enqueue it, ③ run a `Node` that processes it:**

```go
package main

import (
	"context"
	"os/signal"
	"syscall"

	"github.com/glebarez/sqlite" // pure-Go SQLite: no cgo, no C compiler
	flywheel "github.com/mrz1836/go-flywheel"
	"gorm.io/gorm"
)

// ① Define the work: typed args + a worker that handles them.
//    The job here is "summarize a document with an LLM" — slow, metered, and
//    occasionally rate-limited, so you never want to run it inline or lose it.

type SummarizeDoc struct {
	DocID string `json:"doc_id"`
}

func (SummarizeDoc) Kind() string { return "summarize_doc" } // args name the kind they want

// Summarizer holds whatever the worker needs: a model client, DB handle, etc.
type Summarizer struct{}

func (Summarizer) Kind() string { return "summarize_doc" } // worker names the kind it handles

func (Summarizer) Work(ctx context.Context, job *flywheel.Job[SummarizeDoc]) (flywheel.Result, error) {
	summary, costMicros, err := callLLM(ctx, job.Args.DocID)
	if err != nil {
		return flywheel.Result{}, err // returning an error → automatic retry with backoff
	}
	return flywheel.Result{
		Output:     summary,    // recorded on this attempt's audit row
		CostMicros: costMicros, // track spend per attempt, no extra plumbing
	}, nil
}

func main() {
	db, _ := gorm.Open(sqlite.Open("file:flywheel.db?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)&_txlock=immediate"), &gorm.Config{})
	_ = flywheel.Migrate(db) // creates flywheel's tables and indexes

	reg := flywheel.NewRegistry()
	flywheel.Register(reg, Summarizer{})

	// ② Enqueue work. Returns instantly — the caller never waits on the LLM.
	//    (Pass InsertOpts.Tx to enqueue inside your own DB transaction.)
	_, _ = flywheel.Insert(context.Background(), flywheel.NewClient(db),
		SummarizeDoc{DocID: "42"}, flywheel.InsertOpts{})

	// ③ Run a Node: it claims jobs, runs your worker, retries failures, and
	//    drains cleanly on Ctrl+C. (SQLite runs one job at a time; point it at
	//    Postgres and raise Concurrency to run several at once.)
	node, _ := flywheel.NewNode(flywheel.NodeConfig{
		Runners: []flywheel.RunnerConfig{{
			DB: db, Driver: flywheel.NewSQLiteDriver(db), Registry: reg,
			Queues: []string{"default"}, Concurrency: 1, ClaimAnyClass: true,
		}},
	})

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	_ = node.Run(ctx) // blocks until Ctrl+C, then drains in-flight jobs
}

// callLLM stands in for your real model call (Anthropic, OpenAI, a local model…).
func callLLM(ctx context.Context, docID string) (summary string, costMicros int64, err error) {
	return "TL;DR of doc " + docID, 1_200, nil
}
```

That's a durable AI pipeline: enqueue returns instantly, the `Node` summarizes documents
in the background, a failed model call retries itself with backoff, and every attempt —
including what it cost — lands in the `job_runs` audit table. Need periodic or cron-style
runs too? Add a `Scheduler` to the `Node` and declare them in its `Periodics` (see
[Declared periodics](#declared-periodics), and [`examples/`](examples) for the full set).

`Migrate` above is the right call when the database is the runtime's alone. If flywheel's tables
will share a database with your own application schema, use the host-owned path instead — see
**Schema setup** below, and pick exactly one of the two.

<br/>

## 📖 Guides

Each section below is self-contained; open the ones you need.

<details>
<summary><strong><code>Schema setup — pick exactly one install path</code></strong></summary>
<br>

`go-flywheel` owns fourteen tables, the ones `Models()` returns — `jobs`, `job_runs`, and `job_periodics`;
`limiter_buckets` and `limiter_holds` for the shared database-backed limiter (additive: empty unless you
construct a `DBLimiter`); `job_run_finishes`, `job_stats_hourly`, and `job_stats_progress`, the finish
log every finalize writes and the stats rollup's tables; `job_audit`, the history every control
writes; `job_kinds`, the kind catalog `PublishKinds` writes (empty unless you publish one);
`job_error_groups_hourly`, the error groups the stats rollup writes beside its hours;
`job_queue_samples`, each queue's depth over time (empty unless a Scheduler samples queues);
`job_nodes`, one row per process lifetime of a registering Node (empty unless a Node sets
`RegistryInterval`); and `job_queues`, the queues an operator paused, by whom, and why — and there
are **two ways to install them. Pick exactly one.** They are not layers; running both means two
migration authorities against one database.

| Question | **Library-owned** | **Host-owned** |
|---|---|---|
| Who creates the tables? | `Migrate(db)` | your loader, from `Models()` |
| Who creates the indexes? | `Migrate(db)` | you, from `IndexSet(dialect)` / `InstallIndexes` |
| Who sets the storage parameters? | `Migrate(db)` | you, from `InstallStorageParameters` |
| Who owns migration history? | nobody — `AutoMigrate` is declarative | your migration tool |
| Does the runtime run DDL at startup? | yes, every start | no |
| Is a co-located host schema safe? | only if the host's tooling excludes all fourteen tables | yes — the tables are in your loader |
| **Pick this when** | the database is the runtime's alone: a dedicated queue database, a CLI, a local SQLite file | the runtime's tables share a database with an application schema |

**The last row is the rule: a shared database means host-owned.** If your app's own tables live in the
same database, your migration tool must know about all fourteen — a tool that cannot see them will happily
propose dropping them. A tool told to leave the runtime's tables alone by name needs every name above.

**Library-owned** — one call, no external tooling:

```go
import "github.com/mrz1836/go-flywheel"

if err := flywheel.Migrate(db); err != nil { // db is a *gorm.DB
    return err
}
```

`Migrate` runs `AutoMigrate` over the row structs, sets the `jobs` table's storage parameters, and
applies the partial/unique indexes GORM cannot express from struct tags. It is idempotent against an
up-to-date schema — a re-run creates nothing and changes nothing — so it is safe to run on every deploy.

**Host-owned** — your loader creates the tables, you apply the indexes:

```go
// 1. In your schema loader (Atlas / atlas-provider-gorm, or any GORM-based generator),
//    so your migration tool knows all fourteen tables exist and never proposes dropping them.
stmts, err := gormschema.New("postgres").Load(
    append(myapp.AllModels(), flywheel.Models()...)...,
)

// 2. In your install/deploy path, right where you apply your migrations.
if err := flywheel.InstallIndexes(ctx, db); err != nil {
    return err
}
if err := flywheel.InstallStorageParameters(ctx, db); err != nil {
    return err
}
```

Step 2 is **not optional and not an optimization.** `Models()` gives your loader the tables and columns;
it does not give it the indexes, because every one of them has a `WHERE` predicate or spans columns a
GORM struct tag cannot express. `IndexSet` declares seventeen indexes, and four of them are
correctness-bearing: `jobs_unique_key`, `jobs_unique_active_key`, `job_runs_job_attempt`, and
`idx_job_periodics_slug`. Without `jobs_unique_key` and `jobs_unique_active_key` the database accepts
duplicate enqueues and **`ErrAlreadyEnqueued` is never returned.** Use `flywheel.IndexSet(dialect)` when
you want them classified:

```go
for _, idx := range must(flywheel.IndexSet("postgres")) {
    if idx.Kind == flywheel.IndexCorrectness {
        // omitting this one does not cost throughput — it removes a guarantee
    }
}
```

> **Apply the indexes to a database; do not paste them into a generated migration.** Your loader still
> does not describe them, so a migration that creates them has its *next* diff see indexes in the
> migration directory that are absent from the desired state — and propose dropping them back out. Run
> them as an install step instead: a versioned diff compares the directory against the loader and never
> inspects the live database, so indexes created outside the directory are invisible to it. (A
> declarative `schema apply` *does* inspect the database and would drop them, which is one more reason a
> shared database wants versioned mode.)

**The install compares indexes by definition, not by name.** `CREATE INDEX IF NOT EXISTS` matches on the
name alone, so a database already carrying an index of that name keeps its old definition through an
install that reports success — a re-keyed index or a dropped `WHERE` predicate lands nowhere and the
claim it was meant to serve quietly runs the slow plan. So the install reads each definition back from
the catalog: an absent index is created, a matching one is left alone, and one whose definition has
**drifted** is reported rather than silently kept — `InstallIndexes` and `Migrate` return an error
naming the index, what is installed, and what is expected. Call `InspectIndexes(ctx, db)` to make that a
CI parity check: it reports every index that is absent or drifted and writes nothing, so a host that owns
its schema can assert it matches the runtime without copying a normalizer. To correct drift in place, set
`IndexOpts{Reconcile: true}` (or `MigrateOpts{Reconcile: true}`) — it drops and recreates the drifted
index, which takes an `ACCESS EXCLUSIVE` lock on the table for the rebuild, so it is opt-in and the
default leaves that lock to you. `InspectStorageParameters(ctx, db)` gives the same parity check for the
`jobs` storage parameters, which the unconditional `ALTER TABLE … SET` install converges on its own.

> Only PostgreSQL and SQLite are supported, because both express the partial indexes the runtime relies
> on. Every entry point returns `flywheel.ErrUnsupportedDialect` for anything else rather than silently
> dropping idempotency. The module takes **no** hard dependency on Atlas or any external migration tool.

</details>

<details>
<summary><strong><code>Concurrency, claim batching, and graceful drain</code></strong></summary>
<br>

**`Concurrency` is the pool size.** A runner keeps up to that many jobs in flight, claims to fill
whatever slots are free, and dispatches each job independently — so a slot becomes claimable the moment
*its own* job finalizes, with no reference to its siblings. One slow job holds one slot, never the loop:
at `Concurrency: 8`, a single 60-second job leaves the other seven slots claiming and running throughout.

At `Concurrency: 1` a runner dispatches inline on its own loop goroutine — one job in flight, strictly
sequential, no goroutine per job. A SQLite driver requires it: `NewRunner` returns
`ErrSQLiteConcurrency` otherwise, because the SQLite claim is a serialized SELECT-then-UPDATE with no
`SKIP LOCKED`.

```go
flywheel.RunnerConfig{
    // Up to eight jobs in flight. Slots refill independently.
    Concurrency: 8,

    // Optional. Caps how many jobs one claim asks for. Zero claims exactly the
    // free-slot count, which is right for almost every deployment; set it lower to
    // smooth claim bursts across a fleet of runners hitting one database. It is
    // never raised *above* the free-slot count — a claimed job the runner has no
    // slot to start is a lease burning in a queue.
    ClaimBatchSize: 2,

    // Optional. Ceiling on the delay between polls after consecutive failures;
    // zero selects 30s. The delay starts at PollInterval, doubles per consecutive
    // failure with jitter, and resets on the first success.
    MaxPollBackoff: 30 * time.Second,

    // Optional. Base and ceiling of the per-job retry ladder; zero selects 1s and
    // 1m. A retry's delay starts at RetryBackoffBase, doubles per attempt with
    // jitter, and holds flat at MaxRetryBackoff. Size the ceiling against
    // MaxAttempts to match the retry cadence to how long the dependency stays down.
    RetryBackoffBase: 30 * time.Second,
    MaxRetryBackoff:  30 * time.Minute,
}
```

**A failing database is not polled at the empty-queue rate.** Consecutive poll failures climb an
exponential ladder from `PollInterval` to `MaxPollBackoff`, and each attempt logs once — so the log rate
follows the backoff rather than the poll interval. Without it, a fleet of runners against a database that
is restarting or failing over polls it ten times a second forever and writes an error line each time:
a retry storm aimed at a recovering database, plus an unbounded log volume.

`Run` never gives up; it backs off at the ceiling until the database returns. `RunUntilIdle` tolerates a
blip and gives up once the ladder saturates — `⌈log₂(MaxPollBackoff / PollInterval)⌉ + 1` attempts, about
51 seconds at the defaults. The bound is the ladder, not the context, so a caller that passes no deadline
still gets an answer.

**A per-job retry has its own ladder, and its ceiling is sized against the attempt budget.** A failed
attempt is rescheduled after a delay that starts at `RetryBackoffBase` (default 1s), doubles per attempt
with jitter, and holds flat at `MaxRetryBackoff` (default 1m) — separate from the poll ladder above, and
overridable per worker via `Retryable.NextRetry`. The default ceiling suits a dependency that recovers in
seconds: at the defaults a 25-attempt budget saturates at a minute after seven rungs and is spent in about
nineteen minutes. It serves a multi-hour outage poorly — the job fails permanently while the dependency is
still down, and most of those attempts ran a minute apart while it could not possibly succeed. Raise the
pair together to stretch the same budget across the outage. With `RetryBackoffBase: 30s` and
`MaxRetryBackoff: 30m` the ladder climbs `30s → 1m → 2m → 4m → 8m → 16m` and then holds at 30m, so
`MaxAttempts: 8` spans about an hour and `MaxAttempts: 20` spans most of a workday (~7h). The cap only
widens the spacing of the later rungs, never the first-attempt delay. Pick the outage length you must ride
out, then choose the ceiling and `MaxAttempts` so their ladder covers it.

**Args that do not decode are never retried.** When a job's stored args do not fit its worker's args
type, the worker never runs: the attempt fails with `jobs: decode args for kind "…": …`, which matches
`ErrDecodeArgs` and `ErrValidation`, and it is classified `validation` without asking the worker's
`Classifier`, so the job is discarded on that first attempt instead of spending its budget on bytes that
will never change. Fix the producer or the args type, then replay. A worker that must accept a shape its
type cannot decode declares `json.RawMessage` args and decodes inside `Work`, where its `Classifier`
applies.

**Drain is explicit.** `Stop` and `Drain` are safe before `Run`, concurrently with it, and after it
returns:

```go
runner.Stop()                    // claim nothing further; non-blocking, idempotent, final
n := runner.InFlight()           // how many jobs are executing right now

ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()
if err := runner.Drain(ctx); err != nil {
    var timeout *flywheel.DrainTimeoutError
    if errors.As(err, &timeout) {
        log.Warn("drain timed out", "in_flight", timeout.InFlight)
    }
}
```

`Stop` bounds *when the next claim is issued*, not what happens to a claim already in flight: a batch
that came back from `Dequeue` after `Stop` landed is already leased, so it is dispatched rather than
stranded until the sweep, and `Drain` waits for it too.

**A stopped `Runner` is spent — there is no restart.** If your process outlives a single invocation (a
Lambda on a warm container, a bounded drain driven by an external scheduler), build a `Runner` per
invocation rather than stopping a shared one: a stopped `Runner` returns immediately having claimed
nothing, with no error to notice. `NewRunner` does no I/O, and the `Registry`, `Driver`, and database
handle are all safe to share. `Drain` **does not cancel in-flight work** — a
worker that must be interrupted should respect the context it was given, and `DefaultTimeout` already
bounds a hung attempt. Its contract is "no new claims, then wait", which is what makes a rolling deploy
lose no work. On timeout the still-running jobs keep their leases and are recovered by the lease sweep,
exactly as they would be after a process kill. A rolling deploy that adds a job kind has one more thing to
get right; see **Rolling deploys and new job kinds** below.

**A `Node` does this for you.** Cancelling a `Node`'s context is a *drain* request, not an abort: every
runner is told to stop claiming, each is drained against `NodeConfig.DrainTimeout`, the timeout warning
names how many jobs were still in flight, and only then is the scheduler and health server torn down.
A zero `DrainTimeout` waits for in-flight work however long it takes — genuinely unbounded, since the
heartbeat renews a running job's lease indefinitely. Size it to the longest drain your deployment will
tolerate.

</details>

<details>
<summary><strong><code>Rolling deploys and new job kinds</code></strong></summary>
<br>

A runner claims every ready job on its queues, whatever its kind. During a rolling deploy, runners of the
old release and the new one poll the same queues, so a runner of the old release can claim a job of a kind
only the new release registers, whether a new producer, an operator, or a periodic the new release
declares enqueued it. It does not discard that job; it **defers** it.

**How deferral works.** A runner that claims a job of a kind its `Registry` does not register hands the
job back as a snooze while the job is younger than `UnknownKindGrace`, measured from the job's enqueue
(15 minutes by default). A snooze spends none of the retry budget, so the job reaches a runner of the new
release with the headroom it was enqueued with. Its attempt number still advances, one per claim: the
worker's first run of the job can see an `Attempt` above 1, and a retry backoff keyed on the attempt
starts from that rung. Each deferral waits on a ladder of its own, whatever the retry settings: a second
on the first claim, doubling per claim, capped at a minute and at what is left of the window. Past the
window the job is discarded with `ErrUnknownKind`, as work no runner in the fleet can run.

```go
flywheel.RunnerConfig{
    // Optional. How long a job of a kind this runner does not register is
    // deferred, from its enqueue, before it is discarded. Zero selects 15m; a
    // negative value discards it on its first claim.
    UnknownKindGrace: 30 * time.Minute,
}
```

**Size the window to the deploy.** It must exceed the span from the moment the new kind can first be
enqueued to the moment the last old runner has drained. A job enqueued in that span is never older than
the span when an old runner claims it, so it is covered, including one inserted with a `ScheduleAt`. The
default suits a rollout that finishes in minutes; raise it for a slow, canaried, or paused rollout.

**The grace is applied by the runner that lacks the kind, which is the old one.** So the first rollout of
a release that carries `UnknownKindGrace` is not protected by it: the runners it replaces predate it, and
they discard a job of a kind they do not register on its first claim. Any runner older than that release
does the same. Until the whole fleet runs a release that has it, and for a producer that ships ahead of
its consumers, use one of the two orders that need no grace:

- **Put the new kind on a new queue** that only the new release's runners poll. An old runner never
  claims a job it cannot run.
- **Roll out in two phases.** Deploy the worker everywhere first, then the code that enqueues the kind,
  a declared periodic of the kind included. This is also the rule for a producer that lives in another
  service.

**The window is measured from enqueue, not from the first claim.** It covers the work a deploy creates
and nothing older. A job older than the window that reaches a runner without its kind is discarded on
its first claim: a replay of old work onto runners that do not register its kind, or a job of a kind the
new release removed. Before removing a worker, drain its kind; `CountActiveByKind` shows what is left.

**A rollback strands new-kind jobs the same way.** Rolling back to a release without the kind puts every
runner on the old side, so jobs of the new kind are deferred for the window and then discarded, or
discarded at once by a release that predates `UnknownKindGrace`. Roll forward within the window, or
replay them once a release with the worker is back.

**Seeing deferrals.** The runner logs `jobs: deferring jobs of a kind this runner does not register` at
warn, once per kind for each runner (and again if it is still deferring the kind once the grace has
passed since), naming the kind, the job it deferred, its queue, the grace, and when that job will be
discarded (`discard_at`). Every discard is logged at error as
`jobs: discarded a job of a kind this runner does not register`, with the job's age. To everything else
a deferral is an ordinary snooze: a `job_runs` row with outcome `snooze`, `error_message`
`jobs: unknown job kind`, and no `error_class`; an increment of
`flywheel_jobs_finished_total{outcome="snooze"}`; and a job that counts as `scheduled`, not ready. A
discarded job shows in `RecentFailures` and `flywheel status` with `jobs: unknown job kind`. A job of a
kind no runner registers writes at most 21 `job_runs` rows over the default window, one per claim and
whatever the retry settings, before it is discarded.

**Recovering discarded jobs.** Once every runner registers the kind, replay them:

```go
res, err := flywheel.Replay(ctx, db, flywheel.ReplayOpts{
    Kinds:       []string{"new_kind"},
    FailedSince: deployStart,
})
```

A plain replay is enough: the deferrals spent none of the budget, so a discarded job keeps all of its
headroom but the one attempt its discard used, and `ResetAttempts` with no `Budget` would add
`max_attempts`, which every deferral raised, on top.

A replay keeps each job's enqueue time, so a replayed job is already past its window. Replay only once
the deploy, or the roll-forward, is complete; a runner still without the kind discards it again at once.

**`RunUntilIdle` waits out the window.** A deferred job is not terminal, so `RunUntilIdle` over a queue
holding a job of a kind no runner registers keeps polling until the window has passed and the job is
discarded: 15 minutes at the default, longer than `go test`'s default 10-minute timeout. The window is
read from the context's clock, so under a frozen test clock a deferred job never ages and `RunUntilIdle`
runs until its context ends. A test that enqueues a kind it deliberately leaves unregistered sets
`UnknownKindGrace` negative. For a test that forgot to `Register` a worker, the warn line names the kind.

</details>

<details>
<summary><strong><code>Describing kinds — a catalog, validated enqueue, and worker defaults</code></strong></summary>
<br>

A worker can describe its kind beside the code that runs it, so an operator tool reads one catalog
instead of a hand-kept list that drifts from the code. Four optional interfaces declare it, and
`Register` reads each one once:

| Interface | Declares |
|---|---|
| `Describer` — `Description() string` | a one-line description |
| `ArgsSchemer` — `ArgsSchema() (json.RawMessage, error)` | a JSON Schema for the args, from any generator you use |
| `ArgsExampler` — `ExampleArgs() json.RawMessage` | an example of the args |
| `ArgsValidator[A]` — `ValidateArgs(args A) error` | a check the decoded args must pass before a validated enqueue |

`Register` panics, naming the kind, when the schema fails to build or the schema or the example is not
valid JSON, as it does for a duplicate kind, so a bad declaration fails at startup. It does not check the
example against the schema or the validator; prove that in your own test.

```go
func (WelcomeMailer) Description() string { return "Send the welcome email" }

func (WelcomeMailer) ArgsSchema() (json.RawMessage, error) {
    return json.RawMessage(`{"type":"object","required":["user_id"]}`), nil
}

func (WelcomeMailer) ExampleArgs() json.RawMessage { return json.RawMessage(`{"user_id":"u1"}`) }

func (WelcomeMailer) ValidateArgs(args WelcomeMail) error {
    if args.UserID == "" {
        return errors.New("user_id is required")
    }
    return nil
}
```

**`Registry.Kinds` is the catalog.** It returns one `KindInfo` per registered kind, sorted by kind: what
the worker declares; the queue, priority, attempts, and timeout an insert of it defaults to (its
`Defaults`, else the runtime's, with a `Timeouter`'s timeout when `Defaults` names none); and whether it
validates its args, sets its own retry backoff, or classifies its errors.

**A validated enqueue refuses a bad request at once.** `Enqueue` writes any kind it is given: it is the
seed seam, so a mistyped kind waits out a runner's unknown-kind grace and is then discarded.
`Registry.Validate` checks a kind and its args first: an unregistered kind is `ErrUnknownKind`, and args
that do not decode into the kind's type (decoded exactly as the worker receives them) or that its
`ArgsValidator` refuses are a `*ValidationError` on `args`, carrying the reason. `Registry.Enqueue`
validates, fills the fields the call leaves zero from the kind's `Defaults`, and enqueues through the
Client you pass, so the Client's own options, its `InsertHook` among them, apply as on any insert. A
refused call writes nothing.

```go
id, err := reg.Enqueue(ctx, client, kind, args, flywheel.InsertOpts{Priority: 5})
switch {
case errors.Is(err, flywheel.ErrUnknownKind): // no such kind
case errors.Is(err, flywheel.ErrValidation):  // the args are refused; the error says why
}
```

**Where a kind's defaults apply, and in what order,** is **Worker defaults — declare a kind's routing
once**, below: `Registry.Enqueue`, a Client built with `ClientOpts.Registry`, and a Runner's follow-ups
fill each field a call leaves zero, a caller's value always wins, and a barrier's continuation takes the
`Barrier`'s own field, then the kind's default, then the spawning job's.

**Publish the catalog for a process without the registry.** An API or an operator tool often runs where
no worker is registered. A node calls `PublishKinds` at startup, and any process reads the catalog back
with `ListKinds`, or `flywheel kinds` from a shell:

```go
// In the process that registers the workers, after Register:
if err := flywheel.PublishKinds(ctx, db, reg); err != nil {
    return err
}

// Anywhere:
kinds, err := flywheel.ListKinds(ctx, db) // []flywheel.KindRecord, sorted by kind
```

A `KindRecord` is the kind's `KindInfo`, its last publisher (`PublishKindsOpts.Publisher`, else the
process's `host:pid`), and when it was first and last published. A publish upserts and never deletes: a
kind a release stops registering stays listed with an old `PublishedAt`, and a kind's first publish time
never moves. The catalog is the `job_kinds` table, which nothing else reads, so a Runner, a Scheduler,
and a Node start without it; [the upgrade](docs/INTEGRATING.md#upgrading-an-existing-database) adds it.

</details>

<details>
<summary><strong><code>Worker defaults — declare a kind's routing once</code></strong></summary>
<br>

A worker can declare where its jobs run and how often they retry, so no producer has to repeat it.
Implement `Defaults`, and `Register` reads six fields from it, once: `Queue`, `Priority`, `MaxAttempts`,
`Timeout`, `ExecutorClass`, and `Tags`. Every other `InsertOpts` field it returns is ignored.

```go
func (WelcomeMailer) Defaults() flywheel.InsertOpts {
    return flywheel.InsertOpts{Queue: "mail", MaxAttempts: 4, Timeout: 30 * time.Second, Tags: []string{"mail"}}
}
```

**They apply wherever an insert knows the registry.** Each field the call leaves zero takes the kind's
default, then the runtime's (queue `default`, priority 100, 25 attempts, no timeout, any executor class,
no tags). A field the call sets always wins. Tags count as unset when the call names none, and the kind's
then stand in whole: they are never merged with the call's.

| Insert | Takes the kind's `Defaults` |
|---|---|
| `Registry.Enqueue`, through any Client | yes, once the kind and its args pass `Registry.Validate` |
| `Insert`, `Enqueue`, `InsertMany`, `InsertManyTyped` on a Client built with `ClientOpts.Registry` | yes |
| A Scheduler's periodic fires through such a Client | yes, on the periodic's queue |
| A worker's `Result.FollowUps`, inserted by a Runner's finalize | yes, from `RunnerConfig.Registry` |
| The continuation of a worker's `Result.Barrier` | yes, resolved when the barrier is declared |
| A Client built with `NewClient`, and `flywheel enqueue` | no |
| A custom `Driver` that does not finalize through a built-in one | no |

```go
client := flywheel.NewClientWithOptions(db, flywheel.ClientOpts{Registry: reg})

// Lands on "mail" with 4 attempts and a 30-second timeout.
_, err := flywheel.Insert(ctx, client, WelcomeMail{UserID: id}, flywheel.InsertOpts{})

// The caller's queue wins; the attempts and the timeout still come from Defaults.
_, err = flywheel.Insert(ctx, client, WelcomeMail{UserID: id}, flywheel.InsertOpts{Queue: "urgent"})
```

**Follow-ups and barrier continuations take the Runner's registry.** A follow-up's empty queue,
priority, executor class, and tags take its kind's defaults, and so do its attempts and timeout, which a
`FollowUp` does not name. A kind the Runner does not register takes none. A barrier's continuation is
resolved in the finalize that declares it: each of its queue, executor class, and priority takes the
`Barrier`'s own value, then the continuation kind's default, then the spawning job's, and its attempts,
timeout, and tags take the kind's default. All of it is stored with the barrier, so a child's finalize, a
cancel, and the sweep's discard enqueue the same continuation. A continuation kind without `Defaults`
inherits the spawning job's routing, with 25 attempts, no timeout, and no tags.

**A timeout from `Defaults` is stored on the job**, like `InsertOpts.Timeout`, so it overrides the
worker's `Timeouter`. Since a zero field means "unset", a call cannot ask for the any-class wildcard on
a kind whose `Defaults` name an `ExecutorClass`, or for no tags on a kind whose `Defaults` name some.
The kind's tags meet the same limits as `InsertOpts.Tags` (see **Tags, metadata, and who enqueued a
job**), checked at each insert, so a tag the limits refuse fails every insert of the kind.

</details>

<details>
<summary><strong><code>Leases, the heartbeat, and the fence</code></strong></summary>
<br>

A claimed job carries a **lease**. If its executor dies, the lease expires and the scheduler's sweep
returns the job to the queue for another runner — which is what makes a crashed process recoverable
rather than a job lost forever. A crash spends the attempt it interrupted, so when that attempt was the
job's last (`attempt >= max_attempts`) the sweep discards the job instead: a job whose worker takes its
process down on every attempt ends after `max_attempts` crashes rather than looping. To bound crashes on
their own, set `DriverOpts.MaxCrashes`: the sweep then also discards a job on its `MaxCrashes`-th crash,
counted over the job's life, however many attempts it has left — so a kind with a generous retry budget
for ordinary errors still can't take a process down more than a few times. The sweep reports each job it
reclaimed in `SweepEvent.Jobs`, the slog observer logs each crashed attempt as it logs any failed one,
and `MetricsObserver` counts the jobs each pass discarded in `flywheel_sweep_discarded_total`.

**`LeaseDuration` bounds dispatch liveness, not run duration.** It is how long a *crashed* executor's
job stays stranded before it is reclaimed, not a ceiling on how long a worker may take. Size it to how
quickly you want a crash noticed — the default is 30 seconds. `DefaultTimeout` is what bounds a hung
run.

That separation holds because a running job's lease is **renewed automatically** while its worker is
alive, on a background goroutine, with no worker code:

```go
flywheel.RunnerConfig{
    LeaseDuration: 30 * time.Second, // how fast a crash is noticed
    DefaultTimeout: 10 * time.Minute, // how long a single attempt may run

    // Optional. Zero renews at LeaseDuration/3 — two renewals may fail before
    // the lease can expire. Negative disables renewal entirely.
    HeartbeatInterval: 0,

    // Optional. Fires after each successful renewal, so a host holding its own
    // time-bounded resource for the attempt — an external reservation, a
    // distributed lock — can extend it on the same cadence.
    OnLeaseRenewed: func(ctx context.Context, r flywheel.LeaseRenewal) error {
        return reservations.ExtendTo(ctx, r.JobID, r.ExpiresAt)
    },
}
```

Renewal stops when the attempt ends — normal return, panic, or execution timeout alike — and it stops
if the claim is lost.

**The fence is what makes renewal safe.** Renewal can still fail: a network blip, a paused process, a
GC pause longer than the lease. So every claim also stamps a token (`jobs.lease_token`), and both
`Finalize` and renewal require it. An attempt whose job was reclaimed, cancelled, or retried
underneath it therefore **advances nothing**: no state change, no follow-ups, no lease extension. Its
`job_runs` row is still written, because the attempt did happen — what is discarded is its effect on
the job, not the record of the work.

When that happens the runtime says so, loudly and exactly once:

- `Observer.OnSupersede` fires **in place of** `OnFinish`, carrying the outcome that was discarded.
- `observers.NewSlog` logs it at **warn**, as it does a failed attempt, rather than at the debug level of
  routine events.
- `observers.NewMetrics` counts `flywheel_jobs_superseded_total`.

A nonzero supersede rate means work is being executed twice: the lease is too short for the workload,
or the heartbeat is disabled or failing.

**What this does not give you.** The fence closes the *library's* double-dispatch window. It cannot
make a non-idempotent external call safe: a worker killed after its side effect and before its
finalize still re-runs. Write workers that tolerate a re-run — the fence guarantees only one attempt's
outcome is ever recorded, not that only one attempt ever executes.

</details>

<details>
<summary><strong><code>Cancelling a running job</code></strong></summary>
<br>

`CancelJob` (and `flywheel jobs cancel`) moves a job to `cancelled` at once. A job that was waiting never
runs. A job whose attempt is **running** is fenced: the cancel takes its claim, so the attempt's result is
discarded when its worker returns, and the job is never retried or re-claimed. Whether the worker itself
stops is the runner's choice.

**By default the worker runs on.** The heartbeat notices the lost claim within one interval and stops
renewing, and the worker keeps going until it returns or reaches its timeout. Interrupting a worker
mid-side-effect is a failure mode of its own, and the fence discards its result either way, so this is the
right default for a worker that cannot stop cleanly mid-step.

**With `RunnerConfig.CooperativeCancel`, the worker is asked to stop.** The renewal that finds the claim gone
reads the job's state, one primary-key read, and when the job is `cancelled` it cancels the attempt's context
with the cause `flywheel.ErrJobCancelled`, within one heartbeat of the cancel:

```go
flywheel.RunnerConfig{
    // ...
    CooperativeCancel: true, // needs the heartbeat: a negative HeartbeatInterval is refused
}

func (w ExportWorker) Work(ctx context.Context, job *flywheel.Job[ExportArgs]) (flywheel.Result, error) {
    for page := range w.pages(job.Args) {
        if err := ctx.Err(); err != nil {
            if errors.Is(context.Cause(ctx), flywheel.ErrJobCancelled) {
                w.discardPartialExport(ctx, job.Args) // the job was cancelled: clean up and stop
            }
            return flywheel.Result{}, err
        }
        if err := w.write(ctx, page); err != nil {
            return flywheel.Result{}, err
        }
    }
    return flywheel.Result{}, nil
}
```

Only a cancel stops a worker. A claim lost any other way (the lease sweep's reclaim or crash discard, a claim
under another token, a retry with `RetryOpts.AllowRunning`) leaves it running, as it does without the
setting. A state read that fails is retried at the next heartbeat; renewal never resumes. The read goes
through `RunnerConfig.DB`, so point that at the primary: a lagging replica can read the job as still running
and miss the cancel.

**What the attempt records.** A worker that returns an error after the cancel, `ctx.Err()` or any other, is
recorded as the cancel it was: outcome `cancelled`, `superseded`, its error wrapped in `ErrJobCancelled`
(`jobs: job cancelled while running: context canceled`), and no error class, so it never counts as a failure.
A worker that finishes anyway keeps its `success`, also superseded. Either way nothing is applied to the job,
which is already terminal: no follow-ups, and a parent's barrier fires at the cancel, never again at the late
finalize.

**What it cannot stop.** A worker that ignores its context runs on, exactly as without the setting; the cancel
reaches a worker only through `ctx`. Its latency is the heartbeat interval. A `Driver` that issues no lease
token runs no heartbeat, so under one nothing is cancelled.

**Seeing a cancel that is still waiting.** From the cancel until the attempt returns, the job reads
`cancelled` while its run at the job's attempt still reads outcome `started`:

```go
job, err := flywheel.FindJob(ctx, db, id)
// ...
run, ok, err := flywheel.LatestRun(ctx, db, id) // ListRuns reads the same attempt first
// ...
waiting := job.State == string(flywheel.StateCancelled) &&
    ok && run.Attempt == job.Attempt && run.Outcome == string(flywheel.OutcomeStarted)
```

Once the attempt returns, its run is finished and `superseded`. A run that stays `started` long after the
cancel belongs to a worker that ignores its context, or to a process that died before its attempt returned:
the sweep reclaims only running jobs, so nothing will finish that run. The
[runbook](docs/RUNBOOK.md#symptom-a-cancelled-job-is-still-running) covers both.

</details>

<details>
<summary><strong><code>Recording what an attempt did</code></strong></summary>
<br>

**You do not need your own job-lifecycle table.** Every attempt already gets a row in `job_runs`, and
that row has a stable id you can hang a foreign key off. Two fields are the whole seam:

- **`Job.RunID`** — the `job_runs.id` for *this* attempt, handed to your worker before its body runs.
- **`Result.Output`** — your structured record of what the attempt did, stored on `job_runs.output`
  and read back by `flywheel.ListRuns`.

```go
func (w EnrichWorker) Work(ctx context.Context, job *flywheel.Job[EnrichArgs]) (flywheel.Result, error) {
    fetch, err := w.fetchFromProvider(ctx, job.Args.PersonID)
    if err != nil {
        return flywheel.Result{}, err // retried with backoff; the failure is recorded on this run
    }

    // A side-effect row correlated to the attempt that produced it. RunID is safe
    // to reference: the run row is committed before your worker body starts.
    w.db.Create(&SourceFetch{
        ID:       fetch.ID,
        Provider: fetch.Provider,
        JobRunID: &job.RunID, // FK → job_runs.id
    })

    // What this attempt did, on the attempt's own audit row.
    return flywheel.Result{
        Output:     map[string]any{"source_fetch_id": fetch.ID, "records": len(fetch.Records)},
        CostMicros: fetch.CostMicros,
    }, nil
}
```

Read it back — no join table, no lifecycle mirror:

```go
runs, err := flywheel.ListRuns(ctx, db, jobID, flywheel.ListRunsParams{Limit: 20})
for _, run := range runs {
    fmt.Println(run.Outcome, run.StartedAt, run.FinishedAt, string(run.Output))
}
```

**What a run keeps is bounded.** Every read of a job's runs returns each row whole, so what one attempt
can store is capped, and a cut record says so:

- **Output.** `Result.Output` is stored as JSON up to the driver's `DriverOpts.MaxOutputBytes` (64 KiB
  by default). A larger output is not stored: the attempt finalizes as it would have, and its row holds a
  `TruncatedOutput` marker instead, `{"$truncated": true, "$bytes": …, "$head": "…"}`, with the output's
  marshaled size and its first 1,024 bytes. `JobRunView.OutputTruncated` reports it. Keep a large result
  in a table of your own and return its id. The bundled command workers capture up to 64 KiB of each
  stream, so an `ExecOutput` with both streams near that cap is over the default: raise the driver's cap
  if you keep full captures.
- **Errors.** The error message is stored up to 4,096 bytes. A cut message's `error_payload` carries
  `"truncated": true` and `"message_bytes"`, its length before the cut, and `JobRunView.ErrorTruncated`
  is true. `JobRunView.ErrorPayload` returns the stored `error_payload` as it is, so you can show the
  whole payload; it is nil when the run stored none.
- **Panics.** A worker that panics fails its attempt with `jobs: worker panicked: <value>`, and the run's
  `error_payload` keeps the goroutine's stack at the panic (`"stack"`, up to 16 KiB), which
  `JobRunView.ErrorStack` reports.

**The lifetime of `job_runs.id`, so you can decide your FK's `ON DELETE`:**

| Moment | What happens to the row |
|---|---|
| Before your worker body runs | The row is **committed** with outcome `started`. A side-effect row written during the attempt can reference it immediately, and it survives a rollback of your own work. |
| Attempt finishes | The same row is updated in place with the outcome, timings, cost, and `Result.Output`. |
| Process crashes mid-attempt | The lease sweep marks the row `crashed`, with the state it left the job in (`available`, or `discarded` when that was the job's last attempt or its `MaxCrashes`-th crash). The sweep crashes only the run of the attempt it reclaimed: an older attempt that a retry with `AllowRunning` left running keeps its row `started` until it returns. It is **not deleted**, so your FK never dangles. |
| Retention runs | The only thing that removes it. `DeleteFinishedJobs` deletes `job_runs` **before** `jobs`, so a host FK onto `job_runs.id` wants **`ON DELETE SET NULL`** — or leave retention disabled. |

Need run history that no worker produced — fixtures, a backfill, an import? `flywheel.SeedRun` writes a
`job_runs` row directly, honoring your transaction, and returns the same stable id:

```go
runID, err := flywheel.SeedRun(ctx, db, flywheel.RunSeed{
    JobID:      jobID,
    Attempt:    1,
    ExecutorID: "backfill",
    Outcome:    flywheel.OutcomeSuccess,
    Output:     map[string]any{"records": 42},
})
```

> **The anti-pattern this replaces.** A second table with its own `status`, `started_at`,
> `completed_at`, `error`, and `stats` columns — and no join key back to the run that produced them. It
> duplicates most of the `jobs` row, it drifts from the real outcome the moment a retry happens, and
> nothing can answer "which attempt wrote this?". If you have domain columns to store, keep the table
> and give it a `job_run_id` — the lifecycle columns belong to `job_runs`.

</details>

<details>
<summary><strong><code>Waiting on a job — enqueue or join, then wait</code></strong></summary>
<br>

**Enqueue returns at once; `WaitForJob` is how a caller that needs the outcome gets it.** An operator
command that should not report success until the job ran, a deploy script waiting on a backfill, a
request that joins a rebuild already in flight — each enqueues (or joins), then waits:

```go
id, err := flywheel.Insert(ctx, client, RebuildCache{RepoID: repoID}, flywheel.InsertOpts{
    UniqueActiveKey: "rebuild:" + repoID,
})
var dup *flywheel.AlreadyEnqueuedError
if errors.As(err, &dup) && dup.ExistingID != "" {
    id, err = dup.ExistingID, nil // a rebuild is already in flight: join it
}
if err != nil {
    return err
}

ctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
defer cancel()
job, err := flywheel.WaitForJob(ctx, db, id) // blocks until succeeded, cancelled, or discarded
if err != nil {
    return err // e.g. `flywheel: wait for job "…": still running: context deadline exceeded`
}
run, _, err := flywheel.LatestRun(ctx, db, id) // the final attempt: Outcome, Error, Output
```

- **It polls, cheaply.** The first read is immediate; later reads back off from 500 ms to a 5 s ceiling,
  each one primary-key read. `WaitForJobWithOptions` takes a `WaitOpts` to change the cadence.
- **`WaitOpts.States` ends the wait early** on a live state — `StateRunning`, to return once a runner has
  claimed the job. A terminal state always ends it.
- **`ctx` is the only bound.** When it ends first, the call returns the last view it read and an error
  wrapping `ctx.Err()` that names the state the job was still in. Without a deadline, a paused job is
  waited on forever.
- **A job that is gone returns `ErrJobNotFound` at once**, rather than being polled until the deadline.
  A job enqueued on `InsertOpts.Tx` is invisible until your transaction commits, so wait after the
  commit — and wait on the primary, not a replica: a lagging replica reads a job enqueued or named a
  moment ago as missing.
- **`LatestRun`** reads a job's newest attempt — once the job succeeded, the attempt that did, with the
  worker's `Result.Output`.

Callers that enqueue-or-join the same `UniqueActiveKey` while a job holds it resolve to that job and see
its outcome, so the work runs once for all of them. The [cookbook](docs/COOKBOOK.md) has the full recipe,
including the rare collision whose holder finished before it could be named.

</details>

<details>
<summary><strong><code>Tags, metadata, and who enqueued a job</code></strong></summary>
<br>

**A job carries tags and metadata from the producer that enqueued it.** Set them on `InsertOpts`, or on
a `FollowUp` a worker returns:

```go
_, err := flywheel.Insert(ctx, client, Invoice{ID: id}, flywheel.InsertOpts{
    Tags:     []string{"billing", "eu"},
    Metadata: map[string]any{"tenant": tenantID},
})
```

- **Tags** are stored in order with duplicates dropped: at most `MaxTags` (16), each 1 to `MaxTagBytes`
  (64) bytes of valid UTF-8 with no control characters.
- **Metadata** is one JSON object: the request id the job was enqueued under (`InsertOpts.RequestID`, else
  the context's), then your keys, then an insert hook's, which win. Neither yours nor the hook's can
  replace `request_id`. The merged object is at most `MaxMetadataBytes` (8 KiB).
- **A refusal writes nothing.** A bad tag or an oversized metadata object fails the insert with a
  `*ValidationError` naming `tags` or `metadata`. `InsertMany` checks every item before its first chunk
  and names the item; a follow-up's refusal fails its finalize, as a follow-up that cannot be built does.

**One hook stamps every insert.** Passing the same metadata at every call site misses the jobs no call
site creates: follow-ups, periodic fires, barrier continuations. An `InsertHook` on the Client covers them:

```go
client := flywheel.NewClientWithOptions(db, flywheel.ClientOpts{
    InsertHook: func(ctx context.Context, in flywheel.InsertInfo) map[string]any {
        return map[string]any{"enqueued_by": callerFrom(ctx), "via": string(in.Source)}
    },
})
// Hand the same Client to RunnerConfig.Client and SchedulerConfig.Client.
```

It is called once per row, before the row is built, with an `InsertInfo`: the kind, the queue the job
lands on, the `InsertSource`, the job that spawned the insert, the parent, and the periodic schedule.

| Insert | `InsertSource` |
|---|---|
| `Insert`, `Enqueue`, `InsertMany`, `InsertManyTyped` through the Client | `client` |
| A Scheduler's periodic fire, with `SchedulerConfig.Client` | `periodic` |
| A worker's `Result.FollowUps`, with `RunnerConfig.Client` | `follow_up`, spawned by the finishing job |
| A barrier continuation a finalize fires, with `RunnerConfig.Client` | `barrier`, spawned by the finishing child |
| A barrier continuation the lease sweep fires, with `SchedulerConfig.Client` | `barrier`, spawned by the last child it discarded |
| A continuation `CancelJob` or `CancelByParent` fires, even from inside a worker | no hook: a control holds no Client |

The Runner and the Scheduler carry the Client on the context they pass to `Driver.Finalize` and
`Driver.Sweep`, never on a worker's, so a `Driver` that wraps a built-in one must pass that context
through.

**Keep the hook small and quick.** It may run inside the transaction of a finalize or a sweep batch, so
it must not block. It cannot refuse an insert, but its keys count toward the 8 KiB limit: a result that
pushes the metadata past it, or that JSON cannot encode, fails the insert, and inside a finalize or a
sweep batch it fails that transaction. `GetJob` reads a job back in full, its metadata included. The
[cookbook](docs/COOKBOOK.md#7-record-who-enqueued-a-job--an-insert-hook-tags-and-metadata) has the full
recipe.

</details>

<details>
<summary><strong><code>Finding jobs — a filtered query, capped counts, and opt-in indexes</code></strong></summary>
<br>

**`QueryJobs` lists the jobs a filter matches, a page at a time.** Fields combine with AND, and a field's
values with OR:

```go
page, err := flywheel.QueryJobs(ctx, db, flywheel.JobQuery{
    States:      []flywheel.JobState{flywheel.StateDiscarded, flywheel.StateRetryable},
    Kinds:       []string{"send_invoice"},
    Tags:        []string{"eu"},
    CreatedFrom: time.Now().Add(-24 * time.Hour),
})
for _, job := range page.Items { // each a JobView, its metadata, and its latest run in summary
    fmt.Println(job.ID, job.Kind, job.State)
}
// page.NextCursor, when set, reads the next page: JobQuery{…, Cursor: page.NextCursor}
```

- **Filters:** states, kinds, queues, tags (any of them), ids, a parent, priority and attempt ranges, and
  created, scheduled, and finalized windows. The created window is applied to the UUIDv7 id, so the
  primary key serves it. On PostgreSQL, `ArgsContains` and `MetadataContains` find jobs whose args or
  metadata contain a JSON object, within a created window of at most seven days.
- **Filters on the latest attempt:** `ErrorClasses`, `Fingerprint`, and `ErrorContains` match a job by
  the run of the attempt it names, so an error group opens as the list of its jobs and an earlier
  attempt's failure never matches. `ErrorContains` is a case-insensitive text search, on PostgreSQL,
  within a created window of at most seven days. Each probes one index per candidate, so pair them with a
  created window, a kind, or a state.
- **Sorts:** `JobSortNewest` (the default), `JobSortOldest`, `JobSortFinished` (terminal states, most
  recently finished first), and `JobSortScheduled` (waiting states, next to become claimable first). The
  id breaks every tie, so the opaque `jq1.` cursor never skips or repeats a row between pages.
- **Bounds:** a page holds 50 jobs by default and never more than 200, and no list loads args. A query
  that cannot run — an unknown state, an inverted window, the finished sort over a waiting state, a
  cursor from another sort — is refused with a `*ValidationError` naming its field before any statement
  runs.

**`CountJobs` counts honestly.** It is exact up to 10,000 (`CountOpts.Cap`, at most 100,000), then
returns the cap with `Capped` set and, on PostgreSQL, the planner's `Estimate`. It never reads past the
cap, so a count over a large table costs what one over the cap does: show `10,000+` rather than a number
the read could not afford.

**Explorer indexes are yours to choose.** A filter whose matches are common among the newest jobs is
served by the primary key. One whose matches are rare there walks far without an index, and
`ExplorerIndexSet` holds four for those shapes: by kind, by queue, and by state, each with the id, and a
GIN index on tags (PostgreSQL). They are never part of `IndexSet`, `Migrate`, or a plain
`InstallIndexes`, because every index on `jobs` costs a write on every state transition. Install the
ones your lists need through `MigrateOpts.ExtraIndexes` or `IndexOpts.ExtraIndexes`, or copy their DDL
into your own migrations, and check them in CI with `InspectIndexesWithOptions`:

```go
set, err := flywheel.ExplorerIndexSet(db.Name()) // kind, queue, state, tags
err = flywheel.MigrateWithOptions(db, flywheel.MigrateOpts{ExtraIndexes: set[:2]}) // kind and queue
```

[`docs/INTEGRATING.md`](docs/INTEGRATING.md#finding-jobs) has every field and refusal, and
[`docs/BENCHMARKS.md`](docs/BENCHMARKS.md#the-job-query-and-its-opt-in-indexes) each shape's cost at a
million jobs, with and without its index, and each index's write cost. `flywheel jobs ls` and
`flywheel jobs count` run the same query from a terminal.

</details>

<details>
<summary><strong><code>Bulk enqueue and the follow-up ceiling</code></strong></summary>
<br>

Enqueuing one job is one `Insert`/`Enqueue` call and one row. Enqueuing many that way is one round trip
per job — a 100k fan-out is 100k statements. `InsertMany` writes them in bounded, dialect-aware chunks
instead: one multi-row `INSERT` per chunk, so the round trips drop by the chunk factor.

```go
items := make([]flywheel.BatchItem, len(subjects))
for i, subject := range subjects {
    args, err := json.Marshal(reportArgs{Subject: subject})
    if err != nil {
        return err
    }
    items[i] = flywheel.BatchItem{
        Kind: "report",
        Args: args,
        Opts: flywheel.InsertOpts{Queue: "reports", UniqueKey: subject}, // per-row options
    }
}

res, err := flywheel.InsertMany(ctx, client, items, flywheel.BatchOpts{})
// res.Inserted + res.Skipped == len(items).
// res.IDs stays aligned to the input — empty at any row a unique-key collision skipped.
// res.ExistingIDs is aligned too: at a skipped row, the id of the job holding its key.
```

Everything `Enqueue` guarantees for one row holds per row here: the same defaults, the same
`unique_key`/`unique_active_key` idempotency, the same `ErrAlreadyEnqueued` meaning — a collision is a
per-row skip, never a failed batch. `res.Skipped` counts the collisions and `res.ExistingIDs` names the
job holding each skipped row's key (an earlier job, or a sibling earlier in the batch), at the cost of
one read per chunk that skipped a row. Set `BatchOpts.SkipDuplicates` to drop collisions silently
instead, which skips that read. `InsertManyTyped[A]` is the generic form: it reads each job's kind from
the args value, exactly as `Insert` does.

**The outbox guarantee carries over.** Set `BatchOpts.Tx` and every chunk writes on your transaction —
the batch opens none of its own, so the rows land with your domain writes or not at all. Leave it unset
and each chunk commits independently, so a mid-batch failure leaves the earlier chunks committed and the
returned error names the failing chunk.

`ChunkSize` defaults to a value chosen so a chunk stays well inside the driver's bind-parameter ceiling;
a request above the dialect maximum is clamped down, not rejected.

| Dialect | Bind-parameter ceiling | Rows per statement | Default `ChunkSize` |
|---|---|---|---|
| PostgreSQL | 65,535 | 2,730 | **1,000** |
| SQLite | 32,766 | 1,365 | **500** |

**The follow-up fan-out is bounded the same way.** A worker's `Result.FollowUps` is enqueued inside the
finalize transaction, so an unbounded fan-out is an unbounded, lock-holding transaction. The children
are inserted in chunks, and the total is capped: a fan-out past the limit fails the finalize with
`ErrFollowUpLimit` rather than silently truncating. The bounds are `DriverOpts.FollowUpChunkSize`
(default 500) and `DriverOpts.FollowUpLimit` (default 10,000) — a fan-out that large is a signal to
spawn a coordinator job rather than return N children from one attempt.

</details>

<details>
<summary><strong><code>Batch progress, controls, and the fan-in barrier</code></strong></summary>
<br>

A worker spawns children by returning `FollowUp{Parent: true}`, and the enqueuing caller sets a child's
parent with `InsertOpts.Parent`. That lineage pointer is what the batch surface reads.

**The rollup answers "how is this batch doing?" from one call.** `Progress` returns a `BatchProgress`:
the per-state child counts, the totals, the parent's own state, and the age of the oldest pending child
— served by a covering index, so it holds at a hundred thousand children.

```go
p, err := flywheel.Progress(ctx, db, parentID)
// p.CountsByState["succeeded"], p.Total, p.Terminal, p.Pending
// p.Pending == 0 means the batch is complete.
```

An unknown or already-pruned parent is a zero rollup with an empty `ParentState`, not an error: children
outlive the parent row, so `Progress` reports them either way. `ProgressMany` rolls up several parents in
a fixed two reads rather than one per parent — a childless or unknown parent still gets an entry, so a
dashboard tells "no children" from "unknown id". `ProgressByKind` is the parentless form, for a host
whose batches are defined by what the work is rather than by who spawned it.

**`paused` is a real, operator-visible state, not a `scheduled_at` trick.** A far-future `scheduled_at`
would be free, but it is indistinguishable from a legitimately deferred job and invisible to every
rollup and health sample — the exact class of "a condition encoded as a side effect of another field"
the runtime avoids. `paused` is the one state that is neither claimable nor terminal: a runner never
picks it up, and it stays held until a resume returns it to `available`. It is unfinished work, so it
keeps `RunUntilIdle` polling and stays counted as non-terminal.

**Pause, resume, and cancel scope to a whole generation.** Each operates in bounded, per-transaction
batches — the lease sweep's shape — and reports what it did by reason, so a running attempt is never
interrupted and a succeeded child is never clobbered:

```go
res, err := flywheel.CancelByParent(ctx, db, parentID, flywheel.ScopeOpts{})
// res.Changed cancelled; res.SkippedRunning left in flight; res.SkippedTerminal left as they were.
```

`PauseByParent` holds every claimable child; `ResumeByParent` returns the paused ones to `available`,
preserving a deferred child's schedule. The per-id `CancelJob` and `RetryJob` are guarded to the same
standard: a job that has already reached a terminal state is left exactly as it is and `ErrJobTerminal`
is returned, so an operator action can never overwrite a recorded outcome. Re-running a finished job is
deliberate — `RetryJobWithOptions` with `Force` — and replaying a whole cohort of failures, with a
restored retry budget, is its own guide: **Replaying work** below. Moving, reprioritizing, holding,
releasing, or deleting one job is **Changing one job** below. Each of these controls records who ran it,
and why, in the history of every job it changes: **Who changed a job, and why** below.

**A retry never starts a job while its attempt still runs.** `RetryJob` refuses a running job with
`ErrJobRunning` and leaves it and its lease alone: re-queueing it would let a runner start a second
attempt beside the first, and the job's side effects would happen twice at once. `Force` re-runs
terminal jobs only; it also refuses a job cancelled while its attempt runs, because a cancel fences that
attempt without awaiting its worker. Wait for the attempt to finish, or pass
`RetryOpts{AllowRunning: true}` to re-queue it anyway: the running attempt keeps going, its result is
discarded, and the work runs twice. That is also the way past a cancelled job whose process died before
its attempt returned, whose run stays `started`. If the re-queued job's new attempt then crashes, the
sweep crashes only the run of the attempt it reclaimed: the older attempt keeps its run `started` until
it returns, so the crash is counted once.

**The fan-in barrier runs a continuation once a generation completes.** A worker returns `Result.Barrier`
alongside its children, and the runtime enqueues the continuation exactly once, after the last child
reaches *any* terminal state — a half-failed generation still gets its finalizer. It is keyed so a
retried, discarded, or superseded child can never enqueue it twice, and it fires in the transaction
that makes the last pending child terminal: its finalize, a cancel, or the sweep's discard, so no host
writes a retry-unsafe counter. `CancelByParent` over a generation's last pending children therefore
runs the continuation, and the call ends with the batch that fired it, so it never cancels the
continuation it just enqueued. A cancelled running child counts as terminal at the cancel, so the
continuation can start while that child's worker is still running.

```go
return flywheel.Result{
    FollowUps: children,                                  // each with Parent: true
    Barrier:   &flywheel.Barrier{Kind: "finalize_batch"}, // runs once, when they are all terminal
}, nil
```

The continuation's routing is resolved when the barrier is declared and stored with it: an empty
`Queue`, `ExecutorClass`, or `Priority` on the `Barrier` takes the continuation kind's `Defaults` in the
Runner's registry, then the spawning job's, so every path that fires the barrier enqueues the same row
(see **Worker defaults** above).

The continuation reads what the generation produced through `ChildOutputs` — one entry per terminal
child, its final state paired with its last attempt's recorded output. A barrier-bearing generation is
bounded by `DriverOpts.BarrierMaxChildren` (default 10,000): the barrier costs an index-only completion
count per child finalize, so a wider fan-out is refused with `ErrBarrierTooWide`, directing you to a tree
of bounded generations rather than one that costs `O(children²)`.

**No barrier timeout ships, by design — anchor your own deadline to progress.** A deadline measured from
when a batch was *spawned* kills a healthy batch that is merely behind a deep backlog, which is precisely
when a batch most needs to survive. `BatchProgress.OldestPendingAge` is the signal instead: it measures
from the least-recently-scheduled *pending* child, so a host that wants a deadline anchors it to how long
real work has actually been stalled, not to wall-clock age.

</details>

<details>
<summary><strong><code>Fairness across parents — priority banding</code></strong></summary>
<br>

**The claim is `ORDER BY priority, scheduled_at`, so equal-priority work is strict FIFO.** A lower
priority number is claimed first; ties break by schedule time. That is the right default — it is one
index seek to the front of the queue, `O(batch)` regardless of how deep the backlog is — but it means a
large batch enqueued first fully drains before a small batch enqueued a minute later gets a single
claim. When the two batches belong to different parents (or tenants, or customers), that is
head-of-line blocking, and bulk enqueue makes the starving batch cheap to create.

**Fairness is expressed through the priority column, at enqueue time — not by reordering the claim.** A
runtime policy that interleaved parents in the claim would rank the whole ready set with a window
function before applying the `LIMIT`, which is `O(ready)`: measured against a 166,667-row ready set at
one million rows, that claim runs in **216 ms** where the shipped claim runs in **0.06 ms**, and a
priority-band pre-filter still costs 102 ms because computing the band scans the ready set too (see
[BENCHMARKS](docs/BENCHMARKS.md)). A runner claims on every poll, so paying that on every claim against
a deep queue is not an option. So the ranking is moved to enqueue time, where it is paid once per job
and the claim stays `O(batch)`: give each parent's *n*-th child the same priority band, so the claim's
existing `(priority, scheduled_at)` order interleaves the parents for free.

```go
// Round-robin two parents by banding their children's priorities. Child i of every
// parent shares band base+i, so the claim takes one child from each parent per band
// instead of draining parent A before parent B is seen.
for i, item := range parentAChildren {
    item.Opts.Priority = priorityBase + i
}
for i, item := range parentBChildren {
    item.Opts.Priority = priorityBase + i
}
```

**Priority still dominates.** Banding interleaves work *of equal urgency*; it does not let a low-priority
parent jump ahead of a high-priority one. Reserve a range of priority numbers for the band offset (say,
`base + (i % window)`) so a genuinely more urgent job at a lower base is still claimed first, and the
banding only decides the order *within* a base.

**A parentless job is its own group of one.** Fairness is keyed on the parent, so a job with no parent
does not share a band with anyone — id 1000 and id 1001, both parentless and both enqueued at the
default priority, are claimed in schedule order exactly as they are today. Banding changes nothing for
work that was never part of a batch; it is a tool for the case where one lineage would otherwise starve
another.

</details>

<details>
<summary><strong><code>Replaying work — retry vs. re-enqueue</code></strong></summary>
<br>

**Replaying the failures is the common recovery after an incident, and it is a retry, not a re-enqueue.**
`ReplayByParent` returns the failed children of a parent to `available` in bounded, per-transaction
batches; `Replay` does the same for a whole kind or an incident window, unscoped by lineage. Both report
what they did through the same `ScopeResult` the batch controls use:

```go
res, err := flywheel.ReplayByParent(ctx, db, parentID, flywheel.ReplayOpts{
    RetryOpts: flywheel.RetryOpts{ResetAttempts: true}, // restore the retry budget
    Stagger:   5 * time.Minute,                         // spread arrivals over five minutes
})
// res.Changed replayed; res.SkippedTerminal left as they were; res.SkippedRunning left in flight;
// res.SkippedActiveKey left terminal because their UniqueActiveKey was already held.
```

Each replayed job gets an entry in its history, with the options the replay used and who ran it: see
**Who changed a job, and why** below.

**A replay restores the retry budget as headroom, not by rewinding the counter.** A job discarded at
`attempt == max_attempts` has no budget left, so a plain retry gives it exactly one more attempt before
it discards again. `ResetAttempts` raises `max_attempts` — to `attempt + Budget`, or to
`attempt + max_attempts` when `Budget` is zero — so the job gets a real second life. With `Budget` zero
the grant is the job's current `max_attempts`, which is its original budget only if it never snoozed:
every snooze raised it by one. Set `Budget` to grant an exact number. `attempt` is never lowered: it is
the `job_runs(job_id, attempt)` audit key, so the replay's runs continue the sequence rather than
colliding with the recorded failures. It is the same mechanism a snooze uses to stay free.

**A replay is bounded by construction.** Empty `States` replays discarded jobs only — never succeeded
work; naming `StateSucceeded` additionally requires `Force`. An unscoped `Replay` with neither `Kinds`
nor `FailedSince` is refused with `ErrReplayUnbounded` rather than replaying every discarded job in the
database by accident.

**A replay never touches a job whose attempt still runs.** A `States` that names `running` is refused
before any write with a `*ValidationError` on `states`, whatever `AllowRunning` says. With `States`
naming `cancelled`, a job cancelled while its attempt still runs is left cancelled and counted in
`SkippedRunning`, so the replay never starts a second attempt beside the first.

**A replay never puts two live jobs on one `UniqueActiveKey` at a time.** A replayed job whose key
another job holds live when its batch runs stays terminal, is counted in `SkippedActiveKey`, and the
replay carries on past it. Of the jobs in one batch that share a key, the first in id order goes live; a
job in a later batch is skipped while that job is live, and goes live itself if that job has already
finished. With `States` naming a live state, a targeted job that is already live keeps its key. To run a
skipped job, replay or retry it once the job holding its key has finished. With no `FailedSince` window,
`Changed + SkippedTerminal + SkippedRunning + SkippedActiveKey` accounts for every job in scope that was
finished or running when the replay began, unless another operation changed one of them mid-replay.

```go
// The incident-shaped recovery: one kind, bounded to the outage window, budget of three.
res, err := flywheel.Replay(ctx, db, flywheel.ReplayOpts{
    RetryOpts:   flywheel.RetryOpts{ResetAttempts: true, Budget: 3},
    Kinds:       []string{"fetch_report"},
    FailedSince: outageStart,
})
```

**`Stagger` shapes when the cohort arrives; it is not a rate ceiling.** With `Stagger` set, job *i* of
*n* becomes claimable at `now + Stagger*i/n`, so 30,000 replayed jobs do not all hit a just-recovered
dependency at once. The placement is deterministic, so you can predict when the last job lands, and a
job left terminal for its key keeps its slot empty rather than moving the rest. It does not cap how fast
the cohort is claimed once each job is due — an actual claim-rate ceiling is a separate capability.

**Retry an existing row; do not re-enqueue it under the same key.** Which mechanism recovers a unit of
work depends on the intent:

| Intent | Mechanism | Key |
|---|---|---|
| Re-run *this job row*, keeping its id, history, and audit trail | `RetryJobWithOptions` / `Replay*` | — |
| Enqueue a *new* job for the same logical unit, at most once | new `Insert` | `UniqueKey` — collides with the job that bears it, terminal or not, until retention deletes that job |
| Enqueue a *new* job for a subject that may run again later | new `Insert` | `UniqueActiveKey` — frees on a terminal state |

> A job enqueued with `UniqueKey` cannot be re-enqueued while its row exists: the key collides with the
> original row, terminal or not. That is the guarantee `UniqueKey` exists to provide. To run that unit of
> work again, retry the original job — its id, lineage, and audit trail are preserved. If the unit is
> expected to run again later, `UniqueActiveKey` is the key you want.
>
> Retention frees the key: once `DeleteFinishedJobs` (or the scheduler's retention pass) deletes the
> original job, an `Insert` with its key lands and the work runs again. When "at most once, ever" has to
> outlive retention, keep the key where retention cannot reach it — leave retention off or its window
> longer than the key must hold, or record the key in a table of your own under a unique constraint, in
> the same transaction as the enqueue (`InsertOpts.Tx`).

</details>

<details>
<summary><strong><code>Who changed a job, and why — the audit history</code></strong></summary>
<br>

**Every operator control records who ran it, and why.** Put the operator on the request's context once,
with `WithActor`, and each control called with that context writes one entry to `job_audit` for every job
or schedule it changes: `RetryJob`, `RetryJobWithOptions`, `CancelJob`, `PauseByParent`, `ResumeByParent`,
`CancelByParent`, `Replay`, `ReplayByParent`, `SetPeriodicActive`, `DeletePeriodic`, and the single-job
controls of **Changing one job** below.

```go
ctx = flywheel.WithActor(ctx, flywheel.Actor{
    Type:   flywheel.ActorUser,
    ID:     userID,
    Reason: "stuck behind the payments outage",
    Source: "admin-console",
})
err := flywheel.CancelJob(ctx, db, jobID) // one "cancelled" entry, from the state the job was in
```

An entry names its target (a job's id, or a schedule's slug), the action, the actor, and details: the
state the job changed from, and what the call was told to do — a retry's options, whether a cancel fenced
a running attempt, the parent a batched call was scoped to. A control called with no actor is recorded as
`system`, with the context's request id. The runtime requires no reason: whether an action needs one is
your policy, so check it before you call. An actor the history can't record — an unknown `Type`, a
`Reason` over 1,024 bytes, a `Source` over 64 — is refused with a `*ValidationError` before anything is
read or written.

Enqueues write no entry: who enqueued a job is its metadata (see **Tags, metadata, and who enqueued a
job** above). Neither does the lease sweep: its crashes are recorded in `job_runs`.

**The history agrees with the change.** An entry is written in the transaction that makes its change —
each batch's, for the batched calls — so it exists exactly when the change committed, and a caller's
rollback removes both. A control reads the job under a row lock (`FOR UPDATE` on PostgreSQL) and records
the state it found there, so a concurrent change commits first and the entry names the state the job
really changed from. A batch reads back the ids its update changed (`RETURNING id`), so a job a concurrent
call moved, or one a replay left terminal for its key, gets no entry; a GORM handle configured without
`RETURNING` fails the batched controls rather than record less than they changed. A call that changes
nothing writes nothing.

**Your own record, in the same transaction.** `WithAuditHook` runs your function on the control's
transaction after the entries are written, with those entries, so an audit row of your own commits or
rolls back with the change; the hook's error rolls the control back and is returned.

```go
ctx = flywheel.WithAuditHook(ctx, func(ctx context.Context, tx *gorm.DB, entries []flywheel.AuditEntry) error {
    for _, e := range entries {
        row := AppAudit{Target: e.TargetID, Action: string(e.Action), By: e.Actor.ID, Why: e.Actor.Reason}
        if err := tx.Create(&row).Error; err != nil {
            return err
        }
    }
    return nil
})
```

**Reading it.** `ListAudit` reads one target's history newest first, or the whole feed oldest first
(`AuditOldest`), filtered by action, actor, and time, behind an opaque cursor, at most 200 entries a page:

```go
page, err := flywheel.ListAudit(ctx, db, flywheel.ListAuditParams{
    TargetType: flywheel.AuditTargetJob, TargetID: jobID, Limit: 20,
})
// page.Entries newest first; pass page.NextCursor as Cursor for the next page.
```

**Keeping it.** Entries outlive the jobs they name: retention prunes the job and keeps its history, the
record that it existed and of who acted on it. `SchedulerConfig.AuditRetention` bounds the table (see
**Retention and scheduler maintenance** below).

**From the CLI.** `flywheel jobs retry`, `cancel`, `reschedule`, `priority`, `pause`, `resume`, and
`delete`, and `schedule rm`, `enable`, and `disable` take `--reason`, and record the operating-system user with the source `cli`; `flywheel jobs history <job-id>`
prints a job's history.

`Migrate` creates the table. A host-owned schema adds it with the statements in
[Upgrading an existing database](docs/INTEGRATING.md#upgrading-an-existing-database) before deploying,
because every control writes it.

</details>

<details>
<summary><strong><code>Changing one job — reschedule, priority, pause, resume, delete</code></strong></summary>
<br>

**A waiting job can be moved, reprioritized, held, or released in place, and a finished one deleted.**
Each of these controls changes one job by its id and keeps its id, its lineage, and its history, which a
cancel and a fresh enqueue would lose:

```go
ctx = flywheel.WithActor(ctx, flywheel.Actor{Type: flywheel.ActorUser, ID: userID, Reason: "after the vendor window"})

switch err := flywheel.RescheduleJob(ctx, db, jobID, windowCloses); {
case errors.Is(err, flywheel.ErrJobRunning):
    // a runner claimed it first: let it run, or cancel it
case errors.Is(err, flywheel.ErrJobTerminal):
    // it has already finished
case err != nil:
    return err
}
// SetJobPriority(ctx, db, jobID, 10), PauseJob, ResumeJob, and DeleteJob take the same shape.
```

**Moving and reprioritizing.** `RescheduleJob` sets the `scheduled_at` of a waiting job — `available`,
`retryable`, `scheduled`, or `paused` — to the time you give, as `InsertOpts.ScheduleAt` does, and keeps
its state: a time in the past makes an available job due at once, and a paused job stays paused.
`SetJobPriority` gives a waiting job another priority, at least 1; runners claim the lowest first. Setting
the priority a job already has changes nothing.

**Holding and releasing.** `PauseJob` holds an `available`, `retryable`, or `scheduled` job in `paused`,
where no runner claims it, and `ResumeJob` returns a paused job to `available` with its `scheduled_at`
kept, so a job paused while deferred stays deferred. Pausing a paused job, or resuming one that isn't
paused, changes nothing. They are the single-job twins of `PauseByParent` and `ResumeByParent`.

**Deleting.** `DeleteJob` hard-deletes a finished job — `succeeded`, `cancelled`, or `discarded` — with its
runs and their finish-log entries, in the order retention deletes them: the finish-log entries, then the
runs, then the job, so a schema that declares foreign keys between the tables sees one order from both.
The job's history stays, and records the delete. Only the job's own rows go: its children keep their
`parent_job_id`, and a fan-in barrier it declared and had not fired goes with its row, so it never fires.
A job that hasn't finished is refused with `ErrJobNotTerminal`: cancel it first.

**None of them touches a running job.** A running job's row belongs to the attempt in flight until that
attempt finalizes, so each control refuses it with `ErrJobRunning` and leaves the job and its lease as
they were. A finished job is refused with `ErrJobTerminal` (by all but `DeleteJob`), and an id no live job
has with `ErrJobNotFound`. Each reads the job under a row lock (`FOR UPDATE` on PostgreSQL) in a
transaction of its own, a savepoint inside yours, and decides from the state it read, so a job claimed a
moment earlier is refused rather than changed under its attempt.

**Each records who ran it.** A control that changes the job writes one entry to its history in the same
transaction, under the context's actor (see **Who changed a job, and why** above): `rescheduled` with the
old and new time (`from`, `to`, in UTC), `reprioritized` with the old and new priority, `paused`,
`resumed`, or `deleted` with the job's `kind` and `state`, each with the state the job changed from
(`from_state`). A call that changes nothing writes nothing.

**From the CLI.** `flywheel jobs reschedule <job-id> --at <RFC3339>`, `jobs priority <job-id> <priority>`,
`jobs pause <job-id>`, `jobs resume <job-id>`, and `jobs delete <job-id>`, each with `--reason`.

</details>

<details>
<summary><strong><code>Admission control — gating a runner before it claims</code></strong></summary>
<br>

**A `Limiter` is consulted before every claim, so work that cannot run yet is never claimed.** Without
one, the only backpressure is *claim-then-snooze*: a worker is dispatched, discovers the downstream is
at budget, and returns `Result{Snooze}`. A snooze spends no retry attempt, but it does spend a poll, a
claim, a `job_runs` audit row, and a finalize — so under sustained backpressure the queue's dominant
work becomes jobs re-scheduling themselves and `job_runs` grows at the snooze rate. A pre-claim gate
removes all of that: at a 50/s budget it claims 50 jobs a second and writes 50 audit rows a second,
where the same workload under claim-then-snooze wrote 221× the rows (see [BENCHMARKS](docs/BENCHMARKS.md)).

**The gate is keyed on an arbitrary resource string, not on the queue or executor class.** The thing
that needs protecting is a *downstream dependency* — a provider, an API tenant, an external account —
and its budget has nothing to do with which pool runs the job. `Resource` names it:

```go
limiter := flywheel.NewTokenBucket(flywheel.TokenBucketConfig{
    Rate: 50, Interval: time.Second, // 50 operations/second to this downstream
    Burst:         50,               // bucket capacity; defaults to Rate
    MaxConcurrent: 5,                // optional: at most 5 in flight at once
})

flywheel.RunnerConfig{
    Queues:   []string{"provider-x"},
    Limiter:  limiter,
    Resource: "provider:x", // required when Limiter is set
}
```

**A `Runner` is the unit of resource scoping.** The gate runs *before* the claim, so the resource must
be knowable without inspecting a job — which makes it a property of the runner, not the work. A host
protecting several downstreams runs one runner per downstream, each over its own queue, rather than one
runner sorting jobs by destination after claiming them (`NodeConfig.Runners` is a slice, so N gated
runners live in one process). This is the concrete answer to **queue-per-destination**: per-destination
limiting requires per-destination queues, the same conclusion the claim path reaches from the other
direction — a multi-queue claim cannot be indexed, so a queue is best served by its own runner (see
[BENCHMARKS](docs/BENCHMARKS.md)).

**Work that spawns work runs ungated, or on a distinct resource.** A coordinator whose only job is to
dispatch children must not consume the capacity those children need: put it on an ungated runner, or
one whose resource differs from the resource its children consume. Because resource is a runner
property, a coordinator on its own runner already has a different resource by construction — the
deadlock is only reachable by placing coordinators and their children on one gated runner sharing a
resource, and a runner in that shape logs a starvation warning (`LimiterStarvationInterval`).

**Pick the limiter by process count.** `NewTokenBucket` is in-process and correct for one process — a
deployment running N processes against one downstream gets N times the budget. `NewDBLimiter` shares
one budget across every process on the database; it is a round trip per claim, so it suits budgets in
the tens-to-hundreds per second. Its concurrency reservations carry a `HoldTTL` so a crashed holder's
capacity self-heals — set it **above the longest expected job**, since a TTL shorter than the work
reclaims capacity from a healthy holder and over-admits.

```go
limiter, err := flywheel.NewDBLimiter(db, flywheel.DBLimiterConfig{
    MaxConcurrent: 10,
    HoldTTL:       5 * time.Minute, // longer than any job this resource runs
})
// Optional: reclaim expired holds proactively. Acquire also reclaims inline, so
// correctness never depends on the sweeper running.
go limiter.RunSweeper(ctx)
```

**A limiter outage is not a work outage.** When the limiter returns an error the runner claims anyway
and logs, by default. A host guarding a hard external quota sets `LimiterFailClosed` to defer instead.

</details>

<details>
<summary><strong><code>Running one registry across two executors</code></strong></summary>
<br>

One `Registry`, one database, two very different processes: a long-running pool and a bounded one that
must finish inside an invocation budget (Lambda, a Kubernetes Job, a CI step). `ExecutorClass` is what
routes between them — a free-form label, not an enum.

```go
// One registry, built once, compiled into both binaries. Both know every kind;
// the class decides who claims what.
func newRegistry() *flywheel.Registry {
    reg := flywheel.NewRegistry()
    flywheel.Register(reg, ReindexWorker{})
    flywheel.Register(reg, ThumbnailWorker{})
    return reg
}
```

**The long-running process** runs a `Node` on its class, and owns the scheduler:

```go
// One driver, shared by the runner and the scheduler.
driver := flywheel.NewPostgresDriver(db)

node, _ := flywheel.NewNode(flywheel.NodeConfig{
    Runners: []flywheel.RunnerConfig{{
        DB: db, Driver: driver, Registry: newRegistry(),
        Queues: []string{"default", "periodic"}, ExecutorClass: "worker", Concurrency: 4,
    }},
    Scheduler: &flywheel.SchedulerConfig{DB: db, Client: flywheel.NewClient(db), Driver: driver},
})
_ = node.Run(ctx)
```

**The bounded process** builds a bare `Runner` and drains under a deadline that reserves a teardown
margin below its own budget:

```go
runner, _ := flywheel.NewRunner(flywheel.RunnerConfig{
    DB: db, Driver: flywheel.NewPostgresDriver(db), Registry: newRegistry(),
    Queues: []string{"default"}, ExecutorClass: "burst", Concurrency: 4,
})

// Reserve teardown time: a deadline set *at* the platform's limit gets the
// process killed mid-finalize.
budget, cancel := context.WithTimeout(ctx, invocationBudget-teardownMargin)
defer cancel()

switch err := runner.RunUntilIdle(budget); {
case err == nil:
    // Every job reached a terminal state.
case errors.Is(err, context.DeadlineExceeded):
    // Budget spent with work outstanding. Normal for a bounded invocation, not a
    // failure: the leftover jobs stay claimable for the next one.
default:
    return err // a real failure — an outdated schema or a database error
}
```

**What `RunUntilIdle` guarantees.** It returns `nil` **only when no job is in a non-terminal state** —
not merely when this runner found nothing to claim. A job another pool is still running, one this
runner has in flight in its own worker pool, or one waiting out a retry backoff all keep it looping.
That is why the deadline branch is separate: `context.DeadlineExceeded` means "budget spent", not
"something broke". A transient database error does not end it either — it backs off and retries — and
`flywheel.ErrRunnerStopped` is what it returns if `Stop` ended it before the queue drained. A job of a
kind this runner does not register is not an error: it keeps the loop going until `UnknownKindGrace` has
passed since the job's enqueue, and is then discarded (see **Rolling deploys and new job kinds** above).

**Routing rules:**

| The job's class | Who can claim it |
|---|---|
| `flywheel.AnyClass` (the empty default) | any runner, whichever polls first |
| `"burst"` | a runner with `ExecutorClass: "burst"`, or any runner with `ClaimAnyClass: true` |

Leave a job unpinned unless it genuinely needs one pool's hardware, credentials, or budget.

**Deploying the two binaries at different times is a rolling deploy across executors.** A job of a kind
only the newer binary registers can be claimed by the older one, so **Rolling deploys and new job kinds**
above applies, its first-rollout caveat included.

> **The trap: the scheduler must run on exactly one process.** It owns the periodic ticks *and* the
> stuck-lease sweep that reclaims jobs whose executor died. Two schedulers means every tick fires twice
> and two sweeps race. Every process except one leaves `NodeConfig.Scheduler` nil — and a bounded
> process, which by definition is not always running, is never the one that has it.

A complete, runnable version of this is [`examples/split-executors`](examples/split-executors).

</details>

<details>
<summary><strong><code>Observability, health, and metrics</code></strong></summary>
<br>

The runtime is self-diagnosing. The `Observer` seam ([observer.go](internal/core/observer.go)) reports every
attempt's lifecycle — claim, start, finish, retry, supersede — with no metrics dependency in the core,
and the [`observers/`](observers) package ships ready adapters that plug straight in:

- `observers.NewMetrics(rec)` translates events into a `MetricsRecorder` — a small four-method sink you back
  with Prometheus, OpenTelemetry, statsd, or CloudWatch (the core imports none of them).
- `observers.NewSlog(logger)` logs routine events at debug level and failures above it: a failed attempt
  at warn and a job a failure discarded at error, each with the worker's error, the attempt against its
  budget, and the job's resulting state ([RUNBOOK](docs/RUNBOOK.md#symptom-jobs-fail-or-are-discarded)).
  A crashed attempt the lease sweep reclaims logs the same lines, with outcome `crashed`, when the same
  observer is passed to the scheduler.
  `observers.NewMulti(...)` fans events out to several observers at once.

`SampleQueueHealth` ([health.go](internal/core/health.go)) reads a point-in-time gauge snapshot — depth by state
for every state but `succeeded` (most of a retained table, which each sample would otherwise read in full),
ready / in-flight counts, and the **oldest-ready age (lag)**, the canonical "are the runners falling
behind?" signal — and `RecentFailures` lists what was discarded recently and why. Give a `Node` a
metrics handler and its health server also serves Prometheus text at `/metrics` (recorder counters
plus the queue-health gauges, sampled fresh per scrape) alongside `/healthz` and `/readyz`:

```go
mem := observers.NewMemRecorder()
node, _ := flywheel.NewNode(flywheel.NodeConfig{
    Runners: []flywheel.RunnerConfig{{
        DB: db, Driver: flywheel.NewSQLiteDriver(db), Registry: reg,
        Queues: []string{"default"}, ClaimAnyClass: true,
        Observer: observers.NewMulti(observers.NewSlog(logger), observers.NewMetrics(mem)),
    }},
    Health: flywheel.HealthConfig{
        Addr: ":9090",
        MetricsHandler: observers.MetricsHandler(mem, func(ctx context.Context) (flywheel.QueueHealth, error) {
            return flywheel.SampleQueueHealth(ctx, db)
        }),
    },
})
```

The [`flywheel` CLI](cmd/flywheel/README.md) turns all of this on by default and adds `flywheel status`
for an at-a-glance report of queue health, schedules, and recent failures.

**Dashboards over the database.** For history rather than this minute, set the scheduler's
`StatsRollupInterval` and read the runtime's own tables: `ListRunning` (what is running now, slow runs
flagged), `ListFinished`, `QueryJobs` and `CountJobs` (any filter, paged, with an honest count), `GetJob`
(one job in full, args bounded), `QueueDepths` and `QueueDepthHistory` (a queue's depth now and over
time), `Stats` and `StatsSeries` (outcomes, jobs enqueued, success rate, duration and queue-wait
percentiles per kind or per queue over any window, from hourly rollups that outlive retention),
`ErrorGroups` (what is failing, grouped by fingerprint), `Baselines`, and `Anomalies` (a low-noise "this
kind got slower / started failing"), `ListRunFinishes` (every finish once, in order, polled from a
cursor), and `ListChildren` and `JobTree` (a fan-out's children and its tree, within bounds). Every read is bounded and indexed — tens of milliseconds at a million
runs; the next guide covers the queue and failure views.
[`docs/INTEGRATING.md`](docs/INTEGRATING.md) covers the data contract, each API's cost, recipes, direct
SQL, and the upgrade for an existing database; [`examples/dashboard-api`](examples/dashboard-api/main.go)
is a read-only JSON API over all of it.

</details>

<details>
<summary><strong><code>Stats by queue, queue history, and error groups</code></strong></summary>
<br>

`Stats` reports a window per kind by default. `StatsParams.GroupBy` reports it per queue
(`StatsByQueue`) or per kind on each queue (`StatsByKindQueue`), from the same hourly rollups and raw
tail, and every entry's `Enqueued` counts the jobs inserted in the window beside the attempts that
finished — a queue whose inserts outrun its finishes, hour after hour, is falling behind:

```go
res, err := flywheel.Stats(ctx, db, flywheel.StatsParams{
    From: now.Add(-24 * time.Hour), To: now, GroupBy: flywheel.StatsByQueue,
})
for _, q := range res.Kinds {
    // q.Queue, q.Enqueued, q.Attempts, q.Discarded, q.SuccessRate, q.QueueWait.P95 …
}
```

`Enqueued` reads the insert time a job's UUIDv7 id carries, not `created_at`, so a job a fixed clock
stamped with another time counts where its id says, and a job inserted with an id of your own counts in
the hour its leading 48 bits name. An hour the rollup closed before it counted inserts reads zero until
`RebuildStats` re-rolls it. `flywheel stats --by queue` prints the same table.

**A queue's depth over time.** `QueueDepths` is this instant. Set `SchedulerConfig.QueueSampleInterval`
(a minute is typical) and the scheduler samples every queue's depth into `job_queue_samples`, keeping
`QueueSampleRetention` (30 days by default). `QueueDepthHistory` reads one queue's buckets, each count the
bucket's maximum so a spike shows, at most `MaxHistoryPoints` (1,440) of them:

```go
points, err := flywheel.QueueDepthHistory(ctx, db, flywheel.QueueHistoryParams{
    Queue: "default", From: now.Add(-7 * 24 * time.Hour), // Step zero: the smallest whole minute that fits
})
```

**Error groups.** Every attempt that ends in `error` or `timeout` stores a fingerprint: 16 hex characters
over its kind, its error class, and its message with the values that differ between occurrences masked —
UUIDs, email addresses, URLs, hex runs, quoted values, numbers (`NormalizeErrorMessage`). The stats rollup
groups each hour's failures by kind and fingerprint, and `ErrorGroups` reads any window, most failures
first, with when each group was first and last seen and a sample of its message:

```go
res, err := flywheel.ErrorGroups(ctx, db, flywheel.ErrorGroupsParams{From: now.Add(-24 * time.Hour), To: now})
for _, g := range res.Groups {
    // g.Kind, g.Message, g.Failures, g.Retried, g.Discarded, g.FirstSeen, g.LastSeen …
}
```

A worker whose messages vary while its failure is one thing — an upstream's error code — names the
failure by returning an error that implements `ErrorFingerprinter`; every attempt that returns the same
value joins one group. `ErrorFingerprint(kind, class, message)` computes the fingerprint a message gets,
and `JobRunView.ErrorFingerprint` reports a run's. The masking rules are versioned
(`ErrorFingerprintVersion`): a change to any of them is a new version, after which groups restart.

To open a group as its jobs, filter `QueryJobs` by its `Fingerprint` and kind (see **Finding jobs**
above), and `flywheel errors` prints the groups of a recent window.

[`docs/INTEGRATING.md`](docs/INTEGRATING.md#error-groups-what-is-failing) covers each read's cost, the
tables behind them for direct SQL, and the upgrade.

</details>

<details>
<summary><strong><code>The finish feed, children, and job trees</code></strong></summary>
<br>

**`ListRunFinishes` is the feed of what just finished.** It reads the finish log in order, a page at a
time, and its `NextCursor` is always set: poll from it, and every finish arrives once, in order, with its
run, job, kind, queue, attempt, outcome, error class, and duration.

```go
page, err := flywheel.ListRunFinishes(ctx, db, flywheel.ListRunFinishesParams{After: cursor})
for _, f := range page.Finishes {
    fmt.Println(f.FinishedAt, f.Kind, f.Outcome, f.JobID)
}
cursor = page.NextCursor // the next poll starts here
```

A finalize stamps its finish before it commits, so a commit can land behind a reader that has read past
its time. The feed holds back the newest `Settle` of the log (`DefaultFeedSettle`, 5 seconds) and reports
that horizon, so a finalize that commits within it is never skipped. A page scans at most 1,000 entries
and its cursor moves past every one, so a `Kinds` or `Queues` filter returns a short page rather than a
slow one.

**Children and trees.** `ListChildren` pages a job's children newest first, each with its metadata and
latest run, behind a `jc1.` cursor. `JobTree` walks a job's descendants breadth first, within
`MaxDepth` levels (5 by default, 10 at most) and `MaxNodes` nodes (500 by default, 2,000 at most), and
reports each node's full child count and whether a bound cut the walk:

```go
tree, err := flywheel.JobTree(ctx, db, rootID, flywheel.JobTreeParams{})
// tree.Root.Children, tree.Root.ChildCount, tree.Nodes, tree.Truncated
```

The walk reads one level at a time with bounded statements, the same on PostgreSQL and SQLite, so a
fan-out of any width costs what the bounds allow. `ChildOutputsWithOptions` reads the children's outputs
in any states, so a batch's view shows what has been produced before the batch has finished.
`flywheel jobs tree <job-id>` prints a tree.

</details>

<details>
<summary><strong><code>Nodes and queue pause — seeing the fleet and holding a queue</code></strong></summary>
<br>

**The node registry.** Set `NodeConfig.RegistryInterval` and a `Node` registers itself in `job_nodes`.
Before it starts anything it writes a row for this process lifetime — a new id, its `host:pid` (the
`executor_id` its runners stamp on every run), its host, the `Version` and `Commit` you give it, its
runners' queues, executor classes, and slots, and every kind their registries register — then heartbeats
it with each runner's jobs in flight, and stamps its stop time when `Run` returns:

```go
node, err := flywheel.NewNode(flywheel.NodeConfig{
    Runners:          runners,
    Scheduler:        &schedulerCfg, // its NodeRetention prunes old node rows: 7 days by default
    Version:          buildVersion,
    Commit:           buildCommit,
    RegistryInterval: 15 * time.Second,
})
```

`ListNodes` lists the nodes that have not stopped, then, with `StoppedSince`, those that stopped since,
each flagged `Stale` once it has gone `StaleAfter` (a minute by default) without a heartbeat — which is
how a process that died looks, since it never wrote a stop time. A run belongs to the node whose
`ExecutorID` matches the run's `executor_id` and whose span covers the run's start. A registering Node that
runs a Scheduler deletes the rows of nodes silent for longer than `SchedulerConfig.NodeRetention`, at
start and hourly. With `RegistryInterval` set, `Run` refuses to start, with `ErrSchemaOutdated`, until
`job_nodes` exists; keep the interval well below `StaleAfter`.

**Draining a node.** `RequestDrain(ctx, db, nodeID)` asks one node to stop taking work, and records the
request, with its actor and reason, in the control history. Within one heartbeat the node stops every
runner's claims and reports `Draining`, then `Drained` once its jobs in flight finish. It keeps running —
its health server and its Scheduler — until its context ends, and takes work again only after a restart.

**Pausing a queue across the fleet.** `PauseQueue(ctx, db, queue)` holds a queue for every Runner: each
reads the paused set once per `RunnerConfig.QueueStateRefresh` (five seconds by default), before it
reserves a slot or asks its limiter, and leaves a paused queue out of its claims. Attempts already running
finish, the queue's jobs keep their states, and `ResumeQueue` lets them be claimed again. Each pause and
resume records who ran it and why:

```go
ctx = flywheel.WithActor(ctx, flywheel.Actor{Type: flywheel.ActorUser, ID: userID, Reason: "provider outage"})
if err := flywheel.PauseQueue(ctx, db, "mail"); err != nil {
    return err
}
queues, err := flywheel.ListQueues(ctx, db) // every queue: its depth, IsPaused, PausedAt, PausedBy
```

A queue no job has used can be paused, and stays paused for the jobs that arrive. A Runner whose every
queue is paused claims nothing and holds no slot; `RunUntilIdle` waits for a resume while a paused queue
holds jobs, and returns once nothing is pending anywhere. A Runner that cannot read the paused set keeps
the set it last read, with a warning; one that has never read it claims every queue. A Runner refuses to
start, with `ErrSchemaOutdated`, until `job_queues` exists — unless `QueueStateRefresh` is negative, which
turns the check off and claims every queue.

`flywheel nodes`, `flywheel nodes drain`, `flywheel queues`, and `flywheel queues pause|resume` do the same
from the [CLI](cmd/flywheel/README.md). [`docs/INTEGRATING.md`](docs/INTEGRATING.md#nodes-and-queue-state)
covers the reads, the tables, and the rule that joins a run to its node, and the
[runbook](docs/RUNBOOK.md) the operations.

</details>

<details>
<summary><strong><code>Retention and scheduler maintenance</code></strong></summary>
<br/>

The `Scheduler` runs up to seven maintenance activities, each on its own goroutine and its own
cadence: the periodic tick, the lease sweep, and — when you enable them — a retention prune, a
queue-health heartbeat, the hourly stats rollup, the queue-depth sampler, and the reconcile that keeps
declared periodics present.

Independence is the point. A slow retention prune must not delay the lease sweep, because the sweep is
the only path by which work lost to a crashed process comes back. An activity also never overlaps
itself: a tick that arrives while its predecessor is still running is skipped and logged at warn with
a **consecutive** skip count. One skip is a slow pass; a climbing count is a cadence your deployment
needs to widen.

```go
driver := flywheel.NewPostgresDriver(db)

node, _ := flywheel.NewNode(flywheel.NodeConfig{
    Runners: []flywheel.RunnerConfig{{DB: db, Driver: driver, Registry: reg, Queues: queues}},
    Scheduler: &flywheel.SchedulerConfig{
        DB:     db,
        Client: flywheel.NewClient(db),
        Driver: driver, // the runner's driver, not a second one
    },
})
```

The `Driver` is required, and it should be the **same instance** your runners hold. The lease sweep is
a database operation like any other: if you wrap your driver for metrics or tracing, the wrapper only
sees the sweep when it is the wrapper you passed here.

### Everything is bounded

Both maintenance operations work in bounded batches, one transaction per batch, looping until the
backlog is drained. No batch size — zero, negative, or unset — produces an unbounded transaction.

```go
// Tune the batches when the defaults do not fit your deployment.
driver := flywheel.NewPostgresDriverWithOptions(db, flywheel.DriverOpts{SweepBatchSize: 2000})

cfg := flywheel.SchedulerConfig{
    RetentionBatchSize:  500,
    RetentionMaxBatches: 20, // cap one pass's total work
}
```

`RetentionMaxBatches` bounds how much a single scheduled prune does, which makes its duty cycle
predictable when the backlog is months deep. The sweep has no equivalent ceiling on purpose: an
unpruned row is only storage, while an unreclaimed lease is stalled work.

A cancelled pass reports the rows it committed alongside the context error — the completed batches are
not rolled back — so treat a non-nil error as "partial progress", not "nothing happened".

The reads are bounded the same way. Every list read — `ListJobs`, `ListFinished`, `ListRunning`,
`SlowRuns`, `RecentFailures`, `ListRuns`, `ListAudit`, `ListRunFinishes`, and `ListChildren` — returns at most `flywheel.MaxReadLimit` (200) rows: a
larger `Limit` is clamped, so a handler can pass a request's limit straight through. `ListRuns` with no
`Limit` reads a job's newest 200 attempts; page past them with `BeforeAttempt`. `ChildOutputs` and
`ChildOutputsWithOptions` are the one exception: with no `Limit` they read every matching child, the fold
a barrier continuation needs, and `DriverOpts.BarrierMaxChildren` bounds that generation. `JobTree` has
bounds of its own: 10 levels and 2,000 nodes at most.

### Retention

Retention is **off by default**. `jobs` and `job_runs` otherwise grow forever: one row per job plus one
per attempt, so a daemon at 1,000 jobs/day with 1.1 attempts/job accumulates roughly 800k rows a year.

It is off rather than on because the runtime cannot know whether you treat `job_runs` as an audit
record of record. Turn it on deliberately:

```go
cfg := flywheel.SchedulerConfig{
    RetentionMaxAge:   30 * 24 * time.Hour,
    RetentionInterval: time.Hour,
}
```

**Choosing a window.** Longer than the longest question you ask of the audit trail. Thirty to ninety
days suits most deployments; if you answer "what happened to this job last quarter", you need a
quarter.

**Before you enable it.** Run one bounded pass by hand and look at what it removes:

```go
deleted, err := flywheel.DeleteFinishedJobsWithOptions(ctx, db, cutoff,
    flywheel.RetentionOpts{BatchSize: 100, MaxBatches: 1})
```

`flywheel prune --older-than 720h` does the same from the CLI. With the stats rollup on, both hold for
it — set `HoldForStatsRollup` in `RetentionOpts` for a pass of your own — so no run is deleted before the
rollup has counted it.

**If your schema references `job_runs`.** The library declares no foreign key between `jobs` and
`job_runs`, but your schema may. A row of yours pointing at `job_runs.id` must be `ON DELETE SET NULL`
— or retention has to stay off, because a prune becomes a foreign-key event in a table the runtime
does not own. Within each batch the audit rows are deleted before their jobs, which is contractual
rather than incidental: under an `ON DELETE CASCADE` the reverse order lets the cascade do the work
silently.

Only terminal jobs are ever removed — succeeded, cancelled, discarded, and only those finalized before
the cutoff. Pending and running work is never touched, whatever its age.

**Retention frees `UniqueKey`s.** A `UniqueKey` collides only with a job whose row exists, so deleting a
job frees its key, and an `Insert` with that key afterwards runs the work again. If a key must keep
refusing duplicates for longer than the window, see **Replaying work** above for where to hold it.

**The control history has a window of its own.** Retention keeps a pruned job's `job_audit` entries, so
the history of who changed what outlives the jobs. Set `AuditRetention` to bound it; zero, the default,
keeps every entry:

```go
cfg := flywheel.SchedulerConfig{
    RetentionMaxAge: 30 * 24 * time.Hour,
    AuditRetention:  400 * 24 * time.Hour, // the history outlives the jobs it names
}
```

`AuditRetention` alone turns the retention activity on, and each pass prunes finished jobs first, then the
entries older than the window, oldest first, in the same bounded batches (`RetentionBatchSize`,
`RetentionMaxBatches`), logging `jobs: retention sweep pruned audit entries`. A scheduler with it set
refuses to start without the table (`ErrSchemaOutdated`). `DeleteAuditEntries` prunes on demand.

### The queue-health heartbeat

The heartbeat is **off by default**. Set `HealthSampleInterval` and the scheduler samples queue health
on that cadence and logs one `jobs: queue health` line: `ready`, `inflight`, `scheduled_ahead`,
`discarded`, and the lag twice — `oldest_ready` as a duration string for people, and
`oldest_ready_seconds` as a number.

The number is for a deployment that derives metrics from its logs instead of scraping `/metrics`. It
is the same value the `flywheel_queue_oldest_ready_seconds` gauge reports, and it is on every pulse,
zero when nothing is ready, so a log-metric filter gets a datapoint each interval and can alarm on lag
directly.

### Declared periodics

Declare the periodic jobs a host runs in `SchedulerConfig.Periodics`. The scheduler applies them when
it starts and keeps them present while it runs: a definition whose row goes missing — a database
restored from an older snapshot, a development database rebuilt from migrations under a running node,
a `DeletePeriodic` — is re-created by the next reconcile pass and logged once, at warn.

```go
node, _ := flywheel.NewNode(flywheel.NodeConfig{
    Runners: []flywheel.RunnerConfig{{DB: db, Driver: driver, Registry: reg, Queues: queues}},
    Scheduler: &flywheel.SchedulerConfig{
        DB:     db,
        Client: flywheel.NewClient(db),
        Driver: driver,
        Periodics: []flywheel.PeriodicSpec{
            {Slug: "nightly-report", Kind: "send_report", Cron: "0 2 * * *", Active: true},
            {Slug: "inventory-sync", Kind: "sync_inventory", Every: 15 * time.Minute, Active: true},
        },
    },
})
```

| Field | Default | What it does |
|---|---|---|
| `Periodics` | none | The definitions this host declares. Each is validated, and slugs must be unique, when the scheduler is constructed; `Run` applies them on start. |
| `ReconcileInterval` | `1m` | How often a declared definition whose row is missing is re-created. A pass with nothing missing is one indexed read that writes nothing. Negative turns the pass off. |

**A cron runs in the zone it is evaluated in, unless it names one.** A `Cron` with no zone fires in the
zone of the Scheduler clock when the definition is created or changed (the default clock reads
`time.Now()`, the process's local zone), and in the zone of the stored `next_run_at`, as the database
driver returns it, on every fire after that. Whether `0 2 * * *` means 02:00 UTC or 02:00 local time, and
whether it follows daylight saving, therefore depends on the host and the driver. Prefix the expression
with `CRON_TZ=<zone>` to pin it: `CRON_TZ=UTC 0 2 * * *`, or `CRON_TZ=America/New_York 0 2 * * *` for
02:00 New York time across daylight-saving changes. An unknown zone is refused like any malformed
expression.

**On start, the declaration wins, except over a pause.** Every start applies the whole declaration with
`UpsertPeriodic`'s semantics: a missing definition is inserted, a changed one updated, and an unchanged one
keeps its `next_run_at` cursor, so a restart does not reset a cadence. `Active` is applied too, so a
declared definition an operator deactivated with `SetPeriodicActive` is active again after the next start.
A `PeriodicSpec`'s zero `Active` is inactive: a definition meant to fire says `Active: true`. When the
database is unreachable at start, `Run` keeps going — it logs `jobs: declared periodic apply failed`
and retries the declarations that did not land on the reconcile cadence, every minute when the pass is
off, until they do. A declaration that keeps failing holds back only itself: the reconcile keeps every
other declared definition present meanwhile.

**To stop a declared definition at run time, pause it.** `Active` is the declaration's switch; a pause is
the operator's, and nothing that declares a definition writes it — not a start, not the reconcile, not
`UpsertPeriodic`. `PausePeriodic(ctx, db, slug)` (`flywheel schedule pause`) holds a definition through
every restart until `ResumePeriodic` (`flywheel schedule resume`). A start still applies a paused
definition's kind, queue, args, and cadence, and logs `jobs: declared periodic is paused` (fields `slug`,
`paused_at`) once, at info. A definition fires only when it is active and not paused, so a pause holds
whatever the declaration says, and `Active: false` in code turns a definition off whatever the pause says.
A resume moves `next_run_at` to the first fire after now on the definition's own schedule: the fires the
pause skipped are not backfilled, and an inactive definition stays inactive. Pausing a paused definition,
or resuming one that is not paused, changes nothing; an unknown slug is `ErrPeriodicNotFound`.
`ListPeriodics` reports `Paused` and `PausedAt`. The pause lives on the row, in the nullable
`job_periodics.paused_at` column, so `DeletePeriodic` removes it with the row. A scheduler refuses to start
without that column (`ErrSchemaOutdated`): `Migrate` adds it to an existing database, and
[From a release before the schedule pause](docs/INTEGRATING.md#from-a-release-before-the-schedule-pause)
has the statement for a host-owned schema on each dialect.

**While it runs, the reconcile only inserts.** A definition that exists — deactivated, paused, or edited by
hand — is left exactly as it is until the next start, and one this host does not declare is never
touched. A re-created definition fires next at its first fire time after the re-creation; the runs it
missed while gone are not backfilled. Several schedulers declaring the same definitions is safe: the
insert does nothing when the slug exists, and only the scheduler whose insert landed logs
`jobs: declared periodic was missing and has been re-created` (fields `slug`, `kind`, `next_run_at`).

**Retiring a declared definition** takes two steps, in this order: remove it from `Periodics` on every
host that declares it, then delete its row. A row deleted while a running scheduler still declares it
comes back — including from a host a rolling deploy has not reached yet.

</details>

<details>
<summary><strong><code>Local daemon & cron replacement</code></strong></summary>
<br>

The [`flywheel` CLI](cmd/flywheel/README.md) runs the runtime as a local daemon over a SQLite file
(zero-ops) or Postgres, and replaces cron with durable scheduled jobs — no custom Go required.
Grab the prebuilt binary for your platform from the
[releases page](https://github.com/mrz1836/go-flywheel/releases) and install it to `~/.local/bin`
so `flywheel update` can keep it current (see [Install](cmd/flywheel/README.md#install)):

```bash
flywheel migrate   # stand up the schema
flywheel serve     # run runner + scheduler until Ctrl+C
flywheel jobs ls   # inspect the queue
```

Declare your jobs in `flywheel.yaml` — each run is retried, audited, and overlap-protected,
strictly better than a crontab line. Pick the worker that matches what you run locally: `shell`
(a `.sh` file or inline snippet), `python` (a script, `-m` module, or `-c` snippet), `mage`
(magex/mage build targets), `exec` (any binary), or `http` (call a URL):

```yaml
schedules:
  - slug: nightly-maintenance      # a shell script — file or inline, no +x needed
    every: 24h
    worker: shell
    shell:
      script: /usr/local/bin/maintenance.sh
      args: ["--verbose"]
      timeout_seconds: 600

  - slug: hourly-sync              # a Python script — resolves python3, then python
    cron: "0 * * * *"
    worker: python
    python:
      script: /opt/hermes/sync.py
      args: ["--since=1h"]

  - slug: repo-deps-update         # magex/mage targets — the Go-native task runner
    every: 24h
    worker: mage
    mage:
      targets: ["deps:update"]     # e.g. ["test"], ["lint"], ["version:bump", "push=true"]
      dir: /Users/me/projects/my-repo

  - slug: gateway-healthcheck      # call a URL
    cron: "*/5 * * * *"
    worker: http
    http:
      url: https://gateway.internal/healthz
```

The file is the source of truth: `serve` applies it on every start, disables the schedules it does
not name, and re-creates a declared schedule deleted while it runs. Every run's stdout, stderr, and
exit code are captured to the `job_runs` audit trail — inspect them with `flywheel jobs inspect <id>`. Prefer to wire it from Go? The
[examples/local-tasks](examples/local-tasks) program registers the shell, python, and mage
workers and schedules one of each. See the [CLI README](cmd/flywheel/README.md) for every
command, the config reference, and the macOS launchd setup.

</details>

<br/>

## 📚 Documentation

- **API Reference** – Dive into the godocs at [pkg.go.dev/github.com/mrz1836/go-flywheel](https://pkg.go.dev/github.com/mrz1836/go-flywheel)
- **Contract** – The exactly-once guarantees and their limits in [`docs/CONTRACT.md`](docs/CONTRACT.md)
- **Cookbook** – Enqueue recipes for deduplication, joining an in-flight job, side-effect correlation, and recording who enqueued a job in [`docs/COOKBOOK.md`](docs/COOKBOOK.md)
- **Integrating** – Building dashboards and external platforms on the runtime's data, and upgrading an existing database, in [`docs/INTEGRATING.md`](docs/INTEGRATING.md)
- **Runbook** – Operating the runtime and reading its metrics in [`docs/RUNBOOK.md`](docs/RUNBOOK.md)
- **Tuning** – Sizing the knobs from measured numbers in [`docs/TUNING.md`](docs/TUNING.md)
- **Dashboards** – An importable Grafana dashboard over the metrics in [`docs/dashboards/`](docs/dashboards/)
- **Benchmarks** – The measured 100k baseline, environment, and index comparison in [`docs/BENCHMARKS.md`](docs/BENCHMARKS.md)
- **Test Suite** – Review the [test suite](internal/core/integration_test.go) (powered by [`testify`](https://github.com/stretchr/testify))

<br/>

<details>
<summary><strong><code>Repository Features</code></strong></summary>
<br/>

This repository includes 24 built-in features covering CI/CD, security, code quality, developer experience, and community tooling.

**[View the full Repository Features list →](.github/docs/repository-features.md)**

</details>

<details>
<summary><strong><code>Library Deployment</code></strong></summary>
<br/>

This project uses [goreleaser](https://github.com/goreleaser/goreleaser) for streamlined binary and library deployment to GitHub. To get started, install it via:

```bash
brew install goreleaser
```

The release process is defined in the [.goreleaser.yml](.goreleaser.yml) configuration file.


Then create and push a new Git tag using:

```bash
magex version:bump push=true bump=patch branch=master
```

This process ensures consistent, repeatable releases with properly versioned artifacts and metadata.

</details>

<details>
<summary><strong><code>Pre-commit Hooks</code></strong></summary>
<br/>

Set up the Go-Pre-commit System to run the same formatting, linting, and tests defined in [AGENTS.md](.github/AGENTS.md) before every commit:

```bash
go install github.com/mrz1836/go-pre-commit/cmd/go-pre-commit@latest
go-pre-commit install
```

The system is configured via modular env files in [`.github/env/`](.github/env/README.md) and provides 17x faster execution than traditional Python-based pre-commit hooks. See the [complete documentation](http://github.com/mrz1836/go-pre-commit) for details.

</details>

<details>
<summary><strong><code>GitHub Workflows</code></strong></summary>
<br/>

All workflows are driven by modular configuration in [`.github/env/`](.github/env/README.md) — no YAML editing required.

**[View all workflows and the control center →](.github/docs/workflows.md)**

</details>

<details>
<summary><strong><code>Updating Dependencies</code></strong></summary>
<br/>

To update all dependencies (Go modules, linters, and related tools), run:

```bash
magex deps:update
```

This command ensures all dependencies are brought up to date in a single step, including Go modules and any tools managed by [MAGE-X](https://github.com/mrz1836/mage-x). It is the recommended way to keep your development environment and CI in sync with the latest versions.

</details>

<details>
<summary><strong><code>Build Commands</code></strong></summary>
<br/>

View all build commands

```bash script
magex help
```

</details>

<br/>

## 🧪 Examples & Tests

Four complete, runnable programs live under [`examples/`](examples), smallest first:

- **[`sqlite-quickstart`](examples/sqlite-quickstart)** — the smallest "bolt flywheel onto an app": one worker, one enqueue, a runner + scheduler over a local SQLite file.
- **[`exec-cron`](examples/exec-cron)** — flywheel as a cron replacement, the generic `ExecWorker` running a shell command on an interval with no job-specific Go.
- **[`local-tasks`](examples/local-tasks)** — local developer tasks run durably: a shell script, a Python script, and a magex/mage target, each a typed worker with a captured audit trail.
- **[`split-executors`](examples/split-executors)** — one registry across two executors, a long-running pool and a bounded invocation-scoped burst, routed by `ExecutorClass`.

Testing your own integration? The [`flywheeltest`](flywheeltest) package ships ready-made fixtures — an in-memory or file-backed SQLite database (or an isolated PostgreSQL schema) with the runtime schema migrated in, plus `WaitForJobState`, which fails fast on a wrong terminal state or a missing job, and a trivial success worker — so your tests can enqueue and assert without a bespoke harness.

All unit tests run via [GitHub Actions](https://github.com/mrz1836/go-flywheel/actions) and use [Go version 1.26.x](https://go.dev/doc/go1.26). View the [configuration file](.github/workflows/fortress.yml).

Run all tests (fast):

```bash script
magex test
```

Run all tests with race detector (slower):
```bash script
magex test:race
```

<br/>

## ⚡ Benchmarks

Run the Go benchmarks:

```bash script
magex bench
```

**[`docs/BENCHMARKS.md`](docs/BENCHMARKS.md) publishes the runtime's measured baseline**: enqueue,
claim, finalize, and sweep against real PostgreSQL at 100,000 jobs, with the full environment, the
commands that produced every number, and a full-index versus correctness-index-only comparison. The
JSON reports behind it are committed under [`docs/benchmarks/`](docs/benchmarks/).

Three numbers to size a deployment against, all at 100,000 jobs on a developer laptop sharing its
cores with the database — so treat them as floors:

| | |
|---|---|
| **Claim p50 / p99** | 0.96 ms / 1.91 ms — served by an ordered index scan, not a scan-and-sort |
| **Drain throughput** | ~9,500 jobs/s at `-runners 4 -workers 8` with no simulated work |
| **Slot utilization** | 95 % at `Concurrency: 8` on a workload where 10 % of jobs run 20× longer |

That last one is the property to design against: throughput on a real workload is bounded by the work
itself, not by the dispatch loop, because a slow job occupies one slot instead of stalling its siblings.

The hot paths are measured by a load harness in [`loadtest/`](loadtest/), behind its own build tag so
its runs never join an ordinary `go test ./...`:

```bash
export FLYWHEEL_LOADTEST_DATABASE_URL="postgres://localhost:5432/flywheel_test?sslmode=disable"
go test -tags=loadtest -run='^$' -bench='BenchmarkClaim100k|BenchmarkEnqueue100k' -benchtime=1x ./loadtest/
```

<br/>

## 🛠️ Code Standards
Read more about this Go project's [code standards](.github/CODE_STANDARDS.md). The library-specific
conventions a contributor inherits — the `jobs:`/`flywheel:` prefix rule, sentinel naming, the
`…WithOptions`/`…Opts` pairing, the Postgres test mirror, and the retained-seam pattern — are recorded in
[`docs/CONVENTIONS.md`](docs/CONVENTIONS.md).

<br/>

## 🤖 AI Usage & Assistant Guidelines
Read the [AI Usage & Assistant Guidelines](.github/tech-conventions/ai-compliance.md) for details on how AI is used in this project and how to interact with the AI assistants.

<br/>

## 👥 Maintainers
| [<img src="https://github.com/mrz1836.png" height="50" width="50" alt="MrZ" />](https://github.com/mrz1836) |
|:-----------------------------------------------------------------------------------------------------------:|
|                                      [MrZ](https://github.com/mrz1836)                                      |

<br/>

## 🤝 Contributing
View the [contributing guidelines](.github/CONTRIBUTING.md) and please follow the [code of conduct](.github/CODE_OF_CONDUCT.md).

### How can I help?
All kinds of contributions are welcome :raised_hands:!
The most basic way to show your support is to star :star2: the project, or to raise issues :speech_balloon:.
You can also support this project by [becoming a sponsor on GitHub](https://github.com/sponsors/mrz1836) :clap:
or by making a [**bitcoin donation**](https://mrz1818.com/?tab=tips&utm_source=github&utm_medium=sponsor-link&utm_campaign=go-flywheel&utm_term=go-flywheel&utm_content=go-flywheel) to ensure this journey continues indefinitely! :rocket:

[![Stars](https://img.shields.io/github/stars/mrz1836/go-flywheel?label=Please%20like%20us&style=social&v=1)](https://github.com/mrz1836/go-flywheel/stargazers)

<br/>

## 📝 License

[![License](https://img.shields.io/github/license/mrz1836/go-flywheel.svg?style=flat&v=1)](LICENSE)
