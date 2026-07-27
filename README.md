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
       🚀&nbsp;<a href="#-installation"><code>Installation</code></a>
    </td>
    <td align="center" width="33%">
       🧪&nbsp;<a href="#-examples--tests"><code>Examples&nbsp;&&nbsp;Tests</code></a>
    </td>
    <td align="center" width="33%">
       📚&nbsp;<a href="#-documentation"><code>Documentation</code></a>
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
      ⚡&nbsp;<a href="#-benchmarks"><code>Benchmarks</code></a>
    </td>
  </tr>
  <tr>
    <td align="center">
      🤖&nbsp;<a href="#-ai-usage--assistant-guidelines"><code>AI&nbsp;Usage</code></a>
    </td>
    <td align="center">
       ⚖️&nbsp;<a href="#-license"><code>License</code></a>
    </td>
    <td align="center">
       👥&nbsp;<a href="#-maintainers"><code>Maintainers</code></a>
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

- **Typed workers** — generic `Worker[A]` interface, registered by `Kind()` ([registry.go](registry.go))
- **One-call lifecycle** — `Node` runs N runners + the scheduler + an optional health/metrics server and drains cleanly on shutdown ([node.go](node.go))
- **Scheduler** — periodic / cron job enqueuing plus stuck-lease recovery; declare schedules in code with `UpsertPeriodic` ([scheduler.go](scheduler.go), [schedule.go](schedule.go))
- **Bounded worker pool** — `Concurrency` slots that refill independently, so one slow job never idles the rest; explicit `Stop`/`Drain` with an in-flight count ([runner.go](runner.go))
- **Retries with backoff** — exponential backoff with jitter, overridable per worker; consecutive poll failures climb their own ladder so a failing database is not hammered ([runner.go](runner.go))
- **Worker timeouts** — per-job or per-kind execution deadlines that classify as a retryable timeout ([runner.go](runner.go))
- **Lease-based recovery** — orphaned, crashed jobs reclaimed via `leased_until` sweeps ([scheduler.go](scheduler.go))
- **Per-run audit** — append-only `job_runs` table records every attempt, outcome, timing, and cost ([read.go](read.go))
- **Observability built in** — a dependency-free `Observer` seam with ready-made metrics, slog, and Prometheus adapters, queue-health/lag inspection, a `/metrics` endpoint, and `flywheel status` ([observer.go](observer.go), [observers/](observers), [health.go](health.go))
- **Postgres + SQLite** — `FOR UPDATE SKIP LOCKED` and `BEGIN IMMEDIATE` drivers ([driver_postgres.go](driver_postgres.go), [driver_sqlite.go](driver_sqlite.go))
- **Free-form routing** — a `ExecutorClass` label routes jobs to executor pools; empty is the wildcard ([types.go](types.go))
- **Idempotent enqueue** — `jobs_unique_key` partial unique index dedupes work ([client.go](client.go))
- **Follow-up jobs (DAG)** — workers return child jobs that are enqueued atomically ([types.go](types.go))
- **Outbox pattern** — enqueue on the caller's own `*gorm.DB` transaction for exactly-once side effects ([client.go](client.go))
- **Generic workers** — ready-made `ExecWorker`, `ShellWorker`, `PythonWorker`, `MageWorker` (magex/mage), and `HTTPWorker` so local scripts and build tasks need no custom Go ([workers/](workers))

<br/>

### Schema setup

`go-flywheel` owns three tables — `jobs`, `job_runs`, `job_periodics` — and there are **two ways to
install them. Pick exactly one.** They are not layers; running both means two migration authorities
against one database.

| Question | **Library-owned** | **Host-owned** |
|---|---|---|
| Who creates the three tables? | `Migrate(db)` | your loader, from `Models()` |
| Who creates the indexes? | `Migrate(db)` | you, from `IndexSet(dialect)` / `InstallIndexes` |
| Who owns migration history? | nobody — `AutoMigrate` is declarative | your migration tool |
| Does the runtime run DDL at startup? | yes, every start | no |
| Is a co-located host schema safe? | only if the host's tooling excludes the three tables | yes — the tables are in your loader |
| **Pick this when** | the database is the runtime's alone: a dedicated queue database, a CLI, a local SQLite file | the runtime's tables share a database with an application schema |

**The last row is the rule: a shared database means host-owned.** If your app's own tables live in the
same database, your migration tool must know about `jobs`, `job_runs`, and `job_periodics` — a tool that
cannot see them will happily propose dropping them.

**Library-owned** — one call, no external tooling:

```go
import "github.com/mrz1836/go-flywheel"

if err := flywheel.Migrate(db); err != nil { // db is a *gorm.DB
    return err
}
```

`Migrate` runs `AutoMigrate` over the row structs and then applies the partial/unique indexes GORM
cannot express from struct tags. It is idempotent (`AutoMigrate` no-op + `CREATE INDEX IF NOT EXISTS`),
so repeated calls are safe.

**Host-owned** — your loader creates the tables, you apply the indexes:

```go
// 1. In your schema loader (Atlas / atlas-provider-gorm, or any GORM-based generator),
//    so your migration tool knows the three tables exist and never proposes dropping them.
stmts, err := gormschema.New("postgres").Load(
    append(myapp.AllModels(), flywheel.Models()...)...,
)

// 2. In your install/deploy path, right where you apply your migrations.
if err := flywheel.InstallIndexes(ctx, db); err != nil {
    return err
}
```

Step 2 is **not optional and not an optimization.** `Models()` gives your loader the tables and columns;
it does not give it the indexes, because every one of them has a `WHERE` predicate or spans columns a
GORM struct tag cannot express. Four of the eight are correctness-bearing — without `jobs_unique_key`
and `jobs_unique_active_key` the database accepts duplicate enqueues and **`ErrAlreadyEnqueued` is never
returned.** Use `flywheel.IndexSet(dialect)` when you want them classified:

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

A host that owns its schema history but still wants the installer can have both:
`MigrateWithOptions(db, MigrateOpts{SkipColumnReconcile: true})` skips the pre-1.0 routing-column rename
pass, so the runtime issues no `ALTER TABLE` of its own inside a versioned schema. That reconciliation
is removed in v1.0.0.

> Only PostgreSQL and SQLite are supported, because both express the partial indexes the runtime relies
> on. Every entry point returns `flywheel.ErrUnsupportedDialect` for anything else rather than silently
> dropping idempotency. The module takes **no** hard dependency on Atlas or any external migration tool.

<br/>

### Quick start (embedded)

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
	db, _ := gorm.Open(sqlite.Open("flywheel.db"), &gorm.Config{})
	_ = flywheel.Migrate(db) // creates the jobs / job_runs / job_periodics tables

	reg := flywheel.NewRegistry()
	flywheel.Register(reg, Summarizer{})

	// ② Enqueue work. Returns instantly — the caller never waits on the LLM.
	//    (Pass InsertOpts.Tx to enqueue inside your own DB transaction.)
	_, _ = flywheel.Insert(context.Background(), flywheel.NewClient(db),
		SummarizeDoc{DocID: "42"}, flywheel.InsertOpts{})

	// ③ Run a Node: it claims jobs, runs your worker, retries failures, and
	//    drains cleanly on Ctrl+C. Concurrency: 4 → four summaries at once.
	node, _ := flywheel.NewNode(flywheel.NodeConfig{
		Runners: []flywheel.RunnerConfig{{
			DB: db, Driver: flywheel.NewSQLiteDriver(db), Registry: reg,
			Queues: []string{"default"}, Concurrency: 4, ClaimAnyClass: true,
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

That's a durable AI pipeline: enqueue returns instantly, the `Node` summarizes four
documents at a time, a failed model call retries itself with backoff, and every attempt —
including what it cost — lands in the `job_runs` audit table. Need periodic or cron-style
runs too? Add a `Scheduler` to the `Node` (see [`examples/`](examples) for the full set).

<br/>

### Recording what an attempt did

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

**The lifetime of `job_runs.id`, so you can decide your FK's `ON DELETE`:**

| Moment | What happens to the row |
|---|---|
| Before your worker body runs | The row is **committed** with outcome `started`. A side-effect row written during the attempt can reference it immediately, and it survives a rollback of your own work. |
| Attempt finishes | The same row is updated in place with the outcome, timings, cost, and `Result.Output`. |
| Process crashes mid-attempt | The lease sweep marks the row `crashed`. It is **not deleted**, so your FK never dangles. |
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

<br/>

### Running one registry across two executors

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
node, _ := flywheel.NewNode(flywheel.NodeConfig{
    Runners: []flywheel.RunnerConfig{{
        DB: db, Driver: flywheel.NewPostgresDriver(db), Registry: newRegistry(),
        Queues: []string{"default", "periodic"}, ExecutorClass: "worker", Concurrency: 4,
    }},
    Scheduler: &flywheel.SchedulerConfig{DB: db, Client: flywheel.NewClient(db)},
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
    return err // a real failure — driver error, unregistered kind
}
```

**What `RunUntilIdle` guarantees.** It returns `nil` **only when no job is in a non-terminal state** —
not merely when this runner found nothing to claim. A job another pool is still running, one this
runner has in flight in its own worker pool, or one waiting out a retry backoff all keep it looping.
That is why the deadline branch is separate: `context.DeadlineExceeded` means "budget spent", not
"something broke". A transient database error does not end it either — see
[the poll backoff](#concurrency-claim-batching-and-drain) — and `flywheel.ErrRunnerStopped` is what it
returns if `Stop` ended it before the queue drained.

**Routing rules:**

| The job's class | Who can claim it |
|---|---|
| `flywheel.AnyClass` (the empty default) | any runner, whichever polls first |
| `"burst"` | a runner with `ExecutorClass: "burst"`, or any runner with `ClaimAnyClass: true` |

Leave a job unpinned unless it genuinely needs one pool's hardware, credentials, or budget.

> **The trap: the scheduler must run on exactly one process.** It owns the periodic ticks *and* the
> stuck-lease sweep that reclaims jobs whose executor died. Two schedulers means every tick fires twice
> and two sweeps race. Every process except one leaves `NodeConfig.Scheduler` nil — and a bounded
> process, which by definition is not always running, is never the one that has it.

A complete, runnable version of this is [`examples/split-executors`](examples/split-executors).

<br/>

### Local daemon & cron replacement

The [`flywheel` CLI](cmd/flywheel/README.md) runs the runtime as a local daemon over a SQLite file
(zero-ops) or Postgres, and replaces cron with durable scheduled jobs — no custom Go required:

```bash
go install github.com/mrz1836/go-flywheel/cmd/flywheel@latest

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

Every run's stdout, stderr, and exit code are captured to the `job_runs` audit trail — inspect
them with `flywheel jobs inspect <id>`. Prefer to wire it from Go? The
[examples/local-tasks](examples/local-tasks) program registers the shell, python, and mage
workers and schedules one of each. See the [CLI README](cmd/flywheel/README.md) for every
command, the config reference, and the macOS launchd setup.

<br/>

### Concurrency, claim batching, and drain

**`Concurrency` is the pool size.** A runner keeps up to that many jobs in flight, claims to fill
whatever slots are free, and dispatches each job independently — so a slot becomes claimable the moment
*its own* job finalizes, with no reference to its siblings. One slow job holds one slot, not the loop.

> **Changed in v0.8.0.** `Concurrency` used to mean "claim exactly N, run all N, wait for the slowest,
> claim N again". At `Concurrency: 8`, one 60-second job among seven 1-second jobs left seven slots idle
> for 59 seconds. Nothing in your config changes; the field now means what most readers already assumed
> it meant. If you were relying on the old batch-and-barrier behavior to serialize batches, set
> `Concurrency: 1`.

At `Concurrency: 1` nothing changed at all: one job in flight, dispatched sequentially on the loop's own
goroutine, with no goroutine per job. A SQLite driver still requires 1 — `NewRunner` returns
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
stranded until the sweep, and `Drain` waits for it too. `Drain` **does not cancel in-flight work** — a
worker that must be interrupted should respect the context it was given, and `DefaultTimeout` already
bounds a hung attempt. Its contract is "no new claims, then wait", which is what makes a rolling deploy
lose no work. On timeout the still-running jobs keep their leases and are recovered by the lease sweep,
exactly as they would be after a process kill.

**A `Node` does this for you.** Cancelling a `Node`'s context is a *drain* request, not an abort: every
runner is told to stop claiming, each is drained against `NodeConfig.DrainTimeout`, the timeout warning
names how many jobs were still in flight, and only then is the scheduler and health server torn down.
A zero `DrainTimeout` waits for in-flight work however long it takes — genuinely unbounded, since the
heartbeat renews a running job's lease indefinitely. Size it to the longest drain your deployment will
tolerate.

<br/>

### Leases, the heartbeat, and the fence

A claimed job carries a **lease**. If its executor dies, the lease expires and the scheduler's sweep
returns the job to the queue for another runner — which is what makes a crashed process recoverable
rather than a job lost forever.

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
- `observers.NewSlog` logs it at **warn** — the one lifecycle event it does not log at debug.
- `observers.NewMetrics` counts `flywheel_jobs_superseded_total`.

A nonzero supersede rate means work is being executed twice: the lease is too short for the workload,
or the heartbeat is disabled or failing.

**What this does not give you.** The fence closes the *library's* double-dispatch window. It cannot
make a non-idempotent external call safe: a worker killed after its side effect and before its
finalize still re-runs. Write workers that tolerate a re-run — the fence guarantees only one attempt's
outcome is ever recorded, not that only one attempt ever executes.

<br/>

### Observability

The runtime is self-diagnosing. The `Observer` seam ([observer.go](observer.go)) reports every
attempt's lifecycle — claim, start, finish, retry, supersede — with no metrics dependency in the core,
and the [`observers/`](observers) package ships ready adapters that plug straight in:

- `observers.NewMetrics(rec)` translates events into a `MetricsRecorder` — a one-method sink you back
  with Prometheus, OpenTelemetry, statsd, or CloudWatch (the core imports none of them).
- `observers.NewSlog(logger)` logs each event at debug level; `observers.NewMulti(...)` fans events
  out to several observers at once.

`SampleQueueHealth` ([health.go](health.go)) reads a point-in-time gauge snapshot — depth by state,
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

## 📚 Documentation

- **API Reference** – Dive into the godocs at [pkg.go.dev/github.com/mrz1836/go-flywheel](https://pkg.go.dev/github.com/mrz1836/go-flywheel)
- **Benchmarks** – The measured 100k baseline, environment, and index comparison in [`docs/BENCHMARKS.md`](docs/BENCHMARKS.md)
- **Test Suite** – Review both the [unit tests](integration_test.go) (powered by [`testify`](https://github.com/stretchr/testify))

<br/>

<details>
<summary><strong><code>Repository Features</code></strong></summary>
<br/>

This repository includes 25+ built-in features covering CI/CD, security, code quality, developer experience, and community tooling.

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

All unit tests run via [GitHub Actions](https://github.com/mrz1836/go-flywheel/actions) and use [Go version 1.25.x](https://go.dev/doc/go1.25). View the [configuration file](.github/workflows/fortress.yml).

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

The hot paths are measured by a load harness in [`loadtest/`](loadtest/), behind its own build tag so
its runs never join an ordinary `go test ./...`:

```bash
export FLYWHEEL_LOADTEST_DATABASE_URL="postgres://localhost:5432/flywheel_test?sslmode=disable"
go test -tags=loadtest -run='^$' -bench='BenchmarkClaim100k|BenchmarkEnqueue100k' -benchtime=1x ./loadtest/
```

<br/>

## 🛠️ Code Standards
Read more about this Go project's [code standards](.github/CODE_STANDARDS.md).

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
