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
- **Scheduler** — periodic / cron job enqueuing plus stuck-lease recovery; declare schedules in code with `UpsertPeriodic` ([scheduler.go](internal/core/scheduler.go), [schedule.go](internal/core/schedule.go))
- **Retries with backoff** — exponential backoff with jitter, overridable per worker; consecutive poll failures climb their own ladder so a failing database is not hammered ([runner.go](internal/core/runner.go))
- **Lease-based recovery** — orphaned, crashed jobs reclaimed via `leased_until` sweeps ([scheduler.go](internal/core/scheduler.go))
- **Worker timeouts** — per-job or per-kind execution deadlines that classify as a retryable timeout ([runner.go](internal/core/runner.go))
- **Per-run audit** — append-only `job_runs` table records every attempt, outcome, timing, and cost ([read.go](internal/core/read.go))
- **Idempotent enqueue** — `jobs_unique_key` partial unique index dedupes work ([client.go](internal/core/client.go))
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
runs too? Add a `Scheduler` to the `Node` (see [`examples/`](examples) for the full set).

`Migrate` above is the right call when the database is the runtime's alone. If flywheel's tables
will share a database with your own application schema, use the host-owned path instead — see
**Schema setup** below, and pick exactly one of the two.

<br/>

## 📖 Guides

Each section below is self-contained; open the ones you need.

<details>
<summary><strong><code>Schema setup — pick exactly one install path</code></strong></summary>
<br>

`go-flywheel` owns five tables — `jobs`, `job_runs`, and `job_periodics`, plus `limiter_buckets` and
`limiter_holds` for the shared database-backed limiter (additive: empty unless you construct a
`DBLimiter`) — and there are **two ways to install them. Pick exactly one.** They are not layers; running
both means two migration authorities against one database.

| Question | **Library-owned** | **Host-owned** |
|---|---|---|
| Who creates the five tables? | `Migrate(db)` | your loader, from `Models()` |
| Who creates the indexes? | `Migrate(db)` | you, from `IndexSet(dialect)` / `InstallIndexes` |
| Who sets the storage parameters? | `Migrate(db)` | you, from `InstallStorageParameters` |
| Who owns migration history? | nobody — `AutoMigrate` is declarative | your migration tool |
| Does the runtime run DDL at startup? | yes, every start | no |
| Is a co-located host schema safe? | only if the host's tooling excludes the five tables | yes — the tables are in your loader |
| **Pick this when** | the database is the runtime's alone: a dedicated queue database, a CLI, a local SQLite file | the runtime's tables share a database with an application schema |

**The last row is the rule: a shared database means host-owned.** If your app's own tables live in the
same database, your migration tool must know about all five — a tool that cannot see them will happily
propose dropping them.

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
//    so your migration tool knows all five tables exist and never proposes dropping them.
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
GORM struct tag cannot express. Four of the eleven are correctness-bearing — without `jobs_unique_key`
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
exactly as they would be after a process kill.

**A `Node` does this for you.** Cancelling a `Node`'s context is a *drain* request, not an abort: every
runner is told to stop claiming, each is drained against `NodeConfig.DrainTimeout`, the timeout warning
names how many jobs were still in flight, and only then is the scheduler and health server torn down.
A zero `DrainTimeout` waits for in-flight work however long it takes — genuinely unbounded, since the
heartbeat renews a running job's lease indefinitely. Size it to the longest drain your deployment will
tolerate.

</details>

<details>
<summary><strong><code>Leases, the heartbeat, and the fence</code></strong></summary>
<br>

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
```

Everything `Enqueue` guarantees for one row holds per row here: the same defaults, the same
`unique_key`/`unique_active_key` idempotency, the same `ErrAlreadyEnqueued` meaning — a collision is a
per-row skip, never a failed batch. `res.Skipped` counts the collisions; set `BatchOpts.SkipDuplicates`
to drop them silently instead. `InsertManyTyped[A]` is the generic form: it reads each job's kind from
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
restored retry budget, is its own guide: **Replaying work** below.

**The fan-in barrier runs a continuation once a generation completes.** A worker returns `Result.Barrier`
alongside its children, and the runtime enqueues the continuation exactly once, after the last child
reaches *any* terminal state — a half-failed generation still gets its finalizer. It is keyed so a
retried, discarded, or superseded child can never enqueue it twice, and it fires inside that last child's
finalize transaction, so no host writes a retry-unsafe counter.

```go
return flywheel.Result{
    FollowUps: children,                                  // each with Parent: true
    Barrier:   &flywheel.Barrier{Kind: "finalize_batch"}, // runs once, when they are all terminal
}, nil
```

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
// res.Changed replayed; res.SkippedTerminal left as they were; res.SkippedRunning left in flight.
```

**A replay restores the retry budget as headroom, not by rewinding the counter.** A job discarded at
`attempt == max_attempts` has no budget left, so a plain retry gives it exactly one more attempt before
it discards again. `ResetAttempts` raises `max_attempts` — to `attempt + Budget`, or the job's original
budget when `Budget` is zero — so the job gets a real second life. `attempt` is never lowered: it is the
`job_runs(job_id, attempt)` audit key, so the replay's runs continue the sequence rather than colliding
with the recorded failures. It is the same mechanism a snooze uses to stay free.

**A replay is bounded by construction.** Empty `States` replays discarded jobs only — never succeeded
work; naming `StateSucceeded` additionally requires `Force`. An unscoped `Replay` with neither `Kinds`
nor `FailedSince` is refused with `ErrReplayUnbounded` rather than replaying every discarded job in the
database by accident.

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
dependency at once. The placement is deterministic, so you can predict when the last job lands. It does
not cap how fast the cohort is claimed once each job is due — an actual claim-rate ceiling is a separate
capability.

**Retry an existing row; do not re-enqueue it under the same key.** Which mechanism recovers a unit of
work depends on the intent:

| Intent | Mechanism | Key |
|---|---|---|
| Re-run *this job row*, keeping its id, history, and audit trail | `RetryJobWithOptions` / `Replay*` | — |
| Enqueue a *new* job for the same logical unit, at most once ever | new `Insert` | `UniqueKey` — collides forever, terminal or not |
| Enqueue a *new* job for a subject that may run again later | new `Insert` | `UniqueActiveKey` — frees on a terminal state |

> A job enqueued with `UniqueKey` can never be re-enqueued: the key collides with the original row
> forever, terminal or not. That is the guarantee `UniqueKey` exists to provide. To run that unit of work
> again, retry the original job — its id, lineage, and audit trail are preserved. If the unit is expected
> to run again later, `UniqueActiveKey` is the key you want.

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
    return err // a real failure — driver error, unregistered kind
}
```

**What `RunUntilIdle` guarantees.** It returns `nil` **only when no job is in a non-terminal state** —
not merely when this runner found nothing to claim. A job another pool is still running, one this
runner has in flight in its own worker pool, or one waiting out a retry backoff all keep it looping.
That is why the deadline branch is separate: `context.DeadlineExceeded` means "budget spent", not
"something broke". A transient database error does not end it either — it backs off and retries — and
`flywheel.ErrRunnerStopped` is what it returns if `Stop` ended it before the queue drained.

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

</details>

<details>
<summary><strong><code>Observability, health, and metrics</code></strong></summary>
<br>

The runtime is self-diagnosing. The `Observer` seam ([observer.go](internal/core/observer.go)) reports every
attempt's lifecycle — claim, start, finish, retry, supersede — with no metrics dependency in the core,
and the [`observers/`](observers) package ships ready adapters that plug straight in:

- `observers.NewMetrics(rec)` translates events into a `MetricsRecorder` — a small four-method sink you back
  with Prometheus, OpenTelemetry, statsd, or CloudWatch (the core imports none of them).
- `observers.NewSlog(logger)` logs each event at debug level; `observers.NewMulti(...)` fans events
  out to several observers at once.

`SampleQueueHealth` ([health.go](internal/core/health.go)) reads a point-in-time gauge snapshot — depth by state,
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

</details>

<details>
<summary><strong><code>Retention and scheduler maintenance</code></strong></summary>
<br/>

The `Scheduler` runs three or four maintenance activities, each on its own goroutine and its own
cadence: the periodic tick, the lease sweep, and — when you enable them — a retention prune and a
queue-health heartbeat.

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

`flywheel prune --older-than 720h` does the same from the CLI.

**If your schema references `job_runs`.** The library declares no foreign key between `jobs` and
`job_runs`, but your schema may. A row of yours pointing at `job_runs.id` must be `ON DELETE SET NULL`
— or retention has to stay off, because a prune becomes a foreign-key event in a table the runtime
does not own. Within each batch the audit rows are deleted before their jobs, which is contractual
rather than incidental: under an `ON DELETE CASCADE` the reverse order lets the cascade do the work
silently.

Only terminal jobs are ever removed — succeeded, cancelled, discarded, and only those finalized before
the cutoff. Pending and running work is never touched, whatever its age.

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

Every run's stdout, stderr, and exit code are captured to the `job_runs` audit trail — inspect
them with `flywheel jobs inspect <id>`. Prefer to wire it from Go? The
[examples/local-tasks](examples/local-tasks) program registers the shell, python, and mage
workers and schedules one of each. See the [CLI README](cmd/flywheel/README.md) for every
command, the config reference, and the macOS launchd setup.

</details>

<br/>

## 📚 Documentation

- **API Reference** – Dive into the godocs at [pkg.go.dev/github.com/mrz1836/go-flywheel](https://pkg.go.dev/github.com/mrz1836/go-flywheel)
- **Contract** – The exactly-once guarantees and their limits in [`docs/CONTRACT.md`](docs/CONTRACT.md)
- **Cookbook** – Unique-key recipes for deduplication and side-effect correlation in [`docs/COOKBOOK.md`](docs/COOKBOOK.md)
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
