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
       <a href="https://goreportcard.com/report/github.com/mrz1836/go-flywheel"><img src="https://goreportcard.com/badge/github.com/mrz1836/go-flywheel?style=flat-square" alt="Go Report"></a>
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
scripts and HTTP calls durably, with retries, backfill, and a full audit trail.

The runtime is built from focused, composable pieces:

- **Typed workers** — generic `Worker[A]` interface, registered by `Kind()` ([registry.go](registry.go))
- **One-call lifecycle** — `Node` runs N runners + the scheduler + an optional health/metrics server and drains cleanly on shutdown ([node.go](node.go))
- **Scheduler** — periodic / cron job enqueuing plus stuck-lease recovery; declare schedules in code with `UpsertPeriodic` ([scheduler.go](scheduler.go), [schedule.go](schedule.go))
- **Retries with backoff** — exponential backoff with jitter, overridable per worker ([runner.go](runner.go))
- **Worker timeouts** — per-job or per-kind execution deadlines that classify as a retryable timeout ([runner.go](runner.go))
- **Lease-based recovery** — orphaned, crashed jobs reclaimed via `leased_until` sweeps ([scheduler.go](scheduler.go))
- **Per-run audit** — append-only `job_runs` table records every attempt, outcome, timing, and cost ([read.go](read.go))
- **Observability built in** — a dependency-free `Observer` seam with ready-made metrics, slog, and Prometheus adapters, queue-health/lag inspection, a `/metrics` endpoint, and `flywheel status` ([observer.go](observer.go), [observers/](observers), [health.go](health.go))
- **Postgres + SQLite** — `FOR UPDATE SKIP LOCKED` and `BEGIN IMMEDIATE` drivers ([driver_postgres.go](driver_postgres.go), [driver_sqlite.go](driver_sqlite.go))
- **Free-form routing** — a `ExecutorClass` label routes jobs to executor pools; empty is the wildcard ([types.go](types.go))
- **Idempotent enqueue** — `jobs_unique_key` partial unique index dedupes work ([client.go](client.go))
- **Follow-up jobs (DAG)** — workers return child jobs that are enqueued atomically ([types.go](types.go))
- **Outbox pattern** — enqueue on the caller's own `*gorm.DB` transaction for exactly-once side effects ([client.go](client.go))
- **Generic workers** — ready-made `ExecWorker` (shell/binary) and `HTTPWorker` so cron jobs need no custom Go ([workers/](workers))

<br/>

### Schema setup

`go-flywheel` owns its three tables (`jobs`, `job_runs`, `job_periodics`) and ships them via a single exported entry point:

```go
import "github.com/mrz1836/go-flywheel"

if err := flywheel.Migrate(db); err != nil { // db is a *gorm.DB
    return err
}
```

`Migrate` runs `AutoMigrate` over the row structs and then reconciles the partial/unique indexes GORM cannot express from struct tags — including the correctness-bearing `jobs_unique_key` partial unique index that enforces enqueue idempotency. It is idempotent (`AutoMigrate` no-op + `CREATE INDEX IF NOT EXISTS`), so repeated calls are safe.

It supports two consumption modes:

- **Standalone** — call `Migrate(db)` against a bare SQLite or PostgreSQL database and the runtime stands up its own schema with no external migration tooling.
- **Embedded** — call `Migrate(db)` as one step of a host project's install/migration process.

> Only PostgreSQL and SQLite are supported, because both express the partial indexes the runtime relies on; `Migrate` returns an error for any other dialect rather than silently dropping idempotency. A host that prefers versioned SQL — e.g. an Atlas / `atlas-provider-gorm` flow — can point its loader at `flywheel.Models()` (the runtime's row structs as a single source of truth) and generate migrations from there instead of calling `Migrate`. The module takes **no** hard dependency on Atlas or any external migration tool.

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

### Local daemon & cron replacement

The [`flywheel` CLI](cmd/flywheel/README.md) runs the runtime as a local daemon over a SQLite file
(zero-ops) or Postgres, and replaces cron with durable scheduled jobs — no custom Go required:

```bash
go install github.com/mrz1836/go-flywheel/cmd/flywheel@latest

flywheel migrate   # stand up the schema
flywheel serve     # run runner + scheduler until Ctrl+C
flywheel jobs ls   # inspect the queue
```

Declare your shell jobs in `flywheel.yaml` — each run is retried, audited, and overlap-protected,
strictly better than a crontab line:

```yaml
schedules:
  - slug: nightly-maintenance
    every: 24h
    worker: exec
    exec:
      command: /usr/local/bin/maintenance.sh
      timeout_seconds: 600
  - slug: gateway-healthcheck
    cron: "*/5 * * * *"
    worker: http
    http:
      url: https://gateway.internal/healthz
```

See the [CLI README](cmd/flywheel/README.md) for every command, the config reference, and the
macOS launchd setup.

<br/>

### Observability

The runtime is self-diagnosing. The `Observer` seam ([observer.go](observer.go)) reports every
attempt's lifecycle — claim, start, finish, retry — with no metrics dependency in the core, and the
[`observers/`](observers) package ships ready adapters that plug straight in:

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
- **Benchmarks** – Check the latest numbers in the [benchmark results](#benchmark-results)
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

> Benchmarks for the runtime's hot paths (claim, finalize, sweep) are added as those paths are tuned.

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
