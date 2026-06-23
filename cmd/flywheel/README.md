# flywheel CLI & daemon

`flywheel` is the local daemon and operator CLI for the
[go-flywheel](../../README.md) job runtime. It runs a runner + scheduler over
SQLite or PostgreSQL from a `flywheel.yaml` file, turns declarative schedules into
durable cron replacements, and inspects and operates the queue.

It is a **nested Go module** (its own `go.mod`) so the heavier CLI dependencies
(cobra, yaml) never reach consumers of the root library.

## Install

```bash
go install github.com/mrz1836/go-flywheel/cmd/flywheel@latest
```

No CGO or C toolchain required — SQLite is the pure-Go `modernc` driver, so the
binary cross-compiles to every platform. You can also grab a prebuilt binary
from the releases page, or self-update an existing install with `flywheel update`.

## Quick start

```bash
# Write a config (see flywheel.example.yaml)
mkdir -p ~/.config/flywheel
cp flywheel.example.yaml ~/.config/flywheel/flywheel.yaml

flywheel doctor                       # validate config + database
flywheel migrate                      # stand up the schema
flywheel serve                        # run the runtime until Ctrl+C
```

## Commands

| Command | Purpose |
|---|---|
| `flywheel serve` | Run the runner + scheduler until SIGINT/SIGTERM (drains in-flight work) |
| `flywheel migrate` | Create or update the schema |
| `flywheel enqueue <kind> <json>` | Enqueue one job (`--queue --unique --priority --at`) |
| `flywheel jobs ls` | List recent jobs (`--state --kind --limit --json`) |
| `flywheel jobs inspect <id>` | Show a job and its run history |
| `flywheel jobs retry <id>` | Force a job back to available |
| `flywheel jobs cancel <id>` | Move a job to cancelled |
| `flywheel schedule ls` | List periodic schedules |
| `flywheel schedule add <slug> <kind>` | Add/update a schedule (`--cron \| --every`, `--args`) |
| `flywheel status` | Show queue health, schedules, and recent failures (`--json --watch`) |
| `flywheel doctor` | Validate config, check the database, print effective settings |

All commands take `--config <path>` (default `./flywheel.yaml`, else
`$XDG_CONFIG_HOME/flywheel/flywheel.yaml`).

## Replacing cron

Declare your jobs in `flywheel.yaml` (or with `flywheel schedule add`). Each run
is durable, retried with backoff, overlap-protected by the lease, and fully
recorded in the `job_runs` audit trail — strictly better than a crontab line.
Choose the worker that matches what you run locally:

| `worker:` | Runs | Key fields |
|---|---|---|
| `shell` | a `.sh` script file, or an inline snippet | `script` \| `inline`, `args`, `shell` (default `sh`) |
| `python` | a `.py` script, a `-m` module, or a `-c` snippet | `script` \| `module` \| `inline`, `args`, `interpreter` (default `python3`→`python`) |
| `mage` | magex / mage build targets | `targets` (required), `binary` (default `magex`), `dir` |
| `exec` | any binary or command | `command` (required), `args` |
| `http` | an HTTP request | `url` (required), `method`, `success_status` |

The command workers (`shell`, `python`, `mage`, `exec`) also accept `env`, `dir`,
and `timeout_seconds`, and inherit the host env named in `runtime.env_allowlist`.

```yaml
schedules:
  - slug: nightly-maintenance      # shell script (no executable bit needed)
    every: 24h
    worker: shell
    shell:
      script: /usr/local/bin/maintenance.sh
      timeout_seconds: 600

  - slug: hourly-sync              # python script (resolves python3, then python)
    cron: "0 * * * *"
    worker: python
    python:
      script: /opt/hermes/sync.py
      args: ["--since=1h"]

  - slug: repo-deps-update         # magex/mage targets (magex needs no magefile)
    every: 24h
    worker: mage
    mage:
      targets: ["deps:update"]     # e.g. ["test"], ["lint"], ["version:bump", "push=true"]
      dir: /Users/me/projects/my-repo
```

After a run, see its captured stdout/stderr and exit code with `flywheel jobs
inspect <id>`. A complete [`flywheel.example.yaml`](flywheel.example.yaml) with
every worker type ships alongside this README.

## Metrics & status

`flywheel status` prints an at-a-glance operator report — queue health (ready /
in-flight / **lag**), per-state counts, active schedules, and the last day's
failures — read straight from the database, so it works whether or not a daemon
is running:

```bash
flywheel status            # text report
flywheel status --json     # the same report as JSON
flywheel status --watch    # redraw on an interval until Ctrl+C
```

`serve` exposes telemetry when `runtime.metrics_addr` is set. The daemon then
serves Prometheus text at `/metrics` (per-attempt counters plus queue-health
gauges sampled per scrape) alongside `/healthz` and `/readyz`, and an optional
`runtime.health_sample_interval` logs a queue-health heartbeat on a cadence:

```yaml
runtime:
  metrics_addr: ":9090"          # expose /healthz, /readyz, /metrics; unset = off
  health_sample_interval: 30s    # log a queue-health heartbeat on this cadence; unset = off
```

```bash
curl localhost:9090/metrics      # flywheel_jobs_* counters + flywheel_queue_* gauges
```

## Run as a background daemon (macOS, launchd)

A per-user LaunchAgent template lives in [`dist/com.mrz1836.flywheel.plist`](dist/com.mrz1836.flywheel.plist):

```bash
cp dist/com.mrz1836.flywheel.plist ~/Library/LaunchAgents/
# edit the USERNAME paths inside, then:
launchctl load ~/Library/LaunchAgents/com.mrz1836.flywheel.plist
```

`serve` traps SIGTERM (launchctl's stop signal) and drains within `ExitTimeOut`;
`KeepAlive` restarts it on crash.

## SQLite hardening

The daemon opens SQLite in WAL mode with `busy_timeout=5000`,
`_txlock=immediate`, and a single writer connection — so `flywheel jobs ls` can
read while the daemon writes, and the serialized claim never deadlocks.
