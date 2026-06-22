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
| `flywheel doctor` | Validate config, check the database, print effective settings |

All commands take `--config <path>` (default `./flywheel.yaml`, else
`$XDG_CONFIG_HOME/flywheel/flywheel.yaml`).

## Replacing cron

Declare your shell jobs as `exec` schedules in `flywheel.yaml` (or with `flywheel
schedule add`). Each run is durable, retried with backoff, overlap-protected by
the lease, and fully recorded in the `job_runs` audit trail — strictly better
than a crontab line. `http` schedules call a URL on the same terms.

```yaml
schedules:
  - slug: nightly-maintenance
    every: 24h
    worker: exec
    exec:
      command: /usr/local/bin/maintenance.sh
      timeout_seconds: 600
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
