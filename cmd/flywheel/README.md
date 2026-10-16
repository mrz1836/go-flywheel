# flywheel CLI & daemon

`flywheel` is the local daemon and operator CLI for the
[go-flywheel](../../README.md) job runtime. It runs a runner + scheduler over
SQLite or PostgreSQL from a `flywheel.yaml` file, turns declarative schedules into
durable cron replacements, and inspects and operates the queue.

It lives in the single Go module rooted at the repository — there is no separate
`go.mod` here; the command and the library ship together.

## Install

Install the latest prebuilt release into `~/.local/bin` — a user-writable directory, so no
`sudo`, and `flywheel update` can self-update in place afterward.

No `curl … | sudo bash` here — we don't ask you to pipe a mystery script into your shell
and hope for the best. Every line below is in the open, and the download is checked against
the release's published SHA-256 checksums before anything lands on your `PATH`:

```bash
# Install the latest flywheel release into ~/.local/bin, verified against checksums.txt
VER=$(curl -fsSLI -o /dev/null -w '%{url_effective}' https://github.com/mrz1836/go-flywheel/releases/latest | sed 's#.*/v##')
OS=$(uname -s | tr '[:upper:]' '[:lower:]'); ARCH=$(uname -m | sed 's/x86_64/amd64/;s/aarch64/arm64/')
F="go-flywheel_${VER}_${OS}_${ARCH}.tar.gz"; U="https://github.com/mrz1836/go-flywheel/releases/download/v${VER}"
mkdir -p ~/.local/bin && cd "$(mktemp -d)" && curl -fsSLO "$U/$F" \
  && WANT=$(curl -fsSL "$U/go-flywheel_${VER}_checksums.txt" | awk -v f="$F" '$2==f{print $1}') \
  && GOT=$( { command -v sha256sum >/dev/null && sha256sum "$F" || shasum -a 256 "$F"; } | awk '{print $1}') \
  && [ -n "$WANT" ] && [ "$WANT" = "$GOT" ] \
  && tar -xzf "$F" -C ~/.local/bin flywheel
flywheel version
```

Prefer to grab a specific build yourself? Every release on the [releases page][releases]
ships `go-flywheel_<ver>_checksums.txt`, so you can verify it by hand with `shasum -a 256 -c`.
From here on, `flywheel update` keeps the binary current on its own (see [Updating](#updating)).

No CGO or C toolchain required — SQLite is the pure-Go `modernc` driver, so the binary
cross-compiles to every platform.

> **Skip `go install` for everyday use.** It lands the binary in `~/go/bin`, where the Go
> toolchain owns it, so `flywheel update` will refuse to replace it and you're back to
> updating by hand. Use the release binary above. (`go build ./cmd/flywheel` from a
> checkout is fine for development.)

[releases]: https://github.com/mrz1836/go-flywheel/releases

## Updating

Self-update is provided by [go-selfupdate](https://github.com/mrz1836/go-selfupdate).
`flywheel update` (alias `flywheel upgrade`) downloads the latest release, verifies its
SHA-256 checksum against the published `go-flywheel_<ver>_checksums.txt`, and atomically
replaces the running binary — nothing is written until the download has been verified.

| Flag | Effect |
|---|---|
| `--check` | Report whether a newer release is available without installing |
| `--force` | Reinstall the latest release even when it is not newer than the running build |
| `--verbose`, `-v` | Narrate each step (and print the release notes with `--check`) |

`flywheel update` replaces the binary **in place**, so where it lives matters. Two
conditions have to hold, and both fail loudly with a message that names the fix:

- **The install directory must be writable by you.** A binary in a root-owned location
  like `/usr/local/bin` fails with `install dir not writable` — updating it would need
  `sudo`. Install into a user-writable dir such as `~/.local/bin` instead.
- **The binary must not be owned by another installer.** A `go install` build (in the Go
  bin directory) or a Homebrew binary is **refused rather than overwritten** — replacing
  a file another tool believes it owns would break both. Update those the way they were
  installed: `go install …@latest`, or `brew upgrade flywheel`.

A plain binary in a user-writable `PATH` directory — the [Option B](#install) install —
updates cleanly, no `sudo`.

Every other command runs a passive, cached (24h) background check and prints a one-line
"a new version is available" notice — it never blocks or fails a command. The check is
skipped for a development build and can be silenced with `FLYWHEEL_NO_UPDATE_CHECK=1`
(or the shared `NO_UPDATE_CHECK` / `CI`). When a GitHub token is needed for rate limits
it is read from `FLYWHEEL_GITHUB_TOKEN`, then `GITHUB_TOKEN`, then `GH_TOKEN`.

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
| `flywheel serve` | Validate the config (a malformed schedule is refused before the database is touched), migrate (concurrent index builds on PostgreSQL; an up-to-date schema issues no DDL), disable the schedules the config does not name, then run the runner + scheduler until SIGINT/SIGTERM (drains in-flight work), applying the config's `schedules:` on start and re-creating a declared schedule deleted while it runs |
| `flywheel migrate` | Create or upgrade the schema, reporting what it added (`--concurrently --lock-timeout 5s` for a live PostgreSQL database) |
| `flywheel enqueue <kind> <json>` | Enqueue one job and print its id (`--queue --unique --priority --at`); a `--unique` collision fails with the id of the job holding the key. A kind no runner registers, a typo included, waits as `scheduled` for the runner's unknown-kind grace (15 minutes) before it is discarded |
| `flywheel jobs ls` | List jobs matching a filter, a page at a time: repeat `--state`, `--kind`, `--queue`, or `--tag` for any of several; `--created-after` and `--created-before` (RFC 3339); `--sort newest\|oldest\|finished\|scheduled` (newest by default; `finished` takes terminal `--state`s, `scheduled` waiting ones); `--limit` (50, at most 200). A page with another after it ends with `next: --cursor <cursor>`, which `--cursor` reads; `--json` prints the page (`items`, `next_cursor`) |
| `flywheel jobs count` | Count the jobs the same filter flags match, exactly up to `--cap` (10,000 by default, at most 100,000): it prints the count, the cap with a plus past it (`10000+`), and on PostgreSQL the planner's estimate beside it (`10000+ (estimate 1234567)`); `--json` prints `n`, `capped`, and `estimate` |
| `flywheel jobs running` | List running jobs, longest-running first, flagging any far slower than their kind's baseline (`--kind --queue --limit --json`) |
| `flywheel jobs inspect <id>` | Show a job in full (args bounded by `--max-args-bytes`, 64 KiB by default; metadata, keys, timeout, barrier, cancel pending) and its run history (`--json`) |
| `flywheel jobs history <id>` | Show who changed a job, and why, newest first: when (UTC), the action, the actor, the source, and the reason (`--limit` (50, at most 200); a page with another after it ends with `next: --cursor <cursor>`, which `--cursor` reads; `--json` prints the page) |
| `flywheel jobs retry <id>` | Return a job to available so it runs again (`--force` for a terminal job; `--allow-running` re-queues a running one, whose current attempt keeps running; `--reason`, recorded in its history). Without them, a running job is refused (`flywheel: job is running`), so its work never runs twice at once, and so is a terminal one (`flywheel: job has already reached a terminal state`) |
| `flywheel jobs cancel <id>` | Move a job to cancelled (refused once a job is terminal; `--reason`, recorded in its history) |
| `flywheel jobs reschedule <id>` | Move a waiting job (available, retryable, scheduled, or paused) to another time, keeping its state: `--at` (RFC 3339, required; a time in the past makes it due now) and `--reason`, recorded in its history. A running job is refused (`flywheel: job is running`), and so is a finished one (`flywheel: job has already reached a terminal state`) |
| `flywheel jobs priority <id> <priority>` | Give a waiting job another priority, at least 1; lower runs first (`--reason`, recorded in its history). The priority it already has changes nothing. Refused for a running or finished job, as `reschedule` is |
| `flywheel jobs pause <id>` | Hold an available, retryable, or scheduled job so no runner claims it until it is resumed; a paused job is left as it is (`--reason`, recorded in its history). Refused for a running or finished job, as `reschedule` is |
| `flywheel jobs resume <id>` | Return a paused job to available, keeping its scheduled time; any other waiting job is left as it is (`--reason`, recorded in its history). Refused for a running or finished job, as `reschedule` is |
| `flywheel jobs delete <id>` | Delete a finished job (succeeded, cancelled, or discarded) with its runs, as `prune` deletes them; its history stays, with the delete in it (`--reason`). A job that hasn't finished is refused (`flywheel: job has not reached a terminal state`): cancel it first |
| `flywheel schedule ls` | List periodic schedules, with whether each is active and paused (`--json`) |
| `flywheel schedule add <slug> <kind>` | Add/update a schedule (`--cron \| --every`, `--args`). A `--cron` that names no zone runs in the zone the scheduler evaluates it in: the daemon's local zone, as the database driver returns its times. Prefix it with `CRON_TZ=<zone>` to pin one, for example `--cron "CRON_TZ=UTC 0 9 * * *"` |
| `flywheel schedule rm <slug>` | Remove a schedule (warns when the config declares it: `serve` re-creates it; `--reason`, recorded in its history) |
| `flywheel schedule enable <slug>` / `disable <slug>` | Reactivate or deactivate a schedule, keeping its row (`--reason`, recorded in its history). This is the config's own switch: for a schedule the file declares, the next `serve` start re-applies the file's setting |
| `flywheel schedule pause <slug>` / `resume <slug>` | Hold a schedule through every `serve` start until it is resumed; a resume fires it next at its first fire time after now, without running the fires it missed |
| `flywheel kinds` | List the kind catalog a host's processes published with `PublishKinds`, sorted by kind: each kind's default queue, priority, attempts, and timeout (`-` for none), and when it was last published (`--json` prints the records, with each kind's description, args schema and example, and publisher). The CLI publishes no catalog of its own |
| `flywheel queues` | List every queue that holds an unfinished job or was ever paused: its ready, scheduled-ahead, running, and paused jobs, and whether the queue itself is paused, by whom, and why (`--json`) |
| `flywheel queues pause <queue>` / `resume <queue>` | Stop every runner from claiming a queue's jobs, or let them claim again, within one `QueueStateRefresh` (five seconds by default); jobs already running finish, and a queue no job has used can be paused (`--reason`, recorded in its history). Pausing a paused queue, or resuming one that isn't paused, changes nothing |
| `flywheel nodes` | List the nodes a host's processes registered (`RegistryInterval`), those that have not stopped first, newest first: each node's id, host, version, start, last heartbeat, state (`live`, `stale`, `draining`, `drained`, or `stopped`), and runners, each as its queues and its jobs in flight of its slots (`--stopped-since 24h` also lists those that stopped within that long; `--json`). The CLI registers no node of its own |
| `flywheel nodes drain <id>` | Ask a node to stop taking work: within one heartbeat it stops its runners' claims and reports `draining`, then `drained` once its jobs in flight finish, and keeps running until it is stopped (`--reason`, recorded in its history). A stopped or unknown node is refused (`flywheel: node not found`) |
| `flywheel prune` | Delete finished jobs and their runs older than a cutoff, holding for the stats rollup when it is on (`--older-than 14d --ignore-stats-rollup`) |
| `flywheel status` | Show queue health, schedules, and recent failures (`--json --watch`) |
| `flywheel stats` | Outcomes, jobs enqueued, success rate, duration percentiles, and queue wait, per kind, per queue, or per kind and queue (`--since 24h --kind --queue --by queue --json`) |
| `flywheel stats rebuild` | Backfill the finish log for runs an older release finalized, then recompute the hourly stats rollups for a range, rolling any hours between it and the rollup's progress too (`--from --to [--force]`) |
| `flywheel doctor` | Validate the config as `serve` does (failing on anything `serve` would refuse, such as a malformed schedule), migrate (as `serve` does), print effective settings, compare the declared schedules with the database (missing, inactive, differing, undeclared), and report the stats rollup's lag |

All commands take `--config <path>` (default `./flywheel.yaml`, else
`$XDG_CONFIG_HOME/flywheel/flywheel.yaml`).

The control commands — `jobs retry`, `cancel`, `reschedule`, `priority`, `pause`, `resume`, and `delete`,
and `schedule rm`, `enable`, and `disable` — run as the operating-system user (its name, else `$USER`) with the source `cli`, so `flywheel jobs history` and
`ListAudit` show who ran each one and the `--reason` they gave.

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

**The file is the source of truth.** Every `serve` start applies the
`schedules:` list — an unchanged schedule keeps its cadence, a changed one is
updated, and one an operator disabled is enabled again — and disables any active
schedule the file does not name, keeping its row and history. A schedule paused
with `flywheel schedule pause` stays paused: `serve` applies the file's kind,
args, and cadence to it and logs `jobs: declared periodic is paused`. While `serve`
runs, a declared schedule whose row is deleted (`flywheel schedule rm`, a
restore, a rebuilt database) comes back within `runtime.schedule_reconcile`,
with a `jobs: declared periodic was missing and has been re-created` warning:

```yaml
runtime:
  schedule_reconcile: 1m         # unset = 1m; negative (e.g. -1s) = off
```

To retire a schedule, remove it from the file first and restart `serve`; delete
the row afterwards if you want it gone. `flywheel schedule rm` on a slug the
config still declares warns that `serve` will bring it back, and `flywheel
schedule disable` holds only until the next start: to stop a declared schedule
until further notice, `flywheel schedule pause` it, and `flywheel schedule
resume` it later. A schedule `serve` would
refuse — a malformed cron, or an interval under a second — stops `serve`
before it touches the database, and fails `flywheel doctor` too.
Otherwise `flywheel doctor` shows which declared schedules are missing, inactive,
or different in the database, and which active ones the file does not declare,
without failing: `serve` repairs all of it on start.

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

Each heartbeat is one `jobs: queue health` log line with `ready`, `inflight`,
`scheduled_ahead`, `oldest_ready` (the lag as a duration string, e.g. `1m30.5s`),
`oldest_ready_seconds` (the same lag as a number, `0` when nothing is ready),
`discarded`, and — with the stats rollup on — `slow_running`, the number of
running jobs far slower than their kind's baseline. Alarm on
`oldest_ready_seconds` when you derive metrics from logs rather than scraping
`/metrics`. The heartbeat logs at info through `serve`'s logger, like every
scheduler line, so it follows `log.level` and `log.format`: `log.level: warn`
hides it.

## Job statistics

`serve` rolls finished runs into hourly statistics on the `runtime.stats_rollup`
cadence (on by default, every minute), so `flywheel stats` answers for any window
quickly and trends survive `runtime.retention`:

```yaml
runtime:
  stats_rollup: 1m               # unset = 1m; negative (e.g. -1s) = off
```

```bash
flywheel stats                   # last 24h: per-kind attempts, jobs enqueued, success rate, p50/p95/p99, queue wait
flywheel stats --by queue        # one row per queue; --by kind_queue for one per kind on each queue
flywheel stats --since 7d --kind exec
flywheel stats --json            # the full result, including the slowest run per kind
flywheel jobs running            # what is running now, and whether it is far slower than usual
flywheel stats rebuild --from 2026-06-01 --to 2026-06-08   # backfill or repair a range
```

With the rollup on, `serve` also logs one `jobs: stats anomaly` warning the hour
a kind's successful runs become markedly slower than its 7-day baseline, or its
jobs start failing at markedly more than their usual rate, and one warning the
first time a running job crosses its kind's slow line. Without the rollup,
`flywheel stats` reads raw runs and refuses windows longer than 7 days.
`flywheel doctor` reports how far the rollup trails now.

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
