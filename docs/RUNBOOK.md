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
| `flywheel_queue_jobs{state}` | gauge | How many jobs sit in each state? Every state but `succeeded`, which is history rather than queue (`Overview` counts it). |
| `flywheel_jobs_claimed_total` | counter | Total jobs claimed. |
| `flywheel_jobs_finished_total{outcome}` | counter | State-advancing finalizations by outcome. |
| `flywheel_jobs_retried_total` | counter | Scheduled retries. |
| `flywheel_jobs_superseded_total` | counter | Attempts executed and discarded — the double-execution signal. |
| `flywheel_sweep_discarded_total` | counter | Jobs the lease sweep discarded: a crash on the job's last attempt, or its `MaxCrashes`-th crash. A steady rate is a job taking its process down. |
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
past, in bounded batches, on its sweep cadence (30 seconds by default). The stale run stub of each
reclaimed attempt — the `job_runs` row committed before the worker body ran — is marked with outcome
`crashed`, so a crash leaves an audit record rather than a hole. A crash spends the attempt it
interrupted: a job whose crashed attempt was its last (`attempt >= max_attempts`), or, with
`DriverOpts.MaxCrashes` set, whose crash was its `MaxCrashes`-th, is discarded rather than returned to
the queue, and its stub records `job_state = 'discarded'` (`available` for a job that runs again).

**A job that keeps killing its worker ends discarded.** A worker that takes its whole process down — out
of memory, a fatal runtime error, a panic on a goroutine it started, `os.Exit` — crashes every attempt.
Each one is reclaimed and claimed again until the job's attempts are spent, and the sweep then discards
it. With `observers.NewSlog` wired into the scheduler, every crashed attempt logs
`flywheel: job attempt failed` at warn, and the last one `flywheel: job failed and was discarded` at
error, each with `outcome` `crashed` (next section). A job with a large `MaxAttempts` takes the process
down that many times first. Bound it with `DriverOpts.MaxCrashes` on the scheduler's driver: the sweep
then also discards a job on its `MaxCrashes`-th crash, counted over the job's life, whatever attempts it
has left. Then fix the worker. `flywheel_sweep_discarded_total` counts every job the sweep discarded.

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

## Symptom: jobs fail or are discarded

A worker that returns an error fails its attempt. A transient or timeout error retries on the backoff
ladder until the job's `MaxAttempts` is spent; a permanent or validation error, or a failure on the
last attempt, discards the job, and its work is lost until it is replayed (`flywheel.Replay`).
`flywheel.RecentFailures` and `flywheel status` list what was discarded recently and why, and
`job_runs.error_message` holds every attempt's error.

**Log lines.** With `observers.NewSlog` wired as the runner's observer, a failure never waits for
debug logging:

| Line | Level | Meaning |
|---|---|---|
| `flywheel: job attempt failed` | warn | an attempt failed and the job will retry — fields `job_id`, `run_id`, `kind`, `queue`, `outcome` (`error`, `timeout`, or `crashed`), `attempt`, `max_attempts`, `job_state` (`retryable`, or `available` for a crash), `error_class`, `error`, `duration` |
| `flywheel: job failed and was discarded` | error | the failure ended the job: its work is lost until it is replayed — the same fields, with `job_state` `discarded` |

**A crash logs the same two lines, from the sweep.** A crashed attempt never returned, so the scheduler's
lease sweep logs it when it reclaims the job, just before its `flywheel: lease sweep completed` line, with
`outcome` `crashed` and no `error_class`, `error`, or `duration`: there was no worker result to report.
The `run_id` is the crashed run, empty when the process died before writing it. These lines need the
slog observer passed to the scheduler (`SchedulerConfig.Observer`) as well as the runner.

**A warn now and then is the retry ladder doing its job.** Repeated warns for one `kind` with the same
`error` mean the downstream keeps refusing, and each of those jobs ends in a discard once its budget is
spent: fix the cause while they are still retrying.

**Every error line is lost work.** Read its `error` and `error_class`, fix the cause, then replay the
job. A job of a kind no runner registers is discarded the same way and also logs the runner's own line
(next section).

**The error text is logged as the worker returned it.** Keep secrets and personal data out of worker
errors, or wrap them before returning.

<br>

## Symptom: jobs discarded as `jobs: decode args`

A job whose stored args do not fit its worker's args type fails before the worker runs, with
`jobs: decode args for kind "<kind>": <decoder error>` and error class `validation`, and it is discarded
on that first attempt: the bytes never change, so a retry could not help, and the worker's `Classifier`
is not consulted. The usual causes are a producer bug, a field whose type changed between releases, or a
hand-written payload passed to `Enqueue`. Fix the producer or the args type, then replay the discarded
jobs (`flywheel.Replay` with the kind). Callers can match the error with `errors.Is(err,
flywheel.ErrDecodeArgs)`.

<br>

## Symptom: a cancelled job is still running

`CancelJob` (and `flywheel jobs cancel`) makes a job `cancelled` at once, but by default a worker already
running keeps going until it returns: the cancel fences the attempt's result and leaves its worker running.
Until it returns, the job reads `cancelled` while its run at the job's attempt still reads outcome `started`
(`flywheel jobs inspect <id>`, or `FindJob` with `LatestRun`), and `GetJob` reports `CancelPending`. That is a
cancel still waiting on its attempt.

- **To have a cancel stop such workers, set `RunnerConfig.CooperativeCancel`** on the runners that run them.
  The first renewal after a cancel then cancels the attempt's context with the cause `ErrJobCancelled`, and
  logs `jobs: job cancelled; cancelling its running attempt` at info. A worker that returns on it is recorded
  as a `cancelled`, superseded attempt with no error class.
- **With the setting on, a run still `started` long after the cancel** belongs to a worker that ignores its
  context: make it return once `ctx` is done. A runner logging `jobs: job state read failed after a lost
  claim; retrying at the next heartbeat` cannot read the job's state, and keeps trying at every heartbeat.
- **A run that never finishes** belongs to a process that died before its attempt returned: the lease sweep
  reclaims only running jobs, so nothing will finish that run. Check the run's `executor_id` against your
  live processes. The job stays cancelled; to run it again, see the next section.

<br>

## Symptom: a retry is refused with `flywheel: job is running`

`RetryJob` (and `flywheel jobs retry`) refuses a job whose attempt is still running, with
`ErrJobRunning`, and leaves it as it is: re-queueing it would start a second copy beside the first. With
`RetryOpts.Force`, the same refusal covers a job cancelled while its attempt was running, because a
cancel fences the attempt without awaiting its worker, and the attempt's run reads `started` until the
worker returns. Wait for the attempt to finish (`flywheel jobs inspect <id>` shows its run). If the process
that ran it is gone — a cancelled job's process died before its attempt returned, so nothing will ever
finish that run — retry it with `RetryOpts{Force: true, AllowRunning: true}`, or
`flywheel jobs retry <id> --force --allow-running`, which re-queues it regardless. If the re-queued job's
new attempt then crashes, the sweep crashes only the run of the attempt it reclaimed: the older
attempt's run stays `started` until it returns, and for good when its process is gone.

<br>

## Who changed this job, and why

Every operator control records who ran it, in the job's history: a retry, a cancel, a pause or resume of
its parent's batch, a replay, a reschedule, a priority change, a pause, resume, or delete of the job itself,
and, for a schedule, a deactivate, activate, or delete. Read it with:

```bash
flywheel jobs history <job-id>             # WHEN (UTC), ACTION, ACTOR, SOURCE, REASON — newest first
flywheel jobs history <job-id> --json      # the entries in full, details included
```

or `ListAudit(ctx, db, ListAuditParams{TargetType: AuditTargetJob, TargetID: id})` in code (a schedule's
history is `AuditTargetPeriodic` and its slug). An entry holds when the control ran, what it did, the actor
(`user`, `agent`, or `system` — `system` when the caller set none with `WithActor`), the source (`cli` for
the CLI's own commands, which take `--reason` and record the operating-system user), the reason, the
request id, and details: the state the job changed from, and the options the call used. The history
outlives the job, so a job retention has pruned still has one.

**A control fails with a missing `job_audit` relation.** Every control writes the table, so this release
was deployed before its migration ran. Migrate (`flywheel migrate`, or the statements in
[`INTEGRATING.md`](INTEGRATING.md#upgrading-an-existing-database) for a host-owned schema), then run the
control again: the failed call changed nothing. A Scheduler with `AuditRetention` set refuses to start
with `ErrSchemaOutdated` naming `job_audit (table)` for the same reason.

**The history grows by one entry per changed job.** Set `SchedulerConfig.AuditRetention` to bound it (see
[Bounded retention](#bounded-retention)).

<br>

## Holding, moving, or deleting one job

Five controls change one job in place, by its id, and keep its id and its history:

```bash
flywheel jobs reschedule <job-id> --at 2026-01-02T09:00:00Z --reason "after the vendor window"
flywheel jobs priority <job-id> 10 --reason "customer waiting"   # at least 1; lower runs first
flywheel jobs pause <job-id> --reason "hold for review"          # no runner claims it
flywheel jobs resume <job-id> --reason "reviewed"                # available again, at its scheduled time
flywheel jobs delete <job-id> --reason "a test job"              # a finished job, with its runs
```

In code they are `RescheduleJob`, `SetJobPriority`, `PauseJob`, `ResumeJob`, and `DeleteJob`. Each one that
changes the job records an entry in its history (`flywheel jobs history <job-id>`). One that finds nothing
to change — pausing a paused job, resuming one that isn't paused, the priority it already has — changes
nothing and records nothing.

- **Refused with `flywheel: job is running` (`ErrJobRunning`).** None of them touches a running job: its row
  belongs to the attempt in flight. Wait for the attempt to finish, or cancel the job
  (`flywheel jobs cancel`), after which it can be retried or deleted.
- **Refused with `flywheel: job has already reached a terminal state` (`ErrJobTerminal`).** The job has
  finished, so there is nothing to move, reprioritize, hold, or release. Retry it with `--force` to run it
  again, or delete it.
- **`delete` refused with `flywheel: job has not reached a terminal state` (`ErrJobNotTerminal`).** Only a
  finished job is deleted. Cancel it first, then delete it.
- **A resumed job isn't claimed yet.** `resume` keeps the job's `scheduled_at`, so a job paused while
  deferred runs at its time, not at once. Reschedule it to the current time to run it now.
- **What a delete removes.** The job, its runs, and their finish-log entries, in the order `flywheel prune`
  deletes them. Its history stays, with the delete in it. Its children keep their `parent_job_id`, and a
  fan-in barrier it declared and had not fired goes with it, so that barrier never fires.

<br>

## A large recovery

When an outage discards thousands of jobs, replay them with a previewed bulk retry: count what the filter
matches, then apply exactly that, spread over a window so the recovered dependency is not hit at once.

**1. Preview.** A dry run changes nothing and reports what the retry would do:

```go
ctx = flywheel.WithActor(ctx, flywheel.Actor{Type: flywheel.ActorUser, ID: operator, Reason: "replay the payments outage"})
spec := flywheel.BulkSpec{
    Query: flywheel.JobQuery{
        Kinds:         []string{"charge_card"},
        States:        []flywheel.JobState{flywheel.StateDiscarded},
        FinalizedFrom: outageStart,
        FinalizedTo:   outageEnd,
        ErrorClasses:  []flywheel.ErrorClass{flywheel.ErrorTransient},
    },
    Action: flywheel.BulkRetry,
    Retry:  flywheel.RetryOpts{Force: true, ResetAttempts: true, Budget: 3},
    DryRun: true,
}
preview, err := flywheel.BulkJobs(ctx, db, spec)
```

- **`Matched` and `Changed`** are how many jobs the filter matched and how many the retry would return to
  `available`; `ByState`, `ByKind`, and the `Sample` show what they are. Check them before going on.
- **`Skipped`** names what the retry leaves alone: `running` jobs are still in flight, a cancelled one
  among them while its cancelled attempt still runs; `active_key` jobs share a `UniqueActiveKey` that
  another live job holds, and run once that job finishes.
- **`MatchedCapped`** means more jobs matched than `MaxJobs` (10,000 by default). Narrow the filter, a
  tighter window or one kind, rather than raising `MaxJobs` toward its 100,000 ceiling: a smaller
  operation is easier to check and to undo.
- **A slow preview** is a filter rare along the id order. A dry run reads every match, and without the
  explorer index for its field (`ExplorerIndexSet`; `jobs_explore_kind` for a kind) it walks the primary key
  to find them: at a million jobs, 1.2 s without the kind index and 0.25 s with it for a 1% kind
  ([measured](BENCHMARKS.md#bulk-controls-by-filter-a-dry-run-and-an-apply-at-1000000-jobs)).

**2. Apply, pinned to the preview.**

```go
spec.DryRun = false
spec.MaxJobs = int(preview.Matched) // never more than the preview counted
spec.MaxID = preview.ResumeAfter    // never a job enqueued since
spec.OperationID = incidentID       // on every entry the run writes
spec.Stagger = 15 * time.Minute     // the cohort arrives over fifteen minutes
res, err := flywheel.BulkJobs(ctx, db, spec)
```

- **`ErrBulkTooLarge`, nothing changed.** The selection grew since the preview: a job at or below the
  ceiling moved into the filter. Preview again, and apply what the new preview counts.
- **`ErrBulkTooLarge`, some changed.** The selection grew past `MaxJobs` while the run walked it. The
  batches it committed stay, and `res.ResumeAfter` is where it stopped: preview again with `After` set to
  it, then apply the rest the same way.
- **Each retried job records the operation.** Its history holds a `retried` entry with `via: bulk` and the
  `operation_id`, under the operator who ran it (`flywheel jobs history <job-id>`; `ListAudit` with
  `Actions: []AuditAction{AuditRetried}` reads them across jobs).
- **Locks.** Each batch locks its jobs until it commits, 30–70 ms at the default 500, and a finalize of
  one of them waits that long. Lower `BatchSize` if that matters more than the run's length.

Without `Retry.Force` a bulk retry changes only waiting jobs, and every finished job is skipped as
`terminal`. `ReplayByParent` replays one parent's children instead, and `Replay` a kind or a failure
window with no preview.

<br>

## Resuming an interrupted bulk run

A `BulkJobs` call whose context ends between batches, on a deploy, a timeout, or a cancelled request,
returns what it did so far with an error wrapping the context's:

```
flywheel: bulk retry cancelled after 3000 changed: context deadline exceeded
```

Its committed batches stay, each with its entries; the batch in flight rolled back whole, entries and all.
`Done` is false, and `ResumeAfter` is the last job id the run passed.

- **Continue with the same spec and `After` set to `ResumeAfter`.** The run picks up past that id, and no
  job is repeated or skipped. Keep `MaxID` as it was: the resumed run counts only the jobs past `After`.
- **Keep `ResumeAfter` where the next attempt can read it.** The call returns it only when it returns, so
  the process running the operation records it with the operation id before it exits.
- **Any failure returns the same partial result.** A database error or an audit hook's error ends the run
  the same way, `Done` false and `ResumeAfter` the last committed batch's end: fix the cause, then resume.
- **`Done` true** means the walk reached the end of the selection, and nothing is left to do.

<br>

## Pausing a queue

Pause a queue when what its jobs call is down — a provider outage, a downstream migration — and its jobs
should wait rather than spend their retries:

```bash
flywheel queues pause mail --reason "provider outage"   # every runner stops claiming it
flywheel queues                                         # PAUSED yes, with BY and REASON
flywheel queues resume mail --reason "provider is back"  # claimed again
```

In code they are `PauseQueue`, `ResumeQueue`, and `ListQueues`. Each pause and resume records who ran it
and why in the history (`ListAudit` with `AuditTargetQueue` and the queue's name). Pausing a paused queue,
or resuming one that isn't paused, changes nothing and records nothing. A queue no job has used yet can be
paused, and stays paused for the jobs that arrive.

- **What stops, and when.** Every runner reads the paused set once per `RunnerConfig.QueueStateRefresh`
  (five seconds by default) and leaves a paused queue out of its claims, so a pause holds on every runner
  within one refresh, and a resume takes as long. A runner whose every queue is paused claims nothing and
  holds no slot and no limiter permit.
- **What keeps running.** An attempt already running on the queue finishes: a pause holds the queue, it
  does not drain it. The queue's jobs keep their states, and its depth grows as producers enqueue. To hold
  one job, use `flywheel jobs pause`; to stop a running one, `flywheel jobs cancel`.
- **`RunUntilIdle` waits.** A paused queue's jobs are unfinished work, so `RunUntilIdle` waits for the
  resume while the queue holds jobs. It returns once nothing is pending anywhere.
- **A runner logs `jobs: queue state refresh failed; keeping the last paused set`.** It could not read
  `job_queues`, so it keeps the paused set it last read, and warns once per run of failures; a runner that
  has never read the set claims every queue. The read recovers on its own once the database answers.
- **A runner refuses to start with `ErrSchemaOutdated` naming `job_queues (table)`.** The release was
  deployed before its migration. Migrate (`flywheel migrate`, or the statements in
  [`INTEGRATING.md`](INTEGRATING.md#upgrading-an-existing-database) for a host-owned schema). A host that
  cannot migrate first sets `QueueStateRefresh` negative, which turns the check off, and queue pause with
  it.
- **During a rolling deploy** a runner of a release without queue pause still claims a paused queue's
  jobs. Pause a queue once the deploy has replaced those runners.

<br>

## Lowering a limit during an incident

When a vendor throttles you or a downstream is drowning, lower the limit rather than stopping the work. A
shared `DBLimiter`'s budget and a queue's concurrency both change at runtime, with no deploy:

```bash
flywheel limits                                                    # each limiter's tokens, holds, and overrides
flywheel limits set provider:x --max-concurrent 2 --reason "vendor throttling"
flywheel queues concurrency exports 3 --reason "downstream drowning"
```

In code they are `SetLimiterBudget` and `SetQueueConcurrency`; each change records one `limits_set` entry in
the history (`ListAudit` with `AuditTargetLimiter` and the resource, or `AuditTargetQueue` and the queue),
with the limit before and after.

- **Which lever.** The limiter is the fleet-wide gate: an override applies to every process sharing the
  database, on its next `Acquire`. A queue's cap applies to each runner serving the queue on its own, so ten
  runners at a cap of 2 still run twenty jobs; reach for it when one runner's slots are the problem.
- **How fast each lands.** An override binds the next `Acquire` against the resource. A queue cap binds
  each runner within one `RunnerConfig.QueueStateRefresh` (five seconds by default), even while every slot
  is busy. A runner with `QueueStateRefresh` negative never reads a cap.
- **Why nothing running stops.** A lower limit holds back new work only. A `--max-concurrent` below what
  is held grants nothing until enough holds are released or expire; a queue cap below what a runner runs
  claims nothing until its running jobs finish. To stop a job, cancel it; to stop a queue, pause it. An
  override is 1 or more: zero is refused, since in a limiter's config zero means "no cap".
- **What can be overridden.** `--rate` applies only to a limiter configured with an `Interval`, and
  `--max-concurrent` only to one configured with a `HoldTTL`; `--burst` always applies. The resource must
  appear in `flywheel limits`: a limiter creates its row on its first `Acquire`.
- **Clear it when the incident ends.** An override outlives the incident until someone clears it:
  `flywheel limits set provider:x --max-concurrent none` (each flag left out keeps its override), and
  `flywheel queues concurrency exports none`. `flywheel limits` and `flywheel queues --json`
  (`max_concurrency`) show what is still set, and the history shows who set it.

<br>

## Draining a node

Drain a node to retire a host, or to take a bad build out of service, without killing its jobs mid-run.
The node must register itself (`NodeConfig.RegistryInterval`):

```bash
flywheel nodes                                          # its ID, among the nodes that have not stopped
flywheel nodes drain <node-id> --reason "retire host"
flywheel nodes                                          # STATE draining, then drained
```

In code it is `RequestDrain`, recorded in the history (`ListAudit` with `AuditTargetNode` and the node's
id). Within one heartbeat the node stops every runner's claims and reports `draining`; once its jobs in
flight finish it reports `drained`. It keeps running — its health server answers, its Scheduler keeps
ticking, and it keeps heartbeating — until you stop it. A stopped runner cannot restart, so a drained node
takes work again only after a restart. A drain already requested changes nothing; a stopped or unknown
node is refused with `flywheel: node not found` (`ErrNodeNotFound`).

A node with `RegistryInterval` set refuses to start with `ErrSchemaOutdated` naming `job_nodes (table)`
until the table exists: migrate first, as for `job_queues` above.

<br>

## Symptom: a node is stale

`flywheel nodes` shows `stale` (`NodeInfo.Stale`) for a node that has not stopped and has not heartbeated
within `ListNodesParams.StaleAfter`, a minute by default:

- **The process died.** A killed or crashed process writes no stop time, so its row stays as it last
  heartbeated. The lease sweep reclaims its jobs in flight, as after any crash. Nothing else is needed:
  retention prunes the row, below.
- **The heartbeat stalled.** A node logs `flywheel: node registry heartbeat failed` when a heartbeat
  write fails, and tries again at the next beat; while its database is out of reach it reads stale. Its
  runners keep working meanwhile: the registry never stops a node.
- **The interval is too long.** A `RegistryInterval` at or above `StaleAfter` makes a healthy node read
  stale between beats. Keep it well below; see [TUNING](TUNING.md#queue-pause-and-the-node-registry).
- **The clocks disagree.** A node stamps its heartbeat by its own clock, and `ListNodes` judges it by the
  reader's, so a reader whose clock runs ahead of the node's by more than `StaleAfter` sees it stale.

**How long node rows are kept.** A registering Node that runs a Scheduler deletes the rows of nodes whose
last heartbeat is older than `SchedulerConfig.NodeRetention` (7 days by default), at start and then
hourly. A Node without a Scheduler prunes nothing, so the setting on the node that runs your Scheduler
governs the fleet. A stale node stays listed until then; `flywheel nodes --stopped-since 24h` also lists
the nodes that stopped in the last day, as far back as retention keeps them.

<br>

## Symptom: jobs deferred or discarded as `jobs: unknown job kind`

A runner that claims a job of a kind its `Registry` does not register runs nothing. While the job is
younger than `RunnerConfig.UnknownKindGrace` (15 minutes from its enqueue by default) it defers the job,
a snooze that spends none of its retry budget, for a runner that does register the kind; past the window
it discards the job. Each deferral is due again after one second, doubling per claim up to a minute, so a
job no runner registers writes at most 21 `job_runs` rows over the default window. The README's
**Rolling deploys and new job kinds** covers the mechanism and how to ship a new kind.

**Log lines.**

| Line | Level | Meaning |
|---|---|---|
| `jobs: deferring jobs of a kind this runner does not register` | warn | this runner handed a job back, logged once per kind per runner, and again if the kind is still being deferred once the grace has passed since — fields `kind`, `job_id`, `queue`, `grace`, and `discard_at`, when that job is discarded if nothing runs it first |
| `jobs: discarded a job of a kind this runner does not register` | error | a job outlived the window, or the grace is disabled, and was discarded: enqueued work lost — fields `kind`, `job_id`, `queue`, `attempt`, `grace`, `age` |

**The warn during a deploy is expected.** A runner of the old release is handing back work for the new
one. Nothing is needed unless discards follow.

**Deferrals that persist past the deploy, or any discard, mean no runner registers the kind.** Check
that the worker is `Register`ed in the release that should run it; that a runner of that release polls
the job's queue and serves its executor class (or sets `ClaimAnyClass`); and that the release was not
rolled back. A mistyped kind from `flywheel enqueue` lands here too.

**Find the deferrals in a window.** Go through the finish log, since `job_runs.finished_at` is not
indexed:

```sql
SELECT r.kind, r.queue, count(*) AS deferrals, count(DISTINCT r.job_id) AS jobs
FROM job_run_finishes f JOIN job_runs r ON r.id = f.run_id
WHERE f.finished_at >= $1 AND f.finished_at < $2
  AND r.outcome = 'snooze' AND r.error_message = 'jobs: unknown job kind'
GROUP BY r.kind, r.queue
ORDER BY deferrals DESC;
```

A deferral is otherwise an ordinary snooze: it counts in `flywheel_jobs_finished_total{outcome="snooze"}`,
and the deferred job in `flywheel_queue_jobs{state="scheduled"}`, not as ready work. `CountActiveByKind`
shows how many jobs of the kind are still waiting. A discarded job shows in `flywheel status` and
`RecentFailures` with the error `jobs: unknown job kind`.

**Recovery.** Fix the cause, then replay what was discarded. A plain replay is enough: deferrals spend no
attempts, so a discarded job keeps all of its headroom but the one attempt its discard used, and
`ResetAttempts` with no `Budget` would add `max_attempts`, which every deferral raised, on top.

```go
res, err := flywheel.Replay(ctx, db, flywheel.ReplayOpts{
    Kinds:       []string{"the_kind"},
    FailedSince: incidentStart,
})
```

Replay only once a runner that registers the kind polls its queue. A replayed job keeps its enqueue time,
so it is already past its window, and a runner without the kind discards it again at once.

<br>

## Bounded retention

Terminal jobs (and their `job_runs`) accumulate forever unless retention is enabled. Without it the
tables grow without bound.

**Enabling it.** Set `SchedulerConfig.RetentionMaxAge` to the age past which a *finalized* job may be
deleted. Zero (the default) disables retention entirely — no surprise deletes for a consumer that never
asked for them. The prune runs on `RetentionInterval` (one hour by default) in bounded batches.

Retention keeps the control history: a pruned job's `job_audit` entries stay, and
`SchedulerConfig.AuditRetention` gives them a window of their own (zero keeps every entry); set alone, it
turns the retention pass on for the history only.

**Choosing the window.** Retention deletes jobs finalized *longer ago than* the window — not jobs
created that long ago. The window is a function of how long you need the audit trail (`job_runs`) and
the terminal job rows for debugging, replay, or compliance, not of throughput. A 30-day window keeps a
month of history; a 2-minute window (used in load tests) keeps almost nothing. The window also bounds
`UniqueKey`: a key collides only with a job whose row exists, so pruning a job frees its key, and a later
insert with that key runs the work again. A key that must refuse duplicates for longer than the window
has to be held where retention does not prune (see [`COOKBOOK.md`](COOKBOOK.md), recipe 1).

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

## A truncated output

A run whose `output` reads `{"$truncated": true, "$bytes": …, "$head": "…"}` is an attempt whose worker
returned a `Result.Output` larger than the driver's `DriverOpts.MaxOutputBytes` (64 KiB of JSON by
default). The attempt finalized as it would have; only its record was cut. `$bytes` is the output's
marshaled size, and `$head` its first 1,024 bytes, a preview that usually ends mid-value.
`JobRunView.OutputTruncated` (`output_truncated`) is true for such a run, and `ChildOutputs` hands the
marker to a barrier continuation as that child's output.

- **A reader needs the result whole** (a continuation folds it, a dashboard shows it): raise
  `MaxOutputBytes` on the driver every runner and the scheduler share, or store the result in a table of
  your own and return its id. A raised cap applies to attempts finalized after it; an earlier attempt
  keeps its marker.
- **A command worker's captures.** The bundled command workers capture up to 64 KiB of stdout and of
  stderr, so an `ExecOutput` with both streams near that cap is over the default. Lower the worker's own
  `MaxOutputBytes`, or raise the driver's.

The error side has the same signals. `error_payload.truncated` (`JobRunView.ErrorTruncated`) means the
message was longer than 4,096 bytes, and `error_payload.message_bytes` says how long. A worker panic's
run keeps `error_payload.stack` (`JobRunView.ErrorStack`), the stack at the panic, while its
`error_message` stays `jobs: worker panicked: <value>`.

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

**Stopping a declared schedule.** Pause it, and say why: `PausePeriodic` (`flywheel schedule pause <slug>
--reason "provider outage"`) holds it through every restart, because no start, reconcile, or
`UpsertPeriodic` writes the pause. The pause records who paused it and the reason, which `flywheel
schedule ls` shows beside it, and writes a `paused` entry to the control history. Each start still applies
the declaration's kind, queue, args, and cadence to it, and says it found the schedule paused.
`ResumePeriodic` (`flywheel schedule resume <slug> --reason …`) releases it at its first fire time after
now; the fires it missed while paused are not run. `SetPeriodicActive(false)` (`flywheel schedule
disable`) is the declaration's switch instead: it holds while the scheduler runs — the reconcile only
inserts, and never re-activates — but the next start re-applies the declaration, `Active` included. To
turn a schedule off in code, declare it with `Active: false`, or remove it from the CLI's file. A schedule
fires only when it is active and not paused; `flywheel schedule ls` shows both.

**Running a schedule now.** When a run is needed before its next fire — the one a pause skipped, or a
re-run once a fix is in — trigger it: `TriggerPeriodic` (`flywheel schedule trigger <slug> --reason …`)
enqueues one job of the schedule's kind, args, and queue, due now, and prints its id. It works on a paused
or disabled schedule and leaves its pause and next fire alone, so the schedule stays stopped while the one
run goes through. `flywheel schedule history <slug>` lists the schedule's latest fires, scheduled and
manual, with how each job ended.

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
| `jobs: declared periodic is paused` | info | a start applied a declaration to a schedule an operator paused; it stays paused, and fires nothing, until `ResumePeriodic` — fields `slug`, `paused_at`, and `paused_by_type`, the kind of actor who paused it. Logged once per start for each |
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
