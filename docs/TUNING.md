# Tuning guide

Every recommendation here cites a measurement from [`BENCHMARKS.md`](BENCHMARKS.md) — nothing is
reasoned from first principles. The numbers come from a developer laptop (Apple M4, local PostgreSQL
17.10), so treat absolute throughput as a floor and the *relative* effects as the transferable result.
Percentiles carry a ±15.5 % relative error from the histogram bucketing; count, min, max, and mean are
exact.

Start from the defaults. They are chosen to be correct, not maximal, and most deployments never need to
move them.

<br>

## Lease and heartbeat sizing

`LeaseDuration` (default 30 s) bounds how long a claim survives without a heartbeat; `HeartbeatInterval`
(default `LeaseDuration / 3`, floored at 1 s) is how often a live worker renews it. A live worker is
never reclaimed, because each renewal pushes `leased_until` ahead of the sweep's cutoff.

- **Size `LeaseDuration` to comfortably exceed your worker's p99 duration**, read from
  `flywheel_job_duration_seconds`. The lease is the recovery deadline for a *crashed* worker — too long
  and a genuinely dead job waits that long before its work is retried; too short and a slow-but-alive
  worker risks reclaim if the heartbeat ever stalls.
- **Do not set a lease under three seconds without setting `HeartbeatInterval` explicitly.** The derived
  interval floors at one second, so a lease at or under three seconds gets less renewal headroom than
  the `/3` divisor promises, and one at or under a second gets none. Short-lease deployments should set
  the cadence by hand.
- **Watch `flywheel_jobs_superseded_total`.** A nonzero supersede rate means work is running twice: the
  lease is too short for the workload, or the heartbeat is disabled (`HeartbeatInterval < 0`) or failing.
  The mass-lease-expiry load test reclaimed 44 leases and superseded exactly 44 attempts — supersede
  count tracks reclaim count one-for-one.

<br>

## Concurrency (pool size) and the connection pool

`Concurrency` is the worker-pool size: the runner claims to fill its free slots and dispatches each job
independently, so a slow job occupies one slot rather than stalling the batch behind it.

- **Removing the old batch barrier was worth 4.2× on a mixed-speed workload** — 10 % of jobs at 20× the
  base duration. Slot utilization went from 24.4 % to 95.4 % and throughput from 247 to 1,036 jobs/s,
  with no change to the workload. On a *uniform* workload the same change is worth nothing measurable
  (there is no straggler to wait for), so the win scales with how heavy-tailed your durations are.
- **Size the connection pool to Concurrency plus headroom.** Each concurrent worker that touches the
  database needs a connection, plus one for the runner's claim and one for the heartbeat writes. A pool
  smaller than `Concurrency` caps effective parallelism below the pool size you configured.
- **`ClaimBatchSize` is almost always best left zero** (claim exactly the free-slot count). Set it below
  `Concurrency` only to smooth claim bursts across a large fleet; it is never raised above the free-slot
  count, because a claimed job the runner cannot immediately start is a job leased and not running.
- **SQLite requires `Concurrency: 1`** and a single-writer pool (`SetMaxOpenConns(1)`). This is enforced
  with `ErrSQLiteConcurrency`; see [`RUNBOOK.md`](RUNBOOK.md).

<br>

## Poll interval versus claim cost

`PollInterval` (default 100 ms) is how often an idle runner re-checks for work. The trade-off is latency
to pick up a new job versus the cost of empty polls, and the cost of an empty poll depends entirely on
the claim's shape.

- **On a single-queue runner an empty poll is a cheap index probe** — 0.012–0.019 ms at a million rows
  — so a short poll interval costs almost nothing and buys lower pickup latency. This is the deployment
  the claim index is built for.
- **A runner naming more than one queue cannot use the ordered index scan**: its claim (empty or not)
  falls back to a sequential scan and a sort, 317–583 ms at a million rows. The fix is not a longer poll
  interval — it is a runner per queue. Splitting the claim is worth turning a 153 ms bitmap-and-sort
  claim into a 0.18 ms index scan (858×) on a single queue.
- **When the database is unavailable, consecutive poll failures climb an exponential ladder** from
  `PollInterval` to `MaxPollBackoff` (30 s default) with jitter, resetting on the first success. Against
  a 60-second outage this cut refused claims from 42,676 to 56 (762× fewer) and stopped the error log
  from filling a disk. `MaxPollBackoff` floors at `2 × PollInterval`, so it can never poll more slowly
  than twice the base interval.

<br>

## Enqueue batch size

`InsertMany` writes N jobs in bounded, dialect-aware chunks — one multi-row `INSERT` per chunk — where
`Enqueue` writes one row per call.

- **Batch bulk enqueues.** At 100,000 jobs, `InsertMany` at the default chunk of 1,000 hit
  52,230 jobs/s against single-row `Enqueue`'s 9,897 — a 5.3× speedup from cutting 100,000 statements to
  100.
- **The default chunk of 1,000 is where the curve knees.** Doubling to 2,000 buys only 1.4 % (the round
  trips are no longer the bottleneck), and 1,000 × 22 columns = 22,000 bind parameters sits at roughly a
  third of PostgreSQL's 65,535 ceiling, so a future column addition cannot push a default-configured
  caller over it. Leave it at 1,000 unless you have measured a reason not to.

<br>

## Sweep, retention, and scoped batch sizes

Both maintenance operations work in bounded batches, one transaction per batch, because the unbounded
versions did not degrade at depth — they *stopped working*. A single `IN (?)` list binds one parameter
per row and PostgreSQL's extended protocol rejects a statement carrying more than 65,535 of them, so a
pool that died holding more than ~65 k leases left a backlog the sweep could never clear.

- **Leave `SweepBatchSize` and `RetentionBatchSize` at their defaults** unless you have a specific reason.
  Below the old ceiling, transaction age was linear in backlog at ≈26 µs/row while holding a row lock on
  every reclaimed job; batching bounds that. The batched sweep drained a million jobs with no client
  transaction held longer than 76 ms.
- **A smaller batch bounds lock-hold time; a larger one cuts round trips.** Tighten the batch when a
  maintenance pass contends with live traffic; widen it when the database is quiet and you want the
  backlog cleared faster. Bound the first retention prune against an old database with `RetentionMaxBatches`
  so it does not run unboundedly — see the retention section of [`RUNBOOK.md`](RUNBOOK.md).

<br>

## `fillfactor` and autovacuum

The runtime's steady-state storage cost is dominated by *update churn* on `jobs`, not row growth: each
job takes two updates (claim, then finalize), and MVCC makes each a new tuple version. A 100k drain left
the table +45 % larger with roughly one dead tuple per live row, and the default autovacuum settings did
not keep pace (two passes in two minutes).

- **Apply the tuned storage parameters** (`Migrate` and `InstallStorageParameters` set them). At a
  1M working set, the default arm grew +19.3 MB/min over its final third while the tuned arm *shrank*
  4.8 MB/min — autovacuum reclaiming faster than churn added — and ran 29 autovacuum cycles against 5.
  Drain throughput rose from 2,596 to 3,947 jobs/s.
- **The win comes mostly from `autovacuum_vacuum_scale_factor`**, not `fillfactor`. The 5-to-29 cycle
  count points at the scale factor, and it is the setting with a mechanism that plainly applies here.
- **`fillfactor` does not enable HOT updates on this table** — the common belief that it does is false
  here. A HOT update needs no index-relevant column to change, but `state` sits in the *predicate* of
  four partial indexes, so every state transition is non-HOT whatever the fillfactor is. What fillfactor
  buys is same-page placement for the new tuple version.
- **The cost is real: WAL per job rose 54 %** (7.0 → 10.8 KB) under tuning, because vacuuming more often
  writes more WAL and a sub-100 fillfactor puts fewer tuples per page. A deployment paying for
  replication or backup by the byte should weigh that against the bloat and throughput it buys.
- **Do not tune `job_runs` or `job_periodics`.** Neither has update churn, and a lower fillfactor on an
  append-only table reserves free space for updates that never come.

<br>

## Retry-backoff cap arithmetic

`MaxRetryBackoff` (default 1 minute) caps the exponential retry delay. It matters only when a
dependency's outages outlast the cap — then it decides whether a job's attempt budget survives the
outage or is spent partway through it.

- **The default is fine for short failures.** With a budget that fits below the cap, raising the cap
  costs nothing: an 8-attempt cohort against a simulated outage burned exactly 80,000 attempts (8.0 per
  job) whether the cap was 1 minute or 30 minutes. The exponential ladder makes attempt volume nearly
  cap-independent, so a higher cap adds no audit rows.
- **Raise the cap when outages last hours.** A 1-minute cap saturates after seven rungs, so a 25-attempt
  budget spans under 25 minutes — every job discarded while a 4-hour outage is still going. A 30-minute
  cap keeps climbing (`1s → 2s → … → 17m → 30m`), so the same 25-attempt budget spans 4.5 hours; the
  outage lifts around attempt 18 and the whole cohort survives — drained, not discarded, on *fewer*
  attempts (18 vs 25 per job).
- **The cap is not a throughput knob.** It never changes the first-attempt delay (`RetryBackoffBase`),
  only the spacing of the later rungs. Size it, and `MaxAttempts`, against your dependency's worst
  realistic outage.

<br>

## Fairness across parents

Equal-priority claims are strict FIFO, so a large batch enqueued first drains entirely before a small
later batch gets a single claim. Fairness is expressed at *enqueue* time through priority banding, not in
the claim, because ranking the ready set in the claim is `O(ready)`: a round-robin CTE cost 216 ms per
claim at a million ready rows (≈3,600× the shipped 0.06 ms claim) and spills its sort to disk. Writing
each parent's *n*-th child into the same priority band lets the untouched `(priority, scheduled_at)`
claim interleave parents for free — the longest single-parent run dropped from 10,000 to 13, and each
parent held at least 43.8 % of every window instead of 0 %. See the README's fairness section for the
how-to.
