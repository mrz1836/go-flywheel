# Unique-key cookbook

Deduplication, at-most-one-active, and side-effect correlation are things the runtime already answers
with a unique index and a pre-allocated id. This is the how-to, recipe by recipe, each with the
anti-pattern it replaces — because recognizing your own code in the anti-pattern is what makes the recipe
land. For the guarantees underneath these recipes, see [`CONTRACT.md`](CONTRACT.md).

<br>

## 1. Enqueue this exact unit of work at most once, ever — `UniqueKey`

Deduplicate a webhook delivery, a one-time migration, an idempotent command: the same logical unit must
produce at most one job for all time. `UniqueKey` collides with any job that ever bore the key — terminal
or not — and returns `ErrAlreadyEnqueued`.

```go
_, err := flywheel.Insert(ctx, client, ProcessWebhook{DeliveryID: id}, flywheel.InsertOpts{
    UniqueKey: "webhook:" + id,
})
if errors.Is(err, flywheel.ErrAlreadyEnqueued) {
    return nil // already submitted; treat as success, not an error
}
if err != nil {
    return err
}
```

**The consequence to plan for: this job can never be re-enqueued.** The key collides with the original
row forever, so a second `Insert` with the same key always returns `ErrAlreadyEnqueued`, even after the
first job succeeded. To run that unit of work *again*, keep its row and replay it rather than
re-enqueuing:

```go
// Re-run one job by id (resets its attempt budget)...
err := flywheel.RetryJob(ctx, db, jobID)
// ...or a whole failed cohort by lineage or failure window.
_, err = flywheel.ReplayByParent(ctx, db, parentJobID, flywheel.ReplayOpts{})
```

If a later run is *expected* rather than exceptional, you want recipe 2 instead — `UniqueActiveKey` frees
on a terminal state.

<br>

## 2. At most one active job per subject — `UniqueActiveKey`

"Is there already a job running for this subject?" — one open order per account, one active
investigation per person, one in-flight sync per repository. `UniqueActiveKey` collides only with a
*still-live* job carrying the same key (available, running, retryable, scheduled, or paused) and frees
once that job reaches a terminal state, so the next request after the current one finishes enqueues
normally.

```go
_, err := flywheel.Insert(ctx, client, SyncRepo{RepoID: repoID}, flywheel.InsertOpts{
    UniqueActiveKey: "sync:" + repoID,
})
if errors.Is(err, flywheel.ErrAlreadyEnqueued) {
    return nil // a sync for this repo is already in flight
}
```

That is the whole check. The unique index answers "is there already an active job for this subject?" in
the insert itself.

**The anti-pattern it replaces — don't do this:**

```go
// ANTI-PATTERN: an N-row scan plus N JSON decodes to answer a question a
// unique index answers in one insert.
active, err := flywheel.ListActiveByKind(ctx, db, "sync_repo")
if err != nil {
    return err
}
for _, view := range active {
    var args SyncRepo
    if err := json.Unmarshal(view.Args, &args); err != nil {
        return err
    }
    if args.RepoID == repoID {
        return nil // already active — found the hard way
    }
}
// ...only now enqueue, with a race window between the scan and the insert.
```

The scan loads every non-terminal job of the kind and decodes each one to find a match. It is `O(active
jobs)`, it re-runs on every enqueue, and — worse than slow — it has a race: two callers can both scan,
both find nothing, and both enqueue. `UniqueActiveKey` has none of those problems; the collision is
resolved atomically by the database.

<br>

## 3. One job per time bucket — the bucketed key

Enqueue at most one job per subject per time window: one digest email per user per day, one rollup per
account per hour. This is `UniqueKey` (or `UniqueActiveKey`) with the time bucket folded into the key,
and it is exactly the pattern the scheduler uses to make a periodic tick idempotent — a tick that fires
twice for the same bucket computes the same key and the second insert is a successful no-op.

```go
// One job per user per calendar day. Truncate the timestamp to the bucket,
// then put the bucket in the key.
bucket := time.Now().UTC().Truncate(24 * time.Hour)
_, err := flywheel.Insert(ctx, client, DailyDigest{UserID: userID}, flywheel.InsertOpts{
    UniqueKey: fmt.Sprintf("digest:%s@%d", userID, bucket.Unix()),
})
if errors.Is(err, flywheel.ErrAlreadyEnqueued) {
    return nil // this user's digest for today is already queued
}
```

The bucket width is yours to choose — `Truncate(time.Hour)` for hourly, a date string for daily. A
retry, a restart, or two producers racing all compute the same key for the same window, so at most one
job per bucket is created no matter how many times the code runs.

<br>

## 4. Correlate a side effect to the attempt that produced it — `Job.RunID`

A worker that writes to another table (or an external system) needs to record *which attempt* produced
each write, so a retry after a crash does not double it and so you can trace an effect back to its run.
`Job.RunID` is the pre-allocated `job_runs.id` for the attempt — unique, stable for the attempt's whole
lifetime, and already committed (the run stub is written before the worker body), so a row you write may
carry it as a foreign key safely.

```go
func (w *EnrichWorker) Work(ctx context.Context, job flywheel.Job[EnrichArgs]) (flywheel.Result, error) {
    data, err := w.provider.Fetch(ctx, job.Args.SubjectID)
    if err != nil {
        return flywheel.Result{}, err
    }
    // Key the side-effect row on the attempt. A retry writes the same job_run_id,
    // so a unique constraint on it makes the second write a no-op instead of a dup.
    if err := w.db.WithContext(ctx).Clauses(onConflictDoNothing).Create(&SourceFetch{
        JobRunID: job.RunID, // the idempotency key the contract points at
        Payload:  data,
    }).Error; err != nil {
        return flywheel.Result{}, err
    }
    // Structured output is stored on the JobRun, queryable later without a side table.
    return flywheel.Result{Output: EnrichSummary{Count: len(data)}}, nil
}
```

**The anti-pattern it replaces:** a parallel job-lifecycle table — your own `enrichment_runs` with your
own status column, attempt counter, and started/finished timestamps, maintained by hand alongside the
job. The runtime already keeps `job_runs`: one audit row per attempt, with the outcome, the timing, and
`Result.Output`. Correlate to `Job.RunID` and read `job_runs` instead of rebuilding it.

<br>

## 5. Choosing between them — the decision table

| You want | Key | Collides with | Frees when | Re-enqueuable? |
|---|---|---|---|---|
| At most once, ever | `UniqueKey` | any job that ever bore the key | never | No — replay the row instead |
| At most one active per subject | `UniqueActiveKey` | a still-live job with the key | the job reaches a terminal state | Yes, once the active one finishes |
| At most one per time window | either, with the bucket in the key | same-bucket job | per the key type above | per the key type above |
| Correlate an effect to an attempt | *(not a unique key)* — use `Job.RunID` | — | — | — |
| Enqueue atomically with your own write | *(any key)* + `InsertOpts.Tx` | per the key | per the key | the job exists iff your transaction commits |

The distinction that trips people up: **`UniqueKey` colliding with a terminal job is a feature, not a
bug.** It is what "at most once, ever" means. If you find yourself wanting the key to free up after the
job finishes, you wanted `UniqueActiveKey` — choose it before working around `UniqueKey`.

`InsertOpts`' godoc points here; this table is the expanded form of the one-line summaries on the
`UniqueKey` and `UniqueActiveKey` fields.
