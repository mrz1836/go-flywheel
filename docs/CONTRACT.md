# The exactly-once contract and its limits

This is the most important document in the set, because it is the one that tells you what you do *not*
need to build. The runtime provides a real exactly-once story for a job's **state**. It does not — and
cannot — provide exactly-once execution of a worker's **side effects**. Knowing exactly where that line
falls is what stops you from either reinventing guarantees you already have, or trusting guarantees you
do not.

Read the two lists below as a pair. The first is what you can rely on. The second is what you must
handle yourself, stated as plainly.

<br>

## What is guaranteed

**A claim is exclusive.** When a job is claimed, exactly one executor holds it. On PostgreSQL this is
`SELECT … FOR UPDATE SKIP LOCKED`: concurrent claimants lock disjoint rows and skip what another already
holds, so N runners polling the same queue never hand the same job to two workers. On SQLite the writer
is serialized to the same end. You do not need your own "is anyone else working this?" check.

**A lease bounds a claim, and a heartbeat holds it while the worker lives.** The claim carries a lease
with an expiry. A live worker renews it on a heartbeat, pushing the expiry ahead for as long as it runs,
so a legitimately long job is never mistaken for a dead one. If the worker's process dies, the lease
expires and the stuck-lease sweep returns the job to the queue — the runtime's recovery path for work
lost to a crash.

**A fence bounds a finalize: only the attempt holding the current claim advances the job's state.** Each
claim mints a lease token, and the finalize's state update is scoped to `id = ? AND lease_token = ?`. If
the job was reclaimed (by a sweep) or moved (by a cancel) while the attempt ran, the token no longer
matches, the update touches no row, and the attempt's outcome is *discarded* rather than racing the new
claim. Whichever attempt holds the lease wins; the other does not silently overwrite it.

**Every attempt is audited.** Before the worker body runs, the runtime commits a `job_runs` row with
outcome `started` — the run stub. Because it is committed first, a crash mid-attempt leaves a record,
not a hole: the sweep marks that stub `crashed` rather than deleting it, and its `id` is a stable
foreign-key target a side-effect row can reference. You always know an attempt happened, even one that
died.

**An outcome, once recorded, survives shutdown.** The finalize transaction detaches from context
cancellation (`context.WithoutCancel`), so a drain or a `SIGTERM` that cancels the worker's context
mid-finalize does not roll back the outcome the worker already produced. Once finalize has persisted a
result, the shutdown that arrives a millisecond later cannot lose it.

**Enqueue is idempotent under a unique key, and enqueue-in-transaction is atomic with your state.** An
insert carrying `UniqueKey` collides with any job that ever bore that key and returns
`ErrAlreadyEnqueued` instead of creating a duplicate; `UniqueActiveKey` does the same only while a job is
still live. Passing `InsertOpts.Tx` writes the job row on your transaction, so the job exists **if and
only if** your own state change commits — the transactional-outbox pattern, without a separate outbox
table. See [`COOKBOOK.md`](COOKBOOK.md).

<br>

## What is not guaranteed

**Exactly-once execution of side effects is not guaranteed, and cannot be.** A worker that makes an
external call — charges a card, sends an email, writes to another system — and is then killed *after*
the call returns but *before* its finalize commits will run again on recovery, and make the call a second
time. This is not a defect to be fixed; it is a fundamental property of any system that survives crashes.
The runtime guarantees exactly-once **state advance** (the job moves forward once), not exactly-once
**side effect** (the external call happens once). The window is real and it is between "the side effect
completed" and "the finalize committed."

**A worker performing a non-idempotent external operation must make it idempotent** — and the runtime
hands you the key to do it. `Job.RunID` is a unique, pre-allocated identifier for the attempt, stable
across the attempt's whole lifetime. Use it as the idempotency key on the external request, the
conditional-write predicate, or the dedup check:

- An idempotency key on the outbound request (most payment and messaging APIs accept one) — pass
  `Job.RunID`.
- A conditional write — insert a row keyed by `Job.RunID`, and let a unique-constraint violation tell you
  the side effect already happened on a prior attempt.
- A dedup check before the call — look up `Job.RunID` in your own ledger; skip if present.

Because the run stub is committed before the worker body, a row you write during the attempt may set its
foreign key to `Job.RunID` safely — the `job_runs` row already exists. That is the seam the checklist
below points at.

**The lease is a liveness mechanism, not a mutual-exclusion primitive.** It answers "is the worker still
alive?", not "is anyone else allowed to touch this?". With the heartbeat disabled or failing, a slow
worker can have its job reclaimed and re-dispatched while it is still running. The fence then stops the
*state* damage — the slow attempt's finalize is discarded — but it does **not** stop the *side-effect*
duplication: both attempts ran their external calls. Do not treat holding a lease as holding a lock over
a shared external resource.

**A worker that ignores its context runs to completion regardless of timeout.** The runtime cancels the
worker's context when a lease or execution timeout elapses, but it cannot stop a worker that does not
check `ctx.Done()`. A goroutine ignoring cancellation runs until it returns on its own. Honor the context
in any worker whose duration you need bounded.

**The scheduler is a singleton by deployment, not by election.** The runtime holds no lock guaranteeing
only one scheduler runs. Running two doubles the sweep and retention load (periodic ticks collapse
harmlessly on the bucketed unique key). Enforce one scheduler in your deployment — see
[`RUNBOOK.md`](RUNBOOK.md).

<br>

## What a host must make idempotent — a checklist

For each non-idempotent effect a worker performs, decide how a re-run is made safe, and use the seam the
runtime provides.

| The effect | Why a re-run is possible | The seam to use |
|---|---|---|
| An external API call with a side effect (charge, send, provision) | The process can die after the call returns and before finalize commits | Pass `Job.RunID` as the request's idempotency key |
| Writing a row from an external response | Same window; the write can repeat | Key the row on `Job.RunID` (unique) so the second write conflicts, or set a foreign key to it and dedup |
| Recording that a side effect happened | The ledger write itself can repeat | Insert keyed by `Job.RunID`; a unique-constraint violation *is* the "already done" answer |
| Enqueuing follow-up work | A retried attempt re-enqueues | Give the follow-up a `UniqueKey`, or return it via `Result.FollowUps` so it is enqueued atomically inside the finalize (exactly once with the state advance) |
| A multi-step external workflow | Any step's completion can outlive the finalize | Make each step individually idempotent on `Job.RunID`, or checkpoint progress in your own table keyed by `Job.RunID` |
| Mutating shared external state under a "lock" | The lease is liveness, not mutual exclusion | Do not rely on the lease; use a conditional/compare-and-swap write against the external system |

The rule underneath the table: **the runtime makes the job's own state exactly-once for free; you make
each external effect idempotent, and `Job.RunID` is the key you build that on.** A worker whose external
effects are all idempotent on `Job.RunID` inherits end-to-end exactly-once behavior — the state advance
from the runtime, the side-effect deduplication from you.

<br>

*The `jobs:` / `flywheel:` log prefixes are an observable contract of a different kind — greppable, and
matched by tests. That rule, and the other conventions a change to the runtime inherits, live in
[`CONVENTIONS.md`](CONVENTIONS.md).*
