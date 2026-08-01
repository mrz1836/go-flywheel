package core

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/mrz1836/go-foundation/models"
	"gorm.io/gorm"
)

// ReplayOpts configures a bulk replay. It embeds RetryOpts, so the budget
// restoration and delay a single retry offers apply to every job in the cohort —
// ResetAttempts is what makes a replay grant a real second life rather than the
// one-more-try a plain retry gives (see RetryOpts).
type ReplayOpts struct {
	RetryOpts

	// States filters which jobs are replayed. Empty replays discarded jobs only —
	// the overwhelmingly common intent, and the safe default: a bulk replay must
	// never re-run work that succeeded by accident. Naming StateSucceeded here
	// additionally requires RetryOpts.Force, or the call returns ErrJobTerminal.
	//
	// The states named here should be terminal ones — a replay reasons about
	// finished work. The accounting in ScopeResult is stated over the terminal
	// jobs a replay targets and the running ones it never interrupts.
	States []JobState

	// Kinds, when non-empty, restricts the replay to these job kinds. It is one of
	// the two bounds an unscoped Replay accepts; it also further narrows a
	// ReplayByParent when a parent's children span more than one kind.
	Kinds []string

	// FailedSince, when non-zero, restricts the replay to jobs finalized at or after
	// it, so an incident window can be replayed without touching older failures. It
	// is the other bound an unscoped Replay accepts.
	FailedSince time.Time

	// Stagger, when > 0, spreads the replayed cohort's scheduled_at uniformly across
	// that window instead of making every job immediately available — how a
	// 30,000-job replay avoids arriving at a just-recovered dependency all at once.
	// Job i of n (a global index across batches, over the pre-counted cohort) lands
	// at base + Stagger*i/n, where base is now + Delay. The placement is
	// deterministic — no randomness — so an operator can predict when the last job
	// lands, and it is reproducible.
	//
	// Stagger shapes arrival; it is not a rate ceiling. It does not cap how fast the
	// cohort is claimed once each job is due, only when each becomes due. Admission
	// control — an actual claim-rate ceiling — is a separate, later capability.
	Stagger time.Duration

	// BatchSize bounds the transaction per batch. Zero or negative selects the
	// default; it is never unbounded.
	BatchSize int
}

// batchSize resolves the configured batch size, applying defaultScopeBatchSize for
// a non-positive value — so a replay and the scoped controls bound a transaction
// identically, and no value is ever unbounded.
func (o ReplayOpts) batchSize() int {
	if o.BatchSize <= 0 {
		return defaultScopeBatchSize
	}
	return o.BatchSize
}

// replayStates returns the states a replay targets, defaulting an empty States to
// discarded alone — the safe default that never re-runs succeeded work.
func (o ReplayOpts) replayStates() []JobState {
	if len(o.States) == 0 {
		return []JobState{StateDiscarded}
	}
	return o.States
}

// ReplayByParent returns the matching children of a parent to available in bounded
// batches, restoring their retry budget when ResetAttempts is set. It is the bulk
// form of RetryJobWithOptions scoped to a lineage, so replaying a cohort is one
// call and one bounded loop rather than N per-id transactions.
//
// It is state-guarded exactly like the scoped controls: a running child is left to
// finalize (SkippedRunning), and a terminal child in a state the replay does not
// target is left alone (SkippedTerminal) — a succeeded child is never replayed
// unless States names its state and Force is set. It replays the discarded children
// by default. Kinds, when set, further narrows the cohort to those kinds, and
// FailedSince to jobs finalized within an incident window.
//
// The work is bounded — one transaction per batch, BatchSize children each — and
// cancellation is checked between batches, so a cancelled replay is partial
// progress returned alongside the wrapped context error, with the committed batches
// kept.
func ReplayByParent(ctx context.Context, db *gorm.DB, parentJobID string, opts ReplayOpts) (ScopeResult, error) {
	scope := func(q *gorm.DB) *gorm.DB {
		return applyKindFilter(q.Where("parent_job_id = ?", parentJobID), opts.Kinds)
	}
	return replay(ctx, db, scope, "replay by parent", opts)
}

// Replay returns every job matching opts to available, unscoped by lineage,
// restoring their retry budget when ResetAttempts is set. It is the incident-shaped
// recovery: replay a kind that a downed dependency failed, bounded to the outage
// window.
//
// Kinds or FailedSince must bound it. An unbounded replay of every discarded job in
// the database is almost never the intent, so a Replay with neither set returns
// ErrReplayUnbounded rather than doing something enormous by accident — use
// ReplayByParent to bound a replay by lineage instead.
//
// It carries the same state guards, batching, and cancellation contract as
// ReplayByParent.
func Replay(ctx context.Context, db *gorm.DB, opts ReplayOpts) (ScopeResult, error) {
	if len(opts.Kinds) == 0 && opts.FailedSince.IsZero() {
		return ScopeResult{}, ErrReplayUnbounded
	}
	scope := func(q *gorm.DB) *gorm.DB {
		return applyKindFilter(q, opts.Kinds)
	}
	return replay(ctx, db, scope, "replay", opts)
}

// applyKindFilter narrows q to the given kinds when any are named, and leaves it
// untouched otherwise. It is shared by both entry points so a kind bound reads the
// same whether it scopes an unscoped Replay or narrows a ReplayByParent.
func applyKindFilter(q *gorm.DB, kinds []string) *gorm.DB {
	if len(kinds) > 0 {
		q = q.Where("kind IN ?", kinds)
	}
	return q
}

// replay is the shared engine behind ReplayByParent and Replay. scope applies the
// caller's lineage and/or kind predicate to a fresh query — it is re-applied to
// each pre-loop count on db and to each batch's SELECT and UPDATE on that batch's
// own transaction, so the engine stays agnostic to which entry point invoked it (a
// *gorm.DB base bound to the outer handle could not run inside a batch's tx).
// label names the operation in errors.
//
// # Accounting
//
// ScopeResult accounts for the finished-or-finishing work in scope. With no
// FailedSince window, Changed + SkippedTerminal + SkippedRunning equals the number
// of in-scope jobs that were terminal or running when the replay began: the
// targeted terminal jobs are replayed (Changed), the terminal jobs in an untargeted
// state are left alone (SkippedTerminal), and the running attempts are never
// interrupted (SkippedRunning). Jobs that were merely available or scheduled are
// neither — a replay reasons about finished work, not pending work. A FailedSince
// window narrows Changed to the jobs finalized within it, deliberately leaving
// older targeted jobs untouched and out of the accounting.
func replay(
	ctx context.Context, db *gorm.DB, scope func(*gorm.DB) *gorm.DB, label string, opts ReplayOpts,
) (ScopeResult, error) {
	if db == nil {
		return ScopeResult{}, fmt.Errorf("flywheel: %s: db is nil", label)
	}
	states := opts.replayStates()
	// Guard: re-running succeeded work is the destructive case here, so naming
	// StateSucceeded requires Force explicitly. Refuse the whole call before any
	// write — a rejected replay changes nothing.
	if !opts.Force && slices.Contains(states, StateSucceeded) {
		return ScopeResult{}, ErrJobTerminal
	}
	stateStrs := stateStrings(states)
	batchSize := opts.batchSize()
	now := models.ClockFrom(ctx).Now(ctx)
	// base is the cohort's un-staggered arrival: Delay 0 makes it immediately
	// claimable, a positive Delay defers it, and a Stagger spreads arrivals from base.
	base := now.Add(opts.Delay)
	var result ScopeResult

	// A dead context on entry does no work at all — not even the counting reads — and
	// reports the progress made in the loop's own vocabulary, matching the sweep and
	// the scoped controls.
	if err := ctx.Err(); err != nil {
		return ScopeResult{}, fmt.Errorf("flywheel: %s cancelled after 0 changed: %w", label, err)
	}

	// Count what the replay leaves alone, before it runs — the scoped controls'
	// shape. Counting the untargeted terminal children before the loop is what keeps
	// the replay's own freshly-available rows out of SkippedTerminal: after the loop
	// those rows are available, and a post-hoc count could not tell them from ones
	// that were never targeted.
	if err := scope(db.WithContext(ctx).Model(&jobRow{})).
		Where("state = ?", string(StateRunning)).
		Count(&result.SkippedRunning).Error; err != nil {
		return ScopeResult{}, fmt.Errorf("flywheel: %s: count running: %w", label, err)
	}
	if err := scope(db.WithContext(ctx).Model(&jobRow{})).
		Where("state IN ? AND state NOT IN ?", terminalStateStrings(), stateStrs).
		Count(&result.SkippedTerminal).Error; err != nil {
		return ScopeResult{}, fmt.Errorf("flywheel: %s: count terminal: %w", label, err)
	}

	// The stagger denominator: the size of the changeable set, counted once so job i
	// of n lands at base + Stagger*i/n across the whole cohort rather than restarting
	// per batch. Only counted when staggering — the common uniform replay skips it.
	total := 0
	if opts.Stagger > 0 {
		q := scope(db.WithContext(ctx).Model(&jobRow{})).Where("state IN ?", stateStrs)
		if !opts.FailedSince.IsZero() {
			q = q.Where("finalized_at >= ?", opts.FailedSince)
		}
		var n int64
		if err := q.Count(&n).Error; err != nil {
			return ScopeResult{}, fmt.Errorf("flywheel: %s: count changeable: %w", label, err)
		}
		total = int(n)
	}

	// Move the targeted children to available in bounded batches, one transaction
	// each, advancing a keyset cursor over id. The cursor — rather than the scoped
	// controls' "moved rows leave the source scope, so a short page means done" — is
	// what guarantees termination even if States overlaps the target state, and it
	// gives the deterministic global running index a staggered replay needs.
	// Cancellation is checked between batches; the committed batches are kept, so a
	// cancelled replay is partial progress. placed is the global running index — how
	// many jobs have been assigned a stagger slot so far, advanced by every selected
	// row so a re-guard skip leaves only a negligible gap in the distribution.
	var cursor string
	placed := 0
	for {
		if err := ctx.Err(); err != nil {
			return result, fmt.Errorf("flywheel: %s cancelled after %d changed: %w", label, result.Changed, err)
		}
		selected, changed, last, err := replayBatch(
			ctx, db, scope, stateStrs, opts, cursor, batchSize, now, base, total, placed,
		)
		if err != nil {
			return result, fmt.Errorf("flywheel: %s: %w", label, err)
		}
		result.Changed += changed
		placed += selected
		if selected > 0 {
			result.Batches++
		}
		if selected < batchSize {
			return result, nil
		}
		cursor = last
	}
}

// replayBatch selects and replays one bounded batch inside a single transaction,
// advancing a keyset cursor over id. It returns how many rows it selected (which
// drives the loop's termination), how many it actually changed, and the last id
// seen — the next cursor.
//
// The UPDATE re-guards on the target states — WHERE id IN ? AND state IN ? — rather
// than trusting the ids the SELECT found: without SKIP LOCKED a concurrent finalize
// or retry could move one of them between the SELECT and the UPDATE, and an
// unguarded UPDATE would resurrect it out of the state that concurrent operation
// left it in. The re-guard makes the batch replay only children still eligible, so
// Changed counts real transitions.
func replayBatch(
	ctx context.Context, db *gorm.DB, scope func(*gorm.DB) *gorm.DB, stateStrs []string,
	opts ReplayOpts, cursor string, batchSize int, now, base time.Time, total, placed int,
) (selected int, changed int64, last string, err error) {
	err = db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		q := scope(tx.Model(&jobRow{})).Where("state IN ?", stateStrs)
		if !opts.FailedSince.IsZero() {
			q = q.Where("finalized_at >= ?", opts.FailedSince)
		}
		if cursor != "" {
			q = q.Where("id > ?", cursor)
		}
		var ids []string
		if e := q.Order("id").Limit(batchSize).Pluck("id", &ids).Error; e != nil {
			return e
		}
		selected = len(ids)
		if selected == 0 {
			return nil
		}
		last = ids[selected-1]

		upd := map[string]any{
			"state":        string(StateAvailable),
			"leased_until": nil,
			"lease_token":  nil,
			"finalized_at": nil,
			"scheduled_at": scheduledAtValue(ids, base, opts.Stagger, total, placed),
			"updated_at":   now,
		}
		applyRetryBudget(upd, opts.RetryOpts)

		res := tx.Model(&jobRow{}).Where("id IN ? AND state IN ?", ids, stateStrs).Updates(upd)
		if res.Error != nil {
			return res.Error
		}
		changed = res.RowsAffected
		return nil
	})
	return selected, changed, last, err
}

// scheduledAtValue is the scheduled_at assignment for one batch. Without a stagger
// window every replayed job lands at base — one uniform value, the fast path and the
// common case. With a window, job at global index placed+k lands at
// base + Stagger*(placed+k)/total, expressed as a portable CASE id WHEN … THEN …
// ELSE scheduled_at END so a single UPDATE places the whole batch at once. The CASE
// binds two parameters per row, bounded by the batch size, and reads identically on
// PostgreSQL and SQLite. The placement is deterministic: an operator can predict
// when the last job lands, and a test can assert exact per-decile counts.
//
// The ELSE scheduled_at branch is never reached — every id in the batch matches a
// WHEN — but it gives the CASE a concrete timestamp type. Without it PostgreSQL
// types the untyped time parameters as text and rejects the assignment to the
// timestamptz column; the existing-value ELSE resolves that portably rather than
// with a dialect-specific cast.
func scheduledAtValue(ids []string, base time.Time, window time.Duration, total, placed int) any {
	if window <= 0 || total <= 0 {
		return base
	}
	var b strings.Builder
	b.WriteString("CASE id")
	args := make([]any, 0, len(ids)*2)
	for k, id := range ids {
		b.WriteString(" WHEN ? THEN ?")
		offset := time.Duration(int64(window) * int64(placed+k) / int64(total))
		args = append(args, id, base.Add(offset))
	}
	b.WriteString(" ELSE scheduled_at END")
	return gorm.Expr(b.String(), args...)
}
