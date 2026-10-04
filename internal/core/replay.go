package core

import (
	"context"
	"errors"
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
	// n counts every job the replay targets, including the ones it leaves terminal
	// because their UniqueActiveKey is held, and such a job keeps its slot unused:
	// the jobs after it land where they would have, so the window stays an upper
	// bound and the gaps are negligible unless much of the cohort shares keys.
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
// It never puts a child live while another job holds its UniqueActiveKey, which the
// unique index would reject: a targeted child whose key another job holds live
// when its batch runs stays terminal, counted in SkippedActiveKey, and the replay
// carries on past it. Of the targeted children in one batch that share a key, the
// first in id order is replayed; a child in a later batch is skipped while that
// one is live, and replayed if it has already finished. When States names a live
// state, a targeted child that is already live keeps its key. A skipped child can
// be replayed once the job holding its key finishes.
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
// It carries the same state guards, UniqueActiveKey rule, batching, and
// cancellation contract as ReplayByParent.
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
// FailedSince window, Changed + SkippedTerminal + SkippedRunning + SkippedActiveKey
// equals the number of in-scope jobs that were terminal or running when the replay
// began: each targeted terminal job is either replayed (Changed) or left terminal
// because its UniqueActiveKey was held (SkippedActiveKey), the terminal jobs in an
// untargeted state are left alone (SkippedTerminal), and the running attempts are
// never interrupted (SkippedRunning). Jobs that were merely available or scheduled
// are neither — a replay reasons about finished work, not pending work. A
// FailedSince window narrows Changed and SkippedActiveKey to the jobs finalized
// within it, deliberately leaving older targeted jobs untouched and out of the
// accounting.
//
// The sum is exact for a replay nothing else races. A job a concurrent operation
// moves, or whose key a concurrent write takes, between its batch's SELECT and
// UPDATE is skipped by the UPDATE's re-guard and counted in no bucket: Changed
// counts only real transitions.
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
	// row so a re-guard skip, or a job left terminal for its key, leaves only a
	// negligible gap in the distribution rather than shifting the jobs after it.
	var cursor string
	placed, races := 0, 0
	for {
		if err := ctx.Err(); err != nil {
			return result, fmt.Errorf("flywheel: %s cancelled after %d changed: %w", label, result.Changed, err)
		}
		b, err := replayBatch(ctx, db, scope, stateStrs, opts, cursor, batchSize, now, base, total, placed)
		if err != nil {
			// A batch that lost a race for a key rolled back whole, so running it
			// again is safe, and the rerun reads the new holder and leaves that job
			// terminal.
			if races < replayBatchRetries && retryableBatchError(err) {
				races++
				continue
			}
			return result, fmt.Errorf("flywheel: %s: %w", label, err)
		}
		races = 0
		result.Changed += b.changed
		result.SkippedActiveKey += b.skippedActiveKey
		placed += b.selected
		if b.changed > 0 {
			result.Batches++
		}
		if b.selected < batchSize {
			return result, nil
		}
		cursor = b.last
	}
}

// replayBatchRetries bounds how many times in a row a replay reruns a batch that
// lost a race (retryableBatchError), so a key that keeps changing hands ends the
// replay with the error rather than looping. A batch that commits resets the
// count, so the budget is per batch.
const replayBatchRetries = 3

// retryableBatchError reports whether a replay batch failed in a way that running
// it again resolves. The batch's transaction rolls back on any error, so none of
// it committed and a rerun is safe; these are the failures a rerun gets past:
//
//   - a duplicate key: a concurrent write took one of the batch's keys while its
//     UPDATE ran, after the NOT EXISTS guard had read the key as free. isDuplicateKey
//     also matches the gorm.ErrDuplicatedKey a host's TranslateError substitutes.
//   - a deadlock (40P01): that write then waited on a key the UPDATE had already
//     taken, and PostgreSQL broke the cycle by failing the UPDATE. Two replays
//     whose cohorts share keys can meet the same way.
//   - a serialization failure (40001), the form the same conflict takes when the
//     host runs transactions at a stricter isolation level.
//
// The SQLSTATE is read through the SQLState method pgconn.PgError carries, so the
// runtime does not import the driver for it.
func retryableBatchError(err error) bool {
	if isDuplicateKey(err) {
		return true
	}
	var coded interface{ SQLState() string }
	if errors.As(err, &coded) {
		switch coded.SQLState() {
		case "40P01", "40001":
			return true
		}
	}
	return false
}

// replayBatchResult is what one replay batch did.
type replayBatchResult struct {
	// selected is how many rows the batch's SELECT returned, which drives the loop's
	// termination and the stagger's running index.
	selected int
	// changed is how many of them the UPDATE moved to available.
	changed int64
	// skippedActiveKey is how many it left terminal because their key was held.
	skippedActiveKey int64
	// last is the last id selected: the next cursor.
	last string
}

// replayCandidate is one row a replay batch selected, with whether another job
// held its UniqueActiveKey live when the batch read it.
type replayCandidate struct {
	ID              string  `gorm:"column:id"`
	UniqueActiveKey *string `gorm:"column:unique_active_key"`
	Held            bool    `gorm:"column:held"`
}

// replayBatch selects and replays one bounded batch inside a single transaction,
// advancing a keyset cursor over id.
//
// No job goes live while another holds its UniqueActiveKey, because
// jobs_unique_active_key allows one live job per key and a violation would fail
// the batch. The SELECT reads, with each row, whether another job holds its key
// live (activeKeyHeld), and
// liveReplayCandidates keeps the rows that may go live: every unkeyed row and, per
// key, the first row in id order whose key is free. A row it drops counts in
// skippedActiveKey. A later batch needs no memory of this one: once this batch
// commits, its replayed rows are live holders that the next SELECT reads.
//
// The UPDATE re-guards rather than trusting what the SELECT found: without SKIP
// LOCKED a concurrent finalize or retry could move a row between the SELECT and the
// UPDATE, and a concurrent enqueue or retry could take its key. WHERE id IN ? AND
// state IN ? keeps an unguarded UPDATE from resurrecting a row out of the state a
// concurrent operation left it in, and NOT activeKeyHeld keeps it from putting a
// second live job on a key. A row either guard drops is simply not changed, so
// Changed counts real transitions. Only a write that takes a key while the UPDATE
// itself runs gets past the guard. It fails the UPDATE with a duplicate key or,
// when that write then waits on a key the UPDATE already took, with a deadlock;
// either way the batch rolls back whole and the caller reruns it.
func replayBatch(
	ctx context.Context, db *gorm.DB, scope func(*gorm.DB) *gorm.DB, stateStrs []string,
	opts ReplayOpts, cursor string, batchSize int, now, base time.Time, total, placed int,
) (b replayBatchResult, err error) {
	err = db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		q := scope(tx.Model(&jobRow{})).Where("state IN ?", stateStrs)
		if !opts.FailedSince.IsZero() {
			q = q.Where("finalized_at >= ?", opts.FailedSince)
		}
		if cursor != "" {
			q = q.Where("id > ?", cursor)
		}
		// The key check runs over a derived table already cut to the batch. In a
		// single SELECT, SQLite evaluates the select list for every row that enters
		// its top-N sorter, and when the scan visits rows out of id order that is
		// most of the rows left in the cohort, on every batch.
		batch := q.Select("id", "unique_active_key").Order("id").Limit(batchSize)
		var rows []replayCandidate
		if e := tx.Table("(?) AS batch", batch).
			Select("batch.id, batch.unique_active_key, " + activeKeyHeld("batch") + " AS held").
			Order("batch.id").Find(&rows).Error; e != nil {
			return e
		}
		b.selected = len(rows)
		if b.selected == 0 {
			return nil
		}
		b.last = rows[b.selected-1].ID

		live := liveReplayCandidates(rows)
		b.skippedActiveKey = int64(b.selected - len(live))
		if len(live) == 0 {
			return nil
		}

		// Every selected row keeps its stagger slot, so a skipped row leaves a gap
		// instead of shifting the rows after it.
		ids := make([]string, b.selected)
		for k, r := range rows {
			ids[k] = r.ID
		}
		upd := map[string]any{
			"state":        string(StateAvailable),
			"leased_until": nil,
			"lease_token":  nil,
			"finalized_at": nil,
			"scheduled_at": scheduledAtValue(ids, base, opts.Stagger, total, placed),
			"updated_at":   now,
		}
		applyRetryBudget(upd, opts.RetryOpts)

		res := tx.Model(&jobRow{}).
			Where("id IN ? AND state IN ? AND NOT "+activeKeyHeld("jobs"), live, stateStrs).
			Updates(upd)
		if res.Error != nil {
			return res.Error
		}
		b.changed = res.RowsAffected
		return nil
	})
	return b, err
}

// liveReplayCandidates returns the ids of the rows that may go live, in id order:
// every unkeyed row, and per UniqueActiveKey the first row whose key no other job
// holds live. rows arrive in id order, which makes the choice deterministic.
func liveReplayCandidates(rows []replayCandidate) []string {
	live := make([]string, 0, len(rows))
	taken := make(map[string]bool)
	for _, r := range rows {
		if r.UniqueActiveKey == nil {
			live = append(live, r.ID)
			continue
		}
		if r.Held || taken[*r.UniqueActiveKey] {
			continue
		}
		taken[*r.UniqueActiveKey] = true
		live = append(live, r.ID)
	}
	return live
}

// activeKeyHeld is a correlated SQL predicate over the row the query names outer,
// a jobs row or a projection of its id and unique_active_key: true when another
// job holds that row's UniqueActiveKey live. It is jobs_unique_active_key's own
// predicate — the key, in exactly the index's live states, on an aliased jobs with
// no deleted_at condition, since the index has none — so it is true exactly when
// the index would reject the row going live. A row with no key matches nothing.
// The row itself is excluded: a replay whose States names a live state selects a
// row that holds its own key.
//
// The live states are liveStatesSQL's literals, not bound parameters, so the lookup
// probes the partial index instead of scanning jobs once per row.
func activeKeyHeld(outer string) string {
	return "EXISTS (SELECT 1 FROM jobs AS holder" +
		" WHERE holder.unique_active_key = " + outer + ".unique_active_key" +
		" AND holder.state IN " + liveStatesSQL() +
		" AND holder.id <> " + outer + ".id)"
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
