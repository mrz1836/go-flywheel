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
	"gorm.io/gorm/clause"
)

// BulkAction is what BulkJobs does to each job its spec selects. Each action
// changes a job the way its single-job control does, from the same states.
type BulkAction string

// The actions BulkJobs takes.
const (
	// BulkCancel moves an available, retryable, scheduled, or paused job to
	// cancelled, as CancelJob does.
	BulkCancel BulkAction = "cancel"
	// BulkRetry returns an available, retryable, scheduled, or paused job, and
	// with Retry.Force a terminal one, to available, as RetryJobWithOptions does.
	BulkRetry BulkAction = "retry"
	// BulkDelete deletes a succeeded, cancelled, or discarded job, as DeleteJob
	// does.
	BulkDelete BulkAction = "delete"
	// BulkReschedule moves an available, retryable, scheduled, or paused job's
	// scheduled_at, as RescheduleJob does.
	BulkReschedule BulkAction = "reschedule"
	// BulkPause moves an available, retryable, or scheduled job to paused, as
	// PauseJob does.
	BulkPause BulkAction = "pause"
	// BulkResume moves a paused job to available, as ResumeJob does.
	BulkResume BulkAction = "resume"
)

// BulkSkipReason is why BulkJobs left a selected job as it was. Every job a run
// classifies lands in exactly one of BulkResult.Changed and the skip reasons.
type BulkSkipReason string

// The reasons BulkJobs skips a job.
const (
	// BulkSkipRunning is a running job, which no action ever changes. Under
	// Retry.Force a cancelled job whose attempt is still running counts here
	// too: a cancel fences a running attempt without stopping it, and retrying
	// the job would start a second attempt beside the first.
	BulkSkipRunning BulkSkipReason = "running"
	// BulkSkipTerminal is a succeeded, cancelled, or discarded job an action that
	// needs a live job left alone: a retry without Retry.Force, a cancel, a
	// reschedule, a pause, or a resume.
	BulkSkipTerminal BulkSkipReason = "terminal"
	// BulkSkipNotTerminal is a job a delete left alone because it has not
	// finished: available, retryable, scheduled, or paused.
	BulkSkipNotTerminal BulkSkipReason = "not_terminal"
	// BulkSkipState is a job already in the action's target state: a pause of a
	// paused job, or a resume of one that is available, retryable, or scheduled.
	BulkSkipState BulkSkipReason = "state"
	// BulkSkipActiveKey is a terminal job a forced retry left terminal because
	// another live job holds its UniqueActiveKey, or an earlier job in the run
	// took the same free key.
	BulkSkipActiveKey BulkSkipReason = "active_key"
)

// The bounds on how many jobs one BulkJobs call acts on.
const (
	// DefaultBulkMaxJobs is BulkSpec.MaxJobs when it is zero.
	DefaultBulkMaxJobs = 10000
	// MaxBulkJobs is the largest BulkSpec.MaxJobs.
	MaxBulkJobs = 100000
)

// ErrBulkUnbounded is returned by BulkJobs for a spec whose Query sets no
// filter and that names no IDs: an action on every job in the database is
// almost never the intent.
var ErrBulkUnbounded = errors.New("flywheel: bulk operation must be bounded by a filter or job ids")

// ErrBulkTooLarge is returned by BulkJobs when its selection matches more jobs
// than BulkSpec.MaxJobs: by an apply's first count, before it changes anything,
// or by a run whose selection grew past MaxJobs while it walked, which stops
// there and keeps its committed batches.
var ErrBulkTooLarge = errors.New("flywheel: bulk operation matches more jobs than its limit")

// BulkSpec is what BulkJobs selects and does to it. The zero value is refused:
// it names no action and no filter. With an action and a filter or IDs set,
// every other field's zero value is the common case: an apply (DryRun off) of
// at most DefaultBulkMaxJobs jobs, in batches of 500, from the first match, with
// no id ceiling, no operation id, and no stagger.
type BulkSpec struct {
	// Query is the filter, as QueryJobs applies it; Sort, Cursor, and Limit are
	// ignored.
	Query JobQuery
	// IDs is exact job ids instead, at most MaxJobs of them; used when Query is
	// empty. They are de-duplicated and walked in id order.
	IDs []string
	// Action is what the call does to each selected job.
	Action BulkAction
	// DryRun counts, classifies, and samples the selection, and changes nothing.
	DryRun bool
	// MaxJobs bounds how many jobs the call acts on: zero selects
	// DefaultBulkMaxJobs; negative, or above MaxBulkJobs, is a *ValidationError.
	MaxJobs int
	// BatchSize is the number of jobs per transaction: zero or negative selects
	// 500, and above 5,000 reads 5,000.
	BatchSize int
	// After resumes a run: the ResumeAfter a previous run returned. Only jobs
	// whose id sorts after it are selected.
	After string
	// MaxID selects only jobs whose id sorts at or below it (UUIDv7 ids are
	// insert order), so a job inserted after it is never matched; empty means no
	// bound.
	MaxID string
	// OperationID is recorded in every audit entry's details, as operation_id.
	OperationID string
	// RescheduleAt is when a BulkReschedule makes each job due; it is required
	// there and refused on every other action.
	RescheduleAt time.Time
	// Retry is a BulkRetry's options: Force, ResetAttempts, Budget, and Delay, as
	// RetryJobWithOptions reads them; AllowRunning is ignored, so a running job
	// is never retried. It is refused on every other action.
	Retry RetryOpts
	// Stagger, on a BulkRetry or a BulkReschedule, spreads the jobs' due times
	// across this window, as ReplayOpts.Stagger does; refused on every other
	// action.
	Stagger time.Duration
}

// BulkResult reports what a BulkJobs call did, or for a dry run what it would
// do. An interrupted or refused-midway run returns the result so far beside its
// error.
type BulkResult struct {
	// Matched is the number of jobs the spec matched past After, counted before
	// the walk and at most MaxJobs.
	Matched int64 `json:"matched"`
	// MatchedCapped reports that more than MaxJobs jobs matched.
	MatchedCapped bool `json:"matched_capped"`
	// Changed is the number of jobs the call changed; for a dry run, how many it
	// would change.
	Changed int64 `json:"changed"`
	// Skipped counts the jobs left as they were, by reason.
	Skipped map[BulkSkipReason]int64 `json:"skipped"`
	// ByState counts the jobs the walk classified, by the state it classified
	// each from.
	ByState map[JobState]int64 `json:"by_state"`
	// ByKind counts the same jobs by kind.
	ByKind map[string]int64 `json:"by_kind"`
	// Sample is, for a dry run, up to 20 of the jobs it would change, the newest
	// first, read as JobViews (no args); empty for an apply.
	Sample []JobView `json:"sample,omitempty"`
	// Batches is the number of transactions that changed a job; a dry run
	// reports 0.
	Batches int `json:"batches"`
	// Done reports that the walk reached the end of the selection.
	Done bool `json:"done"`
	// ResumeAfter is the last job id this run passed: pass it as After to
	// continue an interrupted run. For a dry run it is the newest id it matched,
	// the MaxJobs-th when capped, which pins a later apply's MaxID to what the
	// preview saw.
	ResumeAfter string `json:"resume_after,omitempty"`
}

// The batch sizes a bulk run resolves BulkSpec.BatchSize to.
const (
	// defaultBulkBatchSize is a batch when BulkSpec.BatchSize is not positive.
	defaultBulkBatchSize = 500
	// maxBulkBatchSize is the largest batch: a staggered batch binds two
	// parameters per job, so 5,000 jobs stay under SQLite's bind limit.
	maxBulkBatchSize = 5000
)

// BulkJobs acts on every job spec selects, or with DryRun reports what it
// would do and changes nothing. It is the filter-wide form of the single-job
// controls: a cancel, retry, delete, reschedule, pause, or resume changes each
// job as CancelJob, RetryJobWithOptions, DeleteJob, RescheduleJob, PauseJob, or
// ResumeJob does, from the same states, and leaves every other job as it is,
// counted by reason in BulkResult.Skipped. No action changes a running job.
//
// # The selection
//
// spec.Query selects through QueryJobs' own filter code, so every JobQuery
// filter applies, the latest-run filters among them; spec.IDs selects exact ids
// instead. A soft-deleted job never matches, and After and MaxID bound the ids,
// so one selection applies to the count, the dry run, and every batch. Before
// it reads anything, the call refuses a spec bounded by neither a filter nor
// ids (ErrBulkUnbounded); a spec it can't run, with a *ValidationError naming
// the field (the action, MaxJobs, IDs beside a query, an option the action
// doesn't read, a reschedule with no time, or the actor); and a query QueryJobs
// refuses, with QueryJobs' own error, so ErrorContains on SQLite wraps
// ErrUnsupportedDialect.
//
// # The bounds
//
// It first counts the selection up to MaxJobs + 1. An apply over more than
// MaxJobs returns ErrBulkTooLarge, with Matched at MaxJobs and MatchedCapped
// set, and changes nothing; a dry run reports MatchedCapped and walks the first
// MaxJobs. An apply whose selection grows past MaxJobs while it walks stops
// there with ErrBulkTooLarge and keeps the batches it committed. So with MaxJobs
// at a preview's Matched and MaxID at its ResumeAfter, an apply acts on at most
// the jobs the preview saw, and refuses a selection that grew since, changing
// nothing.
//
// # Batches
//
// The walk goes by id, ascending, BatchSize jobs at a time. A dry run reads and
// classifies each batch, with no transaction and no lock, and counts what an
// apply over an unchanged selection would. An apply runs each batch in a
// transaction of its own (a savepoint when db is a caller's transaction): it
// re-reads the batch's jobs through the selection itself, locking their rows
// (FOR UPDATE on PostgreSQL; SQLite serializes writers), classifies each by the
// state it reads then, and updates exactly the rows still selected and in the
// action's source states. A job a concurrent call moved is classified by its
// new state; one that left the selection, or was deleted, is changed by nothing
// and counted in no bucket. A batch holds its rows' locks until it commits. A
// delete removes each job's finish-log entries, runs, and row, in
// DeleteFinishedJobsWithOptions' order, and keeps its history.
//
// # A forced retry
//
// Under Retry.Force a terminal job returns to available too, unless that would
// put a second live job on a UniqueActiveKey: of the jobs that share a free
// key, the first by id is retried, and every other, with every job whose key
// another live job holds, stays terminal, counted as BulkSkipActiveKey. A
// cancelled job whose attempt still runs counts as running. A batch whose
// update a concurrent write beats to a key, or that deadlocks, rolls back and
// runs again, at most three times. Stagger spreads due times as
// ReplayOpts.Stagger does: job i of the n matched is due at base + Stagger·i/n,
// base being the call's now plus Retry.Delay, or a reschedule's RescheduleAt.
//
// # Resuming
//
// The context is checked between batches. An interrupted run returns the result
// so far, Done false, with an error wrapping the context's: the batch in flight
// rolls back whole, and the committed ones stay. Called again with After set to
// its ResumeAfter, the run continues without repeating or skipping a job.
//
// # The history and barriers
//
// Each changed job gets one job_audit entry, in its batch's transaction,
// attributed to the context's actor (WithActor; the system when there is none),
// with details via "bulk", operation_id (OperationID), from_state, and the
// action's own keys, as its single-job control records them; a skipped job gets
// none. The context's AuditHook runs once per batch that wrote entries; its
// error rolls that batch back and is returned with the result so far. A cancel
// batch runs the barrier check of each parent whose last pending child it
// cancelled, so the continuation is enqueued once, from the spec the barrier
// stored, with the context's request id; the run never cancels a continuation
// it enqueued.
func BulkJobs(ctx context.Context, db *gorm.DB, spec BulkSpec) (BulkResult, error) {
	run, err := newBulkRun(ctx, db, spec)
	if err != nil {
		return BulkResult{}, err
	}
	return run.walk(ctx, db)
}

// bulkSampleSize is the most jobs a dry run's sample holds.
const bulkSampleSize = 20

// bulkRun is one BulkJobs call: its resolved spec, and the walk's state.
type bulkRun struct {
	spec      BulkSpec
	actor     Actor
	dialect   string
	maxJobs   int
	batchSize int
	// ids is the selection when spec.Query is empty: spec.IDs sorted, without
	// repeats. next is how many of them the walk has read.
	ids  []string
	next int
	// sources is the states the action changes a job from.
	sources []string
	// now is the call's clock reading, which a retry's due times count from.
	now time.Time

	result BulkResult
	// cursor is the last id the walk passed; passed is how many jobs it passed.
	cursor string
	passed int
	// taken holds the UniqueActiveKeys a forced retry has put live in this run.
	taken map[string]bool
	// fired holds the unique keys, barrier:<parent id>, of the continuations a
	// cancel run has enqueued.
	fired map[string]bool
	// sample holds the last bulkSampleSize ids a dry run would change, in id
	// order.
	sample []string
}

// bulkCandidate is one job a batch read.
type bulkCandidate struct {
	ID              string    `gorm:"column:id"`
	State           string    `gorm:"column:state"`
	Kind            string    `gorm:"column:kind"`
	ParentJobID     *string   `gorm:"column:parent_job_id"`
	UniqueKey       *string   `gorm:"column:unique_key"`
	UniqueActiveKey *string   `gorm:"column:unique_active_key"`
	ScheduledAt     time.Time `gorm:"column:scheduled_at"`
	Attempt         int       `gorm:"column:attempt"`
	// Held and AttemptRuns are read for a forced retry alone: whether another
	// live job holds the job's UniqueActiveKey, and whether it is cancelled
	// while its attempt still runs.
	Held        bool `gorm:"column:held"`
	AttemptRuns bool `gorm:"column:attempt_runs"`
}

// bulkReadColumns is what a batch reads of each job: what it classifies by and
// what the writes and their entries need, never the args.
func bulkReadColumns() []string {
	return []string{
		"jobs.id", "jobs.state", "jobs.kind", "jobs.parent_job_id", "jobs.unique_key", "jobs.unique_active_key",
		"jobs.scheduled_at", "jobs.attempt",
	}
}

// bulkBatch is one batch the walk read.
type bulkBatch struct {
	rows []bulkCandidate
	// last is where the walk continues from: the batch's last id.
	last string
	// end reports that the selection ends with this batch.
	end bool
}

// walk runs the call: it counts the selection up to MaxJobs + 1, refuses an
// apply over more than MaxJobs, then walks the matches by id in batches, a dry
// run reading and classifying each, an apply changing each in a transaction of
// its own.
func (r *bulkRun) walk(ctx context.Context, db *gorm.DB) (BulkResult, error) {
	r.result = BulkResult{
		Skipped: map[BulkSkipReason]int64{}, ByState: map[JobState]int64{}, ByKind: map[string]int64{},
	}
	r.cursor = r.spec.After
	r.now = models.ClockFrom(ctx).Now(ctx)
	if err := ctx.Err(); err != nil {
		return r.stopped(err)
	}
	matched, err := r.countSelection(ctx, db)
	if err != nil {
		return r.failed(fmt.Errorf("count the selection: %w", err))
	}
	r.result.Matched = min(matched, int64(r.maxJobs))
	r.result.MatchedCapped = matched > int64(r.maxJobs)
	if r.result.MatchedCapped && !r.spec.DryRun {
		return r.snapshot(), ErrBulkTooLarge
	}

	for {
		if err := ctx.Err(); err != nil {
			return r.stopped(err)
		}
		limit := min(r.batchSize, r.maxJobs-r.passed)
		if limit == 0 {
			if r.spec.DryRun {
				r.result.Done = !r.result.MatchedCapped
				break
			}
			// The walk passed MaxJobs matches: one more past the cursor means the
			// selection grew while it walked.
			more, err := r.probe(ctx, db)
			if err != nil {
				return r.failed(err)
			}
			if more {
				return r.snapshot(), ErrBulkTooLarge
			}
			r.result.Done = true
			break
		}
		b, err := r.readBatch(ctx, db, limit, r.spec.DryRun && r.forced())
		if err != nil {
			return r.failed(err)
		}
		if r.spec.DryRun {
			r.preview(b.rows)
		} else if len(b.rows) > 0 {
			if err := r.apply(ctx, db, b.rows); err != nil {
				if ctxErr := ctx.Err(); ctxErr != nil {
					return r.stopped(ctxErr)
				}
				return r.failed(err)
			}
		}
		r.passed += len(b.rows)
		r.cursor = b.last
		if hook := bulkHooksFrom(ctx).afterBatch; hook != nil {
			hook(r.snapshot())
		}
		if b.end {
			r.result.Done = true
			break
		}
	}
	sample, err := r.readSample(ctx, db)
	if err != nil {
		return r.failed(err)
	}
	r.result.Sample = sample
	return r.snapshot(), nil
}

// snapshot is the result so far.
func (r *bulkRun) snapshot() BulkResult {
	res := r.result
	res.ResumeAfter = r.cursor
	return res
}

// stopped returns the result so far for a run whose context ended, with an
// error wrapping the context's.
func (r *bulkRun) stopped(err error) (BulkResult, error) {
	return r.snapshot(), fmt.Errorf("flywheel: bulk %s cancelled after %d changed: %w", r.spec.Action, r.result.Changed, err)
}

// failed returns the result so far with err, named for the action.
func (r *bulkRun) failed(err error) (BulkResult, error) {
	return r.snapshot(), fmt.Errorf("flywheel: bulk %s: %w", r.spec.Action, err)
}

// forced reports whether the run is a forced retry, the one action that reads
// keys and running attempts.
func (r *bulkRun) forced() bool {
	return r.spec.Action == BulkRetry && r.spec.Retry.Force
}

// selection narrows stmt, a statement on jobs, to the jobs the spec selects:
// the query's filter, through QueryJobs' own code, or the given chunk of IDs;
// past After and at or below MaxID, each a bound value. Every statement the run
// issues selects through it.
func (r *bulkRun) selection(stmt *gorm.DB, chunk []string) *gorm.DB {
	if r.ids != nil {
		stmt = stmt.Where("jobs.id IN ?", chunk)
	} else {
		stmt = applyJobFilter(stmt, r.spec.Query, r.dialect)
	}
	if r.spec.After != "" {
		stmt = stmt.Where("jobs.id > ?", r.spec.After)
	}
	if r.spec.MaxID != "" {
		stmt = stmt.Where("jobs.id <= ?", r.spec.MaxID)
	}
	return stmt
}

// countSelection counts the selection up to MaxJobs + 1, which is all an
// apply's refusal needs: one subquery limited to that many rows, or with IDs,
// one per chunk until the count passes it.
func (r *bulkRun) countSelection(ctx context.Context, db *gorm.DB) (int64, error) {
	limit := r.maxJobs + 1
	upTo := func(sel *gorm.DB) (int64, error) {
		var n int64
		err := db.WithContext(ctx).Table("(?) AS s", sel.Select("1").Limit(limit)).Count(&n).Error
		return n, err
	}
	if r.ids == nil {
		return upTo(r.selection(db.WithContext(ctx).Model(&jobRow{}), nil))
	}
	var total int64
	for start := 0; start < len(r.ids) && total < int64(limit); start += r.batchSize {
		n, err := upTo(r.selection(db.WithContext(ctx).Model(&jobRow{}), r.ids[start:min(start+r.batchSize, len(r.ids))]))
		if err != nil {
			return 0, err
		}
		total += n
	}
	return total, nil
}

// readBatch reads the next batch of at most limit matches past the cursor, in
// id order, with the key and running-attempt flags when flags is set. The
// flags are read over a derived table already cut to the batch, as a replay
// batch reads them, so SQLite evaluates them for the batch's rows alone.
func (r *bulkRun) readBatch(ctx context.Context, db *gorm.DB, limit int, flags bool) (bulkBatch, error) {
	var chunk []string
	stmt := db.WithContext(ctx).Model(&jobRow{})
	if r.ids != nil {
		chunk = r.ids[r.next:min(r.next+limit, len(r.ids))]
		stmt = r.selection(stmt, chunk)
	} else {
		stmt = r.selection(stmt, nil)
		if r.cursor != "" {
			stmt = stmt.Where("jobs.id > ?", r.cursor)
		}
	}
	stmt = stmt.Select(bulkReadColumns()).Order("jobs.id").Limit(limit)
	if flags {
		stmt = db.WithContext(ctx).Table("(?) AS batch", stmt).
			Select("batch.*, " + activeKeyHeld("batch") + " AS held, " + cancelledAttemptStillRuns("batch") + " AS attempt_runs").
			Order("batch.id")
	}
	var rows []bulkCandidate
	if err := stmt.Find(&rows).Error; err != nil {
		return bulkBatch{}, fmt.Errorf("read a batch: %w", err)
	}
	b := bulkBatch{last: r.cursor}
	if r.ids != nil {
		r.next += len(chunk)
		b.end = r.next >= len(r.ids)
		if len(chunk) > 0 {
			b.last = chunk[len(chunk)-1]
		}
	} else {
		b.end = len(rows) < limit
		if len(rows) > 0 {
			b.last = rows[len(rows)-1].ID
		}
	}
	for _, c := range rows {
		if !r.continuation(c) {
			b.rows = append(b.rows, c)
		}
	}
	return b, nil
}

// probe reports whether the selection holds a match past the cursor, other
// than a continuation the run enqueued, reading past those a batch at a time.
// With IDs it never does: the walk reads every id, and there are at most
// MaxJobs.
func (r *bulkRun) probe(ctx context.Context, db *gorm.DB) (bool, error) {
	if r.ids != nil {
		return false, nil
	}
	for after := r.cursor; ; {
		var rows []bulkCandidate
		if err := r.selection(db.WithContext(ctx).Model(&jobRow{}), nil).Where("jobs.id > ?", after).
			Select("jobs.id", "jobs.unique_key").Order("jobs.id").Limit(r.batchSize).Find(&rows).Error; err != nil {
			return false, fmt.Errorf("probe past MaxJobs: %w", err)
		}
		for _, c := range rows {
			if !r.continuation(c) {
				return true, nil
			}
		}
		if len(rows) < r.batchSize {
			return false, nil
		}
		after = rows[len(rows)-1].ID
	}
}

// candidates narrows stmt, a statement on jobs, to the jobs in ids that the
// selection still selects: a batch's locked re-read and its update both go
// through it, so a job that left the selection since the batch read it is
// neither re-read nor changed.
func (r *bulkRun) candidates(stmt *gorm.DB, ids []string) *gorm.DB {
	if r.ids != nil {
		return r.selection(stmt, ids)
	}
	return r.selection(stmt, nil).Where("jobs.id IN ?", ids)
}

// bulkApplied is what one apply batch's transaction did, merged into the
// result only once it commits.
type bulkApplied struct {
	rows  []bulkCandidate
	skips []BulkSkipReason
	// changed is the rows the batch changed, in id order.
	changed []bulkCandidate
	// fired is the parents whose barrier a cancel batch fired.
	fired []string
}

// apply changes one batch in a transaction of its own. A batch that fails rolls
// back whole and counts nothing; one that lost a race for a key, or a deadlock
// or serialization failure (retryableBatchError), runs again, at most
// replayBatchRetries times, and the rerun reads the new holder under its lock.
func (r *bulkRun) apply(ctx context.Context, db *gorm.DB, read []bulkCandidate) error {
	ids := make([]string, len(read))
	for i, c := range read {
		ids[i] = c.ID
	}
	if hook := bulkHooksFrom(ctx).beforeTx; hook != nil {
		hook(ids)
	}
	var out bulkApplied
	for races := 0; ; races++ {
		err := db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
			var err error
			out, err = r.applyBatch(ctx, tx, ids)
			return err
		})
		if err == nil {
			break
		}
		if races < replayBatchRetries && retryableBatchError(err) {
			continue
		}
		return err
	}
	r.tally(out.rows, out.skips)
	r.result.Changed += int64(len(out.changed))
	if len(out.changed) > 0 {
		r.result.Batches++
	}
	r.take(out.changed)
	for _, parent := range out.fired {
		r.fired["barrier:"+parent] = true
	}
	return nil
}

// applyBatch re-reads ids, the batch's read, through the selection under a
// lock, classifies them by the states it reads, and changes the ones the action
// changes, on tx.
func (r *bulkRun) applyBatch(ctx context.Context, tx *gorm.DB, ids []string) (bulkApplied, error) {
	locked, err := r.lockCandidates(ctx, tx, ids)
	if err != nil {
		return bulkApplied{}, err
	}
	change, skips := r.classify(locked, true)
	out := bulkApplied{rows: locked, skips: skips}
	if len(change) == 0 {
		return out, nil
	}
	now := models.ClockFrom(ctx).Now(ctx)
	moved, err := r.write(tx, change, ids, now)
	if err != nil {
		return bulkApplied{}, err
	}
	out.changed = rowsIn(locked, moved)
	if r.spec.Action == BulkCancel {
		if out.fired, err = fireParentsBarriers(ctx, tx, out.changed); err != nil {
			return bulkApplied{}, err
		}
	}
	if err := recordAudit(ctx, tx, r.actor, now, r.auditChanges(out.changed, ids)); err != nil {
		return bulkApplied{}, err
	}
	return out, nil
}

// rowsIn returns the rows whose ids are in ids, in the rows' order.
func rowsIn(rows []bulkCandidate, ids []string) []bulkCandidate {
	in := make(map[string]bool, len(ids))
	for _, id := range ids {
		in[id] = true
	}
	out := make([]bulkCandidate, 0, len(ids))
	for _, c := range rows {
		if in[c.ID] {
			out = append(out, c)
		}
	}
	return out
}

// fireParentsBarriers runs the barrier check, on tx, once for each distinct
// parent of the jobs a cancel batch cancelled, in id order, as each
// CancelByParent batch does, so a batch that ends a generation's last pending
// children enqueues its continuation from the spec the barrier stored. It
// returns the parents whose barrier fired.
func fireParentsBarriers(ctx context.Context, tx *gorm.DB, cancelled []bulkCandidate) ([]string, error) {
	seen := map[string]bool{}
	var parents []string
	for _, c := range cancelled {
		if c.ParentJobID != nil && !seen[*c.ParentJobID] {
			seen[*c.ParentJobID] = true
			parents = append(parents, *c.ParentJobID)
		}
	}
	slices.Sort(parents)
	var fired []string
	for _, parent := range parents {
		ok, err := fireBarrierIfComplete(ctx, tx, parent, "")
		if err != nil {
			return nil, err
		}
		if ok {
			fired = append(fired, parent)
		}
	}
	return fired, nil
}

// continuation reports whether c is the barrier continuation of a parent this
// run fired. A continuation's UUIDv7 id sorts after every job matched before
// it, so a walk without MaxID would reach it in a later batch, and a selection
// that matches it (its queue, say) would cancel what the run just enqueued. The
// walk leaves such a job out after each read rather than in SQL: a NOT IN over
// the fired keys would bind one value per parent the run fired, which past tens
// of thousands of parents no driver accepts.
func (r *bulkRun) continuation(c bulkCandidate) bool {
	return c.UniqueKey != nil && r.fired[*c.UniqueKey]
}

// auditChanges is one entry per job a batch changed: the action's, with
// details via bulk, the operation id, the state the job changed from, and the
// action's own keys, as its single-job control records them. slots is the
// batch's read, for a reschedule's staggered due times.
func (r *bulkRun) auditChanges(changed []bulkCandidate, slots []string) []auditChange {
	action := bulkAuditAction(r.spec.Action)
	slot := make(map[string]int, len(slots))
	if r.spec.Action == BulkReschedule {
		for k, id := range slots {
			slot[id] = k
		}
	}
	changes := make([]auditChange, 0, len(changed))
	for _, c := range changed {
		details := map[string]any{"from_state": c.State}
		switch r.spec.Action {
		case BulkRetry:
			details = retryDetails(JobState(c.State), r.retry())
		case BulkCancel:
			details["was_running"] = false
		case BulkDelete:
			details["kind"] = c.Kind
			details["state"] = c.State
		case BulkReschedule:
			details["from"] = c.ScheduledAt.UTC().Format(time.RFC3339Nano)
			details["to"] = r.dueAt(r.passed + slot[c.ID]).UTC().Format(time.RFC3339Nano)
		}
		details["via"] = "bulk"
		details["operation_id"] = r.spec.OperationID
		changes = append(changes, auditChange{target: AuditTargetJob, targetID: c.ID, action: action, details: details})
	}
	return changes
}

// bulkAuditAction is the action an entry of a bulk action records, its
// single-job control's.
func bulkAuditAction(action BulkAction) AuditAction {
	switch action {
	case BulkCancel:
		return AuditCancelled
	case BulkRetry:
		return AuditRetried
	case BulkDelete:
		return AuditDeleted
	case BulkReschedule:
		return AuditRescheduled
	case BulkPause:
		return AuditPaused
	default: // BulkResume
		return AuditResumed
	}
}

// dueAt is when the job at the run's index i is due: base + Stagger·i/n over
// the n jobs the run matched, the placement scheduledAtValue writes.
func (r *bulkRun) dueAt(i int) time.Time {
	total := r.result.Matched
	if r.spec.Stagger <= 0 || total <= 0 {
		return r.base()
	}
	return r.base().Add(time.Duration(int64(r.spec.Stagger) * int64(i) / total))
}

// lockCandidates re-reads the jobs in ids the selection still selects, in id
// order, locking their rows on PostgreSQL (FOR UPDATE; SQLite serializes
// writers), with a forced retry's key and running-attempt flags read under the
// lock.
func (r *bulkRun) lockCandidates(ctx context.Context, tx *gorm.DB, ids []string) ([]bulkCandidate, error) {
	cols := bulkReadColumns()
	if r.forced() {
		cols = append(cols, activeKeyHeld("jobs")+" AS held", cancelledAttemptStillRuns("jobs")+" AS attempt_runs")
	}
	q := r.candidates(tx.WithContext(ctx).Model(&jobRow{}), ids).Select(strings.Join(cols, ", ")).Order("jobs.id")
	if tx.Name() == "postgres" {
		q = q.Clauses(clause.Locking{Strength: "UPDATE"})
	}
	var rows []bulkCandidate
	if err := q.Find(&rows).Error; err != nil {
		return nil, fmt.Errorf("lock a batch: %w", err)
	}
	return rows, nil
}

// write makes the action's change to change, on tx, at now, and returns the
// ids it changed, in id order; slots is the batch's read, every job of which
// keeps its stagger slot. An update goes through the selection again, guarded
// on the action's source states, and under a forced retry never puts a second
// live job on a key nor a cancelled job whose attempt still runs back to
// available; a delete removes each job's finish-log entries, runs, and row, in
// retention's order.
func (r *bulkRun) write(tx *gorm.DB, change []bulkCandidate, slots []string, now time.Time) ([]string, error) {
	ids := make([]string, len(change))
	for i, c := range change {
		ids[i] = c.ID
	}
	if r.spec.Action == BulkDelete {
		deleted, err := deleteJobRows(tx, ids)
		if err != nil {
			return nil, err
		}
		if deleted != int64(len(ids)) {
			return nil, fmt.Errorf("deleted %d of %d jobs: the others changed while they were locked", deleted, len(ids))
		}
		return ids, nil
	}
	guard := "id IN (?) AND state IN ?"
	if r.forced() {
		guard += " AND NOT " + activeKeyHeld("jobs") + " AND NOT " + cancelledAttemptStillRuns("jobs")
	}
	selected := r.candidates(tx.Model(&jobRow{}), ids).Select("jobs.id")
	return updateJobsReturningIDs(tx, guard, []any{selected, r.sources}, r.updates(slots, now))
}

// updates is the column map the action writes at now, the single-job
// control's, with a retry's and a reschedule's due times placed across slots
// by scheduledAtValue: job k of the batch lands at the run's index passed+k.
func (r *bulkRun) updates(slots []string, now time.Time) map[string]any {
	due := func(base time.Time) any {
		return scheduledAtValue(slots, base, r.spec.Stagger, int(r.result.Matched), r.passed)
	}
	switch r.spec.Action {
	case BulkCancel:
		return map[string]any{
			"state": string(StateCancelled), "leased_until": nil, "lease_token": nil,
			"finalized_at": now, "updated_at": now,
		}
	case BulkRetry:
		upd := retryUpdates(now, r.retry())
		upd["scheduled_at"] = due(r.base())
		return upd
	case BulkReschedule:
		return map[string]any{"scheduled_at": due(r.base()), "updated_at": now}
	case BulkPause:
		return map[string]any{"state": string(StatePaused), "updated_at": now}
	default: // BulkResume
		return map[string]any{"state": string(StateAvailable), "updated_at": now}
	}
}

// base is when a retry's or a reschedule's jobs are due before the stagger
// spreads them: the call's now plus Retry.Delay, or RescheduleAt.
func (r *bulkRun) base() time.Time {
	if r.spec.Action == BulkReschedule {
		return r.spec.RescheduleAt
	}
	return r.now.Add(r.spec.Retry.Delay)
}

// retry is the spec's retry options as the run applies them: AllowRunning is
// ignored, so a running job is never re-queued.
func (r *bulkRun) retry() RetryOpts {
	opts := r.spec.Retry
	opts.AllowRunning = false
	return opts
}

// preview classifies a dry run's batch and counts it as an apply over an
// unchanged selection would: the keys it would take are kept for later batches.
func (r *bulkRun) preview(rows []bulkCandidate) {
	change, skips := r.classify(rows, true)
	r.tally(rows, skips)
	r.result.Changed += int64(len(change))
	r.take(change)
	for _, c := range change {
		if len(r.sample) == bulkSampleSize {
			copy(r.sample, r.sample[1:])
			r.sample = r.sample[:bulkSampleSize-1]
		}
		r.sample = append(r.sample, c.ID)
	}
}

// tally counts rows by state and kind, and skips by reason.
func (r *bulkRun) tally(rows []bulkCandidate, skips []BulkSkipReason) {
	for _, c := range rows {
		r.result.ByState[JobState(c.State)]++
		r.result.ByKind[c.Kind]++
	}
	for _, s := range skips {
		r.result.Skipped[s]++
	}
}

// classify sorts a batch's candidates, in id order, into the jobs the action
// changes and a skip reason for each other. With keys set, a forced retry
// keeps a terminal job terminal (BulkSkipActiveKey) when another live job holds
// its UniqueActiveKey, the run took the key earlier, or an earlier job in the
// batch takes it: of the jobs that share a free key, the first by id goes live.
func (r *bulkRun) classify(rows []bulkCandidate, keys bool) ([]bulkCandidate, []BulkSkipReason) {
	keyed := keys && r.forced()
	live := map[string]bool{}
	if keyed {
		var held []replayCandidate
		for _, c := range rows {
			if r.keyRuled(c) {
				taken := r.taken[*c.UniqueActiveKey]
				held = append(held, replayCandidate{ID: c.ID, UniqueActiveKey: c.UniqueActiveKey, Held: c.Held || taken})
			}
		}
		for _, id := range liveReplayCandidates(held) {
			live[id] = true
		}
	}
	var change []bulkCandidate
	var skips []BulkSkipReason
	for _, c := range rows {
		reason := r.skipReason(c)
		if reason == "" && keyed && r.keyRuled(c) && !live[c.ID] {
			reason = BulkSkipActiveKey
		}
		if reason != "" {
			skips = append(skips, reason)
			continue
		}
		change = append(change, c)
	}
	return change, skips
}

// keyRuled reports whether the key rule decides c: a terminal job with a
// UniqueActiveKey that a forced retry would otherwise change. A live job
// already holds its key.
func (r *bulkRun) keyRuled(c bulkCandidate) bool {
	return c.UniqueActiveKey != nil && isTerminalStateString(c.State) && r.skipReason(c) == ""
}

// take records the keys the changed jobs put live, so a later batch's job
// sharing one is skipped.
func (r *bulkRun) take(changed []bulkCandidate) {
	if !r.forced() {
		return
	}
	for _, c := range changed {
		if c.UniqueActiveKey != nil {
			r.taken[*c.UniqueActiveKey] = true
		}
	}
}

// skipReason is why the action leaves c as it is, or "" when it changes it,
// by c's state: a source state changes, except a cancelled job under a forced
// retry whose attempt still runs, which counts as running; a running job is
// never changed; a terminal job is terminal, a live one a delete needs finished
// is not_terminal, and any other is already in the action's target state.
func (r *bulkRun) skipReason(c bulkCandidate) BulkSkipReason {
	if slices.Contains(r.sources, c.State) {
		if r.forced() && c.State == string(StateCancelled) && c.AttemptRuns {
			return BulkSkipRunning
		}
		return ""
	}
	switch {
	case c.State == string(StateRunning):
		return BulkSkipRunning
	case isTerminalStateString(c.State):
		return BulkSkipTerminal
	case r.spec.Action == BulkDelete:
		return BulkSkipNotTerminal
	default:
		return BulkSkipState
	}
}

// readSample reads a dry run's sample as views, the newest first.
func (r *bulkRun) readSample(ctx context.Context, db *gorm.DB) ([]JobView, error) {
	if len(r.sample) == 0 {
		return nil, nil
	}
	var rows []jobRow
	if err := db.WithContext(ctx).Model(&jobRow{}).Select(jobViewColumns()).
		Where("id IN ?", r.sample).Order("id DESC").Find(&rows).Error; err != nil {
		return nil, fmt.Errorf("read the sample: %w", err)
	}
	views := make([]JobView, len(rows))
	for i := range rows {
		views[i] = jobViewFromRow(rows[i])
	}
	return views, nil
}

// newBulkRun validates spec and resolves it, before any statement runs. It
// refuses, in this order: an actor the history can't record; an unknown
// action; a MaxJobs outside 0 to MaxBulkJobs; more IDs than MaxJobs, or IDs
// beside a query that filters; an option the action doesn't read, or a
// reschedule with no time; a query QueryJobs refuses, with its error; and
// last, a spec bounded by neither a filter nor ids (ErrBulkUnbounded).
func newBulkRun(ctx context.Context, db *gorm.DB, spec BulkSpec) (*bulkRun, error) {
	actor, err := resolveActor(ctx)
	if err != nil {
		return nil, err
	}
	if !slices.Contains(bulkActions(), spec.Action) {
		return nil, newValidationError("action",
			fmt.Sprintf("%q is not a bulk action: cancel, retry, delete, reschedule, pause, or resume", spec.Action))
	}
	if spec.MaxJobs < 0 || spec.MaxJobs > MaxBulkJobs {
		return nil, newValidationError("max_jobs", fmt.Sprintf("must be between 0 and %d", MaxBulkJobs))
	}
	maxJobs := spec.MaxJobs
	if maxJobs == 0 {
		maxJobs = DefaultBulkMaxJobs
	}
	ids := sortedUnique(spec.IDs)
	filtered := jobQueryFilters(spec.Query)
	switch {
	case len(ids) > maxJobs:
		return nil, newValidationError("ids", fmt.Sprintf("has more than MaxJobs (%d) entries", maxJobs))
	case len(ids) > 0 && filtered:
		return nil, newValidationError("ids", "cannot be combined with a query that filters: pass one or the other")
	}
	if err := validateBulkOptions(spec); err != nil {
		return nil, err
	}
	if err := validateJobFilter(ctx, spec.Query, db.Name()); err != nil {
		return nil, err
	}
	if !filtered && len(ids) == 0 {
		return nil, ErrBulkUnbounded
	}
	batchSize := spec.BatchSize
	switch {
	case batchSize <= 0:
		batchSize = defaultBulkBatchSize
	case batchSize > maxBulkBatchSize:
		batchSize = maxBulkBatchSize
	}
	return &bulkRun{
		spec: spec, actor: actor, dialect: db.Name(), maxJobs: maxJobs, batchSize: batchSize, ids: ids,
		sources: bulkSources(spec), taken: map[string]bool{}, fired: map[string]bool{},
	}, nil
}

// bulkSources returns the states spec's action changes a job from, each the
// single-job control's: a cancel, a retry, and a reschedule change a waiting
// job (a retry also a terminal one under Force, never a running one), a delete
// a terminal one, a pause a claimable one, and a resume a paused one.
func bulkSources(spec BulkSpec) []string {
	switch spec.Action {
	case BulkRetry:
		return retryStateStrings(RetryOpts{Force: spec.Retry.Force})
	case BulkDelete:
		return terminalStateStrings()
	case BulkPause:
		return stateStrings([]JobState{StateAvailable, StateRetryable, StateScheduled})
	case BulkResume:
		return []string{string(StatePaused)}
	default:
		return stateStrings(waitingStates())
	}
}

// bulkActions are the actions BulkJobs takes.
func bulkActions() []BulkAction {
	return []BulkAction{BulkCancel, BulkRetry, BulkDelete, BulkReschedule, BulkPause, BulkResume}
}

// validateBulkOptions refuses a reschedule with no time, and an option the
// spec's action doesn't read: RescheduleAt off a reschedule, Retry off a retry,
// and Stagger off a retry and a reschedule.
func validateBulkOptions(spec BulkSpec) error {
	switch {
	case spec.Action == BulkReschedule && spec.RescheduleAt.IsZero():
		return newValidationError("reschedule_at", "is required for a reschedule")
	case spec.Action != BulkReschedule && !spec.RescheduleAt.IsZero():
		return newValidationError("reschedule_at", "is read by a reschedule alone")
	case spec.Action != BulkRetry && spec.Retry != (RetryOpts{}):
		return newValidationError("retry", "is read by a retry alone")
	case spec.Action != BulkRetry && spec.Action != BulkReschedule && spec.Stagger != 0:
		return newValidationError("stagger", "is read by a retry or a reschedule alone")
	}
	return nil
}

// jobQueryFilters reports whether q sets any filter: every JobQuery field but
// Sort, Cursor, and Limit, which page a read rather than narrow it.
// TestBulkTreatsEveryJobQueryFilterAsABound fails when JobQuery gains a field
// this doesn't name.
func jobQueryFilters(q JobQuery) bool {
	return len(q.IDs) > 0 || len(q.States) > 0 || len(q.Kinds) > 0 || len(q.Queues) > 0 || len(q.Tags) > 0 ||
		q.ParentJobID != "" ||
		q.PriorityMin != nil || q.PriorityMax != nil || q.AttemptMin != nil || q.AttemptMax != nil ||
		!q.CreatedFrom.IsZero() || !q.CreatedTo.IsZero() ||
		!q.ScheduledFrom.IsZero() || !q.ScheduledTo.IsZero() ||
		!q.FinalizedFrom.IsZero() || !q.FinalizedTo.IsZero() ||
		len(q.ArgsContains) > 0 || len(q.MetadataContains) > 0 ||
		len(q.ErrorClasses) > 0 || q.Fingerprint != "" || q.ErrorContains != ""
}

// sortedUnique returns ids sorted, each once; nil for none.
func sortedUnique(ids []string) []string {
	if len(ids) == 0 {
		return nil
	}
	out := slices.Clone(ids)
	slices.Sort(out)
	return slices.Compact(out)
}

// bulkHooks are two points inside a bulk walk a test reaches through the
// context; production code never sets them. beforeTx runs after an apply
// batch's read and before its transaction, with the ids it read, so a test can
// move a selected job out of the selection; afterBatch runs after each batch,
// with the result so far, so a test can cancel between batches.
type bulkHooks struct {
	beforeTx   func(ids []string)
	afterBatch func(res BulkResult)
}

// bulkHooksKey is the context key withBulkHooks stores the hooks under.
type bulkHooksKey struct{}

// withBulkHooks returns a copy of ctx carrying h.
func withBulkHooks(ctx context.Context, h bulkHooks) context.Context {
	return context.WithValue(ctx, bulkHooksKey{}, h)
}

// bulkHooksFrom returns the hooks withBulkHooks put on ctx, or none.
func bulkHooksFrom(ctx context.Context) bulkHooks {
	h, _ := ctx.Value(bulkHooksKey{}).(bulkHooks)
	return h
}
