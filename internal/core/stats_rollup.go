package core

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/mrz1836/go-foundation/models"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// Stats rollup defaults, applied when the SchedulerConfig field is left zero.
const (
	// defaultStatsRollupGrace is how long past an hour's end the rollup waits
	// before closing it, so a run finalized a moment late — clock skew between
	// nodes, a finalize that committed just after the hour turned — still lands in
	// its own hour's rollup.
	defaultStatsRollupGrace = 5 * time.Minute
	// defaultStatsMaxHoursPerPass bounds one pass's work: a first pass over a long
	// history, or a catch-up after downtime, is spread across ticks rather than
	// taken in one.
	defaultStatsMaxHoursPerPass = 24
	// defaultStatsRetention is how long hourly rollup rows are kept: a little over
	// a year, so a year-over-year comparison has its whole range.
	defaultStatsRetention = 400 * 24 * time.Hour
)

// statsTotalKey is the kind or queue of a total row. Every rolled hour stores,
// beside its (kind, queue) group rows, one (kind, "") row per kind — that kind
// across every queue — and one ("", "") row for the whole hour. A read takes the
// narrowest class its filter allows: an all-kinds trend reads one row per hour
// rather than one per group. Real groups never carry the empty key — a run's kind
// is required, its queue defaults to "default", and a run whose kind or queue
// cannot be resolved groups as unknownStatsKey.
const statsTotalKey = ""

// unknownStatsKey is the kind and queue a run is grouped under when they cannot
// be resolved: a row an older binary wrote, whose job has since been deleted.
const unknownStatsKey = "(unknown)"

// rollupRows selects one class of job_stats_hourly rows. The classes are
// disjoint sums of the same runs, so a read takes exactly one.
type rollupRows int

const (
	// groupRows are the (kind, queue) rows.
	groupRows rollupRows = iota
	// kindRows are the per-kind totals, (kind, "").
	kindRows
	// totalRows are the whole-hour totals, ("", "").
	totalRows
)

// rowsFor picks the narrowest row class that can answer a read filtered by f:
// a queue filter needs the groups; a kind filter, or a per-kind breakdown, the
// per-kind totals; nothing narrower than the hour, the hour's total.
func rowsFor(f statsFilter, perKind bool) rollupRows {
	switch {
	case f.Queue != "":
		return groupRows
	case f.Kind != "" || perKind:
		return kindRows
	default:
		return totalRows
	}
}

// ErrStatsNotRolledUp is returned by Stats and StatsSeries when serving the
// requested window would aggregate more raw job_runs than StatsParams.MaxRawSpan
// allows — the window reaches past what the hourly rollups cover. The fix is to
// enable the rollup (SchedulerConfig.StatsRollupInterval) and let it catch up,
// or to backfill the range with RebuildStats.
var ErrStatsNotRolledUp = errors.New("flywheel: stats window is not covered by hourly rollups")

// runAgg is the aggregate of one (kind, queue)'s finished runs over a window: the
// shape both the hourly rollup stores and the stats reads merge. It is built by
// one function, aggregateRuns, whichever of the two is asking — which is what
// makes a window served from rollups identical to the same window served raw.
type runAgg struct {
	Kind, Queue string

	Attempts, Success, Error, Timeout, Snooze, Cancelled, Crashed int64
	Superseded, Discarded                                         int64

	DurCount, DurSumMs, DurMaxMs int64
	DurHist                      histogram
	// SlowestRunID is the successful run with the largest duration, ties broken
	// by the larger id, so the choice is the same however the window is split.
	SlowestRunID string

	WaitCount, WaitSumMs, WaitMaxMs int64
	WaitHist                        histogram

	CostMicrosSum int64
}

// newRunAgg returns an empty aggregate for one group.
func newRunAgg(kind, queue string) *runAgg {
	return &runAgg{Kind: kind, Queue: queue, DurHist: newHistogram(), WaitHist: newHistogram()}
}

// merge adds other into a. Counters and histograms add; maxima take the larger;
// the slowest run follows the duration maximum with the id tie-break.
func (a *runAgg) merge(other *runAgg) {
	a.Attempts += other.Attempts
	a.Success += other.Success
	a.Error += other.Error
	a.Timeout += other.Timeout
	a.Snooze += other.Snooze
	a.Cancelled += other.Cancelled
	a.Crashed += other.Crashed
	a.Superseded += other.Superseded
	a.Discarded += other.Discarded

	if other.DurCount > 0 && (other.DurMaxMs > a.DurMaxMs ||
		(other.DurMaxMs == a.DurMaxMs && other.SlowestRunID > a.SlowestRunID)) {
		a.SlowestRunID = other.SlowestRunID
	}
	a.DurCount += other.DurCount
	a.DurSumMs += other.DurSumMs
	a.DurMaxMs = max(a.DurMaxMs, other.DurMaxMs)
	a.DurHist.add(other.DurHist)

	a.WaitCount += other.WaitCount
	a.WaitSumMs += other.WaitSumMs
	a.WaitMaxMs = max(a.WaitMaxMs, other.WaitMaxMs)
	a.WaitHist.add(other.WaitHist)

	a.CostMicrosSum += other.CostMicrosSum
}

// statsFilter scopes an aggregate to one kind and/or one queue. Empty matches
// every value.
type statsFilter struct {
	Kind, Queue string
}

// resolvedKindExpr and resolvedQueueExpr are a run's kind and queue as the
// stats see them. A row this release wrote carries both. A row an older binary
// wrote — before the upgrade, or during a rolling deploy that mixes versions —
// carries the column default, the empty string, and is resolved through its job
// by primary key.
//
// The subquery is portable, and both dialects evaluate a CASE branch only when
// it is taken, so the lookup costs nothing on a row that carries its own kind.
// That is what lets the upgrade skip a backfill of the history: an old row
// resolves on read, at the price of one PK probe each, and only for as long as
// such rows sit inside a window being aggregated.
const (
	resolvedKindExpr = `CASE WHEN r.kind = '' THEN COALESCE((SELECT j.kind FROM jobs j WHERE j.id = r.job_id), '` +
		unknownStatsKey + `') ELSE r.kind END`
	resolvedQueueExpr = `CASE WHEN r.queue = '' THEN COALESCE((SELECT j.queue FROM jobs j WHERE j.id = r.job_id), '` +
		unknownStatsKey + `') ELSE r.queue END`
	// resolvedJobStateExpr is the job state a run applied. A row this release
	// finalized carries it. A failed attempt that does not — one an older binary
	// wrote, or one SeedRun imported, which cannot know the state its attempt
	// applied — has whether it discarded its job recovered through the job: it did
	// when its job is discarded and this was the job's last attempt. A superseded
	// attempt applied nothing, and is not looked up.
	resolvedJobStateExpr = `CASE WHEN r.job_state IS NOT NULL THEN r.job_state ` +
		`WHEN NOT r.superseded AND r.outcome IN ('error', 'timeout') THEN ` +
		`(SELECT CASE WHEN j.state = 'discarded' AND j.attempt = r.attempt THEN 'discarded' END ` +
		`FROM jobs j WHERE j.id = r.job_id) END`
)

// runWindowSQL is the derived table every aggregate reads: one row per run
// finished in [from, to), with its kind and queue resolved and the flags the
// counters need precomputed.
//
// The window is a range of job_run_finishes' primary key — the finish log is
// what makes job_runs searchable by finish time — and each entry reaches its run
// by job_runs' primary key. An entry whose run retention has since deleted joins
// to nothing and drops out.
const runWindowSQL = `SELECT ` +
	resolvedKindExpr + ` AS kind, ` +
	resolvedQueueExpr + ` AS queue, ` +
	`r.id AS id, r.outcome AS outcome, ` +
	`CASE WHEN r.superseded THEN 1 ELSE 0 END AS sup, ` +
	resolvedJobStateExpr + ` AS job_state, ` +
	`r.duration_ms AS duration_ms, r.queue_wait_ms AS queue_wait_ms, r.cost_micros AS cost_micros ` +
	`FROM job_run_finishes f JOIN job_runs r ON r.id = f.run_id ` +
	`WHERE f.finished_at >= ? AND f.finished_at < ?`

// filterSQL renders f as a WHERE over the derived table's resolved columns.
func filterSQL(f statsFilter) (string, []any) {
	var conds []string
	var args []any
	if f.Kind != "" {
		conds = append(conds, `t.kind = ?`)
		args = append(args, f.Kind)
	}
	if f.Queue != "" {
		conds = append(conds, `t.queue = ?`)
		args = append(args, f.Queue)
	}
	if len(conds) == 0 {
		return "", nil
	}
	return ` WHERE ` + strings.Join(conds, " AND "), args
}

// aggregateRuns aggregates the runs finished in [from, to) per (kind, queue),
// finding them through the job_run_finishes log. It is the one aggregate code
// path: the hourly rollup stores its result, and the stats reads call it directly
// for the part of a window the rollups do not cover yet.
//
// It reads the window once and aggregates in Go. Each run in the window costs
// the database one primary-key probe of job_runs, and that probe is the whole
// cost of the read; computing the counters, both histograms, and the slowest run
// in SQL would take one pass of probes per statement, where streaming the rows
// takes one pass in all. The rows carry only what the aggregate needs, and Go
// buckets each duration with histBucket — exactly the bounds every stored
// histogram uses.
//
// Bounds are bound in UTC, the zone every job_runs timestamp is stamped in, so
// the comparison is right on SQLite, where it is textual.
func aggregateRuns(ctx context.Context, db *gorm.DB, from, to time.Time, f statsFilter) (map[statsGroup]*runAgg, error) {
	out := map[statsGroup]*runAgg{}
	if !from.Before(to) {
		return out, nil
	}
	where, filterArgs := filterSQL(f)
	rows, err := db.WithContext(ctx).Raw(`SELECT t.kind, t.queue, t.id, t.outcome, t.sup, t.job_state, `+
		`t.duration_ms, t.queue_wait_ms, t.cost_micros FROM (`+runWindowSQL+`) t`+where,
		append([]any{from.UTC(), to.UTC()}, filterArgs...)...).Rows()
	if err != nil {
		return nil, fmt.Errorf("aggregate runs: %w", err)
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var (
			kind, queue, id, outcome string
			sup                      int64
			jobState                 *string
			duration, wait, cost     *int64
		)
		if err := rows.Scan(&kind, &queue, &id, &outcome, &sup, &jobState, &duration, &wait, &cost); err != nil {
			return nil, fmt.Errorf("aggregate runs: scan: %w", err)
		}
		g := statsGroup{Kind: kind, Queue: queue}
		a, ok := out[g]
		if !ok {
			a = newRunAgg(kind, queue)
			out[g] = a
		}
		a.observe(id, RunOutcome(outcome), sup != 0, jobState, duration, wait, cost)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("aggregate runs: %w", err)
	}
	return out, nil
}

// observe counts one finished run into a. A superseded run is counted only as
// superseded: its outcome was never applied to the job, so it is in no outcome
// counter and no duration or wait distribution. Durations are successful runs
// only.
//
// A run the lease sweep reclaimed is counted as the crash it was when it
// finished — job state available, which only the sweep writes — whatever its
// row says now. The attempt may still finish after the reclaim, and its late,
// superseded finalize then records its real outcome, duration, and cost on the
// row; but it keeps the row's finished_at, job state, and queue wait. Counting
// by those keeps the run where the sweep put it — one crashed attempt, one
// retry, its wait, and nothing else — so an hour rolled up before the late
// finalize equals the same hour aggregated after it.
func (a *runAgg) observe(
	id string, outcome RunOutcome, superseded bool, jobState *string, duration, wait, cost *int64,
) {
	if jobState != nil && *jobState == string(StateAvailable) {
		outcome, superseded, duration, cost = OutcomeCrashed, false, nil, nil
	}
	if superseded {
		a.Superseded++
		return
	}
	a.Attempts++
	switch outcome {
	case OutcomeSuccess:
		a.Success++
	case OutcomeError:
		a.Error++
	case OutcomeTimeout:
		a.Timeout++
	case OutcomeSnooze:
		a.Snooze++
	case OutcomeCancelled:
		a.Cancelled++
	case OutcomeCrashed:
		a.Crashed++
	}
	if jobState != nil && *jobState == string(StateDiscarded) {
		a.Discarded++
	}
	if outcome == OutcomeSuccess && duration != nil {
		d := *duration
		if a.DurCount == 0 || d > a.DurMaxMs || (d == a.DurMaxMs && id > a.SlowestRunID) {
			a.SlowestRunID = id
		}
		a.DurCount++
		a.DurSumMs += d
		a.DurMaxMs = max(a.DurMaxMs, d)
		a.DurHist.observe(d)
	}
	if wait != nil {
		a.WaitCount++
		a.WaitSumMs += *wait
		a.WaitMaxMs = max(a.WaitMaxMs, *wait)
		a.WaitHist.observe(*wait)
	}
	if cost != nil {
		a.CostMicrosSum += *cost
	}
}

// statsGroup keys an aggregate by (kind, queue).
type statsGroup struct {
	Kind, Queue string
}

// hourRows renders one hour's aggregates as the rows the rollup stores: one per
// (kind, queue) group, one per-kind total, and the hour's total (see
// statsTotalKey), each with the percentile columns a direct-SQL trend chart reads.
// An hour with no aggregates renders as no rows: job_stats_progress, not a row,
// records that it was rolled.
func hourRows(bucket time.Time, aggs map[statsGroup]*runAgg, rolledAt time.Time) []jobStatsHourlyRow {
	if len(aggs) == 0 {
		return nil
	}
	out := make([]jobStatsHourlyRow, 0, len(aggs)*2+1)
	kinds := map[string]*runAgg{}
	total := newRunAgg(statsTotalKey, statsTotalKey)
	for g, a := range aggs {
		out = append(out, rowFromAgg(bucket, a, rolledAt))
		k, ok := kinds[g.Kind]
		if !ok {
			k = newRunAgg(g.Kind, statsTotalKey)
			kinds[g.Kind] = k
		}
		k.merge(a)
		total.merge(a)
	}
	for _, k := range kinds {
		out = append(out, rowFromAgg(bucket, k, rolledAt))
	}
	out = append(out, rowFromAgg(bucket, total, rolledAt))
	// Key order, so every writer of the hour inserts its rows in the same
	// sequence; the maps above iterate in a different order each time.
	sort.Slice(out, func(i, j int) bool {
		if out[i].Kind != out[j].Kind {
			return out[i].Kind < out[j].Kind
		}
		return out[i].Queue < out[j].Queue
	})
	return out
}

// rowFromAgg renders one aggregate as a stored row.
func rowFromAgg(bucket time.Time, a *runAgg, rolledAt time.Time) jobStatsHourlyRow {
	row := jobStatsHourlyRow{
		BucketStartUnix: bucket.Unix(),
		Kind:            a.Kind,
		Queue:           a.Queue,
		Attempts:        a.Attempts,
		Success:         a.Success,
		Error:           a.Error,
		Timeout:         a.Timeout,
		Snooze:          a.Snooze,
		Cancelled:       a.Cancelled,
		Crashed:         a.Crashed,
		Superseded:      a.Superseded,
		Discarded:       a.Discarded,
		DurCount:        a.DurCount,
		DurSumMs:        a.DurSumMs,
		DurMaxMs:        a.DurMaxMs,
		DurP50Ms:        a.DurHist.quantile(0.50, a.DurMaxMs),
		DurP95Ms:        a.DurHist.quantile(0.95, a.DurMaxMs),
		DurP99Ms:        a.DurHist.quantile(0.99, a.DurMaxMs),
		DurHist:         a.DurHist.marshal(),
		WaitCount:       a.WaitCount,
		WaitSumMs:       a.WaitSumMs,
		WaitMaxMs:       a.WaitMaxMs,
		WaitP95Ms:       a.WaitHist.quantile(0.95, a.WaitMaxMs),
		WaitHist:        a.WaitHist.marshal(),
		CostMicrosSum:   a.CostMicrosSum,
		HistVersion:     histVersion,
		RolledAt:        rolledAt.UTC(),
	}
	if a.SlowestRunID != "" {
		id := a.SlowestRunID
		row.SlowestRunID = &id
	}
	return row
}

// errRollupWouldShrink is returned by a guarded rollupHour when the recompute
// counts fewer runs than the rollup already stores for the hour — the signature
// of raw history retention has pruned since the hour was rolled.
var errRollupWouldShrink = errors.New("recompute counts fewer runs than the stored rollup")

// rollupHour recomputes one closed hour and replaces its rollup rows, in one
// transaction: delete the hour, then upsert what the recompute produced. The
// delete is what gives replace semantics — a group the recompute no longer
// produces (re-resolved to another kind, say) disappears rather than lingering —
// and the upsert is what makes a second scheduler racing the first on the same
// hour harmless: both compute the same rows, and whichever commits second
// overwrites the first with identical values.
//
// guard refuses a recompute that would count fewer runs than the stored rows,
// returning errRollupWouldShrink and leaving the hour untouched. RebuildStats
// sets it, and so does the re-roll after a backfill; the rollup activity, which
// rolls hours no rollup has covered yet, does not need it.
//
// step extends the covered range across the hour, in the same transaction, when
// the hour is one the range has not reached; a zero step leaves the range alone.
// Either way the transaction bumps job_stats_progress's version, which is what
// tells every process's Baselines memo that a rolled hour changed.
//
// An hour with no runs writes no rows; it is still recorded as rolled, by the
// step.
//
// It reports the number of (kind, queue) groups written.
func rollupHour(
	ctx context.Context, db *gorm.DB, hour, rolledAt time.Time, guard bool, step coverageStep,
) (int, error) {
	aggs, err := aggregateRuns(ctx, db, hour, hour.Add(time.Hour), statsFilter{})
	if err != nil {
		return 0, err
	}
	rows := hourRows(hour, aggs, rolledAt)
	err = db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := lockStatsHour(tx, hour); err != nil {
			return err
		}
		if guard {
			if err := refuseShrink(tx, hour, aggs); err != nil {
				return err
			}
		}
		if err := tx.Where("bucket_start_unix = ?", hour.Unix()).Delete(&jobStatsHourlyRow{}).Error; err != nil {
			return fmt.Errorf("clear hour: %w", err)
		}
		if len(rows) > 0 {
			if err := tx.Clauses(clause.OnConflict{UpdateAll: true}).Create(&rows).Error; err != nil {
				return fmt.Errorf("write hour: %w", err)
			}
		}
		return applyCoverageStep(tx, step, rolledAt)
	})
	if err != nil {
		return 0, fmt.Errorf("roll up hour %s: %w", hour.UTC().Format(time.RFC3339), err)
	}
	return len(aggs), nil
}

// statsLockClass is the first key of the advisory lock that serializes writers
// of one rollup hour on PostgreSQL: "flyw" as an int32, a namespace no host
// lock is likely to share. The second key is the hour.
const statsLockClass = 0x666c7977

// lockStatsHour serializes, on PostgreSQL, every transaction replacing the same
// hour: a transaction-scoped advisory lock keyed on it, released at commit.
//
// Without it two replaces of one hour — a duplicate Scheduler, or RebuildStats
// overlapping the rollup — delete and re-insert the same keys concurrently, and
// each can end up waiting on a key the other inserted: a deadlock PostgreSQL
// breaks by failing one of them. Serialized, the second replace simply runs after
// the first and writes the same rows. SQLite needs no lock: it has one writer.
func lockStatsHour(tx *gorm.DB, hour time.Time) error {
	if tx.Name() != "postgres" {
		return nil
	}
	if err := tx.Exec(`SELECT pg_advisory_xact_lock(?, ?)`,
		statsLockClass, int32(hour.Unix()/3600)).Error; err != nil { //nolint:gosec // hours since 1970 fit an int32 until the year 246,000
		return fmt.Errorf("lock hour: %w", err)
	}
	return nil
}

// refuseShrink compares a recompute's run count for hour against what the
// stored rollup rows count, inside the replacing transaction.
func refuseShrink(tx *gorm.DB, hour time.Time, aggs map[statsGroup]*runAgg) error {
	var stored struct{ N int64 }
	if err := tx.Model(&jobStatsHourlyRow{}).
		Select("CAST(COALESCE(SUM(attempts + superseded), 0) AS BIGINT) AS n").
		Where("bucket_start_unix = ? AND kind = ? AND queue = ?", hour.Unix(), statsTotalKey, statsTotalKey).
		Scan(&stored).Error; err != nil {
		return fmt.Errorf("read stored hour: %w", err)
	}
	var recomputed int64
	for _, a := range aggs {
		recomputed += a.Attempts + a.Superseded
	}
	if recomputed < stored.N {
		return fmt.Errorf("%w (%d stored, %d recomputed)", errRollupWouldShrink, stored.N, recomputed)
	}
	return nil
}

// floorHour truncates t to the start of its UTC hour.
func floorHour(t time.Time) time.Time {
	return t.UTC().Truncate(time.Hour)
}

// ceilHour rounds t up to the start of the next UTC hour, or returns it when it
// starts one.
func ceilHour(t time.Time) time.Time {
	h := floorHour(t)
	if h.Before(t.UTC()) {
		return h.Add(time.Hour)
	}
	return h
}

// prevFinishedAt returns the latest finish in [lo, hi), or ok false when there
// is none: nextFinishedAt's mirror, one descending probe of job_run_finishes'
// primary key.
func prevFinishedAt(ctx context.Context, db *gorm.DB, lo, hi time.Time) (time.Time, bool, error) {
	var at []time.Time
	if err := db.WithContext(ctx).Model(&jobRunFinishRow{}).
		Where("finished_at >= ? AND finished_at < ?", lo.UTC(), hi.UTC()).
		Order("finished_at DESC").Limit(1).Pluck("finished_at", &at).Error; err != nil {
		return time.Time{}, false, fmt.Errorf("probe previous finished run: %w", err)
	}
	if len(at) == 0 {
		return time.Time{}, false, nil
	}
	return at[0].UTC(), true, nil
}

// nextFinishedAt returns the earliest finish at or after from, or ok false when
// no run has finished since. It is one ordered probe of job_run_finishes' primary
// key — an ordered read of the typed column rather than MIN(), which SQLite
// returns as text — and it is how the rollup skips a quiet stretch in one step
// instead of visiting every empty hour.
func nextFinishedAt(ctx context.Context, db *gorm.DB, from time.Time) (time.Time, bool, error) {
	var at []time.Time
	if err := db.WithContext(ctx).Model(&jobRunFinishRow{}).
		Where("finished_at >= ?", from.UTC()).
		Order("finished_at").Limit(1).Pluck("finished_at", &at).Error; err != nil {
		return time.Time{}, false, fmt.Errorf("probe next finished run: %w", err)
	}
	if len(at) == 0 {
		return time.Time{}, false, nil
	}
	return at[0].UTC(), true, nil
}

// rollupConfig is one rollup pass's parameters, resolved from SchedulerConfig.
type rollupConfig struct {
	grace     time.Duration
	maxHours  int
	retention time.Duration
}

// RollupResult reports what one stats rollup pass did.
type RollupResult struct {
	// Hours is the closed hours rolled up with runs in them. Empty hours, skipped
	// in one probe per quiet stretch, are not counted.
	Hours int `json:"hours"`
	// Rolled lists the start of each hour written, in hour order.
	Rolled []time.Time `json:"rolled"`
	// RolledFrom and Watermark bound the hours the rollups cover after the pass:
	// every hour in [RolledFrom, Watermark) is rolled up.
	RolledFrom time.Time `json:"rolled_from"`
	Watermark  time.Time `json:"watermark"`
	// CaughtUp is true when the pass reached the last closed hour and the
	// covered range reaches back to the retention; false when it stopped on its
	// per-pass ceiling with hours still to roll.
	CaughtUp bool `json:"caught_up"`
	// Pruned is the rollup rows deleted for being older than the retention.
	Pruned int64 `json:"pruned"`
}

// rollupPass rolls up closed hours into job_stats_hourly, at most cfg.maxHours
// of them, extending the covered range recorded in job_stats_progress, then
// prunes rollup rows older than the retention.
//
// An hour is closed once its end plus the grace is in the past. The pass first
// raises the range to the retention boundary (now minus cfg.retention), creating
// it there on the first pass. It then works forward from the range's upper edge
// to the last closed hour — the new hours dashboards want first — and, with
// whatever budget is left, backward from the range's lower edge toward the
// retention boundary, which matters only when the range starts above it (a
// RebuildStats created it, or the retention grew). So enabling the rollup on a
// long-lived database backfills its history a bounded pass at a time, and a
// stretch with no runs costs one indexed probe, however long.
func rollupPass(ctx context.Context, db *gorm.DB, now time.Time, cfg rollupConfig) (RollupResult, error) {
	var res RollupResult
	closedBefore := floorHour(now.Add(-cfg.grace))
	earliest := floorHour(now.Add(-cfg.retention))
	if closedBefore.Before(earliest) {
		earliest = closedBefore
	}

	p, err := advanceStatsFloor(ctx, db, earliest, now)
	if err != nil {
		return res, err
	}
	fwd, err := rollForward(ctx, db, p.To, closedBefore, now, false, cfg.maxHours)
	if err != nil {
		return res, fmt.Errorf("stats rollup: %w", err)
	}
	back := rollWalk{done: true}
	if left := cfg.maxHours - len(fwd.rolled); fwd.done && left > 0 {
		if back, err = rollBackward(ctx, db, earliest, p.From, now, false, left); err != nil {
			return res, fmt.Errorf("stats rollup: %w", err)
		}
	} else if earliest.Before(p.From) {
		back.done = false
	}
	for i := len(back.rolled) - 1; i >= 0; i-- {
		res.Rolled = append(res.Rolled, back.rolled[i])
	}
	res.Rolled = append(res.Rolled, fwd.rolled...)
	res.Hours = len(res.Rolled)
	res.CaughtUp = fwd.done && back.done

	if p, _, err = readStatsProgress(ctx, db); err != nil {
		return res, err
	}
	res.RolledFrom, res.Watermark = p.From, p.To

	// Prune by primary-key range: the hours below the retention boundary, which
	// the range was raised past above.
	pruned := db.WithContext(ctx).Where("bucket_start_unix < ?", earliest.Unix()).Delete(&jobStatsHourlyRow{})
	if pruned.Error != nil {
		return res, fmt.Errorf("prune stats rollups: %w", pruned.Error)
	}
	res.Pruned = pruned.RowsAffected
	return res, nil
}

// RebuildOpts scopes a RebuildStats call. From and To are required; Force is off
// by default.
type RebuildOpts struct {
	// From and To bound the hours rebuilt: every UTC hour that starts in
	// [From, To), with From floored to its hour. To is clamped to the last closed
	// hour — the open hour and the grace window belong to the rollup activity.
	From, To time.Time
	// Grace is the closing grace the clamp uses. Zero selects the rollup's
	// default (5m); pass the Scheduler's StatsRollupGrace when it differs.
	Grace time.Duration
	// Force rebuilds an hour even when the recompute counts fewer runs than the
	// stored rollup. Without it such an hour stops the rebuild: fewer raw runs than
	// were rolled up means retention (or a host) has deleted some since, and the
	// recompute would replace a complete rollup with a partial one. Set it only to
	// repair a rollup you know is wrong.
	Force bool
}

// RebuildResult reports what a RebuildStats call did.
type RebuildResult struct {
	// From and To are the hour range requested, after flooring and clamping.
	From time.Time `json:"from"`
	To   time.Time `json:"to"`
	// Hours is the number of hours recomputed — the requested range's, and any
	// with runs in a gap rolled to keep the covered range contiguous; Groups the
	// (kind, queue) rollup rows written across them.
	Hours  int `json:"hours"`
	Groups int `json:"groups"`
	// RolledFrom and RolledThrough bound the hours the rollups cover after the
	// rebuild — the range Stats serves from them.
	RolledFrom    time.Time `json:"rolled_from"`
	RolledThrough time.Time `json:"rolled_through"`
}

// RebuildStats recomputes the hourly rollups for a range of closed hours from
// the raw job_runs, replacing whatever the rollup stored. It is the repair for
// runs that landed in an hour after the rollup closed it — clock skew beyond the
// grace, a SeedRun import of history — and the fast way to roll up a range
// without waiting out the rollup activity's per-pass ceiling.
//
// Each hour is replaced in its own transaction, exactly as the rollup activity
// replaces it, so a rebuild and a running rollup can overlap safely, and a
// cancelled rebuild leaves every hour it finished rebuilt.
//
// # The covered range
//
// The rollups serve exactly the hours job_stats_progress records as covered,
// and that range never has a gap. A rebuild extends it: hours past its upper
// edge are rebuilt oldest first, each extending the range up as it commits, and
// hours below its lower edge newest first, extending it down. When the requested
// range does not touch the covered one, the hours between are rolled too — only
// those with runs cost a recompute; an empty stretch is one probe — so a
// rebuild of recent hours while the rollup is still catching up on older ones
// rolls everything in between rather than leaving it uncounted. On a database
// no rollup has run on, the rebuilt range becomes the covered range; a rollup
// enabled later extends it both ways.
//
// It sees the runs the finish log holds. Runs an older release finalized —
// before an upgrade, or during a rolling deploy — are visible once
// BackfillRunFinishes has written their entries; the rollup activity does that
// on its own, `flywheel stats rebuild` runs it first, and the backfill re-rolls
// any covered hour it adds runs to, so the order of the two does not matter.
//
// Without Force it refuses to shrink an hour: when the recompute counts fewer
// runs than the stored rollup, the rebuild stops at that hour with an error
// wrapping ErrValidation, leaving it and every hour not yet reached as they
// were. Raw history is what retention prunes; the rollup is what outlives it.
func RebuildStats(ctx context.Context, db *gorm.DB, opts RebuildOpts) (RebuildResult, error) {
	if db == nil {
		return RebuildResult{}, fmt.Errorf("flywheel: RebuildStats: db is nil")
	}
	if opts.From.IsZero() || opts.To.IsZero() {
		return RebuildResult{}, newValidationError("rebuild range", "requires both From and To")
	}
	now := models.ClockFrom(ctx).Now(ctx)
	grace := opts.Grace
	if grace <= 0 {
		grace = defaultStatsRollupGrace
	}
	from := floorHour(opts.From)
	to := opts.To.UTC()
	if closed := floorHour(now.Add(-grace)); to.After(closed) {
		to = closed
	}
	res := RebuildResult{From: from, To: to}
	if end := ceilHour(to); from.Before(end) {
		if err := rebuildRange(ctx, db, from, end, now, !opts.Force, &res); err != nil {
			if errors.Is(err, errRollupWouldShrink) {
				return res, fmt.Errorf("flywheel: RebuildStats: %w", &ValidationError{
					Field: "rebuild range",
					Message: fmt.Sprintf("%v; its raw runs were pruned after it was rolled up "+
						"(set Force to rebuild it anyway)", err),
				})
			}
			return res, fmt.Errorf("flywheel: RebuildStats: %w", err)
		}
	}
	p, _, err := readStatsProgress(ctx, db)
	if err != nil {
		return res, fmt.Errorf("flywheel: RebuildStats: %w", err)
	}
	res.RolledFrom, res.RolledThrough = p.From, p.To
	return res, nil
}

// rebuildRange rebuilds every hour in [from, end) relative to the covered
// range: the hours inside it in place, then the hours above it — after rolling
// any gap below them — oldest first, then the hours below it — after any gap
// above them — newest first, so every hour that extends the range is adjacent to
// it when it commits.
func rebuildRange(
	ctx context.Context, db *gorm.DB, from, end, now time.Time, guard bool, res *RebuildResult,
) error {
	if err := ensureStatsProgress(ctx, db, from, now); err != nil {
		return err
	}
	p, _, err := readStatsProgress(ctx, db)
	if err != nil {
		return err
	}
	rebuild := func(hour time.Time, step coverageStep) error {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("cancelled after %d hours: %w", res.Hours, err)
		}
		n, err := rollupHour(ctx, db, hour, now, guard, step)
		if err != nil {
			return err
		}
		res.Hours++
		res.Groups += n
		return nil
	}
	walked := func(w rollWalk) {
		res.Hours += len(w.rolled)
		res.Groups += w.groups
	}

	for hour := maxTime(from, p.From); hour.Before(minTime(end, p.To)); hour = hour.Add(time.Hour) {
		if err := rebuild(hour, coverageStep{}); err != nil {
			return err
		}
	}
	if p.To.Before(end) {
		start := maxTime(from, p.To)
		gap, err := rollForward(ctx, db, p.To, start, now, guard, 0)
		walked(gap)
		if err != nil {
			return err
		}
		for hour := start; hour.Before(end); hour = hour.Add(time.Hour) {
			if err := rebuild(hour, stepUp(hour, hour.Add(time.Hour))); err != nil {
				return err
			}
		}
	}
	if from.Before(p.From) {
		top := minTime(end, p.From)
		gap, err := rollBackward(ctx, db, top, p.From, now, guard, 0)
		walked(gap)
		if err != nil {
			return err
		}
		for hour := top.Add(-time.Hour); !hour.Before(from); hour = hour.Add(-time.Hour) {
			if err := rebuild(hour, stepDown(hour, hour.Add(time.Hour))); err != nil {
				return err
			}
		}
	}
	return nil
}

// minTime and maxTime are min and max for times.
func minTime(a, b time.Time) time.Time {
	if b.Before(a) {
		return b
	}
	return a
}

func maxTime(a, b time.Time) time.Time {
	if b.After(a) {
		return b
	}
	return a
}

// backfillBatchSize is the number of job_runs rows BackfillRunFinishes examines
// per statement.
const backfillBatchSize = 1000

// unloggedLegacyRunPredicate selects, over job_runs r, the finished runs an
// older binary wrote that have no finish-log entry yet. An older binary leaves
// kind at its column default and writes no entry; this release stamps every
// run's kind. A run SeedRun imported against a job that no longer exists also
// has an empty kind, but SeedRun logs its finish, so the entry check excludes
// it.
const unloggedLegacyRunPredicate = `r.kind = '' AND r.finished_at IS NOT NULL AND NOT EXISTS ` +
	`(SELECT 1 FROM job_run_finishes f WHERE f.finished_at = r.finished_at AND f.run_id = r.id)`

// BackfillRunFinishes writes the job_run_finishes entry of every finished run
// that lacks one, and reports how many it wrote. It exists for the upgrade: a run
// finalized by a release older than the finish log has no entry, so the stats
// reads — which find runs through the log — cannot see it until this writes one.
//
// The stats rollup runs it on its own: on a Scheduler's first pass, and once
// more ten minutes later, which covers the runs an older binary still finalizes
// while a rolling deploy overlaps the two; `flywheel stats rebuild` runs it
// before rebuilding. A host calls it directly to make older runs visible without
// running the rollup at all.
//
// It first asks whether there is anything to do — one statement that reads
// job_runs once and stops at the first unlogged run — and returns at once when
// there is not, which after the upgrade is always. When there is, it walks
// job_runs in primary-key batches and logs exactly the unlogged runs each batch
// holds. On SQLite it first re-stamps legacy timestamps in UTC
// (NormalizeRunTimestamps), so every entry is written in the zone the log is
// compared in. It is idempotent.
//
// A backfilled run whose hour the rollups already cover — an older binary
// finalized it after the rollup closed its hour — is counted by re-rolling that
// hour, guarded like a RebuildStats: an hour whose recompute would count fewer
// runs than its stored rollup is left as it was.
func BackfillRunFinishes(ctx context.Context, db *gorm.DB) (int64, error) {
	if db == nil {
		return 0, fmt.Errorf("flywheel: BackfillRunFinishes: db is nil")
	}
	if _, err := NormalizeRunTimestamps(ctx, db); err != nil {
		return 0, fmt.Errorf("flywheel: BackfillRunFinishes: %w", err)
	}
	var probe []string
	if err := db.WithContext(ctx).Raw(`SELECT r.id FROM job_runs r WHERE ` + unloggedLegacyRunPredicate +
		` LIMIT 1`).Scan(&probe).Error; err != nil {
		return 0, fmt.Errorf("flywheel: BackfillRunFinishes: probe: %w", err)
	}
	if len(probe) == 0 {
		return 0, nil
	}
	written, hours, err := backfillWalk(ctx, db)
	if err != nil {
		return written, fmt.Errorf("flywheel: BackfillRunFinishes: %w", err)
	}
	if err := rerollCoveredHours(ctx, db, hours); err != nil {
		return written, fmt.Errorf("flywheel: BackfillRunFinishes: %w", err)
	}
	return written, nil
}

// backfillWalk logs every unlogged legacy run, one primary-key batch of job_runs
// at a time, and returns how many entries it wrote and the hours they fell in.
// A batch's unlogged runs are read, then exactly those are logged, so the hours
// returned are the hours of the entries written: a run an older binary
// finalizes between the read and the write is not logged now, and the next
// backfill finds it.
func backfillWalk(ctx context.Context, db *gorm.DB) (int64, map[int64]struct{}, error) {
	var (
		written int64
		cursor  string
	)
	hours := map[int64]struct{}{}
	for {
		if err := ctx.Err(); err != nil {
			return written, hours, fmt.Errorf("cancelled after %d entries: %w", written, err)
		}
		query := db.WithContext(ctx).Model(&jobRunRow{})
		if cursor != "" {
			query = query.Where("id > ?", cursor)
		}
		var ids []string
		if err := query.Order("id").Limit(backfillBatchSize).Pluck("id", &ids).Error; err != nil {
			return written, hours, fmt.Errorf("read batch: %w", err)
		}
		if len(ids) == 0 {
			return written, hours, nil
		}
		var found []struct {
			ID         string
			FinishedAt time.Time
		}
		if err := db.WithContext(ctx).Raw(`SELECT r.id, r.finished_at FROM job_runs r WHERE r.id >= ? AND r.id <= ? AND `+
			unloggedLegacyRunPredicate, ids[0], ids[len(ids)-1]).Scan(&found).Error; err != nil {
			return written, hours, fmt.Errorf("read unlogged runs: %w", err)
		}
		if len(found) > 0 {
			logged := make([]string, len(found))
			for i := range found {
				logged[i] = found[i].ID
				hours[floorHour(found[i].FinishedAt).Unix()] = struct{}{}
			}
			res := db.WithContext(ctx).Exec(fmt.Sprintf(finishLogInsert, "job_runs", " AND id IN ?"), logged)
			if res.Error != nil {
				return written, hours, fmt.Errorf("write batch: %w", res.Error)
			}
			written += res.RowsAffected
		}
		cursor = ids[len(ids)-1]
	}
}

// rerollCoveredHours re-rolls each hour in hours that the rollups already
// cover, oldest first, so the runs a backfill just logged are counted. An hour
// whose recompute would shrink its stored rollup — retention pruned some of its
// runs after it was rolled — keeps its rollup: it counted more than a recompute
// can.
func rerollCoveredHours(ctx context.Context, db *gorm.DB, hours map[int64]struct{}) error {
	if len(hours) == 0 {
		return nil
	}
	p, ok, err := readStatsProgress(ctx, db)
	if err != nil || !ok || !p.covered() {
		return err
	}
	sorted := make([]int64, 0, len(hours))
	for h := range hours {
		sorted = append(sorted, h)
	}
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	now := models.ClockFrom(ctx).Now(ctx)
	for _, sec := range sorted {
		hour := unixHour(sec)
		if !p.covers(hour) {
			continue
		}
		if _, err := rollupHour(ctx, db, hour, now, true, coverageStep{}); err != nil &&
			!errors.Is(err, errRollupWouldShrink) {
			return err
		}
	}
	return nil
}

// normalizeBatchSize is the number of job_runs rows NormalizeRunTimestamps
// re-stamps per transaction.
const normalizeBatchSize = 500

// nonUTCRunPredicate selects the job_runs rows whose stored text carries a zone
// other than UTC. SQLite stores a timestamp as the driver formatted it, and the
// runtime's driver writes UTC as a trailing +00:00, so any other suffix is a row
// written before job_runs timestamps were stamped in UTC.
const nonUTCRunPredicate = `(substr(started_at, -6) <> '+00:00' OR substr(created_at, -6) <> '+00:00' ` +
	`OR (finished_at IS NOT NULL AND substr(finished_at, -6) <> '+00:00'))`

// NormalizeRunTimestamps re-stamps, in UTC, any job_runs timestamps SQLite holds
// in another zone, and reports how many rows it changed. On PostgreSQL it does
// nothing and returns zero: timestamptz has no stored zone to normalize.
//
// It exists for SQLite databases written before this release, which stamped
// job_runs in the clock's local zone. SQLite compares timestamps as text, so a
// window over a mix of zones — or over one zone across a DST change, whose offset
// flips — selects the wrong rows. The stats rollup calls it once per Scheduler
// before its first pass, so an upgrade needs no manual step; it is exported for a
// host that reads job_runs windows without running the rollup.
//
// It walks the table in primary-key batches, each its own transaction, parsing
// and re-formatting in Go: SQLite's own datetime functions would drop the
// fractional seconds. Each column is rewritten only while it still holds a
// non-UTC value, so a finalize that stamps a fresh UTC finished_at between the
// read and the write is never overwritten. It is idempotent: once a database is
// normalized, a re-run reads the table once and changes nothing.
func NormalizeRunTimestamps(ctx context.Context, db *gorm.DB) (int64, error) {
	if db == nil {
		return 0, fmt.Errorf("flywheel: NormalizeRunTimestamps: db is nil")
	}
	if db.Name() != "sqlite" {
		return 0, nil
	}
	var (
		changed int64
		cursor  string
	)
	for {
		if err := ctx.Err(); err != nil {
			return changed, fmt.Errorf("flywheel: NormalizeRunTimestamps: cancelled after %d rows: %w", changed, err)
		}
		var rows []jobRunRow
		query := db.WithContext(ctx).Model(&jobRunRow{}).
			Select("id, started_at, finished_at, created_at").Where(nonUTCRunPredicate)
		if cursor != "" {
			query = query.Where("id > ?", cursor)
		}
		if err := query.Order("id").Limit(normalizeBatchSize).Find(&rows).Error; err != nil {
			return changed, fmt.Errorf("flywheel: NormalizeRunTimestamps: read: %w", err)
		}
		if len(rows) == 0 {
			return changed, nil
		}
		err := db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
			for i := range rows {
				var finished any
				if rows[i].FinishedAt != nil {
					finished = rows[i].FinishedAt.UTC()
				}
				res := tx.Exec(`UPDATE job_runs SET `+
					`started_at = CASE WHEN substr(started_at, -6) <> '+00:00' THEN ? ELSE started_at END, `+
					`created_at = CASE WHEN substr(created_at, -6) <> '+00:00' THEN ? ELSE created_at END, `+
					`finished_at = CASE WHEN finished_at IS NOT NULL AND substr(finished_at, -6) <> '+00:00' `+
					`THEN COALESCE(?, finished_at) ELSE finished_at END `+
					`WHERE id = ?`,
					rows[i].StartedAt.UTC(), rows[i].CreatedAt.UTC(), finished, rows[i].ID)
				if res.Error != nil {
					return res.Error
				}
				changed += res.RowsAffected
			}
			return nil
		})
		if err != nil {
			return changed, fmt.Errorf("flywheel: NormalizeRunTimestamps: write: %w", err)
		}
		cursor = rows[len(rows)-1].ID
	}
}
