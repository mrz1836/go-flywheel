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
//
// A ("", "") row with zero counts is also how the rollup records a stretch of
// hours with no runs, so its watermark advances through them.
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
	// finalized carries it. A failed attempt an older binary wrote does not, and
	// whether it discarded its job is recovered the same way the kind is: it did
	// when its job is discarded and this was the job's last attempt.
	resolvedJobStateExpr = `CASE WHEN r.job_state IS NOT NULL THEN r.job_state ` +
		`WHEN r.kind = '' AND r.outcome IN ('error', 'timeout') THEN ` +
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
func (a *runAgg) observe(
	id string, outcome RunOutcome, superseded bool, jobState *string, duration, wait, cost *int64,
) {
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
// An hour with no aggregates renders as a lone zero ("", "") row — the marker
// that records a quiet stretch.
func hourRows(bucket time.Time, aggs map[statsGroup]*runAgg, rolledAt time.Time) []jobStatsHourlyRow {
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
// sets it; the rollup activity, which only ever rolls hours past the watermark,
// does not need it.
//
// An hour with no runs writes only its zero total row — the marker that records
// the rollup's progress through a quiet stretch. Older markers are deleted in the
// same transaction: only the latest one carries information.
//
// It reports the number of (kind, queue) groups written, zero for a marker.
func rollupHour(ctx context.Context, db *gorm.DB, hour, rolledAt time.Time, guard bool) (int, error) {
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
		// A quiet stretch's marker is only needed until a later hour is written.
		if err := tx.Where("kind = ? AND queue = ? AND attempts = 0 AND superseded = 0 AND bucket_start_unix < ?",
			statsTotalKey, statsTotalKey, hour.Unix()).Delete(&jobStatsHourlyRow{}).Error; err != nil {
			return fmt.Errorf("clear stale markers: %w", err)
		}
		if err := tx.Clauses(clause.OnConflict{UpdateAll: true}).Create(&rows).Error; err != nil {
			return fmt.Errorf("write hour: %w", err)
		}
		return nil
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

// readStatsWatermark returns the instant the rollups cover up to: the end of the
// latest rolled hour, marker rows included. ok is false when nothing has been
// rolled up yet. It is a single descending primary-key probe.
func readStatsWatermark(ctx context.Context, db *gorm.DB) (time.Time, bool, error) {
	var buckets []int64
	if err := db.WithContext(ctx).Model(&jobStatsHourlyRow{}).
		Order("bucket_start_unix DESC").Limit(1).Pluck("bucket_start_unix", &buckets).Error; err != nil {
		return time.Time{}, false, fmt.Errorf("read stats watermark: %w", err)
	}
	if len(buckets) == 0 {
		return time.Time{}, false, nil
	}
	return time.Unix(buckets[0], 0).UTC().Add(time.Hour), true, nil
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
	// in one probe, are not counted.
	Hours int `json:"hours"`
	// Rolled lists the start of each hour written, in order.
	Rolled []time.Time `json:"rolled"`
	// Watermark is where the rollups cover up to after the pass.
	Watermark time.Time `json:"watermark"`
	// CaughtUp is true when the pass reached the last closed hour, false when it
	// stopped on its per-pass ceiling with closed hours still to roll.
	CaughtUp bool `json:"caught_up"`
	// Pruned is the rollup rows deleted for being older than the retention.
	Pruned int64 `json:"pruned"`
}

// rollupPass rolls up every closed hour from the watermark forward, at most
// cfg.maxHours of them, then prunes rollup rows older than the retention.
//
// An hour is closed once its end plus the grace is in the past. With no
// watermark yet the pass starts at the oldest finished run's hour, but never
// further back than the retention, so enabling the rollup on a long-lived
// database backfills its history a bounded pass at a time. Empty stretches are
// skipped with one indexed probe each, and the last hour of a quiet stretch is
// recorded with a marker so the watermark advances through it.
//
//nolint:gocognit // one walk over the closed hours with the empty-stretch skip
func rollupPass(ctx context.Context, db *gorm.DB, now time.Time, cfg rollupConfig) (RollupResult, error) {
	var res RollupResult
	closedBefore := floorHour(now.Add(-cfg.grace))
	earliest := floorHour(now.Add(-cfg.retention))

	start, ok, err := readStatsWatermark(ctx, db)
	if err != nil {
		return res, err
	}
	if !ok || start.Before(earliest) {
		start = earliest
	}

	hour := start
	for hour.Before(closedBefore) && res.Hours < cfg.maxHours {
		if err := ctx.Err(); err != nil {
			return res, fmt.Errorf("stats rollup cancelled after %d hours: %w", res.Hours, err)
		}
		next, found, err := nextFinishedAt(ctx, db, hour)
		if err != nil {
			return res, err
		}
		if !found || !floorHour(next).Before(closedBefore) {
			// Nothing finished in [hour, closedBefore): record the stretch as
			// processed with one marker on its last hour and stop.
			if _, err := rollupHour(ctx, db, closedBefore.Add(-time.Hour), now, false); err != nil {
				return res, err
			}
			hour = closedBefore
			break
		}
		hour = floorHour(next)
		if _, err := rollupHour(ctx, db, hour, now, false); err != nil {
			return res, err
		}
		res.Hours++
		res.Rolled = append(res.Rolled, hour)
		hour = hour.Add(time.Hour)
	}
	res.CaughtUp = !hour.Before(closedBefore)
	res.Watermark, _, err = readStatsWatermark(ctx, db)
	if err != nil {
		return res, err
	}

	// Prune by primary-key range. The latest row is never older than the
	// retention unless the whole history is, so the watermark survives the prune
	// except on a database idle for longer than the retention — where restarting
	// from the retention boundary is exactly right.
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
	// From and To are the hour range rebuilt, after flooring and clamping.
	From time.Time `json:"from"`
	To   time.Time `json:"to"`
	// Hours is the number of hours recomputed; Groups the (kind, queue) rollup
	// rows written across them.
	Hours  int `json:"hours"`
	Groups int `json:"groups"`
}

// RebuildStats recomputes the hourly rollups for a range of closed hours from
// the raw job_runs, replacing whatever the rollup stored. It is the repair for
// runs that landed in an hour after the rollup closed it — clock skew beyond the
// grace, a SeedRun import of history — and the fast way to backfill a range
// without waiting out the rollup activity's per-pass ceiling.
//
// Each hour is replaced in its own transaction, exactly as the rollup activity
// replaces it, so a rebuild and a running rollup can overlap safely, and a
// cancelled rebuild leaves every hour it finished rebuilt.
//
// It sees the runs the finish log holds. Runs an older release finalized —
// before an upgrade, or during a rolling deploy — are visible only once
// BackfillRunFinishes has written their entries; the rollup activity does that
// on its own, and `flywheel stats rebuild` runs it before rebuilding. It is not
// run here because it reads all of job_runs, which a targeted rebuild of a few
// hours should not have to pay for.
//
// Without Force it refuses to shrink an hour: when the recompute counts fewer
// runs than the stored rollup, the rebuild stops at that hour with an error
// wrapping ErrValidation, leaving it and every later hour as they were. Raw
// history is what retention prunes; the rollup is what outlives it.
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
	// A rebuild rewrites hours the baseline memo may hold without moving the
	// watermark it is keyed by.
	defer baselineMemo.clear()
	for hour := from; hour.Before(to); hour = hour.Add(time.Hour) {
		if err := ctx.Err(); err != nil {
			return res, fmt.Errorf("flywheel: RebuildStats: cancelled after %d hours: %w", res.Hours, err)
		}
		n, err := rollupHour(ctx, db, hour, now, !opts.Force)
		if errors.Is(err, errRollupWouldShrink) {
			return res, fmt.Errorf("flywheel: RebuildStats: %w", &ValidationError{
				Field: "rebuild range",
				Message: fmt.Sprintf("hour %s: %v; its raw runs were pruned after it was rolled up "+
					"(set Force to rebuild it anyway)", hour.Format(time.RFC3339), err),
			})
		}
		if err != nil {
			return res, fmt.Errorf("flywheel: RebuildStats: %w", err)
		}
		res.Hours++
		res.Groups += n
	}
	return res, nil
}

// backfillBatchSize is the number of job_runs rows BackfillRunFinishes examines
// per statement.
const backfillBatchSize = 1000

// BackfillRunFinishes writes the job_run_finishes entry of every finished run
// that lacks one, and reports how many it wrote. It exists for the upgrade: a run
// finalized by a release older than the finish log has no entry, so the stats
// reads — which find runs through the log — cannot see it until this writes one.
//
// The stats rollup runs it on its own: on a Scheduler's first pass, and once
// more ten minutes later, which covers the runs an older binary still finalizes
// while a rolling deploy overlaps the two; `flywheel stats rebuild` runs it
// before rebuilding. A host calls it directly to make older runs visible before
// a RebuildStats, or without running the rollup at all.
//
// Only runs an older binary wrote need an entry, and they are recognizable: this
// release stamps every run's kind, and an older one left it at the column
// default, the empty string. So the work is one pass over job_runs in
// primary-key batches, each an INSERT … SELECT that touches only those rows and
// skips any entry already present. On SQLite the pass first re-stamps legacy
// timestamps in UTC (NormalizeRunTimestamps), so every entry is written in the
// zone the log is compared in. It is idempotent.
func BackfillRunFinishes(ctx context.Context, db *gorm.DB) (int64, error) {
	if db == nil {
		return 0, fmt.Errorf("flywheel: BackfillRunFinishes: db is nil")
	}
	if _, err := NormalizeRunTimestamps(ctx, db); err != nil {
		return 0, fmt.Errorf("flywheel: BackfillRunFinishes: %w", err)
	}
	var (
		written int64
		cursor  string
	)
	for {
		if err := ctx.Err(); err != nil {
			return written, fmt.Errorf("flywheel: BackfillRunFinishes: cancelled after %d entries: %w", written, err)
		}
		query := db.WithContext(ctx).Model(&jobRunRow{})
		if cursor != "" {
			query = query.Where("id > ?", cursor)
		}
		var ids []string
		if err := query.Order("id").Limit(backfillBatchSize).Pluck("id", &ids).Error; err != nil {
			return written, fmt.Errorf("flywheel: BackfillRunFinishes: read batch: %w", err)
		}
		if len(ids) == 0 {
			return written, nil
		}
		res := db.WithContext(ctx).Exec(fmt.Sprintf(finishLogInsert, "job_runs", " AND kind = '' AND id >= ? AND id <= ?"),
			ids[0], ids[len(ids)-1])
		if res.Error != nil {
			return written, fmt.Errorf("flywheel: BackfillRunFinishes: write batch: %w", res.Error)
		}
		written += res.RowsAffected
		cursor = ids[len(ids)-1]
	}
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
