package core

import (
	"context"
	"fmt"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// statsProgress is job_stats_progress's row in times (see jobStatsProgressRow):
// the hours [From, To) the rollup is complete for, the oldest hour it keeps,
// and the version every rollup write moves.
type statsProgress struct {
	From, To   time.Time
	RetainFrom time.Time
	Version    int64
}

// covered reports whether p covers any hour at all.
func (p statsProgress) covered() bool { return p.From.Before(p.To) }

// covers reports whether the hour starting at h is inside the covered range.
func (p statsProgress) covers(h time.Time) bool { return !h.Before(p.From) && h.Before(p.To) }

// unixHour turns a stored epoch-seconds hour back into a UTC time.
func unixHour(sec int64) time.Time { return time.Unix(sec, 0).UTC() }

// readStatsProgress reads job_stats_progress. ok is false when nothing has ever
// written it — no rollup pass and no RebuildStats has run — and then nothing is
// covered. It is one primary-key read.
func readStatsProgress(ctx context.Context, db *gorm.DB) (statsProgress, bool, error) {
	rows, err := db.WithContext(ctx).Raw(`SELECT covered_from_unix, covered_to_unix, retain_from_unix, version `+
		`FROM job_stats_progress WHERE id = ?`, statsProgressID).Rows()
	if err != nil {
		return statsProgress{}, false, fmt.Errorf("read stats progress: %w", err)
	}
	defer func() { _ = rows.Close() }()
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return statsProgress{}, false, fmt.Errorf("read stats progress: %w", err)
		}
		return statsProgress{}, false, nil
	}
	var from, to, retain, version int64
	if err := rows.Scan(&from, &to, &retain, &version); err != nil {
		return statsProgress{}, false, fmt.Errorf("read stats progress: %w", err)
	}
	return statsProgress{From: unixHour(from), To: unixHour(to), RetainFrom: unixHour(retain), Version: version},
		true, nil
}

// ensureStatsProgress creates job_stats_progress's row, covering nothing and
// anchored at hour, unless it exists. Every writer that extends the range calls
// it first: a range step is an UPDATE, which needs the row to be there.
func ensureStatsProgress(ctx context.Context, db *gorm.DB, hour, now time.Time) error {
	row := jobStatsProgressRow{
		ID: statsProgressID, CoveredFromUnix: hour.Unix(), CoveredToUnix: hour.Unix(), RetainFromUnix: hour.Unix(),
		Version: 1, UpdatedAt: now.UTC(),
	}
	if err := db.WithContext(ctx).Clauses(clause.OnConflict{DoNothing: true}).Create(&row).Error; err != nil {
		return fmt.Errorf("create stats progress: %w", err)
	}
	return nil
}

// coverageStep is one contiguous extension of the covered range, applied in the
// same transaction as the rollup write that justifies it. Its zero value
// extends nothing.
//
// Each direction is conditional on where the range's edge is when the step
// commits, not where the writer last read it. up says "every hour in [upFrom,
// upTo) is rolled or empty": an upper edge anywhere in that span moves to upTo,
// and an edge outside it — already past upTo, or short of upFrom, which would
// leave an unrolled gap — stays. down is the mirror for the lower edge. That is
// what keeps two writers racing — a duplicate Scheduler, a RebuildStats
// overlapping the rollup — from ever extending the range across an hour neither
// rolled.
type coverageStep struct {
	upFrom, upTo     time.Time
	downFrom, downTo time.Time
}

// stepUp is the step that extends the upper edge across [from, to).
func stepUp(from, to time.Time) coverageStep { return coverageStep{upFrom: from, upTo: to} }

// stepDown is the step that extends the lower edge across [from, to).
func stepDown(from, to time.Time) coverageStep { return coverageStep{downFrom: from, downTo: to} }

// applyCoverageStep applies step in tx and bumps the version. It is also the
// version bump alone, for a write inside the range, with a zero step: a zero
// step's spans are empty, so neither edge can match it. When the row does not
// exist it changes nothing.
func applyCoverageStep(tx *gorm.DB, step coverageStep, now time.Time) error {
	err := tx.Exec(`UPDATE job_stats_progress SET `+
		`covered_to_unix = CASE WHEN covered_to_unix >= ? AND covered_to_unix < ? THEN ? ELSE covered_to_unix END, `+
		`covered_from_unix = CASE WHEN covered_from_unix > ? AND covered_from_unix <= ? THEN ? ELSE covered_from_unix END, `+
		`version = version + 1, updated_at = ? WHERE id = ?`,
		step.upFrom.Unix(), step.upTo.Unix(), step.upTo.Unix(),
		step.downFrom.Unix(), step.downTo.Unix(), step.downFrom.Unix(),
		now.UTC(), statsProgressID).Error
	if err != nil {
		return fmt.Errorf("record stats progress: %w", err)
	}
	return nil
}

// markCovered applies a range step that no rollup row accompanies: the stretch
// it extends across had no runs.
func markCovered(ctx context.Context, db *gorm.DB, step coverageStep, now time.Time) error {
	return applyCoverageStep(db.WithContext(ctx), step, now)
}

// advanceStatsFloor records the rollup's retention boundary for a pass: the
// row is created at earliest when absent, either edge below earliest is raised
// to it — hours older than the retention are about to be pruned, so they are
// covered no longer — and RetainFromUnix becomes earliest. A database idle for
// longer than the retention restarts its range at the boundary. It returns the
// progress as it stands after.
//
// The boundary moves once an hour and the rollup passes once a minute, so it
// reads first and writes only when something changes: a pass with nothing to
// do leaves the row, and its version, alone.
func advanceStatsFloor(ctx context.Context, db *gorm.DB, earliest, now time.Time) (statsProgress, error) {
	p, ok, err := readStatsProgress(ctx, db)
	if err != nil {
		return statsProgress{}, err
	}
	if ok && !p.From.Before(earliest) && !p.To.Before(earliest) && p.RetainFrom.Equal(earliest) {
		return p, nil
	}
	if !ok {
		if err := ensureStatsProgress(ctx, db, earliest, now); err != nil {
			return statsProgress{}, err
		}
	}
	e := earliest.Unix()
	if err := db.WithContext(ctx).Exec(`UPDATE job_stats_progress SET `+
		`version = version + CASE WHEN covered_from_unix < ? OR covered_to_unix < ? THEN 1 ELSE 0 END, `+
		`covered_from_unix = CASE WHEN covered_from_unix < ? THEN ? ELSE covered_from_unix END, `+
		`covered_to_unix = CASE WHEN covered_to_unix < ? THEN ? ELSE covered_to_unix END, `+
		`retain_from_unix = ?, updated_at = ? `+
		`WHERE id = ? AND (covered_from_unix < ? OR covered_to_unix < ? OR retain_from_unix <> ?)`,
		e, e, e, e, e, e, e, now.UTC(), statsProgressID, e, e, e).Error; err != nil {
		return statsProgress{}, fmt.Errorf("record stats retention: %w", err)
	}
	p, _, err = readStatsProgress(ctx, db)
	return p, err
}

// rollWalk is the result of a rollForward or rollBackward walk.
type rollWalk struct {
	// rolled lists the hours rolled, in the order the walk visited them.
	rolled []time.Time
	// groups totals the (kind, queue) groups those hours wrote.
	groups int
	// reached is how far the walk extended the range: the upper edge it left for
	// rollForward, the lower edge for rollBackward.
	reached time.Time
	// done is true when the walk covered its whole stretch, false when it
	// stopped on its hour budget.
	done bool
}

// rollForward rolls up every hour with runs in [lo, hi), oldest first, each in
// its own transaction that extends the covered range up across it and the
// empty hours before it, then extends the range across the empty rest of the
// stretch. An empty stretch costs one probe of job_run_finishes, however long.
//
// budget caps the hours rolled (zero is no cap); a walk that stops on it
// reports done false with the range extended as far as it got.
func rollForward(
	ctx context.Context, db *gorm.DB, lo, hi, now time.Time, guard bool, budget int,
) (rollWalk, error) {
	w := rollWalk{reached: lo}
	for w.reached.Before(hi) {
		if budget > 0 && len(w.rolled) >= budget {
			return w, nil
		}
		if err := ctx.Err(); err != nil {
			return w, fmt.Errorf("cancelled after %d hours: %w", len(w.rolled), err)
		}
		next, found, err := nextFinishedAt(ctx, db, w.reached)
		if err != nil {
			return w, err
		}
		if !found || !floorHour(next).Before(hi) {
			if err := markCovered(ctx, db, stepUp(w.reached, hi), now); err != nil {
				return w, err
			}
			w.reached = hi
			break
		}
		hour := floorHour(next)
		n, err := rollupHour(ctx, db, hour, now, guard, stepUp(w.reached, hour.Add(time.Hour)))
		if err != nil {
			return w, err
		}
		w.rolled = append(w.rolled, hour)
		w.groups += n
		w.reached = hour.Add(time.Hour)
	}
	w.done = true
	return w, nil
}

// rollBackward is rollForward's mirror: it rolls up every hour with runs in
// [lo, hi), newest first, each extending the covered range down across it and
// the empty hours above it, then extends the range down across the empty rest.
// It is how the range reaches back to the retention when it starts above it —
// after a RebuildStats created it, or a longer StatsRetention.
func rollBackward(
	ctx context.Context, db *gorm.DB, lo, hi, now time.Time, guard bool, budget int,
) (rollWalk, error) {
	w := rollWalk{reached: hi}
	for lo.Before(w.reached) {
		if budget > 0 && len(w.rolled) >= budget {
			return w, nil
		}
		if err := ctx.Err(); err != nil {
			return w, fmt.Errorf("cancelled after %d hours: %w", len(w.rolled), err)
		}
		prev, found, err := prevFinishedAt(ctx, db, lo, w.reached)
		if err != nil {
			return w, err
		}
		if !found {
			if err := markCovered(ctx, db, stepDown(lo, w.reached), now); err != nil {
				return w, err
			}
			w.reached = lo
			break
		}
		hour := floorHour(prev)
		n, err := rollupHour(ctx, db, hour, now, guard, stepDown(hour, w.reached))
		if err != nil {
			return w, err
		}
		w.rolled = append(w.rolled, hour)
		w.groups += n
		w.reached = hour
	}
	w.done = true
	return w, nil
}

// statsRetentionCap returns the latest cutoff retention may use without
// deleting a run the stats rollup has yet to count, or ok false when it may
// delete nothing because nothing is covered yet.
//
// Runs past the covered range are uncounted, so the cap is the range's upper
// edge. While the range's lower edge is still above the oldest hour the rollup
// keeps, the runs between the two are uncounted too — the rollup is working back
// through them — so the cap drops to that oldest hour until it gets there. Runs
// older than it are outside the rollup's reach and retention's to take.
func statsRetentionCap(ctx context.Context, db *gorm.DB) (time.Time, bool, error) {
	p, ok, err := readStatsProgress(ctx, db)
	if err != nil || !ok || !p.covered() {
		return time.Time{}, false, err
	}
	if p.RetainFrom.Before(p.From) {
		return p.RetainFrom, true, nil
	}
	return p.To, true, nil
}
