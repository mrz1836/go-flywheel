package core

import (
	"context"
	"encoding/json"
	"fmt"

	"gorm.io/gorm"
)

const (
	// DefaultCountCap is how far CountJobs counts exactly when CountOpts.Cap is
	// zero.
	DefaultCountCap = 10000
	// MaxCountCap is the largest CountOpts.Cap: a larger cap counts to it.
	MaxCountCap = 100000
)

// JobCount is a capped count of the jobs a JobQuery matches.
type JobCount struct {
	// N is the exact count when Capped is false, and the cap when it is true.
	N int64 `json:"n"`
	// Capped reports that more jobs than the cap match.
	Capped bool `json:"capped"`
	// Estimate is, when Capped, the planner's estimate of the uncapped count on
	// PostgreSQL, at least the cap plus one; it is 0 on SQLite and whenever the
	// count is exact.
	Estimate int64 `json:"estimate,omitempty"`
}

// CountOpts configures CountJobsWithOptions. The zero value counts exactly up to
// DefaultCountCap.
type CountOpts struct {
	// Cap is how far the count is exact. Zero or negative selects
	// DefaultCountCap; a value above MaxCountCap counts to MaxCountCap.
	Cap int64
}

// CountJobs counts the jobs q matches up to DefaultCountCap: it is
// CountJobsWithOptions with the zero CountOpts.
func CountJobs(ctx context.Context, db *gorm.DB, q JobQuery) (JobCount, error) {
	return CountJobsWithOptions(ctx, db, q, CountOpts{})
}

// CountJobsWithOptions counts the jobs q matches, exactly up to opts.Cap, reading
// through db as given. It shares QueryJobs' filter and its refusals, and ignores
// q's Sort, Cursor, and Limit: it counts what QueryJobs would list across every
// page.
//
// It never reads more than the cap plus one row: it counts a sub-select limited
// to that, so a lens over a large table costs what one over the cap would. At or
// below the cap the count is exact. Past it, N is the cap and Capped is set, and
// on PostgreSQL Estimate is the planner's estimate of the uncapped count, read
// from EXPLAIN with the same bound values and raised to at least the cap plus
// one; an EXPLAIN that fails leaves it there rather than failing a count that
// succeeded. SQLite has no estimate. A count's speed rests on the same indexes
// QueryJobs' does.
func CountJobsWithOptions(ctx context.Context, db *gorm.DB, q JobQuery, opts CountOpts) (JobCount, error) {
	if err := validateJobFilter(ctx, q, db.Name()); err != nil {
		return JobCount{}, err
	}
	countCap := opts.Cap
	if countCap <= 0 {
		countCap = DefaultCountCap
	}
	countCap = min(countCap, MaxCountCap)

	// A GORM chain mutates its statement, so the limited and the unlimited forms
	// are each built afresh.
	matching := func() *gorm.DB {
		return applyJobFilter(db.WithContext(ctx).Model(&jobRow{}).Select("1"), q, db.Name())
	}
	var n int64
	if err := db.WithContext(ctx).Table("(?) AS capped", matching().Limit(int(countCap+1))).Count(&n).Error; err != nil {
		return JobCount{}, fmt.Errorf("flywheel: count jobs: %w", err)
	}
	if n <= countCap {
		return JobCount{N: n}, nil
	}
	out := JobCount{N: countCap, Capped: true}
	if db.Name() == "postgres" {
		out.Estimate = max(estimateRows(db.WithContext(ctx), matching()), countCap+1)
	}
	return out, nil
}

// estimateRows returns the planner's row estimate for stmt, read from the top
// node of EXPLAIN (FORMAT JSON) with stmt's own bound values, or 0 when it
// cannot be read.
func estimateRows(db, stmt *gorm.DB) int64 {
	var raw string
	if err := db.Raw("EXPLAIN (FORMAT JSON) SELECT 1 FROM (?) AS uncapped", stmt).Row().Scan(&raw); err != nil {
		return 0
	}
	var plans []struct {
		Plan struct {
			Rows float64 `json:"Plan Rows"`
		} `json:"Plan"`
	}
	if err := json.Unmarshal([]byte(raw), &plans); err != nil || len(plans) == 0 {
		return 0
	}
	return int64(plans[0].Plan.Rows)
}
