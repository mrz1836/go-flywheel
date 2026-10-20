package core

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/mrz1836/go-foundation/models"
	"gorm.io/gorm"
)

// RetentionStatusRow is what one retention rule, or the default age, keeps and
// what it has due: the row RetentionStatus reports for it.
type RetentionStatusRow struct {
	// Kind and State are the rule's; both are empty on the default's row.
	Kind  string   `json:"kind,omitempty"`
	State JobState `json:"state,omitempty"`
	// MaxAge is how long the row's jobs are kept after finalizing.
	MaxAge time.Duration `json:"max_age"`
	// Cutoff is the clock's now minus MaxAge, capped by the stats-rollup hold
	// once the rollup covers any hour: a pass now deletes the row's jobs
	// finalized before it.
	Cutoff time.Time `json:"cutoff"`
	// Oldest is when the oldest finished job the row decides finalized, in UTC;
	// nil when it decides none.
	Oldest *time.Time `json:"oldest,omitempty"`
	// Due counts the jobs the row decides that finalized before Cutoff, exactly
	// up to DefaultCountCap, with the planner's estimate past it on PostgreSQL,
	// as CountJobs counts.
	Due JobCount `json:"due"`
}

// RetentionStatus reports, for each of rules in the order given and then for
// the default maxAge, the cutoff a retention pass would use now, the oldest
// finished job it keeps, and how many jobs are due: what a pass of
// DeleteFinishedJobsWithOptions with RetentionOpts.Rules, or a Scheduler with
// RetentionMaxAge and RetentionRules, would delete. It reads only.
//
// Each job is decided by one row, the most specific rule that matches it or
// else the default, exactly as the pass decides it, so a job a more specific
// rule decides is neither counted nor reported as the oldest by a less specific
// row. A cutoff is the clock's now (from the context, as models.ClockFrom reads
// it) minus the row's MaxAge, capped by the stats-rollup hold when the rollup
// covers any hour, as a pass that holds for it is.
//
// maxAge zero or less is a *ValidationError naming max_age, and rules are
// refused as RetentionOpts.Rules refuses them, before any read. It reads every
// terminal state's range of jobs_finished that each row needs, twice: an
// operator's read, not one to poll.
func RetentionStatus(
	ctx context.Context, db *gorm.DB, maxAge time.Duration, rules []RetentionRule,
) ([]RetentionStatusRow, error) {
	if maxAge <= 0 {
		return nil, newValidationError("max_age", "must be positive")
	}
	if err := validateRetentionRules(rules); err != nil {
		return nil, err
	}
	now := models.ClockFrom(ctx).Now(ctx)
	held, holds, err := statsRetentionCap(ctx, db)
	if err != nil {
		return nil, fmt.Errorf("flywheel: retention status: %w", err)
	}
	// The row each job falls to, as the pass's CASE picks its cutoff.
	rowOf, rowArgs := retentionCase(rules, func(i int) (string, []any) {
		return strconv.Itoa(i), nil
	}, strconv.Itoa(len(rules)))

	out := make([]RetentionStatusRow, 0, len(rules)+1)
	for i := range len(rules) + 1 {
		row := RetentionStatusRow{MaxAge: maxAge}
		states := TerminalStates()
		if i < len(rules) {
			row.Kind, row.State, row.MaxAge = rules[i].Kind, rules[i].State, rules[i].MaxAge
			if row.State != "" {
				states = []JobState{row.State}
			}
		}
		row.Cutoff = now.Add(-row.MaxAge)
		if holds && held.Before(row.Cutoff) {
			row.Cutoff = held.In(now.Location())
		}
		decides := rowOf + " = " + strconv.Itoa(i)
		if row.Oldest, err = retentionOldest(ctx, db, states, decides, rowArgs); err != nil {
			return nil, fmt.Errorf("flywheel: retention status: %w", err)
		}
		if row.Due, err = retentionDue(ctx, db, states, row.Cutoff, decides, rowArgs); err != nil {
			return nil, fmt.Errorf("flywheel: retention status: %w", err)
		}
		out = append(out, row)
	}
	return out, nil
}

// retentionOldest returns when the oldest finished job in states that decides
// selects finalized, or nil: one jobs_finished range read per state, the least
// of them.
func retentionOldest(
	ctx context.Context, db *gorm.DB, states []JobState, decides string, args []any,
) (*time.Time, error) {
	var oldest *time.Time
	for _, state := range states {
		var row struct{ FinalizedAt *time.Time }
		err := db.WithContext(ctx).Unscoped().Model(&jobRow{}).Select("finalized_at").
			Where("state = ? AND finalized_at IS NOT NULL", string(state)).Where(decides, args...).
			Order("finalized_at").Limit(1).Scan(&row).Error
		if err != nil {
			return nil, fmt.Errorf("read the oldest %s job: %w", state, err)
		}
		if row.FinalizedAt != nil && (oldest == nil || row.FinalizedAt.Before(*oldest)) {
			at := row.FinalizedAt.UTC()
			oldest = &at
		}
	}
	return oldest, nil
}

// retentionDue counts the finished jobs in states that decides selects and that
// finalized before cutoff, as CountJobs counts: exactly to DefaultCountCap, then
// capped, with the planner's estimate on PostgreSQL.
func retentionDue(
	ctx context.Context, db *gorm.DB, states []JobState, cutoff time.Time, decides string, args []any,
) (JobCount, error) {
	// A GORM chain mutates its statement, so the limited and the unlimited forms
	// are each built afresh.
	matching := func() *gorm.DB {
		return db.WithContext(ctx).Unscoped().Model(&jobRow{}).Select("1").
			Where("state IN ? AND finalized_at IS NOT NULL AND finalized_at < ?", stateStrings(states), cutoff).
			Where(decides, args...)
	}
	var n int64
	if err := db.WithContext(ctx).Table("(?) AS capped", matching().Limit(DefaultCountCap+1)).Count(&n).Error; err != nil {
		return JobCount{}, fmt.Errorf("count the due jobs: %w", err)
	}
	if n <= DefaultCountCap {
		return JobCount{N: n}, nil
	}
	out := JobCount{N: DefaultCountCap, Capped: true}
	if db.Name() == "postgres" {
		out.Estimate = max(estimateRows(db.WithContext(ctx), matching()), DefaultCountCap+1)
	}
	return out, nil
}
