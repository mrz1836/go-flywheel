package flywheel

import (
	"context"
	"fmt"
	"time"

	"gorm.io/gorm"
)

// QueueHealth is a point-in-time gauge snapshot of the queue: how much work is
// piling up and how far behind the oldest claimable job has fallen. Where the
// Observer seam reports per-attempt flows (counters, durations), QueueHealth is
// the depth/lag view an operator scrapes to answer "is work starving?". It is
// read by SampleQueueHealth, surfaced by the Scheduler heartbeat, the `/metrics`
// gauges, and `flywheel status`.
type QueueHealth struct {
	// CountsByState is the job count per state, soft-deleted jobs excluded (the
	// same scope as Overview). Every present state maps to its count; absent
	// states are zero.
	CountsByState map[string]int64 `json:"counts_by_state"`
	// Ready is the number of jobs claimable right now: a claimable state
	// (available, retryable, scheduled) whose scheduled_at is at or before the
	// sample instant.
	Ready int64 `json:"ready"`
	// InFlight is the number of running jobs (claimed, not yet finalized).
	InFlight int64 `json:"inflight"`
	// ScheduledAhead is the number of claimable jobs not yet due (scheduled_at in
	// the future) — work that is waiting on the clock, not on a runner.
	ScheduledAhead int64 `json:"scheduled_ahead"`
	// OldestReadyAge is the lag: how long the oldest ready job has been claimable
	// (sample instant minus its scheduled_at). It is zero when nothing is ready.
	// A growing OldestReadyAge is the canonical "the runners are falling behind"
	// signal.
	OldestReadyAge time.Duration `json:"oldest_ready_age"`
	// SampledAt is the clock instant the snapshot was taken (from the context
	// Clock), so a caller can reason about staleness deterministically.
	SampledAt time.Time `json:"sampled_at"`
}

// SampleQueueHealth reads a QueueHealth gauge snapshot through db. "Now" comes
// from the context Clock (ClockFrom), so a test drives it deterministically with
// a FixedClock and production uses the wall clock.
//
// It runs four index-backed reads, none of which mutate: a GROUP BY state count
// (soft-deleted excluded), a ready count and a scheduled-ahead count over the
// claimable states (both served by the jobs_ready partial index), and an ordered
// single-row read for the oldest ready scheduled_at. The lag is read by ordering
// rather than MIN(scheduled_at) because SQLite returns a bare aggregate as text
// and drops the column's datetime affinity, which would fail the time scan; an
// ordered read of the real typed column round-trips on both dialects.
//
// The snapshot is a sample, not a transaction: the reads are not serialized
// against concurrent claims, so a busy queue's numbers may not be mutually
// consistent to the single job. That is the right trade for a gauge an operator
// scrapes infrequently — correctness of each number, not a frozen global view.
func SampleQueueHealth(ctx context.Context, db *gorm.DB) (QueueHealth, error) {
	now := ClockFrom(ctx).Now(ctx)
	qh := QueueHealth{CountsByState: map[string]int64{}, SampledAt: now}

	// Counts by state (soft-deleted excluded via the gorm DeletedAt scope). The
	// running count doubles as InFlight, so no separate query is needed.
	var stateRows []struct {
		State string
		N     int64
	}
	if err := db.WithContext(ctx).Model(&jobRow{}).
		Select("state, count(*) as n").Group("state").Scan(&stateRows).Error; err != nil {
		return QueueHealth{}, fmt.Errorf("flywheel: queue health counts: %w", err)
	}
	for _, r := range stateRows {
		qh.CountsByState[r.State] = r.N
		if r.State == string(StateRunning) {
			qh.InFlight = r.N
		}
	}

	// Ready now: claimable and due.
	if err := db.WithContext(ctx).Model(&jobRow{}).
		Where("state IN ? AND scheduled_at <= ?", claimableStates, now).
		Count(&qh.Ready).Error; err != nil {
		return QueueHealth{}, fmt.Errorf("flywheel: queue health ready: %w", err)
	}

	// Scheduled ahead: claimable but not yet due.
	if err := db.WithContext(ctx).Model(&jobRow{}).
		Where("state IN ? AND scheduled_at > ?", claimableStates, now).
		Count(&qh.ScheduledAhead).Error; err != nil {
		return QueueHealth{}, fmt.Errorf("flywheel: queue health scheduled-ahead: %w", err)
	}

	// Lag: age of the oldest ready job. Read the real typed scheduled_at column of
	// the earliest-scheduled ready job (ordered, not MIN — see the doc comment).
	if qh.Ready > 0 {
		var oldest time.Time
		res := db.WithContext(ctx).Model(&jobRow{}).
			Select("scheduled_at").
			Where("state IN ? AND scheduled_at <= ?", claimableStates, now).
			Order("scheduled_at asc").Limit(1).Scan(&oldest)
		if res.Error != nil {
			return QueueHealth{}, fmt.Errorf("flywheel: queue health oldest-ready: %w", res.Error)
		}
		if age := now.Sub(oldest); age > 0 {
			qh.OldestReadyAge = age
		}
	}

	return qh, nil
}

// FailureView is one recently discarded job paired with the error from its final
// recorded attempt. It is the "what is failing, and why" diagnosis surface behind
// `flywheel status` and an operator dashboard: a discarded job is one that
// exhausted its retries (or hit a permanent error), so its last job_runs row
// carries the classification and message that ended it.
type FailureView struct {
	JobID        string    `json:"job_id"`
	Kind         string    `json:"kind"`
	Queue        string    `json:"queue"`
	Attempt      int       `json:"attempt"`
	FinalizedAt  time.Time `json:"finalized_at"`
	ErrorClass   string    `json:"error_class"`
	ErrorMessage string    `json:"error_message"`
}

// RecentFailuresParams scopes a RecentFailures query. Since bounds the window —
// only jobs finalized at or after it are returned; a zero Since means no lower
// bound. Limit caps the rows (default 20 when not positive).
type RecentFailuresParams struct {
	Since time.Time
	Limit int
}

// defaultRecentFailuresLimit caps a RecentFailures page when the caller passes no
// positive limit.
const defaultRecentFailuresLimit = 20

// RecentFailures returns the most recently discarded jobs (newest finalized
// first) within the window, each joined to the error on its latest attempt,
// reading through db. Soft-deleted jobs are excluded. It powers the failures
// section of `flywheel status` so an operator can glance and see what broke.
//
// The errors come from a second read keyed by the page's job ids rather than a
// SQL join, so the page size bounds both queries and the dialects stay identical:
// the runs for the page are loaded ordered by attempt, and the last row seen per
// job — the highest attempt — wins, which is the attempt that discarded it.
func RecentFailures(ctx context.Context, db *gorm.DB, p RecentFailuresParams) ([]FailureView, error) {
	limit := p.Limit
	if limit <= 0 {
		limit = defaultRecentFailuresLimit
	}

	query := db.WithContext(ctx).Model(&jobRow{}).
		Where("state = ? AND finalized_at IS NOT NULL", string(StateDiscarded))
	if !p.Since.IsZero() {
		query = query.Where("finalized_at >= ?", p.Since)
	}
	var jobs []jobRow
	if err := query.Order("finalized_at desc, id desc").Limit(limit).Find(&jobs).Error; err != nil {
		return nil, fmt.Errorf("flywheel: recent failures: %w", err)
	}
	if len(jobs) == 0 {
		return []FailureView{}, nil
	}

	ids := make([]string, len(jobs))
	for i := range jobs {
		ids[i] = jobs[i].ID
	}
	var runs []jobRunRow
	if err := db.WithContext(ctx).Model(&jobRunRow{}).
		Where("job_id IN ?", ids).
		Order("attempt asc").Find(&runs).Error; err != nil {
		return nil, fmt.Errorf("flywheel: recent failures runs: %w", err)
	}
	// attempt asc means the last write per job is the highest (final) attempt.
	latest := make(map[string]jobRunRow, len(runs))
	for i := range runs {
		latest[runs[i].JobID] = runs[i]
	}

	views := make([]FailureView, len(jobs))
	for i := range jobs {
		v := FailureView{
			JobID:   jobs[i].ID,
			Kind:    jobs[i].Kind,
			Queue:   jobs[i].Queue,
			Attempt: jobs[i].Attempt,
		}
		if jobs[i].FinalizedAt != nil {
			v.FinalizedAt = *jobs[i].FinalizedAt
		}
		if run, ok := latest[jobs[i].ID]; ok {
			v.Attempt = run.Attempt
			if run.ErrorClass != nil {
				v.ErrorClass = *run.ErrorClass
			}
			if run.ErrorMessage != nil {
				v.ErrorMessage = *run.ErrorMessage
			}
		}
		views[i] = v
	}
	return views, nil
}
