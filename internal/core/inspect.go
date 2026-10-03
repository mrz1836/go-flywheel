package core

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/mrz1836/go-foundation/models"
	"gorm.io/gorm"
)

// Read-API page defaults, applied when a Limit is not positive.
const (
	defaultListRunningLimit  = 100
	defaultListFinishedLimit = 50
	defaultSlowRunsLimit     = 20
	// defaultSlowRunsWindow is SlowRuns' window when Since is zero. Slow runs
	// further back than a day come from the rollup's slowest_run_id per hour,
	// which Stats reports, rather than from a scan of raw history.
	defaultSlowRunsWindow = 24 * time.Hour
)

// Slow-run rule: a running attempt is slow once it has run longer than
// max(slowRunFactor × the kind's baseline p99, p99 + slowRunMargin), and only
// when that p99 is drawn from at least slowRunMinSamples successful runs. The
// factor alone flags a 30 ms job at 100 ms, which is noise; the margin alone
// never flags a 3-hour job; together they flag what an operator would call stuck.
const (
	slowRunFactor     = 3
	slowRunMargin     = time.Minute
	slowRunMinSamples = 200
)

// RunningJob is one job in the running state and the attempt running it.
type RunningJob struct {
	JobID       string `json:"job_id"`
	Kind        string `json:"kind"`
	Queue       string `json:"queue"`
	Attempt     int    `json:"attempt"`
	MaxAttempts int    `json:"max_attempts"`
	ParentJobID string `json:"parent_job_id,omitempty"`
	// RunID, ExecutorClass, and ExecutorID identify the attempt. They are empty,
	// and StartedAt nil, while the job is Starting.
	RunID         string     `json:"run_id,omitempty"`
	ExecutorClass string     `json:"executor_class,omitempty"`
	ExecutorID    string     `json:"executor_id,omitempty"`
	StartedAt     *time.Time `json:"started_at,omitempty"`
	// Starting is true in the instant between the claim and the attempt's audit
	// row: the job is leased but its run has not begun. It never falls back to the
	// job's updated_at, which every lease renewal moves.
	Starting bool `json:"starting"`
	// Elapsed is how long the attempt has been running, zero while Starting.
	Elapsed time.Duration `json:"elapsed"`
	// LeasedUntil is the claim's lease expiry. LeaseExpired is true once it has
	// passed: the worker has stopped renewing — usually because its process died —
	// and the next lease sweep will reclaim the job.
	LeasedUntil  *time.Time `json:"leased_until,omitempty"`
	LeaseExpired bool       `json:"lease_expired"`
	// BaselineP99 is the kind's baseline p99 success duration, set only with
	// ListRunningParams.WithBaseline and a baseline drawn from enough samples.
	BaselineP99 *time.Duration `json:"baseline_p99,omitempty"`
	// Slow is true when Elapsed exceeds max(3 × BaselineP99, BaselineP99 + 1m).
	Slow bool `json:"slow"`
}

// ListRunningParams scopes a ListRunning page. The zero value lists the 100
// longest-running jobs of every kind and queue, without baselines.
type ListRunningParams struct {
	// Kind and Queue, when set, are exact-match filters.
	Kind  string
	Queue string
	// Limit caps the page (default 100 when not positive).
	Limit int
	// WithBaseline compares each run to its kind's baseline from the rollups —
	// one extra read — and sets BaselineP99 and Slow.
	WithBaseline bool
	// BaselineWindow is the baseline's trailing window; zero selects 7 days.
	BaselineWindow time.Duration
}

// ListRunning returns the jobs running right now, longest-running first: what
// each is, which attempt, on which executor, for how long, and whether its lease
// is still being renewed.
//
// It reads jobs in the running state — served by the jobs_running_leased
// partial index, whose predicate is exactly that — joined to each one's current
// attempt by the job_runs_job_attempt unique index. The join is a LEFT JOIN
// because a job is running from its claim, and its audit row lands a moment
// later: in that gap the job is reported as Starting rather than dropped.
// Elapsed, LeaseExpired, and Slow are computed in Go against the context clock.
func ListRunning(ctx context.Context, db *gorm.DB, p ListRunningParams) ([]RunningJob, error) {
	if db == nil {
		return nil, fmt.Errorf("flywheel: ListRunning: db is nil")
	}
	var baselines map[string]Baseline
	if p.WithBaseline {
		list, err := Baselines(ctx, db, p.BaselineWindow)
		if err != nil {
			return nil, fmt.Errorf("flywheel: ListRunning: %w", err)
		}
		baselines = baselineMap(list)
	}
	out, err := listRunning(ctx, db, p, baselines)
	if err != nil {
		return nil, fmt.Errorf("flywheel: ListRunning: %w", err)
	}
	return out, nil
}

// baselineMap keys a baseline list by kind.
func baselineMap(list []Baseline) map[string]Baseline {
	out := make(map[string]Baseline, len(list))
	for _, b := range list {
		out[b.Kind] = b
	}
	return out
}

// listRunning is ListRunning against baselines the caller already holds — the
// Scheduler's heartbeat passes its cached set rather than re-reading rollups on
// every pulse. A nil map compares nothing.
func listRunning(
	ctx context.Context, db *gorm.DB, p ListRunningParams, baselines map[string]Baseline,
) ([]RunningJob, error) {
	limit := p.Limit
	if limit <= 0 {
		limit = defaultListRunningLimit
	}
	query := db.WithContext(ctx).Table("jobs AS j").
		Select(`j.id AS job_id, j.kind, j.queue, j.attempt, j.max_attempts, j.parent_job_id, j.leased_until, `+
			`r.id AS run_id, r.executor_class, r.executor_id, r.started_at`).
		Joins(`LEFT JOIN job_runs r ON r.job_id = j.id AND r.attempt = j.attempt`).
		Where("j.state = ? AND j.deleted_at IS NULL", string(StateRunning))
	if p.Kind != "" {
		query = query.Where("j.kind = ?", p.Kind)
	}
	if p.Queue != "" {
		query = query.Where("j.queue = ?", p.Queue)
	}
	var rows []struct {
		JobID         string
		Kind          string
		Queue         string
		Attempt       int
		MaxAttempts   int
		ParentJobID   *string
		LeasedUntil   *time.Time
		RunID         *string
		ExecutorClass *string
		ExecutorID    *string
		StartedAt     *time.Time
	}
	// Longest-running first, starting jobs last: the CASE spells NULLS LAST
	// portably, since the two dialects default to opposite ends.
	if err := query.Order("CASE WHEN r.started_at IS NULL THEN 1 ELSE 0 END, r.started_at ASC, j.id ASC").
		Limit(limit).Scan(&rows).Error; err != nil {
		return nil, fmt.Errorf("read running jobs: %w", err)
	}

	now := models.ClockFrom(ctx).Now(ctx)
	out := make([]RunningJob, len(rows))
	for i := range rows {
		r := rows[i]
		rj := RunningJob{
			JobID: r.JobID, Kind: r.Kind, Queue: r.Queue,
			Attempt: r.Attempt, MaxAttempts: r.MaxAttempts,
			LeasedUntil: r.LeasedUntil, StartedAt: r.StartedAt,
			Starting: r.RunID == nil,
		}
		if r.ParentJobID != nil {
			rj.ParentJobID = *r.ParentJobID
		}
		if r.RunID != nil {
			rj.RunID = *r.RunID
		}
		if r.ExecutorClass != nil {
			rj.ExecutorClass = *r.ExecutorClass
		}
		if r.ExecutorID != nil {
			rj.ExecutorID = *r.ExecutorID
		}
		if r.StartedAt != nil {
			rj.Elapsed = max(now.Sub(*r.StartedAt), 0)
		}
		if r.LeasedUntil != nil {
			rj.LeaseExpired = now.After(*r.LeasedUntil)
		}
		if b, ok := baselines[r.Kind]; ok && b.Samples >= slowRunMinSamples && b.P99 > 0 {
			p99 := b.P99
			rj.BaselineP99 = &p99
			rj.Slow = !rj.Starting && rj.Elapsed > slowRunThreshold(p99)
		}
		out[i] = rj
	}
	return out, nil
}

// slowRunThreshold is the elapsed time past which a run of a kind with baseline
// p99 is slow: max(3 × p99, p99 + 1m).
func slowRunThreshold(p99 time.Duration) time.Duration {
	return max(slowRunFactor*p99, p99+slowRunMargin)
}

// FinishedCursor is ListFinished's keyset cursor: the (finalized_at, id) of the
// last job on the previous page.
type FinishedCursor struct {
	FinalizedAt time.Time `json:"finalized_at"`
	ID          string    `json:"id"`
}

// ListFinishedParams scopes a ListFinished page. The zero value lists the 50
// most recently finished jobs in every terminal state.
type ListFinishedParams struct {
	// States restricts the page to some terminal states; empty means all three.
	// A non-terminal state is rejected with ErrValidation.
	States []JobState
	// Kind and Queue, when set, are exact-match filters.
	Kind  string
	Queue string
	// Since, when set, drops jobs finalized before it.
	Since time.Time
	// Before, when set, continues from a previous page: only jobs strictly older
	// than the cursor are returned. Pass the FinalizedAt and ID of the last job on
	// that page.
	Before *FinishedCursor
	// Limit caps the page (default 50 when not positive).
	Limit int
}

// ListFinished returns the most recently finished jobs, newest first by
// (finalized_at, id) — what completed, and how it ended.
//
// It runs one LIMIT n range scan per requested state on the jobs_finished index,
// whose (state, finalized_at, id) key hands each scan its rows already in page
// order, and merges the per-state pages in Go. That is three short index scans
// instead of one scan of every terminal job sorted by finalization, which is what
// a single "state IN (...) ORDER BY finalized_at" has to do.
//
// finalized_at bounds and cursors compare in the zone the jobs table was written
// in. A cursor taken from a previous page's job round-trips exactly.
func ListFinished(ctx context.Context, db *gorm.DB, p ListFinishedParams) ([]JobView, error) {
	if db == nil {
		return nil, fmt.Errorf("flywheel: ListFinished: db is nil")
	}
	states := p.States
	if len(states) == 0 {
		states = TerminalStates()
	}
	for _, s := range states {
		if !isTerminalStateString(string(s)) {
			return nil, newValidationError("states", fmt.Sprintf("%q is not a terminal state", s))
		}
	}
	limit := p.Limit
	if limit <= 0 {
		limit = defaultListFinishedLimit
	}

	var all []jobRow
	for _, s := range states {
		rows, err := finishedPage(ctx, db, s, p, limit)
		if err != nil {
			return nil, fmt.Errorf("flywheel: ListFinished: %w", err)
		}
		all = append(all, rows...)
	}
	sort.Slice(all, func(i, j int) bool {
		a, b := all[i].FinalizedAt, all[j].FinalizedAt
		if !a.Equal(*b) {
			return a.After(*b)
		}
		return all[i].ID > all[j].ID
	})
	if len(all) > limit {
		all = all[:limit]
	}
	views := make([]JobView, len(all))
	for i := range all {
		views[i] = jobViewFromRow(all[i])
	}
	return views, nil
}

// finishedPage reads one state's page off jobs_finished, newest first.
func finishedPage(ctx context.Context, db *gorm.DB, state JobState, p ListFinishedParams, limit int) ([]jobRow, error) {
	query := db.WithContext(ctx).Model(&jobRow{}).
		Where("state = ? AND finalized_at IS NOT NULL", string(state))
	if p.Kind != "" {
		query = query.Where("kind = ?", p.Kind)
	}
	if p.Queue != "" {
		query = query.Where("queue = ?", p.Queue)
	}
	if !p.Since.IsZero() {
		query = query.Where("finalized_at >= ?", p.Since)
	}
	if p.Before != nil {
		query = query.Where("(finalized_at < ? OR (finalized_at = ? AND id < ?))",
			p.Before.FinalizedAt, p.Before.FinalizedAt, p.Before.ID)
	}
	var rows []jobRow
	if err := query.Order("finalized_at DESC, id DESC").Limit(limit).Find(&rows).Error; err != nil {
		return nil, fmt.Errorf("read %s page: %w", state, err)
	}
	return rows, nil
}

// SlowRunsParams scopes a SlowRuns read. The zero value returns the 20 slowest
// finished runs of the last 24 hours.
type SlowRunsParams struct {
	// Kind, when set, restricts the read to one kind.
	Kind string
	// Since bounds the window's start; zero selects 24 hours before now. The
	// window is a range of the job_run_finishes log, so it is meant to be short —
	// for slow runs further back, read the per-hour slowest run from Stats.
	Since time.Time
	// MinDuration, when positive, drops runs faster than it.
	MinDuration time.Duration
	// Limit caps the result (default 20 when not positive).
	Limit int
}

// SlowRuns returns the slowest finished runs in a recent window, slowest first,
// across every outcome — a timeout is exactly the slow run an operator wants to
// see. Superseded runs are included and flagged. Crashed runs have no duration
// and are not ranked.
func SlowRuns(ctx context.Context, db *gorm.DB, p SlowRunsParams) ([]JobRunView, error) {
	if db == nil {
		return nil, fmt.Errorf("flywheel: SlowRuns: db is nil")
	}
	limit := p.Limit
	if limit <= 0 {
		limit = defaultSlowRunsLimit
	}
	since := p.Since
	if since.IsZero() {
		since = models.ClockFrom(ctx).Now(ctx).Add(-defaultSlowRunsWindow)
	}
	query := db.WithContext(ctx).Table("job_run_finishes AS f").Select("r.*").
		Joins("JOIN job_runs r ON r.id = f.run_id").
		Where("f.finished_at >= ? AND r.duration_ms IS NOT NULL", since.UTC())
	if p.Kind != "" {
		query = query.Where(resolvedKindExpr+" = ?", p.Kind)
	}
	if p.MinDuration > 0 {
		query = query.Where("r.duration_ms >= ?", p.MinDuration.Milliseconds())
	}
	var rows []jobRunRow
	if err := query.Order("r.duration_ms DESC, r.id DESC").Limit(limit).Scan(&rows).Error; err != nil {
		return nil, fmt.Errorf("flywheel: SlowRuns: %w", err)
	}
	views := make([]JobRunView, len(rows))
	for i := range rows {
		views[i] = jobRunViewFromRow(rows[i])
	}
	return views, nil
}

// QueueDepth is one queue's point-in-time depth: the per-queue counterpart of
// QueueHealth.
type QueueDepth struct {
	Queue string `json:"queue"`
	// Ready is claimable right now; ScheduledAhead is claimable but not yet due.
	Ready          int64 `json:"ready"`
	ScheduledAhead int64 `json:"scheduled_ahead"`
	// Running is claimed and not yet finalized; Paused is held by an operator.
	Running int64 `json:"running"`
	Paused  int64 `json:"paused"`
	// OldestReadyAge is how long the queue's oldest ready job has been claimable
	// — the queue's lag. Zero when nothing is ready.
	OldestReadyAge time.Duration `json:"oldest_ready_age"`
}

// QueueDepths returns every queue's depth, sorted by queue, with the same
// access paths and cost profile as SampleQueueHealth: the ready and
// scheduled-ahead counts are grouped reads of the jobs_ready index, whose key
// leads with queue; running and paused are read through jobs_state; and the lag
// is one ordered single-row read per queue with ready work.
//
// A queue appears once it holds any non-terminal job. It is a sample, not a
// transaction — see SampleQueueHealth.
func QueueDepths(ctx context.Context, db *gorm.DB) ([]QueueDepth, error) {
	if db == nil {
		return nil, fmt.Errorf("flywheel: QueueDepths: db is nil")
	}
	now := models.ClockFrom(ctx).Now(ctx)
	depths := map[string]*QueueDepth{}
	get := func(q string) *QueueDepth {
		d, ok := depths[q]
		if !ok {
			d = &QueueDepth{Queue: q}
			depths[q] = d
		}
		return d
	}

	type queueCount struct {
		Queue string
		N     int64
	}
	var ready, ahead []queueCount
	if err := db.WithContext(ctx).Model(&jobRow{}).Select("queue, count(*) AS n").
		Where("state IN ? AND scheduled_at <= ?", claimableStates, now).
		Group("queue").Scan(&ready).Error; err != nil {
		return nil, fmt.Errorf("flywheel: QueueDepths: ready: %w", err)
	}
	if err := db.WithContext(ctx).Model(&jobRow{}).Select("queue, count(*) AS n").
		Where("state IN ? AND scheduled_at > ?", claimableStates, now).
		Group("queue").Scan(&ahead).Error; err != nil {
		return nil, fmt.Errorf("flywheel: QueueDepths: scheduled-ahead: %w", err)
	}
	var held []struct {
		Queue string
		State string
		N     int64
	}
	if err := db.WithContext(ctx).Model(&jobRow{}).Select("queue, state, count(*) AS n").
		Where("state IN ?", []string{string(StateRunning), string(StatePaused)}).
		Group("queue, state").Scan(&held).Error; err != nil {
		return nil, fmt.Errorf("flywheel: QueueDepths: running: %w", err)
	}
	for _, c := range ready {
		get(c.Queue).Ready = c.N
	}
	for _, c := range ahead {
		get(c.Queue).ScheduledAhead = c.N
	}
	for _, c := range held {
		if c.State == string(StateRunning) {
			get(c.Queue).Running = c.N
		} else {
			get(c.Queue).Paused = c.N
		}
	}

	for _, c := range ready {
		if c.N == 0 {
			continue
		}
		// Ordered rather than MIN(scheduled_at): SQLite returns a bare aggregate
		// of a time column as text. See SampleQueueHealth.
		var oldest []time.Time
		if err := db.WithContext(ctx).Model(&jobRow{}).
			Where("queue = ? AND state IN ? AND scheduled_at <= ?", c.Queue, claimableStates, now).
			Order("scheduled_at ASC").Limit(1).Pluck("scheduled_at", &oldest).Error; err != nil {
			return nil, fmt.Errorf("flywheel: QueueDepths: oldest-ready: %w", err)
		}
		if len(oldest) > 0 {
			get(c.Queue).OldestReadyAge = max(now.Sub(oldest[0]), 0)
		}
	}

	out := make([]QueueDepth, 0, len(depths))
	for _, d := range depths {
		out = append(out, *d)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Queue < out[j].Queue })
	return out, nil
}

// ActiveCount is the number of jobs of one kind, on one queue, in one
// non-terminal state.
type ActiveCount struct {
	Kind  string   `json:"kind"`
	Queue string   `json:"queue"`
	State JobState `json:"state"`
	Count int64    `json:"count"`
}

// CountActiveByKind returns how many jobs are in flight per (kind, queue,
// state), over the non-terminal states only, sorted by kind, queue, and state.
// The state predicate is the jobs_state index's access path, so the read visits
// only in-flight rows however much finished history the table holds — which is
// why it, and not a GROUP BY kind over all of jobs, is the per-kind backlog read.
func CountActiveByKind(ctx context.Context, db *gorm.DB) ([]ActiveCount, error) {
	if db == nil {
		return nil, fmt.Errorf("flywheel: CountActiveByKind: db is nil")
	}
	var out []ActiveCount
	if err := db.WithContext(ctx).Model(&jobRow{}).
		Select("kind, queue, state, count(*) AS count").
		Where("state IN ?", nonTerminalStateStrings()).
		Group("kind, queue, state").Order("kind, queue, state").
		Scan(&out).Error; err != nil {
		return nil, fmt.Errorf("flywheel: CountActiveByKind: %w", err)
	}
	if out == nil {
		out = []ActiveCount{}
	}
	return out, nil
}
