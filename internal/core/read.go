package core

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"gorm.io/gorm"
)

// ErrJobNotFound is returned by FindJob when no job matches the requested id. A
// host maps it to a 404 without depending on gorm's record-not-found sentinel.
var ErrJobNotFound = errors.New("flywheel: job not found")

// JobView is the public read projection of a job. The runtime keeps its row
// struct unexported and exposes this stable, JSON-tagged view instead, so a host
// inspection API binds to flywheel's contract rather than the mutable schema.
//
// It carries every column a dashboard shows and none it should not: the args
// payload is deliberately absent (it can be large and may hold data a dashboard
// must not render — ListActiveByKind is the host-internal seam that carries it).
type JobView struct {
	ID          string    `json:"id"`
	Kind        string    `json:"kind"`
	Queue       string    `json:"queue"`
	State       string    `json:"state"`
	ParentJobID string    `json:"parent_job_id"`
	EnqueuedAt  time.Time `json:"enqueued_at"`
	Attempt     int       `json:"attempt"`
	MaxAttempts int       `json:"max_attempts"`
	Priority    int       `json:"priority"`
	// ExecutorClass is the routing label the job was enqueued with; empty is the
	// AnyClass wildcard.
	ExecutorClass string `json:"executor_class"`
	// ScheduledAt is when the job is (or was last) claimable. A retry or a snooze
	// moves it forward, so it is not the enqueue time.
	ScheduledAt time.Time `json:"scheduled_at"`
	// FinalizedAt is when the job reached a terminal state; nil while it is still
	// in flight.
	FinalizedAt *time.Time `json:"finalized_at,omitempty"`
	// UpdatedAt is the job row's last write. It moves on every state transition
	// and on every lease renewal, so it is a liveness signal, not a start time —
	// ListRunning reads the attempt's own started_at for that.
	UpdatedAt time.Time `json:"updated_at"`
	Tags      []string  `json:"tags"`
}

// JobRunView is the public read projection of a single job attempt — one
// job_runs row.
type JobRunView struct {
	ID      string `json:"id"`
	JobID   string `json:"job_id"`
	Attempt int    `json:"attempt"`
	// Kind and Queue are the job's, recorded on the attempt. They are empty on a
	// row an older binary wrote before the columns existed.
	Kind          string     `json:"kind"`
	Queue         string     `json:"queue"`
	Outcome       string     `json:"outcome"`
	ExecutorClass string     `json:"executor_class"`
	ExecutorID    string     `json:"executor_id"`
	StartedAt     time.Time  `json:"started_at"`
	FinishedAt    *time.Time `json:"finished_at"`
	// DurationMs is the attempt's wall time from start to finalize. It is nil
	// while the attempt runs and for an attempt the lease sweep marked crashed,
	// which never reported a finish of its own.
	DurationMs *int `json:"duration_ms,omitempty"`
	// QueueWaitMs is how long the attempt waited for a worker after it became
	// claimable. It is nil on a row written before the column existed.
	QueueWaitMs *int `json:"queue_wait_ms,omitempty"`
	// ErrorClass classifies a failed attempt's error (transient, permanent,
	// validation, timeout); empty when the attempt carried none.
	ErrorClass string `json:"error_class,omitempty"`
	// Error is the worker error recorded for a failed attempt, if any.
	Error *string `json:"error,omitempty"`
	// CostMicros is the attempt's accumulated external-call cost, as the worker
	// reported it in Result.CostMicros.
	CostMicros       *int64 `json:"cost_micros,omitempty"`
	EnqueuedChildren int    `json:"enqueued_children"`
	// JobState is the job state this attempt's finalize applied — retryable or
	// discarded is what separates a retried failure from a final one. It is empty
	// while the attempt runs, when the finalize was superseded, and on a row
	// written before the column existed.
	JobState string `json:"job_state,omitempty"`
	// Superseded reports that the attempt's claim was gone by the time it
	// finalized — the job was cancelled or reclaimed underneath it — so its
	// outcome was recorded but never applied to the job.
	Superseded bool `json:"superseded"`
	// Output is the worker's structured Result.Output as stored in job_runs.output
	// (for the command workers, an ExecOutput with exit code and captured streams).
	// It is empty when the attempt produced no output.
	Output json.RawMessage `json:"output,omitempty"`
}

// JobsOverview is the aggregate job-state report: a count per state plus the
// total across all states in scope.
type JobsOverview struct {
	CountsByState map[string]int `json:"counts_by_state"`
	Total         int            `json:"total"`
}

// JobArgsView is a host-internal read projection that carries a job's raw args
// payload so a host can match jobs on their typed arguments without binding to
// the unexported row. Unlike JobView it is not a wire contract — it exists so a
// host (e.g. a "do I already have an active job for this subject?" lookup) can
// inspect args server-side.
type JobArgsView struct {
	ID   string
	Kind string
	Args []byte
}

// NonTerminalStates returns the job states from which a job may still progress.
// The terminal states (succeeded, cancelled, discarded) are excluded. A host
// uses it to scope "still in flight" queries without re-deriving the runtime's
// state vocabulary. paused is included — a held job is still in flight, waiting
// on a resume rather than on a runner.
func NonTerminalStates() []JobState {
	return []JobState{StateAvailable, StateRunning, StateRetryable, StateScheduled, StatePaused}
}

// nonTerminalStateStrings returns NonTerminalStates as the []string a GORM
// "state IN ?" clause binds against, so the inspection queries share one
// conversion instead of each re-deriving it.
func nonTerminalStateStrings() []string {
	return stateStrings(NonTerminalStates())
}

// TerminalStates returns the job states a job can no longer progress from
// (succeeded, cancelled, discarded). A host uses it to scope "finished" queries
// — e.g. retention — without re-deriving the runtime's state vocabulary.
func TerminalStates() []JobState {
	return []JobState{StateSucceeded, StateCancelled, StateDiscarded}
}

// terminalStateStrings returns TerminalStates as the []string a GORM
// "state IN ?" clause binds against.
func terminalStateStrings() []string {
	return stateStrings(TerminalStates())
}

// stateStrings converts a JobState slice to the []string a GORM bind expects.
func stateStrings(states []JobState) []string {
	out := make([]string, len(states))
	for i, s := range states {
		out[i] = string(s)
	}
	return out
}

// ListRunsParams configures a ListRuns (or ChildOutputs) page. Limit caps the
// rows returned; a host that wants a has-more sentinel passes Limit+1 and trims
// the extra row itself. The zero value is the newest page, uncapped.
type ListRunsParams struct {
	// Before is a created_at cursor: only rows created strictly before it are
	// returned. Zero means newest.
	Before time.Time
	// BeforeAttempt is ListRuns' attempt cursor: when positive, only attempts
	// numbered below it are returned. Pass the Attempt of the previous page's
	// last run. It is exact on every dialect and is the cursor to prefer;
	// ChildOutputs, which pages jobs rather than attempts, ignores it.
	BeforeAttempt int
	Limit         int
}

// OverviewParams configures an Overview query. Kind, when non-empty, scopes the
// counts to a single job kind.
type OverviewParams struct {
	Kind string
}

// ListJobsParams filters and pages a ListJobs query. The zero value lists the
// newest 50 jobs of every state, kind, and queue.
type ListJobsParams struct {
	// State, Kind, and Queue, when set, are exact-match filters.
	State string
	Kind  string
	Queue string
	// BeforeID is a keyset cursor: when set, only jobs whose id sorts strictly
	// before it are returned. Pass the last id of the previous page to fetch the
	// next one.
	BeforeID string
	// Limit caps the page (default 50 when not positive).
	Limit int
}

// defaultListJobsLimit caps a ListJobs page when the caller passes no limit.
const defaultListJobsLimit = 50

// ListJobs returns jobs newest-first by id, reading through db and optionally
// filtered by exact state, kind, and queue. Soft-deleted jobs are excluded. It is
// the inspection seam behind a "list jobs" CLI or dashboard.
//
// # Ordered by id, not created_at
//
// Job ids are UUIDv7, whose leading bits are the insert's millisecond timestamp,
// so id order is insertion order. Ordering by it lets the primary key serve the
// page — a LIMIT that stops early — where ORDER BY created_at, which no index
// carries, sorted the whole matching set on every call. BeforeID pages by the
// same key.
//
// The one place the two orders differ is a job whose created_at was not the wall
// clock at insert: a job inserted under a models.SimulatedClock or FixedClock, or
// seeded with an explicit CreatedAt. Such a job is listed where its id puts it.
func ListJobs(ctx context.Context, db *gorm.DB, p ListJobsParams) ([]JobView, error) {
	query := db.WithContext(ctx).Model(&jobRow{})
	if p.State != "" {
		query = query.Where("state = ?", p.State)
	}
	if p.Kind != "" {
		query = query.Where("kind = ?", p.Kind)
	}
	if p.Queue != "" {
		query = query.Where("queue = ?", p.Queue)
	}
	if p.BeforeID != "" {
		query = query.Where("id < ?", p.BeforeID)
	}
	limit := p.Limit
	if limit <= 0 {
		limit = defaultListJobsLimit
	}
	var rows []jobRow
	if err := query.Order("id desc").Limit(limit).Find(&rows).Error; err != nil {
		return nil, fmt.Errorf("flywheel: list jobs: %w", err)
	}
	views := make([]JobView, len(rows))
	for i := range rows {
		views[i] = jobViewFromRow(rows[i])
	}
	return views, nil
}

// FindJob returns the JobView for id, reading through the host-provided db. A
// soft-deleted job is excluded (gorm scopes deleted_at IS NULL). A miss returns
// ErrJobNotFound so the caller can map it to a 404.
func FindJob(ctx context.Context, db *gorm.DB, id string) (JobView, error) {
	var row jobRow
	err := quietMissing(db).WithContext(ctx).Where("id = ?", id).First(&row).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return JobView{}, ErrJobNotFound
	}
	if err != nil {
		return JobView{}, fmt.Errorf("flywheel: find job: %w", err)
	}
	return jobViewFromRow(row), nil
}

// ListRuns returns a job's runs newest-first by attempt (attempt desc, id desc),
// reading through db; the job_runs_job_attempt unique index serves the order. A
// positive p.BeforeAttempt returns only earlier attempts, a non-zero p.Before only
// runs created strictly before it, and a positive p.Limit caps the page.
//
// # Zone-proof on SQLite
//
// The page is ordered by attempt rather than created_at because a job's attempt
// numbers only grow, while SQLite compares its text timestamps as text: a run an
// older release wrote carries the clock's local zone until NormalizeRunTimestamps
// re-stamps it in UTC, and the stats rollup — the only caller of that — may never
// run. So BeforeAttempt is exact everywhere, and Before is compared as text only
// against a row stamped in UTC (exact to the nanosecond) and through julianday —
// which reads the zone suffix, to the millisecond — against any other.
func ListRuns(ctx context.Context, db *gorm.DB, jobID string, p ListRunsParams) ([]JobRunView, error) {
	query := db.WithContext(ctx).Model(&jobRunRow{}).Where("job_id = ?", jobID)
	if p.BeforeAttempt > 0 {
		query = query.Where("attempt < ?", p.BeforeAttempt)
	}
	if !p.Before.IsZero() {
		before := p.Before.UTC()
		if db.Name() == "sqlite" {
			query = query.Where(`CASE WHEN substr(created_at, -6) = '+00:00' THEN created_at < ? `+
				`ELSE julianday(created_at) < julianday(?) END`, before, before)
		} else {
			query = query.Where("created_at < ?", before)
		}
	}
	if p.Limit > 0 {
		query = query.Limit(p.Limit)
	}
	var rows []jobRunRow
	if err := query.Order("attempt desc, id desc").Find(&rows).Error; err != nil {
		return nil, fmt.Errorf("flywheel: list runs: %w", err)
	}
	views := make([]JobRunView, len(rows))
	for i := range rows {
		views[i] = jobRunViewFromRow(rows[i])
	}
	return views, nil
}

// Overview returns the job count grouped by state, optionally scoped to a single
// kind, reading through db. Soft-deleted jobs are excluded.
func Overview(ctx context.Context, db *gorm.DB, p OverviewParams) (JobsOverview, error) {
	query := db.WithContext(ctx).Model(&jobRow{})
	if p.Kind != "" {
		query = query.Where("kind = ?", p.Kind)
	}
	var rows []struct {
		State string
		N     int
	}
	if err := query.Select("state, count(*) as n").Group("state").Scan(&rows).Error; err != nil {
		return JobsOverview{}, fmt.Errorf("flywheel: overview: %w", err)
	}
	counts := make(map[string]int, len(rows))
	total := 0
	for _, row := range rows {
		counts[row.State] = row.N
		total += row.N
	}
	return JobsOverview{CountsByState: counts, Total: total}, nil
}

// ListActiveByKind returns the non-terminal jobs of the given kind, each with
// its raw args payload, reading through db. Soft-deleted jobs are excluded. A
// host uses it to answer "is there already an in-flight job of this kind for
// some subject?" by inspecting the returned args.
func ListActiveByKind(ctx context.Context, db *gorm.DB, kind string) ([]JobArgsView, error) {
	var rows []jobRow
	err := db.WithContext(ctx).
		Where("kind = ? AND state IN ?", kind, nonTerminalStateStrings()).
		Find(&rows).Error
	if err != nil {
		return nil, fmt.Errorf("flywheel: list active by kind: %w", err)
	}
	views := make([]JobArgsView, len(rows))
	for i := range rows {
		views[i] = JobArgsView{
			ID:   rows[i].ID,
			Kind: rows[i].Kind,
			Args: []byte(rows[i].Args),
		}
	}
	return views, nil
}

// CountRuns returns the total number of recorded job attempts (job_runs rows),
// reading through db. It is the inspection seam for run-throughput telemetry.
func CountRuns(ctx context.Context, db *gorm.DB) (int64, error) {
	var n int64
	if err := db.WithContext(ctx).Model(&jobRunRow{}).Count(&n).Error; err != nil {
		return 0, fmt.Errorf("flywheel: count runs: %w", err)
	}
	return n, nil
}

// CountActiveJobs returns how many jobs are still in a non-terminal state,
// reading through db (soft-deleted excluded). It is the inspection seam for
// "pending work remaining" telemetry.
func CountActiveJobs(ctx context.Context, db *gorm.DB) (int64, error) {
	var n int64
	if err := db.WithContext(ctx).Model(&jobRow{}).Where("state IN ?", nonTerminalStateStrings()).Count(&n).Error; err != nil {
		return 0, fmt.Errorf("flywheel: count active jobs: %w", err)
	}
	return n, nil
}

// jobViewFromRow projects an unexported jobRow into the public JobView.
//
// Tags are decoded best-effort: the runtime always writes a JSON array, so a
// value that does not decode is a hand-written row, and it reads as no tags
// rather than failing the whole page it sits on.
func jobViewFromRow(r jobRow) JobView {
	parent := ""
	if r.ParentJobID != nil {
		parent = *r.ParentJobID
	}
	tags := []string{}
	if len(r.Tags) > 0 {
		var decoded []string
		if err := json.Unmarshal(r.Tags, &decoded); err == nil && decoded != nil {
			tags = decoded
		}
	}
	return JobView{
		ID:            r.ID,
		Kind:          r.Kind,
		Queue:         r.Queue,
		State:         r.State,
		ParentJobID:   parent,
		EnqueuedAt:    r.CreatedAt,
		Attempt:       r.Attempt,
		MaxAttempts:   r.MaxAttempts,
		Priority:      r.Priority,
		ExecutorClass: r.ExecutorClass,
		ScheduledAt:   r.ScheduledAt,
		FinalizedAt:   r.FinalizedAt,
		UpdatedAt:     r.UpdatedAt,
		Tags:          tags,
	}
}

// jobRunViewFromRow projects an unexported jobRunRow into the public JobRunView.
func jobRunViewFromRow(r jobRunRow) JobRunView {
	v := JobRunView{
		ID:               r.ID,
		JobID:            r.JobID,
		Attempt:          r.Attempt,
		Kind:             r.Kind,
		Queue:            r.Queue,
		Outcome:          r.Outcome,
		ExecutorClass:    r.ExecutorClass,
		ExecutorID:       r.ExecutorID,
		StartedAt:        r.StartedAt,
		FinishedAt:       r.FinishedAt,
		DurationMs:       r.DurationMs,
		QueueWaitMs:      r.QueueWaitMs,
		Error:            r.ErrorMessage,
		CostMicros:       r.CostMicros,
		EnqueuedChildren: r.EnqueuedChildren,
		Superseded:       r.Superseded,
	}
	if r.ErrorClass != nil {
		v.ErrorClass = *r.ErrorClass
	}
	if r.JobState != nil {
		v.JobState = *r.JobState
	}
	if len(r.Output) > 0 {
		v.Output = json.RawMessage(r.Output)
	}
	return v
}
