package flywheel

import (
	"context"
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
type JobView struct {
	ID          string    `json:"id"`
	Kind        string    `json:"kind"`
	State       string    `json:"state"`
	ParentJobID string    `json:"parent_job_id"`
	EnqueuedAt  time.Time `json:"enqueued_at"`
	Attempt     int       `json:"attempt"`
}

// JobRunView is the public read projection of a single job attempt.
type JobRunView struct {
	ID            string     `json:"id"`
	Outcome       string     `json:"outcome"`
	ExecutorClass string     `json:"executor_class"`
	StartedAt     time.Time  `json:"started_at"`
	FinishedAt    *time.Time `json:"finished_at"`
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
// state vocabulary.
func NonTerminalStates() []JobState {
	return []JobState{StateAvailable, StateRunning, StateRetryable, StateScheduled}
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

// ListRunsParams configures a ListRuns page. Before is a created_at cursor (zero
// means newest); Limit caps the rows returned. A host that wants a has-more
// sentinel passes Limit+1 and trims the extra row itself.
type ListRunsParams struct {
	Before time.Time
	Limit  int
}

// OverviewParams configures an Overview query. Kind, when non-empty, scopes the
// counts to a single job kind.
type OverviewParams struct {
	Kind string
}

// ListJobsParams filters and pages a ListJobs query. State and Kind, when set,
// are exact-match filters; Limit caps the page (default 50).
type ListJobsParams struct {
	State string
	Kind  string
	Limit int
}

// defaultListJobsLimit caps a ListJobs page when the caller passes no limit.
const defaultListJobsLimit = 50

// ListJobs returns jobs newest-first (created_at desc, id desc), reading through
// db and optionally filtered by exact state and kind. Soft-deleted jobs are
// excluded. It is the inspection seam behind a "list jobs" CLI or dashboard.
func ListJobs(ctx context.Context, db *gorm.DB, p ListJobsParams) ([]JobView, error) {
	query := db.WithContext(ctx).Model(&jobRow{})
	if p.State != "" {
		query = query.Where("state = ?", p.State)
	}
	if p.Kind != "" {
		query = query.Where("kind = ?", p.Kind)
	}
	limit := p.Limit
	if limit <= 0 {
		limit = defaultListJobsLimit
	}
	var rows []jobRow
	if err := query.Order("created_at desc, id desc").Limit(limit).Find(&rows).Error; err != nil {
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
	err := db.WithContext(ctx).Where("id = ?", id).First(&row).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return JobView{}, ErrJobNotFound
	}
	if err != nil {
		return JobView{}, fmt.Errorf("flywheel: find job: %w", err)
	}
	return jobViewFromRow(row), nil
}

// ListRuns returns a job's runs newest-first (created_at desc, id desc), reading
// through db. When p.Before is non-zero only rows strictly older than the cursor
// are returned; a positive p.Limit caps the page.
func ListRuns(ctx context.Context, db *gorm.DB, jobID string, p ListRunsParams) ([]JobRunView, error) {
	query := db.WithContext(ctx).Model(&jobRunRow{}).Where("job_id = ?", jobID)
	if !p.Before.IsZero() {
		query = query.Where("created_at < ?", p.Before)
	}
	if p.Limit > 0 {
		query = query.Limit(p.Limit)
	}
	var rows []jobRunRow
	if err := query.Order("created_at desc, id desc").Find(&rows).Error; err != nil {
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
func jobViewFromRow(r jobRow) JobView {
	parent := ""
	if r.ParentJobID != nil {
		parent = *r.ParentJobID
	}
	return JobView{
		ID:          r.ID,
		Kind:        r.Kind,
		State:       r.State,
		ParentJobID: parent,
		EnqueuedAt:  r.CreatedAt,
		Attempt:     r.Attempt,
	}
}

// jobRunViewFromRow projects an unexported jobRunRow into the public JobRunView.
func jobRunViewFromRow(r jobRunRow) JobRunView {
	return JobRunView{
		ID:            r.ID,
		Outcome:       r.Outcome,
		ExecutorClass: r.ExecutorClass,
		StartedAt:     r.StartedAt,
		FinishedAt:    r.FinishedAt,
	}
}
