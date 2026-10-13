package core

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
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
// It carries every column a list shows and none it should not: the args payload
// and the metadata are deliberately absent (args can be large and may hold data
// a list must not render), and the reads that return a JobView never load them.
// GetJob reads one job in full, its args bounded.
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
	// Error is the worker error recorded for a failed attempt, if any, cut at
	// 4,096 bytes; ErrorTruncated reports a cut.
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
	// It is empty when the attempt produced no output, and a TruncatedOutput when
	// the output's JSON was larger than DriverOpts.MaxOutputBytes.
	Output json.RawMessage `json:"output,omitempty"`
	// OutputTruncated reports that Output holds a TruncatedOutput — exactly its
	// three keys, $truncated true — rather than what the worker returned.
	OutputTruncated bool `json:"output_truncated,omitempty"`
	// ErrorTruncated reports that Error was cut at 4,096 bytes. The run's
	// error_payload keeps the message's length before the cut (message_bytes).
	ErrorTruncated bool `json:"error_truncated,omitempty"`
	// ErrorStack is the stack of a worker panic the runner recovered, at most
	// 16 KiB, where Error is the short panic message. It is empty for any other
	// failure.
	ErrorStack string `json:"error_stack,omitempty"`
	// ErrorPayload is the run's error_payload as stored: the error's "message",
	// plus "truncated" and "message_bytes" when the message was cut, and "stack"
	// for a recovered panic. ErrorTruncated and ErrorStack read the same payload;
	// this keeps it whole, so a host can show all of it. It is nil when the run
	// stored no payload.
	ErrorPayload json.RawMessage `json:"error_payload,omitempty"`
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

// liveStatesSQL returns the live states as a parenthesized list of quoted SQL
// literals — ('available', 'running', 'retryable', 'scheduled', 'paused') —
// built from nonTerminalStateStrings, in its order.
//
// A query meant to be served by the partial index jobs_unique_active_key spells
// its state condition with this list instead of binding nonTerminalStateStrings.
// A planner may use a partial index only when it can prove the query's WHERE
// implies the index's, and neither SQLite, which matches the index's terms one by
// one against the query's, nor a PostgreSQL generic plan can prove it through
// bound parameters: with a bound list the read falls back to scanning jobs. The
// index's DDL lists the same states in the same order, which
// TestHolderLookupMirrorsTheUniqueIndexes pins. The states are the runtime's own
// constants, so inlining them carries no injection risk.
func liveStatesSQL() string {
	states := nonTerminalStateStrings()
	quoted := make([]string, len(states))
	for i, s := range states {
		quoted[i] = "'" + s + "'"
	}
	return "(" + strings.Join(quoted, ", ") + ")"
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

// MaxReadLimit is the most rows any list read returns: ListJobs, ListFinished,
// ListRunning, SlowRuns, RecentFailures, and ListRuns. A larger Limit is
// clamped to it rather than refused, so a host that passes a request's limit
// straight through gets a full page, never a whole table. A host that asks for
// one row more than its page, to learn whether another page follows, keeps its
// page below MaxReadLimit, or the extra row is clamped away.
//
// ChildOutputs is the one exception: at a zero Limit it reads every terminal
// child, because a barrier continuation folds its whole generation, which
// DriverOpts.BarrierMaxChildren bounds.
const MaxReadLimit = 200

// readLimit resolves a list read's page size: def when limit is not positive,
// and never more than MaxReadLimit.
func readLimit(limit, def int) int {
	if limit <= 0 {
		return def
	}
	return min(limit, MaxReadLimit)
}

// ListRunsParams configures a ListRuns (or ChildOutputs) page. Limit caps the
// rows returned; a host that wants a has-more sentinel passes Limit+1 and trims
// the extra row itself. The zero value is the newest page of at most
// MaxReadLimit runs for ListRuns; ChildOutputs reads every terminal child at a
// zero Limit.
type ListRunsParams struct {
	// Before is a created_at cursor: only rows created strictly before it are
	// returned. Zero means newest.
	Before time.Time
	// BeforeAttempt is ListRuns' attempt cursor: when positive, only attempts
	// numbered below it are returned. Pass the Attempt of the previous page's
	// last run. It is exact on every dialect and is the cursor to prefer;
	// ChildOutputs, which pages jobs rather than attempts, ignores it.
	BeforeAttempt int
	// Limit caps the page. ListRuns reads MaxReadLimit runs when it is not
	// positive and clamps a larger value to MaxReadLimit; ChildOutputs applies a
	// positive Limit as given.
	Limit int
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
	// Limit caps the page (default 50 when not positive, at most MaxReadLimit).
	Limit int
}

// defaultListJobsLimit caps a ListJobs page when the caller passes no limit.
const defaultListJobsLimit = 50

// ListJobs returns jobs newest-first by id, reading through db and optionally
// filtered by exact state, kind, and queue. Soft-deleted jobs are excluded. It is
// the inspection seam behind a "list jobs" CLI or dashboard. It returns at most
// MaxReadLimit jobs, whatever p.Limit asks for.
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
	limit := readLimit(p.Limit, defaultListJobsLimit)
	var rows []jobRow
	if err := query.Select(jobViewColumns()).Order("id desc").Limit(limit).Find(&rows).Error; err != nil {
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
	view, err := findJobView(ctx, db, id)
	if err != nil && !errors.Is(err, ErrJobNotFound) {
		return JobView{}, fmt.Errorf("flywheel: find job: %w", err)
	}
	return view, err
}

// findJobView reads one job's JobView through db, soft-deleted excluded. A miss
// returns ErrJobNotFound and any other failure the raw error, so FindJob and
// WaitForJobWithOptions each wrap a failure in their own words.
func findJobView(ctx context.Context, db *gorm.DB, id string) (JobView, error) {
	var row jobRow
	err := quietMissing(db).WithContext(ctx).Select(jobViewColumns()).Where("id = ?", id).First(&row).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return JobView{}, ErrJobNotFound
	}
	if err != nil {
		return JobView{}, err
	}
	return jobViewFromRow(row), nil
}

// jobViewColumns is exactly the columns jobViewFromRow reads — every JobView
// field and nothing else, the args payload in particular — so the reads that
// return a JobView never load a payload the view deliberately omits.
func jobViewColumns() []string {
	return []string{
		"id", "kind", "queue", "state", "parent_job_id", "created_at", "attempt", "max_attempts",
		"priority", "executor_class", "scheduled_at", "finalized_at", "updated_at", "tags",
	}
}

// ListRuns returns a job's runs newest-first by attempt (attempt desc, id desc),
// reading through db; the job_runs_job_attempt unique index serves the order. A
// positive p.BeforeAttempt returns only earlier attempts, a non-zero p.Before only
// runs created strictly before it, and p.Limit caps the page: a zero or negative
// Limit reads the newest MaxReadLimit runs, and a larger one is clamped to it.
// Page past them with BeforeAttempt.
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
	var rows []jobRunRow
	if err := query.Order("attempt desc, id desc").Limit(readLimit(p.Limit, MaxReadLimit)).Find(&rows).Error; err != nil {
		return nil, fmt.Errorf("flywheel: list runs: %w", err)
	}
	views := make([]JobRunView, len(rows))
	for i := range rows {
		views[i] = jobRunViewFromRow(rows[i])
	}
	return views, nil
}

// LatestRun returns a job's newest attempt — the job_runs row with the highest
// attempt — and true, or false when the job has none yet. It is ListRuns with
// Limit 1, served by job_runs_job_attempt, and it reads job_runs alone, so a job
// that does not exist reads like one that has not started. Once WaitForJob
// reports a job succeeded, LatestRun is the attempt that succeeded, and its Output
// is what the worker returned.
func LatestRun(ctx context.Context, db *gorm.DB, jobID string) (JobRunView, bool, error) {
	runs, err := ListRuns(ctx, db, jobID, ListRunsParams{Limit: 1})
	if err != nil {
		return JobRunView{}, false, err
	}
	if len(runs) == 0 {
		return JobRunView{}, false, nil
	}
	return runs[0], true, nil
}

// Overview returns the job count grouped by state, optionally scoped to a single
// kind, reading through db. Soft-deleted jobs are excluded.
//
// It is a full count by contract: it reads every job in scope, succeeded history
// included, so its cost grows with the table — without bound while retention is
// off — and its kind filter has no index. It suits an inspection a person runs.
// For a number polled on a timer, use the bounded reads: QueueDepths (each
// queue's live depth), CountActiveByKind (in-flight jobs per kind, on
// jobs_state), or Stats (outcomes over a window, from the hourly rollups).
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
//
// It has no limit, by contract: it loads every in-flight job of the kind with
// its args, so its cost grows with the kind's backlog. A page would make it
// cheaper and wrong, since a job past the page is one the caller never sees, and
// a truncated "is there already a job?" answer is worse than a slow one.
//
// Deprecated: ListActiveByKind is unbounded by contract and returns args; use
// InsertOpts.UniqueActiveKey, which answers the same question with one index
// probe.
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
//
// It is a full count by contract: it reads every job_runs row, so its cost grows
// with the run history — without bound while retention is off. For throughput
// polled on a timer, read Stats, which counts a window's attempts from the
// hourly rollups; QueueDepths and CountActiveByKind are the bounded reads of
// the live queue.
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
		v.OutputTruncated = isTruncatedOutput(r.Output)
	}
	if len(r.ErrorPayload) > 0 {
		v.ErrorPayload = json.RawMessage(r.ErrorPayload)
		// Best-effort, like a JobView's tags: a payload that does not decode is a
		// hand-written row, and it reads as unflagged rather than failing the page.
		var payload struct {
			Truncated bool   `json:"truncated"`
			Stack     string `json:"stack"`
		}
		if err := json.Unmarshal(r.ErrorPayload, &payload); err == nil {
			v.ErrorTruncated = payload.Truncated
			v.ErrorStack = payload.Stack
		}
	}
	return v
}

// isTruncatedOutput reports whether a stored output is a TruncatedOutput: an
// object of exactly $truncated (true), $bytes (a number), and $head (a string).
// An output of the worker's own that shares a key is not one. The byte probe
// keeps a page of ordinary outputs from being decoded twice.
func isTruncatedOutput(raw []byte) bool {
	if !bytes.Contains(raw, []byte(`"$truncated"`)) {
		return false
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil || len(fields) != 3 {
		return false
	}
	var marker struct {
		Truncated *bool   `json:"$truncated"`
		Bytes     *int    `json:"$bytes"`
		Head      *string `json:"$head"`
	}
	if err := json.Unmarshal(raw, &marker); err != nil {
		return false
	}
	return marker.Truncated != nil && *marker.Truncated && marker.Bytes != nil && marker.Head != nil
}
