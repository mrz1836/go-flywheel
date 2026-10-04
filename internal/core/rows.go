package core

import (
	"time"

	"gorm.io/datatypes"
	"gorm.io/gorm"
)

// jobRow is the runtime's own GORM mapping of the jobs table. The runtime owns
// its row structs so it stays decoupled from any host application's models.
//
// The gorm tags carry the canonical schema: every column the runtime relies on
// is NOT NULL with a matching default, and the table is soft-deletable through
// gorm.DeletedAt so a host can retire a job without losing its audit trail. The
// constraints are flywheel's own — no host/foundation base model is imported.
//
// LeaseToken is the exception to "every column the runtime relies on is NOT
// NULL": it identifies the claim that currently holds the job, so it is null
// for exactly as long as no claim does. It is stamped by the claim, required by
// finalize and lease renewal, and cleared on every transition out of running —
// which is what makes "this attempt still holds the job" answerable, as distinct
// from "this job is running". It carries no index: it is only ever read in a
// predicate already anchored by id, the primary key.
type jobRow struct {
	ID              string         `gorm:"column:id;primaryKey"`
	CreatedAt       time.Time      `gorm:"column:created_at;not null"`
	UpdatedAt       time.Time      `gorm:"column:updated_at;not null"`
	Metadata        datatypes.JSON `gorm:"column:metadata;type:jsonb;not null;default:'{}'"`
	Kind            string         `gorm:"column:kind;not null"`
	Queue           string         `gorm:"column:queue;not null;default:'default'"`
	Args            datatypes.JSON `gorm:"column:args;type:jsonb;not null"`
	Priority        int            `gorm:"column:priority;not null;default:100"`
	State           string         `gorm:"column:state;not null;default:'available'"`
	Attempt         int            `gorm:"column:attempt;not null;default:0"`
	MaxAttempts     int            `gorm:"column:max_attempts;not null;default:25"`
	TimeoutMs       *int           `gorm:"column:timeout_ms"`
	ScheduledAt     time.Time      `gorm:"column:scheduled_at;not null"`
	LeasedUntil     *time.Time     `gorm:"column:leased_until"`
	LeaseToken      *string        `gorm:"column:lease_token"`
	UniqueKey       *string        `gorm:"column:unique_key"`
	UniqueActiveKey *string        `gorm:"column:unique_active_key"`
	ParentJobID     *string        `gorm:"column:parent_job_id"`
	ExecutorClass   string         `gorm:"column:executor_class;not null;default:''"`
	FinalizedAt     *time.Time     `gorm:"column:finalized_at"`
	// BarrierKind and BarrierSpec carry a fan-in barrier a job declared over its
	// own children. They are set on the spawning job's row when its Finalize
	// processes Result.Barrier, and read by each child's Finalize to decide whether
	// that child completed the generation. BarrierKind IS NULL is the fast gate that
	// skips the completion count for the overwhelmingly common non-barrier finalize;
	// BarrierSpec is the fully-resolved continuation (args, queue, executor_class,
	// priority), so the child that fires the barrier needs no further defaulting.
	// Both are cleared back to NULL once the barrier fires.
	BarrierKind *string        `gorm:"column:barrier_kind"`
	BarrierSpec datatypes.JSON `gorm:"column:barrier_spec;type:jsonb"`
	Tags        datatypes.JSON `gorm:"column:tags;type:jsonb;not null;default:'[]'"`
	DeletedAt   gorm.DeletedAt `gorm:"column:deleted_at"`
}

// TableName binds jobRow to the jobs table.
func (jobRow) TableName() string { return "jobs" }

// jobRunRow is the runtime's own mapping of the job_runs table: one audit row
// per attempt, with no soft-delete and NOT-NULL constraints on every identity
// and timing column the runtime always writes.
//
// It is insert-plus-one-update, not append-only: InsertRunStub inserts the row
// with outcome started before the worker runs, and exactly one of Finalize or
// the lease sweep updates it to its final outcome. A superseded finalize that
// lands after the sweep is the one case a row is updated twice, and it leaves
// finished_at as the sweep wrote it (see runFinalizeUpdate).
//
// That update is kept HOT-eligible on PostgreSQL, deliberately: no column it
// writes is indexed, so the new tuple version stays on its page and no index
// gains an entry. finished_at in particular is not indexed — an index on it is
// what a time-windowed read would naturally want, and it measured at about 10%
// of drain throughput and 21% more WAL per job (docs/BENCHMARKS.md). Windowed
// reads find runs through job_run_finishes instead (see jobRunFinishRow).
//
// Its three timestamps are stamped in UTC (see BeforeCreate). On PostgreSQL the
// columns are timestamptz and the zone is immaterial; on SQLite a timestamp is
// text, and only a single zone makes a text range comparison a time comparison.
//
// # The analytics columns
//
// Kind, Queue, QueueWaitMs, JobState, and Superseded exist so per-kind
// statistics read job_runs alone. They ride on writes the runtime already makes
// — InsertRunStub's insert and Finalize's update — so they cost no statement.
// Every one is defaulted or nullable, so a binary older than them keeps writing
// valid rows to a schema that has them: an old binary's row carries kind = ”,
// which the stats aggregate resolves through jobs (see resolvedKindExpr).
type jobRunRow struct {
	ID               string         `gorm:"column:id;primaryKey"`
	JobID            string         `gorm:"column:job_id;not null"`
	Attempt          int            `gorm:"column:attempt;not null"`
	Kind             string         `gorm:"column:kind;not null;default:''"`
	Queue            string         `gorm:"column:queue;not null;default:''"`
	ExecutorClass    string         `gorm:"column:executor_class;not null;default:''"`
	ExecutorID       string         `gorm:"column:executor_id;not null"`
	StartedAt        time.Time      `gorm:"column:started_at;not null"`
	FinishedAt       *time.Time     `gorm:"column:finished_at"`
	Outcome          string         `gorm:"column:outcome;not null"`
	ErrorClass       *string        `gorm:"column:error_class"`
	ErrorMessage     *string        `gorm:"column:error_message"`
	ErrorPayload     datatypes.JSON `gorm:"column:error_payload;type:jsonb"`
	Output           datatypes.JSON `gorm:"column:output;type:jsonb"`
	DurationMs       *int           `gorm:"column:duration_ms"`
	QueueWaitMs      *int           `gorm:"column:queue_wait_ms"`
	CostMicros       *int64         `gorm:"column:cost_micros"`
	EnqueuedChildren int            `gorm:"column:enqueued_children;not null;default:0"`
	// JobState is the job state this attempt's finalize applied: retryable versus
	// discarded is what splits a retried failure from a final one, so job-level
	// outcomes are countable from runs alone. It is NULL while the attempt runs,
	// NULL when the finalize was superseded (it applied nothing), and available
	// when the lease sweep reclaimed the job from a crashed attempt.
	JobState *string `gorm:"column:job_state"`
	// Superseded records that the attempt's finalize found its claim gone — the
	// job was cancelled or reclaimed underneath it — so the outcome on this row
	// was never applied to the job. It is the durable form of SupersedeEvent: a
	// double-execution signal a host can count after the fact.
	Superseded bool      `gorm:"column:superseded;not null;default:false"`
	CreatedAt  time.Time `gorm:"column:created_at;not null"`
}

// TableName binds jobRunRow to the job_runs table.
func (jobRunRow) TableName() string { return "job_runs" }

// jobPeriodicRow is the runtime's own mapping of the job_periodics table. It has
// an explicit IsActive operator toggle in place of soft-delete, so it carries no
// DeletedAt column — the deactivated row stays inspectable.
type jobPeriodicRow struct {
	ID              string         `gorm:"column:id;primaryKey"`
	Slug            string         `gorm:"column:slug;not null"`
	Kind            string         `gorm:"column:kind;not null"`
	ArgsTemplate    datatypes.JSON `gorm:"column:args_template;type:jsonb;not null;default:'{}'"`
	Queue           string         `gorm:"column:queue;not null;default:'periodic'"`
	CronExpr        *string        `gorm:"column:cron_expr"`
	IntervalSeconds *int           `gorm:"column:interval_seconds"`
	NextRunAt       time.Time      `gorm:"column:next_run_at;not null"`
	LastEnqueuedAt  *time.Time     `gorm:"column:last_enqueued_at"`
	IsActive        bool           `gorm:"column:is_active;not null;default:true"`
	CreatedAt       time.Time      `gorm:"column:created_at;not null"`
	UpdatedAt       time.Time      `gorm:"column:updated_at;not null"`
}

// TableName binds jobPeriodicRow to the job_periodics table.
func (jobPeriodicRow) TableName() string { return "job_periodics" }

// jobRunFinishRow is one entry in job_run_finishes, the append-only finish log
// that makes job_runs searchable by finished_at without indexing finished_at on
// job_runs itself (see jobRunRow for why that index is not there).
//
// The finalize that sets a run's finished_at, and the lease sweep that crashes a
// stub, write the run's entry in the same transaction — on PostgreSQL in the
// same statement, folded into the job_runs UPDATE with a data-modifying CTE — so
// the log is exactly the set of finished runs, keyed by when each finished. It is
// written once per run and never updated; retention deletes an entry with its
// run. A run an older release finalized has no entry until the backfill writes it
// (BackfillRunFinishes), which the stats rollup runs on its own.
//
// The primary key is the only index: (finished_at, run_id) puts every window
// read on one contiguous range of it, and makes the entry its own uniqueness
// guard, so a second writer of the same finish — the late finalize of a stub the
// sweep already crashed — is absorbed by ON CONFLICT DO NOTHING.
type jobRunFinishRow struct {
	FinishedAt time.Time `gorm:"column:finished_at;primaryKey;autoIncrement:false"`
	RunID      string    `gorm:"column:run_id;primaryKey"`
}

// TableName binds jobRunFinishRow to the job_run_finishes table.
func (jobRunFinishRow) TableName() string { return "job_run_finishes" }

// jobStatsHourlyRow is one hourly rollup of job_runs for one (kind, queue): the
// outcome counters, the success-duration and queue-wait distributions, and the
// cost, for runs whose finished_at falls in the UTC hour starting at
// BucketStartUnix. The rollup activity writes it (stats_rollup.go) and the stats
// reads sum it (stats.go).
//
// The bucket key is epoch seconds rather than a timestamp so that key equality is
// exact on both dialects: a SQLite timestamp is text whose spelling depends on
// the zone it was written in, and an integer has one spelling. Direct SQL turns it
// back into a time with to_timestamp(bucket_start_unix) on PostgreSQL and
// datetime(bucket_start_unix, 'unixepoch') on SQLite.
//
// No field carries a default: tag. GORM omits a zero-valued field that has one
// from the INSERT, and a counter that is legitimately zero must be written as
// zero, not left to whatever the column default happens to be.
//
// An hour with no runs has no rows. Which hours the table speaks for — so that a
// missing hour means "nothing ran" rather than "not rolled up yet" — is recorded
// apart from the rows, in job_stats_progress (see jobStatsProgressRow).
type jobStatsHourlyRow struct {
	BucketStartUnix int64  `gorm:"column:bucket_start_unix;primaryKey;autoIncrement:false"`
	Kind            string `gorm:"column:kind;primaryKey"`
	Queue           string `gorm:"column:queue;primaryKey"`

	// Attempt outcome counters. Superseded attempts are counted only in
	// Superseded and excluded from every other counter and from the durations:
	// their outcome was never applied to the job.
	Attempts   int64 `gorm:"column:attempts;not null"`
	Success    int64 `gorm:"column:success;not null"`
	Error      int64 `gorm:"column:error;not null"`
	Timeout    int64 `gorm:"column:timeout;not null"`
	Snooze     int64 `gorm:"column:snooze;not null"`
	Cancelled  int64 `gorm:"column:cancelled;not null"`
	Crashed    int64 `gorm:"column:crashed;not null"`
	Superseded int64 `gorm:"column:superseded;not null"`
	// Discarded counts the attempts that discarded their job — the job-level
	// failures, as distinct from the retried errors.
	Discarded int64 `gorm:"column:discarded;not null"`

	// The success-duration distribution, over successful attempts only, so a burst
	// of fast failures cannot read as a speed-up.
	DurCount     int64          `gorm:"column:dur_count;not null"`
	DurSumMs     int64          `gorm:"column:dur_sum_ms;not null"`
	DurMaxMs     int64          `gorm:"column:dur_max_ms;not null"`
	DurP50Ms     int64          `gorm:"column:dur_p50_ms;not null"`
	DurP95Ms     int64          `gorm:"column:dur_p95_ms;not null"`
	DurP99Ms     int64          `gorm:"column:dur_p99_ms;not null"`
	DurHist      datatypes.JSON `gorm:"column:dur_hist;type:jsonb;not null"`
	SlowestRunID *string        `gorm:"column:slowest_run_id"`

	// The queue-wait distribution, over every non-superseded attempt that recorded
	// one.
	WaitCount int64          `gorm:"column:wait_count;not null"`
	WaitSumMs int64          `gorm:"column:wait_sum_ms;not null"`
	WaitMaxMs int64          `gorm:"column:wait_max_ms;not null"`
	WaitP95Ms int64          `gorm:"column:wait_p95_ms;not null"`
	WaitHist  datatypes.JSON `gorm:"column:wait_hist;type:jsonb;not null"`

	CostMicrosSum int64 `gorm:"column:cost_micros_sum;not null"`
	// HistVersion names the bucket bounds DurHist and WaitHist were counted
	// against (see histBounds), so a future change of bounds cannot be merged with
	// counts taken under the old ones.
	HistVersion int       `gorm:"column:hist_version;not null"`
	RolledAt    time.Time `gorm:"column:rolled_at;not null"`
}

// TableName binds jobStatsHourlyRow to the job_stats_hourly table.
func (jobStatsHourlyRow) TableName() string { return "job_stats_hourly" }

// statsProgressID is the key of job_stats_progress's one row.
const statsProgressID = 1

// jobStatsProgressRow is the one row of job_stats_progress: the range of UTC
// hours job_stats_hourly is complete for, and a version that moves whenever a
// rolled hour or the range changes.
//
// Every hour in [CoveredFromUnix, CoveredToUnix) has been rolled up: its rows in
// job_stats_hourly count every run that finished in it, and an hour with no rows
// had none. The stats reads serve those hours from the rollup and everything
// outside them from raw runs; retention never deletes a run the range has not
// reached (see statsRetentionCap).
//
// The range is recorded rather than inferred from the rows because the rows
// cannot say it. An empty hour has no rows, so the first and last stored hours
// are not where coverage starts and ends; and a RebuildStats of hours the rollup
// has not reached yet stores rows past a gap of hours nothing has rolled. Every
// writer extends the range only across hours it has itself just rolled or proven
// empty, in the same transaction, so the range never spans an unrolled hour.
//
// RetainFromUnix is the oldest hour the rollup keeps — now minus StatsRetention
// as of its latest pass. While CoveredFromUnix is above it the rollup is still
// working back through history, and retention holds off the runs it has yet to
// count.
//
// Version increases with every write that changes a rolled hour or the range.
// Baselines memoizes on it, so a rebuild in any process invalidates every
// process's memo on its next read.
type jobStatsProgressRow struct {
	ID              int       `gorm:"column:id;primaryKey;autoIncrement:false"`
	CoveredFromUnix int64     `gorm:"column:covered_from_unix;not null"`
	CoveredToUnix   int64     `gorm:"column:covered_to_unix;not null"`
	RetainFromUnix  int64     `gorm:"column:retain_from_unix;not null"`
	Version         int64     `gorm:"column:version;not null"`
	UpdatedAt       time.Time `gorm:"column:updated_at;not null"`
}

// TableName binds jobStatsProgressRow to the job_stats_progress table.
func (jobStatsProgressRow) TableName() string { return "job_stats_progress" }

// limiterBucketRow is the DBLimiter's per-resource rate reservoir. The resource
// string is the primary key — one bucket per protected dependency — and the row
// doubles as the per-resource mutex a shared Acquire locks. It carries no
// BeforeCreate: the limiter sets every column and inserts by the meaningful
// resource key, never Save.
type limiterBucketRow struct {
	Resource   string    `gorm:"column:resource;primaryKey"`
	Tokens     int       `gorm:"column:tokens;not null"`
	RefilledAt time.Time `gorm:"column:refilled_at;not null"`
	UpdatedAt  time.Time `gorm:"column:updated_at;not null"`
}

// TableName binds limiterBucketRow to the limiter_buckets table.
func (limiterBucketRow) TableName() string { return "limiter_buckets" }

// limiterHoldRow is one DBLimiter concurrency grant: a minted token, the resource
// it holds, the count it holds (n), and when its reservation lapses without a
// Release. One row per grant carries the whole batch, so its expiry is a shared
// TTL — the deliberately-coarse over-admission-on-lapse the crashed-holder case
// accepts. Like the bucket row it has no BeforeCreate.
type limiterHoldRow struct {
	Token     string    `gorm:"column:token;primaryKey"`
	Resource  string    `gorm:"column:resource;not null"`
	N         int       `gorm:"column:n;not null"`
	ExpiresAt time.Time `gorm:"column:expires_at;not null"`
}

// TableName binds limiterHoldRow to the limiter_holds table.
func (limiterHoldRow) TableName() string { return "limiter_holds" }
