package flywheel

import (
	"time"

	"gorm.io/datatypes"
)

// jobRow is the runtime's own GORM mapping of the jobs table. The runtime owns
// its row structs so it stays decoupled from any host application's models.
type jobRow struct {
	ID          string         `gorm:"column:id;primaryKey"`
	CreatedAt   time.Time      `gorm:"column:created_at"`
	UpdatedAt   time.Time      `gorm:"column:updated_at"`
	Metadata    datatypes.JSON `gorm:"column:metadata"`
	Kind        string         `gorm:"column:kind"`
	Queue       string         `gorm:"column:queue"`
	Args        datatypes.JSON `gorm:"column:args"`
	Priority    int            `gorm:"column:priority"`
	State       string         `gorm:"column:state"`
	Attempt     int            `gorm:"column:attempt"`
	MaxAttempts int            `gorm:"column:max_attempts"`
	ScheduledAt time.Time      `gorm:"column:scheduled_at"`
	LeasedUntil *time.Time     `gorm:"column:leased_until"`
	UniqueKey   *string        `gorm:"column:unique_key"`
	ParentJobID *string        `gorm:"column:parent_job_id"`
	RunOn       string         `gorm:"column:run_on"`
	FinalizedAt *time.Time     `gorm:"column:finalized_at"`
	Tags        datatypes.JSON `gorm:"column:tags"`
}

// TableName binds jobRow to the jobs table.
func (jobRow) TableName() string { return "jobs" }

// jobRunRow is the runtime's own mapping of the job_runs table.
type jobRunRow struct {
	ID               string         `gorm:"column:id;primaryKey"`
	JobID            string         `gorm:"column:job_id"`
	Attempt          int            `gorm:"column:attempt"`
	ExecutorKind     string         `gorm:"column:executor_kind"`
	ExecutorID       string         `gorm:"column:executor_id"`
	StartedAt        time.Time      `gorm:"column:started_at"`
	FinishedAt       *time.Time     `gorm:"column:finished_at"`
	Outcome          string         `gorm:"column:outcome"`
	ErrorClass       *string        `gorm:"column:error_class"`
	ErrorMessage     *string        `gorm:"column:error_message"`
	ErrorPayload     datatypes.JSON `gorm:"column:error_payload"`
	Output           datatypes.JSON `gorm:"column:output"`
	DurationMs       *int           `gorm:"column:duration_ms"`
	CostMicros       *int64         `gorm:"column:cost_micros"`
	EnqueuedChildren int            `gorm:"column:enqueued_children"`
	CreatedAt        time.Time      `gorm:"column:created_at"`
}

// TableName binds jobRunRow to the job_runs table.
func (jobRunRow) TableName() string { return "job_runs" }

// jobPeriodicRow is the runtime's own mapping of the job_periodics table.
type jobPeriodicRow struct {
	ID              string         `gorm:"column:id;primaryKey"`
	Slug            string         `gorm:"column:slug"`
	Kind            string         `gorm:"column:kind"`
	ArgsTemplate    datatypes.JSON `gorm:"column:args_template"`
	Queue           string         `gorm:"column:queue"`
	CronExpr        *string        `gorm:"column:cron_expr"`
	IntervalSeconds *int           `gorm:"column:interval_seconds"`
	NextRunAt       time.Time      `gorm:"column:next_run_at"`
	LastEnqueuedAt  *time.Time     `gorm:"column:last_enqueued_at"`
	IsActive        bool           `gorm:"column:is_active"`
	CreatedAt       time.Time      `gorm:"column:created_at"`
	UpdatedAt       time.Time      `gorm:"column:updated_at"`
}

// TableName binds jobPeriodicRow to the job_periodics table.
func (jobPeriodicRow) TableName() string { return "job_periodics" }
