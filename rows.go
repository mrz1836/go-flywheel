package flywheel

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
type jobRow struct {
	ID            string         `gorm:"column:id;primaryKey"`
	CreatedAt     time.Time      `gorm:"column:created_at;not null"`
	UpdatedAt     time.Time      `gorm:"column:updated_at;not null"`
	Metadata      datatypes.JSON `gorm:"column:metadata;type:jsonb;not null;default:'{}'"`
	Kind          string         `gorm:"column:kind;not null"`
	Queue         string         `gorm:"column:queue;not null;default:'default'"`
	Args          datatypes.JSON `gorm:"column:args;type:jsonb;not null"`
	Priority      int            `gorm:"column:priority;not null;default:100"`
	State         string         `gorm:"column:state;not null;default:'available'"`
	Attempt       int            `gorm:"column:attempt;not null;default:0"`
	MaxAttempts   int            `gorm:"column:max_attempts;not null;default:25"`
	TimeoutMs     *int           `gorm:"column:timeout_ms"`
	ScheduledAt   time.Time      `gorm:"column:scheduled_at;not null"`
	LeasedUntil   *time.Time     `gorm:"column:leased_until"`
	UniqueKey     *string        `gorm:"column:unique_key"`
	ParentJobID   *string        `gorm:"column:parent_job_id"`
	ExecutorClass string         `gorm:"column:executor_class;not null;default:''"`
	FinalizedAt   *time.Time     `gorm:"column:finalized_at"`
	Tags          datatypes.JSON `gorm:"column:tags;type:jsonb;not null;default:'[]'"`
	DeletedAt     gorm.DeletedAt `gorm:"column:deleted_at"`
}

// TableName binds jobRow to the jobs table.
func (jobRow) TableName() string { return "jobs" }

// jobRunRow is the runtime's own mapping of the job_runs table. It is an
// append-only audit log — no soft-delete — with NOT-NULL constraints on every
// identity and timing column the runtime always writes.
type jobRunRow struct {
	ID               string         `gorm:"column:id;primaryKey"`
	JobID            string         `gorm:"column:job_id;not null"`
	Attempt          int            `gorm:"column:attempt;not null"`
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
	CostMicros       *int64         `gorm:"column:cost_micros"`
	EnqueuedChildren int            `gorm:"column:enqueued_children;not null;default:0"`
	CreatedAt        time.Time      `gorm:"column:created_at;not null"`
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
