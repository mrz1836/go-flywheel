package flywheel

import (
	"gorm.io/datatypes"
	"gorm.io/gorm"
)

// defaultPeriodicQueue is the queue assigned to a periodic definition without an
// explicit choice. The job-side producer defaults (queue/priority/max_attempts)
// live in client.go and are reused here so a row created on any path — Insert,
// the scheduler, or a host seed — lands with identical defaults.
const defaultPeriodicQueue = "periodic"

// BeforeCreate mints the ID, stamps timestamps, requires Kind, and applies the
// queue/state/priority/max_attempts/args/tags/metadata defaults. Running
// the defaulting in the row's own lifecycle hook means a caller gets a correct
// row without the host having to replicate the producer defaults — the runtime
// owns the invariant.
//
//nolint:gocognit,gocyclo // a flat sequence of independent field defaults and checks
func (j *jobRow) BeforeCreate(tx *gorm.DB) error {
	now := ClockFrom(tx.Statement.Context).Now(tx.Statement.Context)
	if j.ID == "" {
		j.ID = NewID()
	}
	if j.CreatedAt.IsZero() {
		j.CreatedAt = now
	}
	if j.UpdatedAt.IsZero() {
		j.UpdatedAt = now
	}

	if j.Kind == "" {
		return newValidationError("kind", "is required")
	}
	if j.Queue == "" {
		j.Queue = defaultQueue
	}
	if j.State == "" {
		j.State = string(StateAvailable)
	}
	if !JobState(j.State).Valid() {
		return newValidationError("state", "is not a recognized job state")
	}
	if j.Priority == 0 {
		j.Priority = defaultPriority
	}
	if j.MaxAttempts == 0 {
		j.MaxAttempts = defaultMaxAttempts
	}
	if len(j.Tags) == 0 {
		j.Tags = datatypes.JSON("[]")
	}
	if len(j.Args) == 0 {
		j.Args = datatypes.JSON("{}")
	}
	if len(j.Metadata) == 0 {
		j.Metadata = datatypes.JSON("{}")
	}
	if j.ScheduledAt.IsZero() {
		j.ScheduledAt = now
	}
	return nil
}

// BeforeCreate mints the ID, requires the mandatory audit fields, and defaults
// StartedAt and CreatedAt to the context clock's now. job_runs is append-only,
// so there is no save-time hook.
func (r *jobRunRow) BeforeCreate(tx *gorm.DB) error {
	now := ClockFrom(tx.Statement.Context).Now(tx.Statement.Context)
	if r.ID == "" {
		r.ID = NewID()
	}
	if r.JobID == "" {
		return newValidationError("job_id", "is required")
	}
	if r.ExecutorID == "" {
		return newValidationError("executor_id", "is required")
	}
	if r.Outcome == "" {
		return newValidationError("outcome", "is required")
	}
	if r.StartedAt.IsZero() {
		r.StartedAt = now
	}
	if r.CreatedAt.IsZero() {
		r.CreatedAt = now
	}
	return nil
}

// BeforeCreate mints the ID and defaults CreatedAt. Required-field and schedule
// validation runs in BeforeSave so it covers both create and full-row update.
func (p *jobPeriodicRow) BeforeCreate(tx *gorm.DB) error {
	now := ClockFrom(tx.Statement.Context).Now(tx.Statement.Context)
	if p.ID == "" {
		p.ID = NewID()
	}
	if p.CreatedAt.IsZero() {
		p.CreatedAt = now
	}
	return nil
}

// BeforeSave validates the required fields and the exactly-one-of
// cron/interval rule, applies the queue/args defaults, and stamps UpdatedAt. It
// runs on both create and full-row update.
//
// The scheduler advances a definition with a column map on a bare model
// (Model(&jobPeriodicRow{}).Updates(...)); that path carries no identity fields,
// so it is detected and skipped — partial column updates neither validate nor
// re-default.
func (p *jobPeriodicRow) BeforeSave(tx *gorm.DB) error {
	if p.ID == "" && p.Slug == "" && p.Kind == "" {
		return nil
	}

	if p.Slug == "" {
		return newValidationError("slug", "is required")
	}
	if p.Kind == "" {
		return newValidationError("kind", "is required")
	}
	if p.NextRunAt.IsZero() {
		return newValidationError("next_run_at", "is required")
	}

	hasCron := p.CronExpr != nil && *p.CronExpr != ""
	hasInterval := p.IntervalSeconds != nil
	if hasCron == hasInterval {
		return newValidationError("schedule", "exactly one of cron_expr or interval_seconds is required")
	}

	if p.Queue == "" {
		p.Queue = defaultPeriodicQueue
	}
	if len(p.ArgsTemplate) == 0 {
		p.ArgsTemplate = datatypes.JSON("{}")
	}
	p.UpdatedAt = ClockFrom(tx.Statement.Context).Now(tx.Statement.Context)
	return nil
}
