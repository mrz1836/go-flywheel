package core

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"gorm.io/gorm"
)

// defaultMaxArgsBytes is the args bound of the zero GetJobOpts.
const defaultMaxArgsBytes = 1 << 20

// JobDetail is one job in full, as GetJob reads it: the JobView a list returns,
// plus every column a list leaves out, as stored, and the run of the job's
// current attempt.
type JobDetail struct {
	JobView
	// Args is the job's args exactly as stored, a large integer kept exact. It
	// is nil when ArgsOmitted is set.
	Args json.RawMessage `json:"args,omitempty"`
	// ArgsBytes is the stored args' length in bytes, measured in the database
	// whether or not they were loaded.
	ArgsBytes int `json:"args_bytes"`
	// ArgsOmitted reports that the args were longer than GetJobOpts.MaxArgsBytes,
	// so they were left in the database and Args is nil.
	ArgsOmitted bool `json:"args_omitted,omitempty"`
	// Metadata is the job's metadata object as stored: the request id it was
	// enqueued under, the producer's keys, and an insert hook's (see InsertOpts).
	// It is {} when the stored value is empty or NULL.
	Metadata json.RawMessage `json:"metadata"`
	// UniqueKey and UniqueActiveKey are the job's unique keys, empty when unset.
	UniqueKey       string `json:"unique_key,omitempty"`
	UniqueActiveKey string `json:"unique_active_key,omitempty"`
	// TimeoutMs is the job's own execution timeout in milliseconds; nil when it
	// has none and the worker's or the runner's applies.
	TimeoutMs *int `json:"timeout_ms,omitempty"`
	// LeasedUntil is when the running attempt's lease expires; nil when no
	// attempt holds the job.
	LeasedUntil *time.Time `json:"leased_until,omitempty"`
	// Barrier is the continuation this job declared over its children, while it
	// has not fired; nil otherwise.
	Barrier *BarrierView `json:"barrier,omitempty"`
	// CancelPending reports a job cancelled while its attempt still runs: the job
	// is cancelled and the run of its current attempt still reads started. It
	// turns false when that attempt's late finalize records it. An attempt whose
	// process died after the cancel never finalizes, so it stays true.
	CancelPending bool `json:"cancel_pending"`
	// LatestRun is the run of the attempt the job names (jobs.attempt); nil when
	// the job was never claimed, or its claim wrote no run.
	LatestRun *JobRunView `json:"latest_run,omitempty"`
}

// BarrierView is a declared barrier's continuation, as stored when the barrier
// was declared: the job it enqueues once every child of the generation is
// terminal (see Barrier).
type BarrierView struct {
	Kind          string        `json:"kind"`
	Queue         string        `json:"queue"`
	ExecutorClass ExecutorClass `json:"executor_class"`
	Priority      int           `json:"priority"`
	// Args is the continuation's args as stored. A barrier's args are routing
	// data its worker wrote, so they are not bounded by GetJobOpts.MaxArgsBytes.
	Args json.RawMessage `json:"args,omitempty"`
}

// GetJobOpts configures GetJobWithOptions. The zero value loads args of up to
// 1 MiB.
type GetJobOpts struct {
	// MaxArgsBytes bounds the args GetJob loads. Zero selects 1,048,576; a
	// negative value loads args whatever their size. Args longer than the bound
	// stay in the database: JobDetail reports their size and ArgsOmitted.
	MaxArgsBytes int
}

// GetJob returns one job in full: GetJobWithOptions with the zero GetJobOpts,
// which loads args of up to 1 MiB.
func GetJob(ctx context.Context, db *gorm.DB, id string) (JobDetail, error) {
	return GetJobWithOptions(ctx, db, id, GetJobOpts{})
}

// GetJobWithOptions returns one job in full, the read behind a page that shows
// a single job: every column as stored, with no redaction, and the run of the
// job's current attempt. A soft-deleted job is excluded, and a miss returns
// ErrJobNotFound.
//
// Args are measured in SQL and selected in the same statement only within
// opts.MaxArgsBytes, so a payload past the bound never leaves the database; the
// detail reports its size instead. On PostgreSQL the length is the jsonb value's
// text rendering, which the database computes from the one row.
//
// It is two keyed reads through db as given, with no transaction of its own:
// the job by primary key, then its run by (job_id, attempt). A host that needs
// the two consistent on a lagging replica passes a transaction.
func GetJobWithOptions(ctx context.Context, db *gorm.DB, id string, opts GetJobOpts) (JobDetail, error) {
	bound := opts.MaxArgsBytes
	if bound == 0 {
		bound = defaultMaxArgsBytes
	}
	length := argsLengthSQL(db.Name())
	cols := append(jobViewColumns(),
		"metadata", "unique_key", "unique_active_key", "timeout_ms", "leased_until", "barrier_kind", "barrier_spec",
		length+" AS args_bytes")
	query := quietMissing(db).WithContext(ctx).Model(&jobRow{})
	if bound < 0 {
		query = query.Select(strings.Join(append(cols, "args"), ", "))
	} else {
		query = query.Select(strings.Join(cols, ", ")+", CASE WHEN "+length+" <= ? THEN args END AS args", bound)
	}
	var row jobDetailRow
	if err := query.Where("id = ?", id).Take(&row).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return JobDetail{}, ErrJobNotFound
		}
		return JobDetail{}, fmt.Errorf("flywheel: get job: %w", err)
	}

	detail := jobDetailFromRow(row, bound)
	if row.Job.Attempt > 0 {
		var run jobRunRow
		err := quietMissing(db).WithContext(ctx).Where("job_id = ? AND attempt = ?", id, row.Job.Attempt).Take(&run).Error
		switch {
		case err == nil:
			view := jobRunViewFromRow(run)
			detail.LatestRun = &view
		case !errors.Is(err, gorm.ErrRecordNotFound):
			return JobDetail{}, fmt.Errorf("flywheel: get job: %w", err)
		}
	}
	detail.CancelPending = detail.State == string(StateCancelled) &&
		detail.LatestRun != nil && detail.LatestRun.Outcome == string(OutcomeStarted)
	return detail, nil
}

// jobDetailRow is GetJob's read of one jobs row: the columns it selects, and
// the args' length the database measured. The row is a named, exported field:
// GORM does not scan into an unexported embedded struct.
type jobDetailRow struct {
	Job       jobRow `gorm:"embedded"`
	ArgsBytes int    `gorm:"column:args_bytes"`
}

// argsLengthSQL is the SQL expression for the stored args' length in bytes: on
// PostgreSQL the jsonb value's text rendering, the bytes a read of the column
// sends; on SQLite the stored text.
func argsLengthSQL(dialect string) string {
	if dialect == "postgres" {
		return "octet_length(args::text)"
	}
	return "length(CAST(args AS BLOB))"
}

// jobDetailFromRow builds a JobDetail from row, read under the args bound
// (negative: none), all but its latest run and CancelPending.
func jobDetailFromRow(row jobDetailRow, bound int) JobDetail {
	job := row.Job
	detail := JobDetail{
		JobView:         jobViewFromRow(job),
		ArgsBytes:       row.ArgsBytes,
		ArgsOmitted:     bound >= 0 && row.ArgsBytes > bound,
		Metadata:        json.RawMessage(`{}`),
		UniqueKey:       derefString(job.UniqueKey),
		UniqueActiveKey: derefString(job.UniqueActiveKey),
		TimeoutMs:       job.TimeoutMs,
		LeasedUntil:     job.LeasedUntil,
	}
	if !detail.ArgsOmitted && len(job.Args) > 0 {
		detail.Args = json.RawMessage(job.Args)
	}
	if len(job.Metadata) > 0 {
		detail.Metadata = json.RawMessage(job.Metadata)
	}
	if job.BarrierKind != nil {
		// Best effort, like a JobView's tags: a spec that does not decode is a
		// hand-written row, and it reads as the barrier's kind alone.
		detail.Barrier = &BarrierView{Kind: *job.BarrierKind}
		var spec barrierSpec
		if err := json.Unmarshal(job.BarrierSpec, &spec); err == nil {
			detail.Barrier.Queue = spec.Queue
			detail.Barrier.ExecutorClass = ExecutorClass(spec.ExecutorClass)
			detail.Barrier.Priority = spec.Priority
			detail.Barrier.Args = spec.Args
		}
	}
	return detail
}
