package core

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/mrz1836/go-foundation/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

// writePathOpener builds the database and Driver a write-path case runs
// against, so one suite proves both dialects.
type writePathOpener func(t *testing.T) (*gorm.DB, Driver)

// sqliteWritePath is the SQLite opener.
func sqliteWritePath(t *testing.T) (*gorm.DB, Driver) {
	t.Helper()
	db := newDB(t)
	return db, NewSQLiteDriver(db)
}

// writePathT0 anchors every write-path case: the job becomes claimable at T0,
// starts 3s later, and finishes 2s after that — so a correct row reads
// queue_wait_ms 3000 and duration_ms 2000. The zone is deliberately not UTC:
// the run row must be stamped in UTC regardless.
//
//nolint:gochecknoglobals // shared fixture instant
var writePathT0 = time.Date(2026, 3, 8, 6, 30, 0, 0, time.FixedZone("UTC-5", -5*3600))

// claimForWritePath enqueues one job (kind "stats.write", queue "wq") at T0,
// claims it at T0+3s through the real Dequeue, and inserts its run stub — the
// exact sequence a Runner performs before the worker runs.
func claimForWritePath(t *testing.T, db *gorm.DB, d Driver, maxAttempts int) (RawJob, string) {
	t.Helper()
	ctx := clockCtx(context.Background(), models.NewFixedClock(writePathT0))
	_, err := Enqueue(ctx, NewClient(db), "stats.write", []byte(`{}`),
		InsertOpts{Queue: "wq", MaxAttempts: maxAttempts})
	require.NoError(t, err)

	start := writePathT0.Add(3 * time.Second)
	claimCtx := clockCtx(context.Background(), models.NewFixedClock(start))
	jobs, err := d.Dequeue(claimCtx, []string{"wq"}, AnyClass, true, 1, 10*time.Second)
	require.NoError(t, err)
	require.Len(t, jobs, 1)
	runID := models.NewID()
	require.NoError(t, d.InsertRunStub(claimCtx, runID, jobs[0], start, "local", "exec-1"))
	return jobs[0], runID
}

// loadRun reads one run row back.
func loadRun(t *testing.T, db *gorm.DB, runID string) jobRunRow {
	t.Helper()
	var row jobRunRow
	require.NoError(t, db.Where("id = ?", runID).First(&row).Error)
	return row
}

// writePathSuite is every outcome's column stamping, run against one dialect.
//
//nolint:gocognit,maintidx // one table plus the three cases a table cannot express
func writePathSuite(t *testing.T, open writePathOpener) {
	finish := writePathT0.Add(5 * time.Second)
	snooze := time.Minute
	cases := []struct {
		name        string
		maxAttempts int
		result      Result
		workErr     error
		outcome     RunOutcome
		jobState    JobState
		errorClass  string
	}{
		{name: "success", maxAttempts: 5, outcome: OutcomeSuccess, jobState: StateSucceeded},
		{
			name: "error retried", maxAttempts: 5, workErr: errors.New("boom"),
			outcome: OutcomeError, jobState: StateRetryable, errorClass: string(ErrorTransient),
		},
		{
			name: "error discarded on the last attempt", maxAttempts: 1, workErr: errors.New("boom"),
			outcome: OutcomeError, jobState: StateDiscarded, errorClass: string(ErrorTransient),
		},
		{
			name: "permanent error discarded", maxAttempts: 5,
			workErr: &classifiedError{cause: errors.New("bad"), class: ErrorPermanent},
			outcome: OutcomeError, jobState: StateDiscarded, errorClass: string(ErrorPermanent),
		},
		{
			name: "timeout", maxAttempts: 5,
			workErr: &classifiedError{cause: context.DeadlineExceeded, class: ErrorTimeout},
			outcome: OutcomeTimeout, jobState: StateRetryable, errorClass: string(ErrorTimeout),
		},
		{name: "snooze", maxAttempts: 5, result: Result{Snooze: &snooze}, outcome: OutcomeSnooze, jobState: StateScheduled},
		{name: "cancel", maxAttempts: 5, result: Result{Cancel: true}, outcome: OutcomeCancelled, jobState: StateCancelled},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			db, d := open(t)
			raw, runID := claimForWritePath(t, db, d, tc.maxAttempts)

			out, err := d.Finalize(context.Background(), raw, runID, tc.result, tc.workErr, finish)
			require.NoError(t, err)
			require.False(t, out.Superseded)

			row := loadRun(t, db, runID)
			assert.Equal(t, "stats.write", row.Kind, "the run records its job's kind")
			assert.Equal(t, "wq", row.Queue, "and its queue")
			require.NotNil(t, row.QueueWaitMs)
			assert.Equal(t, 3000, *row.QueueWaitMs, "the wait is start minus claimable")
			require.NotNil(t, row.DurationMs)
			assert.Equal(t, 2000, *row.DurationMs)
			assert.Equal(t, string(tc.outcome), row.Outcome)
			require.NotNil(t, row.JobState, "a finalize that applied a state records it")
			assert.Equal(t, string(tc.jobState), *row.JobState)
			assert.Equal(t, string(tc.jobState), jobState(t, db, raw.ID), "and it is the state the job holds")
			assert.False(t, row.Superseded)
			require.NotNil(t, row.FinishedAt)
			assert.True(t, row.FinishedAt.Equal(finish))
			entries := finishEntries(t, db, runID)
			require.Len(t, entries, 1, "the finalize logs the run's finish, once")
			assert.True(t, entries[0].FinishedAt.Equal(finish))
			if tc.errorClass != "" {
				require.NotNil(t, row.ErrorClass)
				assert.Equal(t, tc.errorClass, *row.ErrorClass)
			}
		})
	}

	t.Run("superseded by a cancel during the attempt", func(t *testing.T) {
		t.Parallel()
		db, d := open(t)
		raw, runID := claimForWritePath(t, db, d, 5)
		require.NoError(t, CancelJob(context.Background(), db, raw.ID))

		out, err := d.Finalize(context.Background(), raw, runID, Result{}, nil, finish)
		require.NoError(t, err)
		require.True(t, out.Superseded)

		row := loadRun(t, db, runID)
		assert.True(t, row.Superseded, "the lost claim is recorded durably, not only as an event")
		assert.Nil(t, row.JobState, "a superseded finalize applied no state, so it records none")
		assert.Equal(t, string(OutcomeSuccess), row.Outcome, "the attempt's real outcome is kept")
		require.NotNil(t, row.DurationMs)
		assert.Equal(t, 2000, *row.DurationMs)
		assert.Equal(t, string(StateCancelled), jobState(t, db, raw.ID))
		assert.Len(t, finishEntries(t, db, runID), 1, "a superseded finish is still logged, as superseded")
	})

	t.Run("crashed by the sweep then finalized late", func(t *testing.T) {
		t.Parallel()
		db, d := open(t)
		raw, runID := claimForWritePath(t, db, d, 5)

		// The lease (10s from the claim at T0+3s) is long gone at T0+60s.
		sweptAt := writePathT0.Add(time.Minute)
		n, err := d.Sweep(context.Background(), sweptAt)
		require.NoError(t, err)
		require.Equal(t, 1, n)

		crashed := loadRun(t, db, runID)
		assert.Equal(t, string(OutcomeCrashed), crashed.Outcome)
		require.NotNil(t, crashed.FinishedAt)
		assert.True(t, crashed.FinishedAt.Equal(sweptAt))
		require.NotNil(t, crashed.JobState)
		assert.Equal(t, string(StateAvailable), *crashed.JobState, "the sweep records the reclaim")
		assert.Nil(t, crashed.DurationMs, "a crash reports no finish of its own")
		require.Len(t, finishEntries(t, db, runID), 1, "the sweep logs the crash as the run's finish")

		late := writePathT0.Add(2 * time.Hour)
		out, err := d.Finalize(context.Background(), raw, runID, Result{}, nil, late)
		require.NoError(t, err)
		require.True(t, out.Superseded)

		row := loadRun(t, db, runID)
		require.NotNil(t, row.FinishedAt)
		assert.True(t, row.FinishedAt.Equal(sweptAt),
			"finished_at is write-once: the late finalize must not move the run into a later hour")
		assert.Equal(t, string(OutcomeSuccess), row.Outcome, "the attempt's real outcome is still recorded")
		require.NotNil(t, row.DurationMs)
		assert.Equal(t, int((2*time.Hour - 3*time.Second).Milliseconds()), *row.DurationMs,
			"and its real duration")
		assert.True(t, row.Superseded)
		require.NotNil(t, row.JobState)
		assert.Equal(t, string(StateAvailable), *row.JobState, "the sweep's record of the reclaim survives")
		entries := finishEntries(t, db, runID)
		require.Len(t, entries, 1, "the late finalize does not log a second finish")
		assert.True(t, entries[0].FinishedAt.Equal(sweptAt), "the one entry is the sweep's, matching the row")
	})

	t.Run("retention removes the finish entries with their runs", func(t *testing.T) {
		t.Parallel()
		db, d := open(t)
		raw, runID := claimForWritePath(t, db, d, 5)
		_, err := d.Finalize(context.Background(), raw, runID, Result{}, nil, finish)
		require.NoError(t, err)
		require.Len(t, finishEntries(t, db, runID), 1)

		n, err := DeleteFinishedJobs(context.Background(), db, finish.Add(time.Hour))
		require.NoError(t, err)
		require.EqualValues(t, 1, n)
		assert.Empty(t, finishEntries(t, db, runID), "the log never outlives its run")
	})

	t.Run("seeded run copies its job's kind and queue", func(t *testing.T) {
		t.Parallel()
		db, _ := open(t)
		ctx := context.Background()
		id, err := Enqueue(ctx, NewClient(db), "stats.seeded", []byte(`{}`), InsertOpts{Queue: "sq"})
		require.NoError(t, err)
		finished := writePathT0.Add(time.Minute)
		runID, err := SeedRun(ctx, db, RunSeed{
			JobID: id, Attempt: 1, ExecutorID: "importer", Outcome: OutcomeSuccess,
			StartedAt: writePathT0, FinishedAt: &finished,
		})
		require.NoError(t, err)

		row := loadRun(t, db, runID)
		assert.Equal(t, "stats.seeded", row.Kind)
		assert.Equal(t, "sq", row.Queue)
		assert.Len(t, finishEntries(t, db, runID), 1, "a finished seed is logged like a finalize")
		assert.Nil(t, row.QueueWaitMs, "a seed cannot know the wait")
		assert.Nil(t, row.JobState, "nor the state the attempt applied")

		orphan, err := SeedRun(ctx, db, RunSeed{JobID: "no-such-job", Attempt: 1, ExecutorID: "importer"})
		require.NoError(t, err)
		assert.Empty(t, loadRun(t, db, orphan).Kind, "a missing job leaves the kind for the read path to resolve")
		assert.Empty(t, finishEntries(t, db, orphan), "an unfinished seed has no finish to log")
	})
}

// TestRunRowsStampAnalyticsColumns is the SQLite half of the write-path suite.
func TestRunRowsStampAnalyticsColumns(t *testing.T) {
	t.Parallel()
	writePathSuite(t, sqliteWritePath)
}

// TestRunTimestampsAreStampedInUTCSQLite proves the stored text itself is UTC,
// whatever zone the clock that produced it was in. A Go-side Equal would pass
// for any zone; only the stored text decides whether SQLite's textual range
// comparison is a time comparison.
func TestRunTimestampsAreStampedInUTCSQLite(t *testing.T) {
	t.Parallel()
	db, d := sqliteWritePath(t)
	raw, runID := claimForWritePath(t, db, d, 5)
	_, err := d.Finalize(context.Background(), raw, runID, Result{}, nil, writePathT0.Add(5*time.Second))
	require.NoError(t, err)

	var stored struct {
		StartedAt  string
		FinishedAt string
		CreatedAt  string
	}
	require.NoError(t, db.Raw(`SELECT CAST(started_at AS TEXT) AS started_at, CAST(finished_at AS TEXT) AS finished_at,
		CAST(created_at AS TEXT) AS created_at FROM job_runs WHERE id = ?`, runID).Scan(&stored).Error)
	for name, v := range map[string]string{
		"started_at": stored.StartedAt, "finished_at": stored.FinishedAt, "created_at": stored.CreatedAt,
	} {
		assert.Regexp(t, `\+00:00$`, v, "%s is stored in UTC", name)
	}
	assert.Equal(t, "2026-03-08 11:30:05+00:00", stored.FinishedAt, "T0+5s, in UTC")
}

// TestQueueWaitIsClampedAndOptional covers the wait computation's two edges: a
// start before the schedule (clock skew between nodes) records zero rather than
// a negative wait, and a RawJob with no schedule — a custom Driver's — records
// none.
func TestQueueWaitIsClampedAndOptional(t *testing.T) {
	t.Parallel()
	now := time.Now()

	skewed := queueWaitMs(now.Add(time.Second), now)
	require.NotNil(t, skewed)
	assert.Zero(t, *skewed)
	assert.Nil(t, queueWaitMs(time.Time{}, now))
	w := queueWaitMs(now.Add(-1500*time.Millisecond), now)
	require.NotNil(t, w)
	assert.Equal(t, 1500, *w)
}

// statsWriteArgs and statsWriteWorker drive the runtime-path test below: the
// worker fails when Fail is set, and the job is enqueued with one attempt so the
// failure discards it.
type statsWriteArgs struct{ Fail bool }

func (statsWriteArgs) Kind() string { return "stats.runner" }

type statsWriteWorker struct{}

func (statsWriteWorker) Kind() string { return "stats.runner" }

func (statsWriteWorker) Work(_ context.Context, job *Job[statsWriteArgs]) (Result, error) {
	if job.Args.Fail {
		return Result{}, errors.New("nope")
	}
	return Result{CostMicros: 7}, nil
}

// TestRunnerPathStampsAnalyticsColumns proves the columns are stamped through
// the Runner itself, not only when the driver is called by hand: the kind and
// queue come off the claimed RawJob and the wait off its ScheduledAt.
func TestRunnerPathStampsAnalyticsColumns(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	ctx := context.Background()
	reg := NewRegistry()
	Register[statsWriteArgs](reg, statsWriteWorker{})
	okID, err := Insert(ctx, NewClient(db), statsWriteArgs{}, InsertOpts{})
	require.NoError(t, err)
	failID, err := Insert(ctx, NewClient(db), statsWriteArgs{Fail: true}, InsertOpts{MaxAttempts: 1})
	require.NoError(t, err)

	runToIdle(t, ctx, rwRunner(t, db, reg))

	for id, want := range map[string]JobState{okID: StateSucceeded, failID: StateDiscarded} {
		var row jobRunRow
		require.NoError(t, db.Where("job_id = ?", id).First(&row).Error)
		assert.Equal(t, "stats.runner", row.Kind)
		assert.Equal(t, defaultQueue, row.Queue)
		assert.NotNil(t, row.QueueWaitMs)
		require.NotNil(t, row.JobState)
		assert.Equal(t, string(want), *row.JobState)
	}
}

// TestRetentionDeletesFinishEntriesInChunks proves the finish-log delete is
// bounded by runs, not jobs: one job with more runs than a chunk holds — a job
// snoozed or retried many times — is pruned with every entry gone.
func TestRetentionDeletesFinishEntriesInChunks(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	at := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	seedJob(t, db, jobRow{ID: "many-runs", Kind: "k", State: string(StateSucceeded), FinalizedAt: &at, ScheduledAt: at})
	runs := make([]jobRunRow, finishDeleteChunk+250)
	for i := range runs {
		fin := at.Add(time.Duration(i) * time.Second)
		runs[i] = jobRunRow{
			ID: fmt.Sprintf("many-runs-%05d", i), JobID: "many-runs", Attempt: i + 1, Kind: "k", Queue: "q",
			ExecutorID: "x", Outcome: string(OutcomeSnooze), StartedAt: fin, FinishedAt: &fin,
		}
	}
	require.NoError(t, db.CreateInBatches(runs, 200).Error)
	require.NoError(t, db.Exec(fmt.Sprintf(finishLogInsert, "job_runs", "")).Error)

	n, err := DeleteFinishedJobs(context.Background(), db, at.Add(time.Hour))
	require.NoError(t, err)
	require.EqualValues(t, 1, n)
	var left int64
	require.NoError(t, db.Model(&jobRunFinishRow{}).Count(&left).Error)
	assert.Zero(t, left, "every entry is gone, across more than one chunk")
}
