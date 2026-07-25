package flywheel

import (
	"context"
	"errors"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/mrz1836/go-foundation/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

// newSeedDB builds a SQLite database with the *whole* schema, indexes included.
//
// The shared newDB helper applies 2 of the 8 indexes and omits
// job_runs_job_attempt, so a collision test written against it would insert both
// rows happily and pass while proving nothing. Migrate on a bare database is the
// library-owned install, which is exactly the topped-up schema these tests need.
func newSeedDB(t *testing.T) *gorm.DB {
	t.Helper()
	db := newBareSQLite(t)
	require.NoError(t, Migrate(db))
	return db
}

// enqueueSeedJob enqueues one job and returns its id, so a seeded run has a real job to
// belong to.
func enqueueSeedJob(t *testing.T, ctx context.Context, db *gorm.DB) string { //nolint:revive // ctx-after-t matches the test helper convention
	t.Helper()
	id, err := Enqueue(ctx, NewClient(db), "test.kind", []byte(`{}`), InsertOpts{})
	require.NoError(t, err)
	return id
}

// readRun loads a job_runs row by id.
func readRun(t *testing.T, db *gorm.DB, runID string) jobRunRow {
	t.Helper()
	var row jobRunRow
	require.NoError(t, db.Where("id = ?", runID).First(&row).Error)
	return row
}

// TestSeedRunMatchesARuntimeWrittenRow is the shape-parity check: a row SeedRun
// writes must be indistinguishable in shape from one the runtime's own finalize
// path wrote for the same attempt. Both go through the same marshaling and the
// same error truncation, so a host that seeds history gets rows its production
// readers already handle.
//
// error_class is deliberately not compared: it is the state machine's verdict on
// a real failure, not a property of the attempt a host is recording.
func TestSeedRunMatchesARuntimeWrittenRow(t *testing.T) {
	t.Parallel()

	startedAt := time.Date(2026, 7, 25, 12, 0, 0, 0, time.UTC)
	finishedAt := startedAt.Add(1500 * time.Millisecond)
	output := map[string]any{"fetched": 3, "provider": "acme"}
	const failure = "upstream refused the connection"

	ctx := context.Background()
	db := newSeedDB(t)
	driver := NewSQLiteDriver(db)

	// The runtime's own path: a stub committed before the worker body, then a
	// finalize carrying the worker's output and error.
	runtimeJob := enqueueSeedJob(t, ctx, db)
	runtimeRunID := models.NewID()
	raw := RawJob{ID: runtimeJob, Kind: "test.kind", Queue: "default", Attempt: 1, MaxAttempts: 25}
	require.NoError(t, driver.InsertRunStub(ctx, runtimeRunID, raw, startedAt, "worker", "exec-1"))
	require.NoError(t, driver.Finalize(ctx, raw, runtimeRunID, Result{Output: output}, errors.New(failure), finishedAt))
	want := readRun(t, db, runtimeRunID)

	// The host's path: one call, describing the same attempt.
	seededRunID, err := SeedRun(ctx, db, RunSeed{
		JobID:         enqueueSeedJob(t, ctx, db),
		Attempt:       1,
		ExecutorClass: "worker",
		ExecutorID:    "exec-1",
		Outcome:       RunOutcome(want.Outcome),
		StartedAt:     startedAt,
		FinishedAt:    &finishedAt,
		Output:        output,
		ErrorMessage:  failure,
	})
	require.NoError(t, err)
	got := readRun(t, db, seededRunID)

	assert.Equal(t, want.ExecutorClass, got.ExecutorClass)
	assert.Equal(t, want.ExecutorID, got.ExecutorID)
	assert.Equal(t, want.Outcome, got.Outcome)
	assert.Equal(t, want.StartedAt.UTC(), got.StartedAt.UTC())
	require.NotNil(t, got.FinishedAt)
	assert.Equal(t, want.FinishedAt.UTC(), got.FinishedAt.UTC())
	require.NotNil(t, got.DurationMs)
	assert.Equal(t, *want.DurationMs, *got.DurationMs, "duration_ms is derived the same way from the same pair of stamps")
	assert.JSONEq(t, string(want.Output), string(got.Output), "output lands in the column ListRuns reads back")
	require.NotNil(t, got.ErrorMessage)
	assert.Equal(t, *want.ErrorMessage, *got.ErrorMessage)
	assert.JSONEq(t, string(want.ErrorPayload), string(got.ErrorPayload),
		"the error payload shares the runtime's shape, not a second copy of the logic")

	// The seeded id is readable through the runtime's own audit reader.
	runs, err := ListRuns(ctx, db, got.JobID, ListRunsParams{})
	require.NoError(t, err)
	require.Len(t, runs, 1)
	assert.Equal(t, seededRunID, runs[0].ID)
	assert.JSONEq(t, string(got.Output), string(runs[0].Output))
}

// TestSeedRunAppliesDefaults proves the zero-value seed is the stub the runtime
// writes before a worker body runs: outcome started, started_at from the context
// clock, no finish stamp and so no duration.
func TestSeedRunAppliesDefaults(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 25, 9, 30, 0, 0, time.UTC)
	ctx := models.WithClock(context.Background(), models.NewFixedClock(now))
	db := newSeedDB(t)

	runID, err := SeedRun(ctx, db, RunSeed{JobID: enqueueSeedJob(t, ctx, db), Attempt: 1, ExecutorID: "exec-1"})
	require.NoError(t, err)

	row := readRun(t, db, runID)
	assert.Equal(t, string(OutcomeStarted), row.Outcome, "the default outcome matches the runtime's pre-work stub")
	assert.Equal(t, now.UTC(), row.StartedAt.UTC(), "started_at defaults to the context clock")
	assert.Equal(t, now.UTC(), row.CreatedAt.UTC())
	assert.Nil(t, row.FinishedAt)
	assert.Nil(t, row.DurationMs, "an unfinished attempt records no duration")
	assert.Nil(t, row.ErrorMessage)
	assert.Empty(t, row.Output)
}

// TestSeedRunRejectsAttemptCollision proves the (job_id, attempt) collision maps
// onto a sentinel rather than a raw driver error — the ErrAlreadyEnqueued
// pattern, applied to the audit table.
func TestSeedRunRejectsAttemptCollision(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db := newSeedDB(t)
	jobID := enqueueSeedJob(t, ctx, db)

	_, err := SeedRun(ctx, db, RunSeed{JobID: jobID, Attempt: 2, ExecutorID: "exec-1"})
	require.NoError(t, err)

	_, err = SeedRun(ctx, db, RunSeed{JobID: jobID, Attempt: 2, ExecutorID: "exec-1"})
	assert.ErrorIs(t, err, ErrRunAlreadyRecorded, "a second row for the same attempt is a duplicate, not a new record")

	// A different attempt on the same job is not a collision.
	_, err = SeedRun(ctx, db, RunSeed{JobID: jobID, Attempt: 3, ExecutorID: "exec-1"})
	assert.NoError(t, err)

	// Nor is the same attempt on a different job.
	_, err = SeedRun(ctx, db, RunSeed{JobID: enqueueSeedJob(t, ctx, db), Attempt: 2, ExecutorID: "exec-1"})
	assert.NoError(t, err)
}

// TestSeedRunHonorsTx proves Tx puts the write on the caller's transaction: a
// rollback leaves no row, so a host can seed a run and its own provenance row
// atomically.
func TestSeedRunHonorsTx(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db := newSeedDB(t)
	jobID := enqueueSeedJob(t, ctx, db)

	var rolledBackID string
	err := db.Transaction(func(tx *gorm.DB) error {
		var seedErr error
		rolledBackID, seedErr = SeedRun(ctx, db, RunSeed{JobID: jobID, Attempt: 1, ExecutorID: "exec-1", Tx: tx})
		require.NoError(t, seedErr)
		return errors.New("host decided to roll back")
	})
	require.Error(t, err)

	var count int64
	require.NoError(t, db.Model(&jobRunRow{}).Where("id = ?", rolledBackID).Count(&count).Error)
	assert.Zero(t, count, "a rolled-back transaction must leave no run row behind")

	// The same seed on a committed transaction lands.
	var committedID string
	require.NoError(t, db.Transaction(func(tx *gorm.DB) error {
		var seedErr error
		committedID, seedErr = SeedRun(ctx, db, RunSeed{JobID: jobID, Attempt: 1, ExecutorID: "exec-1", Tx: tx})
		return seedErr
	}))
	assert.Equal(t, committedID, readRun(t, db, committedID).ID)
}

// TestSeedRunRequiresIdentityFields proves the mandatory fields are validated
// through the runtime's own validation seam, up front and by name, rather than
// surfacing as a wrapped driver error from the row's lifecycle hook.
func TestSeedRunRequiresIdentityFields(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		seed  RunSeed
		field string
	}{
		"no job":      {seed: RunSeed{Attempt: 1, ExecutorID: "exec-1"}, field: "job_id"},
		"no executor": {seed: RunSeed{JobID: "j1", Attempt: 1}, field: "executor_id"},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			_, err := SeedRun(context.Background(), newSeedDB(t), tc.seed)
			require.ErrorIs(t, err, ErrValidation)

			var verr *ValidationError
			require.ErrorAs(t, err, &verr)
			assert.Equal(t, tc.field, verr.Field)
		})
	}
}

// TestSeedRunNilDB guards the nil-db precondition.
func TestSeedRunNilDB(t *testing.T) {
	t.Parallel()
	_, err := SeedRun(context.Background(), nil, RunSeed{JobID: "j1", ExecutorID: "exec-1"})
	require.Error(t, err)
}

// TestSeedRunTruncatesLongErrors proves a seeded error message goes through the
// same rune-safe cap as a real worker error, so an oversized import cannot write
// a row the runtime would never have produced.
func TestSeedRunTruncatesLongErrors(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db := newSeedDB(t)

	long := ""
	for len(long) <= maxErrorMessage {
		long += "boundary-é-"
	}

	runID, err := SeedRun(ctx, db, RunSeed{
		JobID: enqueueSeedJob(t, ctx, db), Attempt: 1, ExecutorID: "exec-1", ErrorMessage: long,
	})
	require.NoError(t, err)

	row := readRun(t, db, runID)
	require.NotNil(t, row.ErrorMessage)
	assert.LessOrEqual(t, len(*row.ErrorMessage), maxErrorMessage)
	assert.True(t, utf8.ValidString(*row.ErrorMessage), "the cut backs off to a rune boundary")
}
