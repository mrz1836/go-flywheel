//go:build integration

package core

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

// newSeedPostgresDB returns an isolated Postgres schema with the whole index set
// applied.
//
// NewPostgresIsolatedDB applies 2 of the 8 indexes and omits
// job_runs_job_attempt, so a collision test written against it would insert both
// rows happily and pass while proving nothing. Topping it up with InstallIndexes
// is also the host-owned install this plan documents — tables from the loader,
// indexes from the runtime — so the test runs on the shape a real host has.
func newSeedPostgresDB(t *testing.T) *gorm.DB {
	t.Helper()
	db := NewPostgresIsolatedDB(t)
	require.NoError(t, InstallIndexes(context.Background(), db))
	return db
}

// TestSeedRunIsAStableForeignKeyTargetPostgres is A4: a host provenance row with
// a real foreign key onto job_runs.id, inserted in a *separate* transaction from
// the seed, resolves. That is the guarantee the correlation seam rests on — the
// run row is committed before SeedRun returns, so a host does not need to
// interleave its writes with the runtime's.
func TestSeedRunIsAStableForeignKeyTargetPostgres(t *testing.T) {
	ctx := context.Background()
	db := newSeedPostgresDB(t)

	// The host's own provenance table, shaped the way a real one is: its own id,
	// its own payload, and a nullable FK back to the attempt that produced it.
	// ON DELETE SET NULL is what keeps it valid across flywheel's retention pass,
	// which deletes job_runs before jobs.
	require.NoError(t, db.Exec(`CREATE TABLE host_source_fetches (
		id text PRIMARY KEY,
		provider text NOT NULL,
		job_run_id text REFERENCES job_runs(id) ON DELETE SET NULL
	)`).Error)

	jobID, err := Enqueue(ctx, NewClient(db), "test.kind", []byte(`{}`), InsertOpts{})
	require.NoError(t, err)

	runID, err := SeedRun(ctx, db, RunSeed{
		JobID:         jobID,
		Attempt:       1,
		ExecutorClass: "worker",
		ExecutorID:    "exec-1",
		Outcome:       OutcomeSuccess,
		Output:        map[string]any{"fetched": 2},
	})
	require.NoError(t, err)

	// A separate transaction, opened after SeedRun returned: the FK must resolve
	// with no coordination between the two writes.
	require.NoError(t, db.Transaction(func(tx *gorm.DB) error {
		return tx.Exec(
			`INSERT INTO host_source_fetches (id, provider, job_run_id) VALUES (?, ?, ?)`,
			"fetch-1", "acme", runID,
		).Error
	}), "a host row referencing a committed seeded run must insert cleanly")

	// The join is what the seam is for: from the host's row back to the attempt.
	var joined struct {
		Provider string
		Outcome  string
	}
	require.NoError(t, db.Raw(`
		SELECT f.provider, r.outcome
		FROM host_source_fetches f JOIN job_runs r ON r.id = f.job_run_id
		WHERE f.id = ?`, "fetch-1").Scan(&joined).Error)
	assert.Equal(t, "acme", joined.Provider)
	assert.Equal(t, string(OutcomeSuccess), joined.Outcome)

	// And the database really is enforcing it — a dangling reference is rejected,
	// so the passing half above is not a table without a constraint.
	err = db.Exec(
		`INSERT INTO host_source_fetches (id, provider, job_run_id) VALUES (?, ?, ?)`,
		"fetch-2", "acme", "no-such-run",
	).Error
	assert.Error(t, err, "the FK must be real: an unknown job_run_id is rejected")
}

// TestSeedRunRejectsAttemptCollisionPostgres proves the (job_id, attempt)
// collision maps onto ErrRunAlreadyRecorded on Postgres too, through the same
// models.WrapDBError seam Enqueue uses for ErrAlreadyEnqueued.
func TestSeedRunRejectsAttemptCollisionPostgres(t *testing.T) {
	ctx := context.Background()
	db := newSeedPostgresDB(t)

	jobID, err := Enqueue(ctx, NewClient(db), "test.kind", []byte(`{}`), InsertOpts{})
	require.NoError(t, err)

	_, err = SeedRun(ctx, db, RunSeed{JobID: jobID, Attempt: 1, ExecutorID: "exec-1"})
	require.NoError(t, err)

	_, err = SeedRun(ctx, db, RunSeed{JobID: jobID, Attempt: 1, ExecutorID: "exec-2"})
	assert.ErrorIs(t, err, ErrRunAlreadyRecorded)

	_, err = SeedRun(ctx, db, RunSeed{JobID: jobID, Attempt: 2, ExecutorID: "exec-1"})
	assert.NoError(t, err, "a distinct attempt is a new record, not a duplicate")
}

// TestSeedRunHonorsTxPostgres proves the caller's transaction owns the write on
// Postgres: a rollback leaves no row.
func TestSeedRunHonorsTxPostgres(t *testing.T) {
	ctx := context.Background()
	db := newSeedPostgresDB(t)

	jobID, err := Enqueue(ctx, NewClient(db), "test.kind", []byte(`{}`), InsertOpts{})
	require.NoError(t, err)

	var runID string
	require.Error(t, db.Transaction(func(tx *gorm.DB) error {
		var seedErr error
		runID, seedErr = SeedRun(ctx, db, RunSeed{JobID: jobID, Attempt: 1, ExecutorID: "exec-1", Tx: tx})
		require.NoError(t, seedErr)
		return assert.AnError
	}))

	var count int64
	require.NoError(t, db.Model(&jobRunRow{}).Where("id = ?", runID).Count(&count).Error)
	assert.Zero(t, count, "a rolled-back transaction must leave no run row behind")
}
