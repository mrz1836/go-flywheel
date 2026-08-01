//go:build integration

package core

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

// TestDeleteFinishedJobsPrunesADeepBacklogPostgres is the depth half of the
// retention acceptance.
//
// Before this change the pass built one DELETE binding a parameter per row, and
// PostgreSQL's extended protocol rejects a statement carrying more than 65,535
// of them: measured at HEAD, retention failed outright at 65,536 rows. A first
// prune against a long-lived database is exactly where that ceiling is met, so
// the batching is what makes retention usable rather than merely faster.
func TestDeleteFinishedJobsPrunesADeepBacklogPostgres(t *testing.T) {
	t.Parallel()
	db := NewPostgresIsolatedDB(t)

	const backlog = 200_000
	seedTerminalJobsBulkPG(t, db, backlog, time.Now().UTC().Add(-90*24*time.Hour))

	deleted, err := DeleteFinishedJobsWithOptions(
		context.Background(), db, time.Now(), RetentionOpts{BatchSize: 1000},
	)
	require.NoError(t, err, "a backlog past the 65,535-parameter limit must still prune")
	assert.EqualValues(t, backlog, deleted)

	var jobs, runs int64
	require.NoError(t, db.Model(&jobRow{}).Count(&jobs).Error)
	require.NoError(t, db.Model(&jobRunRow{}).Count(&runs).Error)
	assert.EqualValues(t, 0, jobs)
	assert.EqualValues(t, 0, runs, "audit rows go with their jobs")
}

// TestDeleteFinishedJobsHonorsAHostCascadePostgres is the one place the
// runs-before-jobs order is observable in data rather than in the statement
// stream.
//
// A host may declare the foreign key the library does not. Under
// ON DELETE CASCADE, deleting a jobs row removes its job_runs rows
// automatically — so the order decides which statement does the work. Deleting
// runs first, as the contract requires, makes the runs delete report the real
// count; reversing it would leave the runs delete matching nothing and let the
// cascade do the work silently, with the reported counts unchanged. That is
// precisely the kind of change that looks harmless in a diff.
func TestDeleteFinishedJobsHonorsAHostCascadePostgres(t *testing.T) {
	t.Parallel()
	db := NewPostgresIsolatedDB(t)

	// The FK a host declares, which the library never does.
	require.NoError(t, db.Exec(`
		ALTER TABLE job_runs
		ADD CONSTRAINT fk_job_runs_job FOREIGN KEY (job_id) REFERENCES jobs(id) ON DELETE CASCADE
	`).Error)

	seedTerminalJobsBulkPG(t, db, 5_000, time.Now().UTC().Add(-90*24*time.Hour))

	deleted, err := DeleteFinishedJobsWithOptions(
		context.Background(), db, time.Now(), RetentionOpts{BatchSize: 500},
	)
	require.NoError(t, err, "the delete order must not violate the host's constraint")
	assert.EqualValues(t, 5_000, deleted)

	var jobs, runs int64
	require.NoError(t, db.Model(&jobRow{}).Count(&jobs).Error)
	require.NoError(t, db.Model(&jobRunRow{}).Count(&runs).Error)
	assert.EqualValues(t, 0, jobs)
	assert.EqualValues(t, 0, runs)
}

// TestDeleteFinishedJobsKeysetCoversEveryRowPostgres guards the cursor against
// the failure mode keyset pagination actually has: a batch boundary that skips
// or repeats a row.
//
// A backlog that is not a whole multiple of the batch size, pruned to
// exhaustion, is where an off-by-one in the cursor shows up — either as
// survivors or as a pass that never terminates.
func TestDeleteFinishedJobsKeysetCoversEveryRowPostgres(t *testing.T) {
	t.Parallel()
	db := NewPostgresIsolatedDB(t)

	const backlog = 4_097 // deliberately not a multiple of the batch size
	seedTerminalJobsBulkPG(t, db, backlog, time.Now().UTC().Add(-90*24*time.Hour))

	deleted, err := DeleteFinishedJobsWithOptions(
		context.Background(), db, time.Now(), RetentionOpts{BatchSize: 512},
	)
	require.NoError(t, err)
	assert.EqualValues(t, backlog, deleted, "no row is skipped and none is counted twice")

	var jobs int64
	require.NoError(t, db.Model(&jobRow{}).Count(&jobs).Error)
	assert.EqualValues(t, 0, jobs)
}

// TestDeleteFinishedJobsPurgesSoftDeletedTerminalJobsPostgres pins the Unscoped
// half of the contract: retention reclaims storage, so a soft-deleted terminal
// job is purged rather than left behind to orphan its audit rows.
func TestDeleteFinishedJobsPurgesSoftDeletedTerminalJobsPostgres(t *testing.T) {
	t.Parallel()
	db := NewPostgresIsolatedDB(t)
	seedTerminalJobsBulkPG(t, db, 10, time.Now().UTC().Add(-90*24*time.Hour))
	require.NoError(t, db.Where("id = ?", "bulk-terminal-0").Delete(&jobRow{}).Error)

	deleted, err := DeleteFinishedJobsWithOptions(
		context.Background(), db, time.Now(), RetentionOpts{BatchSize: 4},
	)
	require.NoError(t, err)
	assert.EqualValues(t, 10, deleted, "a soft-deleted terminal job is purged, not skipped")

	var jobs int64
	require.NoError(t, db.Unscoped().Model(&jobRow{}).Count(&jobs).Error)
	assert.EqualValues(t, 0, jobs)
}

// TestDeleteFinishedJobsOnAUUIDKeyedSchemaPostgres is the regression gate for a
// defect the library's own tests structurally could not catch.
//
// Migrate creates jobs.id as text, because it comes from a Go string field. A
// host whose migration tool owns the schema is free to declare it uuid, and one
// does. The keyset cursor used to be seeded with an empty string — a valid text
// value that every id sorts after, and an invalid uuid literal — so the first
// batch failed outright with `invalid input syntax for type uuid: ""` on
// precisely the schemas the library never builds for itself.
//
// It was found by running the runtime against a consumer's freshly migrated
// database rather than by review, which is the argument for doing that at all.
func TestDeleteFinishedJobsOnAUUIDKeyedSchemaPostgres(t *testing.T) {
	t.Parallel()
	db := NewPostgresIsolatedDB(t)

	// Rebuild the job tables with uuid keys, the way a host's migration would.
	require.NoError(t, db.Exec(`DROP TABLE IF EXISTS job_runs, jobs CASCADE`).Error)
	require.NoError(t, db.Exec(`
		CREATE TABLE jobs (
			id uuid PRIMARY KEY,
			created_at timestamptz NOT NULL, updated_at timestamptz NOT NULL,
			deleted_at timestamptz,
			metadata jsonb NOT NULL DEFAULT '{}', kind text NOT NULL,
			queue text NOT NULL DEFAULT 'default', args jsonb NOT NULL,
			priority int NOT NULL DEFAULT 100, state text NOT NULL DEFAULT 'available',
			attempt int NOT NULL DEFAULT 0, max_attempts int NOT NULL DEFAULT 25,
			timeout_ms int, scheduled_at timestamptz NOT NULL,
			leased_until timestamptz, lease_token text,
			unique_key text, unique_active_key text, parent_job_id uuid,
			executor_class text NOT NULL DEFAULT '', finalized_at timestamptz,
			tags jsonb NOT NULL DEFAULT '[]'
		)`).Error)
	require.NoError(t, db.Exec(`
		CREATE TABLE job_runs (
			id uuid PRIMARY KEY, job_id uuid NOT NULL, attempt int NOT NULL,
			executor_class text NOT NULL DEFAULT '', executor_id text NOT NULL,
			started_at timestamptz NOT NULL, finished_at timestamptz,
			outcome text NOT NULL, error_class text, error_message text,
			error_payload jsonb, output jsonb, duration_ms int, cost_micros bigint,
			enqueued_children int NOT NULL DEFAULT 0, created_at timestamptz NOT NULL
		)`).Error)

	finalized := time.Now().UTC().Add(-90 * 24 * time.Hour)
	const backlog = 2_500
	require.NoError(t, db.Exec(`
		INSERT INTO jobs (id, created_at, updated_at, metadata, kind, queue, args, priority,
		                  state, attempt, max_attempts, scheduled_at, finalized_at,
		                  executor_class, tags)
		SELECT gen_random_uuid(), ?, ?, '{}'::jsonb, 'retention.uuid', 'default', '{}'::jsonb, 100,
		       'succeeded', 1, 25, ?, ?, '', '[]'::jsonb
		FROM generate_series(1, ?)`,
		finalized, finalized, finalized, finalized, backlog).Error)
	require.NoError(t, db.Exec(`
		INSERT INTO job_runs (id, job_id, attempt, executor_class, executor_id, started_at,
		                      finished_at, outcome, enqueued_children, created_at)
		SELECT gen_random_uuid(), j.id, 1, '', 'exec-1', ?, ?, ?, 0, ?
		FROM jobs j`,
		finalized, finalized, string(OutcomeSuccess), finalized).Error)

	// Several batches, so the cursor is exercised rather than just the first
	// batch's absent predicate.
	deleted, err := DeleteFinishedJobsWithOptions(
		context.Background(), db, time.Now(), RetentionOpts{BatchSize: 250},
	)
	require.NoError(t, err, "a uuid-keyed jobs table must prune like any other")
	assert.EqualValues(t, backlog, deleted)

	var jobs, runs int64
	require.NoError(t, db.Table("jobs").Count(&jobs).Error)
	require.NoError(t, db.Table("job_runs").Count(&runs).Error)
	assert.EqualValues(t, 0, jobs)
	assert.EqualValues(t, 0, runs, "audit rows go with their jobs on a uuid schema too")
}

// seedTerminalJobsBulkPG writes n succeeded jobs finalized at finalizedAt, plus
// their finished run rows, in one server-side statement per table.
func seedTerminalJobsBulkPG(t *testing.T, db *gorm.DB, n int, finalizedAt time.Time) {
	t.Helper()

	require.NoError(t, db.Exec(`
		INSERT INTO jobs (id, created_at, updated_at, metadata, kind, queue, args, priority,
		                  state, attempt, max_attempts, scheduled_at, finalized_at,
		                  executor_class, tags)
		SELECT 'bulk-terminal-' || g, ?, ?, '{}'::jsonb, 'retention.batch', 'default', '{}'::jsonb, 100,
		       'succeeded', 1, 25, ?, ?, '', '[]'::jsonb
		FROM generate_series(0, ?) AS g`,
		finalizedAt, finalizedAt, finalizedAt, finalizedAt, n-1).Error)

	require.NoError(t, db.Exec(`
		INSERT INTO job_runs (id, job_id, attempt, executor_class, executor_id, started_at,
		                      finished_at, outcome, enqueued_children, created_at)
		SELECT 'bulk-trun-' || g, 'bulk-terminal-' || g, 1, 'local', 'exec-1', ?, ?, ?, 0, ?
		FROM generate_series(0, ?) AS g`,
		finalizedAt, finalizedAt, string(OutcomeSuccess), finalizedAt, n-1).Error)
}
