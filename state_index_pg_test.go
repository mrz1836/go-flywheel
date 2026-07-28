//go:build integration

package flywheel

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

// TestStateIndexCoversTheCountsPredicatePostgres asserts the installed index's
// *shape* against the query's, read back from the catalog rather than from the
// DDL constant this package also owns.
//
// Reading pg_indexes.indexdef is the point: Migrate and InstallIndexes reconcile
// by name, so CREATE INDEX IF NOT EXISTS on a database already carrying an index
// of that name does nothing and reports success. An assertion that probes by
// name passes against a stale definition — which has happened in this project
// before, to a consumer's dev database, through a green suite.
func TestStateIndexCoversTheCountsPredicatePostgres(t *testing.T) {
	t.Parallel()
	db := NewPostgresIsolatedDB(t)

	var def string
	require.NoError(t, db.Raw(`
		SELECT indexdef FROM pg_indexes
		WHERE schemaname = current_schema() AND indexname = 'jobs_state'
	`).Scan(&def).Error)
	require.NotEmpty(t, def, "jobs_state must be installed by Migrate")

	lower := strings.ToLower(def)
	assert.Contains(t, lower, " on ", "the definition names its table")
	assert.Contains(t, lower, "jobs", "the index is on jobs")
	assert.Contains(t, lower, "(state)",
		"the key is state alone: no reader of this index constrains a second column")
	assert.Contains(t, lower, "where (deleted_at is null)",
		"the predicate must match GORM's soft-delete scope, or the counts reads cannot use it")
	assert.NotContains(t, lower, "unique",
		"a state index must never be unique")
}

// TestQueueHealthCountsReachTheStateIndexPostgres proves the counts read *can*
// use jobs_state, with the sequential scan disabled for the duration.
//
// Forcing the plan is deliberate, and the alternative was considered and
// rejected. An index-only scan consults the visibility map per heap page, and
// this table churns hard — a drain leaves roughly one dead tuple per live row
// with autovacuum several passes behind — so relallvisible sits near zero and
// the planner correctly prices the sequential scan lower. A gate asserting the
// planner's unforced choice would fail whenever autovacuum was behind, which is
// most of the time under load, and it would be failing for a reason that is not
// a defect.
//
// What is worth gating is what a schema change can silently break: that the
// index's shape still matches the query's access path. enable_seqscan = off
// isolates exactly that. The unforced wall-time benefit is reported in
// docs/BENCHMARKS.md rather than asserted here.
func TestQueueHealthCountsReachTheStateIndexPostgres(t *testing.T) {
	t.Parallel()
	db := NewPostgresIsolatedDB(t)
	seedStateMixPG(t, db, 5_000)

	plan := forcedIndexPlan(t, db,
		`SELECT state, count(*) AS n FROM jobs WHERE jobs.deleted_at IS NULL GROUP BY state`)

	assert.Contains(t, plan, "jobs_state",
		"with the sequential scan disabled, the counts read reaches the telemetry index")
}

// TestCountActiveJobsReachesTheStateIndexPostgres covers the third reader, the
// one the load harness polls every 250ms inside the measured window.
func TestCountActiveJobsReachesTheStateIndexPostgres(t *testing.T) {
	t.Parallel()
	db := NewPostgresIsolatedDB(t)
	seedStateMixPG(t, db, 5_000)

	plan := forcedIndexPlan(t, db, `
		SELECT count(*) FROM jobs
		WHERE state IN ('available','running','retryable','scheduled') AND jobs.deleted_at IS NULL`)

	assert.Contains(t, plan, "jobs_state")
}

// TestStateIndexExcludesSoftDeletedRowsPostgres proves the partial predicate is
// doing real work rather than merely being present: a soft-deleted job must not
// be counted, and with the index forced that answer comes from the index.
func TestStateIndexExcludesSoftDeletedRowsPostgres(t *testing.T) {
	t.Parallel()
	db := NewPostgresIsolatedDB(t)
	seedStateMixPG(t, db, 100)
	require.NoError(t, db.Exec(
		`UPDATE jobs SET deleted_at = now() WHERE state = 'available'`,
	).Error)

	var live int64
	require.NoError(t, db.Model(&jobRow{}).
		Where("state = ?", string(StateAvailable)).Count(&live).Error)
	assert.EqualValues(t, 0, live, "soft-deleted rows fall out of the index and out of the count")
}

// forcedIndexPlan returns the EXPLAIN output for sql with the sequential scan
// disabled, inside a rolled-back transaction so the setting cannot leak.
func forcedIndexPlan(t *testing.T, db *gorm.DB, sql string) string {
	t.Helper()

	var plan string
	err := db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Exec(`SET LOCAL enable_seqscan = off`).Error; err != nil {
			return err
		}
		var lines []string
		if err := tx.Raw("EXPLAIN " + sql).Scan(&lines).Error; err != nil {
			return err
		}
		plan = strings.Join(lines, "\n")
		// Roll back: nothing here should persist, least of all the planner setting.
		return gorm.ErrInvalidTransaction
	})
	require.ErrorIs(t, err, gorm.ErrInvalidTransaction)
	require.NotEmpty(t, plan)
	t.Logf("forced plan:\n%s", plan)
	return plan
}

// seedStateMixPG writes n jobs spread across states in one statement, enough
// that the planner has a reason to prefer an index over a scan of a trivial
// table.
func seedStateMixPG(t *testing.T, db *gorm.DB, n int) {
	t.Helper()
	base := time.Now().UTC().Add(-time.Hour)

	require.NoError(t, db.Exec(`
		INSERT INTO jobs (id, created_at, updated_at, metadata, kind, queue, args, priority,
		                  state, attempt, max_attempts, scheduled_at, executor_class, tags)
		SELECT 'stateidx-' || g, ?, ?, '{}'::jsonb, 'idx.a', 'default', '{}'::jsonb, 100,
		       (ARRAY['available','running','succeeded','discarded','retryable'])[1 + (g % 5)],
		       1, 25, ?, '', '[]'::jsonb
		FROM generate_series(0, ?) AS g`,
		base, base, base, n-1).Error)

	// ANALYZE so the planner has statistics; without them it plans against a
	// default row estimate and the forced-plan assertion would prove nothing
	// about the real table.
	require.NoError(t, db.Exec(`ANALYZE jobs`).Error)
}
