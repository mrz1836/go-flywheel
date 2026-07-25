package flywheel

import (
	"testing"
	"time"

	"github.com/mrz1836/go-foundation/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/datatypes"
)

// TestNewDBInstallsEveryProductionIndex makes the test fixture itself the
// subject. Its oracle is migrateIndexes — the hand-written literal in
// migrate_test.go — not IndexSet, so the assertion is non-tautological by
// construction: a change that dropped an index from the runtime would still be
// caught here, because the expectation is written down independently of the code
// under test.
//
// The fixtures derive from the runtime and the assertions do not. That split is
// the point: newDB and newDBWithIndexKinds build their schemas from IndexSet so
// they cannot drift, while migrateIndexes and correctnessIndexes stay literal so
// something outside the runtime still says what the runtime is supposed to do.
func TestNewDBInstallsEveryProductionIndex(t *testing.T) {
	t.Parallel()

	db := newDB(t)

	for _, table := range migrateTables {
		assert.True(t, db.Migrator().HasTable(table),
			"newDB must create %q: the fixture is the production schema or it is not the production schema", table)
	}
	for _, name := range migrateIndexes {
		assert.True(t, sqliteHasIndex(t, db, name),
			"newDB must install %q — an index-reduced fixture makes a passing test prove something else", name)
	}
}

// TestNewDBWithIndexKindsInstallsOnlyThatKind is the negative half: the
// reduced-schema helper must actually reduce. Without it, a helper that quietly
// installed everything would make the index-condition comparison report a delta
// of zero and read as "the performance indexes are worth nothing."
func TestNewDBWithIndexKindsInstallsOnlyThatKind(t *testing.T) {
	t.Parallel()

	correctness := make(map[string]bool, len(correctnessIndexes))
	for _, name := range correctnessIndexes {
		correctness[name] = true
	}

	t.Run("correctness only", func(t *testing.T) {
		t.Parallel()
		db := newDBWithIndexKinds(t, IndexCorrectness)
		for _, name := range migrateIndexes {
			assert.Equal(t, correctness[name], sqliteHasIndex(t, db, name),
				"index %q: present iff it is correctness-bearing", name)
		}
	})

	t.Run("performance only", func(t *testing.T) {
		t.Parallel()
		db := newDBWithIndexKinds(t, IndexPerformance)
		for _, name := range migrateIndexes {
			assert.Equal(t, !correctness[name], sqliteHasIndex(t, db, name),
				"index %q: present iff it is performance-only", name)
		}
	})

	t.Run("no kinds", func(t *testing.T) {
		t.Parallel()
		db := newDBWithIndexKinds(t)
		for _, name := range migrateIndexes {
			assert.False(t, sqliteHasIndex(t, db, name), "index %q must be absent when no kind is requested", name)
		}
	})
}

// TestNewDBEnforcesOneRunPerAttempt proves job_runs_job_attempt is enforced on
// the default fixture, not merely present. Presence is the weaker claim: an
// index that exists but does not reject is worth nothing to the invariant
// planFinalize's free-snooze reasoning rests on — one audit row per attempt.
//
// The two rows carry different primary keys, so the only constraint that can
// reject the second insert is the unique index on (job_id, attempt).
func TestNewDBEnforcesOneRunPerAttempt(t *testing.T) {
	t.Parallel()
	db := newDB(t)

	first := newJobRunRow(models.NewID(), "job-1", 1)
	require.NoError(t, db.Create(&first).Error, "the first audit row for an attempt always inserts")

	second := newJobRunRow(models.NewID(), "job-1", 1)
	require.Error(t, db.Create(&second).Error,
		"a second audit row for the same (job_id, attempt) must be rejected by job_runs_job_attempt")

	// The constraint is scoped to the pair, not to the job: the next attempt on
	// the same job is a new audit row, which is the whole point of the counter.
	next := newJobRunRow(models.NewID(), "job-1", 2)
	assert.NoError(t, db.Create(&next).Error, "attempt 2 on the same job is a distinct audit row")

	// ...and not to the attempt number either: two jobs may both be on attempt 1.
	other := newJobRunRow(models.NewID(), "job-2", 1)
	assert.NoError(t, db.Create(&other).Error, "attempt 1 on a different job is a distinct audit row")
}

// TestNewDBEnforcesOnePeriodicPerSlug proves idx_job_periodics_slug is enforced
// on the default fixture. It is what makes UpsertPeriodic an upsert rather than
// an append: without the rejection, a redeployed schedule accumulates duplicate
// rows and every one of them enqueues.
func TestNewDBEnforcesOnePeriodicPerSlug(t *testing.T) {
	t.Parallel()
	db := newDB(t)

	first := newJobPeriodicRow(models.NewID(), "nightly-rollup")
	require.NoError(t, db.Create(&first).Error, "the first schedule for a slug always inserts")

	second := newJobPeriodicRow(models.NewID(), "nightly-rollup")
	require.Error(t, db.Create(&second).Error,
		"a second schedule for the same slug must be rejected by idx_job_periodics_slug")

	other := newJobPeriodicRow(models.NewID(), "hourly-sweep")
	assert.NoError(t, db.Create(&other).Error, "a different slug is a different schedule")
}

// newJobRunRow builds a minimal valid job_runs row. The caller owns the primary
// key so a test can vary the key and the (job_id, attempt) pair independently —
// which is what makes it possible to say which constraint rejected an insert.
func newJobRunRow(id, jobID string, attempt int) jobRunRow {
	now := time.Now()
	return jobRunRow{
		ID:            id,
		JobID:         jobID,
		Attempt:       attempt,
		ExecutorClass: "local",
		ExecutorID:    "h1",
		StartedAt:     now,
		Outcome:       "started",
		CreatedAt:     now,
	}
}

// newJobPeriodicRow builds a minimal valid job_periodics row with a caller-owned
// primary key and slug, for the same reason. IntervalSeconds is set because
// BeforeSave requires exactly one of cron_expr or interval_seconds, and a row
// rejected by the hook would never reach the index under test.
func newJobPeriodicRow(id, slug string) jobPeriodicRow {
	now := time.Now()
	every := 60
	return jobPeriodicRow{
		ID:              id,
		Slug:            slug,
		Kind:            "test.kind",
		ArgsTemplate:    datatypes.JSON("{}"),
		Queue:           "periodic",
		IntervalSeconds: &every,
		NextRunAt:       now,
		IsActive:        true,
		CreatedAt:       now,
		UpdatedAt:       now,
	}
}
