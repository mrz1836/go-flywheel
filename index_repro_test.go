package flywheel

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

// These are the exact drifted definitions from real databases, not invented
// ones. staleJobsReadyDDL is the pre-v0.7.1 four-column key a consumer's dev
// database kept through a green suite; staleDeletedAtDDL is the non-partial
// idx_jobs_deleted_at that had been live since a schema's first migration and was
// found only by a hand diff. Both passed every name-level check ever written
// against them, which is the whole reason this reproduction exists.
const (
	staleJobsReadyDDL = `CREATE INDEX jobs_ready ON jobs (queue, executor_class, priority, scheduled_at) ` +
		`WHERE state IN ('available', 'retryable', 'scheduled')`
	staleDeletedAtDDL = `CREATE INDEX idx_jobs_deleted_at ON jobs (deleted_at)`
)

// historicallyDriftedIndexes are the two indexes seedSchemaWithHistoricalDrift
// installs stale; every other runtime index it installs at its current
// definition.
//
//nolint:gochecknoglobals // shared expectation fixture for the reproduction tests
var historicallyDriftedIndexes = map[string]string{
	"jobs_ready":          staleJobsReadyDDL,
	"idx_jobs_deleted_at": staleDeletedAtDDL,
}

// seedSchemaWithHistoricalDrift builds a host-owned schema — tables from Models —
// carrying every runtime index at its current definition except jobs_ready and
// idx_jobs_deleted_at, which it installs at their stale historical definitions.
// The correct indexes come from IndexSet so they cannot drift from the runtime;
// only the two deliberately-stale ones are written out.
func seedSchemaWithHistoricalDrift(t *testing.T, db *gorm.DB) {
	t.Helper()
	require.NoError(t, db.AutoMigrate(Models()...), "the host's loader owns the tables")

	set, err := IndexSet(db.Name())
	require.NoError(t, err)
	for _, idx := range set {
		ddl := idx.DDL
		if stale, ok := historicallyDriftedIndexes[idx.Name]; ok {
			ddl = stale
		}
		require.NoError(t, db.Exec(ddl).Error, "seed index %s", idx.Name)
	}
}

// driftNames returns the names in a drift slice, for set comparison.
func driftNames(drift []IndexDrift) []string {
	out := make([]string, len(drift))
	for i, d := range drift {
		out[i] = d.Name
	}
	return out
}

// TestInstallerCatchesTheHistoricalDrift is the reproduction that matters: a
// schema in the exact state two real databases sat in, and proof that the
// definition-level check catches what every name-level check missed.
//
// The name check is shown green on both stale indexes first — that is the state
// the incidents hid in — and the definition check is then shown to report exactly
// those two, and the default install to fail rather than report success. Each of
// those assertions is one that fails against the stale definition, not one that
// merely passes against a fresh schema: run this reproduction against the
// name-only installer this change replaces and InstallIndexes returns nil and the
// ErrIndexDrift assertion fails.
func TestInstallerCatchesTheHistoricalDrift(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	db := newBareSQLite(t)
	seedSchemaWithHistoricalDrift(t, db)

	// The old-style presence check is blind: both stale indexes exist under their
	// runtime names, so a name assertion is green. This is the failure mode.
	assert.True(t, sqliteHasIndex(t, db, "jobs_ready"),
		"the stale jobs_ready exists by name — a presence check passes against it")
	assert.True(t, sqliteHasIndex(t, db, "idx_jobs_deleted_at"),
		"the stale idx_jobs_deleted_at exists by name — a presence check passes against it")

	// The definition check is not blind: it reports exactly the two stale indexes.
	drift, err := InspectIndexes(ctx, db)
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"jobs_ready", "idx_jobs_deleted_at"}, driftNames(drift),
		"the definition check reports exactly the two historically-drifted indexes and no others")

	// And the default install refuses to report success over them.
	err = InstallIndexes(ctx, db)
	require.ErrorIs(t, err, ErrIndexDrift,
		"the installer no longer reports success on the historical drift")
	assert.Contains(t, err.Error(), "jobs_ready")
	assert.Contains(t, err.Error(), "idx_jobs_deleted_at")

	// Reconcile heals both to the runtime's definitions.
	require.NoError(t, InstallIndexesWithOptions(ctx, db, IndexOpts{Reconcile: true}))
	after, err := InspectIndexes(ctx, db)
	require.NoError(t, err)
	assert.Empty(t, after, "reconcile brings the historical drift to the runtime's definitions")
}
