package core

import (
	"context"
	"testing"
	"time"

	"github.com/mrz1836/go-foundation/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

// previousReleaseGapSuite proves the parity checks name exactly what this
// release adds to a schema the previous release installed: the one table, and
// no index.
func previousReleaseGapSuite(t *testing.T, db *gorm.DB) {
	t.Helper()
	installPreviousReleaseSchema(t, db)
	ctx := context.Background()

	drift, err := InspectSchema(ctx, db)
	require.NoError(t, err)
	assert.Equal(t, []string{"job_kinds (table)"}, schemaDriftNames(drift))
	idx, err := InspectIndexes(ctx, db)
	require.NoError(t, err)
	missing := make([]string, len(idx))
	for i, d := range idx {
		missing[i] = d.Name
		assert.Empty(t, d.Installed, "%s is absent", d.Name)
	}
	assert.ElementsMatch(t, previousReleaseIndexNames, missing, "the release adds no index")
}

// TestInspectSchemaReportsWhatThePreviousReleaseLacks is the parity check
// against the previous release's schema on SQLite.
func TestInspectSchemaReportsWhatThePreviousReleaseLacks(t *testing.T) {
	t.Parallel()
	previousReleaseGapSuite(t, newBareSQLite(t))
}

// schedulerRefusesWithoutPausedAtSuite is the deploy-order guard for the
// column: a Scheduler's tick filters on paused_at, so against a schema without
// it Run stops before starting any activity, naming the column and the fix,
// rather than failing every tick. A Runner never reads job_periodics, so it
// runs against the same schema. The schema is the previous release's without
// the column, as a release before the pause left it.
func schedulerRefusesWithoutPausedAtSuite(t *testing.T, db *gorm.DB) {
	t.Helper()
	installPreviousReleaseSchema(t, db)
	require.NoError(t, db.Exec("ALTER TABLE job_periodics DROP COLUMN paused_at").Error)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	s := newSchedulerCfg(t, SchedulerConfig{DB: db, Client: NewClient(db), Driver: driverForTest(t, db)})
	err := s.Run(ctx)
	require.ErrorIs(t, err, ErrSchemaOutdated)
	assert.Contains(t, err.Error(), "job_periodics.paused_at", "the error names the missing column")
	assert.Contains(t, err.Error(), "flywheel migrate", "and how to fix it")

	r, err := NewRunner(RunnerConfig{
		DB: db, Driver: driverForTest(t, db), Registry: NewRegistry(), Queues: []string{defaultQueue},
	})
	require.NoError(t, err)
	require.NoError(t, r.RunUntilIdle(ctx), "a Runner does not need the column")
}

// TestSchedulerRefusesToStartWithoutThePausedAtColumn runs the guard on SQLite.
func TestSchedulerRefusesToStartWithoutThePausedAtColumn(t *testing.T) {
	t.Parallel()
	schedulerRefusesWithoutPausedAtSuite(t, newBareSQLite(t))
}

// previousReleaseUpgradeSuite runs the upgrade from the previous release: a
// schedule that release wrote is migrated in place, reads as not paused, and
// keeps firing under this release's Scheduler; a row that release's binary
// writes after the migration, as a rolling deploy would, is valid and not
// paused either.
func previousReleaseUpgradeSuite(t *testing.T, db *gorm.DB, migrate func(*gorm.DB) error) {
	t.Helper()
	installPreviousReleaseSchema(t, db)
	due := time.Now().UTC().Truncate(time.Second).Add(-30 * time.Second)
	writeLegacyPeriodic(t, db, "legacy-nightly", "legacy.periodic", due)

	require.NoError(t, migrate(db), "the upgrade")
	ctx := context.Background()
	assertNoSchemaOrIndexDrift(t, db)

	views, err := ListPeriodics(ctx, db)
	require.NoError(t, err)
	require.Len(t, views, 1)
	assert.False(t, views[0].Paused, "a schedule written before the upgrade is not paused")
	assert.Nil(t, views[0].PausedAt)

	s := newSchedulerCfg(t, SchedulerConfig{DB: db, Client: NewClient(db), Driver: driverForTest(t, db)})
	require.NoError(t, s.checkSchema(ctx), "this release's Scheduler starts against the upgraded schema")
	n, err := s.Tick(clockCtx(ctx, models.NewFixedClock(due.Add(time.Second))))
	require.NoError(t, err)
	assert.Equal(t, 1, n, "the schedule fires on its cadence")
	assert.EqualValues(t, 1, jobCount(t, db, "legacy.periodic"))

	writeLegacyPeriodic(t, db, "legacy-late", "legacy.periodic", due.Add(time.Hour))
	views, err = ListPeriodics(ctx, db)
	require.NoError(t, err)
	require.Len(t, views, 2)
	for _, v := range views {
		assert.False(t, v.Paused, "%s: an older binary's insert names no paused_at and is not paused", v.Slug)
	}
}

// assertNoSchemaOrIndexDrift asserts db carries every column, table, and index
// this release declares.
func assertNoSchemaOrIndexDrift(t *testing.T, db *gorm.DB) {
	t.Helper()
	ctx := context.Background()
	gaps, err := InspectSchema(ctx, db)
	require.NoError(t, err)
	assert.Empty(t, gaps, "every column and table is in place")
	idx, err := InspectIndexes(ctx, db)
	require.NoError(t, err)
	assert.Empty(t, idx, "and every index")
}

// TestMigrateUpgradesThePreviousReleaseSchema is the library-owned upgrade from
// the previous release on SQLite.
func TestMigrateUpgradesThePreviousReleaseSchema(t *testing.T) {
	t.Parallel()
	previousReleaseUpgradeSuite(t, newBareSQLite(t), func(db *gorm.DB) error { return Migrate(db) })
}
