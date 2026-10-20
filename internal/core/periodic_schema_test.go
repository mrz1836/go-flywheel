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
// release adds to a schema the previous release installed: the run-log table,
// and its one index.
func previousReleaseGapSuite(t *testing.T, db *gorm.DB) {
	t.Helper()
	installPreviousReleaseSchema(t, db)
	ctx := context.Background()

	drift, err := InspectSchema(ctx, db)
	require.NoError(t, err)
	assert.Equal(t, []string{"job_run_logs (table)"}, schemaDriftNames(drift))
	idx, err := InspectIndexes(ctx, db)
	require.NoError(t, err)
	missing := make([]string, len(idx))
	for i, d := range idx {
		missing[i] = d.Name
		assert.Empty(t, d.Installed, "%s is absent", d.Name)
	}
	assert.ElementsMatch(t, previousReleaseIndexNames, missing, "the release adds the table's one index")
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
// runs against the same schema. db carries the current schema, and the suite
// drops the column, as a release before the pause left it.
func schedulerRefusesWithoutPausedAtSuite(t *testing.T, db *gorm.DB) {
	t.Helper()
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
	schedulerRefusesWithoutPausedAtSuite(t, newDB(t))
}

// schedulerRefusesWithoutPeriodicColumnsSuite is the deploy-order guard for the
// five schedule columns: a Scheduler's fire writes the schedule's history and
// its start-up apply its source, so against a schema without them Run stops
// before starting any activity, naming each column and the fix. A Runner never
// reads job_periodics, so it runs against the same schema. db carries the
// current schema, and the suite drops the five, as a release before them left
// them.
func schedulerRefusesWithoutPeriodicColumnsSuite(t *testing.T, db *gorm.DB) {
	t.Helper()
	for _, col := range scheduleColumns {
		require.NoError(t, db.Exec("ALTER TABLE "+col.Table+" DROP COLUMN "+col.Column).Error, "drop %s", col)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	s := newSchedulerCfg(t, SchedulerConfig{DB: db, Client: NewClient(db), Driver: driverForTest(t, db)})
	err := s.Run(ctx)
	require.ErrorIs(t, err, ErrSchemaOutdated)
	for _, col := range scheduleColumns {
		assert.Contains(t, err.Error(), col.String(), "the error names each missing column")
	}
	assert.Contains(t, err.Error(), "flywheel migrate", "and how to fix it")

	r, err := NewRunner(RunnerConfig{
		DB: db, Driver: driverForTest(t, db), Registry: NewRegistry(), Queues: []string{defaultQueue},
	})
	require.NoError(t, err)
	require.NoError(t, r.RunUntilIdle(ctx), "a Runner does not need the columns")
}

// TestSchedulerRefusesToStartWithoutThePeriodicColumns runs the guard on SQLite.
func TestSchedulerRefusesToStartWithoutThePeriodicColumns(t *testing.T) {
	t.Parallel()
	schedulerRefusesWithoutPeriodicColumnsSuite(t, newDB(t))
}

// previousReleaseUpgradeSuite runs the upgrade from an older release: a
// schedule a release before the schedule columns wrote is migrated in place,
// reads as not paused, and keeps firing under this release's Scheduler; a row
// that release's binary writes after the migration, as a rolling deploy would,
// is valid and not paused either. The schema is the previous release's without
// the schedule columns and the progress and limit columns, which came after
// them, so the upgrade adds those as well as this release's.
func previousReleaseUpgradeSuite(t *testing.T, db *gorm.DB, migrate func(*gorm.DB) error) {
	t.Helper()
	installPreviousReleaseSchema(t, db)
	dropColumns(t, db, scheduleColumns...)
	dropColumns(t, db, progressAndLimitColumns...)
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
	assert.Nil(t, views[0].PausedBy)
	assert.Empty(t, views[0].Source, "and has no source until its next write")
	assert.Empty(t, views[0].LastJobID, "nor any history")

	s := newSchedulerCfg(t, SchedulerConfig{DB: db, Client: NewClient(db), Driver: driverForTest(t, db)})
	require.NoError(t, s.checkSchema(ctx), "this release's Scheduler starts against the upgraded schema")
	n, err := s.Tick(clockCtx(ctx, models.NewFixedClock(due.Add(time.Second))))
	require.NoError(t, err)
	assert.Equal(t, 1, n, "the schedule fires on its cadence")
	assert.EqualValues(t, 1, jobCount(t, db, "legacy.periodic"))
	assertFireRemembered(t, db, "legacy-nightly", "legacy.periodic")

	writeLegacyPeriodic(t, db, "legacy-late", "legacy.periodic", due.Add(time.Hour))
	views, err = ListPeriodics(ctx, db)
	require.NoError(t, err)
	require.Len(t, views, 2)
	for _, v := range views {
		assert.False(t, v.Paused, "%s: an older binary's insert names no paused_at and is not paused", v.Slug)
	}
}

// assertFireRemembered asserts slug's history holds exactly the one job of kind
// it fired: the history column's default reads as an empty history, and the
// fire rewrites it.
func assertFireRemembered(t *testing.T, db *gorm.DB, slug, kind string) {
	t.Helper()
	var fired []string
	require.NoError(t, db.Model(&jobRow{}).Where("kind = ?", kind).Pluck("id", &fired).Error)
	history, err := PeriodicHistory(context.Background(), db, slug)
	require.NoError(t, err)
	require.Len(t, history, 1, "the fire is remembered on the migrated row")
	assert.Equal(t, fired, []string{history[0].JobID})
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
