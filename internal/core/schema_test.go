package core

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/mrz1836/go-foundation/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/datatypes"
	"gorm.io/gorm"
)

// legacyJobRunRow is job_runs exactly as the release before the analytics
// columns declared it. It stands in for two things the upgrade must survive: a
// database installed by that release, and a binary of that release still
// writing to the database after it was migrated — the rolling-deploy window.
//
// It carries no BeforeCreate hook, like the old release's writes as far as the
// new columns are concerned, and it is written with whatever zone its times
// carry, as the old release stamped them.
type legacyJobRunRow struct {
	ID               string         `gorm:"column:id;primaryKey"`
	JobID            string         `gorm:"column:job_id;not null"`
	Attempt          int            `gorm:"column:attempt;not null"`
	ExecutorClass    string         `gorm:"column:executor_class;not null;default:''"`
	ExecutorID       string         `gorm:"column:executor_id;not null"`
	StartedAt        time.Time      `gorm:"column:started_at;not null"`
	FinishedAt       *time.Time     `gorm:"column:finished_at"`
	Outcome          string         `gorm:"column:outcome;not null"`
	ErrorClass       *string        `gorm:"column:error_class"`
	ErrorMessage     *string        `gorm:"column:error_message"`
	ErrorPayload     datatypes.JSON `gorm:"column:error_payload;type:jsonb"`
	Output           datatypes.JSON `gorm:"column:output;type:jsonb"`
	DurationMs       *int           `gorm:"column:duration_ms"`
	CostMicros       *int64         `gorm:"column:cost_micros"`
	EnqueuedChildren int            `gorm:"column:enqueued_children;not null;default:0"`
	CreatedAt        time.Time      `gorm:"column:created_at;not null"`
}

// TableName binds legacyJobRunRow to the job_runs table.
func (legacyJobRunRow) TableName() string { return "job_runs" }

// legacyIndexNames are the indexes the analytics release added. A legacy
// schema is the current IndexSet minus these — derived, never hand-written.
//
//nolint:gochecknoglobals // shared fixture
var legacyIndexNames = []string{"jobs_finished"}

// installLegacySchema builds, on a bare database, the schema the previous
// release installed: its tables (no job_run_finishes or job_stats_hourly,
// job_runs without the analytics columns) and its indexes.
func installLegacySchema(t *testing.T, db *gorm.DB) {
	t.Helper()
	require.NoError(t, db.AutoMigrate(
		&jobRow{}, &legacyJobRunRow{}, &jobPeriodicRow{}, &limiterBucketRow{}, &limiterHoldRow{},
	))
	set, err := IndexSet(db.Name())
	require.NoError(t, err)
	for _, idx := range set {
		if slices.Contains(legacyIndexNames, idx.Name) {
			continue
		}
		require.NoError(t, db.Exec(idx.DDL).Error, "legacy index %s", idx.Name)
	}
}

// writeLegacyHistory writes what an old binary leaves behind: a finished job
// and its run, in the given zone, with no analytics columns. It returns the job
// id.
func writeLegacyHistory(t *testing.T, db *gorm.DB, id, kind string, state JobState, at time.Time) string {
	t.Helper()
	finalized := at
	require.NoError(t, db.Create(&jobRow{
		ID: id, Kind: kind, Queue: "legacy-q", Args: datatypes.JSON("{}"), State: string(state),
		Attempt: 1, MaxAttempts: 1, CreatedAt: at.Add(-time.Minute), UpdatedAt: at,
		ScheduledAt: at.Add(-time.Minute), FinalizedAt: &finalized,
	}).Error)
	outcome := OutcomeSuccess
	if state == StateDiscarded {
		outcome = OutcomeError
	}
	dur := 1500
	require.NoError(t, db.Create(&legacyJobRunRow{
		ID: id + "-run", JobID: id, Attempt: 1, ExecutorID: "old-binary",
		StartedAt: at.Add(-1500 * time.Millisecond), FinishedAt: &finalized, Outcome: string(outcome),
		DurationMs: &dur, CreatedAt: at.Add(-1500 * time.Millisecond),
	}).Error)
	return id
}

// schemaDriftNames renders a drift list as table.column strings.
func schemaDriftNames(drift []SchemaDrift) []string {
	out := make([]string, len(drift))
	for i, d := range drift {
		out[i] = d.String()
	}
	return out
}

// TestInspectSchemaReportsWhatAnOlderSchemaLacks proves the parity check names
// exactly the columns and table the analytics release added, and nothing on a
// current schema.
func TestInspectSchemaReportsWhatAnOlderSchemaLacks(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	legacy := newBareSQLite(t)
	installLegacySchema(t, legacy)
	drift, err := InspectSchema(ctx, legacy)
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{
		"job_runs.kind", "job_runs.queue", "job_runs.queue_wait_ms", "job_runs.job_state",
		"job_runs.superseded", "job_run_finishes (table)", "job_stats_hourly (table)",
		"job_stats_progress (table)",
	}, schemaDriftNames(drift))

	current, err := InspectSchema(ctx, newDB(t))
	require.NoError(t, err)
	assert.Empty(t, current, "a schema Migrate installed has no gap")

	_, err = InspectSchema(ctx, nil)
	require.Error(t, err)
}

// TestRunnerFailsFastOnAnOutdatedSchema is the deploy-order guard: a new binary
// against a database nobody migrated must stop before it claims anything.
// Claiming first would lease a job, fail the stub insert on the missing column,
// and leave the lease sweep to reclaim the same jobs forever.
func TestRunnerFailsFastOnAnOutdatedSchema(t *testing.T) {
	t.Parallel()
	db := newBareSQLite(t)
	installLegacySchema(t, db)
	ctx := context.Background()
	id, err := Enqueue(ctx, NewClient(db), "stats.write", []byte(`{}`), InsertOpts{})
	require.NoError(t, err)

	err = rwRunner(t, db, NewRegistry()).RunUntilIdle(ctx)
	require.ErrorIs(t, err, ErrSchemaOutdated)
	assert.Contains(t, err.Error(), "job_runs.kind", "the error names what is missing")
	assert.Contains(t, err.Error(), "flywheel migrate", "and how to fix it")
	assert.Equal(t, string(StateAvailable), jobState(t, db, id), "nothing was claimed")
}

// TestSchedulerWithStatsFailsFastOnAnOutdatedSchema is the same guard for the
// Scheduler: its lease sweep writes the run tables, so against a schema missing
// their new columns Run stops before starting any activity rather than logging
// the failure on every tick — and with stats enabled it also names the rollup
// tables.
func TestSchedulerWithStatsFailsFastOnAnOutdatedSchema(t *testing.T) {
	t.Parallel()
	db := newBareSQLite(t)
	installLegacySchema(t, db)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	s := newSchedulerCfg(t, SchedulerConfig{DB: db, Client: NewClient(db), StatsRollupInterval: time.Minute})
	err := s.Run(ctx)
	require.ErrorIs(t, err, ErrSchemaOutdated)
	assert.Contains(t, err.Error(), "job_stats_hourly")
	assert.Contains(t, err.Error(), "job_stats_progress")

	plain := newSchedulerCfg(t, SchedulerConfig{DB: db, Client: NewClient(db)})
	err = plain.Run(ctx)
	require.ErrorIs(t, err, ErrSchemaOutdated, "the sweep alone writes the new job_runs columns")
	assert.Contains(t, err.Error(), "job_runs.job_state")
	assert.NotContains(t, err.Error(), "job_stats_hourly", "a Scheduler without stats does not need the rollup tables")
}

// TestSchemaProbeIsNotAVerdictOnAnUnreachableDatabase proves a database that is
// down at startup does not crash a Runner: it has always survived that, on its
// poll backoff, and a schema check must not turn an outage into a crash.
func TestSchemaProbeIsNotAVerdictOnAnUnreachableDatabase(t *testing.T) {
	t.Parallel()
	db := newBareSQLite(t)
	sqlDB, err := db.DB()
	require.NoError(t, err)
	require.NoError(t, sqlDB.Close())

	checked, err := probeSchema(context.Background(), db, &jobRunRow{})
	require.NoError(t, err)
	assert.False(t, checked, "an unreachable database is no verdict, so the probe runs again next start")
}

// TestMigrateUpgradesALegacySchemaInPlace is the upgrade path end to end on
// SQLite: a database the previous release installed and wrote history into is
// migrated, after which the schema and indexes are at parity, the history is
// still readable, and an old binary still writing to it keeps working.
func TestMigrateUpgradesALegacySchemaInPlace(t *testing.T) {
	t.Parallel()
	upgradeSuite(t, func(t *testing.T) *gorm.DB {
		t.Helper()
		return newBareSQLite(t)
	}, func(db *gorm.DB) error { return Migrate(db) })
}

// upgradeSuite runs the upgrade path against one dialect: bare opens a database
// with no schema, and migrate is the upgrade under test.
func upgradeSuite(t *testing.T, bare func(*testing.T) *gorm.DB, migrate func(*gorm.DB) error) {
	t.Helper()
	db := bare(t)
	installLegacySchema(t, db)
	// The previous release stamped job_runs in the clock's zone. A zone with a
	// DST rule makes the re-stamp below do real work.
	ny, err := time.LoadLocation("America/New_York")
	require.NoError(t, err)
	at := time.Date(2026, 9, 1, 10, 15, 0, 0, ny)
	writeLegacyHistory(t, db, "legacy-ok", "legacy.kind", StateSucceeded, at)
	writeLegacyHistory(t, db, "legacy-bad", "legacy.kind", StateDiscarded, at.Add(time.Minute))

	require.NoError(t, migrate(db), "the upgrade")
	ctx := context.Background()

	drift, err := InspectSchema(ctx, db)
	require.NoError(t, err)
	assert.Empty(t, drift, "every new column and table is in place")
	idx, err := InspectIndexes(ctx, db)
	require.NoError(t, err)
	assert.Empty(t, idx, "and every new index")

	var legacy jobRunRow
	require.NoError(t, db.Where("id = ?", "legacy-ok-run").First(&legacy).Error)
	assert.Empty(t, legacy.Kind, "history is not backfilled; it resolves on read")
	assert.False(t, legacy.Superseded, "the new column's default applies to old rows")

	// The old runs reach the stats once the finish log has entries for them —
	// which the rollup's first pass writes on its own — and the read path
	// resolves their kind, queue, and discard through jobs.
	n, err := BackfillRunFinishes(ctx, db)
	require.NoError(t, err)
	assert.EqualValues(t, 2, n, "one finish-log entry per old finished run")
	n, err = BackfillRunFinishes(ctx, db)
	require.NoError(t, err)
	assert.Zero(t, n, "the backfill is idempotent")
	res, err := Stats(ctx, db, StatsParams{From: at.Add(-time.Hour), To: at.Add(time.Hour)})
	require.NoError(t, err)
	require.Len(t, res.Kinds, 1)
	ks := res.Kinds[0]
	assert.Equal(t, "legacy.kind", ks.Kind)
	assert.EqualValues(t, 2, ks.Attempts)
	assert.EqualValues(t, 1, ks.Succeeded)
	assert.EqualValues(t, 1, ks.Discarded, "an old failed attempt that ended its job counts as a discard")
	assert.EqualValues(t, 1, ks.Duration.Count)

	// An old binary still running during a rolling deploy writes a row without
	// any analytics column. The defaults make it a valid row.
	writeLegacyHistory(t, db, "legacy-late", "legacy.kind", StateSucceeded, at.Add(2*time.Minute))
	var late jobRunRow
	require.NoError(t, db.Where("id = ?", "legacy-late-run").First(&late).Error)
	assert.Empty(t, late.Kind)
	assert.False(t, late.Superseded)
	n, err = BackfillRunFinishes(ctx, db)
	require.NoError(t, err)
	assert.EqualValues(t, 1, n, "the recheck pass picks up what an old binary finalized during the deploy")

	// And the new binary runs against the upgraded schema.
	reg := NewRegistry()
	Register[statsWriteArgs](reg, statsWriteWorker{})
	_, err = Insert(ctx, NewClient(db), statsWriteArgs{}, InsertOpts{})
	require.NoError(t, err)
	r, err := NewRunner(RunnerConfig{
		DB: db, Driver: driverForTest(t, db), Registry: reg, Queues: []string{defaultQueue},
		ClaimAnyClass: true, PollInterval: 2 * time.Millisecond,
	})
	require.NoError(t, err)
	require.NoError(t, r.RunUntilIdle(ctx))
}

// driverForTest selects the dialect's Driver.
func driverForTest(t *testing.T, db *gorm.DB) Driver {
	t.Helper()
	d, err := driverFor(db)
	require.NoError(t, err)
	return d
}

// TestOldRowsAreAcceptedByTheNewSchemaDefaults pins the column defaults the
// rolling-deploy guarantee rests on, read from the GORM tags the schema is
// generated from: each analytics column is nullable or defaulted, so an INSERT
// that names none of them is valid.
func TestOldRowsAreAcceptedByTheNewSchemaDefaults(t *testing.T) {
	t.Parallel()
	stmt := &gorm.Statement{DB: newDB(t)}
	require.NoError(t, stmt.Parse(&jobRunRow{}))
	for _, col := range []string{"kind", "queue", "queue_wait_ms", "job_state", "superseded"} {
		f := stmt.Schema.LookUpField(col)
		require.NotNil(t, f, col)
		assert.True(t, !f.NotNull || f.HasDefaultValue,
			"%s must be nullable or defaulted, or an older binary's insert fails against the new schema", col)
	}
}

// TestSchemaOutdatedErrorIsMatchable proves the sentinel survives the runner's
// wrapping, so a host can branch on it.
func TestSchemaOutdatedErrorIsMatchable(t *testing.T) {
	t.Parallel()
	err := schemaOutdatedError([]SchemaDrift{{Table: "job_runs", Column: "kind"}, {Table: "job_stats_hourly"}})
	require.ErrorIs(t, err, ErrSchemaOutdated)
	wrapped := errors.Join(errors.New("context"), err)
	require.ErrorIs(t, wrapped, ErrSchemaOutdated)
	assert.Contains(t, err.Error(), "job_runs.kind, job_stats_hourly (table)")
}

// fixedClockCtx is a convenience for the stats tests: a background context
// carrying a fixed clock at t.
func fixedClockCtx(t time.Time) context.Context {
	return clockCtx(context.Background(), models.NewFixedClock(t))
}
