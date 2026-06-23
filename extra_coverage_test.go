package flywheel

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/datatypes"
	"gorm.io/gorm"
)

// closeDB closes a SQLite DB's underlying connection so every subsequent gorm
// operation returns an error, the canonical way to drive the runtime's
// "if err != nil { return ... }" branches without a live database fault.
func closeDB(t *testing.T, db *gorm.DB) {
	t.Helper()
	sqlDB, err := db.DB()
	require.NoError(t, err)
	require.NoError(t, sqlDB.Close())
}

// --- base.go lifecycle hook validation branches -----------------------------

// TestJobRunRowBeforeCreateRequiresAuditFields proves the append-only job_runs
// hook rejects a row missing either mandatory audit field (executor_id, outcome)
// before it ever reaches the database.
func TestJobRunRowBeforeCreateRequiresAuditFields(t *testing.T) {
	t.Parallel()
	db := newDB(t)

	tests := map[string]struct {
		row   jobRunRow
		field string
	}{
		"missing executor_id": {
			row:   jobRunRow{JobID: "j", Outcome: string(OutcomeStarted)},
			field: "executor_id",
		},
		"missing outcome": {
			row:   jobRunRow{JobID: "j", ExecutorID: "h"},
			field: "outcome",
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			err := db.Create(&tc.row).Error
			require.Error(t, err, "a job_runs row missing %s is rejected by BeforeCreate", tc.field)
			assert.ErrorIs(t, err, ErrValidation)
		})
	}
}

// TestJobPeriodicRowBeforeSaveRequiresIdentity proves the periodic hook rejects a
// full-row save that carries an identity (so it is not the scheduler's bare
// column-map advance) but is missing slug or kind.
func TestJobPeriodicRowBeforeSaveRequiresIdentity(t *testing.T) {
	t.Parallel()
	db := newDB(t)

	tests := map[string]struct {
		slug string
		kind string
	}{
		"missing slug": {kind: "k"},
		"missing kind": {slug: "s"},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			// Kind is set on the missing-slug case (and slug on the missing-kind case)
			// so the row carries an identity and is not treated as the scheduler's
			// bare column-map advance, forcing BeforeSave's required-field check.
			row := jobPeriodicRow{Slug: tc.slug, Kind: tc.kind, NextRunAt: time.Now()}
			err := db.Create(&row).Error
			require.Error(t, err, "a periodic row carrying an identity but missing a required field is rejected")
			assert.ErrorIs(t, err, ErrValidation)
		})
	}
}

// --- client.go non-duplicate insert error ----------------------------------

// TestInsertSurfacesNonDuplicateDBError proves a producer insert that fails for a
// reason other than a unique_key collision is surfaced (not mistaken for
// ErrAlreadyEnqueued).
func TestInsertSurfacesNonDuplicateDBError(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	closeDB(t, db)

	_, err := Insert(context.Background(), NewClient(db), successArgs{V: "x"}, InsertOpts{})
	require.Error(t, err, "a non-duplicate insert failure is surfaced")
	assert.NotErrorIs(t, err, ErrAlreadyEnqueued)
}

// --- driver_sqlite.go claim transaction error branches ----------------------

// TestSQLiteDequeueSurfacesSelectError proves the SQLite claim transaction
// surfaces a failure of the claimable-rows SELECT (closed DB inside the txn).
func TestSQLiteDequeueSurfacesSelectError(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	d := NewSQLiteDriver(db)
	closeDB(t, db)

	_, err := d.Dequeue(context.Background(), []string{"default"}, AnyClass, true, 5, time.Second)
	require.Error(t, err, "a select failure inside the claim transaction is surfaced")
}

// TestSQLiteDequeueSurfacesRowConversionError proves a claimed row with a
// malformed tags blob aborts the claim transaction via rawFromRow rather than
// returning a corrupt RawJob.
func TestSQLiteDequeueSurfacesRowConversionError(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	d := NewSQLiteDriver(db)
	ctx := context.Background()

	// Seed a claimable row whose tags blob cannot decode to []string.
	seedJob(t, db, jobRow{
		ID: "bad-tags", Kind: "k", State: string(StateAvailable),
		ScheduledAt: time.Now().Add(-time.Minute), Tags: datatypes.JSON(`"not-an-array"`),
	})

	_, err := d.Dequeue(ctx, []string{"default"}, AnyClass, true, 5, time.Second)
	require.Error(t, err, "a row whose tags cannot be decoded aborts the claim transaction")
}

// --- driver.go Finalize / InsertChild / Sweep error branches ----------------

// TestFinalizeSurfacesLoadStubError proves Finalize surfaces a failure loading
// the run stub it must read to compute the attempt duration. The transaction
// begins (DB live) but the job_runs table is dropped so the stub First fails
// inside the closure — exercising the load-stub branch rather than a failed
// BEGIN.
func TestFinalizeSurfacesLoadStubError(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	d := baseDriver{db: db}
	require.NoError(t, db.Migrator().DropTable(&jobRunRow{}), "drop job_runs so the stub load fails")

	raw := RawJob{ID: "job-x", Attempt: 1, MaxAttempts: 5}
	err := d.Finalize(context.Background(), raw, NewID(), Result{}, nil, time.Now())
	require.ErrorContains(t, err, "load run stub", "a failed run-stub load surfaces a finalize error")
}

// TestFinalizeSurfacesAdvanceError proves Finalize surfaces a failure of the
// jobs.state advance UPDATE. The run stub is seeded so its load succeeds, then
// the jobs table is dropped so only the advance step fails.
func TestFinalizeSurfacesAdvanceError(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	d := baseDriver{db: db}
	ctx := context.Background()

	raw := RawJob{ID: "job-a", Attempt: 1, MaxAttempts: 5}
	seedJob(t, db, jobRow{ID: raw.ID, Kind: "k", State: string(StateRunning), ScheduledAt: time.Now()})
	runID := NewID()
	require.NoError(t, d.InsertRunStub(ctx, runID, raw, time.Now(), ExecutorClass("local"), "h1"))
	require.NoError(t, db.Migrator().DropTable(&jobRow{}), "drop jobs so only the state advance fails")

	err := d.Finalize(ctx, raw, runID, Result{}, nil, time.Now())
	require.ErrorContains(t, err, "advance job state", "a failed state advance surfaces a finalize error")
}

// TestFinalizeRecordsErrorPayload proves a finalize for a failed attempt marshals
// the work error into a non-empty error_payload and persists it on the run row —
// exercising runFinalizeUpdate's workErr branch end to end.
func TestFinalizeRecordsErrorPayload(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	d := baseDriver{db: db}
	ctx := context.Background()

	raw := RawJob{ID: "job-e", Attempt: 1, MaxAttempts: 5}
	seedJob(t, db, jobRow{ID: raw.ID, Kind: "k", State: string(StateRunning), ScheduledAt: time.Now()})
	runID := NewID()
	require.NoError(t, d.InsertRunStub(ctx, runID, raw, time.Now(), ExecutorClass("local"), "h1"))

	require.NoError(t, d.Finalize(ctx, raw, runID, Result{}, errors.New("boom"), time.Now()))

	var payload string
	require.NoError(t, db.Table("job_runs").
		Select("error_payload").Where("id = ?", runID).Scan(&payload).Error)
	assert.NotEmpty(t, payload, "the error payload is marshaled and stored")
}

// TestInsertChildSurfacesMarshalError proves InsertChild rejects a follow-up
// whose args cannot be JSON-marshaled.
func TestInsertChildSurfacesMarshalError(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	d := baseDriver{db: db}
	ctx := context.Background()

	fu := FollowUp{Kind: "k", Args: make(chan int)} // a channel cannot marshal
	err := d.db.Transaction(func(tx *gorm.DB) error {
		return d.InsertChild(ctx, tx, fu, "parent")
	})
	require.Error(t, err, "an unmarshalable follow-up arg surfaces an error")
}

// TestInsertChildSurfacesNonDuplicateError proves InsertChild surfaces a generic
// create failure (not a unique_key collision) rather than swallowing it as
// ErrAlreadyEnqueued.
func TestInsertChildSurfacesNonDuplicateError(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	d := baseDriver{db: db}
	closeDB(t, db)

	fu := FollowUp{Kind: "k", Args: map[string]any{"n": 1}}
	err := d.InsertChild(context.Background(), db, fu, "parent")
	require.Error(t, err, "a non-duplicate child insert failure is surfaced")
	assert.NotErrorIs(t, err, ErrAlreadyEnqueued)
}

// TestInsertFollowUpsPropagatesError proves a non-collision insert error from one
// follow-up aborts the loop and is returned to the caller.
func TestInsertFollowUpsPropagatesError(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	d := baseDriver{db: db}
	closeDB(t, db)

	_, err := d.insertFollowUps(context.Background(), db,
		[]FollowUp{{Kind: "k", Args: map[string]any{}}}, "parent")
	require.Error(t, err, "a follow-up insert failure aborts and is returned")
}

// TestSweepSurfacesCrashStubError proves Sweep surfaces a failure of the
// crash-stale-stubs UPDATE that runs after expired-lease ids are found and the
// jobs reclaim has succeeded. An expired-lease job is seeded so the find yields
// ids and the jobs reclaim runs, then job_runs is dropped so only the final
// stub-crash UPDATE fails.
func TestSweepSurfacesCrashStubError(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	d := baseDriver{db: db}

	past := time.Now().Add(-time.Hour)
	seedJob(t, db, jobRow{
		ID: "stuck", Kind: "k", State: string(StateRunning),
		ScheduledAt: past, LeasedUntil: &past,
	})
	require.NoError(t, db.Migrator().DropTable(&jobRunRow{}), "drop job_runs so the stub-crash update fails")

	_, err := d.Sweep(context.Background(), time.Now())
	require.ErrorContains(t, err, "crash stale stubs", "a failed stub-crash update surfaces a sweep error")
}

// --- health.go read error branches ------------------------------------------

// TestSampleQueueHealthSurfacesError proves SampleQueueHealth surfaces a read
// failure rather than returning a half-populated snapshot. A closed DB fails the
// first count and drives the error return.
func TestSampleQueueHealthSurfacesError(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	closeDB(t, db)

	_, err := SampleQueueHealth(context.Background(), db)
	require.Error(t, err, "a closed DB surfaces a queue-health read error")
}

// TestSampleQueueHealthOldestReadyAge proves the lag branch (Ready > 0) reads the
// oldest ready scheduled_at and reports a positive age — covering the
// oldest-ready read that only runs when something is ready.
func TestSampleQueueHealthOldestReadyAge(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	now := time.Now().UTC().Truncate(time.Second)
	ctx := clockCtx(context.Background(), NewFixedClock(now))

	seedJob(t, db, jobRow{
		ID: "old-ready", Kind: "k", State: string(StateAvailable),
		ScheduledAt: now.Add(-10 * time.Minute),
	})

	qh, err := SampleQueueHealth(ctx, db)
	require.NoError(t, err)
	assert.EqualValues(t, 1, qh.Ready)
	assert.Equal(t, 10*time.Minute, qh.OldestReadyAge, "the lag is the age of the oldest ready job")
}

// TestRecentFailuresSurfacesRunsQueryError proves RecentFailures surfaces a
// failure of the second (runs) query after the jobs page has loaded. The jobs row
// is committed first, then the connection is closed so only the runs read fails.
func TestRecentFailuresSurfacesRunsQueryError(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	now := time.Now().UTC().Truncate(time.Second)

	// Seed one discarded job so the first query returns a non-empty page, then
	// drop job_runs so the second (runs) read is the one that fails.
	fin := now
	seedJob(t, db, jobRow{
		ID: "disc-1", Kind: "k", State: string(StateDiscarded),
		ScheduledAt: now, FinalizedAt: &fin,
	})
	require.NoError(t, db.Migrator().DropTable(&jobRunRow{}), "drop job_runs so only the runs read fails")

	_, err := RecentFailures(context.Background(), db, RecentFailuresParams{Since: now.Add(-time.Hour)})
	require.ErrorContains(t, err, "recent failures runs", "the runs read failure is surfaced")
}

// --- migrate.go error branches ----------------------------------------------

// TestMigrateSurfacesErrorsOnClosedDB proves Migrate surfaces the failure of its
// first step (the column-rename reconcile) against a closed connection.
func TestMigrateSurfacesErrorsOnClosedDB(t *testing.T) {
	t.Parallel()
	db := newBareSQLite(t)
	closeDB(t, db)

	require.Error(t, Migrate(db), "Migrate against a closed DB surfaces an error")
}

// TestReconcileColumnRenamesSurfacesExecError proves the legacy column-rename
// step surfaces a failed ALTER TABLE ... RENAME COLUMN, both directly and through
// Migrate's wrapper. A legacy-shaped jobs table forces the rename branch, and a
// read-only PRAGMA makes the ALTER fail.
func TestReconcileColumnRenamesSurfacesExecError(t *testing.T) {
	t.Parallel()
	db := newBareSQLite(t)

	// Pin a single connection so the read-only PRAGMA stays in effect for the
	// rename ALTER under the shared-cache pool.
	sqlDB, err := db.DB()
	require.NoError(t, err)
	sqlDB.SetMaxOpenConns(1)

	// A legacy-shaped jobs table carrying the old run_on column (and no
	// executor_class) forces the rename branch; the read-only PRAGMA then makes the
	// ALTER TABLE ... RENAME COLUMN exec fail, exercising the error return.
	require.NoError(t, db.Exec(`CREATE TABLE jobs (id text primary key, run_on text)`).Error)
	require.NoError(t, db.Exec(`PRAGMA query_only = ON`).Error)

	require.Error(t, reconcileColumnRenames(db), "a failed rename ALTER surfaces an error")

	// Drive the same failure through Migrate so its reconcile-column-renames error
	// wrapper (the first step) is exercised too.
	require.Error(t, Migrate(db), "Migrate wraps a column-rename failure")
}

// --- read.go ListJobs error branch ------------------------------------------

// TestListJobsSurfacesDBError proves ListJobs surfaces a query failure.
func TestListJobsSurfacesDBError(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	closeDB(t, db)

	_, err := ListJobs(context.Background(), db, ListJobsParams{State: "available", Kind: "k", Limit: 5})
	require.Error(t, err, "a closed DB surfaces a list-jobs error")
}

// --- registry.go dispatch decode error --------------------------------------

type decodeArgs struct {
	N int `json:"n"`
}

func (decodeArgs) Kind() string { return "cov.decode" }

type decodeWorker struct{}

func (decodeWorker) Kind() string { return "cov.decode" }
func (decodeWorker) Work(context.Context, *Job[decodeArgs]) (Result, error) {
	return Result{}, nil
}

// TestRegisterDispatchSurfacesDecodeError proves the dispatch closure surfaces a
// JSON decode failure when a job's stored args do not match the worker's typed
// argument struct.
func TestRegisterDispatchSurfacesDecodeError(t *testing.T) {
	t.Parallel()
	reg := NewRegistry()
	Register(reg, decodeWorker{})
	entry, ok := reg.lookup("cov.decode")
	require.True(t, ok)

	_, err := entry.dispatch(context.Background(), dispatchInput{
		Kind:    "cov.decode",
		RawArgs: []byte(`{"n":"not-a-number"}`), // type mismatch
	})
	require.Error(t, err, "a typed-args decode failure surfaces from the dispatch closure")
}

// --- retention.go error branches --------------------------------------------

// TestDeleteFinishedJobsSurfacesFindError proves DeleteFinishedJobs surfaces a
// failure of the find-terminal-ids query inside its transaction. The jobs table
// is dropped (the transaction still begins) so the Pluck fails.
func TestDeleteFinishedJobsSurfacesFindError(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	require.NoError(t, db.Migrator().DropTable(&jobRow{}), "drop jobs so the id find fails")

	_, err := DeleteFinishedJobs(context.Background(), db, time.Now())
	require.ErrorContains(t, err, "find finished jobs", "a find failure surfaces a retention error")
}

// TestDeleteFinishedJobsSurfacesRunDeleteError proves the run-delete step inside
// the transaction surfaces an error once ids have been found: seed a deletable
// terminal job, then drop the job_runs table so the run delete fails while the id
// find succeeds.
func TestDeleteFinishedJobsSurfacesRunDeleteError(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	now := time.Now().UTC().Truncate(time.Second)
	fin := now.Add(-time.Hour)

	seedFinished(t, db, "old-done", StateSucceeded, fin)
	require.NoError(t, db.Migrator().DropTable(&jobRunRow{}), "drop job_runs so the run delete fails")

	_, err := DeleteFinishedJobs(context.Background(), db, now)
	require.Error(t, err, "a failed run delete after ids are found surfaces an error")
}

// --- schedule.go validation and writer error branches -----------------------

// TestPeriodicSpecValidateRejectsSubSecondInterval proves an Every below one
// second is rejected (it would round to a scheduleless row).
func TestPeriodicSpecValidateRejectsSubSecondInterval(t *testing.T) {
	t.Parallel()
	err := PeriodicSpec{Slug: "s", Kind: "k", Every: time.Millisecond}.validate()
	require.Error(t, err, "a sub-second interval is rejected")
	assert.ErrorIs(t, err, ErrValidation)
}

// TestNextFireAfterRejectsBadCron proves nextFireAfter surfaces a cron parse
// error for a malformed expression on the insert path.
func TestNextFireAfterRejectsBadCron(t *testing.T) {
	t.Parallel()
	_, err := nextFireAfter(PeriodicSpec{Cron: "not a cron"}, time.Now())
	require.Error(t, err, "a malformed cron expression is surfaced")
}

// TestUpsertPeriodicSurfacesLoadError proves UpsertPeriodic surfaces a failure of
// the existing-row load that is not a record-not-found miss.
func TestUpsertPeriodicSurfacesLoadError(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	closeDB(t, db)

	err := UpsertPeriodic(context.Background(), db, PeriodicSpec{Slug: "s", Kind: "k", Every: time.Minute})
	require.Error(t, err, "a closed DB surfaces the periodic load error")
}

// TestInsertPeriodicSurfacesCreateError proves the insert path surfaces a create
// failure on a fresh (record-not-found) slug.
func TestInsertPeriodicSurfacesCreateError(t *testing.T) {
	t.Parallel()
	db := newDB(t)

	// The table stays present so the existing-row load is a clean record-not-found
	// miss (taking the insert branch), but a read-only PRAGMA makes the Create
	// fail. Pin a single connection so the PRAGMA holds for the insert.
	sqlDB, err := db.DB()
	require.NoError(t, err)
	sqlDB.SetMaxOpenConns(1)
	require.NoError(t, db.Exec(`PRAGMA query_only = ON`).Error)

	err = UpsertPeriodic(context.Background(), db, PeriodicSpec{Slug: "s", Kind: "k", Every: time.Minute})
	require.ErrorContains(t, err, "insert periodic", "a create failure on a fresh slug is surfaced")
}

// TestUpdatePeriodicSurfacesUpdateError proves the update path surfaces a failure
// of the in-place reconcile UPDATE for an existing slug. The first upsert seeds
// the row (so the load hits it), then a read-only PRAGMA makes the second
// upsert's UPDATE fail.
func TestUpdatePeriodicSurfacesUpdateError(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	ctx := context.Background()

	sqlDB, err := db.DB()
	require.NoError(t, err)
	sqlDB.SetMaxOpenConns(1)
	require.NoError(t, UpsertPeriodic(ctx, db, PeriodicSpec{Slug: "s", Kind: "k", Every: time.Minute, Active: true}))
	require.NoError(t, db.Exec(`PRAGMA query_only = ON`).Error)

	err = UpsertPeriodic(ctx, db, PeriodicSpec{Slug: "s", Kind: "k2", Every: 2 * time.Minute, Active: true})
	require.ErrorContains(t, err, "update periodic", "a failed reconcile UPDATE is surfaced")
}

// TestPeriodicWritersSurfaceDBErrors proves the by-slug writers and the list read
// surface a DB failure.
func TestPeriodicWritersSurfaceDBErrors(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	closeDB(t, db)
	ctx := context.Background()

	assert.Error(t, SetPeriodicActive(ctx, db, "s", true), "SetPeriodicActive surfaces a DB error")
	assert.Error(t, DeletePeriodic(ctx, db, "s"), "DeletePeriodic surfaces a DB error")
	_, listErr := ListPeriodics(ctx, db)
	assert.Error(t, listErr, "ListPeriodics surfaces a DB error")
	assert.Error(t, RetryJob(ctx, db, "id"), "RetryJob surfaces a DB error")
	assert.Error(t, CancelJob(ctx, db, "id"), "CancelJob surfaces a DB error")
}

// TestPeriodicWritersReturnNotFound proves the by-slug/id writers return their
// not-found sentinel when no row matches.
func TestPeriodicWritersReturnNotFound(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	ctx := context.Background()

	assert.ErrorIs(t, SetPeriodicActive(ctx, db, "missing", true), ErrPeriodicNotFound)
	assert.ErrorIs(t, DeletePeriodic(ctx, db, "missing"), ErrPeriodicNotFound)
	assert.ErrorIs(t, RetryJob(ctx, db, "missing"), ErrJobNotFound)
	assert.ErrorIs(t, CancelJob(ctx, db, "missing"), ErrJobNotFound)
}

// --- scheduler.go Tick / fire / enqueueBucket error branches ----------------

// TestSchedulerTickSurfacesLoadError proves Tick surfaces a failure loading the
// due periodic definitions.
func TestSchedulerTickSurfacesLoadError(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	sched := NewScheduler(db, NewClient(db))
	closeDB(t, db)

	_, err := sched.Tick(context.Background())
	require.Error(t, err, "a closed DB surfaces the due-periodics load error")
}

// TestSchedulerFireSurfacesAdvanceError proves fire surfaces the failure of the
// next_run_at advance UPDATE after the buckets are enqueued. fire is called
// directly with a due interval definition; job_periodics is dropped so the
// enqueue (into jobs) succeeds but the periodic-advance UPDATE fails.
func TestSchedulerFireSurfacesAdvanceError(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	now := time.Now().UTC().Truncate(time.Second)
	ctx := clockCtx(context.Background(), NewFixedClock(now))
	sched := NewScheduler(db, NewClient(db))

	require.NoError(t, db.Migrator().DropTable(&jobPeriodicRow{}), "drop job_periodics so only the advance fails")

	secs := 60
	def := jobPeriodicRow{
		ID: NewID(), Slug: "due", Kind: "cov.k", Queue: "periodic",
		ArgsTemplate: datatypes.JSON("{}"), NextRunAt: now.Add(-time.Hour),
		IntervalSeconds: &secs, IsActive: true,
	}
	_, err := sched.fire(ctx, def, now)
	require.ErrorContains(t, err, "advance periodic", "a failed periodic advance is surfaced")
}

// TestSchedulerFireSurfacesCronBucketError proves fire surfaces a cron parse
// failure when a definition's stored cron expression is malformed.
func TestSchedulerFireSurfacesCronBucketError(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	now := time.Now().UTC().Truncate(time.Second)
	ctx := clockCtx(context.Background(), NewFixedClock(now))
	sched := NewScheduler(db, NewClient(db))

	bad := "not a cron"
	def := jobPeriodicRow{
		ID: NewID(), Slug: "c", Kind: "cov.k", Queue: "periodic",
		ArgsTemplate: datatypes.JSON("{}"), NextRunAt: now.Add(-time.Hour),
		CronExpr: &bad, IsActive: true,
	}
	_, err := sched.fire(ctx, def, now)
	require.ErrorContains(t, err, "parse cron", "a malformed stored cron expression aborts fire")
}

// TestSchedulerFireSurfacesEnqueueError proves fire surfaces a failure of the
// per-bucket enqueue. A due interval definition is fired directly with a
// read-only DB so the bucket insert fails.
func TestSchedulerFireSurfacesEnqueueError(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	now := time.Now().UTC().Truncate(time.Second)
	ctx := clockCtx(context.Background(), NewFixedClock(now))
	sched := NewScheduler(db, NewClient(db))

	sqlDB, err := db.DB()
	require.NoError(t, err)
	sqlDB.SetMaxOpenConns(1)
	require.NoError(t, db.Exec(`PRAGMA query_only = ON`).Error)

	secs := 60
	def := jobPeriodicRow{
		ID: NewID(), Slug: "due", Kind: "cov.k", Queue: "periodic",
		ArgsTemplate: datatypes.JSON("{}"), NextRunAt: now.Add(-time.Hour),
		IntervalSeconds: &secs, IsActive: true,
	}
	_, err = sched.fire(ctx, def, now)
	require.ErrorContains(t, err, "enqueue periodic job", "a failed bucket enqueue aborts fire")
}

// TestEnqueueBucketDefaultsEmptyPayload proves enqueueBucket substitutes an empty
// JSON object for a definition with no args template, so the enqueued job carries
// a valid payload.
func TestEnqueueBucketDefaultsEmptyPayload(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	now := time.Now().UTC().Truncate(time.Second)
	ctx := clockCtx(context.Background(), NewFixedClock(now))
	sched := NewScheduler(db, NewClient(db))

	def := jobPeriodicRow{Slug: "e", Kind: "cov.k", Queue: "periodic", ArgsTemplate: nil}
	ok, err := sched.enqueueBucket(ctx, def, now)
	require.NoError(t, err)
	assert.True(t, ok, "the enqueue with a defaulted empty payload succeeds")
}

// TestEnqueueBucketCollisionIsNoOp proves a bucketed unique_key collision is a
// successful no-op (false, nil), the idempotency guarantee for a redundant tick.
func TestEnqueueBucketCollisionIsNoOp(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	now := time.Now().UTC().Truncate(time.Second)
	ctx := clockCtx(context.Background(), NewFixedClock(now))
	sched := NewScheduler(db, NewClient(db))

	def := jobPeriodicRow{
		Slug: "dup", Kind: "cov.k", Queue: "periodic",
		ArgsTemplate: datatypes.JSON("{}"), NextRunAt: now,
	}
	bucket := now.Truncate(time.Second)

	ok, err := sched.enqueueBucket(ctx, def, bucket)
	require.NoError(t, err)
	assert.True(t, ok, "the first enqueue succeeds")

	ok, err = sched.enqueueBucket(ctx, def, bucket)
	require.NoError(t, err)
	assert.False(t, ok, "a redundant enqueue of the same bucket is a no-op")
}

// TestEnqueueBucketSurfacesInsertError proves a non-collision insert failure from
// enqueueBucket is surfaced.
func TestEnqueueBucketSurfacesInsertError(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	now := time.Now().UTC().Truncate(time.Second)
	ctx := clockCtx(context.Background(), NewFixedClock(now))
	sched := NewScheduler(db, NewClient(db))
	closeDB(t, db)

	def := jobPeriodicRow{Slug: "s", Kind: "cov.k", Queue: "periodic", ArgsTemplate: datatypes.JSON("{}")}
	_, err := sched.enqueueBucket(ctx, def, now)
	require.Error(t, err, "a non-collision insert failure is surfaced")
}

// --- runner.go RunUntilIdle error branches ----------------------------------

// TestRunUntilIdleSurfacesPollError proves RunUntilIdle returns the error from a
// failing pollOnce.
func TestRunUntilIdleSurfacesPollError(t *testing.T) {
	t.Parallel()
	fd := &fakeDriver{dequeueErr: errors.New("dequeue boom")}
	r := newFakeRunner(t, fd, 1)

	err := r.RunUntilIdle(context.Background())
	require.ErrorContains(t, err, "dequeue boom", "a poll failure stops the drain loop")
}

// TestRunUntilIdleSurfacesPendingCountError proves RunUntilIdle returns the error
// from a failing pendingCount once the queue has nothing claimable. The fake
// driver claims nothing, and the runner's own DB is closed so pendingCount fails.
func TestRunUntilIdleSurfacesPendingCountError(t *testing.T) {
	t.Parallel()
	fd := &fakeDriver{} // serves an empty batch, claims nothing
	r := newFakeRunner(t, fd, 1)
	closeDB(t, r.cfg.DB)

	err := r.RunUntilIdle(context.Background())
	require.Error(t, err, "a pendingCount failure stops the drain loop")
}

// TestRunUntilIdleStopsWhenContextCancelledDuringWait proves RunUntilIdle returns
// a context error when ctx is cancelled while it is waiting out the poll interval
// for a not-yet-claimable (future-scheduled) pending job.
func TestRunUntilIdleStopsWhenContextCancelledDuringWait(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	reg := NewRegistry()
	Register(reg, &successWorker{})
	r := newRunner(t, db, reg)
	r.cfg.PollInterval = 200 * time.Millisecond

	// A pending job scheduled an hour out is never claimable, so RunUntilIdle
	// enters its wait-and-retry branch where the cancel lands.
	future := time.Now().Add(time.Hour)
	_, err := Insert(context.Background(), NewClient(db), successArgs{V: "x"}, InsertOpts{ScheduleAt: &future})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(40 * time.Millisecond)
		cancel()
	}()
	err = r.RunUntilIdle(ctx)
	require.ErrorIs(t, err, context.Canceled, "a cancel during the backoff wait stops the drain loop")
}

// --- node.go component-error / health-listen branches -----------------------

// TestNodeRunSurfacesHealthListenError proves a Node whose health server cannot
// bind its address fails fast: serveHealth returns the listen error, the fail
// closure records it and tears down the siblings, and Run returns that first
// error through drainErrors.
func TestNodeRunSurfacesHealthListenError(t *testing.T) {
	t.Parallel()
	db := newWALFileDB(t)
	reg := NewRegistry()
	Register(reg, &successWorker{})

	node, err := NewNode(NodeConfig{
		Runners: []RunnerConfig{{
			DB: db, Driver: NewSQLiteDriver(db), Registry: reg,
			Queues: []string{"default"}, ExecutorClass: "local", Concurrency: 1,
			PollInterval: time.Hour, // keep the runner quiet so the health error is the first
		}},
		Health: HealthConfig{Addr: "256.256.256.256:99999"}, // an unbindable address
	})
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err = node.Run(ctx)
	require.ErrorContains(t, err, "health server", "the health listen failure is surfaced as the node's first error")
}

// --- observer.go default no-op methods --------------------------------------

// TestNoopObserverMethodsAreInert proves the default Observer's methods run
// without panicking — the dispatch hot path relies on them being safe no-ops.
func TestNoopObserverMethodsAreInert(t *testing.T) {
	t.Parallel()
	var o noopObserver
	ctx := context.Background()
	o.OnClaim(ctx, ClaimEvent{})
	o.OnStart(ctx, JobEvent{})
	o.OnFinish(ctx, FinishEvent{})
	o.OnRetry(ctx, RetryEvent{})
}
