package core

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"testing"
	"time"

	"github.com/mrz1836/go-foundation/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

// defaultPeriodicIntervalSeconds is the every-minute cadence the scheduler
// tests use across the board.
const defaultPeriodicIntervalSeconds = 60

// installPeriodic seeds a JobPeriodic row directly, bypassing the
// work-context BeforeSave hook. The scheduler reads job_periodics rows so
// this is the minimum setup needed.
func installPeriodic(t *testing.T, db *gorm.DB, slug, kind string, nextRunAt time.Time, active bool) {
	t.Helper()
	require.NoError(t, db.Exec(
		`INSERT INTO job_periodics(id, slug, kind, args_template, queue, interval_seconds, next_run_at, is_active, created_at, updated_at)
		 VALUES (?,?,?,?,?,?,?,?,?,?)`,
		models.NewID(), slug, kind, "{}", "periodic", defaultPeriodicIntervalSeconds, nextRunAt, active,
		time.Now().UTC(), time.Now().UTC(),
	).Error)
}

func installPeriodicCron(t *testing.T, db *gorm.DB, slug, kind, cronExpr string, nextRunAt time.Time) {
	t.Helper()
	require.NoError(t, db.Exec(
		`INSERT INTO job_periodics(id, slug, kind, args_template, queue, cron_expr, next_run_at, is_active, created_at, updated_at)
		 VALUES (?,?,?,?,?,?,?,?,?,?)`,
		models.NewID(), slug, kind, "{}", "periodic", cronExpr, nextRunAt, true,
		time.Now().UTC(), time.Now().UTC(),
	).Error)
}

func jobCount(t *testing.T, db *gorm.DB, kind string) int64 {
	t.Helper()
	var n int64
	require.NoError(t, db.Table("jobs").Where("kind = ?", kind).Count(&n).Error)
	return n
}

func TestSchedulerTickFiresIntervalAndAdvancesNextRunAt(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	sched := newScheduler(t, db)

	// Anchor a fixed clock so the test is unaffected by wall-clock drift /
	// stored-text-vs-time-Time round-trip subtleties.
	now := time.Now().UTC().Truncate(time.Second)
	ctx := clockCtx(context.Background(), models.NewFixedClock(now))

	due := now.Add(-time.Minute)
	installPeriodic(t, db, "tick-interval", "test.interval", due, true)

	enqueued, err := sched.Tick(ctx)
	require.NoError(t, err)
	assert.Positive(t, enqueued, "an interval that has been due for a minute must fire at least once")
	assert.Positive(t, jobCount(t, db, "test.interval"))

	var nextRunAt time.Time
	require.NoError(t, db.Table("job_periodics").
		Select("next_run_at").Where("slug = ?", "tick-interval").Scan(&nextRunAt).Error)
	assert.True(t, nextRunAt.After(now), "NextRunAt must be advanced past now after firing")
}

func TestSchedulerTickFiresCron(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	sched := newScheduler(t, db)

	now := time.Now().UTC().Truncate(time.Second)
	ctx := clockCtx(context.Background(), models.NewFixedClock(now))

	due := now.Add(-2 * time.Minute)
	installPeriodicCron(t, db, "tick-cron", "test.cron", "*/1 * * * *", due)

	enqueued, err := sched.Tick(ctx)
	require.NoError(t, err)
	assert.Positive(t, enqueued, "an every-minute cron with 2 minutes of slack must fire at least once")
	assert.Positive(t, jobCount(t, db, "test.cron"))
}

func TestSchedulerTickIdempotentOnRepeatTick(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	sched := newScheduler(t, db)

	now := time.Now().UTC().Truncate(time.Second)
	ctx := clockCtx(context.Background(), models.NewFixedClock(now))

	due := now.Add(-time.Minute)
	installPeriodic(t, db, "tick-idem", "test.idem", due, true)

	_, err := sched.Tick(ctx)
	require.NoError(t, err)
	first := jobCount(t, db, "test.idem")
	require.Positive(t, first)

	// Tick again at the same clock instant — no new job should land because
	// next_run_at advanced past now in the first tick.
	_, err = sched.Tick(ctx)
	require.NoError(t, err)
	second := jobCount(t, db, "test.idem")
	assert.Equal(t, first, second, "a repeat tick at the same clock instant must be a no-op")
}

func TestSchedulerTickSkipsInactiveDefinitions(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	sched := newScheduler(t, db)

	now := time.Now().UTC().Truncate(time.Second)
	ctx := clockCtx(context.Background(), models.NewFixedClock(now))

	due := now.Add(-time.Minute)
	installPeriodic(t, db, "tick-inactive", "test.inactive", due, false)

	enqueued, err := sched.Tick(ctx)
	require.NoError(t, err)
	assert.Zero(t, enqueued)
	assert.Zero(t, jobCount(t, db, "test.inactive"))
}

func TestSchedulerTickBackfillsAtMostBackfillCapJobs(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	sched := newScheduler(t, db)

	now := time.Now().UTC().Truncate(time.Second)
	ctx := clockCtx(context.Background(), models.NewFixedClock(now))

	// 60s interval, due-time was 1 hour ago = 60 missed buckets. The default
	// backfill cap is 10.
	due := now.Add(-time.Hour)
	installPeriodic(t, db, "tick-backfill", "test.backfill", due, true)

	enqueued, err := sched.Tick(ctx)
	require.NoError(t, err)
	assert.LessOrEqual(t, enqueued, defaultBackfillCap, "backfill is capped, not unbounded")
	assert.LessOrEqual(t, jobCount(t, db, "test.backfill"), int64(defaultBackfillCap))
}

func TestSchedulerSweepReclaimsExpiredLeases(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	sched := newScheduler(t, db)

	now := time.Now().UTC()
	pastLease := now.Add(-time.Minute)
	jobID := models.NewID()
	require.NoError(t, db.Exec(
		`INSERT INTO jobs(id, kind, queue, args, priority, state, attempt, max_attempts, scheduled_at, executor_class, leased_until, tags, created_at, updated_at, metadata)
		 VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		jobID, "test.k", "default", "{}", 100, string(StateRunning), 1, 25, now, string(AnyClass), pastLease, "[]", now, now, "{}",
	).Error)
	require.NoError(t, db.Exec(
		`INSERT INTO job_runs(id, job_id, attempt, executor_class, executor_id, started_at, outcome, created_at)
		 VALUES (?,?,?,?,?,?,?,?)`,
		models.NewID(), jobID, 1, "local", "h1", now.Add(-time.Hour), string(OutcomeStarted), now.Add(-time.Hour),
	).Error)

	reclaimed, err := sched.Sweep(clockCtx(context.Background(), models.NewFixedClock(now)))
	require.NoError(t, err)
	assert.Equal(t, 1, reclaimed)
}

// TestSchedulerSweepFiresOnSweep proves the scheduler reports each sweep pass
// through the configured Observer, carrying the reclaimed count so sweep timing
// joins the same telemetry stream as claims and finalizes.
func TestSchedulerSweepFiresOnSweep(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	obs := &recordingObserver{}
	sched := newSchedulerCfg(t, SchedulerConfig{DB: db, Client: NewClient(db), Observer: obs})

	now := time.Now().UTC()
	jobID := models.NewID()
	require.NoError(t, db.Exec(
		`INSERT INTO jobs(id, kind, queue, args, priority, state, attempt, max_attempts, scheduled_at, executor_class, leased_until, tags, created_at, updated_at, metadata)
		 VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		jobID, "test.k", "default", "{}", 100, string(StateRunning), 1, 25, now, string(AnyClass), now.Add(-time.Minute), "[]", now, now, "{}",
	).Error)
	require.NoError(t, db.Exec(
		`INSERT INTO job_runs(id, job_id, attempt, executor_class, executor_id, started_at, outcome, created_at)
		 VALUES (?,?,?,?,?,?,?,?)`,
		models.NewID(), jobID, 1, "local", "h1", now.Add(-time.Hour), string(OutcomeStarted), now.Add(-time.Hour),
	).Error)

	reclaimed, err := sched.Sweep(clockCtx(context.Background(), models.NewFixedClock(now)))
	require.NoError(t, err)
	require.Equal(t, 1, reclaimed)

	sweeps := obs.snapshotSweeps()
	require.Len(t, sweeps, 1, "one pass fires exactly one OnSweep")
	assert.Equal(t, 1, sweeps[0].Reclaimed, "the event carries the reclaimed count")
}

// --- logMaintenanceError: suppress the shutdown, log the rest ----------------

// TestLogMaintenanceErrorSuppressesShutdownErrors proves a cancellation or
// deadline — what a clean drain looks like from inside a maintenance activity —
// is swallowed, while any other failure is logged. Without this every ordinary
// shutdown would gain an error line the unbounded implementation never produced.
func TestLogMaintenanceErrorSuppressesShutdownErrors(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	sched := newScheduler(t, db)
	h := &captureHandler{}
	sched.logger = slog.New(h)
	ctx := context.Background()

	sched.logMaintenanceError(ctx, "jobs: suppressed cancel", context.Canceled)
	sched.logMaintenanceError(ctx, "jobs: suppressed deadline", context.DeadlineExceeded)
	sched.logMaintenanceError(ctx, "jobs: suppressed wrapped", fmt.Errorf("sweep: %w", context.Canceled))
	assert.Empty(t, h.messages, "a cancelled or deadline-exceeded maintenance error is not logged")

	sched.logMaintenanceError(ctx, "jobs: real failure", errors.New("disk full"))
	require.Len(t, h.recordsFor("jobs: real failure"), 1, "a genuine failure is logged")
	assert.Equal(t, "disk full", h.recordsFor("jobs: real failure")[0].attrs["error"].String())
}

// --- intervalBuckets / cronBuckets: bounded allocation, exact buckets --------

// TestIntervalBucketsGuardsAndBounds pins the closed-form skip-ahead: a
// non-positive interval and a start after now do no work, a normal run returns
// exactly the missed fires and the next, and a run past the cap keeps only the
// most recent limit while still reporting the true next fire.
func TestIntervalBucketsGuardsAndBounds(t *testing.T) {
	t.Parallel()
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	at := func(n int) time.Time { return t0.Add(time.Duration(n) * time.Minute) }

	t.Run("non-positive interval does no work", func(t *testing.T) {
		t.Parallel()
		buckets, next := intervalBuckets(t0, 0, at(60), 5)
		assert.Empty(t, buckets, "a zero interval has no valid schedule and cannot loop")
		assert.Equal(t, t0, next)
	})
	t.Run("start after now has no missed fire", func(t *testing.T) {
		t.Parallel()
		buckets, next := intervalBuckets(at(60), 60, t0, 5)
		assert.Empty(t, buckets)
		assert.Equal(t, at(60), next, "the first fire is start itself, still in the future")
	})
	t.Run("under the cap returns every missed fire", func(t *testing.T) {
		t.Parallel()
		buckets, next := intervalBuckets(t0, 60, at(3), 10)
		assert.Equal(t, []time.Time{t0, at(1), at(2), at(3)}, buckets)
		assert.Equal(t, at(4), next, "the next fire is strictly after now")
	})
	t.Run("past the cap keeps only the most recent limit", func(t *testing.T) {
		t.Parallel()
		buckets, next := intervalBuckets(t0, 60, at(60), 10)
		require.Len(t, buckets, 10, "allocation is bounded by limit, not the 61-fire history")
		assert.Equal(t, at(51), buckets[0], "the oldest kept fire is now-limit+1")
		assert.Equal(t, at(60), buckets[9], "the newest kept fire is the one at now")
		assert.Equal(t, at(61), next)
	})
	t.Run("a non-positive limit is unbounded", func(t *testing.T) {
		t.Parallel()
		buckets, next := intervalBuckets(t0, 60, at(3), 0)
		assert.Equal(t, []time.Time{t0, at(1), at(2), at(3)}, buckets)
		assert.Equal(t, at(4), next)
	})
}

// TestCronBucketsRingBufferBounds pins the ring-buffer cap: the unbounded branch
// returns every missed fire, a capped run keeps only the most recent limit in
// order, and a malformed expression is surfaced.
func TestCronBucketsRingBufferBounds(t *testing.T) {
	t.Parallel()
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	at := func(n int) time.Time { return t0.Add(time.Duration(n) * time.Minute) }

	t.Run("a non-positive limit returns every fire", func(t *testing.T) {
		t.Parallel()
		buckets, next, err := cronBuckets("* * * * *", t0, at(3), 0)
		require.NoError(t, err)
		assert.Equal(t, []time.Time{t0, at(1), at(2), at(3)}, buckets)
		assert.Equal(t, at(4), next)
	})
	t.Run("the ring keeps only the most recent limit, in order", func(t *testing.T) {
		t.Parallel()
		buckets, next, err := cronBuckets("* * * * *", t0, at(3), 2)
		require.NoError(t, err)
		assert.Equal(t, []time.Time{at(2), at(3)}, buckets, "the two most recent fires, oldest first")
		assert.Equal(t, at(4), next, "next is the true next fire, not one the ring dropped")
	})
	t.Run("a limit above the fire count returns them all", func(t *testing.T) {
		t.Parallel()
		buckets, _, err := cronBuckets("* * * * *", t0, at(3), 10)
		require.NoError(t, err)
		assert.Equal(t, []time.Time{t0, at(1), at(2), at(3)}, buckets)
	})
	t.Run("a malformed expression is surfaced", func(t *testing.T) {
		t.Parallel()
		_, _, err := cronBuckets("not a cron", t0, at(3), 10)
		require.ErrorContains(t, err, "parse cron")
	})
}

// TestTickOnceAndSweepOnceLogErrors proves the maintenance-activity wrappers log a
// failure and carry on rather than propagating it, so a transient fault does not
// stop the scheduler loop.
func TestTickOnceAndSweepOnceLogErrors(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	sched := newScheduler(t, db)
	h := &captureHandler{}
	sched.logger = slog.New(h)
	closeDB(t, db)
	ctx := context.Background()

	sched.tickOnce(ctx)
	assert.True(t, h.has("jobs: periodic tick failed"), "a failed tick is logged, not propagated")

	sched.sweepOnce(ctx)
	assert.True(t, h.has("jobs: lease sweep failed"), "a failed sweep is logged, not propagated")
}
