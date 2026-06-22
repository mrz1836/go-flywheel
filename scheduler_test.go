package flywheel

import (
	"context"
	"testing"
	"time"

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
		NewID(), slug, kind, "{}", "periodic", defaultPeriodicIntervalSeconds, nextRunAt, active,
		time.Now().UTC(), time.Now().UTC(),
	).Error)
}

func installPeriodicCron(t *testing.T, db *gorm.DB, slug, kind, cronExpr string, nextRunAt time.Time) {
	t.Helper()
	require.NoError(t, db.Exec(
		`INSERT INTO job_periodics(id, slug, kind, args_template, queue, cron_expr, next_run_at, is_active, created_at, updated_at)
		 VALUES (?,?,?,?,?,?,?,?,?,?)`,
		NewID(), slug, kind, "{}", "periodic", cronExpr, nextRunAt, true,
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
	sched := NewScheduler(db, NewClient(db))

	// Anchor a fixed clock so the test is unaffected by wall-clock drift /
	// stored-text-vs-time-Time round-trip subtleties.
	now := time.Now().UTC().Truncate(time.Second)
	ctx := clockCtx(context.Background(), NewFixedClock(now))

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
	sched := NewScheduler(db, NewClient(db))

	now := time.Now().UTC().Truncate(time.Second)
	ctx := clockCtx(context.Background(), NewFixedClock(now))

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
	sched := NewScheduler(db, NewClient(db))

	now := time.Now().UTC().Truncate(time.Second)
	ctx := clockCtx(context.Background(), NewFixedClock(now))

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
	sched := NewScheduler(db, NewClient(db))

	now := time.Now().UTC().Truncate(time.Second)
	ctx := clockCtx(context.Background(), NewFixedClock(now))

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
	sched := NewScheduler(db, NewClient(db))

	now := time.Now().UTC().Truncate(time.Second)
	ctx := clockCtx(context.Background(), NewFixedClock(now))

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
	sched := NewScheduler(db, NewClient(db))

	now := time.Now().UTC()
	pastLease := now.Add(-time.Minute)
	jobID := NewID()
	require.NoError(t, db.Exec(
		`INSERT INTO jobs(id, kind, queue, args, priority, state, attempt, max_attempts, scheduled_at, executor_class, leased_until, tags, created_at, updated_at, metadata)
		 VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		jobID, "test.k", "default", "{}", 100, string(StateRunning), 1, 25, now, string(AnyClass), pastLease, "[]", now, now, "{}",
	).Error)
	require.NoError(t, db.Exec(
		`INSERT INTO job_runs(id, job_id, attempt, executor_class, executor_id, started_at, outcome, created_at)
		 VALUES (?,?,?,?,?,?,?,?)`,
		NewID(), jobID, 1, "local", "h1", now.Add(-time.Hour), string(OutcomeStarted), now.Add(-time.Hour),
	).Error)

	reclaimed, err := sched.Sweep(clockCtx(context.Background(), NewFixedClock(now)))
	require.NoError(t, err)
	assert.Equal(t, 1, reclaimed)
}
