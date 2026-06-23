package flywheel

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

// healthAnchor is the fixed clock instant the queue-health and failures tests
// seed and sample against, so past/future scheduled_at and the lag are exact.
//
//nolint:gochecknoglobals // shared deterministic test anchor
var healthAnchor = time.Date(2026, 6, 22, 12, 0, 0, 0, time.UTC)

// ptrTime returns a pointer to t for seeding nullable time columns.
func ptrTime(t time.Time) *time.Time { return &t }

// seedDiscarded seeds a terminal discarded job finalized at finalizedAt.
func seedDiscarded(t *testing.T, db *gorm.DB, id string, finalizedAt time.Time, attempt int) {
	t.Helper()
	seedJob(t, db, jobRow{
		ID: id, Kind: "k", Queue: "default", State: string(StateDiscarded),
		Attempt: attempt, ScheduledAt: finalizedAt, FinalizedAt: ptrTime(finalizedAt),
		CreatedAt: finalizedAt, UpdatedAt: finalizedAt,
	})
}

// seedRunErr seeds a failed job_runs row carrying an error class and message.
func seedRunErr(t *testing.T, db *gorm.DB, id, jobID string, attempt int, class ErrorClass, msg string) {
	t.Helper()
	c := string(class)
	m := msg
	seedRun(t, db, jobRunRow{
		ID: id, JobID: jobID, Attempt: attempt, ExecutorID: "h",
		StartedAt: healthAnchor, Outcome: string(OutcomeError),
		ErrorClass: &c, ErrorMessage: &m, CreatedAt: healthAnchor,
	})
}

// assertQueueHealthSnapshot seeds a known mix of jobs across states with past and
// future scheduled_at and asserts every QueueHealth field — counts, ready,
// in-flight, scheduled-ahead, and the oldest-ready lag — plus that a soft-deleted
// job is excluded from all of them. Shared by the SQLite and Postgres suites so
// both dialects prove the gauge.
func assertQueueHealthSnapshot(t *testing.T, db *gorm.DB) {
	t.Helper()
	now := healthAnchor
	ctx := clockCtx(context.Background(), NewFixedClock(now))

	// Ready: claimable and due. qh-ready-old is the oldest, so it sets the lag.
	seedJob(t, db, jobRow{ID: "qh-ready-old", Kind: "k", State: string(StateAvailable), ScheduledAt: now.Add(-10 * time.Minute)})
	seedJob(t, db, jobRow{ID: "qh-ready-new", Kind: "k", State: string(StateRetryable), ScheduledAt: now.Add(-1 * time.Minute)})
	// Scheduled ahead: claimable but not yet due.
	seedJob(t, db, jobRow{ID: "qh-ahead", Kind: "k", State: string(StateScheduled), ScheduledAt: now.Add(30 * time.Minute)})
	// In flight: running.
	seedJob(t, db, jobRow{ID: "qh-running", Kind: "k", State: string(StateRunning), ScheduledAt: now.Add(-5 * time.Minute)})
	// Terminal: counted in CountsByState only.
	seedJob(t, db, jobRow{ID: "qh-done", Kind: "k", State: string(StateSucceeded), ScheduledAt: now.Add(-time.Hour)})
	seedJob(t, db, jobRow{ID: "qh-discarded", Kind: "k", State: string(StateDiscarded), ScheduledAt: now.Add(-time.Hour)})
	// Soft-deleted but otherwise the oldest ready job: must be excluded everywhere.
	seedJob(t, db, jobRow{ID: "qh-deleted", Kind: "k", State: string(StateAvailable), ScheduledAt: now.Add(-2 * time.Hour)})
	require.NoError(t, db.Where("id = ?", "qh-deleted").Delete(&jobRow{}).Error)

	qh, err := SampleQueueHealth(ctx, db)
	require.NoError(t, err)

	assert.EqualValues(t, 2, qh.Ready, "two claimable-and-due jobs are ready")
	assert.EqualValues(t, 1, qh.InFlight, "one running job is in flight")
	assert.EqualValues(t, 1, qh.ScheduledAhead, "one claimable job is not yet due")
	assert.Equal(t, 10*time.Minute, qh.OldestReadyAge, "lag is now minus the oldest ready scheduled_at")
	assert.True(t, qh.SampledAt.Equal(now), "the snapshot is stamped with the context clock")

	assert.EqualValues(t, 1, qh.CountsByState[string(StateAvailable)])
	assert.EqualValues(t, 1, qh.CountsByState[string(StateRetryable)])
	assert.EqualValues(t, 1, qh.CountsByState[string(StateScheduled)])
	assert.EqualValues(t, 1, qh.CountsByState[string(StateRunning)])
	assert.EqualValues(t, 1, qh.CountsByState[string(StateSucceeded)])
	assert.EqualValues(t, 1, qh.CountsByState[string(StateDiscarded)])

	var total int64
	for _, n := range qh.CountsByState {
		total += n
	}
	assert.EqualValues(t, 6, total, "the soft-deleted job is excluded from the counts")
}

// assertQueueHealthEmpty asserts the zero-value snapshot of an empty queue.
func assertQueueHealthEmpty(t *testing.T, db *gorm.DB) {
	t.Helper()
	now := healthAnchor
	ctx := clockCtx(context.Background(), NewFixedClock(now))

	qh, err := SampleQueueHealth(ctx, db)
	require.NoError(t, err)
	assert.Empty(t, qh.CountsByState, "no states present on an empty queue")
	assert.Zero(t, qh.Ready)
	assert.Zero(t, qh.InFlight)
	assert.Zero(t, qh.ScheduledAhead)
	assert.Zero(t, qh.OldestReadyAge, "no ready jobs means zero lag, not a negative age")
	assert.True(t, qh.SampledAt.Equal(now))
}

// assertRecentFailures seeds discarded and non-discarded jobs with runs and
// asserts RecentFailures returns only the in-window discarded jobs, newest first,
// each carrying the error from its final attempt, with the limit honored. Shared
// by the SQLite and Postgres suites.
func assertRecentFailures(t *testing.T, db *gorm.DB) {
	t.Helper()
	ctx := context.Background()
	now := healthAnchor

	// d1: discarded 1h ago over two attempts; the second (permanent) ended it.
	seedDiscarded(t, db, "d1", now.Add(-time.Hour), 2)
	seedRunErr(t, db, "d1-r1", "d1", 1, ErrorTransient, "boom transient")
	seedRunErr(t, db, "d1-r2", "d1", 2, ErrorPermanent, "boom final")
	// d2: discarded 30m ago on its first attempt.
	seedDiscarded(t, db, "d2", now.Add(-30*time.Minute), 1)
	seedRunErr(t, db, "d2-r1", "d2", 1, ErrorPermanent, "kaboom")
	// d3: discarded 48h ago — outside the 24h window.
	seedDiscarded(t, db, "d3", now.Add(-48*time.Hour), 1)
	seedRunErr(t, db, "d3-r1", "d3", 1, ErrorPermanent, "stale")
	// s1: succeeded — never a failure.
	seedJob(t, db, jobRow{ID: "s1", Kind: "k", State: string(StateSucceeded), ScheduledAt: now, FinalizedAt: ptrTime(now.Add(-10 * time.Minute))})

	since := now.Add(-24 * time.Hour)
	fails, err := RecentFailures(ctx, db, RecentFailuresParams{Since: since, Limit: 50})
	require.NoError(t, err)
	require.Len(t, fails, 2, "only the two in-window discarded jobs are returned")

	// Newest finalized first: d2 (30m ago) then d1 (1h ago).
	assert.Equal(t, "d2", fails[0].JobID)
	assert.Equal(t, "default", fails[0].Queue)
	assert.Equal(t, string(ErrorPermanent), fails[0].ErrorClass)
	assert.Equal(t, "kaboom", fails[0].ErrorMessage)
	assert.Equal(t, 1, fails[0].Attempt)
	assert.True(t, fails[0].FinalizedAt.Equal(now.Add(-30*time.Minute)))

	assert.Equal(t, "d1", fails[1].JobID)
	assert.Equal(t, string(ErrorPermanent), fails[1].ErrorClass, "the final attempt's class wins over the earlier transient one")
	assert.Equal(t, "boom final", fails[1].ErrorMessage)
	assert.Equal(t, 2, fails[1].Attempt, "the attempt number is the final recorded attempt")

	limited, err := RecentFailures(ctx, db, RecentFailuresParams{Since: since, Limit: 1})
	require.NoError(t, err)
	require.Len(t, limited, 1, "the limit caps the page")
	assert.Equal(t, "d2", limited[0].JobID, "the newest failure is kept under the limit")
}

func TestSampleQueueHealthSnapshotSQLite(t *testing.T) {
	t.Parallel()
	assertQueueHealthSnapshot(t, newDB(t))
}

func TestSampleQueueHealthEmptySQLite(t *testing.T) {
	t.Parallel()
	assertQueueHealthEmpty(t, newDB(t))
}

func TestRecentFailuresSQLite(t *testing.T) {
	t.Parallel()
	assertRecentFailures(t, newDB(t))
}

func TestRecentFailuresJobWithoutRunsHasEmptyError(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	ctx := context.Background()
	// A discarded job with no job_runs row (e.g. a hand-cancelled-to-discarded
	// edge) falls back to the job's own attempt and carries no error fields.
	seedDiscarded(t, db, "no-runs", healthAnchor.Add(-time.Minute), 3)

	fails, err := RecentFailures(ctx, db, RecentFailuresParams{Since: healthAnchor.Add(-time.Hour), Limit: 10})
	require.NoError(t, err)
	require.Len(t, fails, 1)
	assert.Equal(t, "no-runs", fails[0].JobID)
	assert.Equal(t, 3, fails[0].Attempt, "the job's own attempt is used when no run exists")
	assert.Empty(t, fails[0].ErrorClass)
	assert.Empty(t, fails[0].ErrorMessage)
}

func TestRecentFailuresZeroSinceHasNoLowerBound(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	ctx := context.Background()
	// Two discarded jobs far apart in time; a zero Since returns both.
	seedDiscarded(t, db, "ancient", healthAnchor.Add(-365*24*time.Hour), 1)
	seedDiscarded(t, db, "recent", healthAnchor.Add(-time.Minute), 1)

	fails, err := RecentFailures(ctx, db, RecentFailuresParams{Limit: 10})
	require.NoError(t, err)
	require.Len(t, fails, 2, "a zero Since applies no lower bound")
	assert.Equal(t, "recent", fails[0].JobID, "still newest finalized first")
	assert.Equal(t, "ancient", fails[1].JobID)
}

func TestRecentFailuresEmptyReturnsEmptySlice(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	fails, err := RecentFailures(context.Background(), db, RecentFailuresParams{Since: healthAnchor})
	require.NoError(t, err)
	assert.NotNil(t, fails, "an empty result is a non-nil empty slice")
	assert.Empty(t, fails)
}

func TestRecentFailuresExcludesSoftDeleted(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	seedDiscarded(t, db, "soft", healthAnchor.Add(-time.Minute), 1)
	require.NoError(t, db.Where("id = ?", "soft").Delete(&jobRow{}).Error)

	fails, err := RecentFailures(context.Background(), db, RecentFailuresParams{Since: healthAnchor.Add(-time.Hour)})
	require.NoError(t, err)
	assert.Empty(t, fails, "a soft-deleted discarded job is excluded")
}

func TestSampleQueueHealthJustDueJobHasZeroLag(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	now := healthAnchor
	ctx := clockCtx(context.Background(), NewFixedClock(now))
	// A job whose scheduled_at is exactly now is ready, but its lag is zero — it
	// just became claimable, it has not fallen behind.
	seedJob(t, db, jobRow{ID: "just-due", Kind: "k", State: string(StateAvailable), ScheduledAt: now})

	qh, err := SampleQueueHealth(ctx, db)
	require.NoError(t, err)
	assert.EqualValues(t, 1, qh.Ready, "a job due exactly now is ready")
	assert.Zero(t, qh.OldestReadyAge, "a just-due job contributes zero lag, not a negative age")
}

func TestSampleQueueHealthErrorsOnClosedDB(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	sqlDB, err := db.DB()
	require.NoError(t, err)
	require.NoError(t, sqlDB.Close())

	_, err = SampleQueueHealth(context.Background(), db)
	require.Error(t, err, "a broken database surfaces as an error, not a partial snapshot")
}

func TestRecentFailuresErrorsOnClosedDB(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	sqlDB, err := db.DB()
	require.NoError(t, err)
	require.NoError(t, sqlDB.Close())

	_, err = RecentFailures(context.Background(), db, RecentFailuresParams{})
	require.Error(t, err)
}

func TestSampleQueueHealthSurfacesReadyQueryError(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	// Drop scheduled_at so the by-state count still succeeds but the ready query —
	// which filters on scheduled_at — errors, proving a mid-snapshot query failure
	// is surfaced rather than yielding a half-built gauge.
	require.NoError(t, db.Exec("ALTER TABLE jobs DROP COLUMN scheduled_at").Error)

	_, err := SampleQueueHealth(context.Background(), db)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "ready", "the failing query is named in the error")
}
