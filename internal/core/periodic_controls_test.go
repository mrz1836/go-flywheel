package core

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/mrz1836/go-foundation/models"
	"github.com/robfig/cron/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

// pausedMessage is the line a start logs for each declared schedule it finds
// paused.
const pausedMessage = "jobs: declared periodic is paused"

// pauseClock returns a function that builds a context whose clock reads base
// plus an offset, so a test steps through time by offsets from one instant.
func pauseClock(base time.Time) func(time.Duration) context.Context {
	return func(d time.Duration) context.Context {
		return clockCtx(context.Background(), models.NewFixedClock(base.Add(d)))
	}
}

// mustPeriodic reads slug's job_periodics row, failing when there is none.
func mustPeriodic(t testing.TB, db *gorm.DB, slug string) jobPeriodicRow {
	t.Helper()
	row, found := periodicRowBySlug(t, db, slug)
	require.True(t, found, "%s has a row", slug)
	return row
}

// tickingScheduler builds a Scheduler over db, with the Driver its dialect
// selects, for a test that drives Tick directly.
func tickingScheduler(t *testing.T, db *gorm.DB) *Scheduler {
	t.Helper()
	return newSchedulerCfg(t, SchedulerConfig{DB: db, Client: NewClient(db), Driver: driverForTest(t, db)})
}

// pausedRedeclaredSuite is the issue's scenario: a schedule a host declares in
// code is paused at run time, and the host restarts — with the same
// declaration, then with its cadence, kind, queue, and args changed. Each start
// applies the declaration and keeps the pause, and the schedule fires at no due
// time.
func pausedRedeclaredSuite(t *testing.T, db *gorm.DB) {
	t.Helper()
	t0 := time.Now().UTC().Truncate(time.Second)
	at := pauseClock(t0)
	spec := PeriodicSpec{Slug: "nightly", Kind: "test.nightly", Every: time.Hour, Active: true}

	declaringScheduler(t, db, &captureHandler{}, 0, spec).applyDeclaredPeriodics(at(0))
	require.NoError(t, PausePeriodic(at(time.Minute), db, "nightly"))

	restart := declaringScheduler(t, db, &captureHandler{}, 0, spec)
	restart.applyDeclaredPeriodics(at(2 * time.Minute))
	row := mustPeriodic(t, db, "nightly")
	require.NotNil(t, row.PausedAt, "a restart that re-declares the schedule keeps its pause")
	assert.True(t, row.PausedAt.Equal(t0.Add(time.Minute)), "and when it was paused")
	require.NotNil(t, row.IsActive)
	assert.True(t, *row.IsActive, "the declaration's Active is applied beside it")
	n, err := restart.Tick(at(time.Hour + time.Minute))
	require.NoError(t, err)
	assert.Zero(t, n, "a paused schedule does not fire at its due time")

	changed := spec
	changed.Kind, changed.Queue, changed.Every = "test.nightly.v2", "reports", 2*time.Hour
	changed.ArgsTemplate = []byte(`{"v":2}`)
	again := declaringScheduler(t, db, &captureHandler{}, 0, changed)
	again.applyDeclaredPeriodics(at(3 * time.Hour))
	row = mustPeriodic(t, db, "nightly")
	require.NotNil(t, row.IntervalSeconds)
	assert.Equal(t, 7200, *row.IntervalSeconds, "the changed cadence is applied")
	assert.True(t, row.NextRunAt.Equal(t0.Add(5*time.Hour)), "from the start that changed it")
	assert.Equal(t, "test.nightly.v2", row.Kind)
	assert.Equal(t, "reports", row.Queue)
	assert.JSONEq(t, `{"v":2}`, string(row.ArgsTemplate))
	require.NotNil(t, row.PausedAt, "and the schedule stays paused")
	assert.True(t, row.PausedAt.Equal(t0.Add(time.Minute)))
	n, err = again.Tick(at(6 * time.Hour))
	require.NoError(t, err)
	assert.Zero(t, n)
	assert.Zero(t, jobCount(t, db, "test.nightly")+jobCount(t, db, "test.nightly.v2"), "nothing fired")
}

// TestAPausedScheduleStaysPausedWhenRedeclared runs the scenario on SQLite.
func TestAPausedScheduleStaysPausedWhenRedeclared(t *testing.T) {
	t.Parallel()
	pausedRedeclaredSuite(t, newDB(t))
}

// pausedDoesNotFireSuite proves the tick passes over a paused schedule and
// leaves its cursor where the pause found it.
func pausedDoesNotFireSuite(t *testing.T, db *gorm.DB) {
	t.Helper()
	t0 := time.Now().UTC().Truncate(time.Second)
	at := pauseClock(t0)
	require.NoError(t, UpsertPeriodic(at(0), db, PeriodicSpec{Slug: "tick", Kind: "test.tick", Every: time.Minute, Active: true}))
	require.NoError(t, PausePeriodic(at(0), db, "tick"))

	n, err := tickingScheduler(t, db).Tick(at(5 * time.Minute))
	require.NoError(t, err)
	assert.Zero(t, n, "a paused schedule is not due")
	assert.Zero(t, jobCount(t, db, "test.tick"))
	row := mustPeriodic(t, db, "tick")
	assert.True(t, row.NextRunAt.Equal(t0.Add(time.Minute)), "its cursor is left where the pause found it")
	assert.Nil(t, row.LastEnqueuedAt)
}

// TestAPausedScheduleDoesNotFire runs the tick on SQLite.
func TestAPausedScheduleDoesNotFire(t *testing.T) {
	t.Parallel()
	pausedDoesNotFireSuite(t, newDB(t))
}

// resumeNoBackfillSuite proves a resume moves a schedule's cursor to its first
// fire after now, on its own grid, so the fires the pause skipped are never
// enqueued: an interval keeps its phase, a cron takes its next match, and a
// cursor still in the future is kept.
func resumeNoBackfillSuite(t *testing.T, db *gorm.DB) {
	t.Helper()
	t0 := time.Now().UTC().Truncate(time.Minute)
	at := pauseClock(t0)
	s := tickingScheduler(t, db)

	require.NoError(t, UpsertPeriodic(at(0), db, PeriodicSpec{Slug: "every", Kind: "test.every", Every: time.Minute, Active: true}))
	require.NoError(t, PausePeriodic(at(30*time.Second), db, "every"))
	require.NoError(t, ResumePeriodic(at(3*time.Minute+30*time.Second), db, "every"))
	row := mustPeriodic(t, db, "every")
	assert.Nil(t, row.PausedAt, "the resume clears the pause")
	assert.True(t, row.NextRunAt.Equal(t0.Add(4*time.Minute)),
		"the first fire after now on the schedule's grid, not %s", row.NextRunAt)
	n, err := s.Tick(at(3*time.Minute + 30*time.Second))
	require.NoError(t, err)
	assert.Zero(t, n, "the three fires the pause skipped are not backfilled")
	n, err = s.Tick(at(4 * time.Minute))
	require.NoError(t, err)
	assert.Equal(t, 1, n, "the schedule fires again at its next fire time")
	assert.EqualValues(t, 1, jobCount(t, db, "test.every"))

	const expr = "*/5 * * * *"
	require.NoError(t, UpsertPeriodic(at(0), db, PeriodicSpec{Slug: "cron", Kind: "test.cron", Cron: expr, Active: true}))
	require.NoError(t, PausePeriodic(at(time.Minute), db, "cron"))
	require.NoError(t, ResumePeriodic(at(22*time.Minute), db, "cron"))
	sched, err := cron.ParseStandard(expr)
	require.NoError(t, err)
	assert.True(t, mustPeriodic(t, db, "cron").NextRunAt.Equal(sched.Next(t0.Add(22*time.Minute))),
		"a cron resumes at its next match after now")
	_, err = s.Tick(at(22 * time.Minute))
	require.NoError(t, err)
	assert.Zero(t, jobCount(t, db, "test.cron"), "none of the matches the pause skipped is enqueued")

	require.NoError(t, UpsertPeriodic(at(0), db, PeriodicSpec{Slug: "later", Kind: "test.later", Every: time.Hour, Active: true}))
	require.NoError(t, PausePeriodic(at(time.Minute), db, "later"))
	require.NoError(t, ResumePeriodic(at(2*time.Minute), db, "later"))
	assert.True(t, mustPeriodic(t, db, "later").NextRunAt.Equal(t0.Add(time.Hour)),
		"a cursor still in the future when resumed is kept")
}

// TestResumeDoesNotBackfillSkippedFires runs the resume on SQLite.
func TestResumeDoesNotBackfillSkippedFires(t *testing.T) {
	t.Parallel()
	resumeNoBackfillSuite(t, newDB(t))
}

// pauseIdempotentSuite proves pausing a paused schedule and resuming one that is
// not paused change nothing — not even updated_at — and that both refuse an
// unknown slug with ErrPeriodicNotFound.
func pauseIdempotentSuite(t *testing.T, db *gorm.DB) {
	t.Helper()
	t0 := time.Now().UTC().Truncate(time.Second)
	at := pauseClock(t0)
	require.NoError(t, UpsertPeriodic(at(0), db, PeriodicSpec{Slug: "twice", Kind: "test.twice", Every: time.Minute, Active: true}))
	before := mustPeriodic(t, db, "twice")

	require.NoError(t, ResumePeriodic(at(5*time.Minute), db, "twice"), "resuming a schedule that is not paused is not an error")
	row := mustPeriodic(t, db, "twice")
	assert.Nil(t, row.PausedAt)
	assert.True(t, row.NextRunAt.Equal(t0.Add(time.Minute)), "and it moves nothing, a cursor in the past included")
	assert.True(t, row.UpdatedAt.Equal(before.UpdatedAt), "nor writes anything")

	require.NoError(t, PausePeriodic(at(6*time.Minute), db, "twice"))
	require.NoError(t, PausePeriodic(at(7*time.Minute), db, "twice"), "pausing a paused schedule is not an error")
	row = mustPeriodic(t, db, "twice")
	require.NotNil(t, row.PausedAt)
	assert.True(t, row.PausedAt.Equal(t0.Add(6*time.Minute)), "the first pause's time stands")
	assert.True(t, row.UpdatedAt.Equal(t0.Add(6*time.Minute)), "the second pause wrote nothing")

	require.ErrorIs(t, PausePeriodic(at(0), db, "ghost"), ErrPeriodicNotFound)
	require.ErrorIs(t, ResumePeriodic(at(0), db, "ghost"), ErrPeriodicNotFound)
}

// TestPausingAndResumingAreIdempotent runs the no-op cases on SQLite.
func TestPausingAndResumingAreIdempotent(t *testing.T) {
	t.Parallel()
	pauseIdempotentSuite(t, newDB(t))
}

// TestAPauseAndActiveAreSeparateSwitches proves a schedule fires only when it is
// active and not paused, and that neither switch moves the other: a schedule
// declared Active: false stays off through a pause, a resume, and a restart; a
// declaration turning it on again does not lift its pause; and
// SetPeriodicActive(true) does not resume a paused schedule.
func TestAPauseAndActiveAreSeparateSwitches(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	t0 := time.Now().UTC().Truncate(time.Second)
	at := pauseClock(t0)
	s := tickingScheduler(t, db)
	off := PeriodicSpec{Slug: "off", Kind: "test.off", Every: time.Minute, Active: false}

	declaringScheduler(t, db, &captureHandler{}, 0, off).applyDeclaredPeriodics(at(0))
	require.NoError(t, PausePeriodic(at(10*time.Second), db, "off"))
	require.NoError(t, ResumePeriodic(at(20*time.Second), db, "off"))
	row := mustPeriodic(t, db, "off")
	assert.False(t, *row.IsActive, "resuming a schedule declared off leaves it off")
	assert.Nil(t, row.PausedAt)
	n, err := s.Tick(at(10 * time.Minute))
	require.NoError(t, err)
	assert.Zero(t, n)

	require.NoError(t, PausePeriodic(at(11*time.Minute), db, "off"))
	declaringScheduler(t, db, &captureHandler{}, 0, off).applyDeclaredPeriodics(at(12 * time.Minute))
	row = mustPeriodic(t, db, "off")
	assert.False(t, *row.IsActive, "a restart keeps it off")
	assert.NotNil(t, row.PausedAt, "and paused")

	on := off
	on.Active = true
	declaringScheduler(t, db, &captureHandler{}, 0, on).applyDeclaredPeriodics(at(13 * time.Minute))
	row = mustPeriodic(t, db, "off")
	assert.True(t, *row.IsActive, "declaring it on applies Active")
	assert.NotNil(t, row.PausedAt, "and does not lift the pause")
	require.NoError(t, SetPeriodicActive(at(14*time.Minute), db, "off", true))
	assert.NotNil(t, mustPeriodic(t, db, "off").PausedAt, "nor does SetPeriodicActive")
	n, err = s.Tick(at(20 * time.Minute))
	require.NoError(t, err)
	assert.Zero(t, n, "an active, paused schedule does not fire")
	assert.Zero(t, jobCount(t, db, "test.off"))

	require.NoError(t, ResumePeriodic(at(20*time.Minute), db, "off"))
	n, err = s.Tick(at(21 * time.Minute))
	require.NoError(t, err)
	assert.Equal(t, 1, n, "active and not paused, it fires")
}

// TestReconcileLeavesAPauseAlone proves the background reconcile never writes
// the pause: a paused declared schedule is present, so the pass inserts
// nothing, logs nothing, and the schedule stays paused.
func TestReconcileLeavesAPauseAlone(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	h := &captureHandler{}
	ctx := context.Background()
	s := declaringScheduler(t, db, h, 0, declaredSpec("a"))
	s.applyDeclaredPeriodics(ctx)
	require.NoError(t, PausePeriodic(ctx, db, "a"))

	n, err := s.ReconcilePeriodics(ctx)
	require.NoError(t, err)
	assert.Zero(t, n)
	assert.NotNil(t, mustPeriodic(t, db, "a").PausedAt, "the pass leaves the pause alone")
	assert.Empty(t, h.messages)
}

// TestDeclaredApplyLogsAPausedScheduleOnce proves each start logs, once and at
// info, every declared schedule it finds paused, with the slug and when it was
// paused; a schedule that is not paused logs nothing, and the start-up apply's
// retry of a declaration that failed does not log the paused one again.
func TestDeclaredApplyLogsAPausedScheduleOnce(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	t0 := time.Now().UTC().Truncate(time.Second)
	at := pauseClock(t0)
	specs := []PeriodicSpec{declaredSpec("a"), declaredSpec("b")}

	first := &captureHandler{}
	declaringScheduler(t, db, first, 0, specs...).applyDeclaredPeriodics(at(0))
	assert.Empty(t, first.recordsFor(pausedMessage), "nothing is paused yet")
	require.NoError(t, PausePeriodic(at(time.Minute), db, "a"))

	// The second start also declares c, whose first insert fails, so the start
	// leaves it pending and its retry lands it.
	failPeriodicInsert(t, db, "c", true)
	second := &captureHandler{}
	s := declaringScheduler(t, db, second, 0, append(specs, declaredSpec("c"))...)
	s.applyDeclaredPeriodics(at(2 * time.Minute))
	recs := second.recordsFor(pausedMessage)
	require.Len(t, recs, 1, "the paused declaration is logged once")
	assert.Equal(t, "a", recs[0].attrs["slug"].String())
	assert.True(t, recs[0].attrs["paused_at"].Time().Equal(t0.Add(time.Minute)), "with when it was paused")
	require.True(t, s.hasPendingPeriodics())
	s.reconcileOnce(at(3 * time.Minute))
	require.False(t, s.hasPendingPeriodics(), "the retry landed c")
	assert.Len(t, second.recordsFor(pausedMessage), 1, "the retry does not log the paused declaration again")

	var buf bytes.Buffer
	third := newSchedulerCfg(t, SchedulerConfig{
		DB: db, Client: NewClient(db), Logger: slog.New(slog.NewTextHandler(&buf, nil)), Periodics: specs,
	})
	third.applyDeclaredPeriodics(at(4 * time.Minute))
	assert.Equal(t, 1, strings.Count(buf.String(), `level=INFO msg="`+pausedMessage+`" slug=a paused_at=`),
		"every start logs it once, at info:\n%s", buf.String())
	assert.NotContains(t, buf.String(), "slug=b")
}

// TestPeriodicViewReportsThePause proves ListPeriodics reports whether each
// schedule is paused and since when, under the JSON names a dashboard reads.
func TestPeriodicViewReportsThePause(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	t0 := time.Now().UTC().Truncate(time.Second)
	at := pauseClock(t0)
	require.NoError(t, UpsertPeriodic(at(0), db, PeriodicSpec{Slug: "view", Kind: "test.view", Every: time.Minute, Active: true}))
	require.NoError(t, PausePeriodic(at(time.Minute), db, "view"))

	views, err := ListPeriodics(context.Background(), db)
	require.NoError(t, err)
	require.Len(t, views, 1)
	assert.True(t, views[0].Paused)
	require.NotNil(t, views[0].PausedAt)
	assert.True(t, views[0].PausedAt.Equal(t0.Add(time.Minute)))
	raw, err := json.Marshal(views[0])
	require.NoError(t, err)
	assert.Contains(t, string(raw), `"paused":true`)
	assert.Contains(t, string(raw), `"paused_at":`)

	require.NoError(t, ResumePeriodic(at(2*time.Minute), db, "view"))
	views, err = ListPeriodics(context.Background(), db)
	require.NoError(t, err)
	assert.False(t, views[0].Paused)
	assert.Nil(t, views[0].PausedAt)
	raw, err = json.Marshal(views[0])
	require.NoError(t, err)
	assert.Contains(t, string(raw), `"paused":false`)
	assert.NotContains(t, string(raw), "paused_at", "a schedule that is not paused carries no paused_at")
}

// TestSchedulerRunKeepsAPausedScheduleFromFiring runs the issue's steps
// through Run itself: a Scheduler declares a schedule, an operator pauses it,
// and the Scheduler is stopped and started again with the same config. The
// restart logs the pause and fires nothing across five due times; after a
// resume and another restart, the schedule fires at its next fire time.
func TestSchedulerRunKeepsAPausedScheduleFromFiring(t *testing.T) {
	t.Parallel()
	db := newWALFileDB(t)
	clock := models.NewSimulatedClock(time.Now().UTC().Truncate(time.Second))
	start := func(h *captureHandler) func() {
		t.Helper()
		s := newSchedulerCfg(t, SchedulerConfig{
			DB: db, Client: NewClient(db), Logger: slog.New(h),
			TickInterval: 10 * time.Millisecond, SweepInterval: time.Hour,
			Periodics: []PeriodicSpec{{Slug: "nightly", Kind: "test.nightly", Every: time.Minute, Active: true}},
		})
		ctx, cancel := context.WithCancel(clockCtx(context.Background(), clock))
		done := make(chan error, 1)
		go func() { done <- s.Run(ctx) }()
		return func() {
			cancel()
			select {
			case err := <-done:
				require.ErrorIs(t, err, context.Canceled)
			case <-time.After(5 * time.Second):
				t.Fatal("Run did not return after cancellation")
			}
		}
	}
	operator := clockCtx(context.Background(), clock)

	stop := start(&captureHandler{})
	require.Eventually(t, func() bool {
		_, found := periodicRowBySlug(t, db, "nightly")
		return found
	}, 5*time.Second, 5*time.Millisecond, "Run applied the declaration")
	require.NoError(t, PausePeriodic(operator, db, "nightly"))
	stop()

	h := &captureHandler{}
	stop = start(h)
	require.Eventually(t, func() bool { return h.has(pausedMessage) }, 5*time.Second, 5*time.Millisecond,
		"the restart logs the paused declaration")
	clock.Advance(5 * time.Minute)
	assert.Never(t, func() bool { return jobCount(t, db, "test.nightly") > 0 }, 300*time.Millisecond, 10*time.Millisecond,
		"the paused schedule fires at none of its due times")
	stop()
	assert.Len(t, h.recordsFor(pausedMessage), 1)
	assert.NotNil(t, mustPeriodic(t, db, "nightly").PausedAt, "the pause outlived the restart")

	require.NoError(t, ResumePeriodic(operator, db, "nightly"))
	stop = start(&captureHandler{})
	defer stop()
	clock.Advance(61 * time.Second)
	require.Eventually(t, func() bool { return jobCount(t, db, "test.nightly") == 1 }, 5*time.Second, 5*time.Millisecond,
		"resumed, it fires at its next fire time, once")
}
