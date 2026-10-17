package core

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
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

// scheduleOperator is the actor the schedule-pause tests pause under.
//
//nolint:gochecknoglobals // shared fixture
var scheduleOperator = Actor{Type: ActorUser, ID: "ops-1", Reason: "provider outage", Source: "api"}

// pauseAttributedSuite proves a pause and a resume each write the schedule's
// pause and one entry for its slug, attributed to the context's actor, and that
// a pause with no actor is the system's; that pausing a paused schedule, or
// resuming one that isn't paused, changes nothing and records nothing; that an
// unknown slug is ErrPeriodicNotFound; and that an actor the history can't
// record, and a failing audit hook, each leave everything as it was.
func pauseAttributedSuite(t *testing.T, db *gorm.DB) {
	t.Helper()
	t0 := time.Now().UTC().Truncate(time.Second)
	at := pauseClock(t0)
	as := func(a Actor, d time.Duration) context.Context { return WithActor(at(d), a) }
	require.NoError(t, UpsertPeriodic(at(0), db, PeriodicSpec{Slug: "nightly", Kind: "test.nightly", Every: time.Hour, Active: true}))

	require.NoError(t, PausePeriodic(as(scheduleOperator, time.Minute), db, "nightly"))
	row := mustPeriodic(t, db, "nightly")
	require.NotNil(t, row.PausedAt)
	assert.True(t, row.PausedAt.Equal(t0.Add(time.Minute)))
	assert.Equal(t, string(ActorUser), row.PausedByType)
	assert.Equal(t, "ops-1", row.PausedByID)
	assert.Equal(t, "provider outage", row.PausedReason)
	entry := onlyAuditRow(t, db, "nightly")
	assert.Equal(t, string(AuditTargetPeriodic), entry.TargetType)
	assert.Equal(t, string(AuditPaused), entry.Action)
	assert.Equal(t, string(ActorUser), entry.ActorType)
	assert.Equal(t, "ops-1", entry.ActorID)
	assert.Equal(t, "provider outage", entry.Reason)
	assert.Equal(t, "api", entry.Source)
	assert.True(t, t0.Add(time.Minute).Equal(entry.CreatedAt))
	assert.Empty(t, auditDetails(t, entry))
	n, err := tickingScheduler(t, db).Tick(at(time.Hour + time.Minute))
	require.NoError(t, err)
	assert.Zero(t, n, "a paused schedule does not fire at its due time")

	bot := Actor{Type: ActorAgent, ID: "bot", Reason: "provider back"}
	require.NoError(t, PausePeriodic(as(bot, 2*time.Minute), db, "nightly"), "pausing a paused schedule is not an error")
	assert.Equal(t, row, mustPeriodic(t, db, "nightly"), "and changes nothing: the first pause's time and actor stand")
	assert.Len(t, auditRows(t, db, "nightly"), 1, "and records nothing")

	require.NoError(t, ResumePeriodic(as(bot, 3*time.Hour+30*time.Minute), db, "nightly"))
	row = mustPeriodic(t, db, "nightly")
	assert.Nil(t, row.PausedAt)
	assert.Empty(t, row.PausedByType)
	assert.Empty(t, row.PausedByID)
	assert.Empty(t, row.PausedReason)
	assert.True(t, row.NextRunAt.Equal(t0.Add(4*time.Hour)), "the three fires the pause skipped are not run")
	entries := auditRows(t, db, "nightly")
	require.Len(t, entries, 2)
	assert.Equal(t, string(AuditResumed), entries[1].Action)
	assert.Equal(t, "bot", entries[1].ActorID)
	assert.Equal(t, "provider back", entries[1].Reason)
	require.NoError(t, ResumePeriodic(as(bot, 4*time.Hour), db, "nightly"), "resuming a schedule that isn't paused is not an error")
	assert.Len(t, auditRows(t, db, "nightly"), 2, "and records nothing")

	require.NoError(t, PausePeriodic(at(4*time.Hour), db, "nightly"))
	row = mustPeriodic(t, db, "nightly")
	assert.Equal(t, string(ActorSystem), row.PausedByType, "a pause with no actor is the system's")
	assert.Empty(t, row.PausedByID)
	entries = auditRows(t, db, "nightly")
	require.Len(t, entries, 3)
	assert.Equal(t, string(ActorSystem), entries[2].ActorType)

	require.ErrorIs(t, PausePeriodic(as(scheduleOperator, 0), db, "ghost"), ErrPeriodicNotFound)
	require.ErrorIs(t, ResumePeriodic(as(scheduleOperator, 0), db, "ghost"), ErrPeriodicNotFound)
	assert.Empty(t, auditRows(t, db, "ghost"))

	require.NoError(t, UpsertPeriodic(at(0), db, PeriodicSpec{Slug: "guarded", Kind: "test.guarded", Every: time.Hour, Active: true}))
	for _, call := range []func(context.Context, *gorm.DB, string) error{PausePeriodic, ResumePeriodic} {
		for _, bad := range invalidActors() {
			assertRefusedActor(t, call(WithActor(at(0), bad.actor), db, "guarded"), bad)
		}
	}
	assert.Nil(t, mustPeriodic(t, db, "guarded").PausedAt, "a refused actor writes nothing")
	assert.Empty(t, auditRows(t, db, "guarded"))

	hookErr := errors.New("host record failed")
	failing := WithAuditHook(as(scheduleOperator, 0), func(context.Context, *gorm.DB, []AuditEntry) error { return hookErr })
	require.ErrorIs(t, PausePeriodic(failing, db, "guarded"), hookErr)
	assert.Nil(t, mustPeriodic(t, db, "guarded").PausedAt, "the hook's error rolls the pause back")
	assert.Empty(t, auditRows(t, db, "guarded"))
	var seen []AuditEntry
	recording := WithAuditHook(as(scheduleOperator, 0), func(_ context.Context, _ *gorm.DB, e []AuditEntry) error {
		seen = append(seen, e...)
		return nil
	})
	require.NoError(t, PausePeriodic(recording, db, "guarded"))
	require.Len(t, seen, 1, "the hook runs with the pause's entry")
	assert.Equal(t, AuditTargetPeriodic, seen[0].TargetType)
	assert.Equal(t, "guarded", seen[0].TargetID)
	assert.Equal(t, AuditPaused, seen[0].Action)
}

// TestPausingAScheduleIsAttributedAndIdempotent runs the attributed pause on
// SQLite.
func TestPausingAScheduleIsAttributedAndIdempotent(t *testing.T) {
	t.Parallel()
	pauseAttributedSuite(t, newDB(t))
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
// info, every declared schedule it finds paused, with the slug, when it was
// paused, and the type of the actor who paused it; a schedule that is not
// paused logs nothing, and the start-up apply's retry of a declaration that
// failed does not log the paused one again.
func TestDeclaredApplyLogsAPausedScheduleOnce(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	t0 := time.Now().UTC().Truncate(time.Second)
	at := pauseClock(t0)
	specs := []PeriodicSpec{declaredSpec("a"), declaredSpec("b")}

	first := &captureHandler{}
	declaringScheduler(t, db, first, 0, specs...).applyDeclaredPeriodics(at(0))
	assert.Empty(t, first.recordsFor(pausedMessage), "nothing is paused yet")
	require.NoError(t, PausePeriodic(WithActor(at(time.Minute), scheduleOperator), db, "a"))

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
	assert.Equal(t, string(ActorUser), recs[0].attrs["paused_by_type"].String(), "and by what kind of actor")
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
	assert.Equal(t, 1, strings.Count(buf.String(), "paused_by_type=user"), buf.String())
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

// firedJobID returns the id of the job slug's scheduled fire enqueued for
// bucket, found by the bucket's unique key.
func firedJobID(t *testing.T, db *gorm.DB, slug string, bucket time.Time) string {
	t.Helper()
	var ids []string
	require.NoError(t, db.Model(&jobRow{}).Where("unique_key = ?", fmt.Sprintf("%s@%d", slug, bucket.Unix())).
		Pluck("id", &ids).Error)
	require.Len(t, ids, 1, "one job for %s at %s", slug, bucket)
	return ids[0]
}

// mustHistory reads slug's history, failing on an error.
func mustHistory(t *testing.T, db *gorm.DB, slug string) []PeriodicFire {
	t.Helper()
	history, err := PeriodicHistory(context.Background(), db, slug)
	require.NoError(t, err)
	return history
}

// fireHistorySuite proves every scheduled fire is remembered on its schedule's
// row, newest first, at its bucket's time: a bucket that collided with one
// already enqueued is not remembered again, a backfill remembers each bucket it
// enqueued, and the history keeps the latest PeriodicHistorySize fires, stored
// as job_id, fired_at in UTC, and manual.
func fireHistorySuite(t *testing.T, db *gorm.DB) {
	t.Helper()
	t0 := time.Now().UTC().Truncate(time.Minute)
	at := pauseClock(t0)
	s := tickingScheduler(t, db)
	require.NoError(t, UpsertPeriodic(at(0), db, PeriodicSpec{Slug: "minutely", Kind: "test.minutely", Every: time.Minute, Active: true}))
	history := mustHistory(t, db, "minutely")
	assert.NotNil(t, history)
	assert.Empty(t, history, "a schedule that has not fired has no history")

	n, err := s.Tick(at(time.Minute))
	require.NoError(t, err)
	require.Equal(t, 1, n)
	history = mustHistory(t, db, "minutely")
	require.Len(t, history, 1)
	assert.Equal(t, firedJobID(t, db, "minutely", t0.Add(time.Minute)), history[0].JobID)
	assert.True(t, history[0].FiredAt.Equal(t0.Add(time.Minute)), "a scheduled fire is remembered at its bucket")
	assert.False(t, history[0].Manual)

	require.NoError(t, db.Model(&jobPeriodicRow{}).Where("slug = ?", "minutely").Update("next_run_at", t0.Add(time.Minute)).Error)
	n, err = s.Tick(at(time.Minute))
	require.NoError(t, err)
	assert.Zero(t, n, "a redundant tick of the bucket collides")
	assert.Len(t, mustHistory(t, db, "minutely"), 1, "and a bucket that collided is not remembered again")

	n, err = s.Tick(at(5 * time.Minute))
	require.NoError(t, err)
	require.Equal(t, 4, n)
	history = mustHistory(t, db, "minutely")
	require.Len(t, history, 5)
	for i, f := range history {
		assert.True(t, f.FiredAt.Equal(t0.Add(time.Duration(5-i)*time.Minute)), "a backfill is remembered newest first: %d is %s", i, f.FiredAt)
	}

	for m := 6; m <= 25; m++ {
		n, err = s.Tick(at(time.Duration(m) * time.Minute))
		require.NoError(t, err)
		require.Equal(t, 1, n)
	}
	history = mustHistory(t, db, "minutely")
	require.Len(t, history, PeriodicHistorySize, "twenty-five fires keep the newest twenty")
	for i, f := range history {
		bucket := t0.Add(time.Duration(25-i) * time.Minute)
		assert.True(t, f.FiredAt.Equal(bucket), "%d fired at %s, not %s", i, f.FiredAt, bucket)
		assert.Equal(t, firedJobID(t, db, "minutely", bucket), f.JobID)
	}

	row := mustPeriodic(t, db, "minutely")
	require.NotNil(t, row.LastEnqueuedAt)
	assert.True(t, row.LastEnqueuedAt.Equal(t0.Add(25*time.Minute)))
	var stored []map[string]any
	require.NoError(t, json.Unmarshal(row.RecentJobs, &stored))
	require.Len(t, stored, PeriodicHistorySize)
	assert.Equal(t, map[string]any{
		"job_id":   firedJobID(t, db, "minutely", t0.Add(25*time.Minute)),
		"fired_at": t0.Add(25 * time.Minute).Format(time.RFC3339Nano),
		"manual":   false,
	}, stored[0])
}

// TestEveryFireIsRememberedNewestFirst runs the scheduled fires' history on
// SQLite.
func TestEveryFireIsRememberedNewestFirst(t *testing.T) {
	t.Parallel()
	fireHistorySuite(t, newDB(t))
}

// historyOutcomeSuite proves PeriodicHistory reports each fired job's state,
// its latest run's outcome, and that run's finish time: a job that succeeded,
// one discarded at its second attempt (the second attempt's outcome, not the
// first's), one soft-deleted (its state is still its state), one still
// waiting, and one retention removed, which keeps its id and fire time with an
// empty state.
func historyOutcomeSuite(t *testing.T, db *gorm.DB) {
	t.Helper()
	ctx := context.Background()
	t0 := time.Now().UTC().Truncate(time.Minute)
	at := pauseClock(t0)
	require.NoError(t, UpsertPeriodic(at(0), db, PeriodicSpec{Slug: "report", Kind: "test.report", Every: time.Minute, Active: true}))
	n, err := tickingScheduler(t, db).Tick(at(5 * time.Minute))
	require.NoError(t, err)
	require.Equal(t, 5, n)
	ids := make([]string, 5)
	for i := range ids {
		ids[i] = firedJobID(t, db, "report", t0.Add(time.Duration(i+1)*time.Minute))
	}
	succeeded, discarded, gone, softDeleted, waiting := ids[0], ids[1], ids[2], ids[3], ids[4]

	finished := t0.Add(10 * time.Minute)
	setState := func(id string, state JobState, attempt int) {
		require.NoError(t, db.Model(&jobRow{}).Where("id = ?", id).
			Updates(map[string]any{"state": string(state), "attempt": attempt}).Error)
	}
	run := func(jobID string, attempt int, outcome RunOutcome, at time.Time) {
		seedRun(t, db, jobRunRow{
			ID: models.NewID(), JobID: jobID, Attempt: attempt, ExecutorID: "test", StartedAt: at.Add(-time.Second),
			FinishedAt: &at, Outcome: string(outcome), CreatedAt: at.Add(-time.Second),
		})
	}
	setState(succeeded, StateSucceeded, 1)
	run(succeeded, 1, OutcomeSuccess, finished)
	setState(discarded, StateDiscarded, 2)
	run(discarded, 1, OutcomeTimeout, finished.Add(-time.Minute))
	run(discarded, 2, OutcomeError, finished)
	require.NoError(t, db.Unscoped().Where("id = ?", gone).Delete(&jobRow{}).Error)
	setState(softDeleted, StateCancelled, 0)
	require.NoError(t, db.Where("id = ?", softDeleted).Delete(&jobRow{}).Error)

	history, err := PeriodicHistory(ctx, db, "report")
	require.NoError(t, err)
	require.Len(t, history, 5)
	for i, f := range history {
		assert.Equal(t, ids[4-i], f.JobID, "newest first")
		assert.True(t, f.FiredAt.Equal(t0.Add(time.Duration(5-i)*time.Minute)))
	}
	byID := make(map[string]PeriodicFire, len(history))
	for _, f := range history {
		byID[f.JobID] = f
	}
	assert.Equal(t, StateSucceeded, byID[succeeded].State)
	assert.Equal(t, string(OutcomeSuccess), byID[succeeded].Outcome)
	require.NotNil(t, byID[succeeded].FinishedAt)
	assert.True(t, byID[succeeded].FinishedAt.Equal(finished))
	assert.Equal(t, StateDiscarded, byID[discarded].State)
	assert.Equal(t, string(OutcomeError), byID[discarded].Outcome, "the latest attempt's outcome")
	require.NotNil(t, byID[discarded].FinishedAt)
	assert.True(t, byID[discarded].FinishedAt.Equal(finished))
	assert.Equal(t, StateCancelled, byID[softDeleted].State, "a soft-deleted job's state is still its state")
	assert.Empty(t, byID[softDeleted].Outcome)
	assert.Equal(t, StateAvailable, byID[waiting].State)
	assert.Empty(t, byID[waiting].Outcome, "a job that has not run has no outcome")
	assert.Nil(t, byID[waiting].FinishedAt)
	assert.Empty(t, byID[gone].State, "a job retention removed has no state")
	assert.True(t, byID[gone].FiredAt.Equal(t0.Add(3*time.Minute)), "and keeps its fire time")
	raw, err := json.Marshal(byID[gone])
	require.NoError(t, err)
	assert.JSONEq(t, fmt.Sprintf(`{"job_id":%q,"fired_at":%q,"manual":false}`, gone,
		t0.Add(3*time.Minute).Format(time.RFC3339Nano)), string(raw))

	_, err = PeriodicHistory(ctx, db, "ghost")
	require.ErrorIs(t, err, ErrPeriodicNotFound)
}

// TestPeriodicHistoryReportsEachJobsOutcome runs the history's read on SQLite.
func TestPeriodicHistoryReportsEachJobsOutcome(t *testing.T) {
	t.Parallel()
	historyOutcomeSuite(t, newDB(t))
}

// triggerSuite proves TriggerPeriodic enqueues a schedule's kind, args, and
// queue, due now, with no unique key, through the client, so its insert hook
// sees a periodic insert and the slug; that it runs a paused schedule and an
// inactive one, leaving the pause and the cadence alone; that two triggers at
// one instant enqueue two jobs; that each records a manual fire, moves
// last_enqueued_at, and writes one triggered entry naming its job; and that an
// unknown slug, an actor the history can't record, and a failing audit hook
// each enqueue nothing.
func triggerSuite(t *testing.T, db *gorm.DB) {
	t.Helper()
	t0 := time.Now().UTC().Truncate(time.Second)
	at := pauseClock(t0)
	rec, client := hookedClient(db)
	require.NoError(t, UpsertPeriodic(at(0), db, PeriodicSpec{
		Slug: "nightly", Kind: "test.nightly", Queue: "reports", ArgsTemplate: []byte(`{"region":"eu"}`),
		Every: time.Hour, Active: true,
	}))
	require.NoError(t, PausePeriodic(WithActor(at(time.Minute), scheduleOperator), db, "nightly"))

	rerun := Actor{Type: ActorUser, ID: "ops-2", Reason: "re-run last night's report", Source: "api"}
	id, err := TriggerPeriodic(WithActor(at(2*time.Minute), rerun), client, "nightly")
	require.NoError(t, err)
	job := jobRowByID(t, db, id)
	assert.Equal(t, "test.nightly", job.Kind)
	assert.Equal(t, "reports", job.Queue)
	assert.JSONEq(t, `{"region":"eu"}`, string(job.Args))
	assert.Equal(t, string(StateAvailable), job.State)
	assert.True(t, job.ScheduledAt.Equal(t0.Add(2*time.Minute)), "due now")
	assert.Nil(t, job.UniqueKey, "with no unique key")
	assert.Nil(t, job.UniqueActiveKey)
	assert.Equal(t, []InsertInfo{{
		Kind: "test.nightly", Queue: "reports", Source: InsertSourcePeriodic, PeriodicSlug: "nightly",
	}}, rec.list(), "the hook sees a periodic insert and the slug")

	row := mustPeriodic(t, db, "nightly")
	require.NotNil(t, row.LastEnqueuedAt)
	assert.True(t, row.LastEnqueuedAt.Equal(t0.Add(2*time.Minute)))
	assert.NotNil(t, row.PausedAt, "a trigger leaves the pause alone")
	assert.True(t, row.NextRunAt.Equal(t0.Add(time.Hour)), "and the cadence")
	history := mustHistory(t, db, "nightly")
	require.Len(t, history, 1)
	assert.Equal(t, id, history[0].JobID)
	assert.True(t, history[0].Manual, "a trigger is remembered as a manual fire")
	assert.True(t, history[0].FiredAt.Equal(t0.Add(2*time.Minute)))
	entries := auditRows(t, db, "nightly")
	require.Len(t, entries, 2, "the pause's entry, then the trigger's")
	assert.Equal(t, string(AuditTargetPeriodic), entries[1].TargetType)
	assert.Equal(t, string(AuditTriggered), entries[1].Action)
	assert.Equal(t, "ops-2", entries[1].ActorID)
	assert.Equal(t, "re-run last night's report", entries[1].Reason)
	assert.Equal(t, map[string]any{"job_id": id}, auditDetails(t, entries[1]))

	again, err := TriggerPeriodic(WithActor(at(2*time.Minute), rerun), client, "nightly")
	require.NoError(t, err, "a second trigger at the same instant is not refused")
	assert.NotEqual(t, id, again)
	assert.EqualValues(t, 2, jobCount(t, db, "test.nightly"), "two triggers enqueue two jobs")
	history = mustHistory(t, db, "nightly")
	require.Len(t, history, 2)
	assert.Equal(t, []string{again, id}, []string{history[0].JobID, history[1].JobID}, "newest first")
	n, err := tickingScheduler(t, db).Tick(at(2 * time.Hour))
	require.NoError(t, err)
	assert.Zero(t, n, "the paused schedule still does not fire")

	require.NoError(t, UpsertPeriodic(at(0), db, PeriodicSpec{Slug: "off", Kind: "test.off", Every: time.Hour, Active: false}))
	_, err = TriggerPeriodic(at(3*time.Minute), client, "off")
	require.NoError(t, err)
	assert.EqualValues(t, 1, jobCount(t, db, "test.off"), "an inactive schedule runs on demand too")
	assert.Equal(t, string(ActorSystem), onlyAuditRow(t, db, "off").ActorType, "a trigger with no actor is the system's")

	_, err = TriggerPeriodic(at(0), client, "ghost")
	require.ErrorIs(t, err, ErrPeriodicNotFound)
	for _, bad := range invalidActors() {
		_, err = TriggerPeriodic(WithActor(at(0), bad.actor), client, "off")
		assertRefusedActor(t, err, bad)
	}
	hookErr := errors.New("host record failed")
	_, err = TriggerPeriodic(WithAuditHook(at(4*time.Minute), func(context.Context, *gorm.DB, []AuditEntry) error {
		return hookErr
	}), client, "off")
	require.ErrorIs(t, err, hookErr)
	assert.EqualValues(t, 1, jobCount(t, db, "test.off"), "a refused call, and the hook's error, enqueue nothing")
	assert.Len(t, mustHistory(t, db, "off"), 1, "and remember nothing")
	assert.Len(t, auditRows(t, db, "off"), 1)
	_, err = TriggerPeriodic(at(0), nil, "off")
	require.Error(t, err)
}

// TestTriggerEnqueuesOnceAndRecordsTheFire runs the manual run on SQLite.
func TestTriggerEnqueuesOnceAndRecordsTheFire(t *testing.T) {
	t.Parallel()
	triggerSuite(t, newDB(t))
}

// periodicSources reads every definition's source through ListPeriodics, by
// slug.
func periodicSources(t *testing.T, db *gorm.DB) map[string]PeriodicSource {
	t.Helper()
	views, err := ListPeriodics(context.Background(), db)
	require.NoError(t, err)
	out := make(map[string]PeriodicSource, len(views))
	for _, v := range views {
		out[v.Slug] = v.Source
	}
	return out
}

// TestSourceRecordsTheLastWriter proves a definition's source names the path
// that last wrote it: the start-up apply and the reconcile write declared,
// UpsertPeriodic writes upserted, each overwriting the other, and a row an
// older binary wrote reports none until one of them writes it.
func TestSourceRecordsTheLastWriter(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	ctx := context.Background()
	s := declaringScheduler(t, db, &captureHandler{}, 0, declaredSpec("declared"), declaredSpec("recreated"))
	s.applyDeclaredPeriodics(ctx)
	require.NoError(t, UpsertPeriodic(ctx, db, PeriodicSpec{Slug: "upserted", Kind: "test.upserted", Every: time.Minute, Active: true}))
	writeLegacyPeriodic(t, db, "legacy", "test.legacy", time.Now().UTC().Add(time.Hour))
	assert.Equal(t, map[string]PeriodicSource{
		"declared": PeriodicDeclared, "recreated": PeriodicDeclared, "upserted": PeriodicUpserted, "legacy": "",
	}, periodicSources(t, db))

	require.NoError(t, UpsertPeriodic(ctx, db, declaredSpec("declared")))
	assert.Equal(t, PeriodicUpserted, periodicSources(t, db)["declared"], "an upsert of a declared definition is the last writer")
	declaringScheduler(t, db, &captureHandler{}, 0, declaredSpec("declared")).applyDeclaredPeriodics(ctx)
	assert.Equal(t, PeriodicDeclared, periodicSources(t, db)["declared"], "and the next start declares it again")

	require.NoError(t, DeletePeriodic(ctx, db, "recreated"))
	n, err := s.ReconcilePeriodics(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, n)
	assert.Equal(t, PeriodicDeclared, periodicSources(t, db)["recreated"], "the reconcile re-creates it as declared")

	require.NoError(t, UpsertPeriodic(ctx, db, PeriodicSpec{Slug: "legacy", Kind: "test.legacy", Every: time.Minute, Active: true}))
	assert.Equal(t, PeriodicUpserted, periodicSources(t, db)["legacy"], "an older row takes its source at its next write")

	raw, err := json.Marshal(PeriodicView{Slug: "legacy"})
	require.NoError(t, err)
	assert.NotContains(t, string(raw), `"source"`, "an empty source is left out of the JSON")
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
