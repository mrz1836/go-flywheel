package core

import (
	"context"
	"fmt"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mrz1836/go-foundation/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

// The log lines the declared-periodic paths emit, which hosts key alerts on.
const (
	recreatedMessage   = "jobs: declared periodic was missing and has been re-created"
	applyFailedMessage = "jobs: declared periodic apply failed"
	appliedMessage     = "jobs: declared periodics applied"
	reconcileFailedMsg = "jobs: periodic reconcile failed"
)

// declaredSpec is a declared every-minute definition whose kind is named after
// its slug, so a row read back says which declaration wrote it.
func declaredSpec(slug string) PeriodicSpec {
	return PeriodicSpec{Slug: slug, Kind: "test." + slug, Every: time.Minute, Active: true}
}

// declaringScheduler builds a Scheduler over db that declares specs and logs to h,
// with the Driver db's dialect selects.
func declaringScheduler(
	t testing.TB, db *gorm.DB, h *captureHandler, interval time.Duration, specs ...PeriodicSpec,
) *Scheduler {
	t.Helper()
	driver, err := driverFor(db)
	require.NoError(t, err)
	return newSchedulerCfg(t, SchedulerConfig{
		DB: db, Client: NewClient(db), Driver: driver, Logger: slog.New(h),
		Periodics: specs, ReconcileInterval: interval,
	})
}

// periodicRowBySlug reads the job_periodics row for slug, reporting whether one
// exists.
func periodicRowBySlug(t testing.TB, db *gorm.DB, slug string) (jobPeriodicRow, bool) {
	t.Helper()
	row, found, err := loadPeriodic(context.Background(), db, slug)
	require.NoError(t, err)
	return row, found
}

// periodicIsActive reads the active flag of slug's row, failing when there is none.
func periodicIsActive(t testing.TB, db *gorm.DB, slug string) bool {
	t.Helper()
	row, found := periodicRowBySlug(t, db, slug)
	require.True(t, found, "%s has a row", slug)
	require.NotNil(t, row.IsActive)
	return *row.IsActive
}

// failPeriodicInsert makes inserts of slug's row fail — every one, or only the
// first when once is set — by adding an error ahead of GORM's create.
func failPeriodicInsert(t testing.TB, db *gorm.DB, slug string, once bool) {
	t.Helper()
	var fired atomic.Bool
	require.NoError(t, db.Callback().Create().Before("gorm:create").Register("test:fail_insert_"+slug, func(tx *gorm.DB) {
		row, ok := tx.Statement.Dest.(*jobPeriodicRow)
		if !ok || row.Slug != slug {
			return
		}
		if once && !fired.CompareAndSwap(false, true) {
			return
		}
		_ = tx.AddError(fmt.Errorf("injected insert failure for %q", slug))
	}))
}

// activityIntervals maps each activity the Scheduler would run to its cadence.
func activityIntervals(s *Scheduler) map[string]time.Duration {
	out := map[string]time.Duration{}
	for _, a := range s.activities() {
		out[a.name] = a.interval
	}
	return out
}

// --- construction -------------------------------------------------------------

// TestNewSchedulerWithConfigRejectsInvalidPeriodics proves a malformed
// declaration fails construction — where a host fixes it in source — rather than
// a minute into Run. Every rejection is a validation failure and names the
// declaration it is about.
func TestNewSchedulerWithConfigRejectsInvalidPeriodics(t *testing.T) {
	t.Parallel()
	db := newDB(t)

	tests := map[string]struct {
		periodics []PeriodicSpec
		names     string
	}{
		"missing slug":     {[]PeriodicSpec{declaredSpec("ok"), {Kind: "k", Every: time.Minute}}, "Periodics[1]"},
		"missing kind":     {[]PeriodicSpec{{Slug: "s", Every: time.Minute}}, `periodic "s"`},
		"neither schedule": {[]PeriodicSpec{{Slug: "s", Kind: "k"}}, `periodic "s"`},
		"both schedules":   {[]PeriodicSpec{{Slug: "s", Kind: "k", Every: time.Minute, Cron: "* * * * *"}}, `periodic "s"`},
		"malformed cron":   {[]PeriodicSpec{{Slug: "s", Kind: "k", Cron: "not a cron"}}, `periodic "s"`},
		"sub-second every": {[]PeriodicSpec{{Slug: "s", Kind: "k", Every: time.Millisecond}}, `periodic "s"`},
		"malformed args": {
			[]PeriodicSpec{{Slug: "s", Kind: "k", Every: time.Minute, ArgsTemplate: []byte("{not json")}},
			`periodic "s": flywheel: args_template must be valid JSON`,
		},
		"duplicate slug": {
			[]PeriodicSpec{declaredSpec("s"), declaredSpec("other"), declaredSpec("s")},
			`periodic "s" is declared more than once`,
		},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			_, err := NewSchedulerWithConfig(SchedulerConfig{
				DB: db, Client: NewClient(db), Driver: NewSQLiteDriver(db), Periodics: tt.periodics,
			})
			require.ErrorIs(t, err, ErrValidation)
			assert.Contains(t, err.Error(), "jobs: scheduler config: "+tt.names)
		})
	}
}

// TestNewSchedulerWithConfigCopiesPeriodics proves the Scheduler declares what it
// was constructed with: a caller that reuses its slice, or the ArgsTemplate buffer
// inside it, cannot change what a running Scheduler re-creates.
func TestNewSchedulerWithConfigCopiesPeriodics(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	specs := []PeriodicSpec{{
		Slug: "copied", Kind: "test.original", Every: time.Minute, Active: true,
		ArgsTemplate: []byte(`{"v":"a"}`),
	}}
	s := declaringScheduler(t, db, &captureHandler{}, 0, specs...)

	specs[0].Kind = "test.mutated"
	specs[0].ArgsTemplate[6] = 'z'

	n, err := s.ReconcilePeriodics(context.Background())
	require.NoError(t, err)
	require.Equal(t, 1, n)
	row, found := periodicRowBySlug(t, db, "copied")
	require.True(t, found)
	assert.Equal(t, "test.original", row.Kind)
	assert.JSONEq(t, `{"v":"a"}`, string(row.ArgsTemplate))
}

// TestReconcileIntervalResolution pins the interval's three cases, the same rule
// the heartbeat and starvation intervals follow.
func TestReconcileIntervalResolution(t *testing.T) {
	t.Parallel()
	tests := map[string]struct {
		config, want time.Duration
	}{
		"zero selects the default": {0, time.Minute},
		"negative disables":        {-time.Second, 0},
		"positive is kept":         {5 * time.Second, 5 * time.Second},
		"positive is not floored":  {10 * time.Millisecond, 10 * time.Millisecond},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, resolveReconcileInterval(tt.config))
		})
	}
}

// TestReconcileActivityRunsOnlyWithDeclaredPeriodics pins the structural opt-in:
// no declarations, no activity, and a disabled pass costs no goroutine either.
func TestReconcileActivityRunsOnlyWithDeclaredPeriodics(t *testing.T) {
	t.Parallel()
	db := newDB(t)

	tests := map[string]struct {
		periodics []PeriodicSpec
		interval  time.Duration
		want      time.Duration // zero: no reconcile activity
	}{
		"nothing declared":                  {nil, 0, 0},
		"nothing declared, interval set":    {nil, time.Second, 0},
		"declared, default interval":        {[]PeriodicSpec{declaredSpec("a")}, 0, time.Minute},
		"declared, explicit interval":       {[]PeriodicSpec{declaredSpec("a")}, 5 * time.Second, 5 * time.Second},
		"declared, pass disabled":           {[]PeriodicSpec{declaredSpec("a")}, -1, 0},
		"declared, pass disabled, negative": {[]PeriodicSpec{declaredSpec("a")}, -time.Hour, 0},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			s := declaringScheduler(t, db, &captureHandler{}, tt.interval, tt.periodics...)
			got, present := activityIntervals(s)["reconcile"]
			if tt.want == 0 {
				assert.False(t, present, "no reconcile activity")
				return
			}
			require.True(t, present, "the reconcile activity runs")
			assert.Equal(t, tt.want, got)
		})
	}
}

// reconcileActivity returns the reconcile activity s would run, failing the
// test when there is none.
func reconcileActivity(t *testing.T, s *Scheduler) *activity {
	t.Helper()
	for _, a := range s.activities() {
		if a.name == "reconcile" {
			return a
		}
	}
	t.Fatal("no reconcile activity")
	return nil
}

// TestReconcileActivityRetriesAFailedStartEvenWhenDisabled drives the reconcile
// activity's own loop with the pass disabled and the start-up apply failed: the
// loop retries at the default cadence while the declaration is pending — staying
// up through failed retries — lands it once the database recovers, and then exits
// on its own, because with the pass off there is nothing left for it to do.
func TestReconcileActivityRetriesAFailedStartEvenWhenDisabled(t *testing.T) {
	t.Parallel()
	db := newSingleConnMemoryDB(t)
	h := &captureHandler{}
	s := declaringScheduler(t, db, h, -1, declaredSpec("a"))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	require.NoError(t, db.Exec(`PRAGMA query_only = ON`).Error)
	s.applyDeclaredPeriodics(ctx)
	a := reconcileActivity(t, s)
	assert.Equal(t, time.Minute, a.interval, "a pending apply keeps the activity even with the pass disabled")

	// Drive the real loop at a test cadence rather than the one-minute default.
	a.interval = 5 * time.Millisecond
	exited := make(chan struct{})
	go func() { defer close(exited); a.loop(ctx, slog.New(h)) }()

	require.Eventually(t, func() bool { return len(h.recordsFor(applyFailedMessage)) >= 3 },
		5*time.Second, 5*time.Millisecond, "the loop keeps retrying while the database refuses writes")
	select {
	case <-exited:
		t.Fatal("the loop stopped while a declaration was still pending")
	default:
	}

	require.NoError(t, db.Exec(`PRAGMA query_only = OFF`).Error)
	select {
	case <-exited:
	case <-time.After(5 * time.Second):
		t.Fatal("the loop did not exit after the retry landed")
	}
	_, found := periodicRowBySlug(t, db, "a")
	assert.True(t, found, "the retry applied the declaration")
	assert.True(t, h.has(appliedMessage))
	assert.NoError(t, ctx.Err(), "the loop ended on its own, not on cancellation")

	// With the pass on, the activity never runs out of work.
	on := declaringScheduler(t, db, &captureHandler{}, 0, declaredSpec("a"))
	assert.False(t, reconcileActivity(t, on).done(), "an enabled pass keeps its goroutine")
}

// --- the start-up apply -------------------------------------------------------

// TestApplyDeclaredPeriodicsOnStart covers what a restart does to each kind of
// declared row, which is UpsertPeriodic's semantics: create what is missing,
// update what changed, keep the cursor of what did not, and re-activate what an
// operator deactivated. A declaration taking effect is not drift, so none of it
// warns — a clean start logs nothing at all.
func TestApplyDeclaredPeriodicsOnStart(t *testing.T) {
	t.Parallel()
	t0 := time.Now().UTC().Truncate(time.Second)
	t1 := t0.Add(10 * time.Second)
	at := func(ts time.Time) context.Context {
		return clockCtx(context.Background(), models.NewFixedClock(ts))
	}

	t.Run("creates a missing definition", func(t *testing.T) {
		t.Parallel()
		db := newDB(t)
		h := &captureHandler{}
		declaringScheduler(t, db, h, 0, declaredSpec("a")).applyDeclaredPeriodics(at(t0))

		row, found := periodicRowBySlug(t, db, "a")
		require.True(t, found)
		assert.True(t, row.NextRunAt.Equal(t0.Add(time.Minute)), "a new definition fires first one interval out")
		assert.Empty(t, h.messages, "a clean start logs nothing")
	})

	t.Run("updates a changed definition", func(t *testing.T) {
		t.Parallel()
		db := newDB(t)
		h := &captureHandler{}
		require.NoError(t, UpsertPeriodic(at(t0), db, PeriodicSpec{Slug: "a", Kind: "test.old", Every: time.Hour, Active: true}))

		declaringScheduler(t, db, h, 0, declaredSpec("a")).applyDeclaredPeriodics(at(t1))

		row, found := periodicRowBySlug(t, db, "a")
		require.True(t, found)
		assert.Equal(t, "test.a", row.Kind)
		require.NotNil(t, row.IntervalSeconds)
		assert.Equal(t, 60, *row.IntervalSeconds)
		assert.True(t, row.NextRunAt.Equal(t1.Add(time.Minute)), "a changed cadence resets the cursor")
		assert.Empty(t, h.messages)
	})

	t.Run("keeps the cursor of an unchanged definition", func(t *testing.T) {
		t.Parallel()
		db := newDB(t)
		h := &captureHandler{}
		declaringScheduler(t, db, h, 0, declaredSpec("a")).applyDeclaredPeriodics(at(t0))
		declaringScheduler(t, db, h, 0, declaredSpec("a")).applyDeclaredPeriodics(at(t1))

		row, found := periodicRowBySlug(t, db, "a")
		require.True(t, found)
		assert.True(t, row.NextRunAt.Equal(t0.Add(time.Minute)), "a restart does not reset an unchanged cadence")
		assert.Empty(t, h.messages)
	})

	t.Run("re-activates a deactivated definition", func(t *testing.T) {
		t.Parallel()
		db := newDB(t)
		h := &captureHandler{}
		declaringScheduler(t, db, h, 0, declaredSpec("a")).applyDeclaredPeriodics(at(t0))
		require.NoError(t, SetPeriodicActive(at(t0), db, "a", false))

		declaringScheduler(t, db, h, 0, declaredSpec("a")).applyDeclaredPeriodics(at(t1))

		assert.True(t, periodicIsActive(t, db, "a"), "a restart re-applies Active")
		assert.Empty(t, h.messages)
	})
}

// TestStartupApplyFailureIsLoggedThenRetried proves an outage at start is logged
// once per attempt with what is still pending, retried by the reconcile pass, and
// that the recovery is logged; after it the pass is back to inserting what goes
// missing.
func TestStartupApplyFailureIsLoggedThenRetried(t *testing.T) {
	t.Parallel()
	db := newSingleConnMemoryDB(t)
	h := &captureHandler{}
	s := declaringScheduler(t, db, h, 0, declaredSpec("a"), declaredSpec("b"))
	ctx := context.Background()

	require.NoError(t, db.Exec(`PRAGMA query_only = ON`).Error)
	s.applyDeclaredPeriodics(ctx)
	failures := h.recordsFor(applyFailedMessage)
	require.Len(t, failures, 1)
	assert.EqualValues(t, 2, failures[0].attrs["pending"].Int64())
	assert.Equal(t, "1m0s", failures[0].attrs["retry_in"].String())
	assert.Contains(t, failures[0].attrs["error"].String(), `flywheel: insert periodic "a"`)
	assert.Contains(t, failures[0].attrs["error"].String(), `flywheel: insert periodic "b"`)

	// A retry while the outage lasts fails the same way, and is logged again.
	s.reconcileOnce(ctx)
	require.Len(t, h.recordsFor(applyFailedMessage), 2)
	assert.True(t, s.hasPendingPeriodics())

	require.NoError(t, db.Exec(`PRAGMA query_only = OFF`).Error)
	s.reconcileOnce(ctx)
	for _, slug := range []string{"a", "b"} {
		_, found := periodicRowBySlug(t, db, slug)
		assert.True(t, found, "the retry applied %s", slug)
	}
	applied := h.recordsFor(appliedMessage)
	require.Len(t, applied, 1, "the recovery is logged")
	assert.EqualValues(t, 2, applied[0].attrs["periodics"].Int64())
	assert.False(t, h.has(recreatedMessage), "applying a declaration that never landed is not drift")

	require.NoError(t, DeletePeriodic(ctx, db, "b"))
	s.reconcileOnce(ctx)
	_, found := periodicRowBySlug(t, db, "b")
	assert.True(t, found, "the pass re-creates what goes missing after the retry landed")
	assert.Len(t, h.recordsFor(recreatedMessage), 1)
	assert.Len(t, h.recordsFor(appliedMessage), 1, "the recovery is logged once")
}

// TestStartupApplyRetryLeavesAppliedDeclarationsAlone is why only the failures are
// retried. "a" landed at start and an operator then deactivated it; the retry that
// lands "b" must not re-send "a"'s Active and undo the operator.
func TestStartupApplyRetryLeavesAppliedDeclarationsAlone(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	h := &captureHandler{}
	s := declaringScheduler(t, db, h, 0, declaredSpec("a"), declaredSpec("b"))
	ctx := context.Background()
	failPeriodicInsert(t, db, "b", true)

	s.applyDeclaredPeriodics(ctx)
	require.Len(t, h.recordsFor(applyFailedMessage), 1)
	assert.EqualValues(t, 1, h.recordsFor(applyFailedMessage)[0].attrs["pending"].Int64())
	require.NoError(t, SetPeriodicActive(ctx, db, "a", false))

	s.reconcileOnce(ctx)
	_, found := periodicRowBySlug(t, db, "b")
	assert.True(t, found, "the failed declaration is retried")
	assert.False(t, periodicIsActive(t, db, "a"), "the declaration that already landed is not re-applied")
}

// TestReconcileRecreatesOthersWhileOneDeclarationKeepsFailing proves a
// declaration that can never be applied holds back only itself. "bad" fails every
// insert; "good" landed at start and is then deleted. The reconcile re-creates
// "good" and keeps retrying "bad", and neither path ever reports "bad" — which
// never existed — as re-created.
func TestReconcileRecreatesOthersWhileOneDeclarationKeepsFailing(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	h := &captureHandler{}
	s := declaringScheduler(t, db, h, 0, declaredSpec("bad"), declaredSpec("good"))
	ctx := context.Background()
	failPeriodicInsert(t, db, "bad", false)

	s.applyDeclaredPeriodics(ctx)
	require.Len(t, h.recordsFor(applyFailedMessage), 1)
	require.NoError(t, DeletePeriodic(ctx, db, "good"))

	s.reconcileOnce(ctx)
	_, found := periodicRowBySlug(t, db, "good")
	assert.True(t, found, "the pass re-creates good although bad is still pending")
	warns := h.recordsFor(recreatedMessage)
	require.Len(t, warns, 1)
	assert.Equal(t, "good", warns[0].attrs["slug"].String())
	assert.Len(t, h.recordsFor(applyFailedMessage), 2, "bad is retried, and fails again")
	assert.True(t, s.hasPendingPeriodics())

	// A host calling the pass directly skips the pending declaration too: bad
	// is the start-up apply's to land, not a missing row to report.
	require.NoError(t, DeletePeriodic(ctx, db, "good"))
	n, err := s.ReconcilePeriodics(ctx)
	require.NoError(t, err, "the pending declaration is skipped, not attempted")
	assert.Equal(t, 1, n)
	_, found = periodicRowBySlug(t, db, "bad")
	assert.False(t, found)
	for _, w := range h.recordsFor(recreatedMessage) {
		assert.Equal(t, "good", w.attrs["slug"].String(), "bad is never reported as re-created")
	}
}

// TestRunSurvivesAnUnreachableDatabaseWithPeriodics proves declaring periodics
// does not turn an outage at start into a crash: Run logs the failed apply and
// keeps running until it is stopped, like the schema check before it.
func TestRunSurvivesAnUnreachableDatabaseWithPeriodics(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	h := &captureHandler{}
	s := declaringScheduler(t, db, h, 0, declaredSpec("a"))
	closeDB(t, db)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	err := s.Run(ctx)
	require.ErrorIs(t, err, context.DeadlineExceeded, "Run stops only when its context does")
	assert.True(t, h.has(applyFailedMessage), "the failed apply is logged")
}

// --- ReconcilePeriodics ---------------------------------------------------------

// TestReconcilePeriodicsRecreatesADeletedDefinition is the issue: a declared row
// deleted while the Scheduler runs comes back, from its declaration, with its
// next fire after now — no immediate fire and no backfill — and the drift is
// logged once, at warn.
func TestReconcilePeriodicsRecreatesADeletedDefinition(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	h := &captureHandler{}
	s := declaringScheduler(t, db, h, 0, declaredSpec("a"))
	t0 := time.Now().UTC().Truncate(time.Second)
	t1 := t0.Add(time.Hour)

	s.applyDeclaredPeriodics(clockCtx(context.Background(), models.NewFixedClock(t0)))
	require.NoError(t, DeletePeriodic(context.Background(), db, "a"))

	ctx := clockCtx(context.Background(), models.NewFixedClock(t1))
	n, err := s.ReconcilePeriodics(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, n)

	row, found := periodicRowBySlug(t, db, "a")
	require.True(t, found, "the deleted definition is back")
	assert.Equal(t, "test.a", row.Kind)
	assert.True(t, *row.IsActive)
	assert.True(t, row.NextRunAt.Equal(t1.Add(time.Minute)), "it fires next at its first fire time after now")

	fired, err := s.Tick(ctx)
	require.NoError(t, err)
	assert.Zero(t, fired, "the hour it was gone is not backfilled")

	warns := h.recordsFor(recreatedMessage)
	require.Len(t, warns, 1)
	assert.Equal(t, "a", warns[0].attrs["slug"].String())
	assert.Equal(t, "test.a", warns[0].attrs["kind"].String())
	assert.True(t, warns[0].attrs["next_run_at"].Time().Equal(t1.Add(time.Minute)))
}

// TestReconcilePeriodicsLeavesAnInactiveDefinitionInactive proves the pass never
// overrides an operator's deactivation: the row exists, so it is not touched.
func TestReconcilePeriodicsLeavesAnInactiveDefinitionInactive(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	h := &captureHandler{}
	s := declaringScheduler(t, db, h, 0, declaredSpec("a"))
	ctx := context.Background()
	s.applyDeclaredPeriodics(ctx)
	require.NoError(t, SetPeriodicActive(ctx, db, "a", false))

	n, err := s.ReconcilePeriodics(ctx)
	require.NoError(t, err)
	assert.Zero(t, n)
	assert.False(t, periodicIsActive(t, db, "a"))
	assert.Empty(t, h.messages)
}

// TestReconcilePeriodicsLeavesAHandEditedDefinitionAlone proves the pass only
// inserts: a row whose kind, schedule, and queue were changed out from under the
// declaration keeps every change, and its cursor.
func TestReconcilePeriodicsLeavesAHandEditedDefinitionAlone(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	h := &captureHandler{}
	s := declaringScheduler(t, db, h, 0, declaredSpec("a"))
	t0 := time.Now().UTC().Truncate(time.Second)
	ctx0 := clockCtx(context.Background(), models.NewFixedClock(t0))
	s.applyDeclaredPeriodics(ctx0)
	require.NoError(t, UpsertPeriodic(ctx0, db, PeriodicSpec{
		Slug: "a", Kind: "test.edited", Cron: "0 4 * * *", Queue: "edited", Active: true,
	}))
	before, _ := periodicRowBySlug(t, db, "a")

	n, err := s.ReconcilePeriodics(clockCtx(context.Background(), models.NewFixedClock(t0.Add(time.Hour))))
	require.NoError(t, err)
	assert.Zero(t, n)

	after, found := periodicRowBySlug(t, db, "a")
	require.True(t, found)
	assert.Equal(t, "test.edited", after.Kind)
	assert.Equal(t, "edited", after.Queue)
	require.NotNil(t, after.CronExpr)
	assert.Equal(t, "0 4 * * *", *after.CronExpr)
	assert.Nil(t, after.IntervalSeconds)
	assert.True(t, after.NextRunAt.Equal(before.NextRunAt), "the cursor is untouched")
	assert.Empty(t, h.messages)
}

// TestReconcilePeriodicsIsSilentWhenNothingIsMissing pins the steady state: a
// pass over present rows changes nothing and logs nothing.
func TestReconcilePeriodicsIsSilentWhenNothingIsMissing(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	h := &captureHandler{}
	s := declaringScheduler(t, db, h, 0, declaredSpec("a"), declaredSpec("b"))
	ctx := context.Background()
	s.applyDeclaredPeriodics(ctx)

	n, err := s.ReconcilePeriodics(ctx)
	require.NoError(t, err)
	assert.Zero(t, n)
	assert.Empty(t, h.messages)
}

// TestReconcilePeriodicsWithNothingDeclaredSkipsTheDatabase proves a Scheduler
// that declares nothing costs nothing: the call returns without a query, so even
// a closed database is not an error.
func TestReconcilePeriodicsWithNothingDeclaredSkipsTheDatabase(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	s := declaringScheduler(t, db, &captureHandler{}, 0)
	closeDB(t, db)

	n, err := s.ReconcilePeriodics(context.Background())
	require.NoError(t, err)
	assert.Zero(t, n)
}

// TestReconcilePeriodicsRecreatesAnInactiveDeclarationInactive proves a
// re-created row is the declaration, Active included: a definition declared off
// comes back off.
func TestReconcilePeriodicsRecreatesAnInactiveDeclarationInactive(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	off := declaredSpec("off")
	off.Active = false
	s := declaringScheduler(t, db, &captureHandler{}, 0, off)

	n, err := s.ReconcilePeriodics(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 1, n)
	assert.False(t, periodicIsActive(t, db, "off"))
}

// TestReconcilePeriodicsAcrossTwoSchedulersInsertsOnce proves two Schedulers
// declaring the same slug re-create it once: the second finds it present, and
// only the first logs.
func TestReconcilePeriodicsAcrossTwoSchedulersInsertsOnce(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	h1, h2 := &captureHandler{}, &captureHandler{}
	s1 := declaringScheduler(t, db, h1, 0, declaredSpec("a"))
	s2 := declaringScheduler(t, db, h2, 0, declaredSpec("a"))
	ctx := context.Background()

	n1, err := s1.ReconcilePeriodics(ctx)
	require.NoError(t, err)
	n2, err := s2.ReconcilePeriodics(ctx)
	require.NoError(t, err)

	assert.Equal(t, 1, n1)
	assert.Zero(t, n2)
	views, err := ListPeriodics(ctx, db)
	require.NoError(t, err)
	assert.Len(t, views, 1)
	assert.Len(t, h1.recordsFor(recreatedMessage), 1)
	assert.Empty(t, h2.messages)
}

// TestReconcilePeriodicsLosingTheInsertRaceStaysSilent covers the window between
// the pass's read and its insert: another Scheduler re-creates the slug first, so
// this one's insert does nothing, counts nothing, and logs nothing — the one
// whose insert landed reports it.
func TestReconcilePeriodicsLosingTheInsertRaceStaysSilent(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	h := &captureHandler{}
	s := declaringScheduler(t, db, h, 0, declaredSpec("p"))

	var fired atomic.Bool
	require.NoError(t, db.Callback().Query().After("gorm:query").Register("test:after_reconcile_read", func(tx *gorm.DB) {
		if tx.Statement.Table == "job_periodics" && fired.CompareAndSwap(false, true) {
			insertCompetingPeriodic(t, db, "p")
		}
	}))

	n, err := s.ReconcilePeriodics(context.Background())
	require.NoError(t, err)
	assert.Zero(t, n, "a lost insert is not counted")
	assert.Empty(t, h.messages, "a lost insert is not logged")
	row, found := periodicRowBySlug(t, db, "p")
	require.True(t, found)
	assert.Equal(t, "test.competitor", row.Kind, "the row that won is left as it is")
}

// TestReconcilePeriodicsKeepsGoingPastAFailure proves one definition that cannot
// be re-created does not keep the others missing.
func TestReconcilePeriodicsKeepsGoingPastAFailure(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	h := &captureHandler{}
	s := declaringScheduler(t, db, h, 0, declaredSpec("a"), declaredSpec("b"), declaredSpec("c"))
	ctx := context.Background()
	require.NoError(t, UpsertPeriodic(ctx, db, declaredSpec("a")))
	failPeriodicInsert(t, db, "b", false)

	n, err := s.ReconcilePeriodics(ctx)
	assert.Equal(t, 1, n, "c landed")
	require.ErrorContains(t, err, `jobs: reconcile periodic "b": `)
	require.ErrorContains(t, err, `injected insert failure for "b"`)
	_, found := periodicRowBySlug(t, db, "c")
	assert.True(t, found, "the definition after the failure is re-created")
	_, found = periodicRowBySlug(t, db, "b")
	assert.False(t, found)
	assert.Len(t, h.recordsFor(recreatedMessage), 1)
}

// TestReconcilePeriodicsSurfacesALoadError proves a failed read of the declared
// slugs is returned, wrapped, and re-creates nothing.
func TestReconcilePeriodicsSurfacesALoadError(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	s := declaringScheduler(t, db, &captureHandler{}, 0, declaredSpec("a"))
	closeDB(t, db)

	n, err := s.ReconcilePeriodics(context.Background())
	require.ErrorContains(t, err, "jobs: reconcile periodics: ")
	assert.Zero(t, n)
}

// TestReconcileOnceLogsFailures proves the activity's pass logs a failed
// reconcile and carries on, and that a cancellation — a shutdown arriving — is not
// logged as one.
func TestReconcileOnceLogsFailures(t *testing.T) {
	t.Parallel()

	t.Run("a failed pass is logged", func(t *testing.T) {
		t.Parallel()
		db := newDB(t)
		h := &captureHandler{}
		s := declaringScheduler(t, db, h, 0, declaredSpec("a"))
		closeDB(t, db)

		s.reconcileOnce(context.Background())
		failures := h.recordsFor(reconcileFailedMsg)
		require.Len(t, failures, 1)
		assert.Contains(t, failures[0].attrs["error"].String(), "jobs: reconcile periodics: ")
	})

	t.Run("a cancelled pass is not", func(t *testing.T) {
		t.Parallel()
		db := newDB(t)
		h := &captureHandler{}
		s := declaringScheduler(t, db, h, 0, declaredSpec("a"))
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		s.reconcileOnce(ctx)
		assert.Empty(t, h.messages)
	})

	t.Run("a cancelled retry stays pending, silently", func(t *testing.T) {
		t.Parallel()
		db := newDB(t)
		h := &captureHandler{}
		s := declaringScheduler(t, db, h, 0, declaredSpec("a"))
		failPeriodicInsert(t, db, "a", true)
		s.applyDeclaredPeriodics(context.Background())
		require.Len(t, h.recordsFor(applyFailedMessage), 1)

		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		s.reconcileOnce(ctx)
		assert.True(t, s.hasPendingPeriodics(), "a shutdown keeps the declaration pending")
		assert.Len(t, h.messages, 1, "and is not logged as a failure")
	})

	t.Run("a disabled pass does nothing", func(t *testing.T) {
		t.Parallel()
		db := newDB(t)
		h := &captureHandler{}
		s := declaringScheduler(t, db, h, -1, declaredSpec("a"))

		s.reconcileOnce(context.Background())
		_, found := periodicRowBySlug(t, db, "a")
		assert.False(t, found, "with nothing pending and the pass off, the activity writes nothing")
		assert.Empty(t, h.messages)
	})
}

// --- Run, end to end ------------------------------------------------------------

// TestSchedulerRunRecreatesADeletedPeriodicAndFiresIt drives the whole loop on a
// simulated clock: Run applies the declaration, a DeletePeriodic removes it, the
// reconcile activity brings it back and says so, and the periodic tick fires it
// once its next fire time arrives.
func TestSchedulerRunRecreatesADeletedPeriodicAndFiresIt(t *testing.T) {
	t.Parallel()
	db := newWALFileDB(t)
	h := &captureHandler{}
	clock := models.NewSimulatedClock(time.Now().UTC().Truncate(time.Second))
	s := newSchedulerCfg(t, SchedulerConfig{
		DB: db, Client: NewClient(db), Logger: slog.New(h),
		TickInterval: 10 * time.Millisecond, SweepInterval: time.Hour,
		Periodics:         []PeriodicSpec{declaredSpec("a")},
		ReconcileInterval: 10 * time.Millisecond,
	})

	ctx, cancel := context.WithCancel(clockCtx(context.Background(), clock))
	defer cancel()
	runDone := make(chan error, 1)
	go func() { runDone <- s.Run(ctx) }()

	require.Eventually(t, func() bool {
		_, found := periodicRowBySlug(t, db, "a")
		return found
	}, 5*time.Second, 5*time.Millisecond, "Run applied the declaration")

	require.NoError(t, DeletePeriodic(context.Background(), db, "a"))
	require.Eventually(t, func() bool {
		_, found := periodicRowBySlug(t, db, "a")
		return found && h.has(recreatedMessage)
	}, 5*time.Second, 5*time.Millisecond, "the reconcile activity re-created the deleted declaration")
	assert.Zero(t, jobCount(t, db, "test.a"), "re-creating it fired nothing")

	clock.Advance(61 * time.Second)
	require.Eventually(t, func() bool {
		return jobCount(t, db, "test.a") == 1
	}, 5*time.Second, 5*time.Millisecond, "the re-created definition fires at its next fire time")

	cancel()
	select {
	case err := <-runDone:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after cancellation")
	}
}
