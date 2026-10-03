package core

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	anomalyLogMsg = "jobs: stats anomaly"
	slowRunLogMsg = "jobs: run is far slower than its kind's baseline"
	healthLogMsg  = "jobs: queue health"
)

// TestRollupLogsEachAnomalyOnsetOnce drives the stats activity's pass the way
// the Scheduler does and captures its log: a regression that begins in the
// rolled hours is warned about once, a second pass over nothing new warns about
// nothing, and the warning carries the numbers an operator needs.
func TestRollupLogsEachAnomalyOnsetOnce(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	// Ten quiet hours of 30 runs at ~100ms (the baseline: 300 samples), then
	// three hours of 30 runs at ~700ms.
	var runs []histRun
	for h := range 13 {
		dur := 100
		if h >= 10 {
			dur = 700
		}
		for i := range 30 {
			runs = append(runs, okRun("k", hourAt(rollupBase, h).Add(time.Duration(i)*time.Minute), dur+i))
		}
	}
	seedHistory(t, db, runs)

	h := &captureHandler{}
	s := newSchedulerCfg(t, SchedulerConfig{
		DB: db, Client: NewClient(db), Logger: slog.New(h),
		StatsRollupInterval: time.Minute, StatsAnomalyLog: true,
	})
	ctx := fixedClockCtx(hourAt(rollupBase, 13).Add(10 * time.Minute))
	s.rollupOnce(ctx)

	warns := h.recordsFor(anomalyLogMsg)
	require.Len(t, warns, 1, "one onset, one warning")
	w := warns[0].attrs
	assert.Equal(t, "k", w["kind"].String())
	assert.Equal(t, string(SignalDurationRegression), w["signal"].String())
	assert.Equal(t, "p50", w["metric"].String(), "30 samples an hour is below the 100 a p95 comparison needs")
	assert.GreaterOrEqual(t, w["ratio"].Float64(), 2.0)

	s.rollupOnce(ctx)
	assert.Len(t, h.recordsFor(anomalyLogMsg), 1, "an hour is rolled once, so it is evaluated — and warned about — once")
}

// TestRollupDoesNotWarnAboutOldHistory pins the recency guard: hours rolled
// while catching up on old history are not evaluated, so enabling the rollup on
// a long-lived database does not page anyone for last month.
func TestRollupDoesNotWarnAboutOldHistory(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	var runs []histRun
	for h := range 13 {
		dur := 100
		if h >= 10 {
			dur = 900
		}
		for i := range 30 {
			runs = append(runs, okRun("k", hourAt(rollupBase, h).Add(time.Duration(i)*time.Minute), dur))
		}
	}
	seedHistory(t, db, runs)
	h := &captureHandler{}
	s := newSchedulerCfg(t, SchedulerConfig{
		DB: db, Client: NewClient(db), Logger: slog.New(h),
		StatsRollupInterval: time.Minute, StatsAnomalyLog: true,
	})
	s.rollupOnce(fixedClockCtx(hourAt(rollupBase, 13+24)))
	assert.Empty(t, h.recordsFor(anomalyLogMsg))
}

// TestHeartbeatCountsSlowRunsAndWarnsOnce covers the heartbeat integration: the
// pulse carries slow_running as a number, the first pulse that sees a slow run
// warns once, later pulses do not repeat it, and a run that stops being slow
// (here, it finished) leaves the dedup set.
func TestHeartbeatCountsSlowRunsAndWarnsOnce(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	var hist []histRun
	for i := range 300 {
		hist = append(hist, okRun("k", inspectT0.Add(-3*time.Hour).Add(time.Duration(i)*time.Second), 1000))
	}
	seedHistory(t, db, hist)
	h := &captureHandler{}
	s := newSchedulerCfg(t, SchedulerConfig{
		DB: db, Client: NewClient(db), Logger: slog.New(h),
		StatsRollupInterval: time.Minute, HealthSampleInterval: time.Minute,
	})
	ctx := fixedClockCtx(inspectT0)
	_, err := s.RollupStats(ctx)
	require.NoError(t, err)
	s.refreshBaselines(ctx)
	seedRunning(t, db, "stuck", "k", "q", 5*time.Minute, time.Minute)
	seedRunning(t, db, "fine", "k", "q", 10*time.Second, time.Minute)

	s.logHealth(ctx)
	s.logHealth(ctx)
	pulses := h.recordsFor(healthLogMsg)
	require.Len(t, pulses, 2)
	assert.EqualValues(t, 1, pulses[0].attrs["slow_running"].Int64())
	assert.EqualValues(t, 1, pulses[1].attrs["slow_running"].Int64())
	warns := h.recordsFor(slowRunLogMsg)
	require.Len(t, warns, 1, "warned once, not once per pulse")
	assert.Equal(t, "stuck", warns[0].attrs["job_id"].String())

	require.NoError(t, db.Model(&jobRow{}).Where("id = ?", "stuck").Update("state", string(StateSucceeded)).Error)
	s.logHealth(ctx)
	assert.EqualValues(t, 0, h.recordsFor(healthLogMsg)[2].attrs["slow_running"].Int64())
	assert.Empty(t, s.stats.warned, "a run no longer slow leaves the dedup set")
}

// TestHeartbeatOmitsSlowRunningWithoutTheRollup pins the opt-in: the field is
// only on the pulse when the rollup that supplies its baselines is on.
func TestHeartbeatOmitsSlowRunningWithoutTheRollup(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	h := &captureHandler{}
	s := newSchedulerCfg(t, SchedulerConfig{
		DB: db, Client: NewClient(db), Logger: slog.New(h), HealthSampleInterval: time.Minute,
	})
	s.logHealth(context.Background())
	pulses := h.recordsFor(healthLogMsg)
	require.Len(t, pulses, 1)
	_, has := pulses[0].attrs["slow_running"]
	assert.False(t, has)
}

// TestRollupOnceLogsCatchUp proves a pass that stops on its ceiling says so,
// which is what tells an operator why a long Stats window is still refused.
func TestRollupOnceLogsCatchUp(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	var runs []histRun
	for h := range 5 {
		runs = append(runs, okRun("k", hourAt(rollupBase, h).Add(time.Minute), 100))
	}
	seedHistory(t, db, runs)
	h := &captureHandler{}
	s := newSchedulerCfg(t, SchedulerConfig{
		DB: db, Client: NewClient(db), Logger: slog.New(h),
		StatsRollupInterval: time.Minute, StatsMaxHoursPerPass: 2,
	})
	s.rollupOnce(fixedClockCtx(hourAt(rollupBase, 6)))
	assert.True(t, h.has("jobs: stats rollup catching up"))
}
