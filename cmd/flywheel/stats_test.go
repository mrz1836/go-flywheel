package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	flywheel "github.com/mrz1836/go-flywheel"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

// openCLIConfigDB opens the database a CLI config file points at, for seeding.
func openCLIConfigDB(t *testing.T, cfgPath string) (*gorm.DB, flywheel.Driver) {
	t.Helper()
	loaded, err := LoadConfig(cfgPath)
	require.NoError(t, err)
	db, driver, err := openDB(loaded)
	require.NoError(t, err)
	t.Cleanup(func() { closeDB(db) })
	return db, driver
}

// insertStatsJob writes one jobs row directly.
func insertStatsJob(t *testing.T, db *gorm.DB, id, kind, queue, state string, at time.Time, leasedUntil any) {
	t.Helper()
	var finalized any
	switch state {
	case "succeeded", "discarded", "cancelled":
		finalized = at
	}
	require.NoError(t, db.Exec(
		`INSERT INTO jobs(id, kind, queue, args, priority, state, attempt, max_attempts, scheduled_at, finalized_at, leased_until, executor_class, tags, created_at, updated_at, metadata)
		 VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		id, kind, queue, "{}", 100, state, 1, 25, at, finalized, leasedUntil, "", "[]", at, at, "{}",
	).Error)
}

// insertStatsRun writes one job_runs row directly, timestamps in UTC as the
// runtime stamps them, and — for a finished run — its job_run_finishes entry,
// as the finalize does in the same transaction. A nil finishedAt leaves the
// attempt running.
func insertStatsRun(
	t *testing.T, db *gorm.DB, id, jobID, kind, queue, outcome string, jobState any,
	startedAt time.Time, finishedAt *time.Time, durationMs any,
) {
	t.Helper()
	var finished any
	if finishedAt != nil {
		finished = finishedAt.UTC()
	}
	require.NoError(t, db.Exec(
		`INSERT INTO job_runs(id, job_id, attempt, kind, queue, executor_class, executor_id, started_at, finished_at, outcome, duration_ms, queue_wait_ms, job_state, superseded, enqueued_children, created_at)
		 VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		id, jobID, 1, kind, queue, "local", "host", startedAt.UTC(), finished, outcome, durationMs, 5, jobState, false, 0, startedAt.UTC(),
	).Error)
	if finishedAt != nil {
		require.NoError(t, db.Exec(
			`INSERT INTO job_run_finishes (finished_at, run_id) SELECT finished_at, id FROM job_runs WHERE id = ?`, id,
		).Error)
	}
}

// seedStatsHistory writes finished history two hours before statusAnchor: n
// successful "exec" runs of 100ms each and one discarded "exec" job, all on the
// default queue. It returns the history's hour.
func seedStatsHistory(t *testing.T, db *gorm.DB, n int) time.Time {
	t.Helper()
	hour := statusAnchor.Add(-2 * time.Hour)
	require.NoError(t, db.Transaction(func(tx *gorm.DB) error {
		for i := range n {
			at := hour.Add(time.Duration(i) * time.Second)
			id := fmt.Sprintf("hist-%04d", i)
			insertStatsJob(t, tx, id, "exec", "default", "succeeded", at, nil)
			fin := at.Add(100 * time.Millisecond)
			insertStatsRun(t, tx, id+"-r", id, "exec", "default", "success", "succeeded", at, &fin, 100)
		}
		at := hour.Add(30 * time.Minute)
		insertStatsJob(t, tx, "hist-bad", "exec", "default", "discarded", at, nil)
		fin := at.Add(time.Second)
		insertStatsRun(t, tx, "hist-bad-r", "hist-bad", "exec", "default", "error", "discarded", at, &fin, 1000)
		return nil
	}))
	return hour
}

// rollUp runs one stats rollup pass at statusAnchor through a Scheduler built
// over db, the same pass `serve` runs on its stats_rollup cadence.
func rollUp(t *testing.T, db *gorm.DB, driver flywheel.Driver) flywheel.RollupResult {
	t.Helper()
	s, err := flywheel.NewSchedulerWithConfig(flywheel.SchedulerConfig{
		DB: db, Client: flywheel.NewClient(db), Driver: driver, StatsRollupInterval: time.Minute,
	})
	require.NoError(t, err)
	res, err := s.RollupStats(statusClockCtx())
	require.NoError(t, err)
	return res
}

func TestCLIStatsReportsRolledUpHistory(t *testing.T) {
	t.Parallel()
	cfg := migratedCLIConfig(t)
	db, driver := openCLIConfigDB(t, cfg)
	seedStatsHistory(t, db, 40)
	res := rollUp(t, db, driver)
	require.Equal(t, 1, res.Hours, "the one hour of history is rolled up")

	out, err := runRoot(statusClockCtx(), "--config", cfg, "stats")
	require.NoError(t, err)
	assert.Contains(t, out, "flywheel stats:")
	assert.Contains(t, out, "SUCCESS%", "the table header is printed")
	assert.Regexp(t, `exec\s+41\s+40\s+1\s+0\s+97\.6%\s+\d+ms\s+\d+ms\s+\d+ms\s+100ms\s+5ms`, out,
		"the kind row carries attempts, outcomes, the success rate, the durations, and the queue wait")
	assert.Contains(t, out, "TOTAL")
	assert.Contains(t, out, "coverage: rollups through 2026-06-22T11:00:00Z",
		"the coverage line names the rollup watermark")

	out, err = runRoot(statusClockCtx(), "--config", cfg, "stats", "--json", "--kind", "exec", "--since", "7d")
	require.NoError(t, err)
	var parsed flywheel.StatsResult
	require.NoError(t, json.Unmarshal([]byte(out), &parsed))
	require.Len(t, parsed.Kinds, 1)
	assert.Equal(t, "exec", parsed.Kinds[0].Kind)
	assert.EqualValues(t, 40, parsed.Kinds[0].Succeeded)
	assert.EqualValues(t, 1, parsed.Kinds[0].Discarded)
	assert.Equal(t, 100*time.Millisecond, parsed.Kinds[0].Duration.Max, "the max is exact")
	assert.InDelta(t, float64(100*time.Millisecond), float64(parsed.Kinds[0].Duration.P50), float64(5*time.Millisecond),
		"the median is estimated from the histogram, within a few percent")

	out, err = runRoot(statusClockCtx(), "--config", cfg, "stats", "--kind", "nope")
	require.NoError(t, err)
	assert.NotContains(t, out, "exec ", "a kind filter excludes every other kind")
}

func TestCLIStatsWithoutRollupsReadsRawAndCapsTheWindow(t *testing.T) {
	t.Parallel()
	cfg := migratedCLIConfig(t)

	out, err := runRoot(statusClockCtx(), "--config", cfg, "stats", "--since", "1h")
	require.NoError(t, err)
	assert.Contains(t, out, "no hourly rollups yet", "an unrolled database says the window was read raw")
	assert.Regexp(t, `TOTAL\s+0\s+0\s+0\s+0\s+-`, out, "an empty window renders zeros and dashes")

	out, err = runRoot(statusClockCtx(), "--config", cfg, "stats", "--since", "30d")
	require.NoError(t, err, "a month with no runs in it costs nothing to read raw")
	assert.Contains(t, out, "raw runs")

	// History spread over three weeks: reading it raw is the scan the cap refuses.
	db, _ := openCLIConfigDB(t, cfg)
	for i, at := range []time.Time{statusAnchor.Add(-21 * 24 * time.Hour), statusAnchor.Add(-time.Hour)} {
		id := fmt.Sprintf("spread-%d", i)
		insertStatsJob(t, db, id, "exec", "default", "succeeded", at, nil)
		fin := at.Add(time.Second)
		insertStatsRun(t, db, id+"-r", id, "exec", "default", "success", "succeeded", at, &fin, 1000)
	}
	_, err = runRoot(statusClockCtx(), "--config", cfg, "stats", "--since", "30d")
	require.ErrorIs(t, err, flywheel.ErrStatsNotRolledUp,
		"three weeks of raw runs is refused rather than scanned")
	assert.Contains(t, err.Error(), "stats_rollup", "the error names the daemon setting that fixes it")
}

func TestCLIStatsRejectsBadSince(t *testing.T) {
	t.Parallel()
	cfg := migratedCLIConfig(t)
	for _, since := range []string{"soon", "0s", "-1h"} {
		_, err := runRoot(context.Background(), "--config", cfg, "stats", "--since", since)
		require.Error(t, err, "--since %q is rejected", since)
		assert.Contains(t, err.Error(), "--since")
	}
}

func TestCLIStatsRebuildRecomputesAndRefusesPrunedHours(t *testing.T) {
	t.Parallel()
	cfg := migratedCLIConfig(t)
	db, driver := openCLIConfigDB(t, cfg)
	hour := seedStatsHistory(t, db, 5)
	rollUp(t, db, driver)

	from := hour.Format(time.RFC3339)
	out, err := runRoot(statusClockCtx(), "--config", cfg, "stats", "rebuild", "--from", from, "--to", "2026-06-23")
	require.NoError(t, err)
	assert.Contains(t, out, "rebuilt 1 hour(s), 1 group row(s)", "every closed hour in range is recomputed")
	assert.Contains(t, out, "→ 2026-06-22T11:00:00Z", "the range is clamped to the last closed hour")
	assert.Regexp(t, `rollups now cover \S+ → 2026-06-22T11:00:00Z`, out, "and the covered range is reported")

	// Delete the raw history the rollup counted: a rebuild without --force must
	// refuse to replace the complete rollup with a partial one.
	require.NoError(t, db.Exec(`DELETE FROM job_runs`).Error)
	_, err = runRoot(statusClockCtx(), "--config", cfg, "stats", "rebuild", "--from", from, "--to", "2026-06-23")
	require.ErrorIs(t, err, flywheel.ErrValidation)
	assert.Contains(t, err.Error(), "Force")

	out, err = runRoot(statusClockCtx(), "--config", cfg, "stats", "rebuild", "--from", from, "--to", "2026-06-23", "--force")
	require.NoError(t, err)
	assert.Contains(t, out, "rebuilt 1 hour(s), 0 group row(s)", "--force rebuilds the pruned hour anyway")
}

func TestCLIStatsRebuildRejectsBadRange(t *testing.T) {
	t.Parallel()
	cfg := migratedCLIConfig(t)
	cases := map[string][]string{
		"unparseable from": {"--from", "yesterday", "--to", "2026-06-22"},
		"unparseable to":   {"--from", "2026-06-21", "--to", "tomorrow"},
		"empty range":      {"--from", "2026-06-22", "--to", "2026-06-22"},
		"missing to":       {"--from", "2026-06-22"},
	}
	for name, args := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			_, err := runRoot(context.Background(), append([]string{"--config", cfg, "stats", "rebuild"}, args...)...)
			require.Error(t, err)
		})
	}
}

func TestParseStatsTime(t *testing.T) {
	t.Parallel()
	got, err := parseStatsTime("2026-06-22")
	require.NoError(t, err)
	assert.Equal(t, time.Date(2026, 6, 22, 0, 0, 0, 0, time.UTC), got, "a bare date is UTC midnight")

	got, err = parseStatsTime("2026-06-22T10:30:00-04:00")
	require.NoError(t, err)
	assert.True(t, got.Equal(time.Date(2026, 6, 22, 14, 30, 0, 0, time.UTC)))

	_, err = parseStatsTime("June 22")
	require.Error(t, err)
}

func TestStatsFormatting(t *testing.T) {
	t.Parallel()
	assert.Equal(t, "12ms", formatStatDuration(12345*time.Microsecond))
	assert.Equal(t, "1.23s", formatStatDuration(1234*time.Millisecond))
	assert.Equal(t, "2m5s", formatStatDuration(125400*time.Millisecond))
	assert.Equal(t, "none", formatSpan(0))
	assert.Equal(t, "1h0m0s", formatSpan(time.Hour))
	assert.Equal(t, "-", formatSuccessRate(flywheel.KindStats{}), "no ended jobs has no rate")
	assert.Equal(t, "75.0%", formatSuccessRate(flywheel.KindStats{Succeeded: 3, Discarded: 1, SuccessRate: 0.75}))
	assert.Equal(t, "-", summaryDuration(flywheel.DurationSummary{}, time.Second), "an empty distribution renders a dash")
}

func TestRenderStatsTextSurfacesAWriteFailure(t *testing.T) {
	t.Parallel()
	err := renderStatsText(&failAfterWriter{okWrites: 1}, flywheel.StatsResult{
		Kinds: []flywheel.KindStats{{Kind: "exec", Attempts: 1}},
	})
	require.Error(t, err, "a failing writer surfaces from the table flush")
}

func TestCLIJobsRunningFlagsSlowAndStartingJobs(t *testing.T) {
	t.Parallel()
	cfg := migratedCLIConfig(t)
	db, driver := openCLIConfigDB(t, cfg)
	seedStatsHistory(t, db, 250) // a 100ms baseline with enough samples to judge against
	rollUp(t, db, driver)

	now := statusAnchor
	lease := now.Add(time.Minute)
	expired := now.Add(-time.Minute)
	// A run five minutes in on a 100ms kind: slow.
	insertStatsJob(t, db, "run-slow", "exec", "default", "running", now.Add(-5*time.Minute), lease)
	insertStatsRun(t, db, "run-slow-r", "run-slow", "exec", "default", "started", nil, now.Add(-5*time.Minute), nil, nil)
	// A kind with no baseline, whose lease has lapsed: never slow, lease expired.
	insertStatsJob(t, db, "run-new", "fresh", "batch", "running", now.Add(-time.Minute), expired)
	insertStatsRun(t, db, "run-new-r", "run-new", "fresh", "batch", "started", nil, now.Add(-time.Minute), nil, nil)
	// Claimed, audit row not yet written: starting.
	insertStatsJob(t, db, "run-starting", "exec", "default", "running", now, lease)

	out, err := runRoot(statusClockCtx(), "--config", cfg, "jobs", "running")
	require.NoError(t, err)
	assert.Contains(t, out, "BASELINE P99", "the table header is printed")
	assert.Regexp(t, `run-slow\s+exec\s+default\s+1/25\s+5m0s\s+ok\s+100ms\s+SLOW`, out,
		"a run far past its kind's baseline is flagged")
	assert.Regexp(t, `run-new\s+fresh\s+batch\s+1/25\s+1m0s\s+expired\s+-\s*\n`, out,
		"a kind with no baseline is never flagged, and a lapsed lease reads expired")
	assert.Regexp(t, `run-starting\s+exec\s+default\s+1/25\s+starting`, out, "a claimed job with no run yet is starting")
	assert.Less(t, strings.Index(out, "run-slow"), strings.Index(out, "run-new"), "longest-running first")

	out, err = runRoot(statusClockCtx(), "--config", cfg, "jobs", "running", "--kind", "fresh", "--json")
	require.NoError(t, err)
	var parsed []flywheel.RunningJob
	require.NoError(t, json.Unmarshal([]byte(out), &parsed))
	require.Len(t, parsed, 1)
	assert.Equal(t, "run-new", parsed[0].JobID)
	assert.True(t, parsed[0].LeaseExpired)
}

func TestRunJobsRunningSurfacesAReadFailure(t *testing.T) {
	t.Parallel()
	db := newCLITestDB(t)
	sqlDB, err := db.DB()
	require.NoError(t, err)
	require.NoError(t, sqlDB.Close())

	var buf bytes.Buffer
	err = runJobsRunning(context.Background(), &buf, db, flywheel.ListRunningParams{}, false)
	require.Error(t, err, "a closed database surfaces as an error, not an empty table")
}

func TestCLIJobsLsFiltersByQueueAndPagesByID(t *testing.T) {
	t.Parallel()
	cfg := migratedCLIConfig(t)
	ctx := context.Background()
	ids := make([]string, 3)
	for i := range ids {
		out, err := runRoot(ctx, "--config", cfg, "enqueue", "exec", `{"command":"true"}`, "--queue", "batch")
		require.NoError(t, err)
		ids[i] = strings.TrimSpace(out)
	}
	other := enqueueOne(t, cfg)

	out, err := runRoot(ctx, "--config", cfg, "jobs", "ls", "--queue", "batch")
	require.NoError(t, err)
	assert.Contains(t, out, "QUEUE", "the table carries a queue column")
	assert.Contains(t, out, ids[0])
	assert.NotContains(t, out, other, "a queue filter excludes other queues")

	out, err = runRoot(ctx, "--config", cfg, "jobs", "ls", "--queue", "batch", "--before", ids[2], "--limit", "1", "--json")
	require.NoError(t, err)
	var page []flywheel.JobView
	require.NoError(t, json.Unmarshal([]byte(out), &page))
	require.Len(t, page, 1)
	assert.Equal(t, ids[1], page[0].ID, "--before pages to the next-older job by id")
}

func TestCLIJobsRunningAndStatsFailOnUnopenableDB(t *testing.T) {
	t.Parallel()
	cfg := unopenableConfig(t)
	cases := [][]string{
		{"jobs", "running"},
		{"stats"},
		{"stats", "rebuild", "--from", "2026-06-21", "--to", "2026-06-22"},
	}
	for _, args := range cases {
		t.Run(strings.Join(args, " "), func(t *testing.T) {
			t.Parallel()
			_, err := runRoot(context.Background(), append([]string{"--config", cfg}, args...)...)
			require.Error(t, err, "a database that cannot be opened is a command error")
		})
	}
}

func TestCLIStatusListsPausedJobs(t *testing.T) {
	t.Parallel()
	cfg := migratedCLIConfig(t)
	db, _ := openCLIConfigDB(t, cfg)
	insertStatsJob(t, db, "held", "exec", "default", "paused", statusAnchor, nil)

	out, err := runRoot(statusClockCtx(), "--config", cfg, "status")
	require.NoError(t, err)
	assert.Regexp(t, `paused\s+1`, out, "a paused job is counted in the per-state breakdown")
	assert.Less(t, strings.Index(out, "scheduled "), strings.Index(out, "paused"),
		"paused is listed after scheduled, with the other non-terminal states")
}

// newBareCLITestDB opens a SQLite database through the CLI's own openDB with no
// schema applied — the database an operator points doctor at before migrating.
func newBareCLITestDB(t *testing.T) *gorm.DB {
	t.Helper()
	db, _, err := openDB(&Config{DB: DBConfig{SQLite: filepath.Join(t.TempDir(), "flywheel.db")}})
	require.NoError(t, err)
	t.Cleanup(func() { closeDB(db) })
	return db
}

func TestRunDoctorReportsTheSchemaUpgradeAndRollupLag(t *testing.T) {
	t.Parallel()
	db := newBareCLITestDB(t)
	cfg := &Config{DB: DBConfig{SQLite: "/x/y.db"}, Runtime: defaultConfig().Runtime}

	var buf bytes.Buffer
	require.NoError(t, runDoctor(statusClockCtx(), &buf, "cfg.yaml", cfg, db))
	out := buf.String()
	assert.Contains(t, out, "schema:       upgraded (added jobs (table)", "the first doctor run reports what Migrate created")
	assert.Contains(t, out, "job_stats_hourly (table)")
	assert.Contains(t, out, "indexes:      in sync")
	assert.Contains(t, out, "stats_rollup: every 1m0s (in serve)", "the rollup is on by default")
	assert.Contains(t, out, "stats:        rollup not run yet")

	// A second run finds nothing to add, and after a rollup pass reports its lag.
	seedStatsHistory(t, db, 3)
	rollUp(t, db, flywheel.NewSQLiteDriver(db))
	buf.Reset()
	cfg.Runtime.StatsRollup = Duration(-time.Second)
	require.NoError(t, runDoctor(statusClockCtx(), &buf, "cfg.yaml", cfg, db))
	out = buf.String()
	assert.Contains(t, out, "schema:       up to date")
	assert.Contains(t, out, "stats_rollup: off", "a negative stats_rollup disables it")
	assert.Contains(t, out, "stats:        rolled up through 2026-06-22T11:00:00Z (lag 1h0m0s)")
}

func TestRunDoctorSurfacesASchemaInspectionFailure(t *testing.T) {
	t.Parallel()
	db := newCLITestDB(t)
	// Ping succeeds on a live handle, but a cancelled context fails the inspection
	// read that follows it — the first database touch after the ping.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var buf bytes.Buffer
	err := runDoctor(ctx, &buf, "cfg.yaml", &Config{DB: DBConfig{SQLite: "x"}, Runtime: defaultConfig().Runtime}, db)
	require.Error(t, err)
}

func TestDescribeDoctorHelpers(t *testing.T) {
	t.Parallel()
	assert.Equal(t, "up to date", describeSchemaChange(nil))
	assert.Equal(t, "upgraded (added job_runs.kind, job_stats_hourly (table))", describeSchemaChange([]flywheel.SchemaDrift{
		{Table: "job_runs", Column: "kind"}, {Table: "job_stats_hourly"},
	}))
	assert.Equal(t, "in sync", describeIndexDrift(nil))
	assert.Equal(t, "1 drifted (jobs_finished)", describeIndexDrift([]flywheel.IndexDrift{{Name: "jobs_finished"}}))
	assert.Equal(t, "off", describeRollupSetting(0))
	assert.Equal(t, "every 5m0s (in serve)", describeRollupSetting(5*time.Minute))
}

func TestStatsRollupConfig(t *testing.T) {
	t.Parallel()
	cfg, err := LoadConfig(writeConfigFile(t, "runtime:\n  queues: [default]\n"))
	require.NoError(t, err)
	assert.Equal(t, time.Minute, cfg.Runtime.statsRollupInterval(), "unset selects the one-minute default")

	cfg, err = LoadConfig(writeConfigFile(t, "runtime:\n  stats_rollup: 5m\n"))
	require.NoError(t, err)
	assert.Equal(t, 5*time.Minute, cfg.Runtime.statsRollupInterval())

	cfg, err = LoadConfig(writeConfigFile(t, "runtime:\n  stats_rollup: -1s\n"))
	require.NoError(t, err)
	assert.Zero(t, cfg.Runtime.statsRollupInterval(), "a negative value disables the rollup")
}

// TestRunServeKeepsAShortRetentionConfigStarting is the upgrade guarantee for
// the daemon: a config written before the rollup existed, with a retention too
// short for it, still starts — the default rollup steps aside with a warning —
// while a config that asks for the rollup explicitly is told the combination
// cannot work.
func TestRunServeKeepsAShortRetentionConfigStarting(t *testing.T) {
	t.Parallel()
	db := newCLITestDB(t)
	cfg := &Config{Runtime: defaultConfig().Runtime}
	cfg.Runtime.Retention = Duration(30 * time.Minute)
	interval, reason := cfg.Runtime.effectiveStatsRollup()
	assert.Zero(t, interval)
	assert.Contains(t, reason, "runtime.retention")

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	require.NoError(t, runServe(ctx, cfg, db, flywheel.NewSQLiteDriver(db)),
		"an existing short-retention config keeps working after the upgrade")

	cfg.Runtime.StatsRollup = Duration(time.Minute)
	err := runServe(context.Background(), cfg, db, flywheel.NewSQLiteDriver(db))
	require.ErrorIs(t, err, flywheel.ErrValidation,
		"asking for the rollup explicitly with a retention that would delete runs before they are rolled up is refused")
}

func TestCLIServeRunsTheStatsRollup(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	cfg := writeCLIConfig(t, dir, "")
	// Serve the loaded config with a fast rollup cadence so the first pass lands quickly.
	loaded, err := LoadConfig(cfg)
	require.NoError(t, err)
	loaded.Runtime.StatsRollup = Duration(20 * time.Millisecond)
	db, driver, err := openDB(loaded)
	require.NoError(t, err)
	t.Cleanup(func() { closeDB(db) })

	serveCtx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runServe(serveCtx, loaded, db, driver) }()

	// The first pass over an empty history records its watermark, which the doctor
	// lag report and `flywheel stats` coverage both read.
	require.Eventually(t, func() bool {
		out, serr := runRoot(context.Background(), "--config", cfg, "stats", "--since", "1h", "--json")
		if serr != nil {
			return false
		}
		var res flywheel.StatsResult
		return json.Unmarshal([]byte(out), &res) == nil && !res.Coverage.RolledThrough.IsZero()
	}, 8*time.Second, 50*time.Millisecond, "serve rolled up at least once")

	cancel()
	require.NoError(t, <-done)
}
