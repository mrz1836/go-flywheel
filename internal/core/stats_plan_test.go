package core

import (
	"context"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// sqlRecorder is a GORM logger that records every statement it is shown, with
// its values interpolated, so a plan test can EXPLAIN exactly what an API
// emitted rather than a hand-written copy that drifts from it.
type sqlRecorder struct {
	logger.Interface
	mu   sync.Mutex
	stmt []string
}

// Trace records the statement.
func (r *sqlRecorder) Trace(_ context.Context, _ time.Time, fc func() (string, int64), _ error) {
	sql, _ := fc()
	r.mu.Lock()
	r.stmt = append(r.stmt, sql)
	r.mu.Unlock()
}

// LogMode keeps the recorder installed whatever level a caller asks for.
func (r *sqlRecorder) LogMode(logger.LogLevel) logger.Interface { return r }

// captureSQL runs fn against a session of db that records its statements, and
// returns the SELECTs it issued.
func captureSQL(t *testing.T, db *gorm.DB, fn func(db *gorm.DB)) []string {
	t.Helper()
	rec := &sqlRecorder{Interface: logger.Discard}
	fn(db.Session(&gorm.Session{Logger: rec}))
	rec.mu.Lock()
	defer rec.mu.Unlock()
	var out []string
	for _, s := range rec.stmt {
		if strings.HasPrefix(strings.ToUpper(strings.TrimSpace(s)), "SELECT") {
			out = append(out, s)
		}
	}
	require.NotEmpty(t, out, "the API issued no SELECT")
	return out
}

// planReader is one new read API driven for its statements.
type planReader struct {
	name string
	run  func(t *testing.T, db *gorm.DB)
	// orderedPage marks a read whose plan must also avoid a sort: its index
	// hands rows back in page order, which is the whole point of the index.
	orderedPage bool
	// pkWalk marks a read whose intended plan is an ordered primary-key walk
	// that stops at LIMIT, which SQLite reports as a SCAN.
	pkWalk bool
}

// planReaders drives every new read API once, against a database carrying data
// in every shape it reads.
//
//nolint:gochecknoglobals // shared fixture
var planReaders = []planReader{
	{name: "Stats raw tail", run: func(t *testing.T, db *gorm.DB) {
		_, err := Stats(context.Background(), db, StatsParams{From: rollupBase, To: rollupBase.Add(time.Hour), MaxRawSpan: -1})
		require.NoError(t, err)
	}},
	{name: "Stats rolled hours", run: func(t *testing.T, db *gorm.DB) {
		_, err := Stats(context.Background(), db, StatsParams{From: rollupBase.Add(-48 * time.Hour), To: rollupBase, Kind: "a"})
		require.NoError(t, err)
	}},
	{name: "StatsSeries", run: func(t *testing.T, db *gorm.DB) {
		_, err := StatsSeries(context.Background(), db, SeriesParams{From: rollupBase.Add(-6 * time.Hour), To: rollupBase.Add(2 * time.Hour)})
		require.NoError(t, err)
	}},
	{name: "rollup pass", run: func(t *testing.T, db *gorm.DB) {
		_, err := rollupPass(context.Background(), db, rollupBase.Add(3*time.Hour),
			rollupConfig{grace: defaultStatsRollupGrace, maxHours: 2, retention: defaultStatsRetention})
		require.NoError(t, err)
	}},
	{name: "Baselines", run: func(t *testing.T, db *gorm.DB) {
		_, err := Baselines(context.Background(), db, 0)
		require.NoError(t, err)
	}},
	{name: "Anomalies", run: func(t *testing.T, db *gorm.DB) {
		_, err := Anomalies(context.Background(), db, AnomalyParams{})
		require.NoError(t, err)
	}},
	{name: "ListRunning", run: func(t *testing.T, db *gorm.DB) {
		_, err := ListRunning(context.Background(), db, ListRunningParams{})
		require.NoError(t, err)
	}},
	{name: "ListFinished", orderedPage: true, run: func(t *testing.T, db *gorm.DB) {
		_, err := ListFinished(context.Background(), db, ListFinishedParams{
			Before: &FinishedCursor{FinalizedAt: rollupBase, ID: "zzz"},
		})
		require.NoError(t, err)
	}},
	{name: "RecentFailures", orderedPage: true, run: func(t *testing.T, db *gorm.DB) {
		_, err := RecentFailures(context.Background(), db, RecentFailuresParams{})
		require.NoError(t, err)
	}},
	{name: "SlowRuns", run: func(t *testing.T, db *gorm.DB) {
		_, err := SlowRuns(fixedClockCtx(rollupBase), db, SlowRunsParams{Kind: "a"})
		require.NoError(t, err)
	}},
	{name: "QueueDepths", run: func(t *testing.T, db *gorm.DB) {
		_, err := QueueDepths(fixedClockCtx(rollupBase), db)
		require.NoError(t, err)
	}},
	{name: "CountActiveByKind", run: func(t *testing.T, db *gorm.DB) {
		_, err := CountActiveByKind(context.Background(), db)
		require.NoError(t, err)
	}},
	{name: "ListJobs", orderedPage: true, pkWalk: true, run: func(t *testing.T, db *gorm.DB) {
		_, err := ListJobs(context.Background(), db, ListJobsParams{BeforeID: "zzz"})
		require.NoError(t, err)
	}},
}

// seedPlanData gives every reader something to read: rolled history, a raw
// tail, running and ready jobs, and a legacy row so the kind-resolving subquery
// is in the plan.
func seedPlanData(t *testing.T, db *gorm.DB) {
	t.Helper()
	var runs []histRun
	for i := range 48 {
		runs = append(runs, okRun("a", rollupBase.Add(-time.Duration(i)*time.Hour), 100), discardRun("b", rollupBase.Add(-time.Duration(i)*time.Hour)))
	}
	legacy := okRun("a", rollupBase.Add(30*time.Minute), 100)
	legacy.Legacy = true
	runs = append(runs, legacy)
	seedHistory(t, db, runs)
	rollupAll(t, db, rollupBase)
	seedJob(t, db, jobRow{ID: "ready", Kind: "a", State: string(StateAvailable), ScheduledAt: rollupBase.Add(-time.Hour)})
	seedJob(t, db, jobRow{ID: "running", Kind: "a", State: string(StateRunning), ScheduledAt: rollupBase.Add(-time.Hour)})
}

// fullScanRe matches a SQLite plan line that walks a runtime table rather than
// searching it. indexWalkRe matches the subset that walks an index in order,
// which is the intended plan for an ORDER BY … LIMIT read — the watermark probe
// reads one entry off the end of the primary key — and only for one.
var (
	fullScanRe  = regexp.MustCompile(`(?m)^SCAN (jobs|job_runs|job_run_finishes|job_stats_hourly|r|j|f)\b.*$`)
	indexWalkRe = regexp.MustCompile(`^SCAN \S+ USING (COVERING )?INDEX `)
)

// fullScans returns the plan's full-scan lines, excusing an ordered index walk
// in a statement that stops at a LIMIT.
func fullScans(plan, stmt string) []string {
	limited := strings.Contains(strings.ToUpper(stmt), " LIMIT ")
	var out []string
	for _, line := range fullScanRe.FindAllString(plan, -1) {
		if limited && indexWalkRe.MatchString(line) {
			continue
		}
		out = append(out, line)
	}
	return out
}

// TestNewReadsNeverScanATableSQLite pins every new read's SQLite plan: each
// statement it emits searches an index — no full SCAN of jobs, job_runs, or
// job_stats_hourly — and the paged reads come back in index order, with no temp
// B-tree sort. It EXPLAINs the statements the API actually issued.
func TestNewReadsNeverScanATableSQLite(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	seedPlanData(t, db)
	for _, r := range planReaders {
		t.Run(r.name, func(t *testing.T) {
			for _, stmt := range captureSQL(t, db, func(db *gorm.DB) { r.run(t, db) }) {
				plan := sqlitePlan(t, db, stmt)
				if !r.pkWalk {
					assert.Empty(t, fullScans(plan, stmt), "statement:\n%s\nplan:\n%s", stmt, plan)
				}
				// The page itself comes off jobs in index order; a follow-up read of
				// the page's runs sorts a page-sized set and is not the concern.
				if r.orderedPage && strings.Contains(stmt, "FROM `jobs`") {
					assert.NotContains(t, plan, "TEMP B-TREE FOR ORDER BY", "statement:\n%s", stmt)
				}
			}
		})
	}
}

// TestStatsReadsUseTheirIndexesSQLite names the index behind each hot read, so
// a regression to a different access path is visible even when it is not a
// full scan.
func TestStatsReadsUseTheirIndexesSQLite(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	from, to := rollupBase, rollupBase.Add(time.Hour)
	plan := sqlitePlan(t, db, runWindowSQL, from, to)
	assert.Regexp(t, `SEARCH f USING (COVERING )?INDEX sqlite_autoindex_job_run_finishes_1 \(finished_at>\? AND finished_at<\?\)`, plan,
		"the window aggregate is a range of the finish log's primary key")
	assert.Regexp(t, `SEARCH r USING INDEX sqlite_autoindex_job_runs_1 \(id=\?\)`, plan,
		"each finish reaches its run by job_runs' primary key")
	assert.Regexp(t, `SEARCH j USING (INTEGER PRIMARY KEY|INDEX sqlite_autoindex_jobs_1)`, plan,
		"a legacy row's kind resolves by the jobs primary key")

	plan = sqlitePlan(t, db,
		`SELECT * FROM jobs WHERE state = 'discarded' AND finalized_at IS NOT NULL AND jobs.deleted_at IS NULL
		 ORDER BY finalized_at DESC, id DESC LIMIT 20`)
	assert.Contains(t, plan, "jobs_finished")
	assert.NotContains(t, plan, "TEMP B-TREE", "the id tie-break is in the index, so there is no sort")

	plan = sqlitePlan(t, db,
		`SELECT finished_at FROM job_run_finishes WHERE finished_at >= '2026-09-01 00:00:00+00:00' ORDER BY finished_at LIMIT 1`)
	assert.Contains(t, plan, "sqlite_autoindex_job_run_finishes_1", "the rollup's next-run probe")
	assert.NotContains(t, plan, "TEMP B-TREE", "and it reads the key in order")
}
