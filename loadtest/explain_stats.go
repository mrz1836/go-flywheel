//go:build loadtest

package loadtest

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	flywheel "github.com/mrz1836/go-flywheel"
	"github.com/mrz1836/go-foundation/models"
	"gorm.io/gorm"
)

// Stats-explain fixture shape. The defaults are the plan's acceptance scale: a
// million runs over thirty days across twenty kinds, plus one dense hour of a
// hundred thousand runs for the rollup's per-hour bound.
const (
	statsExplainKinds     = 20
	statsExplainDays      = 30
	statsExplainDenseHour = 100_000
	statsExplainRunning   = 200
	statsExplainReady     = 5_000
	// statsTimingRuns is how many timed calls each read gets after one warm-up;
	// the report publishes the median.
	statsTimingRuns = 7
)

// StatsExplainReport is one stats characterization: the database it ran
// against, how each read API performed, and the plan of every statement each
// emitted.
type StatsExplainReport struct {
	Target      string
	Server      string
	Schema      string
	Runs        int
	Kinds       int
	Days        int
	RunsBytes   int64
	RunsIndex   int64
	LogBytes    int64
	JobsBytes   int64
	StatsRows   int64
	StatsBytes  int64
	RollupAll   time.Duration
	RollupHours int
	Reads       []StatsExplainRead
}

// StatsExplainRead is one read API's measured latency and its statements'
// plans.
type StatsExplainRead struct {
	Name       string
	Median     time.Duration
	Min        time.Duration
	Max        time.Duration
	Statements []ProgressExplainStatement
}

// statsRead is one API call the characterization measures.
type statsRead struct {
	name string
	call func(ctx context.Context, db *gorm.DB) error
}

// ExplainStats seeds cfg.Jobs job_runs (with a job each) across thirty days and
// twenty kinds, rolls every closed hour up, and then — for every read API the
// analytics surface exposes — captures the SQL it emits through a recording
// logger, explains each statement with EXPLAIN (ANALYZE, BUFFERS), and times the
// call. The artifact is docs/benchmarks/stats-plans-1m.txt.
//
// It provisions and drops its own schema, like ExplainClaim. The clock the reads
// see is fixed at the end of the seeded range, so every window is deterministic.
//
//nolint:gocognit,gocyclo,funlen // one linear characterization: seed, roll, vacuum, measure
func ExplainStats(ctx context.Context, cfg ExplainConfig) (StatsExplainReport, error) {
	cfg, err := cfg.validate()
	if err != nil {
		return StatsExplainReport{}, err
	}
	report := StatsExplainReport{
		Target: redactDSN(cfg.DSN), Runs: cfg.Jobs, Kinds: statsExplainKinds, Days: statsExplainDays,
	}

	admin, err := openPool(cfg.DSN, 2)
	if err != nil {
		return report, fmt.Errorf("loadtest: open admin pool: %w", err)
	}
	defer closePool(admin)
	if admin.Name() != "postgres" {
		return report, fmt.Errorf("loadtest: target dialect is %q: %w", admin.Name(), ErrUnsupportedDialect)
	}
	schema := newSchemaName()
	report.Schema = schema
	if err = createSchema(ctx, admin, schema); err != nil {
		return report, err
	}
	defer func() { _ = dropSchema(context.WithoutCancel(ctx), admin, schema) }()

	// The session runs in UTC. A captured statement carries its bound times
	// interpolated as zone-less literals, and every window this characterization
	// binds is a UTC instant; a session in any other zone would shift each literal
	// by its offset and explain an empty window instead of the real one.
	db, err := openPool(withSearchPath(cfg.DSN, schema)+"&timezone=UTC", 4)
	if err != nil {
		return report, fmt.Errorf("loadtest: open work pool: %w", err)
	}
	defer closePool(db)
	if err = installSchema(ctx, db, IndexesFull, StorageTuned); err != nil {
		return report, err
	}

	end := seedEpoch.Add(statsExplainDays * 24 * time.Hour)
	if err = seedStatsHistory(ctx, db, cfg.Jobs, end); err != nil {
		return report, err
	}
	clockCtx := models.WithClock(ctx, models.NewFixedClock(end))

	start := time.Now()
	rebuilt, err := flywheel.RebuildStats(clockCtx, db, flywheel.RebuildOpts{
		From: seedEpoch, To: end, Force: true,
	})
	if err != nil {
		return report, fmt.Errorf("loadtest: roll up history: %w", err)
	}
	report.RollupAll = time.Since(start)
	report.RollupHours = rebuilt.Hours

	for _, table := range []string{"jobs", "job_runs", "job_run_finishes", "job_stats_hourly"} {
		if err = db.WithContext(ctx).Exec(`VACUUM (ANALYZE) ` + table).Error; err != nil {
			return report, fmt.Errorf("loadtest: vacuum %s: %w", table, err)
		}
	}
	report.Server = scalarString(ctx, db, `SELECT version()`)
	report.RunsBytes = scalarInt(ctx, db, `SELECT pg_total_relation_size('job_runs')`)
	report.RunsIndex = scalarInt(ctx, db, `SELECT pg_indexes_size('job_runs')`)
	report.LogBytes = scalarInt(ctx, db, `SELECT pg_total_relation_size('job_run_finishes')`)
	report.JobsBytes = scalarInt(ctx, db, `SELECT pg_total_relation_size('jobs')`)
	report.StatsRows = scalarInt(ctx, db, `SELECT count(*) FROM job_stats_hourly`)
	report.StatsBytes = scalarInt(ctx, db, `SELECT pg_total_relation_size('job_stats_hourly')`)

	// The Scheduler the rollup-pass row times has made its start-up pass — the
	// backfill check and the retention boundary — so what it times is the pass
	// a caught-up rollup runs every StatsRollupInterval.
	sched, err := flywheel.NewSchedulerWithConfig(flywheel.SchedulerConfig{
		DB: db, Client: flywheel.NewClient(db), Driver: flywheel.NewPostgresDriver(db),
		StatsRollupInterval: time.Minute,
	})
	if err != nil {
		return report, fmt.Errorf("loadtest: rollup scheduler: %w", err)
	}
	if _, err = sched.RollupStats(clockCtx); err != nil {
		return report, fmt.Errorf("loadtest: start-up rollup pass: %w", err)
	}

	denseHour := seedEpoch.Add(15*24*time.Hour + 12*time.Hour)
	typicalHour := seedEpoch.Add(20*24*time.Hour + 9*time.Hour)
	for _, r := range statsReads(end, typicalHour, denseHour, sched) {
		read, readErr := measureStatsRead(clockCtx, db, r)
		if readErr != nil {
			return report, readErr
		}
		report.Reads = append(report.Reads, read)
	}
	return report, nil
}

// statsReads lists every read the characterization measures, as the APIs a
// dashboard calls — nothing here retypes their SQL — and the two maintenance
// calls a running Scheduler makes unprompted: its rollup pass, through sched,
// and the backfill check every Scheduler start runs.
func statsReads(now, typicalHour, denseHour time.Time, sched *flywheel.Scheduler) []statsRead {
	day := 24 * time.Hour
	return []statsRead{
		{"Stats 24h", func(ctx context.Context, db *gorm.DB) error {
			_, err := flywheel.Stats(ctx, db, flywheel.StatsParams{From: now.Add(-day), To: now})
			return err
		}},
		{"Stats 30d", func(ctx context.Context, db *gorm.DB) error {
			_, err := flywheel.Stats(ctx, db, flywheel.StatsParams{From: now.Add(-statsExplainDays * day), To: now})
			return err
		}},
		{"Stats 24h, one kind", func(ctx context.Context, db *gorm.DB) error {
			_, err := flywheel.Stats(ctx, db, flywheel.StatsParams{From: now.Add(-day), To: now, Kind: "kind-7"})
			return err
		}},
		{"StatsSeries 30d hourly", func(ctx context.Context, db *gorm.DB) error {
			_, err := flywheel.StatsSeries(ctx, db, flywheel.SeriesParams{From: now.Add(-statsExplainDays * day), To: now})
			return err
		}},
		{"StatsSeries 30d daily", func(ctx context.Context, db *gorm.DB) error {
			_, err := flywheel.StatsSeries(ctx, db, flywheel.SeriesParams{
				From: now.Add(-statsExplainDays * day), To: now, Interval: flywheel.IntervalDay,
			})
			return err
		}},
		{"Baselines 7d", func(ctx context.Context, db *gorm.DB) error {
			_, err := flywheel.Baselines(ctx, db, 7*day)
			return err
		}},
		{"Anomalies (latest hour)", func(ctx context.Context, db *gorm.DB) error {
			_, err := flywheel.Anomalies(ctx, db, flywheel.AnomalyParams{})
			return err
		}},
		{"ListRunning (with baseline)", func(ctx context.Context, db *gorm.DB) error {
			_, err := flywheel.ListRunning(ctx, db, flywheel.ListRunningParams{WithBaseline: true})
			return err
		}},
		{"ListFinished", func(ctx context.Context, db *gorm.DB) error {
			_, err := flywheel.ListFinished(ctx, db, flywheel.ListFinishedParams{})
			return err
		}},
		{"RecentFailures", func(ctx context.Context, db *gorm.DB) error {
			_, err := flywheel.RecentFailures(ctx, db, flywheel.RecentFailuresParams{})
			return err
		}},
		{"SlowRuns 24h", func(ctx context.Context, db *gorm.DB) error {
			_, err := flywheel.SlowRuns(ctx, db, flywheel.SlowRunsParams{})
			return err
		}},
		{"QueueDepths", func(ctx context.Context, db *gorm.DB) error {
			_, err := flywheel.QueueDepths(ctx, db)
			return err
		}},
		{"CountActiveByKind", func(ctx context.Context, db *gorm.DB) error {
			_, err := flywheel.CountActiveByKind(ctx, db)
			return err
		}},
		{"ListJobs", func(ctx context.Context, db *gorm.DB) error {
			_, err := flywheel.ListJobs(ctx, db, flywheel.ListJobsParams{})
			return err
		}},
		{"Rollup pass, caught up", func(ctx context.Context, _ *gorm.DB) error {
			_, err := sched.RollupStats(ctx)
			return err
		}},
		{"BackfillRunFinishes, none to log", func(ctx context.Context, db *gorm.DB) error {
			_, err := flywheel.BackfillRunFinishes(ctx, db)
			return err
		}},
		{"Roll up one hour (~1.4k runs)", func(ctx context.Context, db *gorm.DB) error {
			_, err := flywheel.RebuildStats(ctx, db, flywheel.RebuildOpts{
				From: typicalHour, To: typicalHour.Add(time.Hour), Force: true,
			})
			return err
		}},
		{"Roll up one hour of 100k runs", func(ctx context.Context, db *gorm.DB) error {
			_, err := flywheel.RebuildStats(ctx, db, flywheel.RebuildOpts{
				From: denseHour, To: denseHour.Add(time.Hour), Force: true,
			})
			return err
		}},
	}
}

// measureStatsRead captures one read's statements, explains each, and times the
// call: one warm-up, then statsTimingRuns timed calls.
func measureStatsRead(ctx context.Context, db *gorm.DB, r statsRead) (StatsExplainRead, error) {
	out := StatsExplainRead{Name: r.name}
	rec := &claimSQLRecorder{}
	if err := r.call(ctx, db.Session(&gorm.Session{Logger: rec})); err != nil {
		return out, fmt.Errorf("loadtest: %s: %w", r.name, err)
	}
	for _, sql := range rec.all {
		if !strings.HasPrefix(strings.ToUpper(strings.TrimSpace(sql)), "SELECT") {
			continue
		}
		var lines []string
		if err := db.WithContext(ctx).Raw("EXPLAIN (ANALYZE, BUFFERS) " + sql).Scan(&lines).Error; err != nil {
			return out, fmt.Errorf("loadtest: explain %s: %w", r.name, err)
		}
		out.Statements = append(out.Statements, ProgressExplainStatement{SQL: sql, Plan: lines})
	}
	times := make([]time.Duration, 0, statsTimingRuns)
	for i := range statsTimingRuns + 1 {
		start := time.Now()
		if err := r.call(ctx, db); err != nil {
			return out, fmt.Errorf("loadtest: %s: %w", r.name, err)
		}
		if i > 0 {
			times = append(times, time.Since(start))
		}
	}
	slices.Sort(times)
	out.Median, out.Min, out.Max = times[len(times)/2], times[0], times[len(times)-1]
	return out, nil
}

// seedStatsHistory writes the characterization's fixture server-side:
//
//   - runs job_runs rows spread evenly over the thirty days before end, each with
//     a terminal job — twenty kinds whose median duration steps from 40 ms up by
//     ×1.35 per kind, lognormal (σ 0.6), with a realistic outcome mix: 90%
//     success, 5% retried errors, 2% discards, 1% timeouts, 1% crashes, 1%
//     snoozes;
//   - a dense hour of statsExplainDenseHour more runs of one kind, the rollup's
//     per-hour bound;
//   - statsExplainRunning running jobs with their stubs, and statsExplainReady
//     ready jobs across three queues, for the live reads.
//
// It is fixture, not a measured insert, so it goes through generate_series like
// the progress characterization's seed. setseed makes random() reproducible.
func seedStatsHistory(ctx context.Context, db *gorm.DB, runs int, end time.Time) error {
	from := end.Add(-statsExplainDays * 24 * time.Hour)
	spanSeconds := float64(statsExplainDays * 24 * 3600)
	exec := func(what, sql string, args ...any) error {
		if err := db.WithContext(ctx).Exec(sql, args...).Error; err != nil {
			return fmt.Errorf("loadtest: seed %s: %w", what, err)
		}
		return nil
	}
	if err := exec("setseed", `SELECT setseed(0.42)`); err != nil {
		return err
	}
	// One temp table drives both inserts, so a job and its run agree on kind,
	// outcome, and timing.
	if err := exec("plan", `
		CREATE TEMP TABLE stats_seed AS
		SELECT g,
		       'sr-' || lpad(g::text, 8, '0') AS id,
		       'kind-' || (g % ?) AS kind,
		       (ARRAY['default','bulk','priority'])[1 + (g % 3)] AS queue,
		       g % 100 AS bucket,
		       ?::timestamptz + make_interval(secs => (g::float8 / ?) * ?) AS finished_at,
		       exp(ln(40 * power(1.35, g % ?)) + 0.6 * sqrt(-2 * ln(1 - random())) * cos(2 * pi() * random())) AS dur
		FROM generate_series(1, ?) AS g`,
		statsExplainKinds, from, runs, spanSeconds, statsExplainKinds, runs); err != nil {
		return err
	}
	if err := exec("dense hour", `
		INSERT INTO stats_seed
		SELECT g, 'sd-' || lpad(g::text, 8, '0'), 'kind-dense', 'default', g % 100,
		       ?::timestamptz + make_interval(secs => (g::float8 / ?) * 3600),
		       exp(ln(200) + 0.5 * sqrt(-2 * ln(1 - random())) * cos(2 * pi() * random()))
		FROM generate_series(1, ?) AS g`,
		from.Add(15*24*time.Hour+12*time.Hour), statsExplainDenseHour, statsExplainDenseHour); err != nil {
		return err
	}
	// bucket → outcome: 0–89 success, 90–94 retried error, 95–96 discard, 97
	// timeout, 98 crash, 99 snooze.
	if err := exec("jobs", `
		INSERT INTO jobs (id, created_at, updated_at, metadata, kind, queue, args, priority, state,
		                  attempt, max_attempts, scheduled_at, executor_class, tags, finalized_at)
		SELECT id, finished_at - interval '1 minute', finished_at, '{}'::jsonb, kind, queue, '{}'::jsonb, 100,
		       CASE WHEN bucket BETWEEN 95 AND 96 THEN 'discarded' ELSE 'succeeded' END,
		       1, 25, finished_at - interval '1 minute', '', '[]'::jsonb, finished_at
		FROM stats_seed`); err != nil {
		return err
	}
	if err := exec("runs", `
		INSERT INTO job_runs (id, job_id, attempt, kind, queue, executor_class, executor_id, started_at,
		                      finished_at, outcome, error_class, duration_ms, queue_wait_ms, cost_micros,
		                      enqueued_children, job_state, superseded, created_at)
		SELECT id || '-r', id, 1, kind, queue, '', 'seed', finished_at - make_interval(secs => dur / 1000),
		       finished_at,
		       CASE WHEN bucket < 90 THEN 'success' WHEN bucket < 97 THEN 'error' WHEN bucket = 97 THEN 'timeout'
		            WHEN bucket = 98 THEN 'crashed' ELSE 'snooze' END,
		       CASE WHEN bucket BETWEEN 90 AND 97 THEN 'transient' END,
		       CASE WHEN bucket = 98 THEN NULL ELSE round(dur)::int END,
		       (random() * 2000)::int, (random() * 500)::bigint, 0,
		       CASE WHEN bucket < 90 THEN 'succeeded' WHEN bucket BETWEEN 95 AND 96 THEN 'discarded'
		            WHEN bucket = 98 THEN 'available' WHEN bucket = 99 THEN 'scheduled' ELSE 'retryable' END,
		       false, finished_at - make_interval(secs => dur / 1000)
		FROM stats_seed`); err != nil {
		return err
	}
	// The finish log, written as the runtime writes it — read back off the run
	// rows — so the window reads find the history the way they find a live one.
	if err := exec("finish log", `
		INSERT INTO job_run_finishes (finished_at, run_id)
		SELECT finished_at, id FROM job_runs WHERE finished_at IS NOT NULL`); err != nil {
		return err
	}
	if err := exec("running jobs", `
		INSERT INTO jobs (id, created_at, updated_at, metadata, kind, queue, args, priority, state,
		                  attempt, max_attempts, scheduled_at, leased_until, lease_token, executor_class, tags)
		SELECT 'run-' || g, ?, ?, '{}'::jsonb, 'kind-' || (g % ?), 'default', '{}'::jsonb, 100, 'running',
		       1, 25, ?, ?::timestamptz + interval '30 seconds', 'tok', '', '[]'::jsonb
		FROM generate_series(1, ?) AS g`,
		end.Add(-time.Hour), end, statsExplainKinds, end.Add(-time.Hour), end, statsExplainRunning); err != nil {
		return err
	}
	if err := exec("running stubs", `
		INSERT INTO job_runs (id, job_id, attempt, kind, queue, executor_class, executor_id, started_at,
		                      outcome, enqueued_children, superseded, created_at)
		SELECT 'run-' || g || '-r', 'run-' || g, 1, 'kind-' || (g % ?), 'default', '', 'worker-1',
		       ?::timestamptz - make_interval(secs => g * 10), 'started', 0, false,
		       ?::timestamptz - make_interval(secs => g * 10)
		FROM generate_series(1, ?) AS g`,
		statsExplainKinds, end, end, statsExplainRunning); err != nil {
		return err
	}
	return exec("ready jobs", `
		INSERT INTO jobs (id, created_at, updated_at, metadata, kind, queue, args, priority, state,
		                  attempt, max_attempts, scheduled_at, executor_class, tags)
		SELECT 'ready-' || g, ?, ?, '{}'::jsonb, 'kind-' || (g % ?), (ARRAY['default','bulk','priority'])[1 + (g % 3)],
		       '{}'::jsonb, 100, 'available', 0, 25, ?::timestamptz - make_interval(secs => g), '', '[]'::jsonb
		FROM generate_series(1, ?) AS g`,
		end, end, statsExplainKinds, end, statsExplainReady)
}

// Text renders the stats-explain artifact: the fixture, a latency table, then
// every captured statement and its plan.
func (r StatsExplainReport) Text() string {
	var b strings.Builder
	fmt.Fprintf(&b, "stats read plans — %s\n", r.Target)
	if r.Server != "" {
		fmt.Fprintf(&b, "server: %s\n", strings.TrimSpace(r.Server))
	}
	fmt.Fprintf(&b, "schema: %s\n", r.Schema)
	fmt.Fprintf(&b, "fixture: %d runs over %d days across %d kinds, plus one hour of %d runs; %d running, %d ready\n",
		r.Runs, r.Days, r.Kinds, statsExplainDenseHour, statsExplainRunning, statsExplainReady)
	fmt.Fprintf(&b, "job_runs: %.1f MB total, %.1f MB of indexes; job_run_finishes: %.1f MB; jobs: %.1f MB total\n",
		mb(r.RunsBytes), mb(r.RunsIndex), mb(r.LogBytes), mb(r.JobsBytes))
	fmt.Fprintf(&b, "job_stats_hourly: %d rows, %.1f MB, rolled up from scratch in %s (%d hours)\n",
		r.StatsRows, mb(r.StatsBytes), r.RollupAll.Round(time.Millisecond), r.RollupHours)
	b.WriteString(
		"\nEvery statement was captured from the flywheel read API named above it through a recording GORM\n" +
			"logger and explained verbatim with EXPLAIN (ANALYZE, BUFFERS), after VACUUM (ANALYZE). The clock is\n" +
			"fixed at the end of the seeded range: the rollups cover every closed hour, and the hour in\n" +
			"progress is read raw. Latency is the wall time of the whole API call — every statement plus the\n" +
			"Go-side merge — as the median of 7 calls after a warm-up. The rollup pass runs through a Scheduler\n" +
			"that has made its start-up pass, on its own handle, so its statements (primary-key reads of\n" +
			"job_stats_progress and an empty range delete) are not captured.\n\n",
	)
	fmt.Fprintf(&b, "%-36s %10s %10s %10s %6s\n", "read", "median", "min", "max", "stmts")
	for _, read := range r.Reads {
		fmt.Fprintf(&b, "%-36s %10s %10s %10s %6d\n", read.Name,
			fmtMS(read.Median), fmtMS(read.Min), fmtMS(read.Max), len(read.Statements))
	}
	for _, read := range r.Reads {
		fmt.Fprintf(&b, "\n=== %s ===\n", read.Name)
		for i, stmt := range read.Statements {
			fmt.Fprintf(&b, "\n--- statement %d ---\n%s\n\nplan:\n", i+1, strings.TrimSpace(stmt.SQL))
			for _, line := range stmt.Plan {
				b.WriteString("  ")
				b.WriteString(line)
				b.WriteByte('\n')
			}
		}
	}
	return b.String()
}

// mb renders bytes as megabytes.
func mb(n int64) float64 { return float64(n) / (1 << 20) }

// fmtMS renders a duration in milliseconds with three decimals.
func fmtMS(d time.Duration) string {
	return fmt.Sprintf("%.3fms", float64(d.Microseconds())/1000)
}
