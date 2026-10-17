//go:build loadtest

package loadtest

import (
	"context"
	"fmt"
	"math"
	"slices"
	"strings"
	"time"

	flywheel "github.com/mrz1836/go-flywheel"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// Bulk-explain fixture shape: a month of jobs across five kinds, two of them
// cohorts of discarded jobs a forced retry replays, and the batch size every
// run walks them in.
const (
	bulkExplainDays      = 30
	bulkExplainBatchSize = 500
)

// bulkCohort is one cohort of discarded jobs the characterization replays: its
// kind, and how often it falls among the seeded jobs, every Every-th job.
type bulkCohort struct {
	Kind  string
	Every int
}

// bulkCohorts are the two cohorts, the small first: at a million jobs, 10,000
// and 100,000.
func bulkCohorts() []bulkCohort {
	return []bulkCohort{{Kind: "cohort.small", Every: 100}, {Kind: "cohort.large", Every: 10}}
}

// BulkExplainReport is one characterization of BulkJobs: the database it ran
// against, the fixture's size, and each run's timing and one batch's plan.
type BulkExplainReport struct {
	Target    string
	Server    string
	Schema    string
	Jobs      int
	JobsBytes int64
	JobsIndex int64
	// KindIndex is jobs_explore_kind's size on the seeded table.
	KindIndex int64
	Runs      []BulkExplainRun
}

// BulkExplainRun is one measured BulkJobs call: a forced retry of one cohort,
// a dry run or an apply, with or without jobs_explore_kind.
type BulkExplainRun struct {
	Cohort string
	// Size is the cohort's job count, which the run's MaxJobs is set to.
	Size    int64
	DryRun  bool
	Index   bool
	Wall    time.Duration
	Matched int64
	Changed int64
	// Batches is the number of batches the walk read; P50, P95, and Max are
	// their times.
	Batches int
	P50     time.Duration
	P95     time.Duration
	Max     time.Duration
	// Plan is one batch's read, past the first batch's cursor, explained.
	Plan ProgressExplainStatement
}

// Name labels the run in the artifact.
func (r BulkExplainRun) Name() string {
	mode := "apply"
	if r.DryRun {
		mode = "dry run"
	}
	return fmt.Sprintf("%s (%d), %s", r.Cohort, r.Size, mode)
}

// ExplainBulk seeds cfg.Jobs jobs server-side and, for each cohort, with
// jobs_explore_kind installed from ExplorerIndexSet and then without it, times a
// forced-retry dry run and a forced-retry apply of the cohort's discarded jobs
// (BatchSize 500, MaxJobs the cohort's size), splits each call into its
// batches' times through a recording GORM logger, and explains one batch's read
// with EXPLAIN (ANALYZE, BUFFERS). An apply's cohort is reset after it runs, so
// every run starts from the same table. The artifact is
// docs/benchmarks/bulk-1m.txt.
//
// It provisions and drops its own schema, like ExplainExplorer.
//
//nolint:gocognit,funlen // one linear characterization: seed, index, vacuum, measure, measure again
func ExplainBulk(ctx context.Context, cfg ExplainConfig) (BulkExplainReport, error) {
	cfg, err := cfg.validate()
	if err != nil {
		return BulkExplainReport{}, err
	}
	report := BulkExplainReport{Target: redactDSN(cfg.DSN), Jobs: cfg.Jobs}
	for _, c := range bulkCohorts() {
		if size := cfg.Jobs / c.Every; size > flywheel.MaxBulkJobs {
			return report, fmt.Errorf("loadtest: the %s cohort of %d jobs is more than MaxBulkJobs (%d): %w",
				c.Kind, size, flywheel.MaxBulkJobs, ErrInvalidConfig)
		}
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

	db, err := openPool(withSearchPath(cfg.DSN, schema)+"&timezone=UTC", 4)
	if err != nil {
		return report, fmt.Errorf("loadtest: open work pool: %w", err)
	}
	defer closePool(db)
	if err = installSchema(ctx, db, IndexesFull, StorageDefault); err != nil {
		return report, err
	}
	end := seedEpoch.Add(bulkExplainDays * 24 * time.Hour)
	if err = seedBulkExplainJobs(ctx, db, cfg.Jobs, end); err != nil {
		return report, err
	}
	// jobs_explore_kind is built over the seeded table, as a host adding it to a
	// live table does, rather than maintained through the seed.
	if err = installExplorerIndexes(ctx, db, []string{"kind"}); err != nil {
		return report, err
	}
	if err = vacuumJobs(ctx, db); err != nil {
		return report, err
	}
	set, err := flywheel.ExplorerIndexSet(db.Name())
	if err != nil {
		return report, fmt.Errorf("loadtest: explorer index set: %w", err)
	}
	i := slices.IndexFunc(set, func(idx flywheel.Index) bool { return idx.Name == "jobs_explore_kind" })
	if i < 0 {
		return report, fmt.Errorf("loadtest: ExplorerIndexSet declares no jobs_explore_kind: %w", ErrInvalidConfig)
	}
	kindIndex := set[i]
	report.Server = scalarString(ctx, db, `SELECT version()`)
	report.JobsBytes = scalarInt(ctx, db, `SELECT pg_total_relation_size('jobs')`)
	report.JobsIndex = scalarInt(ctx, db, `SELECT pg_indexes_size('jobs')`)
	report.KindIndex = scalarInt(ctx, db, `SELECT pg_relation_size('jobs_explore_kind')`)

	sizes := map[string]int64{}
	for _, c := range bulkCohorts() {
		var n int64
		if err = db.WithContext(ctx).Raw(`SELECT count(*) FROM jobs WHERE kind = ?`, c.Kind).Scan(&n).Error; err != nil {
			return report, fmt.Errorf("loadtest: count the %s cohort: %w", c.Kind, err)
		}
		sizes[c.Kind] = n
	}
	for _, present := range []bool{true, false} {
		if !present {
			if err = setExplorerIndex(ctx, db, kindIndex, false); err != nil {
				return report, err
			}
		}
		for _, c := range bulkCohorts() {
			for _, dry := range []bool{true, false} {
				run, read, runErr := measureBulk(ctx, db, c.Kind, sizes[c.Kind], dry, present)
				if runErr != nil {
					return report, runErr
				}
				if !dry {
					if err = resetBulkCohort(ctx, db, c.Kind); err != nil {
						return report, err
					}
				}
				// An apply's read is explained against the reset cohort, which is
				// the table the read ran against.
				if run.Plan, err = explainBulkRead(ctx, db, read); err != nil {
					return report, err
				}
				report.Runs = append(report.Runs, run)
			}
		}
	}
	return report, nil
}

// measureBulk times one forced retry of kind's discarded jobs and splits it into
// its batches, returning the run and the batch read it explains: the second,
// past the first batch's cursor, or the only one.
func measureBulk(
	ctx context.Context, db *gorm.DB, kind string, size int64, dry, index bool,
) (BulkExplainRun, bulkStatement, error) {
	run := BulkExplainRun{Cohort: kind, Size: size, DryRun: dry, Index: index}
	rec := &bulkRecorder{}
	spec := flywheel.BulkSpec{
		Query:  flywheel.JobQuery{Kinds: []string{kind}, States: []flywheel.JobState{flywheel.StateDiscarded}},
		Action: flywheel.BulkRetry, Retry: flywheel.RetryOpts{Force: true},
		DryRun: dry, BatchSize: bulkExplainBatchSize, MaxJobs: int(size),
	}
	start := time.Now()
	res, err := flywheel.BulkJobs(ctx, db.Session(&gorm.Session{Logger: rec}), spec)
	end := time.Now()
	if err != nil {
		return run, bulkStatement{}, fmt.Errorf("loadtest: %s: %w", run.Name(), err)
	}
	run.Wall, run.Matched, run.Changed = end.Sub(start), res.Matched, res.Changed
	spans, reads := bulkBatchSpans(rec.statements, end)
	run.Batches = len(spans)
	run.P50, run.P95, run.Max = percentile(spans, 0.50), percentile(spans, 0.95), percentile(spans, 1)
	if len(reads) == 0 {
		return run, bulkStatement{}, fmt.Errorf("loadtest: %s read no batch", run.Name())
	}
	return run, reads[min(1, len(reads)-1)], nil
}

// bulkStatement is one statement a bulk run issued: its SQL as written, the
// values it bound, and when it ran.
type bulkStatement struct {
	sql   string
	vars  []any
	begin time.Time
}

// bulkRecorder records each statement a bulk run issues, with when it began,
// without rendering its values into its text: a batch's audit insert binds
// thousands, and rendering them would time the logger rather than the batch.
// GORM calls ParamsFilter inside the closure Trace runs, so Trace learns the
// statement before it is rendered, and keeps its values for the batch reads
// alone, which are the statements the artifact explains.
type bulkRecorder struct {
	sql        string
	vars       []any
	statements []bulkStatement
}

// LogMode is part of logger.Interface. The recorder has one mode.
func (r *bulkRecorder) LogMode(logger.LogLevel) logger.Interface { return r }

// Info, Warn, and Error are discarded, as claimSQLRecorder discards them.
func (r *bulkRecorder) Info(context.Context, string, ...any)  {}
func (r *bulkRecorder) Warn(context.Context, string, ...any)  {}
func (r *bulkRecorder) Error(context.Context, string, ...any) {}

// ParamsFilter keeps the statement's text and values, and hands GORM none to
// render.
func (r *bulkRecorder) ParamsFilter(_ context.Context, sql string, params ...any) (string, []any) {
	r.sql, r.vars = sql, nil
	if isBulkBatchRead(sql) {
		r.vars = slices.Clone(params)
	}
	return sql, nil
}

// Trace records the statement and when it began.
func (r *bulkRecorder) Trace(_ context.Context, begin time.Time, fc func() (string, int64), _ error) {
	fc()
	r.statements = append(r.statements, bulkStatement{sql: r.sql, vars: r.vars, begin: begin})
}

// isBulkBatchRead reports whether sql is a bulk walk's batch read, the
// statement each batch starts with: an apply's plain read, or a forced dry
// run's read with its flags over the batch.
func isBulkBatchRead(sql string) bool {
	return strings.HasPrefix(sql, "SELECT jobs.id,jobs.state,") || strings.HasPrefix(sql, "SELECT batch.*")
}

// bulkBatchSpans splits a run's statements into its batches' times: each from
// the start of its read to the start of the next batch's, or for the last to
// end, the call's return. It returns them with the batch reads, in order.
func bulkBatchSpans(statements []bulkStatement, end time.Time) ([]time.Duration, []bulkStatement) {
	var reads []bulkStatement
	for _, s := range statements {
		if isBulkBatchRead(s.sql) {
			reads = append(reads, s)
		}
	}
	spans := make([]time.Duration, len(reads))
	for k, read := range reads {
		next := end
		if k+1 < len(reads) {
			next = reads[k+1].begin
		}
		spans[k] = next.Sub(read.begin)
	}
	return spans, reads
}

// percentile returns the nearest-rank p-th percentile of spans, 0 for none.
func percentile(spans []time.Duration, p float64) time.Duration {
	if len(spans) == 0 {
		return 0
	}
	sorted := slices.Clone(spans)
	slices.Sort(sorted)
	rank := int(math.Ceil(p * float64(len(sorted))))
	return sorted[min(max(rank, 1), len(sorted))-1]
}

// explainBulkRead explains one batch read, its values rendered into it, with
// EXPLAIN (ANALYZE, BUFFERS). The read changes nothing, so it runs as it is.
func explainBulkRead(ctx context.Context, db *gorm.DB, read bulkStatement) (ProgressExplainStatement, error) {
	sql := db.Explain(read.sql, read.vars...)
	var lines []string
	if err := db.WithContext(ctx).Raw("EXPLAIN (ANALYZE, BUFFERS) " + sql).Scan(&lines).Error; err != nil {
		return ProgressExplainStatement{}, fmt.Errorf("loadtest: explain a bulk batch read: %w", err)
	}
	return ProgressExplainStatement{SQL: sql, Plan: lines}, nil
}

// resetBulkCohort returns an applied cohort to where the seed left it: every
// job discarded, finalized a minute after it was created, due when it was; the
// apply's entries truncated, and jobs vacuumed, so the next run starts from the
// same table.
func resetBulkCohort(ctx context.Context, db *gorm.DB, kind string) error {
	if err := db.WithContext(ctx).Exec(`
		UPDATE jobs SET state = 'discarded', finalized_at = created_at + interval '1 minute',
		                scheduled_at = created_at, updated_at = created_at
		WHERE kind = ?`, kind).Error; err != nil {
		return fmt.Errorf("loadtest: reset the %s cohort: %w", kind, err)
	}
	if err := db.WithContext(ctx).Exec(`TRUNCATE job_audit`).Error; err != nil {
		return fmt.Errorf("loadtest: truncate job_audit: %w", err)
	}
	return vacuumJobs(ctx, db)
}

// vacuumJobs vacuums and analyzes jobs, and settles the writes it made.
func vacuumJobs(ctx context.Context, db *gorm.DB) error {
	if err := db.WithContext(ctx).Exec(`VACUUM (ANALYZE) jobs`).Error; err != nil {
		return fmt.Errorf("loadtest: vacuum jobs: %w", err)
	}
	settle(ctx, db)
	return nil
}

// seedBulkExplainJobs writes the characterization's fixture server-side:
//
//   - jobs jobs, UUIDv7-shaped ids spread evenly over the thirty days before end;
//   - every 100th job of kind cohort.small and every 10th of kind cohort.large
//     (10,000 and 100,000 at a million), each discarded on its third of three
//     attempts, spread evenly across the ids;
//   - the rest of kinds mail.send, report.build, and sync.pull, succeeded (75%),
//     discarded (5%), available (10%), retryable (5%), or running (5%);
//   - queues default (69%), bulk (30%), and urgent (1%).
//
// It is fixture, not a measured insert, so it goes through generate_series like
// the explorer characterization's seed, in one transaction so setseed makes the
// insert's random() reproducible.
func seedBulkExplainJobs(ctx context.Context, db *gorm.DB, jobs int, end time.Time) error {
	from := end.Add(-bulkExplainDays * 24 * time.Hour)
	span := float64(bulkExplainDays * 24 * 3600)
	small, large := bulkCohorts()[0], bulkCohorts()[1]
	lastSeed := jobs - 1 // generate_series is inclusive
	err := db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Exec(`SELECT setseed(0.42)`).Error; err != nil {
			return err
		}
		return tx.Exec(`
			INSERT INTO jobs (id, created_at, updated_at, metadata, kind, queue, args, priority, state, attempt,
			                  max_attempts, scheduled_at, executor_class, tags, finalized_at)
			SELECT substr(ms, 1, 8) || '-' || substr(ms, 9, 4) || '-7000-8000-' || lpad(to_hex(g), 12, '0'),
			       at, at, '{}'::jsonb, kind,
			       CASE WHEN qb < 69 THEN 'default' WHEN qb < 99 THEN 'bulk' ELSE 'urgent' END,
			       jsonb_build_object('n', g), 100, state,
			       CASE WHEN state = 'discarded' THEN 3 WHEN state = 'available' THEN 0 ELSE 1 END, 3,
			       at, '', '[]'::jsonb,
			       CASE WHEN state IN ('succeeded', 'discarded') THEN at + interval '1 minute' END
			FROM (
			  SELECT g, at, qb, lpad(to_hex((extract(epoch FROM at) * 1000)::bigint), 12, '0') AS ms,
			         CASE WHEN g % ? = 0 THEN ? WHEN g % ? = ? THEN ?
			              ELSE (ARRAY['mail.send', 'report.build', 'sync.pull'])[(g % 3)::int + 1] END AS kind,
			         CASE WHEN g % ? = 0 OR g % ? = ? THEN 'discarded'
			              WHEN sb < 75 THEN 'succeeded' WHEN sb < 80 THEN 'discarded' WHEN sb < 90 THEN 'available'
			              WHEN sb < 95 THEN 'retryable' ELSE 'running' END AS state
			  FROM (SELECT g, ?::timestamptz + make_interval(secs => g::float8 * ? / ?) AS at,
			               floor(random() * 100)::int AS sb, floor(random() * 100)::int AS qb
			        FROM generate_series(0, ?) AS g) AS seed
			) AS s`,
			// The large cohort takes g = 5 mod 10 and the small one g = 0 mod 100,
			// so no job falls in both.
			small.Every, small.Kind, large.Every, large.Every/2, large.Kind,
			small.Every, large.Every, large.Every/2,
			from, span, jobs, lastSeed).Error
	})
	if err != nil {
		return fmt.Errorf("loadtest: seed bulk jobs: %w", err)
	}
	return nil
}

// Text renders the bulk-explain artifact: the fixture, a table of every run's
// wall time and batch times, then each run's explained batch read.
func (r BulkExplainReport) Text() string {
	var b strings.Builder
	fmt.Fprintf(&b, "bulk controls by filter — %s\n", r.Target)
	if r.Server != "" {
		fmt.Fprintf(&b, "server: %s\n", strings.TrimSpace(r.Server))
	}
	fmt.Fprintf(&b, "schema: %s\n", r.Schema)
	fmt.Fprintf(&b, "fixture: %d jobs over %d days across five kinds: cohort.small (every 100th job) and cohort.large\n"+
		"  (every 10th), each discarded on its last attempt and spread evenly across the ids; and mail.send,\n"+
		"  report.build, and sync.pull: succeeded 75%%, discarded 5%%, available 10%%, retryable 5%%, running 5%%;\n"+
		"  queues default 69%%, bulk 30%%, urgent 1%%\n", r.Jobs, bulkExplainDays)
	fmt.Fprintf(&b, "jobs: %.1f MB total, %.1f MB of indexes; jobs_explore_kind %.1f MB\n",
		mb(r.JobsBytes), mb(r.JobsIndex), mb(r.KindIndex))
	b.WriteString(
		"\nEach run is one BulkJobs call: a forced retry (Retry.Force) of one cohort's discarded jobs,\n" +
			"Query{Kinds: [cohort], States: [discarded]}, in batches of 500 with MaxJobs at the cohort's size,\n" +
			"timed whole by the wall clock. A batch's time runs from the start of its read to the start of the\n" +
			"next batch's read, or to the call's return for the last, so it holds the read and, for an apply, the\n" +
			"batch's transaction: the locked re-read, the update, the audit entries, and the commit. After an\n" +
			"apply, its cohort is reset to discarded, job_audit truncated, and jobs vacuumed, so every run starts\n" +
			"from the same table. \"with\" runs with jobs_explore_kind installed from ExplorerIndexSet; \"without\"\n" +
			"drops it and analyzes jobs. Each plan is one batch's read past the first batch's cursor, explained\n" +
			"with EXPLAIN (ANALYZE, BUFFERS) against the cohort as the run read it.\n\n",
	)
	fmt.Fprintf(&b, "%-30s %-8s %8s %8s %8s %13s %12s %12s %12s\n",
		"run", "index", "matched", "changed", "batches", "wall", "p50 batch", "p95 batch", "max batch")
	for _, run := range r.Runs {
		fmt.Fprintf(&b, "%-30s %-8s %8d %8d %8d %13s %12s %12s %12s\n",
			run.Name(), withOrWithout(run.Index), run.Matched, run.Changed, run.Batches,
			fmtMS(run.Wall), fmtMS(run.P50), fmtMS(run.P95), fmtMS(run.Max))
	}
	for _, run := range r.Runs {
		fmt.Fprintf(&b, "\n=== %s, %s jobs_explore_kind: one batch's read ===\n", run.Name(), withOrWithout(run.Index))
		fmt.Fprintf(&b, "\n%s\n\nplan:\n", strings.TrimSpace(run.Plan.SQL))
		for _, line := range run.Plan.Plan {
			b.WriteString("  ")
			b.WriteString(line)
			b.WriteByte('\n')
		}
	}
	return b.String()
}

// withOrWithout renders whether jobs_explore_kind was installed.
func withOrWithout(index bool) string {
	if index {
		return "with"
	}
	return "without"
}
