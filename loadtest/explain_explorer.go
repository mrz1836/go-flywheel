//go:build loadtest

package loadtest

import (
	"context"
	"fmt"
	"regexp"
	"slices"
	"strings"
	"time"

	flywheel "github.com/mrz1836/go-flywheel"
	"github.com/mrz1836/go-foundation/models"
	"gorm.io/gorm"
)

// Explorer-explain fixture shape: a month of jobs across twenty kinds, and one
// dedicated kind of exactly the count's default cap, so a count of it is exact
// at the cap.
const (
	explorerExplainDays    = 30
	explorerExplainKinds   = 20
	explorerExplainParents = 100
	explorerCapKindJobs    = flywheel.DefaultCountCap
)

// ExplorerExplainReport is one characterization of the job query: the database
// it ran against, the fixture's size, and each read's latency and plans, with
// and without the explorer index its shape is meant for.
type ExplorerExplainReport struct {
	Target     string
	Server     string
	Schema     string
	Jobs       int
	JobsBytes  int64
	JobsIndex  int64
	IndexSizes []ExplorerIndexSize
	Reads      []ExplorerExplainRead
}

// ExplorerIndexSize is one explorer index's size on the seeded table.
type ExplorerIndexSize struct {
	Name  string
	Bytes int64
}

// ExplorerExplainRead is one read: its measurement with every explorer index
// installed, and, for a shape an explorer index serves, without that index.
type ExplorerExplainRead struct {
	Name string
	// Index is the index the shape is meant to reach; empty for a count or a
	// detail read, whose plan is the planner's choice.
	Index   string
	With    StatsExplainRead
	Without *StatsExplainRead
}

// explorerShape is one read the characterization measures.
type explorerShape struct {
	name  string
	index string
	// explorer marks a shape whose index is an explorer index, measured again
	// without it.
	explorer bool
	call     func(ctx context.Context, db *gorm.DB) error
}

// ExplainExplorer seeds cfg.Jobs jobs server-side, installs the explorer index
// set, and, for each shape of the job query's plans and each capped count,
// captures the statements QueryJobs or CountJobs emits, explains each with
// EXPLAIN (ANALYZE, BUFFERS), and times the call: one warm-up, then the median of
// seven. Each explorer index is then dropped, the table analyzed, its shapes
// measured again, and the index rebuilt. GetJob is timed beside them. The
// artifact is docs/benchmarks/explorer-plans-1m.txt.
//
// It provisions and drops its own schema, like ExplainStats.
//
//nolint:gocognit,funlen // one linear characterization: seed, index, vacuum, measure, measure again
func ExplainExplorer(ctx context.Context, cfg ExplainConfig) (ExplorerExplainReport, error) {
	cfg, err := cfg.validate()
	if err != nil {
		return ExplorerExplainReport{}, err
	}
	report := ExplorerExplainReport{Target: redactDSN(cfg.DSN), Jobs: cfg.Jobs}

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

	// UTC, as ExplainStats explains: a captured statement carries its bound times
	// as zone-less literals.
	db, err := openPool(withSearchPath(cfg.DSN, schema)+"&timezone=UTC", 4)
	if err != nil {
		return report, fmt.Errorf("loadtest: open work pool: %w", err)
	}
	defer closePool(db)
	if err = installSchema(ctx, db, IndexesFull, StorageDefault); err != nil {
		return report, err
	}

	end := seedEpoch.Add(explorerExplainDays * 24 * time.Hour)
	if err = seedExplorerJobs(ctx, db, cfg.Jobs, end); err != nil {
		return report, err
	}
	// The explorer indexes are built over the seeded table, as a host adding one
	// to a live table does, rather than maintained through the seed.
	if err = installExplorerIndexes(ctx, db, ExplorerIndexNames()); err != nil {
		return report, err
	}
	for _, table := range []string{"jobs", "job_runs"} {
		if err = db.WithContext(ctx).Exec(`VACUUM (ANALYZE) ` + table).Error; err != nil {
			return report, fmt.Errorf("loadtest: vacuum %s: %w", table, err)
		}
	}
	settle(ctx, db)
	set, err := flywheel.ExplorerIndexSet(db.Name())
	if err != nil {
		return report, fmt.Errorf("loadtest: explorer index set: %w", err)
	}
	report.Server = scalarString(ctx, db, `SELECT version()`)
	report.JobsBytes = scalarInt(ctx, db, `SELECT pg_total_relation_size('jobs')`)
	report.JobsIndex = scalarInt(ctx, db, `SELECT pg_indexes_size('jobs')`)
	for _, idx := range set {
		report.IndexSizes = append(report.IndexSizes, ExplorerIndexSize{
			Name:  idx.Name,
			Bytes: scalarInt(ctx, db, `SELECT pg_relation_size('`+idx.Name+`')`),
		})
	}

	running := scalarString(ctx, db, `SELECT id FROM jobs WHERE state = 'running' ORDER BY id LIMIT 1`)
	clockCtx := models.WithClock(ctx, models.NewFixedClock(end))
	shapes := explorerShapes(end, running)
	for _, sh := range shapes {
		read, readErr := measureStatsRead(clockCtx, db, statsRead{name: sh.name, call: sh.call})
		if readErr != nil {
			return report, readErr
		}
		report.Reads = append(report.Reads, ExplorerExplainRead{Name: sh.name, Index: sh.index, With: read})
	}

	for _, idx := range set {
		if err = setExplorerIndex(ctx, db, idx, false); err != nil {
			return report, err
		}
		for i, sh := range shapes {
			if !sh.explorer || sh.index != idx.Name {
				continue
			}
			read, readErr := measureStatsRead(clockCtx, db, statsRead{name: sh.name, call: sh.call})
			if readErr != nil {
				return report, readErr
			}
			report.Reads[i].Without = &read
		}
		if err = setExplorerIndex(ctx, db, idx, true); err != nil {
			return report, err
		}
	}
	return report, nil
}

// setExplorerIndex drops or rebuilds one explorer index, then analyzes jobs, so
// the planner plans the next measurement against the schema as it now stands,
// and settles the writes it made.
func setExplorerIndex(ctx context.Context, db *gorm.DB, idx flywheel.Index, present bool) error {
	stmt := `DROP INDEX ` + idx.Name
	if present {
		stmt = idx.DDL
	}
	if err := db.WithContext(ctx).Exec(stmt).Error; err != nil {
		return fmt.Errorf("loadtest: %s: %w", stmt, err)
	}
	if err := db.WithContext(ctx).Exec(`ANALYZE jobs`).Error; err != nil {
		return fmt.Errorf("loadtest: analyze jobs: %w", err)
	}
	settle(ctx, db)
	return nil
}

// settle checkpoints, so the dirty pages a seed or an index build left are
// written before the next measurement rather than during it: a 1 ms read timed
// while the checkpointer flushes a fresh 65 MB index measures the flush. It is
// best effort, since CHECKPOINT needs a privilege a target may not grant.
func settle(ctx context.Context, db *gorm.DB) {
	_ = db.WithContext(ctx).Exec(`CHECKPOINT`).Error
}

// explorerShapes lists the reads the characterization measures: each shape of the
// job query's plans, a kind, queue, state, and tag filter each at a selective
// and a common value, the three capped counts, and one job in full. now is the
// end of the seeded month; running is a running job's id.
func explorerShapes(now time.Time, running string) []explorerShape {
	page := func(q flywheel.JobQuery) func(ctx context.Context, db *gorm.DB) error {
		return func(ctx context.Context, db *gorm.DB) error {
			_, err := flywheel.QueryJobs(ctx, db, q)
			return err
		}
	}
	count := func(q flywheel.JobQuery) func(ctx context.Context, db *gorm.DB) error {
		return func(ctx context.Context, db *gorm.DB) error {
			_, err := flywheel.CountJobs(ctx, db, q)
			return err
		}
	}
	terminal := flywheel.TerminalStates()
	claimable := []flywheel.JobState{flywheel.StateAvailable, flywheel.StateRetryable, flywheel.StateScheduled}
	window := now.Add(-15 * 24 * time.Hour)
	return []explorerShape{
		{"kind, newest first: 1% of jobs", "jobs_explore_kind", true, page(flywheel.JobQuery{Kinds: []string{"kind-cap"}})},
		{"kind, newest first: 5% of jobs", "jobs_explore_kind", true, page(flywheel.JobQuery{Kinds: []string{"kind-3"}})},
		{
			"kind, newest first: 0.2%, all a month old", "jobs_explore_kind", true,
			page(flywheel.JobQuery{Kinds: []string{"kind-retired"}}),
		},
		{"queue, newest first: 1% of jobs", "jobs_explore_queue", true, page(flywheel.JobQuery{Queues: []string{"urgent"}})},
		{"queue, newest first: 30% of jobs", "jobs_explore_queue", true, page(flywheel.JobQuery{Queues: []string{"bulk"}})},
		{
			"one state, newest first: 2% of jobs", "jobs_explore_state", true,
			page(flywheel.JobQuery{States: []flywheel.JobState{flywheel.StatePaused}}),
		},
		{
			"one state, newest first: 60% of jobs", "jobs_explore_state", true,
			page(flywheel.JobQuery{States: []flywheel.JobState{flywheel.StateSucceeded}}),
		},
		{"tag, newest first: 0.1% of jobs", "jobs_explore_tags", true, page(flywheel.JobQuery{Tags: []string{"tag-7"}})},
		{"tag, newest first: 10% of jobs", "jobs_explore_tags", true, page(flywheel.JobQuery{Tags: []string{"common"}})},
		{
			"finished, three terminal states", "jobs_finished", false,
			page(flywheel.JobQuery{Sort: flywheel.JobSortFinished, States: terminal}),
		},
		{"scheduled, claimable states in one queue", "jobs_ready", false, page(flywheel.JobQuery{
			Sort: flywheel.JobSortScheduled, States: claimable, Queues: []string{"default"},
		})},
		{"a parent's children", "jobs_parent_state", false, page(flywheel.JobQuery{ParentJobID: "parent-7"})},
		{
			"created window, one hour", "jobs_pkey", false,
			page(flywheel.JobQuery{CreatedFrom: window, CreatedTo: window.Add(time.Hour)}),
		},
		{"CountJobs below the cap: one kind, one state", "", false, count(flywheel.JobQuery{
			Kinds: []string{"kind-3"}, States: []flywheel.JobState{flywheel.StatePaused},
		})},
		{"CountJobs at the cap: 10,000 of one kind", "", false, count(flywheel.JobQuery{Kinds: []string{"kind-cap"}})},
		{"CountJobs past the cap: every job", "", false, count(flywheel.JobQuery{})},
		{"GetJob, a running job and its run", "", false, func(ctx context.Context, db *gorm.DB) error {
			_, err := flywheel.GetJob(ctx, db, running)
			return err
		}},
	}
}

// seedExplorerJobs writes the characterization's fixture server-side:
//
//   - jobs jobs, UUIDv7-shaped ids spread evenly over the thirty days before end;
//   - twenty kinds, and every (jobs / 10,000)-th job of the dedicated kind-cap,
//     exactly 10,000 of them at a million; a twentieth of the first day's jobs
//     are kind-retired instead, a kind that has not run since;
//   - queues default (69%), bulk (30%), and urgent (1%);
//   - states succeeded (60%), discarded (10%), cancelled (5%), available (12%,
//     half due and half scheduled past end), retryable (6%), running (5%), and
//     paused (2%), finalized_at on the terminal ones;
//   - two tags on 10%: common, and one of a hundred rarer ones (0.1% each);
//   - 5% the children of 100 parents;
//   - one run for each job claimed once, its error a short line over a stack.
//
// It is fixture, not a measured insert, so it goes through generate_series like
// the stats characterization's seed. setseed makes random() reproducible.
func seedExplorerJobs(ctx context.Context, db *gorm.DB, jobs int, end time.Time) error {
	from := end.Add(-explorerExplainDays * 24 * time.Hour)
	span := float64(explorerExplainDays * 24 * 3600)
	step := max(jobs/explorerCapKindJobs, 1)
	lastSeed := jobs - 1 // generate_series is inclusive
	exec := func(what, sql string, args ...any) error {
		if err := db.WithContext(ctx).Exec(sql, args...).Error; err != nil {
			return fmt.Errorf("loadtest: seed %s: %w", what, err)
		}
		return nil
	}
	if err := exec("setseed", `SELECT setseed(0.42)`); err != nil {
		return err
	}
	if err := exec("plan", `
		CREATE TEMP TABLE explorer_seed AS
		SELECT g,
		       ?::timestamptz + make_interval(secs => g::float8 * ? / ?) AS at,
		       floor(random() * 100)::int AS sb,
		       floor(random() * ?)::int AS kb,
		       floor(random() * 100)::int AS qb,
		       floor(random() * 1000)::int AS tb,
		       floor(random() * 100)::int AS pb
		FROM generate_series(0, ?) AS g`,
		from, span, jobs, explorerExplainKinds, lastSeed); err != nil {
		return err
	}
	if err := exec("jobs", `
		INSERT INTO jobs (id, created_at, updated_at, metadata, kind, queue, args, priority, state, attempt,
		                  max_attempts, scheduled_at, executor_class, tags, parent_job_id, finalized_at)
		SELECT substr(ms, 1, 8) || '-' || substr(ms, 9, 4) || '-7000-8000-' || lpad(to_hex(g), 12, '0'),
		       at, at, '{}'::jsonb,
		       CASE WHEN g % ? = 0 AND g / ? < ? THEN 'kind-cap'
		            WHEN g < ? AND kb = 0 THEN 'kind-retired' ELSE 'kind-' || kb END,
		       CASE WHEN qb < 69 THEN 'default' WHEN qb < 99 THEN 'bulk' ELSE 'urgent' END,
		       jsonb_build_object('n', g), 100, state,
		       CASE WHEN state IN ('available', 'paused') THEN 0 ELSE 1 END, 25,
		       CASE WHEN sb BETWEEN 81 AND 86 THEN ?::timestamptz + make_interval(secs => g % 86400)
		            WHEN state = 'retryable' THEN at + interval '1 minute' ELSE at END,
		       '',
		       CASE WHEN tb < 100 THEN jsonb_build_array('common', 'tag-' || tb) ELSE '[]'::jsonb END,
		       CASE WHEN pb < 5 THEN 'parent-' || (g % ?) END,
		       CASE WHEN state IN ('succeeded', 'discarded', 'cancelled') THEN at + interval '1 minute' END
		FROM (SELECT *, lpad(to_hex((extract(epoch FROM at) * 1000)::bigint), 12, '0') AS ms,
		             CASE WHEN sb < 60 THEN 'succeeded' WHEN sb < 70 THEN 'discarded' WHEN sb < 75 THEN 'cancelled'
		                  WHEN sb < 87 THEN 'available' WHEN sb < 93 THEN 'retryable' WHEN sb < 98 THEN 'running'
		                  ELSE 'paused' END AS state
		      FROM explorer_seed) s`,
		step, step, explorerCapKindJobs, jobs/explorerExplainDays, end, explorerExplainParents); err != nil {
		return err
	}
	return exec("runs", `
		INSERT INTO job_runs (id, job_id, attempt, kind, queue, executor_class, executor_id, started_at,
		                      finished_at, outcome, error_class, error_message, duration_ms, enqueued_children,
		                      job_state, superseded, created_at)
		SELECT id || '-r1', id, 1, kind, queue, '', 'seed', created_at + interval '30 seconds',
		       CASE WHEN state = 'running' THEN NULL ELSE created_at + interval '1 minute' END,
		       CASE state WHEN 'succeeded' THEN 'success' WHEN 'cancelled' THEN 'cancelled'
		                  WHEN 'running' THEN 'started' ELSE 'error' END,
		       CASE WHEN state IN ('discarded', 'retryable') THEN 'transient' END,
		       CASE WHEN state IN ('discarded', 'retryable')
		            THEN 'upstream timed out' || chr(10) || repeat('  at frame (worker.go:42)' || chr(10), 40) END,
		       CASE WHEN state = 'running' THEN NULL ELSE 30000 END,
		       0,
		       CASE WHEN state = 'running' THEN NULL ELSE state END,
		       false, created_at + interval '30 seconds'
		FROM jobs WHERE attempt = 1`)
}

// planIndexRe finds the index a plan line reads.
//
//nolint:gochecknoglobals // compiled once
var planIndexRe = regexp.MustCompile(`(?:Index (?:Only )?Scan(?: Backward)? using|Bitmap Index Scan on) (\S+)`)

// planJobsAccess names how a read's plans reach jobs: each index they use on it,
// in order of first use, and "Seq Scan" for a sequential scan of it. A run's
// own index on job_runs is left out: every page joins its runs the same way.
func planJobsAccess(read StatsExplainRead) []string {
	var out []string
	add := func(s string) {
		if !slices.Contains(out, s) {
			out = append(out, s)
		}
	}
	for _, st := range read.Statements {
		for _, line := range st.Plan {
			if m := planIndexRe.FindStringSubmatch(line); m != nil {
				if !strings.HasPrefix(m[1], "job_runs") {
					add(m[1])
				}
				continue
			}
			if strings.Contains(line, "Seq Scan on jobs") {
				add("Seq Scan")
			}
		}
	}
	return out
}

// Text renders the explorer-explain artifact: the fixture, a latency table with
// each plan's access path, then every captured statement and its plan.
func (r ExplorerExplainReport) Text() string {
	var b strings.Builder
	fmt.Fprintf(&b, "explorer read plans — %s\n", r.Target)
	if r.Server != "" {
		fmt.Fprintf(&b, "server: %s\n", strings.TrimSpace(r.Server))
	}
	fmt.Fprintf(&b, "schema: %s\n", r.Schema)
	fmt.Fprintf(&b, "fixture: %d jobs over %d days across %d kinds, a dedicated kind of %d, and a retired kind "+
		"whose jobs all ran on the first day;\n  queues default 69%%, bulk 30%%, urgent 1%%; states succeeded 60%%, "+
		"discarded 10%%, cancelled 5%%, available 12%%,\n  retryable 6%%, running 5%%, paused 2%%; two tags on 10%% "+
		"(common, and one of a hundred at 0.1%% each);\n  5%% the children of %d parents; one run per claimed job\n",
		r.Jobs, explorerExplainDays, explorerExplainKinds, explorerCapKindJobs, explorerExplainParents)
	fmt.Fprintf(&b, "jobs: %.1f MB total, %.1f MB of indexes", mb(r.JobsBytes), mb(r.JobsIndex))
	for _, s := range r.IndexSizes {
		fmt.Fprintf(&b, "; %s %.1f MB", s.Name, mb(s.Bytes))
	}
	b.WriteString("\n")
	b.WriteString(
		"\nEvery statement was captured from the flywheel call named above it through a recording GORM logger\n" +
			"and explained verbatim with EXPLAIN (ANALYZE, BUFFERS), after VACUUM (ANALYZE). Latency is the wall\n" +
			"time of the whole call as the median of 7 after a warm-up. \"with\" is measured with all four explorer\n" +
			"indexes installed; \"without\" drops the one the shape is meant for, analyzes jobs, measures, and\n" +
			"rebuilds it. A page is QueryJobs' default 50 items; access is the index each plan reads jobs through.\n" +
			"The plans explain each statement with its values inlined. The timed calls bind them, as an application's\n" +
			"driver does, so after five executions PostgreSQL may run a generic plan that does not know how common a\n" +
			"value is: where a latency is far above its plan's execution time, that is the generic plan.\n\n",
	)
	fmt.Fprintf(&b, "%-46s %-18s %11s %11s  %-32s %s\n", "read", "index", "with", "without", "access with", "access without")
	for _, read := range r.Reads {
		without, accessWithout := "-", "-"
		if read.Without != nil {
			without = fmtMS(read.Without.Median)
			accessWithout = strings.Join(planJobsAccess(*read.Without), ", ")
		}
		fmt.Fprintf(&b, "%-46s %-18s %11s %11s  %-32s %s\n", read.Name, orDash(read.Index),
			fmtMS(read.With.Median), without, strings.Join(planJobsAccess(read.With), ", "), accessWithout)
	}
	for _, read := range r.Reads {
		writeExplorerPlans(&b, read.Name+" (with every explorer index)", read.With)
		if read.Without != nil {
			writeExplorerPlans(&b, read.Name+" (without "+read.Index+")", *read.Without)
		}
	}
	return b.String()
}

// writeExplorerPlans writes one measurement's statements and plans.
func writeExplorerPlans(b *strings.Builder, title string, read StatsExplainRead) {
	fmt.Fprintf(b, "\n=== %s: median %s, min %s, max %s ===\n", title, fmtMS(read.Median), fmtMS(read.Min), fmtMS(read.Max))
	for i, stmt := range read.Statements {
		fmt.Fprintf(b, "\n--- statement %d ---\n%s\n\nplan:\n", i+1, strings.TrimSpace(stmt.SQL))
		for _, line := range stmt.Plan {
			b.WriteString("  ")
			b.WriteString(line)
			b.WriteByte('\n')
		}
	}
}

// orDash returns s, or "-" when it is empty.
func orDash(s string) string {
	if s == "" {
		return "-"
	}
	return s
}
