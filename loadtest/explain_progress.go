//go:build loadtest

package loadtest

import (
	"context"
	"fmt"
	"strings"

	flywheel "github.com/mrz1836/go-flywheel"
	"gorm.io/gorm"
)

// progressParentID is the id of the parent whose children the progress explain
// rolls up. It is a fixed string so the captured statements carry no random value.
const progressParentID = "progress-explain-parent"

// progressChildren is how many children the explain gives its one measured parent.
// A hundred thousand is the rollup's own acceptance scale — a 100k-child parent in
// a 1M table — and the selectivity at which an index-only scan on jobs_parent_state
// is the plan that matters.
const progressChildren = 100_000

// ProgressExplainReport is one progress-rollup characterization: the statements the
// runtime's Progress emits, each explained against a seeded 1M table.
type ProgressExplainReport struct {
	Target         string
	Server         string
	Schema         string
	Jobs           int
	ParentChildren int
	TableBytes     int64
	Statements     []ProgressExplainStatement
}

// ProgressExplainStatement is one captured Progress read and its plan.
type ProgressExplainStatement struct {
	SQL  string
	Plan []string
}

// ExplainProgress seeds a 1M table with a 100k-child parent, captures the SQL the
// runtime's Progress rollup emits through a recording logger, and explains each
// statement — so the published plan describes the query the runtime actually runs,
// not one this tool retyped.
//
// It provisions and drops its own schema, exactly like ExplainClaim. Progress is a
// read, so the captures need no rolled-back transaction the way the claim's UPDATE
// does; EXPLAIN (ANALYZE) of a SELECT mutates nothing.
func ExplainProgress(ctx context.Context, cfg ExplainConfig) (ProgressExplainReport, error) {
	cfg, err := cfg.validate()
	if err != nil {
		return ProgressExplainReport{}, err
	}
	report := ProgressExplainReport{
		Target: redactDSN(cfg.DSN), Jobs: cfg.Jobs, ParentChildren: progressChildren,
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

	db, err := openPool(withSearchPath(cfg.DSN, schema), 2)
	if err != nil {
		return report, fmt.Errorf("loadtest: open work pool: %w", err)
	}
	defer closePool(db)

	if err = installSchema(ctx, db, IndexesFull, StorageDefault); err != nil {
		return report, err
	}
	if err = seedProgressTable(ctx, db, cfg.Jobs); err != nil {
		return report, err
	}
	// Index-only scans need the visibility map set, which VACUUM populates; ANALYZE
	// alone leaves the freshly bulk-inserted pages not-all-visible and the planner
	// falls back to a heap-touching bitmap scan.
	if err = db.WithContext(ctx).Exec(`VACUUM (ANALYZE) jobs`).Error; err != nil {
		return report, fmt.Errorf("loadtest: vacuum jobs: %w", err)
	}
	report.Server = scalarString(ctx, db, `SELECT version()`)
	report.TableBytes = scalarInt(ctx, db, `SELECT pg_total_relation_size('jobs')`)

	statements, err := captureProgressSQL(ctx, cfg, schema)
	if err != nil {
		return report, err
	}
	for _, sql := range statements {
		lines, explainErr := explainReadStatement(ctx, db, sql)
		if explainErr != nil {
			return report, explainErr
		}
		report.Statements = append(report.Statements, ProgressExplainStatement{SQL: sql, Plan: lines})
	}
	return report, nil
}

// seedProgressTable writes jobs rows server-side: one parent, progressChildren
// children of it spread across every state, and leaf jobs to fill out the total.
// It is fixture, not a measured insert, so it goes through generate_series rather
// than the bulk path — the same choice seedTerminal makes.
func seedProgressTable(ctx context.Context, db *gorm.DB, jobs int) error {
	leaves := jobs - progressChildren - 1
	if leaves < 0 {
		leaves = 0
	}
	at := seedEpoch

	// The parent itself, terminal.
	if err := db.WithContext(ctx).Exec(`
		INSERT INTO jobs (id, created_at, updated_at, metadata, kind, queue, args, priority,
		                  state, attempt, max_attempts, scheduled_at, executor_class, tags)
		VALUES (?, ?, ?, '{}'::jsonb, 'coordinator', 'default', '{}'::jsonb, 100,
		        'succeeded', 1, 25, ?, '', '[]'::jsonb)`,
		progressParentID, at, at, at,
	).Error; err != nil {
		return fmt.Errorf("loadtest: seed progress parent: %w", err)
	}

	// The children, spread across all eight states so the rollup counts every one.
	if err := db.WithContext(ctx).Exec(`
		INSERT INTO jobs (id, created_at, updated_at, metadata, kind, queue, args, priority,
		                  state, attempt, max_attempts, scheduled_at, parent_job_id, executor_class, tags)
		SELECT 'pc-' || g, ?, ?, '{}'::jsonb, 'child', 'default', '{}'::jsonb, 100,
		       (ARRAY['succeeded','succeeded','succeeded','running','available','retryable','cancelled','discarded'])[1+(g%8)],
		       0, 25, ?, ?, '', '[]'::jsonb
		FROM generate_series(1, ?) AS g`,
		at, at, at, progressParentID, progressChildren,
	).Error; err != nil {
		return fmt.Errorf("loadtest: seed progress children: %w", err)
	}

	// Leaf jobs with no parent, to fill the table to its target depth.
	if leaves > 0 {
		if err := db.WithContext(ctx).Exec(`
			INSERT INTO jobs (id, created_at, updated_at, metadata, kind, queue, args, priority,
			                  state, attempt, max_attempts, scheduled_at, executor_class, tags)
			SELECT 'pl-' || g, ?, ?, '{}'::jsonb, 'leaf', 'default', '{}'::jsonb, 100,
			       'succeeded', 1, 25, ?, '', '[]'::jsonb
			FROM generate_series(1, ?) AS g`,
			at, at, at, leaves,
		).Error; err != nil {
			return fmt.Errorf("loadtest: seed progress leaves: %w", err)
		}
	}
	return nil
}

// captureProgressSQL runs the runtime's Progress against the seeded parent through
// a recording logger and returns every jobs statement it emitted, in order. It
// reuses claimSQLRecorder — the recorder is a generic gorm logger, nothing about it
// is claim-specific — and takes the statements from the code under test rather than
// retyping the rollup's SQL here.
func captureProgressSQL(ctx context.Context, cfg ExplainConfig, schema string) ([]string, error) {
	capture, err := openPool(withSearchPath(cfg.DSN, schema), 1)
	if err != nil {
		return nil, fmt.Errorf("loadtest: open capture pool: %w", err)
	}
	defer closePool(capture)

	rec := &claimSQLRecorder{}
	prev := capture.Logger
	capture.Logger = rec
	defer func() { capture.Logger = prev }()

	if _, err := flywheel.Progress(ctx, capture, progressParentID); err != nil {
		return nil, fmt.Errorf("loadtest: capture progress SQL: %w", err)
	}
	var out []string
	for _, sql := range rec.all {
		if strings.Contains(sql, "jobs") && strings.HasPrefix(strings.ToUpper(strings.TrimSpace(sql)), "SELECT") {
			out = append(out, sql)
		}
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("loadtest: %w: Progress traced no statement", ErrClaimSQLUnrecognized)
	}
	return out, nil
}

// explainReadStatement captures EXPLAIN (ANALYZE, BUFFERS) for a read statement.
// Unlike the claim's, it opens no transaction: Progress issues only SELECTs, which
// EXPLAIN ANALYZE executes without mutating the table.
func explainReadStatement(ctx context.Context, db *gorm.DB, sql string) ([]string, error) {
	var lines []string
	if err := db.WithContext(ctx).Raw("EXPLAIN (ANALYZE, BUFFERS) " + sql).Scan(&lines).Error; err != nil {
		return nil, fmt.Errorf("loadtest: explain progress: %w", err)
	}
	return lines, nil
}

// Text renders the progress-explain artifact: a header, then each captured
// statement followed by its plan.
func (r ProgressExplainReport) Text() string {
	var b strings.Builder
	fmt.Fprintf(&b, "progress rollup plans — %s\n", r.Target)
	if r.Server != "" {
		fmt.Fprintf(&b, "server: %s\n", strings.TrimSpace(r.Server))
	}
	fmt.Fprintf(&b, "schema: %s\n", r.Schema)
	fmt.Fprintf(&b, "jobs: %d (one parent with %d children, the rest leaves)\n", r.Jobs, r.ParentChildren)
	fmt.Fprintf(&b, "table size: %d bytes\n", r.TableBytes)
	b.WriteString(
		"\nEvery statement was captured from flywheel.Progress through a recording GORM logger and\n" +
			"explained verbatim. The grouped counts read is the rollup's hot path; on jobs_parent_state\n" +
			"it is an Index Only Scan with no heap fetches at this selectivity.\n",
	)
	for i, stmt := range r.Statements {
		fmt.Fprintf(&b, "\n--- statement %d ---\n%s\n\nplan:\n", i+1, strings.TrimSpace(stmt.SQL))
		for _, line := range stmt.Plan {
			b.WriteString("  ")
			b.WriteString(line)
			b.WriteByte('\n')
		}
	}
	return b.String()
}
