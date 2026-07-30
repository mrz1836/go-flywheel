//go:build loadtest

package loadtest

import (
	"context"
	"fmt"
	"strconv"
	"strings"
)

// This file characterizes the cost of a round-robin-across-parents claim, so the
// decision to ship it or to document priority banding instead is made from
// numbers rather than from an assumption. It builds the candidate SQL inline —
// there is no production change in this measurement, and the driver ships no
// fairness query yet — exactly as the claim-index characterization built its
// candidate index definitions inline. The one query it does capture from the
// driver is the shipped claim, so the baseline it compares against is the real
// one and not a retyped approximation.

// fairnessNow is the pinned instant the candidate predicates compare scheduled_at
// against. Every seeded row is dated at the seed epoch, comfortably before it, so
// the whole table is due — and pinning it (rather than now()) keeps the committed
// artifact reproducible across runs.
const fairnessNow = "2099-01-01 00:00:00+00"

// FairnessVariant is one claim shape under test: the shipped claim, the ranked
// round-robin CTE, and the ranked CTE with a priority-band pre-filter.
type FairnessVariant struct {
	// Name is the variant label carried into the report.
	Name string `json:"name"`
	// Desc says what the variant is and why it is a candidate.
	Desc string `json:"desc"`
	// SQL is the statement explained, its scheduled_at bound pinned to fairnessNow.
	SQL string `json:"sql"`
}

// FairnessCell is one measured variant: its plan and the numbers lifted from it.
type FairnessCell struct {
	Variant FairnessVariant `json:"variant"`
	Summary PlanSummary     `json:"summary"`
	Plan    []string        `json:"plan"`
}

// FairnessExplainReport is one fairness-cost characterization run.
type FairnessExplainReport struct {
	// Target is the redacted DSN — host, port, database, and nothing else.
	Target string `json:"target"`
	Server string `json:"server"`
	Schema string `json:"schema"`
	// Jobs, Queues, Classes, Limit, and Seed describe the seeded table.
	Jobs    int      `json:"jobs"`
	Queues  []string `json:"queues"`
	Classes []string `json:"executor_classes"`
	Limit   int      `json:"claim_limit"`
	Seed    int64    `json:"seed"`
	// ClaimQueues is the subset of Queues the claim names — one, so the shipped
	// claim reaches jobs_ready, matching the runtime's own claim-plan gate.
	ClaimQueues []string `json:"claim_queues"`
	// ReadySet is how many rows the routed claim's predicate matches — the input
	// the window function ranks before the LIMIT applies. It is the size the ranked
	// variant's cost is O(of), and the headline the whole run exists to produce.
	ReadySet int64 `json:"ready_set"`
	// TableBytes is the seeded table's size at measurement time.
	TableBytes int64 `json:"table_bytes"`

	Cells []FairnessCell `json:"cells"`
	Notes []string       `json:"notes,omitempty"`
}

// fairnessPredicate is the routed claim's WHERE clause, spelled exactly as
// postgresDriver.Dequeue writes it (driver_postgres.go): the same state list,
// deleted_at guard, scheduled_at bound, queue IN, and OR-on-class clause. Every
// variant splices it, so the only thing that differs between the shipped claim
// and the ranked candidates is the ordering, not the rows they consider.
func fairnessPredicate(queues []string, class string) string {
	return "state IN ('available', 'retryable', 'scheduled')\n" +
		"      AND deleted_at IS NULL\n" +
		"      AND scheduled_at <= '" + fairnessNow + "'\n" +
		"      AND queue IN (" + inQueues(queues) + ")\n" +
		"      AND (executor_class = " + pgLiteral(class) + " OR executor_class = '')"
}

// inQueues renders a queue list as a PostgreSQL IN body.
func inQueues(queues []string) string {
	quoted := make([]string, len(queues))
	for i, q := range queues {
		quoted[i] = pgLiteral(q)
	}
	return strings.Join(quoted, ", ")
}

// fairnessVariants builds the three claim shapes the run compares.
//
// The shipped claim is the O(LIMIT) baseline: it walks jobs_ready in order and
// stops at the batch size. The ranked variant partitions the whole ready set by
// parent and numbers each parent's rows, which forces every ready row through a
// WindowAgg before the LIMIT can apply — the O(ready) cost the decision turns on.
// The banded variant adds the priority pre-filter from the plan, narrowing the
// window's input to the top priority band, which is the mitigation whose effect
// this run measures rather than assumes.
func fairnessVariants(queues []string, class string, limit int) []FairnessVariant {
	pred := fairnessPredicate(queues, class)
	lim := strconv.Itoa(limit)

	shipped := "SELECT id FROM jobs\n" +
		"    WHERE " + pred + "\n" +
		"    ORDER BY priority, scheduled_at\n" +
		"    LIMIT " + lim + "\n" +
		"    FOR UPDATE SKIP LOCKED"

	ranked := "WITH ranked AS (\n" +
		"    SELECT id, priority, scheduled_at,\n" +
		"           row_number() OVER (PARTITION BY COALESCE(parent_job_id, id)\n" +
		"                              ORDER BY priority, scheduled_at) AS rn\n" +
		"    FROM jobs\n" +
		"    WHERE " + pred + "\n" +
		")\n" +
		"SELECT id FROM ranked\n" +
		"ORDER BY priority, rn, scheduled_at\n" +
		"LIMIT " + lim

	banded := "WITH ranked AS (\n" +
		"    SELECT id, priority, scheduled_at,\n" +
		"           row_number() OVER (PARTITION BY COALESCE(parent_job_id, id)\n" +
		"                              ORDER BY priority, scheduled_at) AS rn\n" +
		"    FROM jobs\n" +
		"    WHERE " + pred + "\n" +
		"      AND priority <= (SELECT min(priority) FROM jobs WHERE " + pred + ")\n" +
		")\n" +
		"SELECT id FROM ranked\n" +
		"ORDER BY priority, rn, scheduled_at\n" +
		"LIMIT " + lim

	return []FairnessVariant{
		{
			Name: "F0",
			Desc: "shipped claim — jobs_ready walked in (priority, scheduled_at) order, O(LIMIT)",
			SQL:  shipped,
		},
		{
			Name: "F1",
			Desc: "round-robin: row_number() OVER (PARTITION BY COALESCE(parent_job_id, id)) then LIMIT",
			SQL:  ranked,
		},
		{
			Name: "F2",
			Desc: "F1 + priority-band pre-filter (WHERE priority <= (SELECT min(priority) ...))",
			SQL:  banded,
		},
	}
}

// ExplainFairness measures the round-robin-across-parents claim's cost against a
// freshly seeded table, at whatever depth the config names.
//
// It provisions its own schema, installs the full production index set, seeds a
// table of all-claimable rows through the same generator every other artifact
// uses, EXPLAINs the shipped claim and the two ranked candidates, and drops the
// schema. Nothing it measures survives it, and it makes no production change: the
// candidate SQL is built here, not shipped.
func ExplainFairness(ctx context.Context, cfg ExplainConfig) (FairnessExplainReport, error) {
	cfg, err := cfg.validate()
	if err != nil {
		return FairnessExplainReport{}, err
	}

	queues := explainQueues(cfg.Queues)
	classes := explainClasses(cfg.ExecutorClass)
	report := FairnessExplainReport{
		Target:  redactDSN(cfg.DSN),
		Jobs:    cfg.Jobs,
		Queues:  queues,
		Classes: classes,
		Limit:   cfg.Limit,
		Seed:    cfg.Seed,
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
	if err = seedClaimable(ctx, db, cfg, queues, classes); err != nil {
		return report, err
	}
	if err = db.WithContext(ctx).Exec(`ANALYZE jobs`).Error; err != nil {
		return report, fmt.Errorf("loadtest: analyze jobs: %w", err)
	}
	report.Server = scalarString(ctx, db, `SELECT version()`)
	report.TableBytes = scalarInt(ctx, db, `SELECT pg_total_relation_size('jobs')`)

	// The claim names a single queue, which is the shape the shipped claim reaches
	// jobs_ready in: with queue an equality, the index supplies (priority,
	// scheduled_at) order and F0 stops at LIMIT. A multi-queue claim would merge
	// several index ranges and sort, which would make F0 scan the ready set too and
	// hide the very divergence this run exists to measure. Rows are still seeded
	// across every queue, exactly as the runtime's own claim-plan gate seeds them.
	claimQueues := queues[:1]
	report.ClaimQueues = claimQueues
	report.ReadySet = scalarInt(ctx, db, `SELECT count(*) FROM jobs WHERE `+
		fairnessPredicate(claimQueues, cfg.ExecutorClass))

	for _, variant := range fairnessVariants(claimQueues, cfg.ExecutorClass, cfg.Limit) {
		lines, explainErr := explainStatement(ctx, db, variant.SQL)
		if explainErr != nil {
			return report, explainErr
		}
		report.Cells = append(report.Cells, FairnessCell{
			Variant: variant, Summary: parsePlan(lines), Plan: lines,
		})
	}

	report.Notes = fairnessNotes(cfg, report.ReadySet)
	return report, nil
}

// fairnessNotes records the caveats a reader needs to weigh the numbers.
func fairnessNotes(cfg ExplainConfig, readySet int64) []string {
	return []string{
		"F0 mirrors postgresDriver.Dequeue's inner select verbatim (driver_postgres.go), its scheduled_at " +
			"bound pinned to a fixed instant so the artifact is reproducible. The state list, deleted_at guard, " +
			"queue IN, and OR-on-class clause are byte-identical across all three variants, so the only " +
			"difference measured is the ordering.",
		fmt.Sprintf(
			"The claim names a single queue — the shape the shipped claim reaches jobs_ready in, matching the "+
				"runtime's own claim-plan gate — while rows are seeded across every queue. Its predicate matches "+
				"%s rows, the ready set. F0 walks jobs_ready and stops at LIMIT=%d; F1 ranks the whole ready set "+
				"through a WindowAgg before the LIMIT applies, which is the O(ready) cost the ship-or-band "+
				"decision turns on.",
			withThousands(readySet), cfg.Limit,
		),
		"Every row is seeded parentless, so COALESCE(parent_job_id, id) makes each job its own partition of one " +
			"— the common case the plan's 1000/1001 paragraph describes. The WindowAgg still processes every ready " +
			"row, so its cost is O(ready) whatever the partition sizes are.",
		"F1 carries no FOR UPDATE SKIP LOCKED: PostgreSQL forbids a locking clause on a query with a window " +
			"function, so a shipped fairness claim would rank into a candidate set and re-lock it in a second step. " +
			"This run measures the ranking cost, which is what dominates and what the decision turns on.",
		"EXPLAIN (ANALYZE) executes the statement, so every capture ran inside a transaction that was rolled " +
			"back. The seeded table is identical for every variant.",
	}
}
