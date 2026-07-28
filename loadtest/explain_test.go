//go:build loadtest

package loadtest

import (
	"context"
	"strings"
	"testing"
	"time"

	flywheel "github.com/mrz1836/go-flywheel"
)

// capturedRoutedSQL is a claim statement in the shape the PostgreSQL driver
// emits it, with bind values already interpolated.
//
// It is a fixture rather than a live capture so the rewrite can be tested
// without a database; TestExplainClaimCapturesTheDriversOwnSQL is what proves
// the fixture still describes what the driver writes.
const capturedRoutedSQL = `
WITH claimed AS (
    SELECT id FROM jobs
    WHERE state IN ('available', 'retryable', 'scheduled')
      AND deleted_at IS NULL
      AND scheduled_at <= '2026-07-27 14:08:23.323'
      AND queue IN ('q0','q1')
      AND (executor_class = 'loadtest' OR executor_class = '')
    ORDER BY priority, scheduled_at
    LIMIT 8
    FOR UPDATE SKIP LOCKED
)
UPDATE jobs
SET state = 'running', attempt = attempt + 1, leased_until = '2026-07-27 14:09:23.323'
FROM claimed
WHERE jobs.id = claimed.id
RETURNING jobs.id`

func TestRewriteClassPredicate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		sql      string
		class    string
		want     string
		wantFail bool
	}{
		{
			name:  "the emitted OR spelling becomes the IN spelling",
			sql:   capturedRoutedSQL,
			class: "loadtest",
			want:  "executor_class IN ('loadtest', '')",
		},
		{
			name:  "a class carrying a quote is escaped the way the dialector escapes it",
			sql:   `AND (executor_class = 'o''brien' OR executor_class = '') AND x`,
			class: "o'brien",
			want:  `executor_class IN ('o''brien', '')`,
		},
		{
			name:     "a ClaimAnyClass statement has no clause to respell",
			sql:      strings.ReplaceAll(capturedRoutedSQL, "AND (executor_class = 'loadtest' OR executor_class = '')", ""),
			class:    "loadtest",
			wantFail: true,
		},
		{
			name:     "a statement whose class differs is not silently rewritten",
			sql:      capturedRoutedSQL,
			class:    "other",
			wantFail: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got, err := rewriteClassPredicate(tc.sql, tc.class)
			if tc.wantFail {
				if err == nil {
					t.Fatalf("rewriteClassPredicate must fail loudly rather than pass the statement through, got:\n%s", got)
				}
				if !strings.Contains(err.Error(), ErrClaimSQLUnrecognized.Error()) {
					t.Errorf("error must be matchable as ErrClaimSQLUnrecognized: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("rewriteClassPredicate: %v", err)
			}
			if !strings.Contains(got, tc.want) {
				t.Errorf("rewritten statement must contain %q:\n%s", tc.want, got)
			}
			if strings.Contains(got, "OR executor_class") {
				t.Errorf("the OR spelling must be gone from the rewritten statement:\n%s", got)
			}
		})
	}
}

// TestRewriteClassPredicateChangesOnlyTheClause is the property the whole P0/P1
// comparison rests on: if the rewrite touched anything else, the two plans would
// differ for a reason the matrix is not about.
func TestRewriteClassPredicateChangesOnlyTheClause(t *testing.T) {
	t.Parallel()

	got, err := rewriteClassPredicate(capturedRoutedSQL, "loadtest")
	if err != nil {
		t.Fatalf("rewriteClassPredicate: %v", err)
	}
	restored := strings.Replace(got, inClassPredicate("loadtest"), orClassPredicate("loadtest"), 1)
	if restored != capturedRoutedSQL {
		t.Errorf("undoing the one documented Replace must restore the statement byte for byte:\n%s", restored)
	}
}

// seqScanPlan is a claim plan in the shape the published baseline reports: a
// sequential scan of the whole claimable set, sorted to disk.
//
// The jobs_pkey index scan at the bottom is deliberate. It sits outside the CTE
// — it is the outer UPDATE's join back to the table — and a parser that took the
// first index scan it found would report this plan as using an index.
var seqScanPlan = []string{ //nolint:gochecknoglobals // a parser fixture
	"Update on jobs  (cost=125000.00..126000.00 rows=8 width=150) (actual time=38.100..38.200 rows=8 loops=1)",
	"  Buffers: shared hit=1200 read=11000, temp read=488 written=489",
	"  CTE claimed",
	"    ->  Limit  (cost=125000.00..125000.02 rows=8 width=54) (actual time=38.000..38.010 rows=8 loops=1)",
	"          ->  LockRows  (cost=125000.00..126000.00 rows=500000 width=54) (actual time=38.000..38.005 rows=8 loops=1)",
	"                ->  Sort  (cost=125000.00..126250.00 rows=500000 width=54) (actual time=37.990..37.995 rows=8 loops=1)",
	"                      Sort Key: jobs_1.priority, jobs_1.scheduled_at",
	"                      Sort Method: external merge  Disk: 3904kB",
	"                      ->  Seq Scan on jobs jobs_1  (cost=0.00..40000.00 rows=500000 width=54) (actual time=0.010..20.000 rows=500000 loops=1)",
	"                            Filter: ((deleted_at IS NULL) AND (state = ANY ('{available,retryable,scheduled}'::text[])))",
	"                            Rows Removed by Filter: 500000",
	"  ->  Nested Loop  (cost=0.14..8.28 rows=8 width=150) (actual time=38.050..38.100 rows=8 loops=1)",
	"        ->  CTE Scan on claimed  (cost=0.00..0.02 rows=8 width=88) (actual time=38.040..38.045 rows=8 loops=1)",
	"        ->  Index Scan using jobs_pkey on jobs  (cost=0.14..8.16 rows=1 width=46) (actual time=0.004..0.004 rows=1 loops=8)",
	"              Index Cond: (id = claimed.id)",
	"Planning Time: 0.412 ms",
	"Execution Time: 38.700 ms",
}

// indexScanPlan is the shape a working claim produces: an ordered index scan
// feeding the limit directly, with no sort anywhere above it.
var indexScanPlan = []string{ //nolint:gochecknoglobals // a parser fixture
	"Update on jobs  (cost=0.55..70.12 rows=8 width=150) (actual time=0.140..0.180 rows=8 loops=1)",
	"  Buffers: shared hit=64 read=3",
	"  CTE claimed",
	"    ->  Limit  (cost=0.55..8.90 rows=8 width=54) (actual time=0.080..0.090 rows=8 loops=1)",
	"          ->  LockRows  (cost=0.55..8.90 rows=8 width=54) (actual time=0.079..0.088 rows=8 loops=1)",
	"                ->  Index Scan using jobs_ready on jobs jobs_1  (cost=0.55..8.90 rows=8 width=54) (actual time=0.070..0.078 rows=8 loops=1)",
	"                      Index Cond: ((queue = 'q0'::text) AND (scheduled_at <= '2026-07-27 14:08:23.323-04'::timestamp with time zone))",
	"                      Filter: (executor_class = ANY ('{loadtest,\"\"}'::text[]))",
	"                      Rows Removed by Filter: 9",
	"  ->  Nested Loop  (cost=0.14..8.28 rows=8 width=150) (actual time=0.150..0.170 rows=8 loops=1)",
	"        ->  CTE Scan on claimed  (cost=0.00..0.02 rows=8 width=88) (actual time=0.140..0.145 rows=8 loops=1)",
	"        ->  Index Scan using jobs_pkey on jobs  (cost=0.14..8.16 rows=1 width=46) (actual time=0.003..0.003 rows=1 loops=8)",
	"Planning Time: 0.310 ms",
	"Execution Time: 0.240 ms",
}

func TestParsePlan(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		plan []string
		want PlanSummary
	}{
		{
			name: "the baseline shape: a full scan sorted to disk",
			plan: seqScanPlan,
			want: PlanSummary{
				Scan: "Seq Scan on jobs jobs_1", ScanKind: "Seq Scan", ScanRows: 500000,
				Sorted: true, SortMethod: "external merge  Disk: 3904kB", RowsRemoved: 500000,
				SharedHit: 1200, SharedRead: 11000, PlanningMS: 0.412, ExecutionMS: 38.700,
			},
		},
		{
			name: "a working claim: an ordered index scan with no sort above it",
			plan: indexScanPlan,
			want: PlanSummary{
				Scan: "Index Scan using jobs_ready on jobs jobs_1", ScanKind: "Index Scan",
				IndexUsed: "jobs_ready", ScanRows: 8, Sorted: false, RowsRemoved: 9,
				SharedHit: 64, SharedRead: 3, PlanningMS: 0.310, ExecutionMS: 0.240,
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got := parsePlan(tc.plan)
			if got != tc.want {
				t.Errorf("parsePlan mismatch\n got: %+v\nwant: %+v", got, tc.want)
			}
		})
	}
}

// TestParsePlanIgnoresTheJoinBackToTheTable states the CTE-subtree rule as its
// own assertion: the outer UPDATE always reaches jobs through jobs_pkey, and
// reporting that as the claim's scan would call every plan in the matrix fixed.
func TestParsePlanIgnoresTheJoinBackToTheTable(t *testing.T) {
	t.Parallel()

	got := parsePlan(seqScanPlan)
	if got.IndexUsed != "" {
		t.Errorf("the claim's scan is a Seq Scan; the pkey lookup below the CTE must not be reported: %+v", got)
	}
	if got.ScanKind != "Seq Scan" {
		t.Errorf("ScanKind = %q, want Seq Scan", got.ScanKind)
	}
}

// TestParsePlanWithoutACTEReadsTheWholePlan proves the parser degrades to the
// whole plan rather than to nothing, so a statement shape it has not seen
// produces a summary a reader can question instead of an empty one they cannot.
func TestParsePlanWithoutACTEReadsTheWholePlan(t *testing.T) {
	t.Parallel()

	plan := []string{
		"Seq Scan on jobs  (cost=0.00..40000.00 rows=500000 width=54) (actual time=0.010..20.000 rows=500000 loops=1)",
		"  Buffers: shared hit=10 read=20",
		"Execution Time: 20.500 ms",
	}
	got := parsePlan(plan)
	if got.ScanKind != "Seq Scan" || got.ScanRows != 500000 {
		t.Errorf("a plan with no CTE must still be summarized: %+v", got)
	}
}

func TestClaimSubtree(t *testing.T) {
	t.Parallel()

	sub := claimSubtree(seqScanPlan)
	joined := strings.Join(sub, "\n")
	if !strings.Contains(joined, "Seq Scan on jobs jobs_1") {
		t.Errorf("the subtree must contain the claim's own scan:\n%s", joined)
	}
	if strings.Contains(joined, "jobs_pkey") {
		t.Errorf("the subtree must stop before the outer join back to the table:\n%s", joined)
	}
	if strings.Contains(joined, "Nested Loop") {
		t.Errorf("the subtree must stop at the CTE's sibling:\n%s", joined)
	}
}

// TestExplainControlComesFromTheRuntime is the anti-drift assertion: V0 is the
// shipped index, taken from the runtime's own set, not a copy of it kept here.
func TestExplainControlComesFromTheRuntime(t *testing.T) {
	t.Parallel()

	set, err := flywheel.IndexSet("postgres")
	if err != nil {
		t.Fatalf("IndexSet: %v", err)
	}
	var shipped string
	for _, idx := range set {
		if idx.Name == claimIndexName {
			shipped = idx.DDL
		}
	}
	if shipped == "" {
		t.Fatalf("the runtime's index set must contain %s", claimIndexName)
	}

	variants, err := explainVariants()
	if err != nil {
		t.Fatalf("explainVariants: %v", err)
	}
	byName := make(map[string]explainVariant, len(variants))
	for _, v := range variants {
		byName[v.Name] = v
	}
	if byName["V0"].DDL != shipped {
		t.Errorf("V0 must be the shipped DDL verbatim\n got: %s\nwant: %s", byName["V0"].DDL, shipped)
	}
	if byName["V1"].DDL != shipped+deletedPredicate {
		t.Errorf("V1 must be V0 plus exactly one clause, got: %s", byName["V1"].DDL)
	}
	if byName["V-"].DDL != "" {
		t.Errorf("V- is the index-absent condition and must install nothing, got: %s", byName["V-"].DDL)
	}
}

// TestExplainVariantsSharePartialPredicate ties the hand-written candidates to
// the shipped one. If the runtime's claimable states change, this fails rather
// than letting the candidates index a different set of rows than the control.
func TestExplainVariantsSharePartialPredicate(t *testing.T) {
	t.Parallel()

	variants, err := explainVariants()
	if err != nil {
		t.Fatalf("explainVariants: %v", err)
	}
	for _, v := range variants {
		if v.DDL == "" {
			continue
		}
		if !strings.Contains(v.DDL, claimStatePredicate) {
			t.Errorf("%s must carry the shipped partial predicate %q, got: %s", v.Name, claimStatePredicate, v.DDL)
		}
		if !strings.Contains(v.DDL, "IF NOT EXISTS "+claimIndexName+" ON jobs (") {
			t.Errorf("%s must install under the name %s so its plan is comparable, got: %s", v.Name, claimIndexName, v.DDL)
		}
		if v.Name != "V0" && !strings.Contains(v.DDL, strings.TrimPrefix(deletedPredicate, " ")) {
			t.Errorf("%s must fold deleted_at IS NULL into the predicate, got: %s", v.Name, v.DDL)
		}
	}
}

// TestExplainVariantsCoverScheduledAt guards a coupling outside this package:
// health_test.go drops jobs_ready so it can drop the scheduled_at column, and
// SQLite refuses to drop a column an index still references. A candidate that
// stopped covering scheduled_at would change that test's premise.
func TestExplainVariantsCoverScheduledAt(t *testing.T) {
	t.Parallel()

	variants, err := explainVariants()
	if err != nil {
		t.Fatalf("explainVariants: %v", err)
	}
	for _, v := range variants {
		if v.DDL == "" {
			continue
		}
		if !strings.Contains(v.DDL, "scheduled_at") {
			t.Errorf("%s must still cover scheduled_at: %s", v.Name, v.DDL)
		}
	}
}

func TestExplainConfigValidate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		cfg     ExplainConfig
		wantErr bool
		check   func(*testing.T, ExplainConfig)
	}{
		{
			name: "defaults fill in the matrix shape",
			cfg:  ExplainConfig{DSN: "postgres://localhost/db", Jobs: 10},
			check: func(t *testing.T, got ExplainConfig) {
				t.Helper()
				if got.Queues != defaultExplainQueues || got.Limit != defaultExplainLimit {
					t.Errorf("queues/limit defaults not applied: %+v", got)
				}
				if got.ExecutorClass != defaultExecutorClass || got.Lease <= 0 {
					t.Errorf("class/lease defaults not applied: %+v", got)
				}
			},
		},
		{name: "no DSN", cfg: ExplainConfig{Jobs: 10}, wantErr: true},
		{name: "no jobs", cfg: ExplainConfig{DSN: "postgres://localhost/db"}, wantErr: true},
		{name: "negative queues", cfg: ExplainConfig{DSN: "postgres://localhost/db", Jobs: 1, Queues: -1}, wantErr: true},
		{name: "negative limit", cfg: ExplainConfig{DSN: "postgres://localhost/db", Jobs: 1, Limit: -1}, wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got, err := tc.cfg.validate()
			if tc.wantErr {
				if err == nil {
					t.Fatalf("validate must reject %+v", tc.cfg)
				}
				return
			}
			if err != nil {
				t.Fatalf("validate: %v", err)
			}
			tc.check(t, got)
		})
	}
}

// TestExplainReportNeverCarriesACredential is the artifact-safety assertion.
// These reports are committed under docs/, so a run that failed on connect must
// still have redacted the target it was pointed at.
func TestExplainReportNeverCarriesACredential(t *testing.T) {
	t.Parallel()

	// Port 1 refuses immediately, so this exercises the failure path without
	// waiting on a timeout.
	const dsn = "postgres://someone:hunter2@127.0.0.1:1/flywheel_test?sslmode=disable"

	report, err := ExplainClaim(context.Background(), ExplainConfig{DSN: dsn, Jobs: 1})
	if err == nil {
		t.Fatalf("a refused connection must be an error")
	}
	if strings.Contains(report.Target, "hunter2") || strings.Contains(report.Target, "someone") {
		t.Fatalf("the report carried a credential: %q", report.Target)
	}
	if !strings.Contains(report.Target, "127.0.0.1:1") {
		t.Errorf("the report must still name the host it targeted: %q", report.Target)
	}
	if strings.Contains(report.Text(), "hunter2") {
		t.Errorf("the rendered artifact carried a credential")
	}
}

// TestPinLeaseToken covers the one edit made to a captured statement beyond the
// predicate rewrite. It must leave the statement executable and must not touch
// anything the planner reads.
func TestPinLeaseToken(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		sql  string
		want string
	}{
		{
			// The stand-in for a minted token is deliberately not UUID-shaped. The
			// function only reads the quotes around it, and a realistic-looking one
			// here is a fresh high-entropy string in a committed file that every
			// secret scanner downstream would flag — which is half of why the
			// pinning exists at all.
			name: "the minted token is replaced",
			sql:  `SET leased_until = '2026-07-27', lease_token = 'MINTED-BY-THE-DRIVER', updated_at = 'x'`,
			want: `SET leased_until = '2026-07-27', lease_token = '` + pinnedLeaseToken + `', updated_at = 'x'`,
		},
		{
			name: "a statement without the marker is untouched",
			sql:  `SELECT 1`,
			want: `SELECT 1`,
		},
		{
			name: "an unterminated literal is untouched rather than mangled",
			sql:  `lease_token = 'oops`,
			want: `lease_token = 'oops`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			if got := pinLeaseToken(tc.sql); got != tc.want {
				t.Errorf("pinLeaseToken\n got: %s\nwant: %s", got, tc.want)
			}
		})
	}
}

// TestPinnedLeaseTokenIsStableAcrossCaptures is why the pinning exists: without
// it every re-run of the matrix rewrites every statement in the artifact with a
// fresh random value, and a before/after diff is unreadable.
func TestPinnedLeaseTokenIsStableAcrossCaptures(t *testing.T) {
	dsn := requireDSN(t)
	ctx := context.Background()

	admin, err := openPool(dsn, 2)
	if err != nil {
		t.Fatalf("openPool: %v", err)
	}
	defer closePool(admin)

	schema := newSchemaName()
	if err = createSchema(ctx, admin, schema); err != nil {
		t.Fatalf("createSchema: %v", err)
	}
	defer func() { _ = dropSchema(ctx, admin, schema) }()

	db, err := openPool(withSearchPath(dsn, schema), 2)
	if err != nil {
		t.Fatalf("openPool: %v", err)
	}
	defer closePool(db)
	if err = installSchema(ctx, db, IndexesFull, StorageDefault); err != nil {
		t.Fatalf("installSchema: %v", err)
	}

	first, err := captureClaimSQL(ctx, db, []string{"q0"}, "loadtest", true, 8, time.Minute)
	if err != nil {
		t.Fatalf("captureClaimSQL: %v", err)
	}
	second, err := captureClaimSQL(ctx, db, []string{"q0"}, "loadtest", true, 8, time.Minute)
	if err != nil {
		t.Fatalf("captureClaimSQL: %v", err)
	}
	if !strings.Contains(first, pinnedLeaseToken) {
		t.Fatalf("the captured statement must carry the pinned token:\n%s", first)
	}
	// Only the token is compared. The timestamps on the same line still differ
	// between captures, and they must: they are predicate values, so pinning them
	// would change the plan the matrix is measuring.
	tokenValue := func(sql string) string {
		const marker = "lease_token = '"
		i := strings.Index(sql, marker)
		if i < 0 {
			return ""
		}
		value, _, _ := strings.Cut(sql[i+len(marker):], "'")
		return value
	}
	if got := tokenValue(first); got != pinnedLeaseToken || got != tokenValue(second) {
		t.Errorf("two captures must agree on the lease token: %q vs %q", got, tokenValue(second))
	}
	// And the statement is still executable after the edit.
	if _, err = explainStatement(ctx, db, first); err != nil {
		t.Fatalf("a pinned statement must still be EXPLAIN-able: %v", err)
	}
}

func TestExplainQueuesAndClasses(t *testing.T) {
	t.Parallel()

	if got := explainQueues(3); len(got) != 3 || got[0] != "q0" || got[2] != "q2" {
		t.Errorf("explainQueues(3) = %v", got)
	}
	// The idle conditions only measure an empty poll if the queue they name is
	// genuinely empty, which means it must never collide with a seeded one.
	for _, q := range explainQueues(64) {
		if q == idleQueue {
			t.Fatalf("the idle queue name %q collides with a seeded queue", idleQueue)
		}
	}
	classes := explainClasses("svc")
	if len(classes) != explainSeedClasses {
		t.Fatalf("explainClasses must produce %d classes, got %v", explainSeedClasses, classes)
	}
	if classes[0] != "svc" || classes[1] != "" {
		t.Errorf("a routed claim matches the class and the empty wildcard; both must be seeded: %v", classes)
	}
	// Exactly half the seeded rows are claimable by a routed runner, which is the
	// selectivity the matrix is characterized at.
	matching := 0
	for _, c := range classes {
		if c == "svc" || c == "" {
			matching++
		}
	}
	if matching*2 != len(classes) {
		t.Errorf("the routed predicate must match half the seeded classes, got %d of %d", matching, len(classes))
	}
}

func TestExplainRenderHelpers(t *testing.T) {
	t.Parallel()

	if got := withThousands(1000000); got != "1,000,000" {
		t.Errorf("withThousands(1000000) = %q", got)
	}
	if got := withThousands(-1234); got != "-1,234" {
		t.Errorf("withThousands(-1234) = %q", got)
	}
	if got := withThousands(0); got != "0" {
		t.Errorf("withThousands(0) = %q", got)
	}
	if got := humanBytes(1536); got != "1.5 KB" {
		t.Errorf("humanBytes(1536) = %q", got)
	}
	if got := truncate("abcdefgh", 4); got != "abc…" {
		t.Errorf("truncate must mark that it clipped, got %q", got)
	}
	if got := nodeLabel("Seq Scan on jobs jobs_1  (cost=0.00..1.00 rows=1 width=1)"); got != "Seq Scan on jobs jobs_1" {
		t.Errorf("nodeLabel = %q", got)
	}
}

// TestExplainReportTextCarriesEveryCell proves the rendered artifact is complete:
// a summary that silently omitted a measured cell would read as a matrix with a
// gap the run did not have.
func TestExplainReportTextCarriesEveryCell(t *testing.T) {
	t.Parallel()

	variants, err := explainVariants()
	if err != nil {
		t.Fatalf("explainVariants: %v", err)
	}
	report := ExplainReport{
		Target: "postgres://localhost:5432/flywheel_test", Schema: "lt_x", Jobs: 10,
		Queues: explainQueues(1), Classes: explainClasses("loadtest"), Limit: 8,
		Conditions: explainConditions(), Variants: variants,
		Statements: []ExplainStatement{{Condition: "A", Predicate: predicateEmitted, SQL: capturedRoutedSQL}},
		Cells: []ExplainCell{
			{Condition: "A", Predicate: predicateEmitted, Variant: "V-", Summary: parsePlan(seqScanPlan), Plan: seqScanPlan},
			{Condition: "A", Predicate: predicateInList, Variant: "V1", Summary: parsePlan(indexScanPlan), Plan: indexScanPlan},
		},
		Notes: []string{"a note"},
	}

	text := report.Text()
	for _, want := range []string{
		"A/P0/V-", "A/P1/V1", "external merge  Disk: 3904kB", "500,000",
		"Seq Scan on jobs jobs_1", "Index Scan using jobs_ready on jobs jobs_1",
		"a note", "1 queue,  routed by executor_class", claimIndexName,
	} {
		if !strings.Contains(text, want) {
			t.Errorf("the rendered artifact must contain %q", want)
		}
	}
	// The full plan text, not only the summary.
	if strings.Count(text, "Sort Key: jobs_1.priority, jobs_1.scheduled_at") == 0 {
		t.Errorf("the artifact must carry the plans in full, not only the summary")
	}
}

// TestExplainReportTextHasNoTrailingWhitespace pins a property of the committed
// file rather than of the code that writes it.
//
// The summary table pads its columns to align them, and padding a row's last
// column leaves trailing spaces on it. That is invisible in review, survives
// gofmt and the linters — neither looks inside a generated .txt — and is caught
// only by the repository's whitespace check, which auto-fixes rather than
// reporting, so the artifact and the renderer silently disagree from then on.
//
// The report here is deliberately built with an empty Server and Schema: those
// interpolate into a padded header line, which is the other way a line acquires
// trailing whitespace.
func TestExplainReportTextHasNoTrailingWhitespace(t *testing.T) {
	t.Parallel()

	variants, err := explainVariants()
	if err != nil {
		t.Fatalf("explainVariants: %v", err)
	}
	report := ExplainReport{
		Target: "postgres://localhost:5432/flywheel_test", Jobs: 10,
		Queues: explainQueues(1), Classes: explainClasses("loadtest"), Limit: 8,
		Conditions: explainConditions(), Variants: variants,
		Statements: []ExplainStatement{{Condition: "A", Predicate: predicateEmitted, SQL: capturedRoutedSQL}},
		Cells: []ExplainCell{
			{Condition: "A", Predicate: predicateEmitted, Variant: "V-", Summary: parsePlan(seqScanPlan), Plan: seqScanPlan},
			{Condition: "A", Predicate: predicateInList, Variant: "V1", Summary: parsePlan(indexScanPlan), Plan: indexScanPlan},
		},
		Notes: []string{"a note long enough that the bullet wrapper has to break it across more than one line of output"},
	}

	for i, line := range strings.Split(report.Text(), "\n") {
		if line != strings.TrimRight(line, " \t") {
			t.Errorf("line %d ends in whitespace: %q", i+1, line)
		}
	}
}

func TestTrimLineEnds(t *testing.T) {
	t.Parallel()

	got := trimLineEnds("a  \nb\t\n\tc\nd \t \n")
	if want := "a\nb\n\tc\nd\n"; got != want {
		t.Errorf("trimLineEnds = %q, want %q (leading whitespace must survive)", got, want)
	}
}

// TestExplainClaimCapturesTheDriversOwnSQL is the fidelity check the whole tool
// rests on, and the one thing no fixture can stand in for: that GORM's Trace
// really hands back a literal, re-executable statement for the Raw call the
// PostgreSQL driver makes, and that the statement is the driver's rather than
// this package's idea of it.
func TestExplainClaimCapturesTheDriversOwnSQL(t *testing.T) {
	dsn := requireDSN(t)
	ctx := context.Background()

	admin, err := openPool(dsn, 2)
	if err != nil {
		t.Fatalf("openPool: %v", err)
	}
	defer closePool(admin)

	schema := newSchemaName()
	if err = createSchema(ctx, admin, schema); err != nil {
		t.Fatalf("createSchema: %v", err)
	}
	defer func() { _ = dropSchema(ctx, admin, schema) }()

	db, err := openPool(withSearchPath(dsn, schema), 2)
	if err != nil {
		t.Fatalf("openPool: %v", err)
	}
	defer closePool(db)
	if err = installSchema(ctx, db, IndexesFull, StorageDefault); err != nil {
		t.Fatalf("installSchema: %v", err)
	}

	sql, err := captureClaimSQL(ctx, db, []string{"q0", "q1"}, "loadtest", false, 8, time.Minute)
	if err != nil {
		t.Fatalf("captureClaimSQL: %v", err)
	}
	for _, want := range []string{
		"FOR UPDATE SKIP LOCKED", "ORDER BY priority, scheduled_at", "LIMIT 8",
		"queue IN ('q0','q1')", orClassPredicate("loadtest"), "deleted_at IS NULL",
	} {
		if !strings.Contains(sql, want) {
			t.Errorf("the captured statement must contain %q:\n%s", want, sql)
		}
	}
	// No placeholder survives: the statement must be directly EXPLAIN-able.
	if strings.Contains(sql, "?") || strings.Contains(sql, "$1") {
		t.Errorf("the captured statement still carries bind placeholders:\n%s", sql)
	}
	if _, err = explainStatement(ctx, db, sql); err != nil {
		t.Fatalf("the captured statement must be directly EXPLAIN-able: %v", err)
	}

	// ClaimAnyClass drops the clause entirely, which is the second of the two
	// ways the claim misses the index.
	anySQL, err := captureClaimSQL(ctx, db, []string{"q0"}, "loadtest", true, 8, time.Minute)
	if err != nil {
		t.Fatalf("captureClaimSQL(claimAny): %v", err)
	}
	if strings.Contains(anySQL, "executor_class") {
		t.Errorf("a ClaimAnyClass statement must carry no class predicate:\n%s", anySQL)
	}
}

// TestExplainClaimRunsTheWholeMatrix is the end-to-end proof against a real
// server, at a size a test suite can carry. It asserts the matrix's shape and
// its isolation, not its numbers — the numbers are the committed artifact's job,
// and a test that asserted them would fail on a different machine.
func TestExplainClaimRunsTheWholeMatrix(t *testing.T) {
	dsn := requireDSN(t)

	report, err := ExplainClaim(context.Background(), ExplainConfig{
		DSN: dsn, Jobs: 400, Queues: 3, Seed: 1, ExecutorClass: "loadtest", Limit: 8,
	})
	if err != nil {
		t.Fatalf("ExplainClaim: %v", err)
	}

	// Nine statements: the three routed conditions in both spellings, the three
	// ClaimAnyClass ones in the single spelling they have.
	if len(report.Statements) != 9 {
		t.Errorf("want 9 captured statements, got %d", len(report.Statements))
	}
	variants, err := explainVariants()
	if err != nil {
		t.Fatalf("explainVariants: %v", err)
	}
	want := 0
	for _, v := range variants {
		for _, s := range report.Statements {
			if v.Only == "" || v.Only == s.Condition {
				want++
			}
		}
	}
	if len(report.Cells) != want {
		t.Fatalf("want %d cells, got %d", want, len(report.Cells))
	}

	seen := make(map[string]bool, len(report.Cells))
	for _, cell := range report.Cells {
		if seen[cell.Key()] {
			t.Errorf("duplicate cell %s", cell.Key())
		}
		seen[cell.Key()] = true
		if len(cell.Plan) == 0 {
			t.Errorf("%s has no plan", cell.Key())
		}
		if cell.Summary.ScanKind == "" {
			t.Errorf("%s: no scan node parsed out of:\n%s", cell.Key(), strings.Join(cell.Plan, "\n"))
		}
		if cell.Summary.ExecutionMS < 0 {
			t.Errorf("%s: no execution time parsed", cell.Key())
		}
	}
	if !seen["A/P0/V-"] || !seen["D/P0/V3"] || !seen["A/P0/V4"] || !seen["E/P1/V2"] || !seen["F/P0/V0"] {
		t.Errorf("the matrix is missing a corner: %v", seen)
	}
	// V4 is a question about one shape, and measuring it everywhere would triple
	// the artifact for no information.
	if seen["B/P0/V4"] {
		t.Errorf("V4 must be measured under condition A only")
	}

	// Every EXPLAIN ANALYZE executed the claim inside a rolled-back transaction,
	// so the table it measured is the table it started with.
	if !strings.Contains(report.Text(), "rolled back") {
		t.Errorf("the artifact must state the isolation its comparability rests on")
	}

	// The run's schema is gone: a matrix that leaked a schema per run would fill
	// the target database.
	admin, err := openPool(dsn, 1)
	if err != nil {
		t.Fatalf("openPool: %v", err)
	}
	defer closePool(admin)
	var count int64
	if err = admin.Raw(
		`SELECT count(*) FROM information_schema.schemata WHERE schema_name = ?`, report.Schema,
	).Scan(&count).Error; err != nil {
		t.Fatalf("count schemata: %v", err)
	}
	if count != 0 {
		t.Errorf("ExplainClaim leaked schema %s", report.Schema)
	}
}

// TestExplainClaimLeavesTheTableIntact is trap 5 stated as a test: EXPLAIN
// ANALYZE on the claim executes the UPDATE, and a matrix that let one cell claim
// rows would have every later cell measuring a different table.
func TestExplainClaimLeavesTheTableIntact(t *testing.T) {
	dsn := requireDSN(t)
	ctx := context.Background()

	admin, err := openPool(dsn, 2)
	if err != nil {
		t.Fatalf("openPool: %v", err)
	}
	defer closePool(admin)

	schema := newSchemaName()
	if err = createSchema(ctx, admin, schema); err != nil {
		t.Fatalf("createSchema: %v", err)
	}
	defer func() { _ = dropSchema(ctx, admin, schema) }()

	db, err := openPool(withSearchPath(dsn, schema), 2)
	if err != nil {
		t.Fatalf("openPool: %v", err)
	}
	defer closePool(db)
	if err = installSchema(ctx, db, IndexesFull, StorageDefault); err != nil {
		t.Fatalf("installSchema: %v", err)
	}

	cfg := ExplainConfig{DSN: dsn, Jobs: 200, Queues: 1, Seed: 1, ExecutorClass: "loadtest", Limit: 8}
	cfg, err = cfg.validate()
	if err != nil {
		t.Fatalf("validate: %v", err)
	}
	if err = seedClaimable(ctx, db, cfg, explainQueues(1), explainClasses("loadtest")); err != nil {
		t.Fatalf("seedClaimable: %v", err)
	}

	available := func() int64 {
		var n int64
		if err := db.Raw(`SELECT count(*) FROM jobs WHERE state = 'available'`).Scan(&n).Error; err != nil {
			t.Fatalf("count available: %v", err)
		}
		return n
	}
	before := available()
	if before != int64(cfg.Jobs) {
		t.Fatalf("seed produced %d available rows, want %d", before, cfg.Jobs)
	}

	sql, err := captureClaimSQL(ctx, db, []string{"q0"}, "loadtest", false, 8, time.Minute)
	if err != nil {
		t.Fatalf("captureClaimSQL: %v", err)
	}
	for range 3 {
		if _, err = explainStatement(ctx, db, sql); err != nil {
			t.Fatalf("explainStatement: %v", err)
		}
	}
	if after := available(); after != before {
		t.Errorf("EXPLAIN ANALYZE claimed rows: %d available before, %d after", before, after)
	}
}

// TestInstallClaimIndexReplacesRatherThanSkips is the reconciliation trap in
// miniature. CREATE INDEX IF NOT EXISTS matches on name alone, so without the
// unconditional drop every variant after the first would silently measure the
// first one's definition.
func TestInstallClaimIndexReplacesRatherThanSkips(t *testing.T) {
	dsn := requireDSN(t)
	ctx := context.Background()

	admin, err := openPool(dsn, 2)
	if err != nil {
		t.Fatalf("openPool: %v", err)
	}
	defer closePool(admin)

	schema := newSchemaName()
	if err = createSchema(ctx, admin, schema); err != nil {
		t.Fatalf("createSchema: %v", err)
	}
	defer func() { _ = dropSchema(ctx, admin, schema) }()

	db, err := openPool(withSearchPath(dsn, schema), 2)
	if err != nil {
		t.Fatalf("openPool: %v", err)
	}
	defer closePool(db)
	if err = installSchema(ctx, db, IndexesFull, StorageDefault); err != nil {
		t.Fatalf("installSchema: %v", err)
	}

	definition := func() string {
		var def string
		if err := db.Raw(
			`SELECT indexdef FROM pg_indexes WHERE schemaname = current_schema() AND indexname = ?`, claimIndexName,
		).Scan(&def).Error; err != nil {
			t.Fatalf("indexdef: %v", err)
		}
		return def
	}

	shipped := definition()
	if shipped == "" {
		t.Fatalf("installSchema must have created %s", claimIndexName)
	}

	// A key no shipped definition can match, so this asserts the replacement
	// happened without asserting anything about what ships — which is the whole
	// point of a variant loop, and would make this test fail the next time the
	// runtime's own definition changes.
	const variantKey = "priority, scheduled_at, queue, executor_class"
	if err = installClaimIndex(ctx, db, variantIndexDDL(variantKey, "")); err != nil {
		t.Fatalf("installClaimIndex: %v", err)
	}
	replaced := definition()
	if replaced == shipped {
		t.Fatalf("installClaimIndex must replace the definition, not skip it: %s", replaced)
	}
	if !strings.Contains(replaced, "("+variantKey+")") {
		t.Errorf("the replacement must carry the variant's key %q: %s", variantKey, replaced)
	}
	if !strings.Contains(replaced, "deleted_at IS NULL") {
		t.Errorf("the replacement must carry the deleted_at predicate: %s", replaced)
	}

	// And the absent condition really removes it.
	if err = installClaimIndex(ctx, db, ""); err != nil {
		t.Fatalf("installClaimIndex(absent): %v", err)
	}
	if got := definition(); got != "" {
		t.Errorf("the absent variant must leave no %s behind: %s", claimIndexName, got)
	}
}
