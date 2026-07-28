package flywheel

import (
	"context"
	"fmt"

	"gorm.io/gorm"
)

// IndexKind classifies an index by what the runtime loses without it.
type IndexKind string

const (
	// IndexCorrectness marks an index the runtime's guarantees depend on.
	// Omitting one does not slow the runtime down — it silently removes a
	// guarantee.
	IndexCorrectness IndexKind = "correctness"
	// IndexPerformance marks an index that only affects query plans. Omitting one
	// is safe and costs throughput at depth.
	IndexPerformance IndexKind = "performance"
)

// Index is one index the runtime needs, as portable DDL plus the reason it
// exists.
type Index struct {
	// Name is the index name as it appears in the database.
	Name string
	// Kind is what omitting the index costs — a guarantee or throughput.
	Kind IndexKind
	// Table is the table the index is created on.
	Table string
	// DDL is the CREATE INDEX statement, written with IF NOT EXISTS so applying
	// it twice is a no-op.
	DDL string
}

// IndexSet returns every index the runtime relies on for the given GORM dialect
// name (db.Name()), classified, in application order. A host that installs the
// schema itself — versioned SQL, an external migration tool — creates the tables
// from Models and then applies these; Migrate applies exactly this set.
//
// Omitting an IndexCorrectness entry does not degrade performance, it removes a
// guarantee: without jobs_unique_key and jobs_unique_active_key the database
// accepts duplicate enqueues and ErrAlreadyEnqueued is never returned.
//
// # This is an install-time step, not a source for generated migration SQL
//
// Apply these statements from a host's install or deploy path — the same place
// it applies its migrations — not by pasting them into a generated migration. A
// GORM schema loader (atlas-provider-gorm and friends) reads the row structs,
// and the row structs carry no index tags for these: every statement here has a
// WHERE predicate or spans columns a struct tag cannot express. So a migration
// that creates them describes indexes the loader still does not, and the *next*
// diff sees indexes in the migration directory that are absent from the desired
// state and proposes dropping them back out.
//
// Applied outside the migration directory they are invisible to a versioned
// diff, which compares the directory against the loader and never inspects the
// live database. (A declarative `schema apply`, which does inspect the database,
// would drop them — one more reason a shared database wants versioned mode.)
//
// An unsupported dialect returns ErrUnsupportedDialect, the same error Migrate
// returns for it.
func IndexSet(dialect string) ([]Index, error) {
	switch dialect {
	case "postgres", "sqlite":
	default:
		return nil, fmt.Errorf(
			"flywheel: %w: %q: partial indexes require postgres or sqlite",
			ErrUnsupportedDialect, dialect,
		)
	}
	return runtimeIndexes(), nil
}

// Indexes returns IndexSet's DDL statements in application order. It is the
// one-line form for a host that wants every index and no classification.
//
// The install-time caveat on IndexSet applies here unchanged: these statements
// are run against a database, not pasted into a generated migration.
func Indexes(dialect string) ([]string, error) {
	set, err := IndexSet(dialect)
	if err != nil {
		return nil, err
	}
	ddl := make([]string, len(set))
	for i, idx := range set {
		ddl[i] = idx.DDL
	}
	return ddl, nil
}

// InstallIndexes applies every index for db's dialect, in order. It is the
// host-owned install step: a host whose migration tool created the three tables
// from Models calls this once per install or deploy to reach index parity with
// Migrate, which applies the same set through this same function.
//
// Every statement uses IF NOT EXISTS, so InstallIndexes is idempotent and safe
// to run on every deploy. It creates no tables and alters no columns: on a
// database that lacks the three tables it fails on the first statement.
func InstallIndexes(ctx context.Context, db *gorm.DB) error {
	if db == nil {
		return fmt.Errorf("flywheel: InstallIndexes: db is nil")
	}
	if err := applyIndexes(ctx, db); err != nil {
		return fmt.Errorf("flywheel: InstallIndexes: %w", err)
	}
	return nil
}

// applyIndexes is the one apply path: InstallIndexes is its exported form and
// Migrate reaches the index step through it too, so both install the same
// statements in the same order. It leaves the caller's name off the error so
// each entry point can prefix its own.
func applyIndexes(ctx context.Context, db *gorm.DB) error {
	set, err := IndexSet(db.Name())
	if err != nil {
		return err
	}
	for _, idx := range set {
		if err := db.WithContext(ctx).Exec(idx.DDL).Error; err != nil {
			return fmt.Errorf("create index %s: %w", idx.Name, err)
		}
	}
	return nil
}

// runtimeIndexes is the dialect-independent index set. PostgreSQL and SQLite
// both support partial indexes (CREATE INDEX ... WHERE) and IF NOT EXISTS, so
// the DDL is portable between them; IndexSet owns the dialect gate and rejects
// dialects that cannot express a partial index (e.g. MySQL) rather than silently
// dropping idempotency.
//
// The order is the application order: it is stable, and Migrate depends on it.
func runtimeIndexes() []Index {
	return []Index{
		{
			// Correctness: enqueue idempotency. A duplicate unique_key insert is
			// rejected by the database, which is what Client.insert maps to
			// ErrAlreadyEnqueued. Without the index there is no rejection.
			Name: "jobs_unique_key", Kind: IndexCorrectness, Table: "jobs",
			DDL: `CREATE UNIQUE INDEX IF NOT EXISTS jobs_unique_key ON jobs (unique_key) WHERE unique_key IS NOT NULL`,
		},
		{
			// Correctness: at most one active job per key. Unlike jobs_unique_key
			// the constraint is scoped to live states, so the key frees up once
			// the job reaches a terminal state and the same key can enqueue again.
			Name: "jobs_unique_active_key", Kind: IndexCorrectness, Table: "jobs",
			DDL: `CREATE UNIQUE INDEX IF NOT EXISTS jobs_unique_active_key ON jobs (unique_active_key) WHERE unique_active_key IS NOT NULL AND state IN ('available', 'running', 'retryable', 'scheduled')`,
		},
		{
			// Performance: the claim hot path.
			//
			// The key is (queue, priority, scheduled_at) — deliberately without
			// executor_class, even though the claim filters on it. A claim orders by
			// priority, scheduled_at, and with the class in the key neither of its
			// two routing modes can reach an ordered scan: the routed mode's
			// `class = ? OR class = ''` is an OR on the second column, and
			// ClaimAnyClass omits the column entirely and leaves a gap in the
			// leading columns. Both fall back to scanning the whole ready set and
			// sorting it. Dropping the column fixes both at once and costs a cheap
			// heap filter on the rows the ordered scan already visits.
			//
			// queue leads rather than priority for the case that dominates a
			// deployment's wall clock: a runner polls on a fixed interval whether or
			// not there is work, so most claims a queue ever serves return nothing.
			// With the ordering columns leading, an empty poll walks the whole index
			// instead of probing it. See docs/BENCHMARKS.md for both plans.
			//
			// deleted_at is in the predicate because the claim filters it, so
			// without it every candidate tuple needs a heap visit to be rejected.
			Name: "jobs_ready", Kind: IndexPerformance, Table: "jobs",
			DDL: `CREATE INDEX IF NOT EXISTS jobs_ready ON jobs (queue, priority, scheduled_at) WHERE state IN ('available', 'retryable', 'scheduled') AND deleted_at IS NULL`,
		},
		{
			// Performance: follow-up / DAG lookup.
			Name: "jobs_parent", Kind: IndexPerformance, Table: "jobs",
			DDL: `CREATE INDEX IF NOT EXISTS jobs_parent ON jobs (parent_job_id) WHERE parent_job_id IS NOT NULL`,
		},
		{
			// Performance: stuck-lease / orphan-recovery sweep.
			Name: "jobs_running_leased", Kind: IndexPerformance, Table: "jobs",
			DDL: `CREATE INDEX IF NOT EXISTS jobs_running_leased ON jobs (leased_until) WHERE state = 'running'`,
		},
		{
			// Performance: counts by state, the telemetry read.
			//
			// Three readers, not two: SampleQueueHealth's GROUP BY state, which the
			// scheduler heartbeat and a /metrics scrape both sample on a timer;
			// Overview's identical grouping; and CountActiveJobs, whose
			// state IN (…) predicate is the same access path.
			//
			// The key is (state) alone rather than (state, queue): none of the three
			// filters by queue, and a second column no reader constrains only widens
			// the index. The deleted_at IS NULL predicate matches GORM's soft-delete
			// scope, which is what makes the index usable for these reads at all.
			//
			// Overview's optional kind filter is not served — that would need
			// (kind, state) — and it is not meant to be. It is an inspection query a
			// human runs, not a scrape path something polls.
			Name: "jobs_state", Kind: IndexPerformance, Table: "jobs",
			DDL: `CREATE INDEX IF NOT EXISTS jobs_state ON jobs (state) WHERE deleted_at IS NULL`,
		},
		{
			// Performance: soft-delete restore/audit lookups. A partial index a
			// struct tag cannot express, so it lives here rather than on
			// jobRow.DeletedAt.
			Name: "idx_jobs_deleted_at", Kind: IndexPerformance, Table: "jobs",
			DDL: `CREATE INDEX IF NOT EXISTS idx_jobs_deleted_at ON jobs (deleted_at) WHERE deleted_at IS NOT NULL`,
		},
		{
			// Correctness: one audit row per attempt. The attempt counter is the
			// job_runs audit key, and the uniqueness of (job_id, attempt) is what
			// makes it one — see planFinalize's free-snooze reasoning.
			Name: "job_runs_job_attempt", Kind: IndexCorrectness, Table: "job_runs",
			DDL: `CREATE UNIQUE INDEX IF NOT EXISTS job_runs_job_attempt ON job_runs (job_id, attempt)`,
		},
		{
			// Correctness: one schedule per slug, which is what makes
			// UpsertPeriodic an upsert rather than an append.
			Name: "idx_job_periodics_slug", Kind: IndexCorrectness, Table: "job_periodics",
			DDL: `CREATE UNIQUE INDEX IF NOT EXISTS idx_job_periodics_slug ON job_periodics (slug)`,
		},
	}
}
