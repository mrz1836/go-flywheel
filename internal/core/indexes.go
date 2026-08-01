package core

import (
	"context"
	"fmt"
	"regexp"
	"strings"

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

// IndexOpts configures InstallIndexesWithOptions. The zero value reports drift as
// an error and installs nothing over a drifted index — the safe default, because
// correcting drift takes a table-wide lock a host should choose to pay
// deliberately rather than have the library take on a deploy it did not ask for.
type IndexOpts struct {
	// Reconcile drops and recreates an index whose installed definition has drifted
	// from the runtime's, rather than reporting it. It is off by default because the
	// rebuild takes an ACCESS EXCLUSIVE lock on the table for its duration: on a
	// large jobs table that blocks every reader and writer until the rebuild
	// finishes, which is a cost a host pays deliberately, not one the library takes
	// on a deploy the host did not ask for.
	//
	// The drop and recreate run inside one transaction, so the lock is held for the
	// whole rebuild and a correctness-bearing unique index is never briefly absent —
	// no duplicate slips through the window. CREATE INDEX CONCURRENTLY, which would
	// avoid the lock, cannot run inside a transaction and is out of scope; a host
	// that needs it acts on the InspectIndexes report by hand.
	//
	// With Reconcile off, drift is returned as an IndexDriftError naming each index,
	// its installed definition, and the expected one, and no index is dropped. An
	// absent index is created either way — that is a first install, not drift.
	Reconcile bool
}

// InstallIndexes applies every index for db's dialect, reporting drift as an
// error rather than reconciling it. It is InstallIndexesWithOptions(ctx, db,
// IndexOpts{}) — the safe default that never takes a lock the caller did not ask
// for.
//
// It is the host-owned install step: a host whose migration tool created the
// runtime's tables from Models calls this once per install or deploy to reach
// index parity with Migrate, which applies the same set through the same path.
//
// It is idempotent and safe to run on every deploy: an absent index is created,
// a matching index is left untouched, and a re-run over an up-to-date schema does
// nothing. It creates no tables and alters no columns: on a database that lacks
// the runtime's tables it fails on the first statement. On a database carrying an
// index whose definition has drifted it returns an IndexDriftError rather than
// silently leaving the stale index in place — see InstallIndexesWithOptions to
// reconcile instead.
func InstallIndexes(ctx context.Context, db *gorm.DB) error {
	return InstallIndexesWithOptions(ctx, db, IndexOpts{})
}

// InstallIndexesWithOptions applies every index for db's dialect, in order,
// reconciling by definition rather than by name.
//
// An absent index is created. An index whose installed definition matches the
// runtime's is left alone. An index whose definition has drifted is, by default,
// reported as an IndexDriftError and left in place; set IndexOpts.Reconcile to
// drop and recreate it instead — see IndexOpts.Reconcile for the lock that takes.
//
// It creates no tables and alters no columns: on a database that lacks the
// runtime's tables it fails on the first statement.
func InstallIndexesWithOptions(ctx context.Context, db *gorm.DB, opts IndexOpts) error {
	if db == nil {
		return fmt.Errorf("flywheel: InstallIndexes: db is nil")
	}
	if err := applyIndexes(ctx, db, opts); err != nil {
		return fmt.Errorf("flywheel: InstallIndexes: %w", err)
	}
	return nil
}

// applyIndexes is the one apply path: InstallIndexes reaches it through
// InstallIndexesWithOptions and Migrate reaches the index step through it too, so
// both act on the same set under the same rules. It leaves the caller's name off
// the error so each entry point can prefix its own.
//
// It acts only on what needs action — inspectIndexes returns absent, drifted, and
// retired indexes, and a matching index appears in none. An absent index is created
// (the normal first install). A drifted index is dropped and recreated when the
// caller opted in, or collected and returned as an IndexDriftError when it did
// not: the library never takes the rebuild's ACCESS EXCLUSIVE lock uninvited. A
// retired index — one the runtime once installed and no longer declares — is
// dropped unconditionally, since that takes no lock worth deferring.
func applyIndexes(ctx context.Context, db *gorm.DB, opts IndexOpts) error {
	set, err := IndexSet(db.Name())
	if err != nil {
		return err
	}
	drift, err := inspectIndexes(ctx, db, set)
	if err != nil {
		return err
	}
	var drifted []IndexDrift
	for _, d := range drift {
		switch {
		case d.Retired:
			// A superseded index the runtime no longer declares, still installed.
			// Drop it unconditionally — DROP INDEX IF EXISTS is cheap, idempotent,
			// and loses no data, so unlike a drift rebuild it needs no opt-in. It runs
			// after the desired-set entries above, so any covering replacement is
			// created before its predecessor is dropped.
			if err := db.WithContext(ctx).Exec(`DROP INDEX IF EXISTS ` + quoteIndexIdent(d.Name)).Error; err != nil {
				return fmt.Errorf("drop retired index %s: %w", d.Name, err)
			}
		case d.Installed == "":
			// Absent: a first install, not drift. Create it. inspectIndexes preserves
			// IndexSet order, so absent indexes are created in that order.
			if err := db.WithContext(ctx).Exec(d.Expected).Error; err != nil {
				return fmt.Errorf("create index %s: %w", d.Name, err)
			}
		case opts.Reconcile:
			if err := reconcileIndex(ctx, db, d); err != nil {
				return err
			}
		default:
			drifted = append(drifted, d)
		}
	}
	if len(drifted) > 0 {
		return &IndexDriftError{Drift: drifted}
	}
	return nil
}

// reconcileIndex drops and recreates one drifted index inside a single
// transaction. The transaction is the point: DROP INDEX + CREATE INDEX each take
// an ACCESS EXCLUSIVE lock on the table, and holding both under one transaction
// keeps the lock for the whole rebuild — so a correctness-bearing unique index is
// never briefly absent and no duplicate slips through the window between drop and
// create.
func reconcileIndex(ctx context.Context, db *gorm.DB, d IndexDrift) error {
	return db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Exec(`DROP INDEX ` + quoteIndexIdent(d.Name)).Error; err != nil {
			return fmt.Errorf("reconcile index %s: drop: %w", d.Name, err)
		}
		if err := tx.Exec(d.Expected).Error; err != nil {
			return fmt.Errorf("reconcile index %s: create: %w", d.Name, err)
		}
		return nil
	})
}

// quoteIndexIdent double-quotes an index name for a DROP INDEX statement. The
// runtime's names are fixed safe identifiers, so this is hygiene rather than a
// guard against injection — but the name reaches SQL as text, so it is quoted the
// way any identifier should be.
func quoteIndexIdent(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

// IndexDrift is one runtime index whose installed definition does not match what
// the runtime declares, or a retired index the runtime no longer declares that is
// still installed.
type IndexDrift struct {
	// Name is the index name, as both the runtime and the catalog call it.
	Name string
	// Installed is the definition the catalog reports, or the empty string when
	// the index is absent.
	Installed string
	// Expected is the runtime's DDL — the definition the index should carry. It is
	// empty for a retired index, which the runtime no longer declares and only
	// wants dropped.
	Expected string
	// Retired is true when this is not a drifted runtime index but a superseded one
	// the runtime once installed and no longer declares (retiredIndexNames). The
	// installer drops it; InspectIndexes surfaces it so a host that hand-applies DDL
	// sees the straggler, but it is never counted as drift and never fails an
	// install.
	Retired bool
}

// InspectIndexes reports every runtime index that is absent from db or whose
// installed definition has drifted from the one the runtime declares. It reads
// the catalog and writes nothing: it is the parity check a host runs to assert
// its schema matches the runtime, without recreating anything and without
// writing its own definition normalizer.
//
// The comparison is by definition, not by name. Both dialects install indexes
// with CREATE INDEX IF NOT EXISTS, which matches on the name alone, so a database
// already carrying an index of that name keeps its old definition through a
// re-install that reports success. A check that probes by name passes against
// that stale index; this does not — it reads the definition back from
// pg_indexes.indexdef or sqlite_master.sql, the only authority on what is
// actually installed.
//
// An index whose definition matches produces no entry, so an empty slice means
// parity: every runtime index is present with the runtime's definition, and no
// retired index survives. A retired index the runtime no longer declares but that
// is still installed is reported with Retired set and an empty Expected — surfaced
// so a host that hand-applies DDL sees the straggler, never counted as drift. An
// unsupported dialect returns ErrUnsupportedDialect.
func InspectIndexes(ctx context.Context, db *gorm.DB) ([]IndexDrift, error) {
	if db == nil {
		return nil, fmt.Errorf("flywheel: InspectIndexes: db is nil")
	}
	set, err := IndexSet(db.Name())
	if err != nil {
		return nil, fmt.Errorf("flywheel: InspectIndexes: %w", err)
	}
	drift, err := inspectIndexes(ctx, db, set)
	if err != nil {
		return nil, fmt.Errorf("flywheel: InspectIndexes: %w", err)
	}
	return drift, nil
}

// inspectIndexes compares the runtime's declared set against what db's catalog
// holds, returning one IndexDrift per index that is absent or whose installed
// definition has drifted, followed by one Retired entry per retiredIndexNames
// index still present. A matching index produces no entry, so the caller acts
// only on what needs action.
//
// It reads the catalog once into a name→definition map and is the shared core of
// InspectIndexes, which reports the drift, and applyIndexes, which acts on it. It
// leaves the caller's name off the error so each entry point can prefix its own.
func inspectIndexes(ctx context.Context, db *gorm.DB, set []Index) ([]IndexDrift, error) {
	installed, err := readInstalledIndexDefs(ctx, db)
	if err != nil {
		return nil, err
	}
	var drift []IndexDrift
	for _, idx := range set {
		def, ok := installed[idx.Name]
		switch {
		case !ok:
			drift = append(drift, IndexDrift{Name: idx.Name, Installed: "", Expected: idx.DDL})
		case normalizeIndexDef(def) != normalizeIndexDef(idx.DDL):
			drift = append(drift, IndexDrift{Name: idx.Name, Installed: def, Expected: idx.DDL})
		}
	}
	// Retired stragglers: an index the runtime once installed and no longer
	// declares, still present in the catalog. It carries Retired so the installer
	// drops it and InspectIndexes reports it without ever counting it as drift. The
	// entries come last, after every desired-set entry, so applyIndexes creates any
	// absent covering index before dropping the one it supersedes.
	for _, name := range retiredIndexNames() {
		if def, ok := installed[name]; ok {
			drift = append(drift, IndexDrift{Name: name, Installed: def, Retired: true})
		}
	}
	return drift, nil
}

// readInstalledIndexDefs reads db's catalog into a name→definition map for the
// runtime's tables. The definition is the statement the database actually holds —
// pg_indexes.indexdef on PostgreSQL, sqlite_master.sql on SQLite — which is the
// only authority the name-level install cannot fake.
//
// The PostgreSQL branch names the tables explicitly, so a new runtime table's
// indexes must be added to its IN list or InspectIndexes reports them as
// perpetually absent; the SQLite branch reads every index and needs no change.
func readInstalledIndexDefs(ctx context.Context, db *gorm.DB) (map[string]string, error) {
	out := map[string]string{}
	switch db.Name() {
	case "postgres":
		var rows []struct {
			Indexname string
			Indexdef  string
		}
		if err := db.WithContext(ctx).Raw(`
			SELECT indexname, indexdef FROM pg_indexes
			WHERE schemaname = current_schema()
			  AND tablename IN ('jobs', 'job_runs', 'job_periodics', 'limiter_buckets', 'limiter_holds')`).Scan(&rows).Error; err != nil {
			return nil, fmt.Errorf("read installed index definitions: %w", err)
		}
		for _, r := range rows {
			out[r.Indexname] = r.Indexdef
		}
	case "sqlite":
		// sqlite_master.sql is NULL for the indexes SQLite creates implicitly for a
		// UNIQUE/PRIMARY KEY constraint; the runtime's are all explicit CREATE
		// statements, so the IS NOT NULL guard keeps the implicit ones out of the map
		// rather than mapping a runtime name to an empty definition.
		var rows []struct {
			Name string
			Sql  string
		}
		if err := db.WithContext(ctx).Raw(`
			SELECT name, sql FROM sqlite_master
			WHERE type = 'index' AND sql IS NOT NULL`).Scan(&rows).Error; err != nil {
			return nil, fmt.Errorf("read installed index definitions: %w", err)
		}
		for _, r := range rows {
			out[r.Name] = r.Sql
		}
	default:
		return nil, fmt.Errorf(
			"flywheel: %w: %q: index inspection requires postgres or sqlite",
			ErrUnsupportedDialect, db.Name(),
		)
	}
	return out, nil
}

// index-definition normalization regexes, compiled once.
//
//nolint:gochecknoglobals // package-level compiled regexes for normalizeIndexDef
var (
	// anyArrayIndexRe matches PostgreSQL's rendering of an IN list, which the
	// catalog returns as `= ANY (ARRAY[...])`.
	anyArrayIndexRe = regexp.MustCompile(`=\s*any\s*\(\s*array\[([^\]]*)\]\s*\)`)
	// schemaOnTableIndexRe matches the `on <schema>.` qualifier PostgreSQL adds to
	// the table name; an isolated schema's name differs every run.
	schemaOnTableIndexRe = regexp.MustCompile(`\bon [a-z0-9_]+\.`)
	// allWhitespaceIndexRe collapses whitespace so formatting differences do not
	// read as drift.
	allWhitespaceIndexRe = regexp.MustCompile(`\s+`)
)

// normalizeIndexDef reduces an index definition to a comparable shape, so a
// definition read back from the catalog compares equal to the DDL that created
// it. Two hosts wrote this same reduction independently before it moved here; it
// is the reason InspectIndexes can compare meaning rather than formatting.
//
// pg_indexes.indexdef and the runtime's CREATE INDEX text are two renderings of
// the same statement, and most of what differs between them is the catalog's own
// formatting. Three rewrites are semantic rather than cosmetic, because the
// catalog genuinely spells the same thing differently:
//
//   - `IN ('a', 'b')` comes back as `= ANY (ARRAY['a', 'b'])`
//   - a literal compared against a text column gains a `::text` cast
//   - the table is qualified with the schema the index lives in
//
// Everything else — case, quoting, IF NOT EXISTS, USING btree, whitespace, and
// parenthesization — is noise the reduction strips. What survives is the name,
// table, key columns in order, and predicate, which is exactly what a drift would
// change. The one thing it cannot see is operator precedence expressed only
// through parentheses, since it strips them; no runtime index depends on that,
// and the must-not-match tests fix the boundary so the reduction is not tightened
// until it matches everything.
func normalizeIndexDef(def string) string {
	s := strings.ToLower(def)
	s = strings.ReplaceAll(s, `"`, "")
	s = strings.ReplaceAll(s, "if not exists ", "")
	s = strings.ReplaceAll(s, "::text", "")
	s = anyArrayIndexRe.ReplaceAllString(s, "in ($1)")
	s = strings.ReplaceAll(s, "public.", "")
	s = schemaOnTableIndexRe.ReplaceAllString(s, "on ")
	s = strings.ReplaceAll(s, "using btree ", "")
	s = allWhitespaceIndexRe.ReplaceAllString(s, "")
	s = strings.ReplaceAll(s, "(", "")
	s = strings.ReplaceAll(s, ")", "")
	return s
}

// retiredIndexNames lists indexes the runtime once installed and no longer wants.
// applyIndexes drops each — DROP INDEX IF EXISTS — whenever it is found installed,
// on every Migrate and InstallIndexes (not gated on the reconcile opt-in, unlike a
// drift rebuild: dropping a superseded index takes no lock worth deferring and
// loses no data, so there is nothing for a host to opt into). InspectIndexes
// surfaces any that survive as an informational straggler.
//
// It is an explicit, hardcoded list, and must never become "drop anything not in
// the desired set". That distinction is the whole safety property: a name absent
// from IndexSet but absent from here too is left strictly alone, so an index a host
// added for its own purposes on the same table is never touched.
//
// The list is currently empty: the runtime declares no superseded index. It stays
// as the seam a future retirement adds a name to — until then applyIndexes drops
// nothing here and InspectIndexes reports no straggler.
func retiredIndexNames() []string {
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
			// paused is one of the live states: a held job still owns its key, so a
			// second active enqueue of that key is still refused while it is paused.
			Name: "jobs_unique_active_key", Kind: IndexCorrectness, Table: "jobs",
			DDL: `CREATE UNIQUE INDEX IF NOT EXISTS jobs_unique_active_key ON jobs (unique_active_key) WHERE unique_active_key IS NOT NULL AND state IN ('available', 'running', 'retryable', 'scheduled', 'paused')`,
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
			// Performance: follow-up / DAG lookup, the batch rollup, and the fan-in
			// barrier's completion count.
			//
			// The key is (parent_job_id, state), a superset of the retired
			// (parent_job_id) index: a leading-column prefix serves every plain
			// parent lookup the old index did, and the trailing state column serves
			// the rollup's GROUP BY parent_job_id, state and the barrier's
			// COUNT(*) WHERE parent_job_id = ? AND state NOT IN (...) index-only,
			// without a heap visit per child. deleted_at IS NULL matches the
			// soft-delete scope, which is what makes it usable for those reads.
			Name: "jobs_parent_state", Kind: IndexPerformance, Table: "jobs",
			DDL: `CREATE INDEX IF NOT EXISTS jobs_parent_state ON jobs (parent_job_id, state) WHERE parent_job_id IS NOT NULL AND deleted_at IS NULL`,
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
		{
			// Performance: the DBLimiter's inline expiry reclaim. Acquire runs
			// DELETE ... WHERE resource = ? AND expires_at < ? before every held
			// count, and the composite key serves that predicate directly. It is a
			// performance index, not a correctness one: admission stays correct
			// without it (the reclaim still runs), only slower at depth.
			Name: "limiter_holds_resource", Kind: IndexPerformance, Table: "limiter_holds",
			DDL: `CREATE INDEX IF NOT EXISTS limiter_holds_resource ON limiter_holds (resource, expires_at)`,
		},
		{
			// Performance: the sweeper's global expiry scan, which is not scoped to a
			// resource and so wants expires_at leading.
			Name: "limiter_holds_expiry", Kind: IndexPerformance, Table: "limiter_holds",
			DDL: `CREATE INDEX IF NOT EXISTS limiter_holds_expiry ON limiter_holds (expires_at)`,
		},
	}
}
