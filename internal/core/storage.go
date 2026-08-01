package core

import (
	"context"
	"fmt"
	"strings"

	"gorm.io/gorm"
)

// StorageParameter is one per-table storage setting the runtime relies on, as
// DDL plus the reason it exists.
//
// It is the storage counterpart of [Index], and it is a separate concept for a
// concrete reason: an index is portable between the runtime's two dialects,
// while a storage parameter is a PostgreSQL-only physical-layout knob that
// SQLite has no equivalent for.
type StorageParameter struct {
	// Table is the table the parameter is set on.
	Table string
	// DDL is the ALTER TABLE statement. Setting a storage parameter is
	// idempotent: applying it twice leaves the same reloptions.
	DDL string
}

// StorageParameterSet returns the per-table storage parameters the runtime sets
// for the given GORM dialect name (db.Name()), in application order.
//
// SQLite returns an empty set rather than an error. It has no per-table
// equivalent of these settings and needs none — it is a single-writer embedded
// database with no autovacuum daemon and no per-page fill target — so "nothing
// to apply" is the correct, documented behavior rather than a silent gap. An
// unsupported dialect returns ErrUnsupportedDialect, the same error IndexSet
// returns for it.
//
// # Why the runtime sets these at all
//
// jobs is an unusually update-heavy table for its size. Every job transitions
// available → running → terminal, plus one extra pair per retry, and none of
// those updates can be HOT: a HOT update requires that no index-relevant column
// change, and `state` appears in the predicate of jobs_ready,
// jobs_unique_active_key, jobs_running_leased, and jobs_state. So every
// transition writes a new tuple version and maintains indexes, and PostgreSQL's
// cluster-wide autovacuum defaults are not tuned for that rate.
//
// Measured at a 1M working set over 33 minutes, with and without (see
// docs/BENCHMARKS.md): dead tuples end at 4.72M against 793k, table size grows
// +19.3 MB/min against shrinking 4.8 MB/min over the final third, autovacuum
// runs 5 cycles against 29, and drain throughput is 2,596 against 3,947 jobs/s.
//
// The cost, which is real and belongs next to the benefit: WAL rises from about
// 7.0 KB to 10.8 KB per job. Vacuuming more often writes more WAL, and a
// fillfactor below 100 puts fewer tuples on a page. A deployment paying for
// replication bandwidth by the byte should know that before it adopts this.
func StorageParameterSet(dialect string) ([]StorageParameter, error) {
	switch dialect {
	case "sqlite":
		return nil, nil
	case "postgres":
	default:
		return nil, fmt.Errorf(
			"flywheel: %w: %q: storage parameters require postgres or sqlite",
			ErrUnsupportedDialect, dialect,
		)
	}
	return runtimeStorageParameters(), nil
}

// StorageParameters returns StorageParameterSet's DDL statements in application
// order. It is the one-line form for a host that wants the statements and no
// structure. SQLite yields an empty slice.
func StorageParameters(dialect string) ([]string, error) {
	set, err := StorageParameterSet(dialect)
	if err != nil {
		return nil, err
	}
	ddl := make([]string, len(set))
	for i, p := range set {
		ddl[i] = p.DDL
	}
	return ddl, nil
}

// InstallStorageParameters applies every storage parameter for db's dialect.
//
// It is the host-owned install step, the counterpart of InstallIndexes: a host
// whose migration tool created the tables calls this once per install or deploy
// to reach storage parity with Migrate, which applies the same set through the
// same function. On SQLite it is a no-op.
//
// # Apply it before the table is written, not after
//
// fillfactor governs only pages written after it is set. Applying it to a table
// that already holds rows leaves every existing page at the old target, so the
// benefit arrives gradually as pages are rewritten rather than immediately.
// That is why Migrate applies these between creating the tables and creating
// the indexes.
//
// Setting a storage parameter is idempotent, so this is safe on every deploy.
func InstallStorageParameters(ctx context.Context, db *gorm.DB) error {
	if db == nil {
		return fmt.Errorf("flywheel: InstallStorageParameters: db is nil")
	}
	if err := applyStorageParameters(ctx, db); err != nil {
		return fmt.Errorf("flywheel: InstallStorageParameters: %w", err)
	}
	return nil
}

// StorageParameterDrift is one per-table storage parameter whose installed value
// differs from the one the runtime sets.
type StorageParameterDrift struct {
	// Table is the table the parameter is set on.
	Table string
	// Parameter is the reloption name, e.g. fillfactor.
	Parameter string
	// Installed is the value the catalog reports, or the empty string when the
	// parameter is unset on the table.
	Installed string
	// Expected is the value the runtime sets.
	Expected string
}

// InspectStorageParameters reports every runtime storage parameter whose
// installed value on db differs from the one the runtime sets. It reads pg_class
// and writes nothing.
//
// Unlike an index, a storage parameter does not silently drift through a
// re-install. InstallStorageParameters emits ALTER TABLE ... SET (...), which
// converges the reloptions to the declared value unconditionally, so a re-install
// — or the next Migrate — heals any difference on its own. There is no fail-loud
// default here for that reason: forcing one would regress the current convergent
// behavior for a defect that does not exist.
//
// What a host cannot get for free is the parity check itself. A host whose
// migration tool owns the schema hand-writes that same ALTER in a migration, and
// this is how it asserts in CI that its migration still sets exactly what the
// runtime does — the counterpart to InspectIndexes, on the parameter that has no
// name-only install to hide a drift behind.
//
// On SQLite it returns no drift: there are no storage parameters to compare. An
// unsupported dialect returns ErrUnsupportedDialect.
func InspectStorageParameters(ctx context.Context, db *gorm.DB) ([]StorageParameterDrift, error) {
	if db == nil {
		return nil, fmt.Errorf("flywheel: InspectStorageParameters: db is nil")
	}
	set, err := StorageParameterSet(db.Name())
	if err != nil {
		return nil, fmt.Errorf("flywheel: InspectStorageParameters: %w", err)
	}
	if len(set) == 0 {
		// SQLite: nothing to compare, and that is not drift.
		return nil, nil
	}

	installed := map[string]map[string]string{}
	var drift []StorageParameterDrift
	for _, exp := range storageParameterExpectations(set) {
		opts, ok := installed[exp.table]
		if !ok {
			opts, err = readReloptions(ctx, db, exp.table)
			if err != nil {
				return nil, fmt.Errorf("flywheel: InspectStorageParameters: %w", err)
			}
			installed[exp.table] = opts
		}
		got := opts[exp.param]
		if normalizeStorageValue(got) != normalizeStorageValue(exp.expected) {
			drift = append(drift, StorageParameterDrift{
				Table:     exp.table,
				Parameter: exp.param,
				Installed: got,
				Expected:  exp.expected,
			})
		}
	}
	return drift, nil
}

// storageParamExpectation is one flattened (table, parameter, value) the runtime
// sets, so each setting can be compared against the catalog independently of how
// they were grouped into ALTER statements.
type storageParamExpectation struct {
	table    string
	param    string
	expected string
}

// storageParameterExpectations flattens the runtime's storage DDL into one
// expectation per setting. Each DDL is ALTER TABLE <t> SET (a = 1, b = 2), so it
// reads the parameters out of the parenthesized list rather than restating them,
// for the same reason the tests do: a second copy would drift from the DDL.
func storageParameterExpectations(set []StorageParameter) []storageParamExpectation {
	var out []storageParamExpectation
	for _, p := range set {
		open := strings.Index(p.DDL, "(")
		end := strings.LastIndex(p.DDL, ")")
		if open < 0 || end <= open {
			continue
		}
		for _, setting := range strings.Split(p.DDL[open+1:end], ",") {
			name, value, ok := strings.Cut(setting, "=")
			if !ok {
				continue
			}
			out = append(out, storageParamExpectation{
				table:    p.Table,
				param:    strings.ToLower(strings.TrimSpace(name)),
				expected: strings.TrimSpace(value),
			})
		}
	}
	return out
}

// readReloptions reads a table's storage parameters from pg_class into a
// name→value map, lower-casing the names. An unset table yields an empty map, so
// every declared parameter reads as absent rather than as an error.
func readReloptions(ctx context.Context, db *gorm.DB, table string) (map[string]string, error) {
	var joined string
	if err := db.WithContext(ctx).Raw(`
		SELECT coalesce(array_to_string(c.reloptions, ','), '')
		FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE n.nspname = current_schema() AND c.relname = ?`, table).Scan(&joined).Error; err != nil {
		return nil, fmt.Errorf("read storage parameters on %s: %w", table, err)
	}
	out := map[string]string{}
	if joined == "" {
		return out, nil
	}
	for _, kv := range strings.Split(joined, ",") {
		name, value, ok := strings.Cut(kv, "=")
		if !ok {
			continue
		}
		out[strings.ToLower(strings.TrimSpace(name))] = strings.TrimSpace(value)
	}
	return out, nil
}

// normalizeStorageValue reduces a reloption value to a comparable shape, so
// `0.02` compares equal whether the runtime wrote it with a space after the `=`
// or the catalog rendered it without one.
func normalizeStorageValue(v string) string {
	return strings.ToLower(strings.ReplaceAll(strings.TrimSpace(v), " ", ""))
}

// applyStorageParameters is the one apply path, shared by
// InstallStorageParameters and Migrate so both set the same parameters in the
// same order. It leaves the caller's name off the error so each entry point can
// prefix its own.
func applyStorageParameters(ctx context.Context, db *gorm.DB) error {
	set, err := StorageParameterSet(db.Name())
	if err != nil {
		return err
	}
	for _, p := range set {
		if err := db.WithContext(ctx).Exec(p.DDL).Error; err != nil {
			return fmt.Errorf("set storage parameters on %s: %w", p.Table, err)
		}
	}
	return nil
}

// runtimeStorageParameters is the PostgreSQL storage-parameter set.
//
// job_runs and job_periodics carry none, and that is a decision rather than an
// omission: job_runs is append-only (see jobRunRow) and job_periodics holds one
// row per schedule. Neither has the update churn either setting acts on, and a
// lower fillfactor on an append-only table is pure waste — it reserves free
// space on every page for updates that never come.
func runtimeStorageParameters() []StorageParameter {
	return []StorageParameter{
		{
			// A lower autovacuum threshold, which is the load-bearing half.
			//
			// The default scale factor of 0.2 means a vacuum triggers only after
			// dead tuples reach 20% of the table: at 1M rows that is 200,050 dead
			// tuples, and jobs churns every row at least twice. 0.02 makes it
			// 20,050, which is what turns 5 autovacuum cycles into 29 over the same
			// window — and a table that grows monotonically into one that holds
			// steady.
			//
			// The raised cost limit is what lets a triggered vacuum actually finish
			// promptly rather than sleeping through its budget: firing more often
			// helps nothing if each pass is throttled to the default 200.
			Table: "jobs",
			DDL: `ALTER TABLE jobs SET (autovacuum_vacuum_scale_factor = 0.02, ` +
				`autovacuum_vacuum_cost_limit = 1000)`,
		},
		{
			// Free space on each page for an updated tuple version to land beside
			// its predecessor, reducing page splits and index churn.
			//
			// It does *not* enable HOT updates here, and the common claim that
			// fillfactor "enables HOT" is false for this table: a HOT update
			// requires that no index-relevant column change, and `state` is in the
			// predicate of four of the runtime's indexes. Every state transition is
			// therefore non-HOT whatever the fillfactor is.
			Table: "jobs",
			DDL:   `ALTER TABLE jobs SET (fillfactor = 80)`,
		},
	}
}
