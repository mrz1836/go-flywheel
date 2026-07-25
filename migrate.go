package flywheel

import (
	"context"
	"fmt"

	"gorm.io/gorm"
)

// Models returns the runtime's row structs so a consumer can drive schema
// generation from a single source of truth — the same structs Migrate uses.
//
// This is the table half of the host-owned install mode: a host that prefers
// versioned SQL (e.g. an Atlas/atlas-provider-gorm flow) points its loader at
// these models instead of re-declaring the columns. The runtime keeps the row
// structs unexported on purpose; Models exposes them as a stable []any without
// widening the package's typed API surface.
//
// It is not the whole install. Pair it with [InstallIndexes] for the index half:
// the indexes are not expressible as struct tags, so they are not in what a
// loader reads from these models, and four of them are correctness-bearing. See
// [Migrate] for the contract that decides between the two modes.
func Models() []any {
	return []any{&jobRow{}, &jobRunRow{}, &jobPeriodicRow{}}
}

// Migrate is the library-owned install: it brings up the three job tables (jobs,
// job_runs, job_periodics) — with their NOT-NULL constraints, column defaults,
// and the jobs soft-delete column — plus the partial/unique indexes GORM
// AutoMigrate cannot express. A host in this mode calls Migrate(db) and nothing
// else.
//
// # Choosing an install mode
//
// The runtime owns three tables and there are two ways to install them. They are
// not layers. A host picks exactly one; running both means two migration
// authorities against one database.
//
//	Question                      Library-owned            Host-owned
//	----------------------------  -----------------------  -------------------------------
//	Who creates the tables?       Migrate(db)              your loader, from Models
//	Who creates the indexes?      Migrate(db)              you, from IndexSet/InstallIndexes
//	Who owns migration history?   nobody (AutoMigrate      your migration tool
//	                              is declarative)
//	Runtime runs DDL at startup?  yes, every start         no
//	Co-located host schema safe?  only if the host's       yes — the tables are in
//	                              tooling excludes the     your loader
//	                              three tables
//	Pick this when                the database is the      the runtime's tables share a
//	                              runtime's alone          database with an app schema
//
// The last row is the rule: a shared database means host-owned. A migration tool
// that cannot see the three tables will propose dropping them, and a runtime
// that runs its own DDL inside a versioned schema is a second migration
// authority with no coordination.
//
// The half a host most often misses is the indexes. Models yields tables and
// columns; it does not yield indexes, because every one of them carries a WHERE
// predicate or spans columns a GORM struct tag cannot express. Four are
// correctness-bearing (see [IndexSet]), so a host-owned schema installed without
// [InstallIndexes] is one in which idempotent enqueue silently does not work.
//
// # Using Migrate
//
// Call it against a bare SQLite or PostgreSQL database the runtime is the only
// writer of. A host that owns its schema history but still wants the installer
// sets [MigrateOpts].SkipColumnReconcile, so the runtime issues no ALTER TABLE
// of its own.
//
// The indexes it applies are IndexSet(db.Name()), in that order, through the
// same apply path [InstallIndexes] uses.
//
// Migrate is idempotent: AutoMigrate is a no-op against an up-to-date schema and
// every index uses IF NOT EXISTS, so repeated calls are safe.
//
// Migrate is MigrateWithOptions(db, MigrateOpts{}).
func Migrate(db *gorm.DB) error {
	return MigrateWithOptions(db, MigrateOpts{})
}

// MigrateOpts configures MigrateWithOptions. The zero value is the library-owned
// install: the runtime brings up its own tables, indexes, and pre-1.0 column
// reconciliation against a database it is the only writer of.
type MigrateOpts struct {
	// SkipColumnReconcile disables the pre-1.0 routing-column rename pass. Set it
	// when the database's schema history is owned by an external migration tool:
	// the rename is imperative DDL, and running it inside a versioned schema means
	// two migration authorities on one database.
	//
	// The reconciliation belongs to the library-owned mode only. It is removed in
	// v1.0.0, at which point this field becomes a no-op — a host that sets it
	// today needs no further change then.
	SkipColumnReconcile bool
}

// MigrateWithOptions installs the schema per opts: the pre-1.0 column
// reconciliation (unless opts skips it), AutoMigrate over Models, then
// IndexSet's statements in order.
//
// A host that owns its schema history but still wants the installer — one
// process, one call, no hand-copied DDL — sets SkipColumnReconcile so the
// runtime issues no ALTER TABLE of its own inside a versioned schema. A host
// whose migration tool already created the tables wants InstallIndexes instead:
// it applies the index half and nothing else.
func MigrateWithOptions(db *gorm.DB, opts MigrateOpts) error {
	if db == nil {
		return fmt.Errorf("flywheel: Migrate: db is nil")
	}

	if !opts.SkipColumnReconcile {
		if err := reconcileColumnRenames(db); err != nil {
			return fmt.Errorf("flywheel: Migrate: reconcile column renames: %w", err)
		}
	}

	if err := db.AutoMigrate(Models()...); err != nil {
		return fmt.Errorf("flywheel: Migrate: automigrate: %w", err)
	}

	if err := applyIndexes(context.Background(), db); err != nil {
		return fmt.Errorf("flywheel: Migrate: %w", err)
	}
	return nil
}

// reconcileColumnRenames renames the pre-1.0 routing columns to their
// executor_class names on an existing database, before AutoMigrate runs. The
// routing model moved from the closed lambda/ecs/either vocabulary to a
// free-form ExecutorClass, so jobs.run_on became jobs.executor_class and
// job_runs.executor_kind became job_runs.executor_class.
//
// On a fresh database neither the old nor the new column exists, so every branch
// is a guarded no-op and AutoMigrate creates the new columns directly. On an
// upgraded database the rename carries the column's indexes with it — both
// PostgreSQL and SQLite (>= 3.25) support ALTER TABLE ... RENAME COLUMN — so the
// jobs_ready partial index keeps covering the routing column without a reindex.
// It is idempotent: once renamed, HasColumn(old) is false and the branch is
// skipped.
//
// This pass belongs to the library-owned install mode. It is imperative DDL, so
// a host whose schema history is owned by a migration tool skips it with
// MigrateOpts.SkipColumnReconcile rather than running a second migration
// authority against its database. It is removed in v1.0.0.
func reconcileColumnRenames(db *gorm.DB) error {
	renames := []struct{ table, oldCol, newCol string }{
		{"jobs", "run_on", "executor_class"},
		{"job_runs", "executor_kind", "executor_class"},
	}
	m := db.Migrator()
	for _, r := range renames {
		if !m.HasTable(r.table) {
			continue
		}
		if m.HasColumn(r.table, r.oldCol) && !m.HasColumn(r.table, r.newCol) {
			stmt := fmt.Sprintf("ALTER TABLE %s RENAME COLUMN %s TO %s", r.table, r.oldCol, r.newCol)
			if err := db.Exec(stmt).Error; err != nil {
				return fmt.Errorf("flywheel: Migrate: rename %s.%s to %s: %w", r.table, r.oldCol, r.newCol, err)
			}
		}
	}
	return nil
}
