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
// same apply path [InstallIndexes] uses, and the storage parameters are
// StorageParameterSet(db.Name()) through the path [InstallStorageParameters]
// uses. On PostgreSQL those tune the jobs table for its update rate; on SQLite
// there are none to apply.
//
// Migrate is idempotent against an up-to-date schema: AutoMigrate is a no-op and
// every index already matches, so repeated calls do nothing. Against a database
// whose index definition has drifted from the runtime's it fails loudly by
// default — with an IndexDriftError naming the drift — rather than leaving the
// stale index in place; set MigrateOpts.Reconcile to rebuild it instead. See
// MigrateOpts.Reconcile for the lock that takes.
//
// Migrate is MigrateWithOptions(db, MigrateOpts{}).
func Migrate(db *gorm.DB) error {
	return MigrateWithOptions(db, MigrateOpts{})
}

// MigrateOpts configures MigrateWithOptions. The zero value is the library-owned
// install: the runtime brings up its own tables, indexes, and legacy column
// reconciliation against a database it is the only writer of.
type MigrateOpts struct {
	// SkipColumnReconcile disables the legacy routing-column rename pass. Set it
	// when the database's schema history is owned by an external migration tool:
	// the rename is imperative DDL, and running it inside a versioned schema means
	// two migration authorities on one database.
	//
	// The reconciliation belongs to the library-owned mode only. It is removed in
	// v0.17.0, at which point this field becomes a no-op — a host that sets it
	// today needs no further change then.
	SkipColumnReconcile bool

	// Reconcile drops and recreates any index whose installed definition has
	// drifted from the runtime's, rather than failing with an IndexDriftError. It
	// is off by default, uniform with InstallIndexes: the rebuild takes a table-wide
	// ACCESS EXCLUSIVE lock, and a Migrate on every start that could take one
	// uninvited is a stall under load. A drifted database therefore fails Migrate
	// loudly by default — recoverable — until a host either sets this or corrects
	// the index by hand. See IndexOpts.Reconcile for the lock this takes.
	//
	// An absent index is still created and a matching one still left alone; this
	// governs only what happens to a drifted one.
	Reconcile bool
}

// MigrateWithOptions installs the schema per opts: the legacy column
// reconciliation (unless opts skips it), AutoMigrate over Models,
// StorageParameterSet's statements, then IndexSet's statements in order.
//
// A host that owns its schema history but still wants the installer — one
// process, one call, no hand-copied DDL — sets SkipColumnReconcile so the
// runtime issues no ALTER TABLE of its own inside a versioned schema.
//
// A host whose migration tool already created the tables wants InstallIndexes
// and InstallStorageParameters instead: together they apply everything Migrate
// does except the tables themselves.
//
// The index step reconciles by definition, not by name: it creates an absent
// index, leaves a matching one alone, and — with opts.Reconcile unset — fails
// with an IndexDriftError on one whose definition has drifted rather than
// silently keeping the stale index. Set opts.Reconcile to rebuild it in place.
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

	// Storage parameters go on between the tables and the indexes. The order is
	// load-bearing rather than tidy: fillfactor governs only pages written after
	// it is set, so applying it after rows exist leaves every existing page at
	// the old target. On SQLite this is a no-op.
	if err := applyStorageParameters(context.Background(), db); err != nil {
		return fmt.Errorf("flywheel: Migrate: %w", err)
	}

	if err := applyIndexes(context.Background(), db, IndexOpts{Reconcile: opts.Reconcile}); err != nil {
		return fmt.Errorf("flywheel: Migrate: %w", err)
	}
	return nil
}

// reconcileColumnRenames renames the legacy routing columns to their
// executor_class names on an existing database, before AutoMigrate runs. The
// routing model moved from the closed lambda/ecs/either vocabulary to a
// free-form ExecutorClass, so jobs.run_on became jobs.executor_class and
// job_runs.executor_kind became job_runs.executor_class.
//
// On a fresh database neither the old nor the new column exists, so every branch
// is a guarded no-op and AutoMigrate creates the new columns directly. On an
// upgraded database the rename preserves the column's data and any index over
// it — both PostgreSQL and SQLite (>= 3.25) support ALTER TABLE ... RENAME
// COLUMN — where adding the new column and dropping the old one would discard
// every routing value in the table. It is idempotent: once renamed,
// HasColumn(old) is false and the branch is skipped.
//
// This pass belongs to the library-owned install mode. It is imperative DDL, so
// a host whose schema history is owned by a migration tool skips it with
// MigrateOpts.SkipColumnReconcile rather than running a second migration
// authority against its database. It is removed in v0.17.0.
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
