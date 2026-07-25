package flywheel

import (
	"context"
	"fmt"

	"gorm.io/gorm"
)

// Models returns the runtime's row structs so a consumer can drive schema
// generation from a single source of truth — the same structs Migrate uses.
//
// This is the seam for the "embedded" install mode: a host that prefers
// versioned SQL (e.g. an Atlas/atlas-provider-gorm flow) points its loader at
// these models instead of re-declaring the columns. The runtime keeps the row
// structs unexported on purpose; Models exposes them as a stable []any without
// widening the package's typed API surface.
func Models() []any {
	return []any{&jobRow{}, &jobRunRow{}, &jobPeriodicRow{}}
}

// Migrate is the single source of truth for the job schema: it brings up the
// three job tables (jobs, job_runs, job_periodics) — with their NOT-NULL
// constraints, column defaults, and the jobs soft-delete column — plus the
// partial/unique indexes GORM AutoMigrate cannot express. A host installs the
// schema by calling Migrate(db) and nothing else. It supports both consumption
// modes:
//
//   - standalone: call it against a bare SQLite or PostgreSQL database and the
//     runtime stands up its own schema with no external migration tooling.
//   - embedded: call it as one step of a host project's install/migration
//     process. The module takes no hard Atlas dependency; a host that wants
//     versioned SQL can generate it from Models instead.
//
// The indexes it applies are IndexSet(db.Name()), in that order — a host in the
// embedded mode reaches the same set through InstallIndexes.
//
// Migrate is idempotent: AutoMigrate is a no-op against an up-to-date schema and
// every index uses IF NOT EXISTS, so repeated calls are safe.
func Migrate(db *gorm.DB) error {
	if db == nil {
		return fmt.Errorf("flywheel: Migrate: db is nil")
	}

	if err := reconcileColumnRenames(db); err != nil {
		return fmt.Errorf("flywheel: Migrate: reconcile column renames: %w", err)
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
