package flywheel

import (
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
// Migrate is idempotent: AutoMigrate is a no-op against an up-to-date schema and
// every reconciled index uses IF NOT EXISTS, so repeated calls are safe.
func Migrate(db *gorm.DB) error {
	if db == nil {
		return fmt.Errorf("flywheel: Migrate: db is nil")
	}

	if err := reconcileColumnRenames(db); err != nil {
		return err
	}

	if err := db.AutoMigrate(Models()...); err != nil {
		return fmt.Errorf("flywheel: Migrate: automigrate: %w", err)
	}

	stmts, err := reconcileIndexDDL(db.Name())
	if err != nil {
		return err
	}
	for _, ddl := range stmts {
		if err := db.Exec(ddl).Error; err != nil {
			return fmt.Errorf("flywheel: Migrate: reconcile index: %w", err)
		}
	}
	return nil
}

// reconcileIndexDDL returns the dialect-appropriate DDL for the indexes
// AutoMigrate cannot express from the row structs. The set is correctness- and
// hot-path-bearing:
//
//   - jobs_unique_key      UNIQUE (unique_key) WHERE unique_key IS NOT NULL
//     — correctness: enforces enqueue idempotency (a duplicate unique_key insert
//     is rejected by the database, surfacing as ErrDuplicateKey).
//   - jobs_ready           (queue, executor_class, priority, scheduled_at) WHERE
//     state IN ('available','retryable','scheduled') — the claim hot path.
//   - jobs_parent          (parent_job_id) WHERE parent_job_id IS NOT NULL
//     — follow-up/DAG lookup.
//   - jobs_running_leased  (leased_until) WHERE state = 'running'
//     — stuck-lease / orphan-recovery sweep.
//   - idx_jobs_deleted_at   (deleted_at) WHERE deleted_at IS NOT NULL
//     — soft-delete restore/audit lookups; a partial index a struct tag cannot
//     express, so it lives here rather than on jobRow.DeletedAt.
//   - job_runs_job_attempt UNIQUE (job_id, attempt) — one audit row per attempt.
//   - idx_job_periodics_slug UNIQUE (slug) — one schedule per slug.
//
// PostgreSQL and SQLite both support partial indexes (CREATE INDEX ... WHERE)
// and IF NOT EXISTS, so the DDL is portable between them. The switch keeps the
// dialect seam explicit and rejects dialects that cannot express partial
// indexes (e.g. MySQL) rather than silently dropping idempotency.
func reconcileIndexDDL(dialect string) ([]string, error) {
	ddl := []string{
		`CREATE UNIQUE INDEX IF NOT EXISTS jobs_unique_key ON jobs (unique_key) WHERE unique_key IS NOT NULL`,
		`CREATE INDEX IF NOT EXISTS jobs_ready ON jobs (queue, executor_class, priority, scheduled_at) WHERE state IN ('available', 'retryable', 'scheduled')`,
		`CREATE INDEX IF NOT EXISTS jobs_parent ON jobs (parent_job_id) WHERE parent_job_id IS NOT NULL`,
		`CREATE INDEX IF NOT EXISTS jobs_running_leased ON jobs (leased_until) WHERE state = 'running'`,
		`CREATE INDEX IF NOT EXISTS idx_jobs_deleted_at ON jobs (deleted_at) WHERE deleted_at IS NOT NULL`,
		`CREATE UNIQUE INDEX IF NOT EXISTS job_runs_job_attempt ON job_runs (job_id, attempt)`,
		`CREATE UNIQUE INDEX IF NOT EXISTS idx_job_periodics_slug ON job_periodics (slug)`,
	}

	switch dialect {
	case "postgres", "sqlite":
		return ddl, nil
	default:
		return nil, fmt.Errorf(
			"flywheel: Migrate: unsupported dialect %q: partial indexes require postgres or sqlite",
			dialect,
		)
	}
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
