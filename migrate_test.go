package flywheel

import (
	"errors"
	"testing"
	"time"

	"github.com/mrz1836/go-foundation/models"
	"gorm.io/datatypes"
	"gorm.io/gorm"
)

// migrateTables and migrateIndexes are the schema objects Migrate must create on
// a bare database. They are the assertion oracle shared by the SQLite and (under
// the integration build tag) the PostgreSQL halves of the migrate test.
//
//nolint:gochecknoglobals // shared expectation fixtures for the migrate tests
var (
	migrateTables = []string{"jobs", "job_runs", "job_periodics"}
	// migrateJobColumns are jobs columns whose absence is silent rather than
	// loud: the runtime writes them through a map-valued Updates, which a missing
	// column turns into a query error at the first claim rather than a failure at
	// install time. lease_token is the fence, so a schema without it is one in
	// which finalize matches no row and every attempt reads as superseded.
	migrateJobColumns = []string{"lease_token"}
	migrateIndexes    = []string{
		"jobs_unique_key",
		"jobs_unique_active_key",
		"jobs_ready",
		"jobs_parent",
		"jobs_running_leased",
		"jobs_state",
		"idx_jobs_deleted_at",
		"job_runs_job_attempt",
		"idx_job_periodics_slug",
	}
)

// sqliteHasIndex reports whether a named index exists in a SQLite database.
func sqliteHasIndex(t *testing.T, db *gorm.DB, name string) bool {
	t.Helper()
	var count int64
	if err := db.Raw(
		`SELECT count(*) FROM sqlite_master WHERE type = 'index' AND name = ?`, name,
	).Scan(&count).Error; err != nil {
		t.Fatalf("sqliteHasIndex(%s): %v", name, err)
	}
	return count > 0
}

// TestMigrateSQLite proves Migrate stands up the full schema on a bare SQLite
// database (standalone mode) and is idempotent on a second call.
func TestMigrateSQLite(t *testing.T) {
	t.Parallel()
	db := newBareSQLite(t)

	if err := Migrate(db); err != nil {
		t.Fatalf("Migrate: %v", err)
	}

	for _, table := range migrateTables {
		if !db.Migrator().HasTable(table) {
			t.Errorf("expected table %q to exist after Migrate", table)
		}
	}
	for _, col := range migrateJobColumns {
		if !db.Migrator().HasColumn("jobs", col) {
			t.Errorf("expected column jobs.%s to exist after Migrate", col)
		}
	}
	for _, idx := range migrateIndexes {
		if !sqliteHasIndex(t, db, idx) {
			t.Errorf("expected index %q to exist after Migrate", idx)
		}
	}

	// Idempotent: a second Migrate must not error (AutoMigrate no-op + IF NOT
	// EXISTS indexes).
	if err := Migrate(db); err != nil {
		t.Fatalf("Migrate (second call): %v", err)
	}
}

// TestModelsCarryJobColumns proves the host-owned install mode delivers the
// same jobs columns the library-owned one does.
//
// The two modes are separate delivery paths for one schema (see Migrate's
// contract), and only one of them is exercised by the rest of this package. A
// host whose loader reads Models() gets whatever the row structs declare, so a
// column added to the runtime's SQL but not to jobRow would pass every test
// here and fail at the first claim against a host-owned database.
func TestModelsCarryJobColumns(t *testing.T) {
	t.Parallel()
	db := newBareSQLite(t)

	// AutoMigrate over Models is what a host's loader does, minus the index half.
	if err := db.AutoMigrate(Models()...); err != nil {
		t.Fatalf("AutoMigrate(Models()): %v", err)
	}
	for _, col := range migrateJobColumns {
		if !db.Migrator().HasColumn("jobs", col) {
			t.Errorf("expected column jobs.%s to reach a host-owned schema through Models()", col)
		}
	}
}

// TestMigrateSQLiteIdempotencyEnforced proves the correctness-bearing
// jobs_unique_key partial unique index is actually enforced after Migrate: a
// duplicate non-null unique_key insert is rejected and classifies as
// models.ErrDuplicateKey via the runtime's models.WrapDBError seam.
func TestMigrateSQLiteIdempotencyEnforced(t *testing.T) {
	t.Parallel()
	db := newBareSQLite(t)
	if err := Migrate(db); err != nil {
		t.Fatalf("Migrate: %v", err)
	}

	uk := "idempotency-key-1"
	first := newJobRowWithUniqueKey("job-1", &uk)
	if err := db.Create(&first).Error; err != nil {
		t.Fatalf("first insert: %v", err)
	}

	dupKey := uk
	second := newJobRowWithUniqueKey("job-2", &dupKey)
	err := db.Create(&second).Error
	if err == nil {
		t.Fatal("expected duplicate unique_key insert to be rejected, got nil error")
	}
	if wrapped := models.WrapDBError(err); !errors.Is(wrapped, models.ErrDuplicateKey) {
		t.Fatalf("expected models.ErrDuplicateKey, got %v", wrapped)
	}

	// The partial index excludes NULL unique_key: two NULL-key jobs must both
	// insert (no false idempotency collision).
	a := newJobRowWithUniqueKey("job-3", nil)
	b := newJobRowWithUniqueKey("job-4", nil)
	if err := db.Create(&a).Error; err != nil {
		t.Fatalf("null-key insert a: %v", err)
	}
	if err := db.Create(&b).Error; err != nil {
		t.Fatalf("null-key insert b: %v", err)
	}
}

// TestMigrateNilDB guards the nil-db precondition.
func TestMigrateNilDB(t *testing.T) {
	t.Parallel()
	if err := Migrate(nil); err == nil {
		t.Fatal("expected an error for a nil db")
	}
}

// TestIndexSetMatchesTheMigrateOracle ties the exported IndexSet back to
// migrateIndexes, the hand-written list this file asserts Migrate creates on a
// real database. migrateIndexes is deliberately an *independent* oracle — it is
// literal names, not derived from the package under test — so this assertion is
// what proves IndexSet describes the same schema Migrate installs. Deriving the
// oracle from IndexSet instead would make both halves agree by construction and
// prove nothing.
func TestIndexSetMatchesTheMigrateOracle(t *testing.T) {
	t.Parallel()
	for _, dialect := range []string{"postgres", "sqlite"} {
		set, err := IndexSet(dialect)
		if err != nil {
			t.Fatalf("IndexSet(%q): %v", dialect, err)
		}
		names := make([]string, len(set))
		for i, idx := range set {
			names[i] = idx.Name
		}
		if len(names) != len(migrateIndexes) {
			t.Fatalf("IndexSet(%q): got %d indexes, want %d", dialect, len(names), len(migrateIndexes))
		}
		for i := range names {
			if names[i] != migrateIndexes[i] {
				t.Errorf("IndexSet(%q)[%d] = %q, want %q", dialect, i, names[i], migrateIndexes[i])
			}
		}
	}
}

// TestMigrateRenamesLegacyRoutingColumns proves Migrate upgrades a legacy
// database — one that still carries the closed-vocabulary jobs.run_on and
// job_runs.executor_kind columns — by renaming them to executor_class in place
// and preserving the stored values, before AutoMigrate runs.
func TestMigrateRenamesLegacyRoutingColumns(t *testing.T) {
	t.Parallel()
	db := newBareSQLite(t)
	seedLegacyRoutingSchema(t, db)

	if err := Migrate(db); err != nil {
		t.Fatalf("Migrate: %v", err)
	}

	m := db.Migrator()
	if !m.HasColumn("jobs", "executor_class") {
		t.Error("jobs.run_on must be renamed to executor_class")
	}
	if m.HasColumn("jobs", "run_on") {
		t.Error("the legacy jobs.run_on column must be gone")
	}
	if !m.HasColumn("job_runs", "executor_class") {
		t.Error("job_runs.executor_kind must be renamed to executor_class")
	}
	if m.HasColumn("job_runs", "executor_kind") {
		t.Error("the legacy job_runs.executor_kind column must be gone")
	}

	// The stored routing values survive the in-place rename.
	var jobClass, runClass string
	if err := db.Raw(`SELECT executor_class FROM jobs WHERE id = 'j1'`).Scan(&jobClass).Error; err != nil {
		t.Fatalf("read jobs.executor_class: %v", err)
	}
	if err := db.Raw(`SELECT executor_class FROM job_runs WHERE id = 'r1'`).Scan(&runClass).Error; err != nil {
		t.Fatalf("read job_runs.executor_class: %v", err)
	}
	if jobClass != "lambda" {
		t.Errorf("jobs.executor_class = %q, want lambda", jobClass)
	}
	if runClass != "ecs" {
		t.Errorf("job_runs.executor_class = %q, want ecs", runClass)
	}

	// Idempotent: a second Migrate is a no-op now the legacy columns are gone.
	if err := Migrate(db); err != nil {
		t.Fatalf("Migrate (second call): %v", err)
	}
}

// seedLegacyRoutingSchema stands up a legacy schema on db: every column the
// runtime expects, with the same NOT NULL constraints the legacy schema had,
// but with the old closed-vocabulary routing column names (jobs.run_on,
// job_runs.executor_kind). The seed rows populate every NOT NULL column, exactly
// as a real pre-upgrade database would, so a rename's effect on stored values is
// observable.
//
// It is shared by the two halves of the reconciliation contract — the rename
// happening and the rename being skipped — so both run against a byte-identical
// starting schema and the only difference is the option under test.
func seedLegacyRoutingSchema(t *testing.T, db *gorm.DB) {
	t.Helper()

	if err := db.Exec(`CREATE TABLE jobs (
		id text PRIMARY KEY, created_at datetime NOT NULL, updated_at datetime NOT NULL,
		metadata jsonb NOT NULL DEFAULT '{}', kind text NOT NULL, queue text NOT NULL DEFAULT 'default',
		args jsonb NOT NULL, priority integer NOT NULL DEFAULT 100, state text NOT NULL DEFAULT 'available',
		attempt integer NOT NULL DEFAULT 0, max_attempts integer NOT NULL DEFAULT 25,
		scheduled_at datetime NOT NULL, leased_until datetime, unique_key text, parent_job_id text,
		run_on text NOT NULL DEFAULT 'either', finalized_at datetime, tags jsonb NOT NULL DEFAULT '[]',
		deleted_at datetime
	)`).Error; err != nil {
		t.Fatalf("create legacy jobs: %v", err)
	}
	if err := db.Exec(`CREATE TABLE job_runs (
		id text PRIMARY KEY, job_id text NOT NULL, attempt integer NOT NULL, executor_kind text NOT NULL,
		executor_id text NOT NULL, started_at datetime NOT NULL, finished_at datetime, outcome text NOT NULL,
		error_class text, error_message text, error_payload jsonb, output jsonb, duration_ms integer,
		cost_micros integer, enqueued_children integer NOT NULL DEFAULT 0, created_at datetime NOT NULL
	)`).Error; err != nil {
		t.Fatalf("create legacy job_runs: %v", err)
	}
	if err := db.Exec(
		`INSERT INTO jobs(id, created_at, updated_at, metadata, kind, queue, args, priority, state, attempt, max_attempts, scheduled_at, run_on, tags)
		 VALUES ('j1','2026-01-01','2026-01-01','{}','k','default','{}',100,'available',0,25,'2026-01-01','lambda','[]')`,
	).Error; err != nil {
		t.Fatalf("seed legacy job: %v", err)
	}
	if err := db.Exec(
		`INSERT INTO job_runs(id, job_id, attempt, executor_kind, executor_id, started_at, outcome, created_at)
		 VALUES ('r1','j1',1,'ecs','h1','2026-01-01','started','2026-01-01')`,
	).Error; err != nil {
		t.Fatalf("seed legacy run: %v", err)
	}
}

// TestMigrateWithOptionsSkipsColumnReconcile proves the opt-out is real (A3): a
// host that owns its schema history gets the installer without the runtime
// issuing an ALTER TABLE ... RENAME of its own. The legacy run_on column is
// still present with its value, and AutoMigrate adds executor_class alongside it
// rather than renaming into it — which is exactly the state a host's own
// migration is expected to reconcile.
func TestMigrateWithOptionsSkipsColumnReconcile(t *testing.T) {
	t.Parallel()
	db := newBareSQLite(t)
	seedLegacyRoutingSchema(t, db)

	if err := MigrateWithOptions(db, MigrateOpts{SkipColumnReconcile: true}); err != nil {
		t.Fatalf("MigrateWithOptions: %v", err)
	}

	m := db.Migrator()
	if !m.HasColumn("jobs", "run_on") {
		t.Error("jobs.run_on must survive: no rename may be issued when the reconciliation is skipped")
	}
	if !m.HasColumn("job_runs", "executor_kind") {
		t.Error("job_runs.executor_kind must survive: no rename may be issued")
	}
	if !m.HasColumn("jobs", "executor_class") {
		t.Error("AutoMigrate must still add jobs.executor_class alongside the legacy column")
	}
	if !m.HasColumn("job_runs", "executor_class") {
		t.Error("AutoMigrate must still add job_runs.executor_class alongside the legacy column")
	}

	// The rename was skipped, not deferred: the legacy column keeps its value and
	// the new column carries the struct default, so nothing was moved.
	var legacy, added string
	if err := db.Raw(`SELECT run_on FROM jobs WHERE id = 'j1'`).Scan(&legacy).Error; err != nil {
		t.Fatalf("read jobs.run_on: %v", err)
	}
	if legacy != "lambda" {
		t.Errorf("jobs.run_on = %q, want lambda — the untouched legacy value", legacy)
	}
	if err := db.Raw(`SELECT executor_class FROM jobs WHERE id = 'j1'`).Scan(&added).Error; err != nil {
		t.Fatalf("read jobs.executor_class: %v", err)
	}
	if added != "" {
		t.Errorf("jobs.executor_class = %q, want the empty default — no value may be carried over", added)
	}

	// The index half runs either way: skipping the reconciliation must not cost a
	// host the correctness-bearing indexes.
	for _, idx := range migrateIndexes {
		if !sqliteHasIndex(t, db, idx) {
			t.Errorf("expected index %q after MigrateWithOptions", idx)
		}
	}
}

// newJobRowWithUniqueKey builds a minimal valid jobs row for migrate tests.
func newJobRowWithUniqueKey(id string, uniqueKey *string) jobRow {
	now := time.Now()
	return jobRow{
		ID:          id,
		CreatedAt:   now,
		UpdatedAt:   now,
		Metadata:    datatypes.JSON("{}"),
		Kind:        "test.kind",
		Queue:       "default",
		Args:        datatypes.JSON("{}"),
		Priority:    100,
		State:       "available",
		MaxAttempts: 25,
		ScheduledAt: now,
		UniqueKey:   uniqueKey,
		Tags:        datatypes.JSON("[]"),
	}
}
