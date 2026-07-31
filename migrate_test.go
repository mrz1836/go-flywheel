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
	migrateTables = []string{"jobs", "job_runs", "job_periodics", "limiter_buckets", "limiter_holds"}
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
		"jobs_parent_state",
		"jobs_running_leased",
		"jobs_state",
		"idx_jobs_deleted_at",
		"job_runs_job_attempt",
		"idx_job_periodics_slug",
		"limiter_holds_resource",
		"limiter_holds_expiry",
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
