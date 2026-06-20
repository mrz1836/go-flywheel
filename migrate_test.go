package flywheel

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"gorm.io/datatypes"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// migrateTables and migrateIndexes are the schema objects Migrate must create on
// a bare database. They are the assertion oracle shared by the SQLite and (under
// the integration build tag) the PostgreSQL halves of the migrate test.
//
//nolint:gochecknoglobals // shared expectation fixtures for the migrate tests
var (
	migrateTables  = []string{"jobs", "job_runs", "job_periodics"}
	migrateIndexes = []string{
		"jobs_unique_key",
		"jobs_ready",
		"jobs_parent",
		"jobs_running_leased",
		"idx_jobs_deleted_at",
		"job_runs_job_attempt",
		"idx_job_periodics_slug",
	}
)

// newBareSQLite opens a fresh in-memory SQLite database with NO schema applied,
// so the test exercises Migrate from scratch rather than the pre-migrated
// helpers_test.go fixture.
func newBareSQLite(t *testing.T) *gorm.DB {
	t.Helper()
	dsn := fmt.Sprintf("file:flywheel-migrate-%d?mode=memory&cache=shared", dbSeq.Add(1))
	db, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	if err != nil {
		t.Fatalf("newBareSQLite: open: %v", err)
	}
	t.Cleanup(func() {
		if sqlDB, derr := db.DB(); derr == nil {
			_ = sqlDB.Close()
		}
	})
	return db
}

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
	db := newBareSQLite(t)

	if err := Migrate(db); err != nil {
		t.Fatalf("Migrate: %v", err)
	}

	for _, table := range migrateTables {
		if !db.Migrator().HasTable(table) {
			t.Errorf("expected table %q to exist after Migrate", table)
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

// TestMigrateSQLiteIdempotencyEnforced proves the correctness-bearing
// jobs_unique_key partial unique index is actually enforced after Migrate: a
// duplicate non-null unique_key insert is rejected and classifies as
// ErrDuplicateKey via the runtime's WrapDBError seam.
func TestMigrateSQLiteIdempotencyEnforced(t *testing.T) {
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
	if wrapped := WrapDBError(err); !errors.Is(wrapped, ErrDuplicateKey) {
		t.Fatalf("expected ErrDuplicateKey, got %v", wrapped)
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
	if err := Migrate(nil); err == nil {
		t.Fatal("expected an error for a nil db")
	}
}

// TestReconcileIndexDDLUnsupportedDialect proves a dialect that cannot express
// partial indexes is rejected rather than silently dropping idempotency.
func TestReconcileIndexDDLUnsupportedDialect(t *testing.T) {
	if _, err := reconcileIndexDDL("mysql"); err == nil {
		t.Fatal("expected mysql to be rejected as an unsupported dialect")
	}
	for _, d := range []string{"postgres", "sqlite"} {
		stmts, err := reconcileIndexDDL(d)
		if err != nil {
			t.Fatalf("reconcileIndexDDL(%q): %v", d, err)
		}
		if len(stmts) != len(migrateIndexes) {
			t.Errorf("reconcileIndexDDL(%q): got %d statements, want %d", d, len(stmts), len(migrateIndexes))
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
		RunOn:       "either",
		Tags:        datatypes.JSON("[]"),
	}
}
