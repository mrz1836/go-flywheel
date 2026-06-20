//go:build integration

package flywheel

import (
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// newBarePostgres mints a fresh, empty PostgreSQL schema with NO tables, so the
// test exercises Migrate from scratch (unlike NewPostgresIsolatedDB, which
// pre-migrates). The schema is dropped on cleanup. The suite is skipped when
// FLYWHEEL_TEST_DATABASE_URL is unset so the matrix degrades gracefully.
func newBarePostgres(t *testing.T) *gorm.DB {
	t.Helper()

	dsn := os.Getenv(testDatabaseURLEnv)
	if dsn == "" {
		t.Skipf("%s is not set; skipping the Postgres migrate suite", testDatabaseURLEnv)
	}

	base, err := gorm.Open(postgres.Open(dsn), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	if err != nil {
		t.Fatalf("connect to the Postgres test database: %v", err)
	}
	t.Cleanup(func() {
		if sqlDB, dbErr := base.DB(); dbErr == nil {
			_ = sqlDB.Close()
		}
	})

	schema := fmt.Sprintf("m_%d_%d", time.Now().UnixNano(), pgIsolatedSeq.Add(1))
	if err := base.Exec(`DROP SCHEMA IF EXISTS ` + schema + ` CASCADE`).Error; err != nil {
		t.Fatalf("drop schema %s: %v", schema, err)
	}
	if err := base.Exec(`CREATE SCHEMA ` + schema).Error; err != nil {
		t.Fatalf("create schema %s: %v", schema, err)
	}

	db, err := gorm.Open(postgres.Open(withSearchPath(dsn, schema)), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	if err != nil {
		t.Fatalf("open schema-scoped connection for %s: %v", schema, err)
	}
	t.Cleanup(func() {
		if sqlDB, dbErr := db.DB(); dbErr == nil {
			_ = sqlDB.Close()
		}
		_ = base.Exec(`DROP SCHEMA IF EXISTS ` + schema + ` CASCADE`).Error
	})
	return db
}

// pgHasIndex reports whether a named index exists in the current schema.
func pgHasIndex(t *testing.T, db *gorm.DB, name string) bool {
	t.Helper()
	var count int64
	if err := db.Raw(
		`SELECT count(*) FROM pg_indexes WHERE schemaname = current_schema() AND indexname = ?`, name,
	).Scan(&count).Error; err != nil {
		t.Fatalf("pgHasIndex(%s): %v", name, err)
	}
	return count > 0
}

// TestMigratePostgres proves Migrate stands up the full schema on a bare
// PostgreSQL schema (standalone mode) and is idempotent on a second call.
func TestMigratePostgres(t *testing.T) {
	db := newBarePostgres(t)

	if err := Migrate(db); err != nil {
		t.Fatalf("Migrate: %v", err)
	}

	for _, table := range migrateTables {
		if !db.Migrator().HasTable(table) {
			t.Errorf("expected table %q to exist after Migrate", table)
		}
	}
	for _, idx := range migrateIndexes {
		if !pgHasIndex(t, db, idx) {
			t.Errorf("expected index %q to exist after Migrate", idx)
		}
	}

	if err := Migrate(db); err != nil {
		t.Fatalf("Migrate (second call): %v", err)
	}
}

// TestMigratePostgresIdempotencyEnforced proves the jobs_unique_key partial
// unique index is enforced on Postgres: a duplicate non-null unique_key insert
// is rejected and classifies as ErrDuplicateKey.
func TestMigratePostgresIdempotencyEnforced(t *testing.T) {
	db := newBarePostgres(t)
	if err := Migrate(db); err != nil {
		t.Fatalf("Migrate: %v", err)
	}

	uk := "idempotency-key-1"
	first := newJobRowWithUniqueKey(NewID(), &uk)
	if err := db.Create(&first).Error; err != nil {
		t.Fatalf("first insert: %v", err)
	}

	dupKey := uk
	second := newJobRowWithUniqueKey(NewID(), &dupKey)
	err := db.Create(&second).Error
	if err == nil {
		t.Fatal("expected duplicate unique_key insert to be rejected, got nil error")
	}
	if wrapped := WrapDBError(err); !errors.Is(wrapped, ErrDuplicateKey) {
		t.Fatalf("expected ErrDuplicateKey, got %v", wrapped)
	}

	a := newJobRowWithUniqueKey(NewID(), nil)
	b := newJobRowWithUniqueKey(NewID(), nil)
	if err := db.Create(&a).Error; err != nil {
		t.Fatalf("null-key insert a: %v", err)
	}
	if err := db.Create(&b).Error; err != nil {
		t.Fatalf("null-key insert b: %v", err)
	}
}
