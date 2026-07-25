//go:build integration

package flywheel

import (
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// testDatabaseURLEnv points NewPostgresIsolatedDB at a running PostgreSQL
// instance. When it is unset the Postgres suite is skipped rather than failed,
// so `go test -tags integration` degrades gracefully where no database is
// available.
const testDatabaseURLEnv = "FLYWHEEL_TEST_DATABASE_URL"

// requirePostgresEnv turns that skip into a failure. An automated environment
// sets it so that a misconfigured or missing DSN surfaces as a red build,
// instead of a green run in which every Postgres test quietly skipped. The
// SKIP LOCKED claim path has no SQLite equivalent, so a suite that cannot
// distinguish "passed" from "never ran" reports a guarantee it did not check.
const requirePostgresEnv = "FLYWHEEL_REQUIRE_POSTGRES"

// requirePostgresDSN resolves the target database for the Postgres suite. It
// returns the DSN, or skips the test when none is configured — unless
// requirePostgresEnv is set, in which case a missing DSN is fatal.
//
// Every Postgres entry point routes through this one function on purpose: the
// strictness guarantee is only worth as much as its least-guarded caller, and a
// second helper that read the environment directly would silently reopen the
// hole this closes.
func requirePostgresDSN(t *testing.T) string {
	t.Helper()

	if dsn := os.Getenv(testDatabaseURLEnv); dsn != "" {
		return dsn
	}
	if os.Getenv(requirePostgresEnv) != "" {
		t.Fatalf(
			"%s is set but %s is empty: the Postgres suite must not be skipped in this environment",
			requirePostgresEnv, testDatabaseURLEnv,
		)
	}
	t.Skipf("%s is not set; skipping the Postgres suite", testDatabaseURLEnv)
	return ""
}

// pgIsolatedSeq disambiguates schema names across parallel calls within the
// same test binary.
//
//nolint:gochecknoglobals // per-test-binary sequence counter for schema uniqueness
var pgIsolatedSeq atomic.Uint64

// pgPartialIndexes is the minimum index set the runtime depends on for
// correctness: the unique_key partial index enforces enqueue idempotency. The
// remaining production partial indexes are performance-only and intentionally
// omitted from the test schema, mirroring the SQLite test setup.
//
//nolint:gochecknoglobals // shared DDL fixtures applied across tests
var pgPartialIndexes = []string{
	`CREATE UNIQUE INDEX IF NOT EXISTS jobs_unique_key ON jobs (unique_key) WHERE unique_key IS NOT NULL`,
	`CREATE UNIQUE INDEX IF NOT EXISTS jobs_unique_active_key ON jobs (unique_active_key) WHERE unique_active_key IS NOT NULL AND state IN ('available', 'running', 'retryable', 'scheduled')`,
}

// NewPostgresIsolatedDB returns a *gorm.DB bound to a freshly created Postgres
// schema with the three runtime row types migrated into it. Each call mints a
// unique schema and its own connection pool, so callers can use t.Parallel
// safely even though the underlying database is shared with sibling tests. The
// schema is dropped on test cleanup.
//
// It is defined in an internal (package flywheel) test file so it can migrate
// the unexported row structs; being exported lets the external
// flywheel_test integration suite call it.
func NewPostgresIsolatedDB(t *testing.T) *gorm.DB {
	t.Helper()

	dsn := requirePostgresDSN(t)

	base, err := gorm.Open(postgres.Open(dsn), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		t.Fatalf("connect to the Postgres test database: %v", err)
	}
	t.Cleanup(func() {
		if sqlDB, dbErr := base.DB(); dbErr == nil {
			_ = sqlDB.Close()
		}
	})

	schema := fmt.Sprintf("t_%d_%d", time.Now().UnixNano(), pgIsolatedSeq.Add(1))
	if err := base.Exec(`DROP SCHEMA IF EXISTS ` + schema + ` CASCADE`).Error; err != nil {
		t.Fatalf("drop schema %s: %v", schema, err)
	}
	if err := base.Exec(`CREATE SCHEMA ` + schema).Error; err != nil {
		t.Fatalf("create schema %s: %v", schema, err)
	}

	db, err := gorm.Open(postgres.Open(withSearchPath(dsn, schema)), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		t.Fatalf("open schema-scoped connection for %s: %v", schema, err)
	}
	t.Cleanup(func() {
		if sqlDB, dbErr := db.DB(); dbErr == nil {
			_ = sqlDB.Close()
		}
		_ = base.Exec(`DROP SCHEMA IF EXISTS ` + schema + ` CASCADE`).Error
	})

	if err := db.AutoMigrate(&jobRow{}, &jobRunRow{}, &jobPeriodicRow{}); err != nil {
		t.Fatalf("migrate schema %s: %v", schema, err)
	}
	for _, ddl := range pgPartialIndexes {
		if err := db.Exec(ddl).Error; err != nil {
			t.Fatalf("apply partial index on %s: %v", schema, err)
		}
	}
	return db
}

// withSearchPath appends a search_path runtime parameter to a Postgres URL DSN
// so every connection on the resulting pool resolves unqualified names to the
// given schema first.
func withSearchPath(dsn, schema string) string {
	sep := "?"
	if strings.Contains(dsn, "?") {
		sep = "&"
	}
	return dsn + sep + "search_path=" + schema
}
