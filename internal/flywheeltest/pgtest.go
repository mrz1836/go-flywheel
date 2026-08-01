//go:build integration

package flywheeltest

import (
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	core "github.com/mrz1836/go-flywheel/internal/core"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// testDatabaseURLEnv points NewPostgresIsolatedDB at a running PostgreSQL
// instance. When it is unset the Postgres suite is skipped rather than failed.
const testDatabaseURLEnv = "FLYWHEEL_TEST_DATABASE_URL"

// requirePostgresEnv turns that skip into a failure, so an automated environment
// cannot report a green run in which every Postgres test quietly skipped.
const requirePostgresEnv = "FLYWHEEL_REQUIRE_POSTGRES"

// PGSchemaSeq disambiguates schema names across parallel calls within one test
// binary.
//
//nolint:gochecknoglobals // per-test-binary sequence counter for schema uniqueness
var PGSchemaSeq atomic.Uint64

// RequirePostgresDSN resolves the target database for the Postgres suite: it
// returns the DSN, skips when none is configured, or fails when requirePostgresEnv
// is set. Every Postgres entry point routes through this one function so the
// strictness guarantee has no unguarded caller.
func RequirePostgresDSN(t *testing.T) string {
	t.Helper()
	if dsn := os.Getenv(testDatabaseURLEnv); dsn != "" {
		return dsn
	}
	if os.Getenv(requirePostgresEnv) != "" {
		t.Fatalf("%s is set but %s is empty: the Postgres suite must not be skipped in this environment",
			requirePostgresEnv, testDatabaseURLEnv)
	}
	t.Skipf("%s is not set; skipping the Postgres suite", testDatabaseURLEnv)
	return ""
}

// NewPostgresIsolatedDB returns a *gorm.DB bound to a freshly created Postgres
// schema carrying the full runtime schema. Each call mints a unique schema and
// its own pool, so callers can use t.Parallel; the schema is dropped on cleanup.
func NewPostgresIsolatedDB(t *testing.T) *gorm.DB {
	t.Helper()
	dsn := RequirePostgresDSN(t)

	base, err := gorm.Open(postgres.Open(dsn), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	if err != nil {
		t.Fatalf("connect to the Postgres test database: %v", err)
	}
	t.Cleanup(func() {
		if sqlDB, dbErr := base.DB(); dbErr == nil {
			_ = sqlDB.Close()
		}
	})

	schema := fmt.Sprintf("t_%d_%d", time.Now().UnixNano(), PGSchemaSeq.Add(1))
	if err := base.Exec(`DROP SCHEMA IF EXISTS ` + schema + ` CASCADE`).Error; err != nil {
		t.Fatalf("drop schema %s: %v", schema, err)
	}
	if err := base.Exec(`CREATE SCHEMA ` + schema).Error; err != nil {
		t.Fatalf("create schema %s: %v", schema, err)
	}

	db, err := gorm.Open(postgres.Open(WithSearchPath(dsn, schema)), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	if err != nil {
		t.Fatalf("open schema-scoped connection for %s: %v", schema, err)
	}
	t.Cleanup(func() {
		if sqlDB, dbErr := db.DB(); dbErr == nil {
			_ = sqlDB.Close()
		}
		_ = base.Exec(`DROP SCHEMA IF EXISTS ` + schema + ` CASCADE`).Error
	})

	if err := core.Migrate(db); err != nil {
		t.Fatalf("migrate schema %s: %v", schema, err)
	}
	return db
}

// WithSearchPath appends a search_path runtime parameter to a Postgres URL DSN so
// every connection on the resulting pool resolves unqualified names to schema.
func WithSearchPath(dsn, schema string) string {
	sep := "?"
	if strings.Contains(dsn, "?") {
		sep = "&"
	}
	return dsn + sep + "search_path=" + schema
}
