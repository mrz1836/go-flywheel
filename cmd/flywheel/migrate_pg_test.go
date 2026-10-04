//go:build integration

package main

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	flywheel "github.com/mrz1836/go-flywheel"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// newCLIPostgresDB opens a pool bound to a fresh, empty PostgreSQL schema,
// dropped on cleanup. It follows the core suite's rules: skipped without
// FLYWHEEL_TEST_DATABASE_URL, and failed instead when FLYWHEEL_REQUIRE_POSTGRES
// says the suite must not skip.
func newCLIPostgresDB(t *testing.T) *gorm.DB {
	t.Helper()
	dsn := os.Getenv("FLYWHEEL_TEST_DATABASE_URL")
	if dsn == "" {
		if os.Getenv("FLYWHEEL_REQUIRE_POSTGRES") != "" {
			t.Fatal("FLYWHEEL_REQUIRE_POSTGRES is set but FLYWHEEL_TEST_DATABASE_URL is empty")
		}
		t.Skip("FLYWHEEL_TEST_DATABASE_URL is not set; skipping the Postgres CLI tests")
	}
	quiet := &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)}
	base, err := gorm.Open(postgres.Open(dsn), quiet)
	require.NoError(t, err)
	schema := fmt.Sprintf("cli_%d", time.Now().UnixNano())
	require.NoError(t, base.Exec(`CREATE SCHEMA `+schema).Error)

	sep := "?"
	if strings.Contains(dsn, "?") {
		sep = "&"
	}
	db, err := gorm.Open(postgres.Open(dsn+sep+"search_path="+schema), quiet)
	require.NoError(t, err)
	t.Cleanup(func() {
		if sqlDB, dbErr := db.DB(); dbErr == nil {
			_ = sqlDB.Close()
		}
		_ = base.Exec(`DROP SCHEMA IF EXISTS ` + schema + ` CASCADE`).Error
		if sqlDB, dbErr := base.DB(); dbErr == nil {
			_ = sqlDB.Close()
		}
	})
	return db
}

// invalidateIndex flips an index's catalog flag the way a failed CREATE INDEX
// CONCURRENTLY leaves it.
func invalidateIndex(t *testing.T, db *gorm.DB, name string) {
	t.Helper()
	require.NoError(t, db.Exec(`UPDATE pg_index SET indisvalid = false
		WHERE indexrelid = (SELECT c.oid FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
		                    WHERE n.nspname = current_schema() AND c.relname = ?)`, name).Error)
}

// TestMigrateOnStartRepairsAnInvalidIndexPostgres is the startup path of
// `flywheel serve` after an interrupted `flywheel migrate --concurrently`: the
// leftover invalid jobs_finished used to fail the start with an IndexDriftError
// until someone re-ran the migration by hand. migrateOnStart builds concurrently,
// so it rebuilds the index without blocking writers and the start proceeds.
func TestMigrateOnStartRepairsAnInvalidIndexPostgres(t *testing.T) {
	t.Parallel()
	db := newCLIPostgresDB(t)
	ctx := context.Background()
	require.NoError(t, migrateOnStart(db), "a fresh install")
	invalidateIndex(t, db, "jobs_finished")

	require.NoError(t, migrateOnStart(db))
	drift, err := flywheel.InspectIndexes(ctx, db)
	require.NoError(t, err)
	assert.Empty(t, drift, "the invalid index was rebuilt valid")
}

// TestRunDoctorRepairsAnInvalidIndexPostgres is the same leftover met by
// `flywheel doctor`, the post-upgrade check: it repairs the index and reports
// the indexes in sync rather than failing.
func TestRunDoctorRepairsAnInvalidIndexPostgres(t *testing.T) {
	t.Parallel()
	db := newCLIPostgresDB(t)
	require.NoError(t, migrateOnStart(db))
	invalidateIndex(t, db, "jobs_finished")

	var out bytes.Buffer
	cfg := &Config{DB: DBConfig{Postgres: "postgres://unused"}, Runtime: defaultConfig().Runtime}
	require.NoError(t, runDoctor(context.Background(), &out, "cfg.yaml", cfg, db, flywheel.NewPostgresDriver(db)))
	assert.Contains(t, out.String(), "indexes:      in sync")
	assert.Contains(t, out.String(), "status:       OK")
}
