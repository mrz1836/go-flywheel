package flywheel

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// dbSeq disambiguates the per-test shared-cache in-memory DSN.
//
//nolint:gochecknoglobals // sequence counter for per-test DSN uniqueness
var dbSeq atomic.Uint64

// jobsPartialIndexes is the minimum index set the jobs runtime depends on for
// correctness: the unique_key partial index enforces idempotency (FR-005).
// Other production partial indexes are performance-only and intentionally
// omitted from the test schema.
//
//nolint:gochecknoglobals // shared DDL fixtures applied across tests
var jobsPartialIndexes = []string{
	`CREATE UNIQUE INDEX IF NOT EXISTS jobs_unique_key ON jobs (unique_key) WHERE unique_key IS NOT NULL`,
}

// newDB builds a fresh in-memory SQLite DB and migrates the jobs-runtime row
// types (jobRow/jobRunRow/jobPeriodicRow from rows.go). The DSN uses
// shared-cache so the runner's claim transaction and a concurrent writer
// reach the same in-memory database.
func newDB(t testing.TB) *gorm.DB {
	t.Helper()
	dsn := fmt.Sprintf("file:jobs-test-%d?mode=memory&cache=shared", dbSeq.Add(1))
	db, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	if err != nil {
		t.Fatalf("newDB: open: %v", err)
	}
	t.Cleanup(func() {
		if sqlDB, derr := db.DB(); derr == nil {
			_ = sqlDB.Close()
		}
	})
	if err := db.AutoMigrate(&jobRow{}, &jobRunRow{}, &jobPeriodicRow{}); err != nil {
		t.Fatalf("newDB: migrate: %v", err)
	}
	for _, ddl := range jobsPartialIndexes {
		if err := db.Exec(ddl).Error; err != nil {
			t.Fatalf("newDB: partial index: %v", err)
		}
	}
	return db
}

// newRunner builds a SQLite-backed Runner. Tests that need a custom registry
// pre-populate it before calling.
func newRunner(t testing.TB, db *gorm.DB, reg *Registry) *Runner {
	t.Helper()
	runner, err := NewRunner(RunnerConfig{
		DB:            db,
		Driver:        NewSQLiteDriver(db),
		Registry:      reg,
		Queues:        []string{"default", "periodic"},
		ExecutorClass: "local",
		Concurrency:   1,
	})
	if err != nil {
		t.Fatalf("newRunner: %v", err)
	}
	return runner
}

// runToIdle drains the queue and fails the test on any error.
//
//nolint:revive // ctx-as-second-arg matches the testing.TB-first convention used by the test helpers
func runToIdle(t testing.TB, ctx context.Context, r *Runner) {
	t.Helper()
	if err := r.RunUntilIdle(ctx); err != nil {
		t.Fatalf("runToIdle: %v", err)
	}
}

// clockCtx returns a ctx carrying clk so scheduler-driven tests get a
// deterministic clock.
func clockCtx(ctx context.Context, clk Clock) context.Context {
	return WithClock(ctx, clk)
}
