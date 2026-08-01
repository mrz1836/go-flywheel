package core

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/glebarez/sqlite"
	"github.com/mrz1836/go-foundation/models"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// dbSeq disambiguates the per-test shared-cache in-memory DSN.
//
//nolint:gochecknoglobals // sequence counter for per-test DSN uniqueness
var dbSeq atomic.Uint64

// newBareSQLite opens a fresh in-memory SQLite database with NO schema applied.
// It is the open half every SQLite fixture in the package shares, so the DSN and
// the cleanup are written once: newDB migrates on top of it, newDBWithIndexKinds
// installs a reduced schema on top of it, and the migrate tests use it directly
// to exercise Migrate from scratch.
func newBareSQLite(t testing.TB) *gorm.DB {
	t.Helper()
	dsn := fmt.Sprintf("file:flywheel-test-%d?mode=memory&cache=shared", dbSeq.Add(1))
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

// newDB builds a fresh in-memory SQLite database carrying the runtime's full
// production schema — every table and every index, exactly what a host gets from
// Migrate. It is the default fixture for the package's functional tests.
//
// # Why the whole schema, not a reduced one
//
// The schema comes from Migrate rather than from a local DDL list so it cannot
// drift: there is one installer, every real caller uses it (cmd/flywheel/serve.go,
// all four examples/, and every downstream host), and a test fixture that
// installs something else is testing a database that does not exist anywhere.
//
// Concretely, an index-reduced fixture makes a passing test prove something
// other than what it says. Without jobs_ready the claim runs as a sequential
// scan, so "the claim returns the highest-priority job" proves the ORDER BY
// works, not that the index serves it. Without job_runs_job_attempt — which is
// correctness-bearing, see IndexSet — nothing enforces one audit row per attempt,
// which is the invariant planFinalize's free-snooze reasoning rests on.
//
// The standing rule that follows: no test file in this package carries local
// index DDL. A test that wants a deliberately reduced schema calls
// newDBWithIndexKinds and says at the call site why.
//
// # Two deliberate divergences from a production pool
//
// The DSN uses shared-cache so the runner's claim transaction and a concurrent
// writer reach the same in-memory database, and the pool is left uncapped.
// A downstream host that opens a bare `:memory:` database must set
// SetMaxOpenConns(1), because each pooled connection would otherwise get its own
// private empty database. That is a workaround for a DSN this fixture does not
// use, not a stricter setting: capping the pool at one here would turn any test
// that holds a transaction open while issuing a second query into a deadlock
// instead of a red assertion.
func newDB(t testing.TB) *gorm.DB {
	t.Helper()
	db := newBareSQLite(t)
	if err := Migrate(db); err != nil {
		t.Fatalf("newDB: migrate: %v", err)
	}
	return db
}

// newDBWithIndexKinds builds an in-memory SQLite database whose schema carries
// only the named index kinds — the tables from Models, then the IndexSet entries
// matching kinds and nothing else. Passing no kinds yields a schema with no
// indexes at all.
//
// It exists to quantify what a kind is worth, and it is derived from IndexSet so
// it cannot drift from the real set. No functional test may use it: a test that
// asserts behavior on a reduced schema asserts it about a database no host runs.
func newDBWithIndexKinds(t testing.TB, kinds ...IndexKind) *gorm.DB {
	t.Helper()
	db := newBareSQLite(t)
	if err := db.AutoMigrate(Models()...); err != nil {
		t.Fatalf("newDBWithIndexKinds: automigrate: %v", err)
	}
	applyIndexKinds(t, db, kinds...)
	return db
}

// applyIndexKinds applies exactly the IndexSet entries for db's dialect whose
// Kind appears in kinds, and reports how many it applied. It fails the test when
// the selection is empty, because a comparison against an empty subset proves
// nothing about the subset.
func applyIndexKinds(t testing.TB, db *gorm.DB, kinds ...IndexKind) int {
	t.Helper()
	set, err := IndexSet(db.Name())
	if err != nil {
		t.Fatalf("applyIndexKinds: IndexSet(%q): %v", db.Name(), err)
	}
	want := make(map[IndexKind]bool, len(kinds))
	for _, k := range kinds {
		want[k] = true
	}
	applied := 0
	for _, idx := range set {
		if !want[idx.Kind] {
			continue
		}
		if err := db.Exec(idx.DDL).Error; err != nil {
			t.Fatalf("applyIndexKinds: create index %s: %v", idx.Name, err)
		}
		applied++
	}
	if len(kinds) > 0 && applied == 0 {
		t.Fatalf("applyIndexKinds: %v matched no index; the comparison would be vacuous", kinds)
	}
	return applied
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
func clockCtx(ctx context.Context, clk models.Clock) context.Context {
	return models.WithClock(ctx, clk)
}

// newScheduler builds a Scheduler over db with the default cadences, failing the
// test on a construction error.
//
// It exists so a test whose subject is a tick, a sweep, or a prune does not
// carry three lines of construction error handling that its assertion never
// reads. The construction contract itself — that a Driver is required, and that
// the dialect selects one — is asserted directly in scheduler_driver_test.go,
// which is where a reader should look for it.
func newScheduler(t testing.TB, db *gorm.DB) *Scheduler {
	t.Helper()
	s, err := NewScheduler(db, NewClient(db))
	if err != nil {
		t.Fatalf("newScheduler: %v", err)
	}
	return s
}

// newSchedulerCfg builds a Scheduler from cfg, defaulting the SQLite driver when
// the caller left one out, and fails the test on a construction error.
//
// The Driver default is a convenience for tests configuring some *other* field —
// a retention window, a backfill cap — and it is deliberately not available to
// production code: NewSchedulerWithConfig rejects a nil Driver, and
// TestNewSchedulerWithConfigRequiresADriver is what holds that line.
func newSchedulerCfg(t testing.TB, cfg SchedulerConfig) *Scheduler {
	t.Helper()
	if cfg.Driver == nil && cfg.DB != nil {
		cfg.Driver = NewSQLiteDriver(cfg.DB)
	}
	s, err := NewSchedulerWithConfig(cfg)
	if err != nil {
		t.Fatalf("newSchedulerCfg: %v", err)
	}
	return s
}

// newSingleConnMemoryDB opens a bare `:memory:` SQLite database capped at one
// connection, which is the shape an embedding host's test boundary uses: each
// pooled connection to a bare `:memory:` DSN gets its own private empty
// database, so the cap is not a tuning choice but a correctness requirement.
func newSingleConnMemoryDB(t *testing.T) *gorm.DB {
	t.Helper()
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	require.NoError(t, err)
	sqlDB, err := db.DB()
	require.NoError(t, err)
	sqlDB.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = sqlDB.Close() })
	require.NoError(t, Migrate(db))
	return db
}

// newWALFileDB opens a fresh file-backed SQLite database in WAL mode — the same
// configuration the local daemon uses — so a Node's runner can write while the
// test polls the DB to observe progress, without hitting shared-cache LOCKED
// errors. Migrate stands up the full schema.
func newWALFileDB(t *testing.T) *gorm.DB {
	t.Helper()
	dsn := "file:" + t.TempDir() + "/flywheel.db?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)&_pragma=synchronous(NORMAL)&_txlock=immediate"
	db, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	require.NoError(t, err)
	t.Cleanup(func() {
		if sqlDB, derr := db.DB(); derr == nil {
			_ = sqlDB.Close()
		}
	})
	require.NoError(t, Migrate(db))
	return db
}

// rwRunner builds a single-concurrency SQLite Runner that claims every executor
// class on the default and periodic queues, with a tight poll interval so
// backoff/snooze scenarios drain fast. The optional mutators tweak the config
// (backoff base, observer, default timeout, ...). It is the generic observed
// runner the real-world scenarios, the observer suite, and the supersede suite
// all build on.
func rwRunner(t testing.TB, db *gorm.DB, reg *Registry, mutators ...func(*RunnerConfig)) *Runner {
	t.Helper()
	cfg := RunnerConfig{
		DB:            db,
		Driver:        NewSQLiteDriver(db),
		Registry:      reg,
		Queues:        []string{"default", "periodic"},
		ExecutorClass: "local",
		ClaimAnyClass: true,
		Concurrency:   1,
		PollInterval:  2 * time.Millisecond,
	}
	for _, m := range mutators {
		m(&cfg)
	}
	r, err := NewRunner(cfg)
	if err != nil {
		t.Fatalf("rwRunner: %v", err)
	}
	return r
}
