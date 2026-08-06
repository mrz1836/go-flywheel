package flywheeltest

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/glebarez/sqlite"
	core "github.com/mrz1836/go-flywheel/internal/core"
	"github.com/mrz1836/go-foundation/models"
	"github.com/mrz1836/go-foundation/testutil"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// dbSeq disambiguates the per-test shared-cache in-memory DSN.
//
//nolint:gochecknoglobals // sequence counter for per-test DSN uniqueness
var dbSeq atomic.Uint64

// NewBareSQLite opens a fresh in-memory SQLite database with no schema applied.
func NewBareSQLite(t testing.TB) *gorm.DB {
	t.Helper()
	dsn := fmt.Sprintf("file:flywheel-test-%d?mode=memory&cache=shared", dbSeq.Add(1))
	db, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	require.NoError(t, err)
	t.Cleanup(func() {
		if sqlDB, derr := db.DB(); derr == nil {
			_ = sqlDB.Close()
		}
	})
	return db
}

// NewDB builds a fresh in-memory SQLite database carrying the full runtime schema.
func NewDB(t testing.TB) *gorm.DB {
	t.Helper()
	db := NewBareSQLite(t)
	require.NoError(t, core.Migrate(db))
	return db
}

// NewWALFileDB opens a fresh file-backed SQLite database in WAL mode — the shape a
// Node test needs so the runner can write while the test polls for progress.
func NewWALFileDB(t testing.TB) *gorm.DB {
	t.Helper()
	dsn := "file:" + t.TempDir() + "/flywheel.db?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)&_pragma=synchronous(NORMAL)&_txlock=immediate"
	db, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	require.NoError(t, err)
	t.Cleanup(func() {
		if sqlDB, derr := db.DB(); derr == nil {
			_ = sqlDB.Close()
		}
	})
	require.NoError(t, core.Migrate(db))
	return db
}

// JobState reads the state column for jobID.
func JobState(t testing.TB, db *gorm.DB, jobID string) string {
	t.Helper()
	var s string
	require.NoError(t, db.Table("jobs").Select("state").Where("id = ?", jobID).Scan(&s).Error)
	return s
}

// WaitForJobState polls until jobID reaches state or the deadline elapses.
func WaitForJobState(t testing.TB, db *gorm.DB, jobID, state string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if JobState(t, db, jobID) == state {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("job %s did not reach state %q within %s (last: %q)", jobID, state, timeout, JobState(t, db, jobID))
}

// FreeAddr reserves and releases an ephemeral loopback port for a server to
// bind. It delegates to go-foundation/testutil so the internal test packages
// keep their existing import path.
func FreeAddr(t testing.TB) string {
	return testutil.FreeAddr(t)
}

// InstallPeriodic seeds a job_periodics row directly, bypassing the work-context
// save hook, so a scheduler test has a due definition to fire.
func InstallPeriodic(t testing.TB, db *gorm.DB, slug, kind string, nextRunAt time.Time, active bool) {
	t.Helper()
	require.NoError(t, db.Exec(
		`INSERT INTO job_periodics(id, slug, kind, args_template, queue, interval_seconds, next_run_at, is_active, created_at, updated_at)
		 VALUES (?,?,?,?,?,?,?,?,?,?)`,
		models.NewID(), slug, kind, "{}", "periodic", 60, nextRunAt, active,
		time.Now().UTC(), time.Now().UTC(),
	).Error)
}

// SuccessArgs is the args for a worker that always succeeds.
type SuccessArgs struct{ V string }

// Kind identifies the worker kind.
func (SuccessArgs) Kind() string { return "test.success" }

// SuccessWorker always succeeds and counts its calls.
type SuccessWorker struct{ Calls atomic.Int32 }

// Kind identifies the worker kind.
func (*SuccessWorker) Kind() string { return "test.success" }

// Work records one call and succeeds.
func (w *SuccessWorker) Work(_ context.Context, _ *core.Job[SuccessArgs]) (core.Result, error) {
	w.Calls.Add(1)
	return core.Result{}, nil
}

// RecordingHandler captures the structured attributes of every log record, so a
// test can assert on the log's cadence and content rather than on its absence.
// It aliases go-foundation/testutil.RecordingHandler so the internal test
// packages keep their existing import path and the map[string]any record shape.
type RecordingHandler = testutil.RecordingHandler
