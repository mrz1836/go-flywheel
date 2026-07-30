package flywheel

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/glebarez/sqlite"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// openSQLiteRaw opens a SQLite connection at dsn with no schema, silencing the
// GORM logger and closing the handle at test end. maxOpen, when positive, caps
// the pool so the :memory: single-writer shape can be reproduced exactly.
func openSQLiteRaw(t *testing.T, dsn string, maxOpen int) *gorm.DB {
	t.Helper()
	db, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	require.NoError(t, err, "open %s", dsn)
	if maxOpen > 0 {
		sqlDB, derr := db.DB()
		require.NoError(t, derr)
		sqlDB.SetMaxOpenConns(maxOpen)
	}
	t.Cleanup(func() {
		if sqlDB, derr := db.DB(); derr == nil {
			_ = sqlDB.Close()
		}
	})
	return db
}

// fileDSN builds a file-backed SQLite DSN under a fresh temp dir, so each case
// gets its own database and the WAL sidecar files are cleaned up with the dir.
func fileDSN(t *testing.T, pragmas string) string {
	t.Helper()
	dsn := "file:" + filepath.Join(t.TempDir(), "pragma.db")
	if pragmas != "" {
		dsn += "?" + pragmas
	}
	return dsn
}

// TestSQLitePragmaCheckRejectsFileWithoutWAL is A5's core case: a file database
// left on the default rollback journal is refused, and the error names the pragma
// so the operator knows what to set.
func TestSQLitePragmaCheckRejectsFileWithoutWAL(t *testing.T) {
	db := openSQLiteRaw(t, fileDSN(t, ""), 0)

	d, err := NewSQLiteDriverWithOptions(db, SQLiteOpts{})
	require.ErrorIs(t, err, ErrSQLitePragma)
	assert.Nil(t, d, "a failed check returns no driver")
	assert.Contains(t, err.Error(), "journal_mode", "the error names the offending pragma")
}

// TestSQLitePragmaCheckRejectsZeroBusyTimeout proves a WAL file database with the
// busy timeout disabled is refused — it would fail the brief claim lock outright
// rather than absorbing it.
func TestSQLitePragmaCheckRejectsZeroBusyTimeout(t *testing.T) {
	db := openSQLiteRaw(t, fileDSN(t, "_pragma=journal_mode(WAL)&_pragma=busy_timeout(0)"), 0)

	_, err := NewSQLiteDriverWithOptions(db, SQLiteOpts{})
	require.ErrorIs(t, err, ErrSQLitePragma)
	assert.Contains(t, err.Error(), "busy_timeout")
}

// TestSQLitePragmaCheckRejectsSynchronousOff proves synchronous=OFF is refused: a
// durable job runtime must not risk losing a committed outcome on power loss.
func TestSQLitePragmaCheckRejectsSynchronousOff(t *testing.T) {
	db := openSQLiteRaw(t, fileDSN(t,
		"_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)&_pragma=synchronous(OFF)"), 0)

	_, err := NewSQLiteDriverWithOptions(db, SQLiteOpts{})
	require.ErrorIs(t, err, ErrSQLitePragma)
	assert.Contains(t, err.Error(), "synchronous")
}

// TestSQLitePragmaCheckAcceptsHardenedFile is the happy file path: WAL, a busy
// timeout, a safe synchronous level, foreign keys on, and _txlock=immediate all
// present construct cleanly.
func TestSQLitePragmaCheckAcceptsHardenedFile(t *testing.T) {
	db := openSQLiteRaw(t, fileDSN(t,
		"_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)&_pragma=synchronous(NORMAL)"+
			"&_pragma=foreign_keys(1)&_txlock=immediate"), 0)

	d, err := NewSQLiteDriverWithOptions(db, SQLiteOpts{})
	require.NoError(t, err)
	assert.NotNil(t, d)
}

// TestSQLitePragmaCheckAcceptsInMemoryShapes is FR-10-08: both real consumer
// in-memory shapes — bare :memory: with a single connection, and the library's
// own shared-cache DSN — must construct successfully. An in-memory database
// cannot use WAL, so the check exempts journal_mode for it.
func TestSQLitePragmaCheckAcceptsInMemoryShapes(t *testing.T) {
	cases := []struct {
		name    string
		dsn     string
		maxOpen int
	}{
		{"bare memory, single connection", ":memory:", 1},
		{"shared-cache memory", "file:pragma-shared?mode=memory&cache=shared", 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			db := openSQLiteRaw(t, tc.dsn, tc.maxOpen)
			d, err := NewSQLiteDriverWithOptions(db, SQLiteOpts{})
			require.NoError(t, err, "an in-memory database must not be rejected")
			assert.NotNil(t, d)
		})
	}
}

// TestSQLitePragmaCheckSkip is FR-10-09: SkipPragmaCheck bypasses verification, so
// even a misconfigured connection constructs — the escape hatch for a host that
// has hardened its connection by other means.
func TestSQLitePragmaCheckSkip(t *testing.T) {
	db := openSQLiteRaw(t, fileDSN(t, ""), 0) // no WAL: would fail the check

	d, err := NewSQLiteDriverWithOptions(db, SQLiteOpts{SkipPragmaCheck: true})
	require.NoError(t, err, "SkipPragmaCheck runs no check")
	assert.NotNil(t, d)
}

// TestSQLitePragmaCheckEmbedsBatchingOpts proves SQLiteOpts carries the batching
// DriverOpts through to the driver, so a host sets both in one value.
func TestSQLitePragmaCheckEmbedsBatchingOpts(t *testing.T) {
	db := openSQLiteRaw(t, ":memory:", 1)
	d, err := NewSQLiteDriverWithOptions(db, SQLiteOpts{DriverOpts: DriverOpts{SweepBatchSize: 7}})
	require.NoError(t, err)
	sd, ok := d.(*sqliteDriver)
	require.True(t, ok)
	assert.Equal(t, 7, sd.opts.SweepBatchSize, "the embedded batching opts reach the driver")
}

// TestNewSQLiteDriverLogsButReturnsDriver proves the one-argument constructor
// keeps its error-free signature: a misconfigured connection is logged, not
// returned, and a usable driver comes back regardless.
func TestNewSQLiteDriverLogsButReturnsDriver(t *testing.T) {
	db := openSQLiteRaw(t, fileDSN(t, ""), 0) // no WAL

	d := NewSQLiteDriver(db)
	assert.NotNil(t, d, "NewSQLiteDriver returns a driver even when the check fails")
}

// TestSQLiteDSNFromDialectorReadsTxlock guards the one place _txlock is inspected:
// the DSN is read off the glebarez dialector, and its presence is detectable.
func TestSQLiteDSNFromDialectorReadsTxlock(t *testing.T) {
	db := openSQLiteRaw(t, "file:txlock?mode=memory&cache=shared&_txlock=immediate", 0)
	d, ok := db.Dialector.(*sqlite.Dialector)
	require.True(t, ok, "the dialector exposes its DSN")
	assert.True(t, strings.Contains(strings.ToLower(d.DSN), "_txlock=immediate"))
}
