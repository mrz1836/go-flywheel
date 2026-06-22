package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	flywheel "github.com/mrz1836/go-flywheel"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

// newCLITestDB opens a temp SQLite database with the flywheel schema, the way the
// CLI does, for the command and reconcile tests.
func newCLITestDB(t *testing.T) *gorm.DB {
	t.Helper()
	cfg := &Config{DB: DBConfig{SQLite: filepath.Join(t.TempDir(), "flywheel.db")}}
	db, _, err := openDB(cfg)
	require.NoError(t, err)
	t.Cleanup(func() { closeDB(db) })
	require.NoError(t, flywheel.Migrate(db))
	return db
}

func TestOpenDBSQLiteCreatesDirAndWALMode(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	cfg := &Config{DB: DBConfig{SQLite: filepath.Join(dir, "nested", "flywheel.db")}}

	db, driver, err := openDB(cfg)
	require.NoError(t, err)
	t.Cleanup(func() { closeDB(db) })
	require.NotNil(t, driver)
	require.NoError(t, flywheel.Migrate(db))

	var mode string
	require.NoError(t, db.Raw("PRAGMA journal_mode").Scan(&mode).Error)
	assert.Equal(t, "wal", strings.ToLower(mode), "the local daemon uses WAL mode")

	_, statErr := os.Stat(filepath.Join(dir, "nested", "flywheel.db"))
	require.NoError(t, statErr, "the sqlite file and its parent dir are created")
}

func TestExpandHome(t *testing.T) {
	t.Parallel()
	home, err := os.UserHomeDir()
	require.NoError(t, err)

	got, err := expandHome("~/x/y")
	require.NoError(t, err)
	assert.Equal(t, filepath.Join(home, "x", "y"), got)

	got, err = expandHome("~")
	require.NoError(t, err)
	assert.Equal(t, home, got)

	got, err = expandHome("/abs/path")
	require.NoError(t, err)
	assert.Equal(t, "/abs/path", got)
}

func TestDBLabel(t *testing.T) {
	t.Parallel()
	assert.Equal(t, "postgres", dbLabel(&Config{DB: DBConfig{Postgres: "postgres://secret@host/db"}}))
	assert.Contains(t, dbLabel(&Config{DB: DBConfig{SQLite: "/a/b.db"}}), "sqlite:/a/b.db")
}

func TestSQLiteDSNHasHardeningPragmas(t *testing.T) {
	t.Parallel()
	dsn := sqliteDSN("/x/y.db")
	for _, want := range []string{"_journal_mode=WAL", "_busy_timeout=5000", "_txlock=immediate"} {
		assert.Contains(t, dsn, want)
	}
}
