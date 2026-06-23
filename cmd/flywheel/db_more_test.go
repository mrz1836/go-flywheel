package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOpenDBPostgresParseError(t *testing.T) {
	t.Parallel()
	// A malformed Postgres DSN fails at the parse stage inside gorm.Open — before any
	// dial — so the Postgres branch's error path is covered with no network or socket
	// connection to a real database.
	_, _, err := openDB(&Config{DB: DBConfig{Postgres: "://bad"}})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "open postgres", "the Postgres open error is wrapped")
}

func TestOpenDBSQLiteDirCreationError(t *testing.T) {
	t.Parallel()
	// A SQLite path whose parent is a regular file cannot have its directory created,
	// hitting openDB's MkdirAll error branch.
	cfg := unopenableSQLiteConfig(t)
	_, _, err := openDB(cfg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "create sqlite dir")
}

func TestExpandHomeErrorsWhenHomeUnset(t *testing.T) {
	// Not parallel: it clears HOME so os.UserHomeDir fails.
	t.Setenv("HOME", "")
	_, err := expandHome("~/state")
	require.Error(t, err, "an unresolvable home directory is an error")
	assert.Contains(t, err.Error(), "home dir")

	_, err = expandHome("~")
	require.Error(t, err)
}

func TestSQLitePathErrorsWhenConfigDirUnresolvable(t *testing.T) {
	// Not parallel: it clears the env so os.UserConfigDir fails for the default path.
	t.Setenv("HOME", "")
	t.Setenv("XDG_CONFIG_HOME", "")
	_, err := sqlitePath(&Config{})
	require.Error(t, err, "an unresolvable config dir surfaces as a sqlitePath error")
	assert.Contains(t, err.Error(), "user config dir")
}

func TestIsSQLite(t *testing.T) {
	t.Parallel()
	assert.True(t, isSQLite(&Config{}), "no postgres dsn means sqlite")
	assert.False(t, isSQLite(&Config{DB: DBConfig{Postgres: "postgres://x"}}), "a postgres dsn is not sqlite")
}
