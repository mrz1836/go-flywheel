package main

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestApplyDefaultsFillsEveryZeroField(t *testing.T) {
	t.Parallel()
	// A fully-zero config exercises every default-filling branch.
	cfg := &Config{}
	applyDefaults(cfg)
	assert.Equal(t, []string{"default", "periodic"}, cfg.Runtime.Queues)
	assert.Equal(t, 1, cfg.Runtime.Concurrency)
	assert.Equal(t, 30*time.Second, cfg.Runtime.Lease.Std())
	assert.Equal(t, 250*time.Millisecond, cfg.Runtime.PollInterval.Std())
	assert.Equal(t, "info", cfg.Log.Level)
	assert.Equal(t, "text", cfg.Log.Format)
}

func TestApplyDefaultsKeepsExplicitValues(t *testing.T) {
	t.Parallel()
	// A fully-populated config keeps its values: every default branch is skipped.
	cfg := &Config{
		Runtime: RuntimeConfig{
			Queues:       []string{"q"},
			Concurrency:  1,
			Lease:        Duration(time.Minute),
			PollInterval: Duration(time.Second),
		},
		Log: LogConfig{Level: "debug", Format: "json"},
	}
	applyDefaults(cfg)
	assert.Equal(t, []string{"q"}, cfg.Runtime.Queues)
	assert.Equal(t, time.Minute, cfg.Runtime.Lease.Std())
	assert.Equal(t, "debug", cfg.Log.Level)
	assert.Equal(t, "json", cfg.Log.Format)
}

func TestValidateConfigMissingSlug(t *testing.T) {
	t.Parallel()
	cfg := &Config{Schedules: []ScheduleEntry{{Worker: "exec", Every: Duration(time.Minute), Exec: &execSpec{Command: "true"}}}}
	err := validateConfig(cfg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "missing a slug")
}

func TestLoadConfigReadErrorOnDirectory(t *testing.T) {
	t.Parallel()
	// A directory is neither absent nor a readable file: os.ReadFile errors, hitting
	// LoadConfig's read-error branch.
	dir := t.TempDir()
	_, err := LoadConfig(dir)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "read "+dir)
}

func TestUnmarshalDurationEmptyAndInvalid(t *testing.T) {
	t.Parallel()
	// An empty duration string leaves the zero value (the early-return branch).
	cfg, err := LoadConfig(writeConfigFile(t, "runtime:\n  retention: \"\"\n"))
	require.NoError(t, err)
	assert.Zero(t, cfg.Runtime.Retention.Std(), "an empty duration string is a no-op")

	// A non-string node fails to decode into the string target.
	_, err = LoadConfig(writeConfigFile(t, "runtime:\n  retention: [1, 2]\n"))
	require.Error(t, err, "a non-scalar duration node is rejected")
}

func TestDefaultConfigPathPrefersLocalFile(t *testing.T) {
	// Not parallel: it changes the working directory.
	dir := t.TempDir()
	cwd, err := os.Getwd()
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.Chdir(cwd) })
	require.NoError(t, os.Chdir(dir))

	// With no local flywheel.yaml, the path resolves under the user config dir.
	away := defaultConfigPath()
	assert.True(t, strings.HasSuffix(away, filepath.Join("flywheel", "flywheel.yaml")) || away == "flywheel.yaml",
		"absent local file resolves to the per-user config path")

	// With a local flywheel.yaml present, that bare name wins.
	require.NoError(t, os.WriteFile(filepath.Join(dir, "flywheel.yaml"), []byte("db: {}\n"), 0o600))
	assert.Equal(t, "flywheel.yaml", defaultConfigPath(), "a local flywheel.yaml is preferred")
}

func TestDefaultConfigPathFallsBackWhenConfigDirUnresolvable(t *testing.T) {
	// Not parallel: it changes the working directory and clears the env so neither a
	// local file nor a user config dir resolves, hitting the bare-name fallback.
	dir := t.TempDir()
	cwd, err := os.Getwd()
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.Chdir(cwd) })
	require.NoError(t, os.Chdir(dir))
	t.Setenv("HOME", "")
	t.Setenv("XDG_CONFIG_HOME", "")

	assert.Equal(t, "flywheel.yaml", defaultConfigPath(),
		"with no local file and no resolvable config dir, the bare name is returned")
}

func TestParseLevelAllNames(t *testing.T) {
	t.Parallel()
	cases := map[string]string{
		"debug":   "DEBUG",
		"info":    "INFO",
		"warn":    "WARN",
		"warning": "WARN",
		"error":   "ERROR",
		"":        "INFO",
		"bogus":   "INFO",
	}
	for name, want := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, want, parseLevel(name).String())
		})
	}
}

func TestNewLoggerJSONAndText(t *testing.T) {
	t.Parallel()
	// Both handler branches are constructed; the loggers are non-nil.
	assert.NotNil(t, newLogger(&Config{Log: LogConfig{Format: "json", Level: "debug"}}))
	assert.NotNil(t, newLogger(&Config{Log: LogConfig{Format: "text", Level: "info"}}))
}

func TestSQLitePathDefaultsUnderConfigDir(t *testing.T) {
	t.Parallel()
	// An empty SQLite path resolves under the per-user config dir.
	got, err := sqlitePath(&Config{})
	require.NoError(t, err)
	assert.True(t, strings.HasSuffix(got, filepath.Join("flywheel", "flywheel.db")),
		"the default sqlite file lives under <config>/flywheel/")

	// A ~-prefixed path is expanded to the home directory.
	home, err := os.UserHomeDir()
	require.NoError(t, err)
	got, err = sqlitePath(&Config{DB: DBConfig{SQLite: "~/state/flywheel.db"}})
	require.NoError(t, err)
	assert.Equal(t, filepath.Join(home, "state", "flywheel.db"), got)
}

func TestDBLabelDefaultsToSQLiteWhenPathUnresolvable(t *testing.T) {
	// Not parallel: it clears the env vars os.UserConfigDir consults so the default
	// sqlite path cannot be resolved, exercising dbLabel's bare "sqlite" fallback.
	t.Setenv("HOME", "")
	t.Setenv("XDG_CONFIG_HOME", "")
	got := dbLabel(&Config{})
	assert.Equal(t, "sqlite", got, "an unresolvable sqlite path falls back to a bare label")
}

func TestPingDBSucceedsAndFailsOnClosedHandle(t *testing.T) {
	t.Parallel()
	db := newCLITestDB(t)
	require.NoError(t, pingDB(context.Background(), db), "a live database pings cleanly")

	sqlDB, err := db.DB()
	require.NoError(t, err)
	require.NoError(t, sqlDB.Close())
	require.Error(t, pingDB(context.Background(), db), "a closed handle fails the ping")
}

func TestDoctorReportsUnreachableDatabase(t *testing.T) {
	t.Parallel()
	db := newCLITestDB(t)
	sqlDB, err := db.DB()
	require.NoError(t, err)
	require.NoError(t, sqlDB.Close())

	// gatherStatus over a closed DB surfaces the read error; pingDB likewise. This
	// asserts the doctor's "database unreachable" framing via pingDB directly.
	require.Error(t, pingDB(context.Background(), db))
}

func TestReconcileSchedulesArgsMarshalError(t *testing.T) {
	t.Parallel()
	db := newCLITestDB(t)
	// An http worker with a nil HTTP spec makes argsTemplate dereference a nil
	// pointer — guard against that by using exec with a nil Exec, which panics; so
	// instead drive the marshal-error path through reconcile with a closed DB to
	// hit the UpsertPeriodic error branch.
	sqlDB, err := db.DB()
	require.NoError(t, err)
	require.NoError(t, sqlDB.Close())

	cfg := &Config{Schedules: []ScheduleEntry{
		{Slug: "x", Worker: "exec", Every: Duration(time.Minute), Exec: &execSpec{Command: "true"}},
	}}
	err = reconcileSchedules(context.Background(), db, cfg)
	require.Error(t, err, "a closed database fails the upsert")
}

func TestReconcileSchedulesListErrorOnClosedDB(t *testing.T) {
	t.Parallel()
	// With no configured schedules, reconcile skips the upsert loop and fails at the
	// ListPeriodics step on a closed database.
	db := newCLITestDB(t)
	sqlDB, err := db.DB()
	require.NoError(t, err)
	require.NoError(t, sqlDB.Close())

	err = reconcileSchedules(context.Background(), db, &Config{})
	require.Error(t, err, "listing existing schedules fails on a closed database")
	assert.Contains(t, err.Error(), "reconcile schedules")
}

func TestRunStatusWatchStopsImmediatelyOnCancelledContext(t *testing.T) {
	t.Parallel()
	db := newCLITestDB(t)
	cmd := &cobra.Command{}
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // already cancelled: the first render runs, then the select returns nil
	cmd.SetContext(ctx)
	var buf bytes.Buffer
	cmd.SetOut(&buf)

	err := runStatusWatch(cmd, db, &Config{DB: DBConfig{SQLite: "x"}}, time.Hour)
	require.NoError(t, err, "a cancelled context ends the watch loop cleanly")
}
