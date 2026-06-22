package main

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// writeConfigFile writes content to a temp flywheel.yaml and returns its path.
func writeConfigFile(t *testing.T, content string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "flywheel.yaml")
	require.NoError(t, os.WriteFile(p, []byte(content), 0o600))
	return p
}

func TestLoadConfigDefaultsWhenFileMissing(t *testing.T) {
	t.Parallel()
	cfg, err := LoadConfig(filepath.Join(t.TempDir(), "absent.yaml"))
	require.NoError(t, err)
	assert.Equal(t, []string{"default", "periodic"}, cfg.Runtime.Queues)
	assert.Equal(t, 1, cfg.Runtime.Concurrency)
	assert.Equal(t, 30*time.Second, cfg.Runtime.Lease.Std())
	assert.Equal(t, "info", cfg.Log.Level)
}

func TestLoadConfigParsesDurationsAndSchedules(t *testing.T) {
	t.Parallel()
	p := writeConfigFile(t, `
db:
  sqlite: /tmp/x.db
runtime:
  queues: [q1]
  lease: 45s
  poll_interval: 100ms
schedules:
  - slug: nightly
    cron: "0 2 * * *"
    worker: exec
    exec:
      command: /bin/echo
      args: ["hi"]
      timeout_seconds: 30
  - slug: ping
    every: 1m
    worker: http
    http:
      url: https://x.test/health
`)
	cfg, err := LoadConfig(p)
	require.NoError(t, err)
	assert.Equal(t, 45*time.Second, cfg.Runtime.Lease.Std())
	assert.Equal(t, 100*time.Millisecond, cfg.Runtime.PollInterval.Std())
	require.Len(t, cfg.Schedules, 2)
	assert.Equal(t, "exec", cfg.Schedules[0].Worker)
	require.NotNil(t, cfg.Schedules[0].Exec)
	assert.Equal(t, "/bin/echo", cfg.Schedules[0].Exec.Command)
	assert.Equal(t, 30, cfg.Schedules[0].Exec.TimeoutSeconds)
	assert.Equal(t, time.Minute, cfg.Schedules[1].Every.Std())
	require.NotNil(t, cfg.Schedules[1].HTTP)
	assert.Equal(t, "https://x.test/health", cfg.Schedules[1].HTTP.URL)
}

func TestLoadConfigValidationErrors(t *testing.T) {
	t.Parallel()
	tests := map[string]string{
		"sqlite concurrency over 1": "runtime:\n  concurrency: 4\n",
		"exec without command":      "schedules:\n  - slug: s\n    every: 1m\n    worker: exec\n",
		"http without url":          "schedules:\n  - slug: s\n    every: 1m\n    worker: http\n",
		"unknown worker":            "schedules:\n  - slug: s\n    every: 1m\n    worker: bogus\n    exec: {command: x}\n",
		"both cron and every":       "schedules:\n  - slug: s\n    cron: \"* * * * *\"\n    every: 1m\n    worker: exec\n    exec: {command: x}\n",
		"neither cron nor every":    "schedules:\n  - slug: s\n    worker: exec\n    exec: {command: x}\n",
		"duplicate slug":            "schedules:\n  - {slug: dup, every: 1m, worker: exec, exec: {command: x}}\n  - {slug: dup, every: 1m, worker: exec, exec: {command: y}}\n",
		"bad duration":              "runtime:\n  lease: not-a-duration\n",
	}
	for name, body := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			_, err := LoadConfig(writeConfigFile(t, body))
			require.Error(t, err)
		})
	}
}

func TestLoadConfigPostgresAllowsHigherConcurrency(t *testing.T) {
	t.Parallel()
	cfg, err := LoadConfig(writeConfigFile(t, "db:\n  postgres: postgres://x\nruntime:\n  concurrency: 8\n"))
	require.NoError(t, err)
	assert.Equal(t, 8, cfg.Runtime.Concurrency)
}
