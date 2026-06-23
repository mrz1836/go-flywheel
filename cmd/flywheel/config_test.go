package main

import (
	"os"
	"path/filepath"
	"strconv"
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

func TestLoadConfigParsesLocalRunnerSchedules(t *testing.T) {
	t.Parallel()
	p := writeConfigFile(t, `
db:
  sqlite: /tmp/x.db
schedules:
  - slug: maint
    every: 24h
    worker: shell
    shell:
      script: /usr/local/bin/maintenance.sh
      args: ["--verbose"]
      timeout_seconds: 600
  - slug: sync
    cron: "0 * * * *"
    worker: python
    python:
      script: /opt/sync.py
      interpreter: python3
  - slug: deps
    every: 24h
    worker: mage
    mage:
      targets: ["deps:update"]
      binary: magex
      dir: /repo
`)
	cfg, err := LoadConfig(p)
	require.NoError(t, err)
	require.Len(t, cfg.Schedules, 3)

	require.NotNil(t, cfg.Schedules[0].Shell)
	assert.Equal(t, "/usr/local/bin/maintenance.sh", cfg.Schedules[0].Shell.Script)
	assert.Equal(t, []string{"--verbose"}, cfg.Schedules[0].Shell.Args)
	assert.Equal(t, 600, cfg.Schedules[0].Shell.TimeoutSeconds)

	require.NotNil(t, cfg.Schedules[1].Python)
	assert.Equal(t, "/opt/sync.py", cfg.Schedules[1].Python.Script)
	assert.Equal(t, "python3", cfg.Schedules[1].Python.Interpreter)

	require.NotNil(t, cfg.Schedules[2].Mage)
	assert.Equal(t, []string{"deps:update"}, cfg.Schedules[2].Mage.Targets)
	assert.Equal(t, "magex", cfg.Schedules[2].Mage.Binary)
	assert.Equal(t, "/repo", cfg.Schedules[2].Mage.Dir)
}

func TestLoadConfigValidationErrors(t *testing.T) {
	t.Parallel()
	tests := map[string]string{
		"sqlite concurrency over 1": "runtime:\n  concurrency: 4\n",
		"exec without command":      "schedules:\n  - slug: s\n    every: 1m\n    worker: exec\n",
		"http without url":          "schedules:\n  - slug: s\n    every: 1m\n    worker: http\n",
		"shell without script":      "schedules:\n  - slug: s\n    every: 1m\n    worker: shell\n",
		"python without source":     "schedules:\n  - slug: s\n    every: 1m\n    worker: python\n",
		"mage without targets":      "schedules:\n  - slug: s\n    every: 1m\n    worker: mage\n    mage: {dir: /x}\n",
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

func TestLoadConfigParsesMetricsAddrAndHealthInterval(t *testing.T) {
	t.Parallel()
	cfg, err := LoadConfig(writeConfigFile(t, `
db:
  sqlite: /tmp/x.db
runtime:
  metrics_addr: ":9090"
  health_sample_interval: 30s
`))
	require.NoError(t, err)
	assert.Equal(t, ":9090", cfg.Runtime.MetricsAddr)
	assert.Equal(t, 30*time.Second, cfg.Runtime.HealthSampleInterval.Std())
}

func TestLoadConfigDefaultsLeaveMetricsDisabled(t *testing.T) {
	t.Parallel()
	cfg, err := LoadConfig(filepath.Join(t.TempDir(), "absent.yaml"))
	require.NoError(t, err)
	assert.Empty(t, cfg.Runtime.MetricsAddr, "metrics server is off by default")
	assert.Zero(t, cfg.Runtime.HealthSampleInterval.Std(), "heartbeat is off by default")
}

// FuzzParseHumanDuration exercises the custom day/week duration parser on
// arbitrary input. It must never panic; the d/w branch must accept any integer
// prefix and compute days*unit; and every other input must behave exactly like
// time.ParseDuration (same value, same error decision).
func FuzzParseHumanDuration(f *testing.F) {
	f.Add("30s")
	f.Add("24h")
	f.Add("14d")
	f.Add("2w")
	f.Add("")
	f.Add("not-a-duration")
	f.Add("-5m")
	f.Add("1.5h30m")

	f.Fuzz(func(t *testing.T, s string) {
		got, err := parseHumanDuration(s)

		// The custom branch fires only for a d/w suffix on an integer prefix.
		if n := len(s); n >= 2 {
			if unit := s[n-1]; unit == 'd' || unit == 'w' {
				if days, aerr := strconv.Atoi(s[:n-1]); aerr == nil {
					require.NoError(t, err, "an integer %c-suffixed duration must parse", unit)
					per := 24 * time.Hour
					if unit == 'w' {
						per = 7 * 24 * time.Hour
					}
					require.Equal(t, time.Duration(days)*per, got, "d/w duration must be days*unit")
					return
				}
			}
		}

		// Every other input must match time.ParseDuration exactly.
		want, werr := time.ParseDuration(s)
		if werr != nil {
			require.Error(t, err, "input time.ParseDuration rejects must also be rejected")
			return
		}
		require.NoError(t, err)
		require.Equal(t, want, got, "non-d/w durations must match time.ParseDuration")
	})
}
