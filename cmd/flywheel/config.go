package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"gopkg.in/yaml.v3"
)

// Duration is a time.Duration that unmarshals from a YAML string like "30s",
// "24h", or "14d", so flywheel.yaml can use human-readable durations.
type Duration time.Duration

// UnmarshalYAML parses a duration string into d.
func (d *Duration) UnmarshalYAML(value *yaml.Node) error {
	var s string
	if err := value.Decode(&s); err != nil {
		return err
	}
	if s == "" {
		return nil
	}
	parsed, err := parseHumanDuration(s)
	if err != nil {
		return fmt.Errorf("invalid duration %q: %w", s, err)
	}
	*d = Duration(parsed)
	return nil
}

// Std returns the underlying time.Duration.
func (d Duration) Std() time.Duration { return time.Duration(d) }

// parseHumanDuration parses a Go duration, additionally accepting a trailing
// `d` (days) or `w` (weeks) — units time.ParseDuration lacks — so config files
// and flags can say "14d" or "2w" for retention windows.
func parseHumanDuration(s string) (time.Duration, error) {
	if n := len(s); n >= 2 {
		switch unit := s[n-1]; unit {
		case 'd', 'w':
			if days, err := strconv.Atoi(s[:n-1]); err == nil {
				per := 24 * time.Hour
				if unit == 'w' {
					per = 7 * 24 * time.Hour
				}
				return time.Duration(days) * per, nil
			}
		}
	}
	return time.ParseDuration(s)
}

// Config is the flywheel.yaml schema: which database to use, how the runtime
// behaves, how to log, and the declarative schedules (the cron replacement).
type Config struct {
	DB        DBConfig        `yaml:"db"`
	Runtime   RuntimeConfig   `yaml:"runtime"`
	Log       LogConfig       `yaml:"log"`
	Schedules []ScheduleEntry `yaml:"schedules"`
}

// DBConfig selects the datastore. A non-empty Postgres DSN wins; otherwise the
// SQLite file path is used (defaulting to a per-user state file).
type DBConfig struct {
	SQLite   string `yaml:"sqlite"`
	Postgres string `yaml:"postgres"`
}

// RuntimeConfig tunes the runner and scheduler.
type RuntimeConfig struct {
	Queues           []string `yaml:"queues"`
	Concurrency      int      `yaml:"concurrency"`
	Lease            Duration `yaml:"lease"`
	PollInterval     Duration `yaml:"poll_interval"`
	RetryBackoffBase Duration `yaml:"retry_backoff_base"`
	// Retention, when > 0, enables the daemon's retention sweep in `serve`:
	// terminal jobs (and their runs) finalized longer ago than this are pruned on
	// a cadence. Zero (the default) disables it — nothing is ever deleted.
	Retention Duration `yaml:"retention"`
	// MetricsAddr, when set (e.g. ":9090"), makes `serve` start the Node's HTTP
	// server exposing /healthz, /readyz, and a Prometheus /metrics endpoint. Empty
	// (the default) disables the server entirely.
	MetricsAddr string `yaml:"metrics_addr"`
	// HealthSampleInterval, when > 0, enables the scheduler's queue-health
	// heartbeat in `serve`: a one-line pulse (ready, in-flight, lag, discarded)
	// logged on this cadence. Zero (the default) disables it.
	HealthSampleInterval Duration `yaml:"health_sample_interval"`
	// StatsRollup is the cadence of the scheduler's hourly stats rollup in
	// `serve`, which rolls finished runs into job_stats_hourly so `flywheel stats`
	// can answer for any window and trends outlive retention. Unset (zero) selects
	// one minute — the rollup is on by default; a negative value (e.g. -1s)
	// disables it. With it on, serve also logs the onset of a duration regression
	// or failure spike once, at warn.
	StatsRollup Duration `yaml:"stats_rollup"`
	// ScheduleReconcile is how often `serve` re-creates a declared schedule whose
	// row was deleted while it runs — by `flywheel schedule rm`, a restore, or a
	// rebuilt database. Unset (zero) selects one minute; a negative value (e.g.
	// -1s) disables it. Serve applies the whole schedules list on every start
	// either way.
	ScheduleReconcile Duration `yaml:"schedule_reconcile"`
	// EnvAllowlist names the host environment variables exec jobs inherit. Nil
	// uses the ExecWorker default (PATH, HOME, SHELL, LANG, TMPDIR).
	EnvAllowlist []string `yaml:"env_allowlist"`
}

// defaultStatsRollup is the stats rollup cadence `serve` uses when
// runtime.stats_rollup is unset. A pass with nothing to roll is two primary-key
// reads and a range delete that finds nothing, so a minute keeps the rollup
// current at negligible cost.
const defaultStatsRollup = time.Minute

// rollupMinRetention is the shortest runtime.retention the stats rollup can
// work beside: one hour plus the scheduler's default five-minute closing grace.
// Retention any shorter would delete runs before their hour closes, so the
// scheduler refuses the combination.
const rollupMinRetention = time.Hour + 5*time.Minute

// statsRollupInterval resolves runtime.stats_rollup to the scheduler's
// StatsRollupInterval: the default when unset, zero (off) when negative. See
// effectiveStatsRollup for the retention guard serve applies on top.
func (r RuntimeConfig) statsRollupInterval() time.Duration {
	interval, _ := r.effectiveStatsRollup()
	return interval
}

// effectiveStatsRollup resolves runtime.stats_rollup and, when the rollup is off
// only because of the retention guard, says why.
//
// The guard applies to the default alone. A config written before the rollup
// existed may carry a retention shorter than the rollup can work beside, and an
// upgrade must not turn that config into a daemon that refuses to start: the
// default steps aside, with a warning. A config that sets stats_rollup
// explicitly asked for both, and the scheduler's own validation rejects it.
func (r RuntimeConfig) effectiveStatsRollup() (time.Duration, string) {
	switch {
	case r.StatsRollup < 0:
		return 0, ""
	case r.StatsRollup > 0:
		return r.StatsRollup.Std(), ""
	case r.Retention > 0 && r.Retention.Std() < rollupMinRetention:
		return 0, fmt.Sprintf("runtime.retention (%s) is shorter than the %s the stats rollup needs; "+
			"raise it, or set runtime.stats_rollup explicitly", r.Retention.Std(), rollupMinRetention)
	default:
		return defaultStatsRollup, ""
	}
}

// defaultScheduleReconcile is the cadence serve re-creates missing declared
// schedules at when runtime.schedule_reconcile is unset. Serve passes the unset
// value through and the scheduler applies its own one-minute default; this names
// the same value so doctor and the start log can report it.
const defaultScheduleReconcile = time.Minute

// scheduleReconcileInterval resolves runtime.schedule_reconcile to the cadence
// serve re-creates missing declared schedules at: the default when unset, zero
// (off) when negative.
func (r RuntimeConfig) scheduleReconcileInterval() time.Duration {
	switch {
	case r.ScheduleReconcile < 0:
		return 0
	case r.ScheduleReconcile > 0:
		return r.ScheduleReconcile.Std()
	default:
		return defaultScheduleReconcile
	}
}

// LogConfig configures the daemon's structured logger.
type LogConfig struct {
	Level  string `yaml:"level"`  // debug|info|warn|error
	Format string `yaml:"format"` // text|json
}

// ScheduleEntry is one declarative periodic job — a cron line replacement. It
// names a worker (exec, http, shell, python, or mage) and exactly one of cron or
// every.
type ScheduleEntry struct {
	Slug   string      `yaml:"slug"`
	Cron   string      `yaml:"cron"`
	Every  Duration    `yaml:"every"`
	Queue  string      `yaml:"queue"`
	Worker string      `yaml:"worker"` // exec|http|shell|python|mage
	Exec   *execSpec   `yaml:"exec"`
	HTTP   *httpSpec   `yaml:"http"`
	Shell  *shellSpec  `yaml:"shell"`
	Python *pythonSpec `yaml:"python"`
	Mage   *mageSpec   `yaml:"mage"`
}

// defaultConfig returns the runtime defaults applied to an unset flywheel.yaml.
func defaultConfig() Config {
	return Config{
		Runtime: RuntimeConfig{
			Queues:       []string{"default", "periodic"},
			Concurrency:  1,
			Lease:        Duration(30 * time.Second),
			PollInterval: Duration(250 * time.Millisecond),
		},
		Log: LogConfig{Level: "info", Format: "text"},
	}
}

// LoadConfig reads and validates flywheel.yaml at path, applying defaults for any
// unset runtime field. A missing file yields the defaults (so `flywheel serve`
// runs out of the box over a default SQLite file).
func LoadConfig(path string) (*Config, error) {
	cfg := defaultConfig()

	data, err := os.ReadFile(path) //nolint:gosec // path is an operator-supplied config file
	switch {
	case os.IsNotExist(err):
		// No file: run on the defaults.
	case err != nil:
		return nil, fmt.Errorf("read %s: %w", path, err)
	default:
		if uerr := yaml.Unmarshal(data, &cfg); uerr != nil {
			return nil, fmt.Errorf("parse %s: %w", path, uerr)
		}
	}

	applyDefaults(&cfg)
	if verr := validateConfig(&cfg); verr != nil {
		return nil, verr
	}
	return &cfg, nil
}

// applyDefaults fills any runtime field the file left zero.
func applyDefaults(cfg *Config) {
	d := defaultConfig()
	if len(cfg.Runtime.Queues) == 0 {
		cfg.Runtime.Queues = d.Runtime.Queues
	}
	if cfg.Runtime.Concurrency <= 0 {
		cfg.Runtime.Concurrency = d.Runtime.Concurrency
	}
	if cfg.Runtime.Lease <= 0 {
		cfg.Runtime.Lease = d.Runtime.Lease
	}
	if cfg.Runtime.PollInterval <= 0 {
		cfg.Runtime.PollInterval = d.Runtime.PollInterval
	}
	if cfg.Log.Level == "" {
		cfg.Log.Level = d.Log.Level
	}
	if cfg.Log.Format == "" {
		cfg.Log.Format = d.Log.Format
	}
}

// validateConfig checks each schedule entry names a known worker and exactly one
// schedule, and that SQLite (a single writer) keeps concurrency at 1.
func validateConfig(cfg *Config) error {
	if cfg.DB.Postgres == "" && cfg.Runtime.Concurrency > 1 {
		return fmt.Errorf("config: sqlite requires concurrency 1 (got %d); set a postgres dsn to raise it", cfg.Runtime.Concurrency)
	}
	seen := map[string]bool{}
	for i := range cfg.Schedules {
		s := &cfg.Schedules[i]
		if s.Slug == "" {
			return fmt.Errorf("config: schedule[%d] is missing a slug", i)
		}
		if seen[s.Slug] {
			return fmt.Errorf("config: duplicate schedule slug %q", s.Slug)
		}
		seen[s.Slug] = true
		switch s.Worker {
		case "exec":
			if s.Exec == nil || s.Exec.Command == "" {
				return fmt.Errorf("config: schedule %q (exec) needs an exec.command", s.Slug)
			}
		case "http":
			if s.HTTP == nil || s.HTTP.URL == "" {
				return fmt.Errorf("config: schedule %q (http) needs an http.url", s.Slug)
			}
		case "shell":
			if s.Shell == nil || (s.Shell.Script == "" && s.Shell.Inline == "") {
				return fmt.Errorf("config: schedule %q (shell) needs a shell.script or shell.inline", s.Slug)
			}
		case "python":
			if s.Python == nil || (s.Python.Script == "" && s.Python.Module == "" && s.Python.Inline == "") {
				return fmt.Errorf("config: schedule %q (python) needs a python.script, python.module, or python.inline", s.Slug)
			}
		case "mage":
			if s.Mage == nil || len(s.Mage.Targets) == 0 {
				return fmt.Errorf("config: schedule %q (mage) needs at least one mage.targets entry", s.Slug)
			}
		default:
			return fmt.Errorf("config: schedule %q has unknown worker %q (want exec, http, shell, python, or mage)", s.Slug, s.Worker)
		}
		if (s.Cron == "") == (s.Every <= 0) {
			return fmt.Errorf("config: schedule %q needs exactly one of cron or every", s.Slug)
		}
	}
	return nil
}

// defaultConfigPath resolves the default flywheel.yaml location: ./flywheel.yaml
// when present, else the per-user config dir.
func defaultConfigPath() string {
	const name = "flywheel.yaml"
	if _, err := os.Stat(name); err == nil {
		return name
	}
	if dir, err := os.UserConfigDir(); err == nil {
		return filepath.Join(dir, "flywheel", name)
	}
	return name
}
