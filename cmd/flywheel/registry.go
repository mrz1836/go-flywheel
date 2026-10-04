package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"strings"

	flywheel "github.com/mrz1836/go-flywheel"
	"github.com/mrz1836/go-flywheel/workers"
	"gorm.io/gorm"
)

// execSpec is the YAML form of an exec schedule's command. It mirrors
// workers.ExecArgs but carries YAML tags so flywheel.yaml reads naturally.
type execSpec struct {
	Command        string            `yaml:"command"`
	Args           []string          `yaml:"args"`
	Env            map[string]string `yaml:"env"`
	Dir            string            `yaml:"dir"`
	Stdin          string            `yaml:"stdin"`
	TimeoutSeconds int               `yaml:"timeout_seconds"`
}

// toArgs converts the spec into the worker's args type.
func (e execSpec) toArgs() workers.ExecArgs {
	return workers.ExecArgs{
		Command:        e.Command,
		Args:           e.Args,
		Env:            e.Env,
		Dir:            e.Dir,
		Stdin:          e.Stdin,
		TimeoutSeconds: e.TimeoutSeconds,
	}
}

// httpSpec is the YAML form of an http schedule's request.
type httpSpec struct {
	Method         string            `yaml:"method"`
	URL            string            `yaml:"url"`
	Headers        map[string]string `yaml:"headers"`
	Body           string            `yaml:"body"`
	TimeoutSeconds int               `yaml:"timeout_seconds"`
	SuccessStatus  []int             `yaml:"success_status"`
}

// toArgs converts the spec into the worker's args type.
func (h httpSpec) toArgs() workers.HTTPArgs {
	return workers.HTTPArgs{
		Method:         h.Method,
		URL:            h.URL,
		Headers:        h.Headers,
		Body:           h.Body,
		TimeoutSeconds: h.TimeoutSeconds,
		SuccessStatus:  h.SuccessStatus,
	}
}

// shellSpec is the YAML form of a shell schedule's script.
type shellSpec struct {
	Script         string            `yaml:"script"`
	Inline         string            `yaml:"inline"`
	Args           []string          `yaml:"args"`
	Shell          string            `yaml:"shell"`
	Env            map[string]string `yaml:"env"`
	Dir            string            `yaml:"dir"`
	Stdin          string            `yaml:"stdin"`
	TimeoutSeconds int               `yaml:"timeout_seconds"`
}

// toArgs converts the spec into the worker's args type.
func (s shellSpec) toArgs() workers.ShellArgs {
	return workers.ShellArgs{
		Script:         s.Script,
		Inline:         s.Inline,
		Args:           s.Args,
		Shell:          s.Shell,
		Env:            s.Env,
		Dir:            s.Dir,
		Stdin:          s.Stdin,
		TimeoutSeconds: s.TimeoutSeconds,
	}
}

// pythonSpec is the YAML form of a python schedule's program.
type pythonSpec struct {
	Script         string            `yaml:"script"`
	Module         string            `yaml:"module"`
	Inline         string            `yaml:"inline"`
	Args           []string          `yaml:"args"`
	Interpreter    string            `yaml:"interpreter"`
	Env            map[string]string `yaml:"env"`
	Dir            string            `yaml:"dir"`
	Stdin          string            `yaml:"stdin"`
	TimeoutSeconds int               `yaml:"timeout_seconds"`
}

// toArgs converts the spec into the worker's args type.
func (p pythonSpec) toArgs() workers.PythonArgs {
	return workers.PythonArgs{
		Script:         p.Script,
		Module:         p.Module,
		Inline:         p.Inline,
		Args:           p.Args,
		Interpreter:    p.Interpreter,
		Env:            p.Env,
		Dir:            p.Dir,
		Stdin:          p.Stdin,
		TimeoutSeconds: p.TimeoutSeconds,
	}
}

// mageSpec is the YAML form of a mage schedule's targets.
type mageSpec struct {
	Targets        []string          `yaml:"targets"`
	Binary         string            `yaml:"binary"`
	Dir            string            `yaml:"dir"`
	Env            map[string]string `yaml:"env"`
	TimeoutSeconds int               `yaml:"timeout_seconds"`
}

// toArgs converts the spec into the worker's args type.
func (m mageSpec) toArgs() workers.MageArgs {
	return workers.MageArgs{
		Targets:        m.Targets,
		Binary:         m.Binary,
		Dir:            m.Dir,
		Env:            m.Env,
		TimeoutSeconds: m.TimeoutSeconds,
	}
}

// workerKind returns the registered worker kind a schedule entry targets.
func (s ScheduleEntry) workerKind() string {
	switch s.Worker {
	case "http":
		return workers.HTTPKind
	case "shell":
		return workers.ShellKind
	case "python":
		return workers.PythonKind
	case "mage":
		return workers.MageKind
	default:
		return workers.ExecKind
	}
}

// argsTemplate marshals the entry's worker spec into the JSON args template the
// worker decodes on each fire.
func (s ScheduleEntry) argsTemplate() ([]byte, error) {
	switch s.Worker {
	case "http":
		return json.Marshal(s.HTTP.toArgs())
	case "shell":
		return json.Marshal(s.Shell.toArgs())
	case "python":
		return json.Marshal(s.Python.toArgs())
	case "mage":
		return json.Marshal(s.Mage.toArgs())
	default:
		return json.Marshal(s.Exec.toArgs())
	}
}

// buildRegistry registers the generic workers so any schedule (or ad-hoc enqueue)
// of those kinds can run with no custom Go: exec/shell/python/mage run local
// commands, scripts, and build targets, and http calls a URL. The command workers
// share the configured host-env allowlist.
func buildRegistry(cfg *Config) *flywheel.Registry {
	reg := flywheel.NewRegistry()
	flywheel.Register(reg, workers.ExecWorker{EnvAllowlist: cfg.Runtime.EnvAllowlist})
	flywheel.Register(reg, workers.ShellWorker{EnvAllowlist: cfg.Runtime.EnvAllowlist})
	flywheel.Register(reg, workers.PythonWorker{EnvAllowlist: cfg.Runtime.EnvAllowlist})
	flywheel.Register(reg, workers.MageWorker{EnvAllowlist: cfg.Runtime.EnvAllowlist})
	flywheel.Register(reg, workers.HTTPWorker{Doer: flywheel.DefaultHTTPDoer()})
	return reg
}

// scheduleSpecs turns the config's schedules into the periodic definitions they
// declare, each active. It is the one place the YAML becomes PeriodicSpecs, so
// what serve declares and what doctor compares against the database cannot
// drift apart.
func scheduleSpecs(cfg *Config) ([]flywheel.PeriodicSpec, error) {
	specs := make([]flywheel.PeriodicSpec, 0, len(cfg.Schedules))
	for i := range cfg.Schedules {
		s := cfg.Schedules[i]
		args, err := s.argsTemplate()
		if err != nil {
			return nil, fmt.Errorf("schedule %q: marshal args: %w", s.Slug, err)
		}
		specs = append(specs, flywheel.PeriodicSpec{
			Slug:         s.Slug,
			Kind:         s.workerKind(),
			Queue:        s.Queue,
			ArgsTemplate: args,
			Cron:         s.Cron,
			Every:        s.Every.Std(),
			Active:       true,
		})
	}
	return specs, nil
}

// disableOrphanSchedules makes flywheel.yaml the declarative source of truth for
// which schedules fire: it disables every active definition the config does not
// name. Removing a schedule from the file therefore stops it firing on the next
// serve, while preserving its row (and history) for inspection.
//
// It is the half of the reconcile the scheduler cannot do. Serve hands the
// declared schedules to the scheduler, which applies them on start — unchanged
// entries keep their cadence cursor — and re-creates any that go missing while
// it runs; what the scheduler cannot know is which rows the file no longer
// names, so serve disables those here, once, on start.
func disableOrphanSchedules(ctx context.Context, db *gorm.DB, cfg *Config) error {
	configured := make(map[string]bool, len(cfg.Schedules))
	for i := range cfg.Schedules {
		configured[cfg.Schedules[i].Slug] = true
	}
	existing, err := flywheel.ListPeriodics(ctx, db)
	if err != nil {
		return fmt.Errorf("reconcile schedules: %w", err)
	}
	for _, v := range existing {
		if v.Active && !configured[v.Slug] {
			if derr := flywheel.SetPeriodicActive(ctx, db, v.Slug, false); derr != nil {
				return fmt.Errorf("disable orphan schedule %q: %w", v.Slug, derr)
			}
		}
	}
	return nil
}

// declaresSchedule reports whether the config declares a schedule with slug.
func declaresSchedule(cfg *Config, slug string) bool {
	for i := range cfg.Schedules {
		if cfg.Schedules[i].Slug == slug {
			return true
		}
	}
	return false
}

// newLogger builds the structured logger the daemon and commands log through.
func newLogger(cfg *Config) *slog.Logger {
	opts := &slog.HandlerOptions{Level: parseLevel(cfg.Log.Level)}
	var handler slog.Handler
	if strings.EqualFold(cfg.Log.Format, "json") {
		handler = slog.NewJSONHandler(os.Stderr, opts)
	} else {
		handler = slog.NewTextHandler(os.Stderr, opts)
	}
	return slog.New(handler)
}

// parseLevel maps a level name to slog.Level, defaulting to info.
func parseLevel(name string) slog.Level {
	switch strings.ToLower(name) {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}
