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

// workerKind returns the registered worker kind a schedule entry targets.
func (s ScheduleEntry) workerKind() string {
	if s.Worker == "http" {
		return workers.HTTPKind
	}
	return workers.ExecKind
}

// argsTemplate marshals the entry's exec/http spec into the JSON args template
// the worker decodes on each fire.
func (s ScheduleEntry) argsTemplate() ([]byte, error) {
	switch s.Worker {
	case "http":
		return json.Marshal(s.HTTP.toArgs())
	default:
		return json.Marshal(s.Exec.toArgs())
	}
}

// buildRegistry registers the generic exec and http workers so any schedule (or
// ad-hoc enqueue) of those kinds can run with no custom Go.
func buildRegistry(cfg *Config) *flywheel.Registry {
	reg := flywheel.NewRegistry()
	flywheel.Register(reg, workers.ExecWorker{EnvAllowlist: cfg.Runtime.EnvAllowlist})
	flywheel.Register(reg, workers.HTTPWorker{Doer: flywheel.DefaultHTTPDoer()})
	return reg
}

// reconcileSchedules upserts every flywheel.yaml schedule into job_periodics, so
// declaring a schedule in config is all it takes to run it durably. It is
// idempotent: unchanged entries keep their cadence cursor across restarts.
func reconcileSchedules(ctx context.Context, db *gorm.DB, cfg *Config) error {
	for i := range cfg.Schedules {
		s := cfg.Schedules[i]
		args, err := s.argsTemplate()
		if err != nil {
			return fmt.Errorf("schedule %q: marshal args: %w", s.Slug, err)
		}
		if err := flywheel.UpsertPeriodic(ctx, db, flywheel.PeriodicSpec{
			Slug:         s.Slug,
			Kind:         s.workerKind(),
			Queue:        s.Queue,
			ArgsTemplate: args,
			Cron:         s.Cron,
			Every:        s.Every.Std(),
			Active:       true,
		}); err != nil {
			return fmt.Errorf("schedule %q: %w", s.Slug, err)
		}
	}
	return nil
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
