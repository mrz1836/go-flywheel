package main

import (
	"context"
	"fmt"
	"os/signal"
	"syscall"

	flywheel "github.com/mrz1836/go-flywheel"
	"github.com/mrz1836/go-flywheel/observers"
	"github.com/spf13/cobra"
	"gorm.io/gorm"
)

// newServeCmd builds `flywheel serve`: the local daemon. It migrates the schema,
// reconciles the declarative schedules, and runs the runner + scheduler until
// SIGINT/SIGTERM, draining in-flight work on the way out.
func newServeCmd(configPath *string) *cobra.Command {
	return &cobra.Command{
		Use:   "serve",
		Short: "Run the job runtime (runner + scheduler) until interrupted",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfg, db, driver, err := loadAndOpen(*configPath)
			if err != nil {
				return err
			}
			defer closeDB(db)
			return runServe(cmd.Context(), cfg, db, driver)
		},
	}
}

// runServe migrates, reconciles the declarative schedules, builds the Node, and
// runs it until ctx is cancelled. It is the testable core of the serve command,
// taking an already-open db so its migrate/reconcile/build error branches can be
// exercised with an injected handle.
func runServe(ctx context.Context, cfg *Config, db *gorm.DB, driver flywheel.Driver) error {
	logger := newLogger(cfg)
	if err := flywheel.Migrate(db); err != nil {
		return fmt.Errorf("migrate: %w", err)
	}
	if err := reconcileSchedules(ctx, db, cfg); err != nil {
		return err
	}

	// Wire telemetry by default: a process-lifetime metrics recorder behind a
	// fan-out observer (structured debug logs + counters), so every consumer
	// gets telemetry for free without hand-rolling and wiring an adapter.
	mem := observers.NewMemRecorder()
	obs := observers.NewMulti(observers.NewSlog(logger), observers.NewMetrics(mem))

	// The metrics HTTP server is opt-in: only with a configured address does
	// the Node expose /healthz, /readyz, and /metrics.
	health := flywheel.HealthConfig{}
	if cfg.Runtime.MetricsAddr != "" {
		health.Addr = cfg.Runtime.MetricsAddr
		health.MetricsHandler = observers.MetricsHandler(mem, func(ctx context.Context) (flywheel.QueueHealth, error) {
			return flywheel.SampleQueueHealth(ctx, db)
		})
	}

	node, err := flywheel.NewNode(flywheel.NodeConfig{
		Runners: []flywheel.RunnerConfig{{
			DB:               db,
			Driver:           driver,
			Registry:         buildRegistry(cfg),
			Queues:           cfg.Runtime.Queues,
			ClaimAnyClass:    true,
			Concurrency:      cfg.Runtime.Concurrency,
			LeaseDuration:    cfg.Runtime.Lease.Std(),
			PollInterval:     cfg.Runtime.PollInterval.Std(),
			RetryBackoffBase: cfg.Runtime.RetryBackoffBase.Std(),
			Logger:           logger,
			Observer:         obs,
		}},
		Scheduler: &flywheel.SchedulerConfig{
			DB:                   db,
			Client:               flywheel.NewClient(db),
			RetentionMaxAge:      cfg.Runtime.Retention.Std(),
			HealthSampleInterval: cfg.Runtime.HealthSampleInterval.Std(),
		},
		Health: health,
		Logger: logger,
	})
	if err != nil {
		return err
	}

	sigCtx, stop := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	logger.Info("flywheel serve: started",
		"version", resolveVersion(),
		"db", dbLabel(cfg),
		"queues", cfg.Runtime.Queues,
		"concurrency", cfg.Runtime.Concurrency,
		"schedules", len(cfg.Schedules),
		"retention", cfg.Runtime.Retention.Std().String(),
		"metrics_addr", cfg.Runtime.MetricsAddr)
	return node.Run(sigCtx)
}
