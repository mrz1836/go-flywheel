package main

import (
	"context"
	"fmt"
	"log/slog"
	"os/signal"
	"syscall"
	"time"

	flywheel "github.com/mrz1836/go-flywheel"
	"github.com/mrz1836/go-flywheel/observers"
	"github.com/spf13/cobra"
	"gorm.io/gorm"
)

// newServeCmd builds `flywheel serve`: the local daemon. It validates the config,
// migrates the schema, disables the schedules the config no longer names, and
// runs the runner + scheduler — which applies the config's schedules on start and
// re-creates any that go missing — until SIGINT/SIGTERM, draining in-flight work
// on the way out.
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

// runServe builds the Node, migrates, disables the schedules the config no longer
// names, and runs the Node until ctx is cancelled. It is the testable core of the
// serve command, taking an already-open db so its build, migrate, and
// orphan-disable error branches can be exercised with an injected handle.
//
// The Node is built first because building it is what validates the config —
// the declared schedules included — and it does no I/O: a config serve refuses
// leaves the database exactly as it was, with no DDL run and no row disabled.
// The Node's scheduler applies the declared schedules itself when it starts.
func runServe(ctx context.Context, cfg *Config, db *gorm.DB, driver flywheel.Driver) error {
	logger := newLogger(cfg)
	statsRollup, rollupOff := cfg.Runtime.effectiveStatsRollup()
	if rollupOff != "" {
		logger.Warn("flywheel serve: stats rollup disabled", "reason", rollupOff)
	}
	node, err := newServeNode(cfg, db, driver, logger, statsRollup)
	if err != nil {
		return err
	}
	if err := migrateOnStart(db); err != nil {
		return fmt.Errorf("migrate: %w", err)
	}
	if err := disableOrphanSchedules(ctx, db, cfg); err != nil {
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
		"stats_rollup", statsRollup.String(),
		"schedule_reconcile", cfg.Runtime.scheduleReconcileInterval().String(),
		"metrics_addr", cfg.Runtime.MetricsAddr)
	return node.Run(sigCtx)
}

// validateServeConfig reports whether serve would accept cfg, by building the
// Node it would run over db and driver and discarding it. Construction does no
// I/O, so it reads and writes nothing.
func validateServeConfig(cfg *Config, db *gorm.DB, driver flywheel.Driver) error {
	statsRollup, _ := cfg.Runtime.effectiveStatsRollup()
	_, err := newServeNode(cfg, db, driver, newLogger(cfg), statsRollup)
	return err
}

// newServeNode builds the daemon's Node from cfg: one runner over the
// configured queues and the scheduler, with the stats rollup at statsRollup and
// the config's schedules declared. Construction is where an invalid combination
// of settings — or a malformed schedule — is refused, so it is separate from
// runServe's migrate and run to be checked on its own, and doctor builds it too,
// to refuse what serve would. It does no I/O.
func newServeNode(
	cfg *Config, db *gorm.DB, driver flywheel.Driver, logger *slog.Logger, statsRollup time.Duration,
) (*flywheel.Node, error) {
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

	scheduler, err := serveSchedulerConfig(cfg, db, driver, logger, obs, statsRollup)
	if err != nil {
		return nil, err
	}

	return flywheel.NewNode(flywheel.NodeConfig{
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
		Scheduler: scheduler,
		Health:    health,
		Logger:    logger,
	})
}

// serveSchedulerConfig builds the daemon's scheduler configuration: the lease
// sweep through the runner's driver and observer, the configured retention and
// heartbeat, the stats rollup at statsRollup, and the config's schedules as the
// declared periodics, kept present on the runtime.schedule_reconcile cadence. It
// logs through the daemon's logger, so its lines honor log.level and log.format.
func serveSchedulerConfig(
	cfg *Config, db *gorm.DB, driver flywheel.Driver, logger *slog.Logger, obs flywheel.Observer,
	statsRollup time.Duration,
) (*flywheel.SchedulerConfig, error) {
	specs, err := scheduleSpecs(cfg)
	if err != nil {
		return nil, err
	}
	return &flywheel.SchedulerConfig{
		DB:     db,
		Client: flywheel.NewClient(db),
		// The runner's driver, not a second one built here: the scheduler's
		// lease sweep is a database operation like any other, and it belongs
		// behind whatever seam the rest of the process observes.
		Driver: driver,
		// The same observer the runners hold, so the sweep's duration lands in
		// the one recorder the /metrics endpoint renders.
		Observer:             obs,
		Logger:               logger,
		RetentionMaxAge:      cfg.Runtime.Retention.Std(),
		HealthSampleInterval: cfg.Runtime.HealthSampleInterval.Std(),
		// The hourly stats rollup is on by default for the daemon — it is what
		// `flywheel stats` reads past the last few hours — and so is the anomaly
		// log, which warns once when a kind slows down or starts failing.
		StatsRollupInterval: statsRollup,
		StatsAnomalyLog:     true,
		// The file is the source of truth: the scheduler applies these on start
		// and re-creates any whose row is deleted while serve runs. The interval
		// passes through as configured — unset selects the scheduler's one-minute
		// default, and a negative value turns the re-creation off.
		Periodics:         specs,
		ReconcileInterval: cfg.Runtime.ScheduleReconcile.Std(),
	}, nil
}
