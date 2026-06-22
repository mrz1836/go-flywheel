package main

import (
	"fmt"
	"os/signal"
	"syscall"

	flywheel "github.com/mrz1836/go-flywheel"
	"github.com/spf13/cobra"
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

			logger := newLogger(cfg)
			ctx := cmd.Context()
			if err := flywheel.Migrate(db); err != nil {
				return fmt.Errorf("migrate: %w", err)
			}
			if err := reconcileSchedules(ctx, db, cfg); err != nil {
				return err
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
				}},
				Scheduler: &flywheel.SchedulerConfig{DB: db, Client: flywheel.NewClient(db)},
				Logger:    logger,
			})
			if err != nil {
				return err
			}

			sigCtx, stop := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
			defer stop()
			logger.Info("flywheel serve: started",
				"db", dbLabel(cfg),
				"queues", cfg.Runtime.Queues,
				"concurrency", cfg.Runtime.Concurrency,
				"schedules", len(cfg.Schedules))
			return node.Run(sigCtx)
		},
	}
}
