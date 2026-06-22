package main

import (
	"context"
	"fmt"

	flywheel "github.com/mrz1836/go-flywheel"
	"github.com/spf13/cobra"
	"gorm.io/gorm"
)

// newDoctorCmd builds `flywheel doctor`: validate the config, open and ping the
// database, confirm the schema, and print the effective settings — a one-shot
// health check before installing the daemon.
func newDoctorCmd(configPath *string) *cobra.Command {
	return &cobra.Command{
		Use:   "doctor",
		Short: "Validate config, check the database, and print effective settings",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			out := cmd.OutOrStdout()
			cfg, db, _, err := loadAndOpen(*configPath)
			if err != nil {
				return err
			}
			defer closeDB(db)

			ctx := cmd.Context()
			if err := pingDB(ctx, db); err != nil {
				return fmt.Errorf("database unreachable: %w", err)
			}
			if err := flywheel.Migrate(db); err != nil {
				return fmt.Errorf("schema check failed: %w", err)
			}

			_, _ = fmt.Fprintln(out, "flywheel doctor:")
			_, _ = fmt.Fprintf(out, "  config:       %s\n", *configPath)
			_, _ = fmt.Fprintf(out, "  database:     %s (reachable, schema OK)\n", dbLabel(cfg))
			if isSQLite(cfg) {
				_, _ = fmt.Fprintf(out, "  sqlite:       WAL, busy_timeout=5000ms, single writer\n")
			}
			_, _ = fmt.Fprintf(out, "  queues:       %v\n", cfg.Runtime.Queues)
			_, _ = fmt.Fprintf(out, "  concurrency:  %d\n", cfg.Runtime.Concurrency)
			_, _ = fmt.Fprintf(out, "  lease:        %s\n", cfg.Runtime.Lease.Std())
			_, _ = fmt.Fprintf(out, "  poll:         %s\n", cfg.Runtime.PollInterval.Std())
			_, _ = fmt.Fprintf(out, "  schedules:    %d\n", len(cfg.Schedules))
			for i := range cfg.Schedules {
				s := cfg.Schedules[i]
				when := s.Cron
				if when == "" {
					when = "every " + s.Every.Std().String()
				}
				_, _ = fmt.Fprintf(out, "    - %-20s %-5s %s\n", s.Slug, s.Worker, when)
			}
			_, _ = fmt.Fprintln(out, "  status:       OK")
			return nil
		},
	}
}

// pingDB verifies the database is reachable.
func pingDB(ctx context.Context, db *gorm.DB) error {
	sqlDB, err := db.DB()
	if err != nil {
		return err
	}
	return sqlDB.PingContext(ctx)
}
