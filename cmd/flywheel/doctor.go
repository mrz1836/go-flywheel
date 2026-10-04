package main

import (
	"context"
	"fmt"
	"io"
	"strings"
	"time"

	flywheel "github.com/mrz1836/go-flywheel"
	"github.com/mrz1836/go-foundation/models"
	"github.com/spf13/cobra"
	"gorm.io/gorm"
)

// newDoctorCmd builds `flywheel doctor`: validate the config, open and ping the
// database, bring the schema up to date (reporting what that changed), and print
// the effective settings, the index parity, and the stats rollup's lag — a
// one-shot health check before installing the daemon or after an upgrade.
func newDoctorCmd(configPath *string) *cobra.Command {
	return &cobra.Command{
		Use:   "doctor",
		Short: "Validate config, check and upgrade the database, and print effective settings",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfg, db, _, err := loadAndOpen(*configPath)
			if err != nil {
				return err
			}
			defer closeDB(db)
			return runDoctor(cmd.Context(), cmd.OutOrStdout(), *configPath, cfg, db)
		},
	}
}

// runDoctor performs the health check against an already-open db and writes the
// effective-settings report to out. It is the testable core of the doctor command,
// so its database-error branches can be driven with an injected handle.
//
// The schema is inspected before Migrate runs, so the report says what the
// upgrade added — the one place an operator sees, after deploying a new release,
// exactly which columns and tables it brought.
//
//nolint:gocyclo // a flat sequence of independent checks and report lines
func runDoctor(ctx context.Context, out io.Writer, configPath string, cfg *Config, db *gorm.DB) error {
	if err := pingDB(ctx, db); err != nil {
		return fmt.Errorf("database unreachable: %w", err)
	}
	gaps, err := flywheel.InspectSchema(ctx, db)
	if err != nil {
		return fmt.Errorf("schema inspection failed: %w", err)
	}
	if err := migrateOnStart(db); err != nil {
		return fmt.Errorf("schema check failed: %w", err)
	}
	indexDrift, err := flywheel.InspectIndexes(ctx, db)
	if err != nil {
		return fmt.Errorf("index check failed: %w", err)
	}

	_, _ = fmt.Fprintln(out, "flywheel doctor:")
	_, _ = fmt.Fprintf(out, "  config:       %s\n", configPath)
	_, _ = fmt.Fprintf(out, "  database:     %s (reachable, schema OK)\n", dbLabel(cfg))
	_, _ = fmt.Fprintf(out, "  schema:       %s\n", describeSchemaChange(gaps))
	_, _ = fmt.Fprintf(out, "  indexes:      %s\n", describeIndexDrift(indexDrift))
	if isSQLite(cfg) {
		_, _ = fmt.Fprintf(out, "  sqlite:       WAL, busy_timeout=5000ms, single writer\n")
	}
	_, _ = fmt.Fprintf(out, "  queues:       %v\n", cfg.Runtime.Queues)
	_, _ = fmt.Fprintf(out, "  concurrency:  %d\n", cfg.Runtime.Concurrency)
	_, _ = fmt.Fprintf(out, "  lease:        %s\n", cfg.Runtime.Lease.Std())
	_, _ = fmt.Fprintf(out, "  poll:         %s\n", cfg.Runtime.PollInterval.Std())
	_, _ = fmt.Fprintf(out, "  stats_rollup: %s\n", describeRollupSetting(cfg.Runtime.statsRollupInterval()))
	_, _ = fmt.Fprintf(out, "  schedules:    %d\n", len(cfg.Schedules))
	for i := range cfg.Schedules {
		s := cfg.Schedules[i]
		when := s.Cron
		if when == "" {
			when = "every " + s.Every.Std().String()
		}
		_, _ = fmt.Fprintf(out, "    - %-20s %-5s %s\n", s.Slug, s.Worker, when)
	}

	health, err := flywheel.SampleQueueHealth(ctx, db)
	if err != nil {
		return fmt.Errorf("queue health check failed: %w", err)
	}
	_, _ = fmt.Fprintf(out, "  queue:        ready=%d in-flight=%d lag=%s\n",
		health.Ready, health.InFlight, formatLag(health.OldestReadyAge))

	rollup, err := describeRollupLag(ctx, db)
	if err != nil {
		return fmt.Errorf("stats rollup check failed: %w", err)
	}
	_, _ = fmt.Fprintf(out, "  stats:        %s\n", rollup)
	_, _ = fmt.Fprintln(out, "  status:       OK")
	return nil
}

// describeSchemaChange renders what Migrate added to the schema: "up to date"
// when the inspection found nothing missing, else the tables and columns it
// brought.
func describeSchemaChange(gaps []flywheel.SchemaDrift) string {
	if len(gaps) == 0 {
		return "up to date"
	}
	names := make([]string, len(gaps))
	for i, g := range gaps {
		names[i] = g.String()
	}
	return "upgraded (added " + strings.Join(names, ", ") + ")"
}

// describeIndexDrift renders the post-migrate index parity: "in sync" when every
// runtime index is installed with the runtime's definition.
func describeIndexDrift(drift []flywheel.IndexDrift) string {
	if len(drift) == 0 {
		return "in sync"
	}
	names := make([]string, len(drift))
	for i, d := range drift {
		names[i] = d.Name
	}
	return fmt.Sprintf("%d drifted (%s)", len(drift), strings.Join(names, ", "))
}

// describeRollupSetting renders the effective runtime.stats_rollup cadence.
func describeRollupSetting(interval time.Duration) string {
	if interval <= 0 {
		return "off"
	}
	return "every " + interval.String() + " (in serve)"
}

// describeRollupLag reports how far the hourly stats rollup trails now. It reads
// the watermark through a one-hour Stats read with no raw cap, which returns it
// as Coverage.RolledThrough at the cost of one primary-key probe and a short raw
// aggregate.
func describeRollupLag(ctx context.Context, db *gorm.DB) (string, error) {
	now := models.ClockFrom(ctx).Now(ctx)
	res, err := flywheel.Stats(ctx, db, flywheel.StatsParams{From: now.Add(-time.Hour), To: now, MaxRawSpan: -1})
	if err != nil {
		return "", err
	}
	through := res.Coverage.RolledThrough
	if through.IsZero() {
		return "rollup not run yet", nil
	}
	lag := max(now.Sub(through), 0)
	return fmt.Sprintf("rolled up through %s (lag %s)", through.Format(time.RFC3339), lag.Round(time.Second)), nil
}

// pingDB verifies the database is reachable.
func pingDB(ctx context.Context, db *gorm.DB) error {
	sqlDB, err := db.DB()
	if err != nil {
		return err
	}
	return sqlDB.PingContext(ctx)
}
