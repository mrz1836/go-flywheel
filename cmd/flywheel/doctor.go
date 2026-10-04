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

// newDoctorCmd builds `flywheel doctor`: validate the config as serve would, open
// and ping the database, bring the schema up to date (reporting what that changed), and print
// the effective settings, the index parity, how the declared schedules compare
// with the database, and the stats rollup's lag — a one-shot health check before
// installing the daemon or after an upgrade.
func newDoctorCmd(configPath *string) *cobra.Command {
	return &cobra.Command{
		Use:   "doctor",
		Short: "Validate config, check and upgrade the database, and print effective settings",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfg, db, driver, err := loadAndOpen(*configPath)
			if err != nil {
				return err
			}
			defer closeDB(db)
			return runDoctor(cmd.Context(), cmd.OutOrStdout(), *configPath, cfg, db, driver)
		},
	}
}

// runDoctor performs the health check against an already-open db and writes the
// effective-settings report to out. It is the testable core of the doctor command,
// so its database-error branches can be driven with an injected handle.
//
// It first validates the config exactly as serve does — by building the Node
// serve would run over db and driver, which does no I/O — so a config serve would
// refuse, a malformed schedule above all, fails the check before the database is
// touched, the way a config LoadConfig rejects does.
//
// The schema is inspected before Migrate runs, so the report says what the
// upgrade added — the one place an operator sees, after deploying a new release,
// exactly which columns and tables it brought.
//
// The schedules are compared with the database and their drift reported, but
// drift does not fail the check: serve repairs every kind of it on start, and an
// inactive declared schedule may be an operator's deliberate `schedule disable`.
// A fresh install, where doctor runs before serve, shows every schedule missing.
//
//nolint:gocyclo // a flat sequence of independent checks and report lines
func runDoctor(
	ctx context.Context, out io.Writer, configPath string, cfg *Config, db *gorm.DB, driver flywheel.Driver,
) error {
	if err := validateServeConfig(cfg, db, driver); err != nil {
		return fmt.Errorf("config: %w", err)
	}
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
	specs, err := scheduleSpecs(cfg)
	if err != nil {
		return fmt.Errorf("schedule check failed: %w", err)
	}
	periodics, err := flywheel.ListPeriodics(ctx, db)
	if err != nil {
		return fmt.Errorf("schedule check failed: %w", err)
	}
	schedules := compareSchedules(specs, periodics)

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
	_, _ = fmt.Fprintf(out, "  schedules:    %s\n",
		schedules.summary(len(specs), cfg.Runtime.scheduleReconcileInterval() <= 0))
	for i := range cfg.Schedules {
		s := cfg.Schedules[i]
		when := s.Cron
		if when == "" {
			when = "every " + s.Every.Std().String()
		}
		line := fmt.Sprintf("    - %-20s %-5s %s", s.Slug, s.Worker, when)
		if note := schedules.notes[i]; note != "" {
			line += "  " + note
		}
		_, _ = fmt.Fprintln(out, line)
	}
	if n := len(schedules.undeclared); n > 0 {
		pronoun := "it"
		if n > 1 {
			pronoun = "them"
		}
		_, _ = fmt.Fprintf(out, "  undeclared:   %d active (%s): serve disables %s on start\n",
			n, strings.Join(schedules.undeclared, ", "), pronoun)
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

// scheduleReport is doctor's comparison of the declared schedules with the
// job_periodics rows serve keeps in line with them.
type scheduleReport struct {
	// notes holds one entry per declared schedule, in config order: empty when
	// its row matches, else what is wrong and what serve does about it.
	notes []string
	// missing, inactive, and differs count the declared schedules in each state;
	// one schedule can be both inactive and different.
	missing, inactive, differs int
	// undeclared names the active rows the config does not declare, which serve
	// disables on start.
	undeclared []string
}

// compareSchedules compares the declared specs with the database's periodic
// rows. Each spec is checked for a missing, inactive, or differing row, and
// every active row no spec declares is listed as undeclared.
func compareSchedules(specs []flywheel.PeriodicSpec, rows []flywheel.PeriodicView) scheduleReport {
	bySlug := make(map[string]*flywheel.PeriodicView, len(rows))
	for i := range rows {
		bySlug[rows[i].Slug] = &rows[i]
	}
	report := scheduleReport{notes: make([]string, len(specs))}
	declared := make(map[string]bool, len(specs))
	for i, spec := range specs {
		declared[spec.Slug] = true
		row, ok := bySlug[spec.Slug]
		if !ok {
			report.missing++
			report.notes[i] = "[not in database: serve creates it]"
			continue
		}
		var notes []string
		if !row.Active {
			report.inactive++
			notes = append(notes, "[inactive: serve re-activates it on start]")
		}
		if fields := scheduleDrift(spec, row); len(fields) > 0 {
			report.differs++
			notes = append(notes, "[differs ("+strings.Join(fields, ", ")+"): serve updates it on start]")
		}
		report.notes[i] = strings.Join(notes, " ")
	}
	for i := range rows {
		if rows[i].Active && !declared[rows[i].Slug] {
			report.undeclared = append(report.undeclared, rows[i].Slug)
		}
	}
	return report
}

// summary renders the schedules line: the declared count first, then either
// "in sync" or how many are missing, inactive, or differ, and a note when serve
// will not re-create a schedule deleted while it runs.
func (r scheduleReport) summary(declared int, repairOff bool) string {
	var drift []string
	for _, c := range []struct {
		n    int
		what string
	}{{r.missing, "missing"}, {r.inactive, "inactive"}, {r.differs, "differs"}} {
		if c.n > 0 {
			drift = append(drift, fmt.Sprintf("%d %s", c.n, c.what))
		}
	}
	line := fmt.Sprintf("%d declared", declared)
	if len(drift) == 0 {
		line += ", in sync"
	} else {
		line += " (" + strings.Join(drift, ", ") + ")"
	}
	if repairOff {
		line += "; drift repair off"
	}
	return line
}

// defaultScheduleQueue is the queue a schedule whose config sets none lands on:
// PeriodicSpec documents an empty Queue as "periodic", and that is what serve's
// apply writes to the row.
const defaultScheduleQueue = "periodic"

// scheduleDrift names the fields in which a declared spec and its database row
// differ: the kind, the schedule (a cron expression, or an interval in whole
// seconds), and the queue — the default one when the config sets none, since
// that is what serve's apply writes. The args are not compared: PeriodicView
// does not carry them.
func scheduleDrift(spec flywheel.PeriodicSpec, row *flywheel.PeriodicView) []string {
	var fields []string
	if row.Kind != spec.Kind {
		fields = append(fields, "kind")
	}
	if spec.Every > 0 {
		if row.Cron != "" || row.IntervalSeconds != int(spec.Every.Seconds()) {
			fields = append(fields, "schedule")
		}
	} else if row.Cron != spec.Cron || row.IntervalSeconds != 0 {
		fields = append(fields, "schedule")
	}
	queue := spec.Queue
	if queue == "" {
		queue = defaultScheduleQueue
	}
	if row.Queue != queue {
		fields = append(fields, "queue")
	}
	return fields
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
