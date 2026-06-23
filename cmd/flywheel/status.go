package main

import (
	"context"
	"fmt"
	"io"
	"text/tabwriter"
	"time"

	flywheel "github.com/mrz1836/go-flywheel"
	"github.com/spf13/cobra"
	"gorm.io/gorm"
)

// statusFailureWindow bounds the "recent failures" section: discarded jobs
// finalized within this window of now are shown.
const statusFailureWindow = 24 * time.Hour

// statusMessageWidth caps a failure's error message in the text rendering so one
// failure stays on one line.
const statusMessageWidth = 80

// statusStateOrder is the canonical order job states are listed in, so the
// per-state breakdown is stable regardless of map iteration.
//
//nolint:gochecknoglobals // static, read-only display ordering
var statusStateOrder = []flywheel.JobState{
	flywheel.StateAvailable, flywheel.StateRunning, flywheel.StateRetryable,
	flywheel.StateScheduled, flywheel.StateSucceeded, flywheel.StateCancelled,
	flywheel.StateDiscarded,
}

// statusReport is the combined diagnostic snapshot `flywheel status` renders: the
// queue-health gauge, the by-state overview, the periodic schedules, and the
// recent failures. It is the struct emitted verbatim by --json.
type statusReport struct {
	Database  string                  `json:"database"`
	SampledAt time.Time               `json:"sampled_at"`
	Health    flywheel.QueueHealth    `json:"health"`
	Overview  flywheel.JobsOverview   `json:"overview"`
	Schedules []flywheel.PeriodicView `json:"schedules"`
	Failures  []flywheel.FailureView  `json:"recent_failures"`
}

// newStatusCmd builds `flywheel status`: a one-glance operator report of queue
// health (ready / in-flight / lag), per-state counts, active schedules, and the
// last day's failures — read straight from the database, so it works whether or
// not a daemon is running.
func newStatusCmd(configPath *string) *cobra.Command {
	var (
		asJSON   bool
		watch    bool
		interval time.Duration
	)
	cmd := &cobra.Command{
		Use:   "status",
		Short: "Show queue health, schedules, and recent failures",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfg, db, _, err := loadAndOpen(*configPath)
			if err != nil {
				return err
			}
			defer closeDB(db)

			if watch {
				return runStatusWatch(cmd, db, cfg, interval)
			}
			return renderStatusOnce(cmd.Context(), cmd.OutOrStdout(), db, cfg, asJSON)
		},
	}
	cmd.Flags().BoolVar(&asJSON, "json", false, "emit the combined status as JSON")
	cmd.Flags().BoolVar(&watch, "watch", false, "redraw the status on an interval until interrupted")
	cmd.Flags().DurationVar(&interval, "interval", 2*time.Second, "redraw cadence for --watch")
	return cmd
}

// runStatusWatch clears and redraws the text status every interval until the
// command's context is cancelled (Ctrl-C). A render error during cancellation is
// treated as a clean stop rather than a failure.
func runStatusWatch(cmd *cobra.Command, db *gorm.DB, cfg *Config, interval time.Duration) error {
	ctx := cmd.Context()
	out := cmd.OutOrStdout()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		// ANSI clear-screen + cursor-home so each redraw replaces the last frame.
		_, _ = fmt.Fprint(out, "\033[2J\033[H")
		if err := renderStatusOnce(ctx, out, db, cfg, false); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

// renderStatusOnce gathers and writes a single status frame, as JSON or text.
// "Now" comes from the context Clock so the gauge, the lag, and the failure
// window all agree, and a test drives them deterministically.
func renderStatusOnce(ctx context.Context, w io.Writer, db *gorm.DB, cfg *Config, asJSON bool) error {
	report, err := gatherStatus(ctx, db, cfg, flywheel.ClockFrom(ctx).Now(ctx))
	if err != nil {
		return err
	}
	if asJSON {
		return writeJSON(w, report)
	}
	return renderStatusText(w, report)
}

// gatherStatus reads every status input through db: the queue-health gauge, the
// by-state overview, the schedules, and the recent failures within the window
// ending at now.
func gatherStatus(ctx context.Context, db *gorm.DB, cfg *Config, now time.Time) (statusReport, error) {
	health, err := flywheel.SampleQueueHealth(ctx, db)
	if err != nil {
		return statusReport{}, err
	}
	overview, err := flywheel.Overview(ctx, db, flywheel.OverviewParams{})
	if err != nil {
		return statusReport{}, err
	}
	schedules, err := flywheel.ListPeriodics(ctx, db)
	if err != nil {
		return statusReport{}, err
	}
	failures, err := flywheel.RecentFailures(ctx, db, flywheel.RecentFailuresParams{Since: now.Add(-statusFailureWindow)})
	if err != nil {
		return statusReport{}, err
	}
	return statusReport{
		Database:  dbLabel(cfg),
		SampledAt: health.SampledAt,
		Health:    health,
		Overview:  overview,
		Schedules: schedules,
		Failures:  failures,
	}, nil
}

// renderStatusText writes the human-readable status report to w.
func renderStatusText(w io.Writer, r statusReport) error {
	_, _ = fmt.Fprintln(w, "flywheel status:")
	_, _ = fmt.Fprintf(w, "  database:     %s\n", r.Database)
	_, _ = fmt.Fprintf(w, "  ready:        %d\n", r.Health.Ready)
	_, _ = fmt.Fprintf(w, "  in-flight:    %d\n", r.Health.InFlight)
	_, _ = fmt.Fprintf(w, "  scheduled:    %d (not yet due)\n", r.Health.ScheduledAhead)
	_, _ = fmt.Fprintf(w, "  lag:          %s (oldest claimable job)\n", formatLag(r.Health.OldestReadyAge))
	_, _ = fmt.Fprintf(w, "  jobs:         %d total\n", r.Overview.Total)
	for _, st := range statusStateOrder {
		_, _ = fmt.Fprintf(w, "    %-11s %d\n", string(st), r.Overview.CountsByState[string(st)])
	}

	active := 0
	for i := range r.Schedules {
		if r.Schedules[i].Active {
			active++
		}
	}
	_, _ = fmt.Fprintf(w, "  schedules:    %d active / %d total\n", active, len(r.Schedules))
	if active > 0 {
		tw := tabwriter.NewWriter(w, 0, 2, 2, ' ', 0)
		for i := range r.Schedules {
			s := r.Schedules[i]
			if !s.Active {
				continue
			}
			_, _ = fmt.Fprintf(tw, "    - %s\t%s\t%s\tnext %s\n",
				s.Slug, s.Kind, scheduleWhen(s), s.NextRunAt.Format("2006-01-02 15:04:05"))
		}
		if err := tw.Flush(); err != nil {
			return err
		}
	}

	_, _ = fmt.Fprintf(w, "  recent failures (last 24h): %d\n", len(r.Failures))
	if len(r.Failures) > 0 {
		tw := tabwriter.NewWriter(w, 0, 2, 2, ' ', 0)
		for i := range r.Failures {
			f := r.Failures[i]
			_, _ = fmt.Fprintf(tw, "    - %s\t%s\t%s: %s\n",
				f.Kind, f.JobID, orDash(f.ErrorClass), truncateMessage(f.ErrorMessage))
		}
		if err := tw.Flush(); err != nil {
			return err
		}
	}
	return nil
}

// formatLag renders the oldest-ready lag, or "none" when nothing is waiting.
func formatLag(d time.Duration) string {
	if d <= 0 {
		return "none"
	}
	return d.Round(time.Second).String()
}

// scheduleWhen renders a schedule's cadence: its cron expression, or "every Ns".
func scheduleWhen(s flywheel.PeriodicView) string {
	if s.Cron != "" {
		return s.Cron
	}
	return "every " + (time.Duration(s.IntervalSeconds) * time.Second).String()
}

// truncateMessage caps an error message for single-line display.
func truncateMessage(s string) string {
	if len(s) <= statusMessageWidth {
		return s
	}
	return s[:statusMessageWidth-1] + "…"
}
