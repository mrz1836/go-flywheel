package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"text/tabwriter"
	"time"

	flywheel "github.com/mrz1836/go-flywheel"
	"github.com/mrz1836/go-foundation/models"
	"github.com/spf13/cobra"
	"gorm.io/gorm"
)

// defaultStatsWindow is the window `flywheel stats` reports when --since is
// unset.
const defaultStatsWindow = "24h"

// newStatsCmd builds `flywheel stats`: per-kind attempt outcomes, job success
// rate, duration percentiles, and queue wait over a recent window, read from the
// hourly rollups plus the raw tail. `stats rebuild` repairs or backfills the
// rollups for a range.
func newStatsCmd(configPath *string) *cobra.Command {
	var (
		since  string
		kind   string
		queue  string
		asJSON bool
	)
	cmd := &cobra.Command{
		Use:   "stats",
		Short: "Show per-kind job statistics (outcomes, success rate, durations) over a recent window",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			window, err := parseHumanDuration(since)
			if err != nil {
				return fmt.Errorf("invalid --since %q: %w", since, err)
			}
			if window <= 0 {
				return fmt.Errorf("invalid --since %q: must be positive", since)
			}
			_, db, _, err := loadAndOpen(*configPath)
			if err != nil {
				return err
			}
			defer closeDB(db)

			ctx := cmd.Context()
			now := models.ClockFrom(ctx).Now(ctx)
			return runStats(ctx, cmd.OutOrStdout(), db, flywheel.StatsParams{
				From: now.Add(-window), To: now, Kind: kind, Queue: queue,
			}, asJSON)
		},
	}
	cmd.Flags().StringVar(&since, "since", defaultStatsWindow, "window to report, ending now (e.g. 1h, 24h, 7d, 30d)")
	cmd.Flags().StringVar(&kind, "kind", "", "restrict to one job kind")
	cmd.Flags().StringVar(&queue, "queue", "", "restrict to one queue")
	cmd.Flags().BoolVar(&asJSON, "json", false, "emit the full StatsResult as JSON")
	cmd.AddCommand(newStatsRebuildCmd(configPath))
	return cmd
}

// runStats reads the statistics for p through db and writes them to w, as JSON
// or a table. A window past what the rollups cover returns ErrStatsNotRolledUp
// unchanged: its message already names the fix.
func runStats(ctx context.Context, w io.Writer, db *gorm.DB, p flywheel.StatsParams, asJSON bool) error {
	res, err := flywheel.Stats(ctx, db, p)
	if err != nil {
		return err
	}
	if asJSON {
		return writeJSON(w, res)
	}
	return renderStatsText(w, res)
}

// renderStatsText writes a StatsResult as one table row per kind, a TOTAL row,
// and a line saying how the window was served.
func renderStatsText(w io.Writer, res flywheel.StatsResult) error {
	_, _ = fmt.Fprintf(w, "flywheel stats: %s → %s\n",
		res.From.Format(time.RFC3339), res.To.Format(time.RFC3339))
	tw := tabwriter.NewWriter(w, 0, 2, 2, ' ', 0)
	_, _ = fmt.Fprintln(tw, "KIND\tATTEMPTS\tSUCCEEDED\tDISCARDED\tRETRIES\tSUCCESS%\tP50\tP95\tP99\tMAX\tWAIT P95")
	for i := range res.Kinds {
		writeStatsRow(tw, res.Kinds[i].Kind, res.Kinds[i])
	}
	writeStatsRow(tw, "TOTAL", res.Total)
	if err := tw.Flush(); err != nil {
		return err
	}
	_, _ = fmt.Fprintln(w, describeCoverage(res.Coverage))
	return nil
}

// writeStatsRow writes one kind's (or the total's) row.
func writeStatsRow(w io.Writer, label string, k flywheel.KindStats) {
	_, _ = fmt.Fprintf(w, "%s\t%d\t%d\t%d\t%d\t%s\t%s\t%s\t%s\t%s\t%s\n",
		label, k.Attempts, k.Succeeded, k.Discarded, k.Retries, formatSuccessRate(k),
		summaryDuration(k.Duration, k.Duration.P50), summaryDuration(k.Duration, k.Duration.P95),
		summaryDuration(k.Duration, k.Duration.P99), summaryDuration(k.Duration, k.Duration.Max),
		summaryDuration(k.QueueWait, k.QueueWait.P95))
}

// describeCoverage renders how the window was served: from the rollups over
// the hours they cover and raw elsewhere, or raw throughout when nothing is
// rolled up yet.
func describeCoverage(c flywheel.StatsCoverage) string {
	if c.RolledThrough.IsZero() {
		return fmt.Sprintf("coverage: no hourly rollups yet; read %s of raw runs "+
			"(the rollup runs in `flywheel serve`, see runtime.stats_rollup)", formatSpan(c.RawSpan))
	}
	return fmt.Sprintf("coverage: rollups through %s (from %s); %s of raw runs",
		c.RolledThrough.Format(time.RFC3339), c.RolledFrom.Format(time.RFC3339), formatSpan(c.RawSpan))
}

// formatSuccessRate renders the job success rate as a percentage, or "-" when
// no job ended in the window.
func formatSuccessRate(k flywheel.KindStats) string {
	if k.Succeeded+k.Discarded == 0 {
		return "-"
	}
	return fmt.Sprintf("%.1f%%", k.SuccessRate*100)
}

// summaryDuration renders one figure of a distribution, or "-" when the
// distribution counted nothing.
func summaryDuration(s flywheel.DurationSummary, d time.Duration) string {
	if s.Count == 0 {
		return "-"
	}
	return formatStatDuration(d)
}

// formatStatDuration rounds a duration to a precision that reads well in a
// table: milliseconds below a second, centiseconds below a minute, seconds above.
func formatStatDuration(d time.Duration) string {
	switch {
	case d < time.Second:
		return d.Round(time.Millisecond).String()
	case d < time.Minute:
		return d.Round(10 * time.Millisecond).String()
	default:
		return d.Round(time.Second).String()
	}
}

// formatSpan renders a raw-read span, "none" when nothing was read raw.
func formatSpan(d time.Duration) string {
	if d <= 0 {
		return "none"
	}
	return d.Round(time.Second).String()
}

// newStatsRebuildCmd builds `flywheel stats rebuild`: recompute the hourly
// rollups for a range of closed hours from the raw runs.
func newStatsRebuildCmd(configPath *string) *cobra.Command {
	var (
		from  string
		to    string
		force bool
	)
	cmd := &cobra.Command{
		Use:   "rebuild",
		Short: "Recompute the hourly stats rollups for a range from the raw runs",
		Long: "Recompute the hourly stats rollups for every closed hour in [--from, --to) from the raw\n" +
			"job_runs, replacing what the rollup stored. Use it to backfill history without waiting for the\n" +
			"daemon's per-pass ceiling, or to repair hours whose runs landed after the rollup closed them.\n" +
			"The hours the rollups cover never have a gap: when the range does not touch them, the hours\n" +
			"in between are rolled too (an empty stretch costs one probe).\n" +
			"It first backfills the finish log for runs an older release finalized, so a rolling deploy's\n" +
			"stragglers are counted.\n" +
			"An hour whose raw runs retention has already pruned is refused unless --force is set.",
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			fromT, err := parseStatsTime(from)
			if err != nil {
				return fmt.Errorf("invalid --from: %w", err)
			}
			toT, err := parseStatsTime(to)
			if err != nil {
				return fmt.Errorf("invalid --to: %w", err)
			}
			if !fromT.Before(toT) {
				return fmt.Errorf("invalid range: --from %s must be before --to %s",
					fromT.Format(time.RFC3339), toT.Format(time.RFC3339))
			}
			_, db, _, err := loadAndOpen(*configPath)
			if err != nil {
				return err
			}
			defer closeDB(db)
			return runStatsRebuild(cmd.Context(), cmd.OutOrStdout(), db,
				flywheel.RebuildOpts{From: fromT, To: toT, Force: force})
		},
	}
	cmd.Flags().StringVar(&from, "from", "", "first hour to rebuild (RFC3339, or YYYY-MM-DD for UTC midnight)")
	cmd.Flags().StringVar(&to, "to", "", "end of the range, exclusive (RFC3339 or YYYY-MM-DD); clamped to the last closed hour")
	cmd.Flags().BoolVar(&force, "force", false, "rebuild hours even when retention has pruned some of their raw runs")
	_ = cmd.MarkFlagRequired("from")
	_ = cmd.MarkFlagRequired("to")
	return cmd
}

// runStatsRebuild rebuilds the rollups for opts through db and reports the
// clamped range and the work done.
//
// It backfills the finish log first, because the rebuild is the operator's
// repair tool: runs an older release finalized — during a rolling deploy that
// outlasted the daemon's own recheck — become visible to the recount here. The
// backfill reads all of job_runs once, which a manual repair can afford and the
// library's targeted RebuildStats does not pay.
func runStatsRebuild(ctx context.Context, w io.Writer, db *gorm.DB, opts flywheel.RebuildOpts) error {
	added, err := flywheel.BackfillRunFinishes(ctx, db)
	if err != nil {
		return err
	}
	if added > 0 {
		_, _ = fmt.Fprintf(w, "backfilled %d finish-log entr(ies) for runs an older release finalized\n", added)
	}
	res, err := flywheel.RebuildStats(ctx, db, opts)
	if err != nil {
		if errors.Is(err, flywheel.ErrValidation) && res.Hours > 0 {
			_, _ = fmt.Fprintf(w, "rebuilt %d hour(s) before stopping\n", res.Hours)
		}
		return err
	}
	_, _ = fmt.Fprintf(w, "rebuilt %d hour(s), %d group row(s), %s → %s\n",
		res.Hours, res.Groups, res.From.Format(time.RFC3339), res.To.Format(time.RFC3339))
	_, _ = fmt.Fprintf(w, "rollups now cover %s → %s\n",
		res.RolledFrom.Format(time.RFC3339), res.RolledThrough.Format(time.RFC3339))
	return nil
}

// parseStatsTime parses a range bound: an RFC3339 timestamp, or a bare
// YYYY-MM-DD date read as UTC midnight.
func parseStatsTime(s string) (time.Time, error) {
	if t, err := time.Parse(time.RFC3339, s); err == nil {
		return t, nil
	}
	t, err := time.Parse(time.DateOnly, s)
	if err != nil {
		return time.Time{}, fmt.Errorf("%q is not RFC3339 or YYYY-MM-DD", s)
	}
	return t, nil
}
