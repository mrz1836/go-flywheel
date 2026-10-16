package main

import (
	"context"
	"fmt"
	"io"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	flywheel "github.com/mrz1836/go-flywheel"
	"github.com/mrz1836/go-foundation/models"
	"github.com/spf13/cobra"
	"gorm.io/gorm"
)

// errorsMessageWidth is how many characters of a group's normalized message
// `flywheel errors` prints.
const errorsMessageWidth = 60

// newErrorsCmd builds `flywheel errors`: a recent window's failures grouped by
// kind and fingerprint, the most failures first, read from the hourly rollups
// plus the raw tail as `flywheel stats` is.
func newErrorsCmd(configPath *string) *cobra.Command {
	var (
		since       string
		kind        string
		fingerprint string
		limit       int
		hourly      bool
		asJSON      bool
	)
	cmd := &cobra.Command{
		Use:   "errors",
		Short: "List error groups (failures that share a kind and fingerprint) over a recent window",
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
			return runErrors(ctx, cmd.OutOrStdout(), db, flywheel.ErrorGroupsParams{
				From: now.Add(-window), To: now, Kind: kind, Fingerprint: fingerprint, Limit: limit, WithHourly: hourly,
			}, asJSON)
		},
	}
	cmd.Flags().StringVar(&since, "since", defaultStatsWindow, "window to report, ending now (e.g. 1h, 24h, 7d, 30d)")
	cmd.Flags().StringVar(&kind, "kind", "", "restrict to one job kind")
	cmd.Flags().StringVar(&fingerprint, "fingerprint", "", "show one group: its 16 hex characters")
	cmd.Flags().IntVar(&limit, "limit", 50, "maximum groups, the most failures first (at most 200)")
	cmd.Flags().BoolVar(&hourly, "hourly", false, "add each group's failures hour by hour (a window of at most 7d)")
	cmd.Flags().BoolVar(&asJSON, "json", false, "emit the full ErrorGroupsResult as JSON")
	return cmd
}

// runErrors reads the error groups for p through db and writes them to w, as
// JSON or a table. A window past what the rollups cover returns
// ErrStatsNotRolledUp unchanged: its message already names the fix.
func runErrors(ctx context.Context, w io.Writer, db *gorm.DB, p flywheel.ErrorGroupsParams, asJSON bool) error {
	res, err := flywheel.ErrorGroups(ctx, db, p)
	if err != nil {
		return err
	}
	if asJSON {
		return writeJSON(w, res)
	}
	return renderErrorsText(w, p, res)
}

// renderErrorsText writes the groups as one table row each, then a line saying
// how the window was served. With p.WithHourly, a last column holds each hour's
// failures, oldest first.
func renderErrorsText(w io.Writer, p flywheel.ErrorGroupsParams, res flywheel.ErrorGroupsResult) error {
	_, _ = fmt.Fprintf(w, "flywheel errors: %s → %s\n", p.From.Format(time.RFC3339), p.To.Format(time.RFC3339))
	tw := tabwriter.NewWriter(w, 0, 2, 2, ' ', 0)
	header := "FINGERPRINT\tKIND\tCLASS\tFAILURES\tRETRIED\tDISCARDED\tFIRST SEEN\tLAST SEEN\tMESSAGE"
	if p.WithHourly {
		header += "\tHOURLY"
	}
	_, _ = fmt.Fprintln(tw, header)
	for i := range res.Groups {
		g := res.Groups[i]
		row := fmt.Sprintf("%s\t%s\t%s\t%d\t%d\t%d\t%s\t%s\t%s", g.Fingerprint, g.Kind, orDash(g.ErrorClass),
			g.Failures, g.Retried, g.Discarded, g.FirstSeen.UTC().Format(time.RFC3339),
			g.LastSeen.UTC().Format(time.RFC3339), cutRunes(g.Message, errorsMessageWidth))
		if p.WithHourly {
			counts := make([]string, len(g.Hourly))
			for j, n := range g.Hourly {
				counts[j] = strconv.FormatInt(n, 10)
			}
			row += "\t" + strings.Join(counts, ",")
		}
		_, _ = fmt.Fprintln(tw, row)
	}
	if err := tw.Flush(); err != nil {
		return err
	}
	_, _ = fmt.Fprintln(w, describeCoverage(res.Coverage))
	return nil
}

// cutRunes returns s cut to its first n characters.
func cutRunes(s string, n int) string {
	r := []rune(s)
	if len(r) <= n {
		return s
	}
	return string(r[:n])
}
