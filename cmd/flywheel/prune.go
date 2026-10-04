package main

import (
	"fmt"
	"io"
	"time"

	flywheel "github.com/mrz1836/go-flywheel"
	"github.com/spf13/cobra"
)

// defaultPruneAge is the cutoff `flywheel prune` uses when --older-than is unset.
const defaultPruneAge = "14d"

// newPruneCmd builds `flywheel prune`: hard-delete terminal jobs (and their run
// rows) finalized before the cutoff, reclaiming storage on a long-lived database.
// It is the one-shot complement to the daemon's optional retention sweep.
//
// With the stats rollup on in the config (runtime.stats_rollup, on by default)
// it holds for the rollup exactly as the daemon's own retention does: no run the
// rollup has yet to count is deleted, so the hourly history `flywheel stats`
// reads is never left with a hole. --ignore-stats-rollup prunes past it.
func newPruneCmd(configPath *string) *cobra.Command {
	var (
		olderThan    string
		ignoreRollup bool
	)
	cmd := &cobra.Command{
		Use:   "prune",
		Short: "Delete finished jobs (and their runs) older than a cutoff",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			age, err := parseHumanDuration(olderThan)
			if err != nil {
				return fmt.Errorf("invalid --older-than %q: %w", olderThan, err)
			}

			cfg, db, _, err := loadAndOpen(*configPath)
			if err != nil {
				return err
			}
			defer closeDB(db)

			cutoff := time.Now().Add(-age)
			hold := cfg.Runtime.statsRollupInterval() > 0 && !ignoreRollup
			deleted, err := flywheel.DeleteFinishedJobsWithOptions(cmd.Context(), db, cutoff,
				flywheel.RetentionOpts{HoldForStatsRollup: hold})
			if err != nil {
				return err
			}
			reportPrune(cmd.OutOrStdout(), deleted, cutoff, hold)
			return nil
		},
	}
	cmd.Flags().StringVar(&olderThan, "older-than", defaultPruneAge,
		"delete terminal jobs finalized before now minus this duration (e.g. 14d, 2w, 720h)")
	cmd.Flags().BoolVar(&ignoreRollup, "ignore-stats-rollup", false,
		"prune runs the hourly stats rollup has not counted yet (their hours are then lost to `flywheel stats`)")
	return cmd
}

// reportPrune writes what a prune did, and — when it held for the stats rollup
// — that it may have stopped short of the cutoff, and why.
func reportPrune(w io.Writer, deleted int64, cutoff time.Time, held bool) {
	_, _ = fmt.Fprintf(w, "pruned %d finished job(s) finalized before %s\n", deleted, cutoff.Format(time.RFC3339))
	if held {
		_, _ = fmt.Fprintln(w, "held for the stats rollup: jobs whose runs it has not counted yet are kept "+
			"(--ignore-stats-rollup prunes them anyway)")
	}
}
