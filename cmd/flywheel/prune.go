package main

import (
	"fmt"
	"time"

	flywheel "github.com/mrz1836/go-flywheel"
	"github.com/spf13/cobra"
)

// defaultPruneAge is the cutoff `flywheel prune` uses when --older-than is unset.
const defaultPruneAge = "14d"

// newPruneCmd builds `flywheel prune`: hard-delete terminal jobs (and their run
// rows) finalized before the cutoff, reclaiming storage on a long-lived database.
// It is the one-shot complement to the daemon's optional retention sweep.
func newPruneCmd(configPath *string) *cobra.Command {
	var olderThan string
	cmd := &cobra.Command{
		Use:   "prune",
		Short: "Delete finished jobs (and their runs) older than a cutoff",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			age, err := parseHumanDuration(olderThan)
			if err != nil {
				return fmt.Errorf("invalid --older-than %q: %w", olderThan, err)
			}

			_, db, _, err := loadAndOpen(*configPath)
			if err != nil {
				return err
			}
			defer closeDB(db)

			cutoff := time.Now().Add(-age)
			deleted, err := flywheel.DeleteFinishedJobs(cmd.Context(), db, cutoff)
			if err != nil {
				return err
			}
			_, _ = fmt.Fprintf(cmd.OutOrStdout(), "pruned %d finished job(s) finalized before %s\n",
				deleted, cutoff.Format(time.RFC3339))
			return nil
		},
	}
	cmd.Flags().StringVar(&olderThan, "older-than", defaultPruneAge,
		"delete terminal jobs finalized before now minus this duration (e.g. 14d, 2w, 720h)")
	return cmd
}
