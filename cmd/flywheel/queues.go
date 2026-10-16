package main

import (
	"context"
	"fmt"
	"text/tabwriter"

	flywheel "github.com/mrz1836/go-flywheel"
	"github.com/spf13/cobra"
	"gorm.io/gorm"
)

// newQueuesCmd builds `flywheel queues`, which lists every queue with its depth
// and pause, and its pause and resume subcommands.
func newQueuesCmd(configPath *string) *cobra.Command {
	var asJSON bool
	cmd := &cobra.Command{
		Use:   "queues",
		Short: "List queues with their depth and pause; pause or resume one",
		Long: "List every queue that holds an unfinished job or was ever paused: its ready, scheduled-ahead,\n" +
			"running, and paused jobs, and whether the queue itself is paused, by whom, and why.",
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			_, db, _, err := loadAndOpen(*configPath)
			if err != nil {
				return err
			}
			defer closeDB(db)

			queues, err := flywheel.ListQueues(cmd.Context(), db)
			if err != nil {
				return err
			}
			if asJSON {
				return writeJSON(cmd.OutOrStdout(), queues)
			}
			tw := tabwriter.NewWriter(cmd.OutOrStdout(), 0, 2, 2, ' ', 0)
			_, _ = fmt.Fprintln(tw, "QUEUE\tREADY\tAHEAD\tRUNNING\tPAUSED-JOBS\tPAUSED\tBY\tREASON")
			for _, q := range queues {
				paused, by, reason := "-", "-", "-"
				if q.IsPaused {
					paused = "yes"
					if q.PausedBy != nil {
						by, reason = actorLabel(*q.PausedBy), orDash(q.PausedBy.Reason)
					}
				}
				_, _ = fmt.Fprintf(tw, "%s\t%d\t%d\t%d\t%d\t%s\t%s\t%s\n",
					q.Queue, q.Ready, q.ScheduledAhead, q.Running, q.Paused, paused, by, reason)
			}
			return tw.Flush()
		},
	}
	cmd.Flags().BoolVar(&asJSON, "json", false, "emit JSON instead of a table")
	cmd.AddCommand(newQueuesPauseCmd(configPath), newQueuesResumeCmd(configPath))
	return cmd
}

// newQueuesPauseCmd builds `flywheel queues pause <queue>`.
func newQueuesPauseCmd(configPath *string) *cobra.Command {
	var reason string
	cmd := &cobra.Command{
		Use:   "pause <queue>",
		Short: "Stop every runner from claiming a queue's jobs",
		Long: "Pause a queue for every runner in the fleet: each stops claiming its jobs within one\n" +
			"QueueStateRefresh (five seconds by default). Jobs already running finish, and the queue's\n" +
			"jobs keep their states. A queue no job has used yet can be paused. Pausing a paused queue\n" +
			"changes nothing.",
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return mutateQueue(cmd, *configPath, args[0], reason, flywheel.PauseQueue, "paused")
		},
	}
	addReasonFlag(cmd, &reason)
	return cmd
}

// newQueuesResumeCmd builds `flywheel queues resume <queue>`.
func newQueuesResumeCmd(configPath *string) *cobra.Command {
	var reason string
	cmd := &cobra.Command{
		Use:   "resume <queue>",
		Short: "Let every runner claim a paused queue's jobs again",
		Long: "Resume a paused queue: every runner claims its jobs again within one QueueStateRefresh.\n" +
			"Resuming a queue that is not paused changes nothing.",
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return mutateQueue(cmd, *configPath, args[0], reason, flywheel.ResumeQueue, "resumed")
		},
	}
	addReasonFlag(cmd, &reason)
	return cmd
}

// mutateQueue runs a queue control (pause, resume) on queue as the
// operating-system user, with reason, and reports the result, mirroring
// mutateJob.
func mutateQueue(
	cmd *cobra.Command, configPath, queue, reason string, mutate func(context.Context, *gorm.DB, string) error, verb string,
) error {
	_, db, _, err := loadAndOpen(configPath)
	if err != nil {
		return err
	}
	defer closeDB(db)
	if err := mutate(flywheel.WithActor(cmd.Context(), operatorActor(reason)), db, queue); err != nil {
		return err
	}
	_, _ = fmt.Fprintf(cmd.OutOrStdout(), "%s %s\n", verb, queue)
	return nil
}
