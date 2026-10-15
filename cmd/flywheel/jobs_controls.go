package main

import (
	"context"
	"fmt"
	"strconv"
	"time"

	flywheel "github.com/mrz1836/go-flywheel"
	"github.com/spf13/cobra"
	"gorm.io/gorm"
)

// newJobsRescheduleCmd moves a waiting job to another time through
// RescheduleJob.
func newJobsRescheduleCmd(configPath *string) *cobra.Command {
	var at, reason string
	cmd := &cobra.Command{
		Use:   "reschedule <job-id>",
		Short: "Move a waiting job to another time",
		Long: `A waiting job (available, retryable, scheduled, or paused) keeps its state
and runs at --at instead. A time in the past makes it due now. A running job
and a finished one are refused.`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			ts, err := time.Parse(time.RFC3339, at)
			if err != nil {
				return fmt.Errorf("invalid --at (want RFC3339): %w", err)
			}
			reschedule := func(ctx context.Context, db *gorm.DB, id string) error {
				return flywheel.RescheduleJob(ctx, db, id, ts)
			}
			return mutateJob(cmd, *configPath, args[0], reason, reschedule, "rescheduled")
		},
	}
	cmd.Flags().StringVar(&at, "at", "", "the RFC3339 time to run the job at")
	_ = cmd.MarkFlagRequired("at")
	addReasonFlag(cmd, &reason)
	return cmd
}

// newJobsPriorityCmd changes a waiting job's priority through SetJobPriority.
func newJobsPriorityCmd(configPath *string) *cobra.Command {
	var reason string
	cmd := &cobra.Command{
		Use:   "priority <job-id> <priority>",
		Short: "Change a waiting job's priority",
		Long: `A waiting job (available, retryable, scheduled, or paused) takes the
priority given, at least 1; lower runs first. A running job and a finished one
are refused.`,
		Args: cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			priority, err := strconv.Atoi(args[1])
			if err != nil {
				return fmt.Errorf("invalid priority %q (want an integer)", args[1])
			}
			reprioritize := func(ctx context.Context, db *gorm.DB, id string) error {
				return flywheel.SetJobPriority(ctx, db, id, priority)
			}
			return mutateJob(cmd, *configPath, args[0], reason, reprioritize, "reprioritized")
		},
	}
	addReasonFlag(cmd, &reason)
	return cmd
}

// newJobsPauseCmd holds one job through PauseJob.
func newJobsPauseCmd(configPath *string) *cobra.Command {
	var reason string
	cmd := &cobra.Command{
		Use:   "pause <job-id>",
		Short: "Hold a waiting job so no runner claims it",
		Long: `An available, retryable, or scheduled job is paused until resumed; a paused
job is left as it is. A running job and a finished one are refused.`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return mutateJob(cmd, *configPath, args[0], reason, flywheel.PauseJob, "paused")
		},
	}
	addReasonFlag(cmd, &reason)
	return cmd
}

// newJobsResumeCmd releases one paused job through ResumeJob.
func newJobsResumeCmd(configPath *string) *cobra.Command {
	var reason string
	cmd := &cobra.Command{
		Use:   "resume <job-id>",
		Short: "Return a paused job to available",
		Long: `A paused job returns to available and keeps its scheduled time; any other
waiting job is left as it is. A running job and a finished one are refused.`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return mutateJob(cmd, *configPath, args[0], reason, flywheel.ResumeJob, "resumed")
		},
	}
	addReasonFlag(cmd, &reason)
	return cmd
}

// newJobsDeleteCmd removes one finished job through DeleteJob.
func newJobsDeleteCmd(configPath *string) *cobra.Command {
	var reason string
	cmd := &cobra.Command{
		Use:   "delete <job-id>",
		Short: "Delete a finished job and its runs; its history stays",
		Long: `A succeeded, cancelled, or discarded job is deleted with its runs. Its
history stays, with the delete recorded in it. A job that hasn't finished is
refused: cancel it first.`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return mutateJob(cmd, *configPath, args[0], reason, flywheel.DeleteJob, "deleted")
		},
	}
	addReasonFlag(cmd, &reason)
	return cmd
}
