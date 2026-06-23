package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"text/tabwriter"

	flywheel "github.com/mrz1836/go-flywheel"
	"github.com/spf13/cobra"
	"gorm.io/gorm"
)

// newJobsCmd builds the `flywheel jobs` command group: ls, inspect, retry, cancel.
func newJobsCmd(configPath *string) *cobra.Command {
	cmd := &cobra.Command{Use: "jobs", Short: "Inspect and operate jobs"}
	cmd.AddCommand(
		newJobsLsCmd(configPath),
		newJobsInspectCmd(configPath),
		newJobsRetryCmd(configPath),
		newJobsCancelCmd(configPath),
	)
	return cmd
}

// newJobsLsCmd lists recent jobs, optionally filtered by state and kind.
func newJobsLsCmd(configPath *string) *cobra.Command {
	var (
		state  string
		kind   string
		limit  int
		asJSON bool
	)
	cmd := &cobra.Command{
		Use:   "ls",
		Short: "List recent jobs",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			_, db, _, err := loadAndOpen(*configPath)
			if err != nil {
				return err
			}
			defer closeDB(db)

			views, err := flywheel.ListJobs(cmd.Context(), db, flywheel.ListJobsParams{
				State: state, Kind: kind, Limit: limit,
			})
			if err != nil {
				return err
			}
			if asJSON {
				return writeJSON(cmd.OutOrStdout(), views)
			}
			tw := tabwriter.NewWriter(cmd.OutOrStdout(), 0, 2, 2, ' ', 0)
			_, _ = fmt.Fprintln(tw, "ID\tKIND\tSTATE\tATTEMPT\tENQUEUED")
			for _, v := range views {
				_, _ = fmt.Fprintf(tw, "%s\t%s\t%s\t%d\t%s\n", v.ID, v.Kind, v.State, v.Attempt, v.EnqueuedAt.Format("2006-01-02 15:04:05"))
			}
			return tw.Flush()
		},
	}
	cmd.Flags().StringVar(&state, "state", "", "filter by state (available, running, succeeded, ...)")
	cmd.Flags().StringVar(&kind, "kind", "", "filter by job kind")
	cmd.Flags().IntVar(&limit, "limit", 50, "maximum rows")
	cmd.Flags().BoolVar(&asJSON, "json", false, "emit JSON instead of a table")
	return cmd
}

// newJobsInspectCmd shows a job and its run history.
func newJobsInspectCmd(configPath *string) *cobra.Command {
	var asJSON bool
	cmd := &cobra.Command{
		Use:   "inspect <job-id>",
		Short: "Show a job and its run history",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			_, db, _, err := loadAndOpen(*configPath)
			if err != nil {
				return err
			}
			defer closeDB(db)

			ctx := cmd.Context()
			job, err := flywheel.FindJob(ctx, db, args[0])
			if err != nil {
				return err
			}
			runs, err := flywheel.ListRuns(ctx, db, args[0], flywheel.ListRunsParams{Limit: 50})
			if err != nil {
				return err
			}
			if asJSON {
				return writeJSON(cmd.OutOrStdout(), map[string]any{"job": job, "runs": runs})
			}
			out := cmd.OutOrStdout()
			_, _ = fmt.Fprintf(out, "Job %s\n  kind:     %s\n  state:    %s\n  attempt:  %d\n  enqueued: %s\n",
				job.ID, job.Kind, job.State, job.Attempt, job.EnqueuedAt.Format("2006-01-02 15:04:05"))
			_, _ = fmt.Fprintf(out, "  runs:     %d\n", len(runs))
			tw := tabwriter.NewWriter(out, 0, 2, 2, ' ', 0)
			_, _ = fmt.Fprintln(tw, "  RUN ID\tOUTCOME\tEXECUTOR\tSTARTED")
			for _, r := range runs {
				_, _ = fmt.Fprintf(tw, "  %s\t%s\t%s\t%s\n", r.ID, r.Outcome, orDash(r.ExecutorClass), r.StartedAt.Format("15:04:05"))
			}
			if err := tw.Flush(); err != nil {
				return err
			}
			// Show the captured output and error for runs that recorded them — for the
			// exec/shell/python/mage workers this is the command's stdout/stderr and
			// exit code, the reason you ran it durably in the first place.
			for _, r := range runs {
				hasErr := r.Error != nil && *r.Error != ""
				if !hasErr && len(r.Output) == 0 {
					continue
				}
				_, _ = fmt.Fprintf(out, "\n  run %s (%s):\n", r.ID, r.Outcome)
				if hasErr {
					_, _ = fmt.Fprintf(out, "    error:  %s\n", *r.Error)
				}
				if len(r.Output) > 0 {
					_, _ = fmt.Fprintf(out, "    output: %s\n", string(r.Output))
				}
			}
			return nil
		},
	}
	cmd.Flags().BoolVar(&asJSON, "json", false, "emit JSON instead of text")
	return cmd
}

// newJobsRetryCmd forces a job back to available.
func newJobsRetryCmd(configPath *string) *cobra.Command {
	return &cobra.Command{
		Use:   "retry <job-id>",
		Short: "Force a job back to available so it is re-claimed",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return mutateJob(cmd, *configPath, args[0], flywheel.RetryJob, "retried")
		},
	}
}

// newJobsCancelCmd cancels a job.
func newJobsCancelCmd(configPath *string) *cobra.Command {
	return &cobra.Command{
		Use:   "cancel <job-id>",
		Short: "Move a job to the terminal cancelled state",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return mutateJob(cmd, *configPath, args[0], flywheel.CancelJob, "cancelled")
		},
	}
}

// mutateJob runs a job mutator (retry/cancel) and reports the result.
func mutateJob(cmd *cobra.Command, configPath, id string, mutate func(context.Context, *gorm.DB, string) error, verb string) error {
	_, db, _, err := loadAndOpen(configPath)
	if err != nil {
		return err
	}
	defer closeDB(db)
	if err := mutate(cmd.Context(), db, id); err != nil {
		return err
	}
	_, _ = fmt.Fprintf(cmd.OutOrStdout(), "%s %s\n", verb, id)
	return nil
}

// writeJSON encodes v as indented JSON to w.
func writeJSON(w io.Writer, v any) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}

// orDash returns s, or "-" when s is empty (the wildcard executor class).
func orDash(s string) string {
	if s == "" {
		return "-"
	}
	return s
}
