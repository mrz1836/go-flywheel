package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	flywheel "github.com/mrz1836/go-flywheel"
	"github.com/spf13/cobra"
	"gorm.io/gorm"
)

// newJobsCmd builds the `flywheel jobs` command group: ls, count, running,
// inspect, history, retry, cancel, reschedule, priority, pause, resume, delete.
func newJobsCmd(configPath *string) *cobra.Command {
	cmd := &cobra.Command{Use: "jobs", Short: "Inspect and operate jobs"}
	cmd.AddCommand(
		newJobsLsCmd(configPath),
		newJobsCountCmd(configPath),
		newJobsRunningCmd(configPath),
		newJobsInspectCmd(configPath),
		newJobsHistoryCmd(configPath),
		newJobsRetryCmd(configPath),
		newJobsCancelCmd(configPath),
		newJobsRescheduleCmd(configPath),
		newJobsPriorityCmd(configPath),
		newJobsPauseCmd(configPath),
		newJobsResumeCmd(configPath),
		newJobsDeleteCmd(configPath),
	)
	return cmd
}

// jobFilterFlags are the filter flags `jobs ls` and `jobs count` share. A
// repeated flag matches any of its values; different flags must all match.
type jobFilterFlags struct {
	states, kinds, queues, tags []string
	createdAfter, createdBefore string
}

// register adds the filter flags to cmd.
func (f *jobFilterFlags) register(cmd *cobra.Command) {
	cmd.Flags().StringArrayVar(&f.states, "state", nil,
		"filter by state (available, running, succeeded, ...); repeat for any of several")
	cmd.Flags().StringArrayVar(&f.kinds, "kind", nil, "filter by job kind; repeat for any of several")
	cmd.Flags().StringArrayVar(&f.queues, "queue", nil, "filter by queue; repeat for any of several")
	cmd.Flags().StringArrayVar(&f.tags, "tag", nil, "filter by tag: a job carrying any of the repeated tags")
	cmd.Flags().StringVar(&f.createdAfter, "created-after", "", "only jobs created at or after this RFC 3339 time")
	cmd.Flags().StringVar(&f.createdBefore, "created-before", "", "only jobs created before this RFC 3339 time")
}

// query builds the JobQuery the flags describe. The library validates it; a
// refusal prints as it comes.
func (f *jobFilterFlags) query() (flywheel.JobQuery, error) {
	q := flywheel.JobQuery{Kinds: f.kinds, Queues: f.queues, Tags: f.tags}
	for _, s := range f.states {
		q.States = append(q.States, flywheel.JobState(s))
	}
	var err error
	if q.CreatedFrom, err = parseCreatedFlag("--created-after", f.createdAfter); err != nil {
		return flywheel.JobQuery{}, err
	}
	if q.CreatedTo, err = parseCreatedFlag("--created-before", f.createdBefore); err != nil {
		return flywheel.JobQuery{}, err
	}
	return q, nil
}

// parseCreatedFlag parses a created-window flag's RFC 3339 value; empty is an
// open end.
func parseCreatedFlag(name, value string) (time.Time, error) {
	if value == "" {
		return time.Time{}, nil
	}
	t, err := time.Parse(time.RFC3339, value)
	if err != nil {
		return time.Time{}, fmt.Errorf("%s wants an RFC 3339 time such as 2026-06-22T00:00:00Z: %w", name, err)
	}
	return t, nil
}

// newJobsLsCmd lists jobs through QueryJobs: filtered by the shared flags,
// ordered by --sort, and paged with the cursor each page prints under its table.
func newJobsLsCmd(configPath *string) *cobra.Command {
	var (
		filter jobFilterFlags
		sortBy string
		cursor string
		limit  int
		asJSON bool
	)
	cmd := &cobra.Command{
		Use:   "ls",
		Short: "List jobs, filtered and paged",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			q, err := filter.query()
			if err != nil {
				return err
			}
			q.Sort, q.Cursor, q.Limit = flywheel.JobSort(sortBy), cursor, limit

			_, db, _, err := loadAndOpen(*configPath)
			if err != nil {
				return err
			}
			defer closeDB(db)

			page, err := flywheel.QueryJobs(cmd.Context(), db, q)
			if err != nil {
				return err
			}
			if asJSON {
				return writeJSON(cmd.OutOrStdout(), page)
			}
			tw := tabwriter.NewWriter(cmd.OutOrStdout(), 0, 2, 2, ' ', 0)
			_, _ = fmt.Fprintln(tw, "ID\tKIND\tQUEUE\tSTATE\tATTEMPT\tENQUEUED")
			for _, v := range page.Items {
				_, _ = fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%d\t%s\n",
					v.ID, v.Kind, v.Queue, v.State, v.Attempt, v.EnqueuedAt.Format("2006-01-02 15:04:05"))
			}
			if err := tw.Flush(); err != nil {
				return err
			}
			if page.NextCursor != "" {
				_, _ = fmt.Fprintf(cmd.OutOrStdout(), "next: --cursor %s\n", page.NextCursor)
			}
			return nil
		},
	}
	filter.register(cmd)
	cmd.Flags().StringVar(&sortBy, "sort", string(flywheel.JobSortNewest),
		"order: newest, oldest, finished (terminal --state only), or scheduled (waiting --state only)")
	cmd.Flags().StringVar(&cursor, "cursor", "", "read the next page: the cursor printed under the previous one, with the same --sort")
	cmd.Flags().IntVar(&limit, "limit", 50, "maximum rows (at most 200)")
	cmd.Flags().BoolVar(&asJSON, "json", false, "emit the page as JSON (items, next_cursor) instead of a table")
	return cmd
}

// newJobsCountCmd counts the jobs the shared filter flags match through
// CountJobsWithOptions: exactly up to --cap, then the cap and, on PostgreSQL,
// the planner's estimate.
func newJobsCountCmd(configPath *string) *cobra.Command {
	var (
		filter   jobFilterFlags
		countCap int64
		asJSON   bool
	)
	cmd := &cobra.Command{
		Use:   "count",
		Short: "Count the jobs a filter matches, exactly up to a cap",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			q, err := filter.query()
			if err != nil {
				return err
			}
			_, db, _, err := loadAndOpen(*configPath)
			if err != nil {
				return err
			}
			defer closeDB(db)

			n, err := flywheel.CountJobsWithOptions(cmd.Context(), db, q, flywheel.CountOpts{Cap: countCap})
			if err != nil {
				return err
			}
			if asJSON {
				return writeJSON(cmd.OutOrStdout(), n)
			}
			_, err = fmt.Fprintln(cmd.OutOrStdout(), formatJobCount(n))
			return err
		},
	}
	filter.register(cmd)
	cmd.Flags().Int64Var(&countCap, "cap", 0, "count exactly up to this many jobs (0 selects 10,000; at most 100,000)")
	cmd.Flags().BoolVar(&asJSON, "json", false, "emit the count as JSON (n, capped, estimate)")
	return cmd
}

// newJobsRunningCmd lists the jobs running right now, longest-running first,
// each compared against its kind's baseline from the stats rollups.
func newJobsRunningCmd(configPath *string) *cobra.Command {
	var (
		kind   string
		queue  string
		limit  int
		asJSON bool
	)
	cmd := &cobra.Command{
		Use:   "running",
		Short: "List running jobs, longest-running first, flagging any far slower than their kind's baseline",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			_, db, _, err := loadAndOpen(*configPath)
			if err != nil {
				return err
			}
			defer closeDB(db)
			return runJobsRunning(cmd.Context(), cmd.OutOrStdout(), db, flywheel.ListRunningParams{
				Kind: kind, Queue: queue, Limit: limit, WithBaseline: true,
			}, asJSON)
		},
	}
	cmd.Flags().StringVar(&kind, "kind", "", "filter by job kind")
	cmd.Flags().StringVar(&queue, "queue", "", "filter by queue")
	cmd.Flags().IntVar(&limit, "limit", 100, "maximum rows")
	cmd.Flags().BoolVar(&asJSON, "json", false, "emit JSON instead of a table")
	return cmd
}

// runJobsRunning reads the running jobs through db and writes them to w, as JSON
// or a table. It is the testable core of `jobs running`.
func runJobsRunning(ctx context.Context, w io.Writer, db *gorm.DB, p flywheel.ListRunningParams, asJSON bool) error {
	running, err := flywheel.ListRunning(ctx, db, p)
	if err != nil {
		return err
	}
	if asJSON {
		return writeJSON(w, running)
	}
	tw := tabwriter.NewWriter(w, 0, 2, 2, ' ', 0)
	_, _ = fmt.Fprintln(tw, "JOB ID\tKIND\tQUEUE\tATTEMPT\tELAPSED\tLEASE\tBASELINE P99\tSLOW")
	for i := range running {
		r := running[i]
		elapsed := "starting"
		if !r.Starting {
			elapsed = formatStatDuration(r.Elapsed)
		}
		lease := "ok"
		switch {
		case r.LeasedUntil == nil:
			lease = "-"
		case r.LeaseExpired:
			lease = "expired"
		}
		baseline := "-"
		if r.BaselineP99 != nil {
			baseline = formatStatDuration(*r.BaselineP99)
		}
		slow := ""
		if r.Slow {
			slow = "SLOW"
		}
		_, _ = fmt.Fprintf(tw, "%s\t%s\t%s\t%d/%d\t%s\t%s\t%s\t%s\n",
			r.JobID, r.Kind, r.Queue, r.Attempt, r.MaxAttempts, elapsed, lease, baseline, slow)
	}
	return tw.Flush()
}

// newJobsInspectCmd shows a job in full and its run history, its args bounded
// by --max-args-bytes.
func newJobsInspectCmd(configPath *string) *cobra.Command {
	var (
		maxArgsBytes int
		asJSON       bool
	)
	cmd := &cobra.Command{
		Use:   "inspect <job-id>",
		Short: "Show a job in full and its run history",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			_, db, _, err := loadAndOpen(*configPath)
			if err != nil {
				return err
			}
			defer closeDB(db)

			ctx := cmd.Context()
			job, err := flywheel.GetJobWithOptions(ctx, db, args[0], flywheel.GetJobOpts{MaxArgsBytes: maxArgsBytes})
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
			return writeJobDetail(cmd.OutOrStdout(), job, runs)
		},
	}
	cmd.Flags().IntVar(&maxArgsBytes, "max-args-bytes", 64<<10,
		"print args up to this many bytes, and only their size past it (0 selects 1 MiB; negative prints any size)")
	cmd.Flags().BoolVar(&asJSON, "json", false, "emit JSON instead of text")
	return cmd
}

// newJobsHistoryCmd prints a job's control history through ListAudit, newest
// first: who retried, cancelled, paused, resumed, or replayed it, and why.
func newJobsHistoryCmd(configPath *string) *cobra.Command {
	var (
		cursor string
		limit  int
		asJSON bool
	)
	cmd := &cobra.Command{
		Use:   "history <job-id>",
		Short: "Show who changed a job, and why, newest first",
		Long: `Print the job's control history, newest first: one line per change a
control made to it, with when it ran (UTC), what it did, who ran it, through
which source, and the reason they gave. The history outlives the job.`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			_, db, _, err := loadAndOpen(*configPath)
			if err != nil {
				return err
			}
			defer closeDB(db)

			page, err := flywheel.ListAudit(cmd.Context(), db, flywheel.ListAuditParams{
				TargetType: flywheel.AuditTargetJob, TargetID: args[0], Cursor: cursor, Limit: limit,
			})
			if err != nil {
				return err
			}
			if asJSON {
				return writeJSON(cmd.OutOrStdout(), page)
			}
			tw := tabwriter.NewWriter(cmd.OutOrStdout(), 0, 2, 2, ' ', 0)
			_, _ = fmt.Fprintln(tw, "WHEN\tACTION\tACTOR\tSOURCE\tREASON")
			for _, e := range page.Entries {
				_, _ = fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\n", e.CreatedAt.UTC().Format(time.RFC3339), e.Action,
					actorLabel(e.Actor), orDash(e.Actor.Source), orDash(e.Actor.Reason))
			}
			if err := tw.Flush(); err != nil {
				return err
			}
			if page.NextCursor != "" {
				_, _ = fmt.Fprintf(cmd.OutOrStdout(), "next: --cursor %s\n", page.NextCursor)
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&cursor, "cursor", "", "read the next page: the cursor printed under the previous one")
	cmd.Flags().IntVar(&limit, "limit", 50, "maximum entries (at most 200)")
	cmd.Flags().BoolVar(&asJSON, "json", false, "emit the page as JSON (entries, next_cursor) instead of a table")
	return cmd
}

// actorLabel renders an actor as history prints it: <type>:<id>, or the type
// alone when the actor has no id, as the system has none.
func actorLabel(a flywheel.Actor) string {
	if a.ID == "" {
		return string(a.Type)
	}
	return string(a.Type) + ":" + a.ID
}

// writeJobDetail writes a job's detail and its runs as text: the job's fields,
// those it does not carry left out, then its metadata and args as stored, then
// the run count, the run table, and each run's captured error and output.
func writeJobDetail(out io.Writer, job flywheel.JobDetail, runs []flywheel.JobRunView) error {
	const stamp = "2006-01-02 15:04:05"
	_, _ = fmt.Fprintf(out, "Job %s\n", job.ID)
	field := func(label, value string) { _, _ = fmt.Fprintf(out, "  %-9s %s\n", label+":", value) }
	field("kind", job.Kind)
	field("queue", job.Queue)
	state := job.State
	if job.CancelPending {
		state += " (cancel pending)"
	}
	field("state", state)
	field("attempt", fmt.Sprintf("%d/%d", job.Attempt, job.MaxAttempts))
	field("priority", strconv.Itoa(job.Priority))
	field("enqueued", job.EnqueuedAt.Format(stamp))
	field("scheduled", job.ScheduledAt.Format(stamp))
	if job.FinalizedAt != nil {
		field("finalized", job.FinalizedAt.Format(stamp))
	}
	if len(job.Tags) > 0 {
		field("tags", strings.Join(job.Tags, ", "))
	}
	if job.UniqueKey != "" {
		field("unique key", job.UniqueKey)
	}
	if job.UniqueActiveKey != "" {
		field("unique active key", job.UniqueActiveKey)
	}
	if job.TimeoutMs != nil {
		field("timeout", (time.Duration(*job.TimeoutMs) * time.Millisecond).String())
	}
	if job.LeasedUntil != nil {
		field("leased until", job.LeasedUntil.Format(stamp))
	}
	if job.Barrier != nil {
		field("barrier", job.Barrier.Kind+" on "+job.Barrier.Queue)
	}
	// The payloads, as stored, each on a line of its own.
	_, _ = fmt.Fprintf(out, "  metadata: %s\n", job.Metadata)
	if job.ArgsOmitted {
		_, _ = fmt.Fprintf(out, "  args: omitted (%d bytes; raise --max-args-bytes to show)\n", job.ArgsBytes)
	} else {
		_, _ = fmt.Fprintf(out, "  args: %s\n", job.Args)
	}
	field("runs", strconv.Itoa(len(runs)))

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
}

// newJobsRetryCmd returns a job to available through RetryJobWithOptions. With
// no flag it retries only a job that is still waiting; --force admits a terminal
// job, and --allow-running a running one.
func newJobsRetryCmd(configPath *string) *cobra.Command {
	var (
		force, allowRunning bool
		reason              string
	)
	cmd := &cobra.Command{
		Use:   "retry <job-id>",
		Short: "Return a job to available so it runs again",
		Long: `A waiting, failed, or paused job returns to available. A terminal job
(succeeded, cancelled, discarded) needs --force. A cancelled job whose attempt
still runs is refused until that attempt returns. A running job is refused:
cancel it first, or pass --allow-running to re-queue it while its current
attempt keeps running; that attempt's result is then discarded, and the work
runs twice.`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			opts := flywheel.RetryOpts{Force: force, AllowRunning: allowRunning}
			retry := func(ctx context.Context, db *gorm.DB, id string) error {
				return flywheel.RetryJobWithOptions(ctx, db, id, opts)
			}
			return mutateJob(cmd, *configPath, args[0], reason, retry, "retried")
		},
	}
	cmd.Flags().BoolVar(&force, "force", false, "retry a job that reached a terminal state")
	cmd.Flags().BoolVar(&allowRunning, "allow-running", false,
		"re-queue a running job; its current attempt keeps running and its result is discarded")
	addReasonFlag(cmd, &reason)
	return cmd
}

// newJobsCancelCmd cancels a job.
func newJobsCancelCmd(configPath *string) *cobra.Command {
	var reason string
	cmd := &cobra.Command{
		Use:   "cancel <job-id>",
		Short: "Move a job to the terminal cancelled state",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return mutateJob(cmd, *configPath, args[0], reason, flywheel.CancelJob, "cancelled")
		},
	}
	addReasonFlag(cmd, &reason)
	return cmd
}

// addReasonFlag adds --reason to a control command.
func addReasonFlag(cmd *cobra.Command, reason *string) {
	cmd.Flags().StringVar(reason, "reason", "", "why you are running it; recorded in the history")
}

// mutateJob runs a job control (retry, cancel, reschedule, priority, pause,
// resume, delete) as the operating-system user, with reason, and reports the
// result.
func mutateJob(
	cmd *cobra.Command, configPath, id, reason string, mutate func(context.Context, *gorm.DB, string) error, verb string,
) error {
	_, db, _, err := loadAndOpen(configPath)
	if err != nil {
		return err
	}
	defer closeDB(db)
	if err := mutate(flywheel.WithActor(cmd.Context(), operatorActor(reason)), db, id); err != nil {
		return err
	}
	_, _ = fmt.Fprintf(cmd.OutOrStdout(), "%s %s\n", verb, id)
	return nil
}

// formatJobCount renders a count as `jobs count` prints it: the exact count,
// or the cap with a plus past it, and the estimate when the database gave one.
func formatJobCount(c flywheel.JobCount) string {
	switch {
	case !c.Capped:
		return strconv.FormatInt(c.N, 10)
	case c.Estimate > 0:
		return fmt.Sprintf("%d+ (estimate %d)", c.N, c.Estimate)
	default:
		return fmt.Sprintf("%d+", c.N)
	}
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
