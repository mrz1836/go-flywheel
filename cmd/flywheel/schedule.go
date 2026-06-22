package main

import (
	"encoding/json"
	"fmt"
	"text/tabwriter"
	"time"

	flywheel "github.com/mrz1836/go-flywheel"
	"github.com/spf13/cobra"
)

// newScheduleCmd builds the `flywheel schedule` group: ls, add.
func newScheduleCmd(configPath *string) *cobra.Command {
	cmd := &cobra.Command{Use: "schedule", Short: "Inspect and add periodic schedules"}
	cmd.AddCommand(newScheduleLsCmd(configPath), newScheduleAddCmd(configPath))
	return cmd
}

// newScheduleLsCmd lists the periodic definitions.
func newScheduleLsCmd(configPath *string) *cobra.Command {
	var asJSON bool
	cmd := &cobra.Command{
		Use:   "ls",
		Short: "List periodic schedules",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			_, db, _, err := loadAndOpen(*configPath)
			if err != nil {
				return err
			}
			defer closeDB(db)

			views, err := flywheel.ListPeriodics(cmd.Context(), db)
			if err != nil {
				return err
			}
			if asJSON {
				return writeJSON(cmd.OutOrStdout(), views)
			}
			tw := tabwriter.NewWriter(cmd.OutOrStdout(), 0, 2, 2, ' ', 0)
			fmt.Fprintln(tw, "SLUG\tKIND\tSCHEDULE\tACTIVE\tNEXT RUN")
			for _, v := range views {
				schedule := v.Cron
				if schedule == "" {
					schedule = fmt.Sprintf("every %ds", v.IntervalSeconds)
				}
				fmt.Fprintf(tw, "%s\t%s\t%s\t%t\t%s\n", v.Slug, v.Kind, schedule, v.Active, v.NextRunAt.Format("2006-01-02 15:04:05"))
			}
			return tw.Flush()
		},
	}
	cmd.Flags().BoolVar(&asJSON, "json", false, "emit JSON instead of a table")
	return cmd
}

// newScheduleAddCmd upserts a periodic schedule from the command line.
func newScheduleAddCmd(configPath *string) *cobra.Command {
	var (
		cron  string
		every time.Duration
		args  string
		queue string
	)
	cmd := &cobra.Command{
		Use:   "add <slug> <kind>",
		Short: "Add or update a periodic schedule",
		Long: "Add or update a periodic schedule for a worker kind. Provide exactly one of\n" +
			"--cron or --every. --args is the JSON args template fired on each run.",
		Args: cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, positional []string) error {
			slug, kind := positional[0], positional[1]
			template := []byte(args)
			if len(template) == 0 {
				template = []byte("{}")
			}
			if !json.Valid(template) {
				return fmt.Errorf("--args must be valid JSON")
			}

			_, db, _, err := loadAndOpen(*configPath)
			if err != nil {
				return err
			}
			defer closeDB(db)

			if err := flywheel.UpsertPeriodic(cmd.Context(), db, flywheel.PeriodicSpec{
				Slug:         slug,
				Kind:         kind,
				Queue:        queue,
				ArgsTemplate: template,
				Cron:         cron,
				Every:        every,
				Active:       true,
			}); err != nil {
				return err
			}
			fmt.Fprintf(cmd.OutOrStdout(), "scheduled %s (%s)\n", slug, kind)
			return nil
		},
	}
	cmd.Flags().StringVar(&cron, "cron", "", "standard 5-field cron expression")
	cmd.Flags().DurationVar(&every, "every", 0, "fixed interval (e.g. 5m, 24h)")
	cmd.Flags().StringVar(&args, "args", "{}", "JSON args template fired on each run")
	cmd.Flags().StringVar(&queue, "queue", "", "queue (default: periodic)")
	return cmd
}
