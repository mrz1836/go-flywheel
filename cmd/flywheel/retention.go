package main

import (
	"fmt"
	"strconv"
	"text/tabwriter"
	"time"

	flywheel "github.com/mrz1836/go-flywheel"
	"github.com/spf13/cobra"
)

// newRetentionCmd builds `flywheel retention`: for each --rule and the default
// age, the cutoff a prune would use now, the oldest finished job it keeps, and
// how many jobs are due. It reads only, so it is how to see what a retention
// setting would delete before changing it.
func newRetentionCmd(configPath *string) *cobra.Command {
	var (
		olderThan string
		ruleSpecs []string
		asJSON    bool
	)
	cmd := &cobra.Command{
		Use:   "retention",
		Short: "Show what retention keeps and what is due, rule by rule",
		Long: "Print one row per --rule, in the order given, then the default's: the jobs it matches (KIND,\n" +
			"STATE; - matches any), how long it keeps them (KEEP), the cutoff a prune would use now (CUTOFF,\n" +
			"held back for the stats rollup once it has counted any hour), the oldest finished job it keeps\n" +
			"(OLDEST), and how many of its jobs finalized before the cutoff (DUE; 10000+ past ten thousand).\n" +
			"Each job counts once, under the most specific rule that matches it, as a prune decides it. The\n" +
			"default age is --older-than, else the config's runtime.retention, else 14d.",
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			rules, err := parseRetentionRules(ruleSpecs)
			if err != nil {
				return err
			}
			cfg, db, _, err := loadAndOpen(*configPath)
			if err != nil {
				return err
			}
			defer closeDB(db)

			age := cfg.Runtime.Retention.Std()
			if cmd.Flags().Changed("older-than") || age <= 0 {
				if age, err = parseHumanDuration(olderThan); err != nil {
					return fmt.Errorf("invalid --older-than %q: %w", olderThan, err)
				}
			}
			rows, err := flywheel.RetentionStatus(cmd.Context(), db, age, rules)
			if err != nil {
				return err
			}
			if asJSON {
				return writeJSON(cmd.OutOrStdout(), rows)
			}
			tw := tabwriter.NewWriter(cmd.OutOrStdout(), 0, 2, 2, ' ', 0)
			_, _ = fmt.Fprintln(tw, "KIND\tSTATE\tKEEP\tCUTOFF\tOLDEST\tDUE")
			for _, r := range rows {
				oldest := "-"
				if r.Oldest != nil {
					oldest = r.Oldest.UTC().Format(time.RFC3339)
				}
				_, _ = fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\t%s\n", orDash(r.Kind), orDash(string(r.State)),
					ageCell(r.MaxAge), r.Cutoff.UTC().Format(time.RFC3339), oldest, dueCell(r.Due))
			}
			return tw.Flush()
		},
	}
	cmd.Flags().StringVar(&olderThan, "older-than", defaultPruneAge,
		"the default age, for jobs no rule matches (e.g. 14d, 2w, 720h); else runtime.retention, else 14d")
	addRuleFlag(cmd, &ruleSpecs)
	cmd.Flags().BoolVar(&asJSON, "json", false, "emit the rows as JSON instead of a table")
	return cmd
}

// dueCell prints a due count, with a + when it is capped.
func dueCell(c flywheel.JobCount) string {
	if c.Capped {
		return strconv.FormatInt(c.N, 10) + "+"
	}
	return strconv.FormatInt(c.N, 10)
}
