package main

import (
	"fmt"
	"text/tabwriter"
	"time"

	flywheel "github.com/mrz1836/go-flywheel"
	"github.com/spf13/cobra"
)

// newKindsCmd builds `flywheel kinds`, which lists the kind catalog a host's
// processes published with PublishKinds.
func newKindsCmd(configPath *string) *cobra.Command {
	var asJSON bool
	cmd := &cobra.Command{
		Use:   "kinds",
		Short: "List the published kind catalog",
		Long: "List the job kinds a host's processes published with PublishKinds: each kind's default queue, " +
			"priority, attempts, and timeout, and when it was last published. The CLI reads the catalog; it " +
			"publishes none of its own.",
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			_, db, _, err := loadAndOpen(*configPath)
			if err != nil {
				return err
			}
			defer closeDB(db)

			records, err := flywheel.ListKinds(cmd.Context(), db)
			if err != nil {
				return err
			}
			if asJSON {
				return writeJSON(cmd.OutOrStdout(), records)
			}
			tw := tabwriter.NewWriter(cmd.OutOrStdout(), 0, 2, 2, ' ', 0)
			_, _ = fmt.Fprintln(tw, "KIND\tQUEUE\tPRIORITY\tATTEMPTS\tTIMEOUT\tPUBLISHED")
			for _, r := range records {
				timeout := "-"
				if r.TimeoutMs > 0 {
					timeout = (time.Duration(r.TimeoutMs) * time.Millisecond).String()
				}
				_, _ = fmt.Fprintf(tw, "%s\t%s\t%d\t%d\t%s\t%s\n",
					r.Kind, r.Queue, r.Priority, r.MaxAttempts, timeout, r.PublishedAt.Format("2006-01-02 15:04:05"))
			}
			return tw.Flush()
		},
	}
	cmd.Flags().BoolVar(&asJSON, "json", false, "emit JSON instead of a table")
	return cmd
}
