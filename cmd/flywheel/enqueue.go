package main

import (
	"encoding/json"
	"fmt"
	"time"

	flywheel "github.com/mrz1836/go-flywheel"
	"github.com/spf13/cobra"
)

// newEnqueueCmd builds `flywheel enqueue <kind> <json-args>`: enqueue one job of
// an arbitrary kind from a JSON payload. It is the seam for ad-hoc work and for
// triggering an exec/http job by hand (e.g. `enqueue exec '{"command":"..."}'`).
func newEnqueueCmd(configPath *string) *cobra.Command {
	var (
		queue    string
		unique   string
		at       string
		priority int
	)
	cmd := &cobra.Command{
		Use:   "enqueue <kind> <json-args>",
		Short: "Enqueue a single job from a JSON payload",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			kind, payload := args[0], []byte(args[1])
			if !json.Valid(payload) {
				return fmt.Errorf("the args argument must be valid JSON")
			}

			_, db, _, err := loadAndOpen(*configPath)
			if err != nil {
				return err
			}
			defer closeDB(db)

			opts := flywheel.InsertOpts{Queue: queue, UniqueKey: unique, Priority: priority}
			if at != "" {
				ts, perr := time.Parse(time.RFC3339, at)
				if perr != nil {
					return fmt.Errorf("invalid --at (want RFC3339): %w", perr)
				}
				opts.ScheduleAt = &ts
			}

			id, err := flywheel.Enqueue(cmd.Context(), flywheel.NewClient(db), kind, payload, opts)
			if err != nil {
				return err
			}
			fmt.Fprintln(cmd.OutOrStdout(), id)
			return nil
		},
	}
	cmd.Flags().StringVar(&queue, "queue", "", "queue to enqueue on (default: default)")
	cmd.Flags().StringVar(&unique, "unique", "", "unique key for an idempotent enqueue")
	cmd.Flags().IntVar(&priority, "priority", 0, "priority; lower runs first")
	cmd.Flags().StringVar(&at, "at", "", "schedule for a future RFC3339 time")
	return cmd
}
