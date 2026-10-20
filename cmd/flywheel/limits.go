package main

import (
	"errors"
	"fmt"
	"strconv"
	"text/tabwriter"

	flywheel "github.com/mrz1836/go-flywheel"
	"github.com/spf13/cobra"
)

// limitNone is the value that clears a runtime limit.
const limitNone = "none"

// newLimitsCmd builds `flywheel limits`, which lists every shared limiter with
// its state and overrides, and its set subcommand.
func newLimitsCmd(configPath *string) *cobra.Command {
	var asJSON bool
	cmd := &cobra.Command{
		Use:   "limits",
		Short: "List shared limiters with their state and overrides; override one",
		Long: "List every resource a shared database limiter has admitted against: its rate tokens as last\n" +
			"stored, the permits its live holds take, when the soonest hold lapses, and the overrides set on\n" +
			"it at runtime (MAX-CONCURRENT, RATE, BURST; - where none is set).",
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			_, db, _, err := loadAndOpen(*configPath)
			if err != nil {
				return err
			}
			defer closeDB(db)

			infos, err := flywheel.ListLimiters(cmd.Context(), db)
			if err != nil {
				return err
			}
			if asJSON {
				return writeJSON(cmd.OutOrStdout(), infos)
			}
			const stamp = "2006-01-02 15:04:05"
			tw := tabwriter.NewWriter(cmd.OutOrStdout(), 0, 2, 2, ' ', 0)
			_, _ = fmt.Fprintln(tw, "RESOURCE\tTOKENS\tHELD\tNEXT-EXPIRY\tMAX-CONCURRENT\tRATE\tBURST")
			for _, l := range infos {
				expiry := "-"
				if l.NextHoldExpiry != nil {
					expiry = l.NextHoldExpiry.Format(stamp)
				}
				_, _ = fmt.Fprintf(tw, "%s\t%d\t%d\t%s\t%s\t%s\t%s\n", l.Resource, l.Tokens, l.Held, expiry,
					limitCell(l.Overrides.MaxConcurrent, "-"), limitCell(l.Overrides.Rate, "-"), limitCell(l.Overrides.Burst, "-"))
			}
			return tw.Flush()
		},
	}
	cmd.Flags().BoolVar(&asJSON, "json", false, "emit JSON instead of a table")
	cmd.AddCommand(newLimitsSetCmd(configPath))
	return cmd
}

// newLimitsSetCmd builds `flywheel limits set <resource>`.
func newLimitsSetCmd(configPath *string) *cobra.Command {
	var reason string
	values := map[string]*string{"max-concurrent": new(string), "rate": new(string), "burst": new(string)}
	cmd := &cobra.Command{
		Use:   "set <resource>",
		Short: "Override a shared limiter's budget without a deploy",
		Long: "Override a resource's budget: each flag given replaces the limiter's configured value on every\n" +
			"acquire against the resource, in every process sharing the database, from the next one on. A\n" +
			"value is a positive integer, or none to clear the override; a flag left out keeps its override.\n" +
			"Lowering a limit revokes no permit already held. --rate applies only to a limiter configured with\n" +
			"an interval, and --max-concurrent only to one configured with a hold TTL. The resource must be one\n" +
			"`flywheel limits` lists.",
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			resource := args[0]
			given := 0
			for name := range values {
				if cmd.Flags().Changed(name) {
					given++
				}
			}
			if given == 0 {
				return errors.New("limits set: give at least one of --max-concurrent, --rate, or --burst")
			}
			parsed := make(map[string]*int, len(values))
			for name, raw := range values {
				if !cmd.Flags().Changed(name) {
					continue
				}
				n, err := parseLimit("--"+name, *raw)
				if err != nil {
					return err
				}
				parsed[name] = n
			}

			_, db, _, err := loadAndOpen(*configPath)
			if err != nil {
				return err
			}
			defer closeDB(db)
			infos, err := flywheel.ListLimiters(cmd.Context(), db)
			if err != nil {
				return err
			}
			var budget flywheel.LimiterBudget
			for _, l := range infos {
				if l.Resource == resource {
					budget = l.Overrides
					break
				}
			}
			for name, n := range parsed {
				switch name {
				case "max-concurrent":
					budget.MaxConcurrent = n
				case "rate":
					budget.Rate = n
				case "burst":
					budget.Burst = n
				}
			}
			if err := flywheel.SetLimiterBudget(
				flywheel.WithActor(cmd.Context(), operatorActor(reason)), db, resource, budget,
			); err != nil {
				return err
			}
			_, _ = fmt.Fprintf(cmd.OutOrStdout(), "limits set %s: max_concurrent=%s, rate=%s, burst=%s\n", resource,
				limitCell(budget.MaxConcurrent, limitNone), limitCell(budget.Rate, limitNone), limitCell(budget.Burst, limitNone))
			return nil
		},
	}
	cmd.Flags().StringVar(values["max-concurrent"], "max-concurrent", "",
		"most permits held at once: a positive integer, or none to clear")
	cmd.Flags().StringVar(values["rate"], "rate", "", "tokens per interval: a positive integer, or none to clear")
	cmd.Flags().StringVar(values["burst"], "burst", "", "most tokens banked: a positive integer, or none to clear")
	addReasonFlag(cmd, &reason)
	return cmd
}

// parseLimit reads a runtime limit as the commands take it: a positive integer,
// or none, which clears it (nil).
func parseLimit(name, raw string) (*int, error) {
	if raw == limitNone {
		return nil, nil //nolint:nilnil // nil is the cleared limit, not a missing one
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n < 1 {
		return nil, fmt.Errorf("%s: want a positive integer or %s, got %q", name, limitNone, raw)
	}
	return &n, nil
}

// limitCell renders a runtime limit, or unset when it has none.
func limitCell(n *int, unset string) string {
	if n == nil {
		return unset
	}
	return strconv.Itoa(*n)
}
