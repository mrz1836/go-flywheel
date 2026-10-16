package main

import (
	"fmt"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	flywheel "github.com/mrz1836/go-flywheel"
	"github.com/spf13/cobra"
)

// newNodesCmd builds `flywheel nodes`, which lists the nodes a host's
// processes registered, and its drain subcommand.
func newNodesCmd(configPath *string) *cobra.Command {
	var (
		asJSON       bool
		stoppedSince time.Duration
	)
	cmd := &cobra.Command{
		Use:   "nodes",
		Short: "List the registered nodes; drain one",
		Long: "List the nodes a host's processes registered (NodeConfig.RegistryInterval): each node's host,\n" +
			"version, start and last heartbeat, state (live, stale, draining, drained, or stopped), and\n" +
			"runners, each as its queues and its jobs in flight of its slots. Nodes that have not stopped\n" +
			"come first, newest first; --stopped-since also lists those that stopped within that long.\n" +
			"The CLI reads the registry; it registers no node of its own.",
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			_, db, _, err := loadAndOpen(*configPath)
			if err != nil {
				return err
			}
			defer closeDB(db)

			var p flywheel.ListNodesParams
			if stoppedSince > 0 {
				p.StoppedSince = time.Now().Add(-stoppedSince)
			}
			nodes, err := flywheel.ListNodes(cmd.Context(), db, p)
			if err != nil {
				return err
			}
			if asJSON {
				return writeJSON(cmd.OutOrStdout(), nodes)
			}
			const stamp = "2006-01-02 15:04:05"
			tw := tabwriter.NewWriter(cmd.OutOrStdout(), 0, 2, 2, ' ', 0)
			_, _ = fmt.Fprintln(tw, "ID\tHOST\tVERSION\tSTARTED\tHEARTBEAT\tSTATE\tRUNNERS")
			for _, n := range nodes {
				_, _ = fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
					n.ID, n.Host, orDash(n.Version), n.StartedAt.Format(stamp), n.HeartbeatAt.Format(stamp),
					nodeState(n), nodeRunners(n.Runners))
			}
			return tw.Flush()
		},
	}
	cmd.Flags().DurationVar(&stoppedSince, "stopped-since", 0,
		"also list the nodes that stopped within this long (e.g. 24h)")
	cmd.Flags().BoolVar(&asJSON, "json", false, "emit JSON instead of a table")
	cmd.AddCommand(newNodesDrainCmd(configPath))
	return cmd
}

// newNodesDrainCmd builds `flywheel nodes drain <id>`, which asks a node to
// stop taking work.
func newNodesDrainCmd(configPath *string) *cobra.Command {
	var reason string
	cmd := &cobra.Command{
		Use:   "drain <id>",
		Short: "Ask a node to stop taking work",
		Long: "Ask a registered node to stop taking work. Within one heartbeat it stops every runner's\n" +
			"claims and reports draining, then drained once its jobs in flight finish; it keeps running\n" +
			"until it is stopped, and takes work again only after a restart. A drain already requested\n" +
			"changes nothing. A stopped or unknown node is refused.",
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			_, db, _, err := loadAndOpen(*configPath)
			if err != nil {
				return err
			}
			defer closeDB(db)
			if err := flywheel.RequestDrain(flywheel.WithActor(cmd.Context(), operatorActor(reason)), db, args[0]); err != nil {
				return err
			}
			_, _ = fmt.Fprintf(cmd.OutOrStdout(), "drain requested %s\n", args[0])
			return nil
		},
	}
	addReasonFlag(cmd, &reason)
	return cmd
}

// nodeState names a node's state as the table prints it.
func nodeState(n flywheel.NodeInfo) string {
	switch {
	case n.StoppedAt != nil:
		return "stopped"
	case n.Drained:
		return "drained"
	case n.Draining:
		return "draining"
	case n.Stale:
		return "stale"
	default:
		return "live"
	}
}

// nodeRunners renders a node's runners as the table prints them: each as its
// queues, then its jobs in flight of its slots, separated by spaces.
func nodeRunners(runners []flywheel.NodeRunner) string {
	parts := make([]string, len(runners))
	for i, r := range runners {
		parts[i] = strings.Join(r.Queues, ",") + ":" + strconv.Itoa(r.InFlight) + "/" + strconv.Itoa(r.Concurrency)
	}
	return orDash(strings.Join(parts, " "))
}
