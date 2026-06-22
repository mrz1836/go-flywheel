// Command flywheel is the local daemon and operator CLI for the go-flywheel job
// runtime. It runs a runner + scheduler over SQLite or PostgreSQL from a
// flywheel.yaml file (`serve`), turns declarative schedules into durable cron
// replacements, and inspects and operates the queue (migrate, enqueue, jobs,
// schedule, doctor).
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"
)

func main() {
	// A root context cancelled on interrupt so a long-running command (serve)
	// can drain; commands that ignore it are unaffected.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := newRootCmd().ExecuteContext(ctx); err != nil {
		fmt.Fprintln(os.Stderr, "flywheel:", err)
		os.Exit(1)
	}
}

// newRootCmd assembles the command tree.
func newRootCmd() *cobra.Command {
	var configPath string
	root := &cobra.Command{
		Use:           "flywheel",
		Short:         "Durable local job runtime: a cron replacement and queue operator CLI",
		SilenceUsage:  true,
		SilenceErrors: true,
	}
	root.PersistentFlags().StringVar(&configPath, "config", defaultConfigPath(), "path to flywheel.yaml")
	root.AddCommand(
		newServeCmd(&configPath),
		newMigrateCmd(&configPath),
		newEnqueueCmd(&configPath),
		newJobsCmd(&configPath),
		newScheduleCmd(&configPath),
		newDoctorCmd(&configPath),
	)
	return root
}
