// Command flywheel is the local daemon and operator CLI for the go-flywheel job
// runtime. It runs a runner + scheduler over SQLite or PostgreSQL from a
// flywheel.yaml file (`serve`), turns declarative schedules into durable cron
// replacements, and inspects and operates the queue (migrate, enqueue, jobs,
// schedule, doctor).
package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/mrz1836/go-flywheel/cmd/flywheel/internal/update"
	"github.com/spf13/cobra"
)

// nudgeDrainTimeout bounds how long the root command waits for the background
// update check before giving up and exiting without a banner.
const nudgeDrainTimeout = 500 * time.Millisecond

// Build-time metadata, injected by goreleaser ldflags
// (-X main.version=… -X main.commit=… -X main.buildDate=…). For a `go install`
// build with no ldflags these stay at their defaults and version.go falls back
// to runtime/debug.ReadBuildInfo().
//
//nolint:gochecknoglobals // build-time injected variables, required for ldflags
var (
	version   = "dev"
	commit    = "none"
	buildDate = "unknown"
)

func main() {
	// A root context cancelled on interrupt so a long-running command (serve)
	// can drain; commands that ignore it are unaffected.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	os.Exit(run(ctx, os.Args[1:], os.Stderr))
}

// run assembles and executes the command tree against ctx and args, writing any
// error to stderr, and returns the process exit code. It is the testable core of
// main: main only wires signal handling and os.Exit around it.
func run(ctx context.Context, args []string, stderr io.Writer) int {
	// Kick off a non-blocking "is a newer version available?" check. It is a no-op
	// under CI / FLYWHEEL_DISABLE_UPDATE_CHECK / a dev build, and the root command
	// drains it with a short timeout so a slow network never delays the CLI.
	current := resolveVersion()
	nudge := update.StartBackgroundCheck(ctx, current, update.NewGitHubFetcher(current))

	root := newRootCmd(nudge)
	root.SetArgs(args)
	if err := root.ExecuteContext(ctx); err != nil {
		_, _ = fmt.Fprintln(stderr, "flywheel:", err)
		return 1
	}
	return 0
}

// newRootCmd assembles the command tree. nudge carries the background update
// check; the root drains it after a successful command and prints a banner when
// an update is available. A nil channel disables the banner (used by tests).
func newRootCmd(nudge <-chan *update.Result) *cobra.Command {
	var configPath string
	root := &cobra.Command{
		Use:           "flywheel",
		Short:         "Durable local job runtime: a cron replacement and queue operator CLI",
		SilenceUsage:  true,
		SilenceErrors: true,
		PersistentPostRunE: func(cmd *cobra.Command, _ []string) error {
			showUpdateBanner(cmd, nudge)
			return nil
		},
	}
	root.PersistentFlags().StringVar(&configPath, "config", defaultConfigPath(), "path to flywheel.yaml")
	root.AddCommand(
		newServeCmd(&configPath),
		newMigrateCmd(&configPath),
		newEnqueueCmd(&configPath),
		newJobsCmd(&configPath),
		newScheduleCmd(&configPath),
		newPruneCmd(&configPath),
		newStatusCmd(&configPath),
		newDoctorCmd(&configPath),
		newVersionCmd(),
		newUpdateCmd(),
	)
	return root
}

// showUpdateBanner drains the background update check (bounded by
// nudgeDrainTimeout) and prints the banner when an update is available. It is a
// no-op for a nil channel and for the update command itself (which speaks for
// itself), and never blocks the CLI on a slow check.
func showUpdateBanner(cmd *cobra.Command, nudge <-chan *update.Result) {
	if nudge == nil || cmd.Name() == "update" {
		return
	}
	select {
	case result := <-nudge:
		update.ShowBanner(result)
	case <-time.After(nudgeDrainTimeout):
	}
}
