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

	selfupdate "github.com/mrz1836/go-selfupdate"
	"github.com/mrz1836/go-selfupdate/cobracmd"
	"github.com/spf13/cobra"
)

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
	root := newRootCmd()
	root.SetArgs(args)
	if err := root.ExecuteContext(ctx); err != nil {
		_, _ = fmt.Fprintln(stderr, "flywheel:", err)
		return 1
	}
	return 0
}

// newRootCmd assembles the command tree. The self-update command and the passive
// "a new version is available" banner are wired from a single selfupdate.Config
// by cobracmd.Attach, which derives the cache slug and the FLYWHEEL_ env prefix
// from BinaryName. The banner check is a no-op under CI / NO_UPDATE_CHECK /
// FLYWHEEL_NO_UPDATE_CHECK / a dev build, and never blocks the CLI on a slow
// network.
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
		newPruneCmd(&configPath),
		newStatusCmd(&configPath),
		newDoctorCmd(&configPath),
		newVersionCmd(),
	)

	current := resolveVersion()
	cobracmd.Attach(root, selfupdate.Config{
		Owner:          "mrz1836",
		Repo:           "go-flywheel",
		BinaryName:     "flywheel",
		CurrentVersion: current,
		TokenEnvVar:    "FLYWHEEL_GITHUB_TOKEN",
	})

	return root
}
