package main

import (
	"fmt"

	"github.com/mrz1836/go-flywheel/cmd/flywheel/internal/update"
	"github.com/spf13/cobra"
)

// newFetcher builds the release fetcher the update command uses. It is a var so
// tests can inject a fake fetcher and exercise the command without the network.
//
//nolint:gochecknoglobals // injectable seam for tests; defaults to the GitHub fetcher
var newFetcher = func(current string) update.ReleaseFetcher { return update.NewGitHubFetcher(current) }

// newUpdateCmd builds `flywheel update`: download and install the latest release,
// verifying its checksum and atomically replacing the running binary. --check
// reports availability without installing; --force reinstalls the latest even
// when it is not newer than the running build.
func newUpdateCmd() *cobra.Command {
	var (
		checkOnly bool
		force     bool
	)
	cmd := &cobra.Command{
		Use:   "update",
		Short: "Update flywheel to the latest release",
		Long: "Download and install the latest flywheel release: the OS/arch asset is\n" +
			"verified against the release checksums and atomically swapped in. Use --check\n" +
			"to report availability without installing.",
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			ctx := cmd.Context()
			current := resolveVersion()
			fetcher := newFetcher(current)
			out := cmd.OutOrStdout()

			if checkOnly {
				result := update.CheckFresh(ctx, current, fetcher)
				if result.Err != nil {
					return result.Err
				}
				if result.UpdateAvailable {
					_, _ = fmt.Fprintf(out, "update available: %s -> %s (run `flywheel update`)\n",
						result.CurrentVersion, result.LatestVersion)
				} else {
					_, _ = fmt.Fprintf(out, "flywheel is up to date (%s)\n", result.CurrentVersion)
				}
				return nil
			}

			latest, updated, err := update.SelfUpdate(ctx, current, fetcher, update.Options{Force: force, Out: out})
			if err != nil {
				return err
			}
			if !updated {
				_, _ = fmt.Fprintf(out, "flywheel is already up to date (%s)\n", latest)
			}
			return nil
		},
	}
	cmd.Flags().BoolVar(&checkOnly, "check", false, "report whether an update is available without installing")
	cmd.Flags().BoolVar(&force, "force", false, "install the latest release even if it is not newer")
	return cmd
}
