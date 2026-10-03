package main

import (
	"fmt"
	"strings"
	"time"

	flywheel "github.com/mrz1836/go-flywheel"
	"github.com/spf13/cobra"
)

// newMigrateCmd builds `flywheel migrate`: stand up or update the schema.
//
// It is also the upgrade. Every release's schema change is additive, so
// migrating a database an older release installed adds what is missing and
// leaves the rest — and binaries still running the older release keep working
// against the result. The report says what was added, so an operator upgrading a
// live system sees the change rather than inferring it.
//
// For a live PostgreSQL database with workers attached, --concurrently builds
// new indexes without blocking writers and --lock-timeout bounds how long a
// schema change may queue behind a long transaction (see the upgrade runbook in
// docs/INTEGRATING.md). Both are no-ops on SQLite.
func newMigrateCmd(configPath *string) *cobra.Command {
	var (
		concurrently bool
		lockTimeout  time.Duration
	)
	cmd := &cobra.Command{
		Use:   "migrate",
		Short: "Create or upgrade the flywheel schema",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfg, db, _, err := loadAndOpen(*configPath)
			if err != nil {
				return err
			}
			defer closeDB(db)

			gaps, err := flywheel.InspectSchema(cmd.Context(), db)
			if err != nil {
				return fmt.Errorf("migrate: inspect schema: %w", err)
			}
			if err := flywheel.MigrateWithOptions(db, flywheel.MigrateOpts{
				Concurrently: concurrently, LockTimeout: lockTimeout,
			}); err != nil {
				return fmt.Errorf("migrate: %w", err)
			}
			_, _ = fmt.Fprintf(cmd.OutOrStdout(), "schema up to date (%s)\n", dbLabel(cfg))
			if added := describeAdded(gaps); added != "" {
				_, _ = fmt.Fprintf(cmd.OutOrStdout(), "added: %s\n", added)
			}
			return nil
		},
	}
	cmd.Flags().BoolVar(&concurrently, "concurrently", false,
		"PostgreSQL: build new indexes with CREATE INDEX CONCURRENTLY, without blocking writers")
	cmd.Flags().DurationVar(&lockTimeout, "lock-timeout", 0,
		"PostgreSQL: fail a schema statement that waits longer than this for its table lock (0 waits indefinitely)")
	return cmd
}

// describeAdded lists the tables and columns a migration brought, or the empty
// string when the schema was already complete. A brand-new database lists every
// table, which is accurate and short.
func describeAdded(gaps []flywheel.SchemaDrift) string {
	names := make([]string, len(gaps))
	for i, g := range gaps {
		names[i] = g.String()
	}
	return strings.Join(names, ", ")
}
