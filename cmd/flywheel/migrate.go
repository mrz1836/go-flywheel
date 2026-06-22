package main

import (
	"fmt"

	flywheel "github.com/mrz1836/go-flywheel"
	"github.com/spf13/cobra"
)

// newMigrateCmd builds `flywheel migrate`: stand up or update the schema.
func newMigrateCmd(configPath *string) *cobra.Command {
	return &cobra.Command{
		Use:   "migrate",
		Short: "Create or update the flywheel schema",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfg, db, _, err := loadAndOpen(*configPath)
			if err != nil {
				return err
			}
			defer closeDB(db)

			if err := flywheel.Migrate(db); err != nil {
				return fmt.Errorf("migrate: %w", err)
			}
			fmt.Fprintf(cmd.OutOrStdout(), "schema up to date (%s)\n", dbLabel(cfg))
			return nil
		},
	}
}
