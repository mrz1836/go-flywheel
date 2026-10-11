//go:build integration

package core

import (
	"context"
	"testing"
	"time"

	"gorm.io/gorm"
)

// TestInspectSchemaReportsWhatThePreviousReleaseLacksPostgres mirrors the parity
// check through information_schema's dialect.
func TestInspectSchemaReportsWhatThePreviousReleaseLacksPostgres(t *testing.T) {
	t.Parallel()
	previousReleaseGapSuite(t, newBarePostgres(t))
}

// TestSchedulerRefusesToStartWithoutThePausedAtColumnPostgres is the
// deploy-order guard on PostgreSQL.
func TestSchedulerRefusesToStartWithoutThePausedAtColumnPostgres(t *testing.T) {
	t.Parallel()
	schedulerRefusesWithoutPausedAtSuite(t, newBarePostgres(t))
}

// TestMigrateUpgradesThePreviousReleaseSchemaPostgres is the library-owned
// upgrade from the previous release, run with the live-database options.
func TestMigrateUpgradesThePreviousReleaseSchemaPostgres(t *testing.T) {
	t.Parallel()
	previousReleaseUpgradeSuite(t, newBarePostgres(t), func(db *gorm.DB) error {
		return MigrateWithOptions(db, MigrateOpts{Concurrently: true, LockTimeout: 5 * time.Second})
	})
}

// TestHostOwnedUpgradeFromThePreviousReleasePostgres is the host-owned upgrade
// from the previous release: the migration tool adds the column (AutoMigrate
// over Models stands in for the generated migration), then the deploy step
// installs the indexes as it always does.
func TestHostOwnedUpgradeFromThePreviousReleasePostgres(t *testing.T) {
	t.Parallel()
	previousReleaseUpgradeSuite(t, newBarePostgres(t), func(db *gorm.DB) error {
		if err := db.AutoMigrate(Models()...); err != nil {
			return err
		}
		return InstallIndexesWithOptions(context.Background(), db, IndexOpts{Concurrently: true, LockTimeout: 5 * time.Second})
	})
}
