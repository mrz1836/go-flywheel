//go:build integration

package core

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

// TestInstallIndexesCreatesEveryIndexPostgres proves the host-owned install step
// reaches index parity with Migrate on PostgreSQL: tables from Models, indexes
// from InstallIndexes.
func TestInstallIndexesCreatesEveryIndexPostgres(t *testing.T) {
	db := newBarePostgres(t)
	require.NoError(t, db.AutoMigrate(Models()...), "the host's loader owns the tables")

	require.NoError(t, InstallIndexes(context.Background(), db))
	for _, name := range migrateIndexes {
		assert.True(t, pgHasIndex(t, db, name), "expected index %q after InstallIndexes", name)
	}

	require.NoError(t, InstallIndexes(context.Background(), db), "InstallIndexes must be re-runnable")
}

// TestCorrectnessIndexesAreWhatEnforceIdempotencyPostgres is the Postgres half
// of the classification honesty check (A1): a host-owned schema with every index
// rejects a duplicate UniqueKey, and the same schema built from only the
// IndexPerformance entries accepts it.
func TestCorrectnessIndexesAreWhatEnforceIdempotencyPostgres(t *testing.T) {
	ctx := context.Background()
	const uniqueKey = "host-owned-idempotency"

	enqueueTwice := func(t *testing.T, db *gorm.DB) error {
		t.Helper()
		client := NewClient(db)
		_, err := Enqueue(ctx, client, "test.kind", []byte(`{}`), InsertOpts{UniqueKey: uniqueKey})
		require.NoError(t, err, "the first enqueue always succeeds")
		_, err = Enqueue(ctx, client, "test.kind", []byte(`{}`), InsertOpts{UniqueKey: uniqueKey})
		return err
	}

	t.Run("every index", func(t *testing.T) {
		db := newBarePostgres(t)
		require.NoError(t, db.AutoMigrate(Models()...))
		require.NoError(t, InstallIndexes(ctx, db))

		assert.ErrorIs(t, enqueueTwice(t, db), ErrAlreadyEnqueued,
			"a complete host-owned install rejects the duplicate")
	})

	t.Run("performance indexes only", func(t *testing.T) {
		// The reduced-schema path, used here for the one purpose it exists for.
		// applyIndexKinds fails the test if the subset is empty, so this half
		// cannot pass vacuously.
		db := newBarePostgres(t)
		require.NoError(t, db.AutoMigrate(Models()...))
		applyIndexKinds(t, db, IndexPerformance)

		assert.NoError(t, enqueueTwice(t, db),
			"omitting the correctness indexes silently accepts the duplicate — ErrAlreadyEnqueued never fires")
	})
}
