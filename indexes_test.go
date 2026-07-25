package flywheel

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

// supportedDialects are the GORM dialect names IndexSet accepts. Both express
// partial indexes, which is the whole reason the set is portable.
//
//nolint:gochecknoglobals // shared expectation fixture for the index tests
var supportedDialects = []string{"postgres", "sqlite"}

// correctnessIndexes is the hand-written list of indexes whose absence removes a
// runtime guarantee rather than throughput. It is an independent oracle: the
// classification test compares IndexSet's own Kind field against it, so a future
// edit that reclassifies an index has to change this list too.
//
//nolint:gochecknoglobals // shared expectation fixture for the index tests
var correctnessIndexes = []string{
	"jobs_unique_key",
	"jobs_unique_active_key",
	"job_runs_job_attempt",
	"idx_job_periodics_slug",
}

// TestIndexSetReturnsEveryIndexForEachDialect proves both supported dialects get
// the same fully-populated set, in a stable order, with idempotent DDL.
func TestIndexSetReturnsEveryIndexForEachDialect(t *testing.T) {
	t.Parallel()

	for _, dialect := range supportedDialects {
		t.Run(dialect, func(t *testing.T) {
			t.Parallel()

			set, err := IndexSet(dialect)
			require.NoError(t, err)
			require.Len(t, set, len(migrateIndexes), "every index the migrate oracle names must be present")

			for i, idx := range set {
				assert.Equal(t, migrateIndexes[i], idx.Name, "index order is stable and matches the oracle")
				assert.NotEmpty(t, idx.Table, "%s must name its table", idx.Name)
				assert.Contains(t, idx.DDL, idx.Name, "%s DDL must create the index it names", idx.Name)
				assert.Contains(t, idx.DDL, "IF NOT EXISTS",
					"%s DDL must be idempotent so a re-run of the install step is a no-op", idx.Name)
				assert.Contains(t, idx.DDL, " ON "+idx.Table+" ", "%s DDL must target its declared table", idx.Name)
			}
		})
	}
}

// TestIndexSetRejectsUnknownDialect proves a dialect that cannot express partial
// indexes is rejected — with the shared sentinel, not a bare string — rather
// than silently dropping idempotency. Migrate reaches IndexSet through the same
// path, so it fails identically.
func TestIndexSetRejectsUnknownDialect(t *testing.T) {
	t.Parallel()

	for _, dialect := range []string{"mysql", "sqlserver", "clickhouse", ""} {
		t.Run("dialect="+dialect, func(t *testing.T) {
			t.Parallel()

			set, err := IndexSet(dialect)
			require.ErrorIs(t, err, ErrUnsupportedDialect)
			assert.Nil(t, set)
			assert.Contains(t, err.Error(), dialect, "the error names the offending dialect")

			ddl, err := Indexes(dialect)
			require.ErrorIs(t, err, ErrUnsupportedDialect, "Indexes fails identically to IndexSet")
			assert.Nil(t, ddl)
		})
	}
}

// TestIndexSetClassifiesEveryEntry proves the classification is total and
// accurate: every entry carries a recognized Kind, and the correctness subset is
// exactly the four indexes whose absence removes a guarantee.
func TestIndexSetClassifiesEveryEntry(t *testing.T) {
	t.Parallel()

	for _, dialect := range supportedDialects {
		t.Run(dialect, func(t *testing.T) {
			t.Parallel()

			set, err := IndexSet(dialect)
			require.NoError(t, err)

			var correctness []string
			for _, idx := range set {
				require.Contains(t, []IndexKind{IndexCorrectness, IndexPerformance}, idx.Kind,
					"%s must carry a recognized kind", idx.Name)
				if idx.Kind == IndexCorrectness {
					correctness = append(correctness, idx.Name)
				}
			}
			assert.ElementsMatch(t, correctnessIndexes, correctness,
				"the correctness-bearing subset must be exactly the indexes a guarantee depends on")
		})
	}
}

// TestIndexesMatchesIndexSetOrder proves the one-line form is the same set in
// the same order, so a host that skips the classification installs no less.
func TestIndexesMatchesIndexSetOrder(t *testing.T) {
	t.Parallel()

	for _, dialect := range supportedDialects {
		t.Run(dialect, func(t *testing.T) {
			t.Parallel()

			set, err := IndexSet(dialect)
			require.NoError(t, err)
			ddl, err := Indexes(dialect)
			require.NoError(t, err)

			want := make([]string, len(set))
			for i, idx := range set {
				want[i] = idx.DDL
			}
			assert.Equal(t, want, ddl)
		})
	}
}

// TestInstallIndexesCreatesEveryIndex proves the host-owned install step reaches
// index parity with Migrate: tables from Models, indexes from InstallIndexes.
func TestInstallIndexesCreatesEveryIndex(t *testing.T) {
	t.Parallel()

	db := newBareSQLite(t)
	require.NoError(t, db.AutoMigrate(Models()...), "the host's loader owns the tables")

	require.NoError(t, InstallIndexes(context.Background(), db))
	for _, name := range migrateIndexes {
		assert.True(t, sqliteHasIndex(t, db, name), "expected index %q after InstallIndexes", name)
	}

	// Idempotent: every statement carries IF NOT EXISTS, so a redeploy that runs
	// the install step again is a no-op rather than an error.
	require.NoError(t, InstallIndexes(context.Background(), db), "InstallIndexes must be re-runnable")
}

// TestInstallIndexesNilDB guards the nil-db precondition.
func TestInstallIndexesNilDB(t *testing.T) {
	t.Parallel()
	require.Error(t, InstallIndexes(context.Background(), nil))
}

// TestInstallIndexesRequiresTheTables proves InstallIndexes is only the index
// half of the install: pointed at a database with no tables it fails loudly
// rather than reporting success on a schema that does not exist.
func TestInstallIndexesRequiresTheTables(t *testing.T) {
	t.Parallel()
	require.Error(t, InstallIndexes(context.Background(), newBareSQLite(t)))
}

// TestCorrectnessIndexesAreWhatEnforceIdempotency is the honesty check on the
// classification (A1, SQLite half). A host-owned schema — tables from Models,
// indexes from IndexSet — must reject a duplicate UniqueKey with
// ErrAlreadyEnqueued; the *same* schema built from only the IndexPerformance
// entries must accept it. Without the negative half the classification is
// decorative: a set where everything was labelled "correctness" would pass the
// positive assertion just as well.
func TestCorrectnessIndexesAreWhatEnforceIdempotency(t *testing.T) {
	t.Parallel()

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
		t.Parallel()
		db := newBareSQLite(t)
		require.NoError(t, db.AutoMigrate(Models()...))
		require.NoError(t, InstallIndexes(ctx, db))

		assert.ErrorIs(t, enqueueTwice(t, db), ErrAlreadyEnqueued,
			"a complete host-owned install rejects the duplicate")
	})

	t.Run("performance indexes only", func(t *testing.T) {
		t.Parallel()
		db := newBareSQLite(t)
		require.NoError(t, db.AutoMigrate(Models()...))

		set, err := IndexSet(db.Name())
		require.NoError(t, err)
		applied := 0
		for _, idx := range set {
			if idx.Kind != IndexPerformance {
				continue
			}
			require.NoError(t, db.Exec(idx.DDL).Error)
			applied++
		}
		require.Positive(t, applied, "the performance subset must be non-empty for this half to mean anything")

		assert.NoError(t, enqueueTwice(t, db),
			"omitting the correctness indexes silently accepts the duplicate — ErrAlreadyEnqueued never fires")
	})
}
