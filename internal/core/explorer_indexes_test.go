package core

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

// explorerSet returns ExplorerIndexSet for dialect, failing the test on an error.
func explorerSet(t *testing.T, dialect string) []Index {
	t.Helper()
	set, err := ExplorerIndexSet(dialect)
	require.NoError(t, err)
	return set
}

// explorerIndexNames lists every explorer index's name, whichever dialect has it.
//
//nolint:gochecknoglobals // shared expectation
var explorerIndexNames = []string{"jobs_explore_kind", "jobs_explore_queue", "jobs_explore_state", "jobs_explore_tags"}

// TestExplorerIndexesAreNeverInstalledByDefault proves the explorer set is
// opt-in: no default install path creates or lists an entry, retirement never
// names one, and InspectIndexes says nothing about one a host installed.
func TestExplorerIndexesAreNeverInstalledByDefault(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	db := newDB(t)
	for _, name := range explorerIndexNames {
		assert.False(t, sqliteHasIndex(t, db, name), "Migrate installs no %s", name)
		assert.NotContains(t, retiredIndexNames(), name)
	}
	for _, dialect := range []string{"postgres", "sqlite"} {
		set, err := IndexSet(dialect)
		require.NoError(t, err)
		ddl, err := Indexes(dialect)
		require.NoError(t, err)
		for _, idx := range set {
			assert.NotContains(t, explorerIndexNames, idx.Name, "IndexSet(%q) never lists an explorer index", dialect)
		}
		for _, stmt := range ddl {
			assert.NotContains(t, stmt, "jobs_explore_", "Indexes(%q) never lists an explorer index", dialect)
		}
	}

	bare := newBareSQLite(t)
	require.NoError(t, bare.AutoMigrate(Models()...))
	require.NoError(t, InstallIndexes(ctx, bare))
	for _, name := range explorerIndexNames {
		assert.False(t, sqliteHasIndex(t, bare, name), "a plain InstallIndexes installs no %s", name)
	}

	require.NoError(t, InstallIndexesWithOptions(ctx, db, IndexOpts{ExtraIndexes: explorerSet(t, "sqlite")}))
	require.True(t, sqliteHasIndex(t, db, "jobs_explore_kind"))
	drift, err := InspectIndexes(ctx, db)
	require.NoError(t, err)
	assert.Empty(t, drift, "InspectIndexes reports nothing about an explorer index a host installed")
	require.NoError(t, Migrate(db), "a plain Migrate leaves a host's explorer indexes alone")
	assert.True(t, sqliteHasIndex(t, db, "jobs_explore_kind"))
}

// TestInstalledExplorerIndexesInspectClean proves the set's entries, and that
// ExtraIndexes installs them through InstallIndexesWithOptions and
// MigrateWithOptions, inspects them clean, and reports, refuses, and rebuilds a
// drifted one by IndexSet's own rules.
func TestInstalledExplorerIndexesInspectClean(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	t.Run("the_set", func(t *testing.T) {
		pg := explorerSet(t, "postgres")
		lite := explorerSet(t, "sqlite")
		require.Len(t, pg, 4)
		require.Len(t, lite, 3, "the GIN tags index is PostgreSQL's alone")
		shapes := map[string]string{
			"jobs_explore_kind":  "ON jobs (kind, id) WHERE deleted_at IS NULL",
			"jobs_explore_queue": "ON jobs (queue, id) WHERE deleted_at IS NULL",
			"jobs_explore_state": "ON jobs (state, id) WHERE deleted_at IS NULL",
			"jobs_explore_tags":  "ON jobs USING gin (tags jsonb_path_ops) WHERE tags <> '[]'::jsonb AND deleted_at IS NULL",
		}
		for i, idx := range pg {
			assert.Equal(t, explorerIndexNames[i], idx.Name, "kind, queue, state, tags, in that order")
			assert.Equal(t, IndexPerformance, idx.Kind)
			assert.Equal(t, "jobs", idx.Table)
			assert.True(t, strings.HasPrefix(idx.DDL, "CREATE INDEX IF NOT EXISTS "+idx.Name+" "), idx.DDL)
			assert.True(t, strings.HasSuffix(idx.DDL, shapes[idx.Name]), idx.DDL)
		}
		assert.Equal(t, pg[:3], lite, "SQLite's entries are PostgreSQL's first three")

		_, err := ExplorerIndexSet("mysql")
		require.ErrorIs(t, err, ErrUnsupportedDialect)
	})

	t.Run("install_and_inspect", func(t *testing.T) {
		db := newDB(t)
		set := explorerSet(t, "sqlite")
		drift, err := InspectIndexesWithOptions(ctx, db, InspectIndexOpts{ExtraIndexes: set})
		require.NoError(t, err)
		require.Len(t, drift, 3, "an extra index not yet installed is reported absent")
		for i, d := range drift {
			assert.Equal(t, set[i].Name, d.Name)
			assert.Empty(t, d.Installed)
			assert.Equal(t, set[i].DDL, d.Expected)
		}

		require.NoError(t, InstallIndexesWithOptions(ctx, db, IndexOpts{ExtraIndexes: set}))
		for _, idx := range set {
			assert.True(t, sqliteHasIndex(t, db, idx.Name), "%s is created", idx.Name)
		}
		drift, err = InspectIndexesWithOptions(ctx, db, InspectIndexOpts{ExtraIndexes: set})
		require.NoError(t, err)
		assert.Empty(t, drift, "installed extras inspect clean")
		require.NoError(t, InstallIndexesWithOptions(ctx, db, IndexOpts{ExtraIndexes: set}), "a re-install is a no-op")

		plain, err := InspectIndexesWithOptions(ctx, db, InspectIndexOpts{})
		require.NoError(t, err)
		zero, err := InspectIndexes(ctx, db)
		require.NoError(t, err)
		assert.Equal(t, zero, plain, "InspectIndexes is the zero-options call")
	})

	t.Run("migrate", func(t *testing.T) {
		db := newBareSQLite(t)
		set := explorerSet(t, "sqlite")
		require.NoError(t, MigrateWithOptions(db, MigrateOpts{ExtraIndexes: set}))
		for _, idx := range set {
			assert.True(t, sqliteHasIndex(t, db, idx.Name), "Migrate creates %s when asked", idx.Name)
		}
		drift, err := InspectIndexesWithOptions(ctx, db, InspectIndexOpts{ExtraIndexes: set})
		require.NoError(t, err)
		assert.Empty(t, drift)
	})

	t.Run("drift_reported_refused_and_rebuilt", func(t *testing.T) {
		db := newDB(t)
		set := explorerSet(t, "sqlite")
		require.NoError(t, db.Exec(`CREATE INDEX jobs_explore_kind ON jobs (kind)`).Error,
			"a jobs_explore_kind without its id or predicate drifts from the set's")

		drift, err := InspectIndexesWithOptions(ctx, db, InspectIndexOpts{ExtraIndexes: set})
		require.NoError(t, err)
		require.Len(t, drift, 3, "the drifted one and the two absent ones")
		assert.Equal(t, "jobs_explore_kind", drift[0].Name)
		assert.NotEmpty(t, drift[0].Installed)
		assert.Equal(t, set[0].DDL, drift[0].Expected)

		err = InstallIndexesWithOptions(ctx, db, IndexOpts{ExtraIndexes: set})
		require.ErrorIs(t, err, ErrIndexDrift)
		var de *IndexDriftError
		require.ErrorAs(t, err, &de)
		require.Len(t, de.Drift, 1)
		assert.Equal(t, "jobs_explore_kind", de.Drift[0].Name)
		assert.True(t, sqliteHasIndex(t, db, "jobs_explore_queue"), "the absent extras were still created")

		require.NoError(t, InstallIndexesWithOptions(ctx, db, IndexOpts{ExtraIndexes: set, Reconcile: true}))
		drift, err = InspectIndexesWithOptions(ctx, db, InspectIndexOpts{ExtraIndexes: set})
		require.NoError(t, err)
		assert.Empty(t, drift, "Reconcile rebuilt the drifted extra")
	})
}

// TestExtraIndexesRefuseANameIndexSetDeclares proves an extra index may not
// shadow one of the runtime's, nor name an index twice, and that the install,
// migrate, and inspect paths refuse either before running any statement.
func TestExtraIndexesRefuseANameIndexSetDeclares(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	kind := explorerSet(t, "sqlite")[0]
	cases := map[string][]Index{
		"a_runtime_name": {kind, {
			Name: "jobs_ready", Kind: IndexPerformance, Table: "jobs",
			DDL: `CREATE INDEX IF NOT EXISTS jobs_ready ON jobs (queue, id)`,
		}},
		"a_name_twice": {kind, kind},
	}
	for name, extras := range cases {
		t.Run(name, func(t *testing.T) {
			db := newDB(t)
			calls := map[string]func(db *gorm.DB) error{
				"install": func(db *gorm.DB) error {
					return InstallIndexesWithOptions(ctx, db, IndexOpts{ExtraIndexes: extras})
				},
				"migrate": func(db *gorm.DB) error {
					return MigrateWithOptions(db, MigrateOpts{ExtraIndexes: extras})
				},
				"inspect": func(db *gorm.DB) error {
					_, err := InspectIndexesWithOptions(ctx, db, InspectIndexOpts{ExtraIndexes: extras})
					return err
				},
			}
			for call, run := range calls {
				recorded, rec := recordStatements(db)
				requireFieldRefusal(t, run(recorded), "extra_indexes")
				assert.Empty(t, rec.stmt, "%s refuses before any statement", call)
			}
			assert.False(t, sqliteHasIndex(t, db, "jobs_explore_kind"), "nothing was installed")
		})
	}
}
