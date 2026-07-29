package flywheel

import (
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

// This file characterizes how each dialect renders a runtime index definition
// back from its catalog. The renderings are committed as golden fixtures under
// testdata/, and the tests here assert the live database still renders them that
// way — so a future PostgreSQL upgrade or GORM driver change that alters the
// spelling is caught rather than silently changing what the normalizer in
// InspectIndexes has to reconcile.
//
// These fixtures are the evidence normalizeIndexDef is written against: every
// pair of (runtime DDL, catalog rendering) it must reduce to the same string is
// one of these renderings paired with its IndexSet DDL.

// schemaPlaceholder is the token a live schema name is replaced with in the
// PostgreSQL golden, so a rendering captured from an ephemeral test schema
// (t_<nanos>_<seq>) is comparable run to run.
const schemaPlaceholder = "<schema>"

// updateGoldenEnv regenerates the golden fixtures in place when set, for a
// maintainer adopting an intended rendering change. The normal run reads them
// and compares.
const updateGoldenEnv = "FLYWHEEL_UPDATE_GOLDEN"

// readCatalogIndexDefs reads the catalog definition of every runtime index for
// db's dialect, keyed by name, with the live PostgreSQL schema normalized to
// schemaPlaceholder.
//
// It reads the catalog directly rather than through inspectIndexes on purpose:
// the characterization is an independent oracle, so a bug in the production
// reader must not be able to hide behind a test that shares it.
func readCatalogIndexDefs(t *testing.T, db *gorm.DB) map[string]string {
	t.Helper()

	set, err := IndexSet(db.Name())
	require.NoError(t, err)
	want := make(map[string]bool, len(set))
	for _, idx := range set {
		want[idx.Name] = true
	}

	out := make(map[string]string, len(set))
	switch db.Name() {
	case "postgres":
		var schema string
		require.NoError(t, db.Raw(`SELECT current_schema()`).Scan(&schema).Error)
		var rows []struct {
			Indexname string
			Indexdef  string
		}
		require.NoError(t, db.Raw(`
			SELECT indexname, indexdef FROM pg_indexes
			WHERE schemaname = current_schema()
			  AND tablename IN ('jobs', 'job_runs', 'job_periodics')`).Scan(&rows).Error)
		for _, r := range rows {
			if !want[r.Indexname] {
				continue
			}
			out[r.Indexname] = strings.ReplaceAll(r.Indexdef, schema+".", schemaPlaceholder+".")
		}
	case "sqlite":
		var rows []struct {
			Name string
			Sql  string
		}
		require.NoError(t, db.Raw(`
			SELECT name, sql FROM sqlite_master
			WHERE type = 'index' AND sql IS NOT NULL`).Scan(&rows).Error)
		for _, r := range rows {
			if !want[r.Name] {
				continue
			}
			out[r.Name] = r.Sql
		}
	default:
		t.Fatalf("readCatalogIndexDefs: unsupported dialect %q", db.Name())
	}
	return out
}

// canonicalGolden renders a name→definition map to the golden file's on-disk
// form: one "name<TAB>definition" line per index, sorted by name, so a rendering
// change produces a minimal diff.
func canonicalGolden(defs map[string]string) string {
	names := make([]string, 0, len(defs))
	for name := range defs {
		names = append(names, name)
	}
	sort.Strings(names)

	var b strings.Builder
	for _, name := range names {
		b.WriteString(name)
		b.WriteByte('\t')
		b.WriteString(defs[name])
		b.WriteByte('\n')
	}
	return b.String()
}

// assertRuntimeIndexRenderingMatchesGolden reads db's catalog renderings for
// every runtime index and asserts they equal the committed golden, or rewrites
// the golden when updateGoldenEnv is set.
func assertRuntimeIndexRenderingMatchesGolden(t *testing.T, db *gorm.DB, goldenFile string) {
	t.Helper()

	defs := readCatalogIndexDefs(t, db)
	set, err := IndexSet(db.Name())
	require.NoError(t, err)
	require.Len(t, defs, len(set),
		"every runtime index must render back from the catalog after Migrate")

	got := canonicalGolden(defs)
	path := filepath.Join("testdata", goldenFile)

	if os.Getenv(updateGoldenEnv) != "" {
		require.NoError(t, os.MkdirAll("testdata", 0o750))
		require.NoError(t, os.WriteFile(path, []byte(got), 0o600))
		t.Logf("updated golden fixture %s", path)
		return
	}

	want, err := os.ReadFile(path) //nolint:gosec // fixed, in-repo test fixture path
	require.NoError(t, err, "read golden fixture; regenerate with %s=1", updateGoldenEnv)
	assert.Equal(t, string(want), got,
		"the catalog renders a runtime index differently than the committed golden. If this is "+
			"an intended rendering change, regenerate with %s=1 and review the diff — the normalizer "+
			"in InspectIndexes is written against these pairs.", updateGoldenEnv)
}

// TestSQLiteRendersEveryRuntimeIndexAsCharacterized pins how SQLite's
// sqlite_master.sql renders each runtime index. SQLite returns the author's
// statement text verbatim except that it drops IF NOT EXISTS.
func TestSQLiteRendersEveryRuntimeIndexAsCharacterized(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	assertRuntimeIndexRenderingMatchesGolden(t, db, "index_defs_sqlite.golden")
}
