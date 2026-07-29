//go:build integration

package flywheel

import "testing"

// TestPostgresRendersEveryRuntimeIndexAsCharacterized pins how PostgreSQL's
// pg_indexes.indexdef renders each runtime index: fully-qualified table,
// USING btree, a parenthesized predicate, IN lists as = ANY (ARRAY[...]), and
// ::text casts on the literals. Those are the rewrites the normalizer has to
// undo, so this fixture is where a PostgreSQL rendering change surfaces.
func TestPostgresRendersEveryRuntimeIndexAsCharacterized(t *testing.T) {
	t.Parallel()
	db := NewPostgresIsolatedDB(t)
	assertRuntimeIndexRenderingMatchesGolden(t, db, "index_defs_postgres.golden")
}
