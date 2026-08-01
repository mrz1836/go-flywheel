package core

import (
	"bufio"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// loadGoldenDefs reads a testdata golden fixture into a name→definition map.
func loadGoldenDefs(t *testing.T, goldenFile string) map[string]string {
	t.Helper()

	f, err := os.Open(filepath.Join("testdata", goldenFile)) //nolint:gosec // fixed, in-repo test fixture path
	require.NoError(t, err)
	defer func() { _ = f.Close() }()

	out := map[string]string{}
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		name, def, ok := strings.Cut(scanner.Text(), "\t")
		require.True(t, ok, "golden line must be name<TAB>definition: %q", scanner.Text())
		out[name] = def
	}
	require.NoError(t, scanner.Err())
	return out
}

// TestNormalizeIndexDefReducesEveryRenderingToOneShape is the must-match half of
// the normalizer's own test set: for every runtime index, the DDL the runtime
// declares and both dialects' catalog renderings of it must reduce to one string.
//
// The renderings are the task-1 golden fixtures, so this runs offline — no
// database — and is written against the exact bytes the catalog returned, not
// against an assumption about how it renders. A normalizer that flagged a correct
// schema would be worse than none, and this is what proves it does not.
func TestNormalizeIndexDefReducesEveryRenderingToOneShape(t *testing.T) {
	t.Parallel()

	set, err := IndexSet("postgres") // dialect-independent DDL
	require.NoError(t, err)
	pg := loadGoldenDefs(t, "index_defs_postgres.golden")
	sqlite := loadGoldenDefs(t, "index_defs_sqlite.golden")
	require.Len(t, pg, len(set))
	require.Len(t, sqlite, len(set))

	for _, idx := range set {
		t.Run(idx.Name, func(t *testing.T) {
			t.Parallel()
			want := normalizeIndexDef(idx.DDL)

			assert.Equal(t, want, normalizeIndexDef(sqlite[idx.Name]),
				"the SQLite rendering must reduce to the runtime's DDL")

			// The PostgreSQL rendering qualifies the table with its schema. Substitute
			// the placeholder back to a real identifier both ways: an isolated schema
			// (exercising the on <schema>. regex) and public (exercising the public.
			// shortcut), since a host can install into either.
			for _, schema := range []string{"t_9_1", "public"} {
				pgDef := strings.ReplaceAll(pg[idx.Name], schemaPlaceholder, schema)
				assert.Equal(t, want, normalizeIndexDef(pgDef),
					"the PostgreSQL rendering in schema %q must reduce to the runtime's DDL", schema)
			}
		})
	}
}

// TestNormalizeIndexDefKeepsGenuineDifferences is the must-not-match half: the
// guard against a normalizer tightened until it matches everything. Each pair is
// a real drift the comparison has to keep seeing — two are the historical
// incidents this whole change exists for, the rest are the single-attribute
// changes a drift is made of.
//
// The reduction strips parentheses, so it cannot see an operator precedence
// expressed only through them; no runtime index depends on that, and no pair here
// asserts it, which is the honest boundary of what this comparison detects.
func TestNormalizeIndexDefKeepsGenuineDifferences(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		a, b string
	}{
		{
			name: "the pre-v0.7.1 four-column jobs_ready vs the current key",
			a:    `CREATE INDEX IF NOT EXISTS jobs_ready ON jobs (queue, executor_class, priority, scheduled_at) WHERE state IN ('available', 'retryable', 'scheduled')`,
			b:    `CREATE INDEX IF NOT EXISTS jobs_ready ON jobs (queue, priority, scheduled_at) WHERE state IN ('available', 'retryable', 'scheduled') AND deleted_at IS NULL`,
		},
		{
			name: "a non-partial idx_jobs_deleted_at vs the partial one",
			a:    `CREATE INDEX idx_jobs_deleted_at ON jobs (deleted_at)`,
			b:    `CREATE INDEX IF NOT EXISTS idx_jobs_deleted_at ON jobs (deleted_at) WHERE deleted_at IS NOT NULL`,
		},
		{
			name: "the unique qualifier dropped",
			a:    `CREATE UNIQUE INDEX idx_job_periodics_slug ON job_periodics (slug)`,
			b:    `CREATE INDEX idx_job_periodics_slug ON job_periodics (slug)`,
		},
		{
			name: "the key columns reordered",
			a:    `CREATE UNIQUE INDEX job_runs_job_attempt ON job_runs (job_id, attempt)`,
			b:    `CREATE UNIQUE INDEX job_runs_job_attempt ON job_runs (attempt, job_id)`,
		},
		{
			name: "a different predicate value",
			a:    `CREATE INDEX jobs_running_leased ON jobs (leased_until) WHERE state = 'running'`,
			b:    `CREATE INDEX jobs_running_leased ON jobs (leased_until) WHERE state = 'available'`,
		},
		{
			name: "IS NULL negated to IS NOT NULL",
			a:    `CREATE INDEX jobs_state ON jobs (state) WHERE deleted_at IS NULL`,
			b:    `CREATE INDEX jobs_state ON jobs (state) WHERE deleted_at IS NOT NULL`,
		},
		{
			name: "the predicate dropped entirely",
			a:    `CREATE INDEX jobs_state ON jobs (state) WHERE deleted_at IS NULL`,
			b:    `CREATE INDEX jobs_state ON jobs (state)`,
		},
		{
			name: "a different key column",
			a:    `CREATE INDEX jobs_parent ON jobs (parent_job_id) WHERE parent_job_id IS NOT NULL`,
			b:    `CREATE INDEX jobs_parent ON jobs (kind) WHERE parent_job_id IS NOT NULL`,
		},
		{
			name: "a state added to the predicate list",
			a:    `CREATE INDEX jobs_ready ON jobs (queue, priority, scheduled_at) WHERE state IN ('available', 'retryable', 'scheduled') AND deleted_at IS NULL`,
			b:    `CREATE INDEX jobs_ready ON jobs (queue, priority, scheduled_at) WHERE state IN ('available', 'retryable', 'scheduled', 'running') AND deleted_at IS NULL`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.NotEqual(t, normalizeIndexDef(tc.a), normalizeIndexDef(tc.b),
				"these are genuinely different indexes and the normalizer must keep them apart:\n  a: %s\n  b: %s",
				tc.a, tc.b)
		})
	}
}

// TestInspectIndexesReportsNoDriftOnAFreshInstall proves the parity check is
// clean on a database the runtime just installed — the schema both consumers run
// against. A check that flagged drift here would be noise a host learns to
// ignore.
func TestInspectIndexesReportsNoDriftOnAFreshInstall(t *testing.T) {
	t.Parallel()
	db := newDB(t)

	drift, err := InspectIndexes(context.Background(), db)
	require.NoError(t, err)
	assert.Empty(t, drift, "a freshly migrated schema is at parity with the runtime")
}

// TestInspectIndexesReportsAbsentIndexes proves the absent branch: a host-owned
// schema that created the tables but not the indexes drifts on every one of them,
// each reported with an empty Installed so a host can tell absent from changed.
func TestInspectIndexesReportsAbsentIndexes(t *testing.T) {
	t.Parallel()
	db := newBareSQLite(t)
	require.NoError(t, db.AutoMigrate(Models()...), "the host's loader owns the tables, not the indexes")

	drift, err := InspectIndexes(context.Background(), db)
	require.NoError(t, err)

	set, err := IndexSet(db.Name())
	require.NoError(t, err)
	require.Len(t, drift, len(set), "every runtime index is absent when only the tables exist")
	for _, d := range drift {
		assert.Empty(t, d.Installed, "index %q is absent, so Installed must be empty", d.Name)
		assert.NotEmpty(t, d.Expected, "index %q must carry the runtime's DDL as Expected", d.Name)
	}
}

// TestInspectIndexesRejectsANilDB guards the precondition, matching InstallIndexes.
func TestInspectIndexesRejectsANilDB(t *testing.T) {
	t.Parallel()
	_, err := InspectIndexes(context.Background(), nil)
	require.Error(t, err)
}

// TestInspectIndexesSurfacesReadError drives the catalog-read failure path shared
// by InspectIndexes, inspectIndexes, and readInstalledIndexDefs: a closed DB makes
// the sqlite_master scan fail, and the error is surfaced rather than reported as
// an empty (parity) result.
func TestInspectIndexesSurfacesReadError(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	closeDB(t, db)

	_, err := InspectIndexes(context.Background(), db)
	require.ErrorContains(t, err, "InspectIndexes", "a failed catalog read is surfaced, not swallowed as parity")
}
