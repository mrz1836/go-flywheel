//go:build integration

package core

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

// TestStatsIndexesHaveTheirShapePostgres asserts the two analytics indexes'
// installed shape, read back from pg_indexes.indexdef rather than from the DDL
// constant this package also owns: key columns in order and the partial
// predicate that keeps each one's write cost to a single entry.
func TestStatsIndexesHaveTheirShapePostgres(t *testing.T) {
	t.Parallel()
	db := NewPostgresIsolatedDB(t)
	def := func(name string) string {
		var d string
		require.NoError(t, db.Raw(`SELECT indexdef FROM pg_indexes
			WHERE schemaname = current_schema() AND indexname = ?`, name).Scan(&d).Error)
		require.NotEmpty(t, d, "%s must be installed by Migrate", name)
		return strings.ToLower(d)
	}

	log := def("job_run_finishes_pkey")
	assert.Contains(t, log, ".job_run_finishes using btree (finished_at, run_id)",
		"the finish log's key is its time order, so a window is one range of it")
	assert.Contains(t, log, "unique")

	var onRuns int64
	// The schema and table filters run first, in a materialized CTE: an indexdef
	// predicate beside them may be evaluated on another schema's index while a
	// parallel test drops it ("could not open relation with OID").
	require.NoError(t, db.Raw(`WITH idx AS MATERIALIZED (
			SELECT x.indexrelid FROM pg_index x
			JOIN pg_class t ON t.oid = x.indrelid
			JOIN pg_namespace n ON n.oid = t.relnamespace
			WHERE n.nspname = current_schema() AND t.relname = 'job_runs')
		SELECT count(*) FROM idx WHERE pg_get_indexdef(indexrelid) ILIKE '%finished_at%'`).Scan(&onRuns).Error)
	assert.Zero(t, onRuns, "nothing on job_runs indexes finished_at: the finalize UPDATE must stay HOT-eligible")

	jobs := def("jobs_finished")
	assert.Contains(t, jobs, ".jobs using btree", "the index is on jobs")
	assert.Contains(t, jobs, "(state, finalized_at, id) where (finalized_at is not null)",
		"only the terminal tuple enters: one index insert per job lifetime")
	assert.NotContains(t, jobs, "unique")
}

// TestNewReadsReachTheirIndexesPostgres EXPLAINs every statement each new read
// API emits with the sequential scan disabled and asserts none falls back to
// one — the same forced-plan approach, and for the same reason, as the
// counts-by-state pins in state_index_pg_test.go: what a schema change can break
// is whether an index can serve the read, not the planner's unforced cost choice
// on a small test table.
func TestNewReadsReachTheirIndexesPostgres(t *testing.T) {
	t.Parallel()
	db := NewPostgresIsolatedDB(t)
	seedPlanData(t, db)
	require.NoError(t, db.Exec(`ANALYZE jobs; ANALYZE job_runs; ANALYZE job_run_finishes; ANALYZE job_stats_hourly`).Error)
	for _, r := range planReaders {
		t.Run(r.name, func(t *testing.T) {
			for _, stmt := range captureSQL(t, db, func(db *gorm.DB) { r.run(t, db) }) {
				plan := forcedIndexPlan(t, db, stmt)
				for _, table := range []string{"jobs", "job_runs", "job_run_finishes", "job_stats_hourly"} {
					assert.NotContains(t, plan, "Seq Scan on "+table+" ",
						"statement:\n%s", stmt)
					assert.NotRegexp(t, `Seq Scan on `+table+`$`, plan, "statement:\n%s", stmt)
				}
			}
		})
	}
}

// TestWindowAggregateRangeScansTheFinishedIndexPostgres names the access path
// behind the hot path: the rollup's hour scan and the live tail are a range of
// the job_run_finishes key, each entry reaches its run by job_runs' primary key,
// and a legacy row's kind is a primary-key probe of jobs.
func TestWindowAggregateRangeScansTheFinishedIndexPostgres(t *testing.T) {
	t.Parallel()
	db := NewPostgresIsolatedDB(t)
	seedPlanData(t, db)
	require.NoError(t, db.Exec(`ANALYZE job_runs; ANALYZE job_run_finishes`).Error)
	stmts := captureSQL(t, db, func(db *gorm.DB) {
		_, err := aggregateRuns(fixedClockCtx(rollupBase), db, rollupBase, rollupBase.Add(time.Hour), statsFilter{})
		require.NoError(t, err)
	})
	plan := forcedIndexPlan(t, db, stmts[0])
	assert.Contains(t, plan, "job_run_finishes_pkey", "the window is a range of the finish log's key")
	assert.Contains(t, plan, "job_runs_pkey", "each finish reaches its run by primary key")
	assert.Contains(t, plan, "jobs_pkey", "the legacy-kind subquery probes jobs by primary key")
}
