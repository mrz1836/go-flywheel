//go:build integration

package core

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

// seedQueryShapesPG writes n jobs across the shapes the query's indexes serve, in
// one statement: twenty kinds and three queues, the terminal and waiting states
// (finalized_at on the terminal ones), two tags on a tenth, and five parents
// with fifty children each. One kind (plan.rare), one queue (q-rare), and one
// state (paused) each hold a hundredth of the jobs. Ids are UUIDv7-shaped, one
// second apart from base, so a created window is a range of them.
func seedQueryShapesPG(t *testing.T, db *gorm.DB, base time.Time, n int) {
	t.Helper()
	require.NoError(t, db.Exec(`
		WITH s AS (
			SELECT g,
			       CASE WHEN g % 100 = 3 THEN 'paused'
			            ELSE (ARRAY['succeeded','succeeded','succeeded','discarded','available','retryable',
			                        'scheduled','running','cancelled'])[1 + g % 9] END AS state,
			       lpad(to_hex((extract(epoch FROM ?::timestamptz) * 1000)::bigint + g * 1000), 12, '0') AS ms,
			       ?::timestamptz + g * interval '1 second' AS at
			FROM generate_series(0, ?) AS g)
		INSERT INTO jobs (id, created_at, updated_at, metadata, kind, queue, args, priority, state, attempt,
		                  max_attempts, scheduled_at, executor_class, tags, parent_job_id, finalized_at)
		SELECT substr(ms, 1, 8) || '-' || substr(ms, 9, 4) || '-7000-8000-' || lpad(to_hex(g), 12, '0'),
		       at, at, '{}'::jsonb,
		       CASE WHEN g % 100 = 7 THEN 'plan.rare' ELSE 'plan.k' || (g % 20) END,
		       CASE WHEN g % 100 = 13 THEN 'q-rare' ELSE (ARRAY['q1','q2','q3'])[1 + g % 3] END,
		       '{}'::jsonb, 100, state, 1, 25, at, '',
		       CASE WHEN g % 10 = 0 THEN '["t1","t2"]' ELSE '[]' END::jsonb,
		       CASE WHEN g % 20 = 1 THEN 'plan-parent-' || (g % 100) END,
		       CASE WHEN state IN ('succeeded','discarded','cancelled') THEN at END
		FROM s`, base, base, n-1).Error)
}

// TestQueryJobsShapesReachTheirIndexesPostgres is the query's plan gate: with the
// explorer set installed and the sequential scan disabled, each common shape's
// statements reach the index meant for it. It EXPLAINs the statements QueryJobs
// and CountJobs actually issued, as the per-read gates do; the unforced costs at
// scale are measured by the load harness instead.
//
// The kind, queue, and state shapes filter by a value a hundredth of the jobs
// carry, the case their explorer indexes exist for: the newest page of a common
// value is a short backward walk of the primary key, index or not, and the
// planner rightly takes it.
func TestQueryJobsShapesReachTheirIndexesPostgres(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	db := NewPostgresIsolatedDB(t)
	set, err := ExplorerIndexSet("postgres")
	require.NoError(t, err)
	require.NoError(t, InstallIndexesWithOptions(ctx, db, IndexOpts{ExtraIndexes: set}))
	base := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	seedQueryShapesPG(t, db, base, 5_000)
	require.NoError(t, db.Exec(`ANALYZE jobs; ANALYZE job_runs`).Error)

	query := func(q JobQuery) func(db *gorm.DB) {
		return func(db *gorm.DB) {
			_, err := QueryJobs(ctx, db, q)
			require.NoError(t, err)
		}
	}
	shapes := []struct {
		name  string
		run   func(db *gorm.DB)
		index string
	}{
		{"a_kind_newest_first", query(JobQuery{Kinds: []string{"plan.rare"}}), "jobs_explore_kind"},
		{"a_kind_counted", func(db *gorm.DB) {
			_, err := CountJobs(ctx, db, JobQuery{Kinds: []string{"plan.rare"}})
			require.NoError(t, err)
		}, "jobs_explore_kind"},
		{"a_queue", query(JobQuery{Queues: []string{"q-rare"}}), "jobs_explore_queue"},
		{"one_state_newest_first", query(JobQuery{States: []JobState{StatePaused}}), "jobs_explore_state"},
		{"finished", query(JobQuery{
			Sort: JobSortFinished, States: []JobState{StateSucceeded, StateDiscarded},
		}), "jobs_finished"},
		{"scheduled_claimable_in_a_queue", query(JobQuery{
			Sort: JobSortScheduled, Queues: []string{"q1"},
			States: []JobState{StateAvailable, StateRetryable, StateScheduled},
		}), "jobs_ready"},
		{"a_parent", query(JobQuery{ParentJobID: "plan-parent-21"}), "jobs_parent_state"},
		{"a_created_window", query(JobQuery{
			CreatedFrom: base.Add(1000 * time.Second), CreatedTo: base.Add(2000 * time.Second),
		}), "jobs_pkey"},
		{"a_tag", query(JobQuery{Tags: []string{"t1"}}), "jobs_explore_tags"},
	}
	for _, s := range shapes {
		t.Run(s.name, func(t *testing.T) {
			for _, stmt := range captureSQL(t, db, s.run) {
				plan := forcedIndexPlan(t, db, stmt)
				assert.Contains(t, plan, s.index, "statement:\n%s", stmt)
			}
		})
	}
}
