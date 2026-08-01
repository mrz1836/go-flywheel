package core

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/mrz1836/go-foundation/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

// sqlitePlan returns the EXPLAIN QUERY PLAN rows for sql as one string.
//
// SQLite names the index it chose in the plan's detail column, which is enough
// to answer "does this read reach jobs_state" without the visibility-map
// complications that make the same question awkward on PostgreSQL.
func sqlitePlan(t testing.TB, db *gorm.DB, sql string, args ...any) string {
	t.Helper()
	var rows []struct {
		ID     int    `gorm:"column:id"`
		Parent int    `gorm:"column:parent"`
		NotUse int    `gorm:"column:notused"`
		Detail string `gorm:"column:detail"`
	}
	require.NoError(t, db.Raw("EXPLAIN QUERY PLAN "+sql, args...).Scan(&rows).Error)

	var b strings.Builder
	for _, r := range rows {
		b.WriteString(r.Detail)
		b.WriteString("\n")
	}
	return b.String()
}

// TestQueueHealthCountsReachTheStateIndexSQLite proves the counts-by-state read
// behind the queue-health heartbeat uses jobs_state rather than scanning.
//
// It asserts against the statement SampleQueueHealth actually issues, built the
// same way — Model(&jobRow{}) with a Group("state") — so the plan under test is
// the plan the runtime produces, soft-delete scope included.
func TestQueueHealthCountsReachTheStateIndexSQLite(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	seedStateMix(t, db)

	plan := sqlitePlan(t, db,
		`SELECT state, count(*) AS n FROM jobs WHERE jobs.deleted_at IS NULL GROUP BY state`)

	assert.Contains(t, plan, "jobs_state",
		"the counts-by-state read must reach the telemetry index")
	assert.NotContains(t, plan, "SCAN jobs\n",
		"the counts read must not fall back to a full table scan")
}

// TestOverviewCountsReachTheStateIndexSQLite proves Overview's identical
// grouping shares the index. It is the second of the three readers named in the
// index's own comment, and the one a host's dashboard calls.
func TestOverviewCountsReachTheStateIndexSQLite(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	seedStateMix(t, db)

	plan := sqlitePlan(t, db,
		`SELECT state, count(*) AS n FROM jobs WHERE jobs.deleted_at IS NULL GROUP BY state`)
	assert.Contains(t, plan, "jobs_state")

	// The Kind-filtered form is documented as not covered. Asserting that keeps
	// the limitation honest: if a later change makes it covered, this fails and
	// the doc comment gets updated deliberately rather than drifting.
	kindPlan := sqlitePlan(t, db,
		`SELECT state, count(*) AS n FROM jobs WHERE kind = ? AND jobs.deleted_at IS NULL GROUP BY state`,
		"idx.a")
	assert.NotEqual(t, "", kindPlan, "the kind-filtered form still produces a plan")
}

// TestCountActiveJobsReachesTheStateIndexSQLite covers the third reader, which
// is easy to overlook because no dashboard calls it: it is the "pending work
// remaining" seam, and the load harness polls it every 250ms during a measured
// run, which puts it inside the window every published number is taken from.
func TestCountActiveJobsReachesTheStateIndexSQLite(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	seedStateMix(t, db)

	plan := sqlitePlan(t, db,
		`SELECT count(*) FROM jobs WHERE state IN (?,?,?,?) AND jobs.deleted_at IS NULL`,
		string(StateAvailable), string(StateRunning), string(StateRetryable), string(StateScheduled))

	assert.Contains(t, plan, "jobs_state",
		"the active-jobs count shares the telemetry index's access path")
}

// TestQueueHealthCountsAreCorrectWithTheStateIndex is the companion the plan
// assertions need: an index that changes the answer is worse than no index. It
// reads through SampleQueueHealth rather than raw SQL so the counts under test
// are the ones a caller receives.
func TestQueueHealthCountsAreCorrectWithTheStateIndex(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	seedStateMix(t, db)

	now := time.Now().UTC().Add(time.Hour)
	qh, err := SampleQueueHealth(clockCtx(context.Background(), models.NewFixedClock(now)), db)
	require.NoError(t, err)

	assert.EqualValues(t, 2, qh.CountsByState[string(StateAvailable)])
	assert.EqualValues(t, 1, qh.CountsByState[string(StateRunning)])
	assert.EqualValues(t, 1, qh.CountsByState[string(StateSucceeded)])
	assert.EqualValues(t, 1, qh.InFlight)
	assert.NotContains(t, qh.CountsByState, "",
		"a soft-deleted job contributes to no state's count")
}

// TestStateIndexIsClassifiedAsPerformance pins the classification. The index
// serves telemetry reads; omitting it costs query time and removes no
// guarantee, so it must never appear in the correctness set — which is the set
// a host with a reduced test schema installs.
func TestStateIndexIsClassifiedAsPerformance(t *testing.T) {
	t.Parallel()

	set, err := IndexSet("sqlite")
	require.NoError(t, err)

	var found bool
	for _, idx := range set {
		if idx.Name != "jobs_state" {
			continue
		}
		found = true
		assert.Equal(t, IndexPerformance, idx.Kind)
		assert.Equal(t, "jobs", idx.Table)
		assert.Contains(t, idx.DDL, "(state)", "the key is state alone")
		assert.Contains(t, idx.DDL, "deleted_at IS NULL",
			"the predicate must match GORM's soft-delete scope or the reads cannot use it")
	}
	assert.True(t, found, "jobs_state is part of the runtime index set")
	assert.NotContains(t, correctnessIndexes, "jobs_state",
		"a telemetry index removes no guarantee")
}

// seedStateMix writes one job in each of several states, plus a soft-deleted
// one, so a counts read has something to group and the soft-delete scope has
// something to exclude.
func seedStateMix(t testing.TB, db *gorm.DB) {
	t.Helper()
	base := time.Now().UTC().Add(-time.Hour)
	states := []JobState{
		StateAvailable, StateAvailable, StateRunning, StateSucceeded, StateDiscarded,
	}
	for i, st := range states {
		seedJob(t, db, jobRow{
			ID: "idx-" + string(rune('a'+i)), Kind: "idx.a", State: string(st),
			ScheduledAt: base,
		})
	}
	seedJob(t, db, jobRow{
		ID: "idx-deleted", Kind: "idx.a", State: string(StateAvailable), ScheduledAt: base,
	})
	require.NoError(t, db.Delete(&jobRow{}, "id = ?", "idx-deleted").Error)
}
