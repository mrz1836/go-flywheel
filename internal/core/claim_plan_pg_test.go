//go:build integration

package core

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// The claim is the runtime's hottest query, and for most of this package's life
// it could not reach its own index: every claim scanned the whole ready set and
// sorted it. That was fixed by changing jobs_ready's key, and nothing about the
// change is visible to a test that checks index *names* — which is what every
// other index assertion in this repository does. A stale definition passes all
// of them.
//
// So this file asserts the plan instead. It seeds a set large enough that the
// planner has a real choice, asks the driver for the statement it would emit,
// and requires an index scan with no sort above it. A regression here means the
// claim silently went back to O(table), which is exactly the failure the name
// checks cannot see.

// planProbeJobs is how many claimable rows the plan probes seed.
//
// It has to be large enough that a sequential scan is not simply the cheapest
// honest plan — on a few hundred rows the planner picks one because it is right
// to, and the assertion would fail for a reason that is not a regression. A few
// thousand rows puts the table well past that point while keeping the fixture
// inside a second.
const planProbeJobs = 5000

// planProbeLimit is the claim's batch size in these probes, standing in for a
// runner's Concurrency. It is what "a sort over more than LIMIT rows" is
// measured against.
const planProbeLimit = 8

// claimSQLRecorder captures the statement GORM finally sends.
//
// Trace receives a closure returning the SQL with bind values already
// interpolated by the dialector, so what comes back is directly re-executable.
// That is the only reason these probes can explain the real claim: the
// statement is built by fmt.Sprintf inside postgresDriver.Dequeue, so there is
// no constant to import, and a copy kept here would drift from the driver
// silently and let this file pass while asserting a plan for a query nobody
// runs.
type claimSQLRecorder struct {
	sql string
}

func (r *claimSQLRecorder) LogMode(logger.LogLevel) logger.Interface { return r }
func (r *claimSQLRecorder) Info(context.Context, string, ...any)     {}
func (r *claimSQLRecorder) Warn(context.Context, string, ...any)     {}
func (r *claimSQLRecorder) Error(context.Context, string, ...any)    {}

func (r *claimSQLRecorder) Trace(_ context.Context, _ time.Time, fc func() (string, int64), _ error) {
	r.sql, _ = fc()
}

// captureClaimSQL returns the statement the PostgreSQL driver emits for one
// claim, without claiming anything: Dequeue is an UPDATE, so it runs inside a
// transaction that is rolled back.
func captureClaimSQL(
	ctx context.Context, t *testing.T, db *gorm.DB, queues []string, class ExecutorClass, claimAny bool,
) string {
	t.Helper()

	rec := &claimSQLRecorder{}
	prev := db.Logger
	db.Logger = rec
	defer func() { db.Logger = prev }()

	tx := db.WithContext(ctx).Begin()
	require.NoError(t, tx.Error)
	_, err := NewPostgresDriver(tx).Dequeue(ctx, queues, class, claimAny, planProbeLimit, time.Minute)
	require.NoError(t, tx.Rollback().Error)
	require.NoError(t, err)
	require.NotEmpty(t, strings.TrimSpace(rec.sql), "the driver traced no statement")
	return rec.sql
}

// explainClaim runs EXPLAIN (ANALYZE) on a captured claim and returns the plan.
//
// ANALYZE executes what it explains, so this too runs inside a rolled-back
// transaction. Without that, the first probe would claim rows and every probe
// after it would be planning against a different table.
func explainClaim(ctx context.Context, t *testing.T, db *gorm.DB, sql string) string {
	t.Helper()

	tx := db.WithContext(ctx).Begin()
	require.NoError(t, tx.Error)
	var lines []string
	explainErr := tx.Raw("EXPLAIN (ANALYZE) " + sql).Scan(&lines).Error
	require.NoError(t, tx.Rollback().Error)
	require.NoError(t, explainErr)
	return strings.Join(lines, "\n")
}

// seedPlanProbeJobs inserts a claimable set spread across queues and executor
// classes, then analyzes the table so the planner is choosing on real
// statistics rather than on its defaults for an unanalyzed relation.
//
// The class spread matters: half the rows are claimable by a routed runner and
// half belong to other classes, so the claim's class filter genuinely rejects
// rows. A fixture in which every row matched would let a plan look good for a
// reason production would not reproduce.
func seedPlanProbeJobs(ctx context.Context, t *testing.T, db *gorm.DB, queues []string) {
	t.Helper()

	classes := []string{"probe", "", "other-a", "other-b"}
	now := time.Now().UTC().Add(-time.Hour)

	rows := make([]jobRow, 0, planProbeJobs)
	for i := range planProbeJobs {
		rows = append(rows, jobRow{
			Kind:          "plan.probe",
			Queue:         queues[i%len(queues)],
			Args:          []byte(`{}`),
			Priority:      100 + i%5,
			State:         string(StateAvailable),
			MaxAttempts:   25,
			ScheduledAt:   now,
			ExecutorClass: classes[i%len(classes)],
		})
	}
	require.NoError(t, db.WithContext(ctx).CreateInBatches(rows, 500).Error)
	require.NoError(t, db.WithContext(ctx).Exec(`ANALYZE jobs`).Error)
}

// TestClaimReachesItsIndex is the regression gate a name-based index assertion
// cannot give.
//
// For each shape the index is keyed to serve, it requires the plan to use
// jobs_ready and to carry no Sort above the scan. A Sort there means the claim
// is ordering the whole ready set to return LIMIT rows, which is the O(table)
// behavior the key change exists to remove.
func TestClaimReachesItsIndex(t *testing.T) {
	ctx := context.Background()
	db := NewPostgresIsolatedDB(t)
	queues := []string{"q0", "q1", "q2"}
	seedPlanProbeJobs(ctx, t, db, queues)

	tests := []struct {
		name     string
		queues   []string
		class    ExecutorClass
		claimAny bool
	}{
		{
			name: "a routed claim on one queue", queues: []string{"q0"}, class: "probe",
		},
		{
			// ClaimAnyClass omits the class predicate entirely. Under the previous
			// key that left a gap in the leading columns and the index was
			// unreachable; the current key has no column for it to skip.
			name: "a ClaimAnyClass claim on one queue", queues: []string{"q0"}, claimAny: true,
		},
		{
			// The empty poll: a runner polls on its interval whether or not there
			// is work, so on a quiet queue this is most of the claims a deployment
			// ever issues.
			name: "a routed claim on a queue with nothing ready", queues: []string{"q-idle"}, class: "probe",
		},
		{
			name: "a ClaimAnyClass claim on a queue with nothing ready", queues: []string{"q-idle"}, claimAny: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			sql := captureClaimSQL(ctx, t, db, tc.queues, tc.class, tc.claimAny)
			plan := explainClaim(ctx, t, db, sql)

			// The claim's own scan, not the outer UPDATE's join back to the table.
			claimed := claimCTEPlan(t, plan)

			assert.Contains(t, claimed, "Index Scan using jobs_ready",
				"the claim must reach its index; plan was:\n%s", plan)
			assert.NotContains(t, claimed, "Seq Scan",
				"the claim must not scan the table; plan was:\n%s", plan)
			assert.NotContains(t, claimed, "Sort",
				"a sort above the scan means the claim ordered the whole ready set to return %d rows; plan was:\n%s",
				planProbeLimit, plan)
		})
	}
}

// TestClaimIndexCoversTheClaimsPredicate pins the index's shape against the
// claim's, in the database rather than in the DDL string.
//
// The two have to agree on more than the columns: the claim filters
// deleted_at IS NULL, and an index whose predicate omits it makes the database
// visit the heap to reject every soft-deleted candidate.
func TestClaimIndexCoversTheClaimsPredicate(t *testing.T) {
	ctx := context.Background()
	db := NewPostgresIsolatedDB(t)

	var def string
	require.NoError(t, db.WithContext(ctx).Raw(
		`SELECT indexdef FROM pg_indexes WHERE schemaname = current_schema() AND indexname = 'jobs_ready'`,
	).Scan(&def).Error)
	require.NotEmpty(t, def, "jobs_ready must exist after Migrate")

	// The claim orders by priority, scheduled_at within a queue, so those three
	// columns in that order are what makes an ordered scan possible.
	assert.Contains(t, def, "(queue, priority, scheduled_at)",
		"the key must match the claim's ORDER BY within a queue")
	// Every state the claim can take, and only those.
	for _, state := range claimableStates {
		assert.Contains(t, def, "'"+state+"'", "the predicate must cover the claimable state %q", state)
	}
	assert.Contains(t, def, "deleted_at IS NULL",
		"the claim filters deleted_at, so the predicate must too or every candidate needs a heap visit")
}

// claimCTEPlan returns the part of an EXPLAIN output belonging to the claim's
// CTE.
//
// The claim is a CTE feeding an UPDATE that joins back to jobs on the primary
// key, so every plan contains a second index scan that says nothing about the
// question. An assertion over the whole text would find jobs_pkey and pass on a
// claim that sequentially scanned.
func claimCTEPlan(t *testing.T, plan string) string {
	t.Helper()

	lines := strings.Split(plan, "\n")
	start, depth := -1, 0
	for i, line := range lines {
		if strings.HasPrefix(strings.TrimSpace(line), "CTE ") {
			start, depth = i+1, len(line)-len(strings.TrimLeft(line, " "))
			break
		}
	}
	require.GreaterOrEqual(t, start, 0, "the claim must still be a CTE; plan was:\n%s", plan)

	for i := start; i < len(lines); i++ {
		trimmed := strings.TrimSpace(lines[i])
		if trimmed == "" {
			continue
		}
		if len(lines[i])-len(strings.TrimLeft(lines[i], " ")) <= depth {
			return strings.Join(lines[start:i], "\n")
		}
	}
	return strings.Join(lines[start:], "\n")
}
