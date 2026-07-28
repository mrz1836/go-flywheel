package flywheel

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// seedTerminalJobs writes n succeeded jobs finalized at finalizedAt, each with a
// finished run row, which is the shape a long-lived database presents to a
// retention pass.
func seedTerminalJobs(t testing.TB, db *gorm.DB, n int, finalizedAt time.Time) {
	t.Helper()
	for i := range n {
		id := fmt.Sprintf("terminal-%04d", i)
		require.NoError(t, db.Create(&jobRow{
			ID: id, CreatedAt: finalizedAt, UpdatedAt: finalizedAt,
			Kind: "retention.batch", Queue: "default", Args: []byte(`{}`),
			Priority: 100, State: string(StateSucceeded), Attempt: 1, MaxAttempts: 25,
			ScheduledAt: finalizedAt, FinalizedAt: &finalizedAt,
			Tags: []byte(`[]`), Metadata: []byte(`{}`),
		}).Error)
		require.NoError(t, db.Create(&jobRunRow{
			ID: "trun-" + id, JobID: id, Attempt: 1, ExecutorClass: "local",
			ExecutorID: "exec-1", StartedAt: finalizedAt, FinishedAt: &finalizedAt,
			Outcome: string(OutcomeSuccess), CreatedAt: finalizedAt,
		}).Error)
	}
}

// countRows returns how many rows the model currently holds.
func countRows(t testing.TB, db *gorm.DB, model any) int64 {
	t.Helper()
	var n int64
	require.NoError(t, db.Model(model).Count(&n).Error)
	return n
}

// TestDeleteFinishedJobsReportsTheTotalAcrossBatches proves the loop runs to
// exhaustion and the returned count keeps its old meaning: the total, not the
// last batch's.
func TestDeleteFinishedJobsReportsTheTotalAcrossBatches(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	old := time.Now().UTC().Add(-90 * 24 * time.Hour)
	seedTerminalJobs(t, db, 250, old)

	deleted, err := DeleteFinishedJobsWithOptions(
		context.Background(), db, time.Now(), RetentionOpts{BatchSize: 40},
	)
	require.NoError(t, err)

	assert.EqualValues(t, 250, deleted, "the count is the total across every batch")
	assert.EqualValues(t, 0, countRows(t, db, &jobRow{}))
	assert.EqualValues(t, 0, countRows(t, db, &jobRunRow{}), "audit rows go with their jobs")
}

// TestDeleteFinishedJobsHonorsTheBatchCeiling proves MaxBatches bounds one
// pass's total work, which is what makes a scheduled prune's duty cycle
// predictable against a backlog of months.
func TestDeleteFinishedJobsHonorsTheBatchCeiling(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	old := time.Now().UTC().Add(-90 * 24 * time.Hour)
	seedTerminalJobs(t, db, 100, old)

	deleted, err := DeleteFinishedJobsWithOptions(
		context.Background(), db, time.Now(), RetentionOpts{BatchSize: 10, MaxBatches: 3},
	)
	require.NoError(t, err)

	assert.EqualValues(t, 30, deleted, "the pass stops on its ceiling")
	assert.EqualValues(t, 70, countRows(t, db, &jobRow{}), "the rest survives for the next pass")
}

// TestDeleteFinishedJobsResumesWhereTheCeilingStoppedIt proves the cursor is not
// stateful across passes: each pass restarts from the beginning of what remains,
// so a bounded pass followed by another eventually drains the backlog rather
// than repeatedly deleting the same window.
func TestDeleteFinishedJobsResumesWhereTheCeilingStoppedIt(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	old := time.Now().UTC().Add(-90 * 24 * time.Hour)
	seedTerminalJobs(t, db, 50, old)

	total := int64(0)
	for range 5 {
		n, err := DeleteFinishedJobsWithOptions(
			context.Background(), db, time.Now(), RetentionOpts{BatchSize: 10, MaxBatches: 1},
		)
		require.NoError(t, err)
		total += n
	}

	assert.EqualValues(t, 50, total, "successive bounded passes drain the whole backlog")
	assert.EqualValues(t, 0, countRows(t, db, &jobRow{}))
}

// TestDeleteFinishedJobsLeavesJobsInsideTheWindow proves the cutoff still
// governs after the rewrite: batching changed which rows a pass visits and in
// what order, and it must not have changed which rows qualify.
func TestDeleteFinishedJobsLeavesJobsInsideTheWindow(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	now := time.Now().UTC()
	seedTerminalJobs(t, db, 10, now.Add(-90*24*time.Hour))

	recent := now.Add(-time.Minute)
	require.NoError(t, db.Create(&jobRow{
		ID: "recent", CreatedAt: recent, UpdatedAt: recent,
		Kind: "retention.batch", Queue: "default", Args: []byte(`{}`),
		Priority: 100, State: string(StateSucceeded), Attempt: 1, MaxAttempts: 25,
		ScheduledAt: recent, FinalizedAt: &recent,
		Tags: []byte(`[]`), Metadata: []byte(`{}`),
	}).Error)

	deleted, err := DeleteFinishedJobsWithOptions(
		context.Background(), db, now.Add(-time.Hour), RetentionOpts{BatchSize: 3},
	)
	require.NoError(t, err)

	assert.EqualValues(t, 10, deleted)
	assert.EqualValues(t, 1, countRows(t, db, &jobRow{}), "a job inside the window survives")
}

// TestDeleteFinishedJobsLeavesNonTerminalJobsAlone proves a running or available
// job is never pruned, whatever its age. Retention removes recorded history, not
// pending work.
func TestDeleteFinishedJobsLeavesNonTerminalJobsAlone(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	old := time.Now().UTC().Add(-90 * 24 * time.Hour)
	seedTerminalJobs(t, db, 5, old)
	seedExpiredLeases(t, db, 3, time.Now().UTC())

	deleted, err := DeleteFinishedJobsWithOptions(
		context.Background(), db, time.Now(), RetentionOpts{BatchSize: 2},
	)
	require.NoError(t, err)

	assert.EqualValues(t, 5, deleted)
	assert.EqualValues(t, 3, countRows(t, db, &jobRow{}), "the running jobs survive")
}

// TestRetentionBatchSizeIsNeverUnbounded states the bound guarantee
// directly against the resolver.
func TestRetentionBatchSizeIsNeverUnbounded(t *testing.T) {
	t.Parallel()

	assert.Equal(t, defaultRetentionBatchSize, RetentionOpts{}.batchSize(),
		"a zero value selects the documented default")
	assert.Equal(t, defaultRetentionBatchSize, RetentionOpts{BatchSize: -1}.batchSize(),
		"a negative value selects the default rather than disabling the bound")
	assert.Equal(t, 9, RetentionOpts{BatchSize: 9}.batchSize(),
		"a positive value is honored verbatim")
	assert.Positive(t, defaultRetentionBatchSize, "the default is itself a bound")
}

// TestDeleteFinishedJobsCancelledReportsPartialProgress covers cancellation for
// retention: a cancelled pass reports what it committed, and committed batches
// are not rolled back.
func TestDeleteFinishedJobsCancelledReportsPartialProgress(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	old := time.Now().UTC().Add(-90 * 24 * time.Hour)
	seedTerminalJobs(t, db, 40, old)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	deleted, err := DeleteFinishedJobsWithOptions(ctx, db, time.Now(), RetentionOpts{BatchSize: 10})

	require.ErrorIs(t, err, context.Canceled, "a cancelled pass wraps the context error")
	assert.EqualValues(t, 0, deleted)
	assert.Contains(t, err.Error(), "cancelled after 0 deleted", "the error names the progress made")
	assert.EqualValues(t, 40, countRows(t, db, &jobRow{}), "nothing was touched")
}

// stmtRecorder captures every statement GORM sends, in order.
//
// It is the same recording-logger pattern claim_plan_pg_test.go uses, for the
// same reason: the statements are built inside unexported code, so there is
// nothing to import and a copy kept here would drift.
type stmtRecorder struct {
	mu   sync.Mutex
	sqls []string
}

func (r *stmtRecorder) LogMode(logger.LogLevel) logger.Interface { return r }
func (r *stmtRecorder) Info(context.Context, string, ...any)     {}
func (r *stmtRecorder) Warn(context.Context, string, ...any)     {}
func (r *stmtRecorder) Error(context.Context, string, ...any)    {}

func (r *stmtRecorder) Trace(_ context.Context, _ time.Time, fc func() (string, int64), _ error) {
	sql, _ := fc()
	r.mu.Lock()
	defer r.mu.Unlock()
	r.sqls = append(r.sqls, sql)
}

// deletes returns the recorded DELETE statements, in order, labelled by table.
func (r *stmtRecorder) deletes() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []string
	for _, sql := range r.sqls {
		upper := strings.ToUpper(sql)
		if !strings.HasPrefix(strings.TrimSpace(upper), "DELETE") {
			continue
		}
		switch {
		case strings.Contains(upper, "JOB_RUNS"):
			out = append(out, "job_runs")
		case strings.Contains(upper, "JOBS"):
			out = append(out, "jobs")
		}
	}
	return out
}

// TestDeleteFinishedJobsDeletesRunsBeforeJobsInEveryBatch asserts the delete
// order that the exported doc comment makes contractual.
//
// It reads the statement stream rather than the resulting rows, because on the
// library's own schema there is nothing to observe: no foreign key is declared
// between jobs and job_runs, so an out-of-order delete inside one transaction
// leaves no residue visible from outside it. The order is only observable in
// the statements, or under a host's ON DELETE CASCADE — which the Postgres twin
// of this test covers.
func TestDeleteFinishedJobsDeletesRunsBeforeJobsInEveryBatch(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	old := time.Now().UTC().Add(-90 * 24 * time.Hour)
	seedTerminalJobs(t, db, 25, old)

	rec := &stmtRecorder{}
	prev := db.Logger
	db.Logger = rec
	t.Cleanup(func() { db.Logger = prev })

	deleted, err := DeleteFinishedJobsWithOptions(
		context.Background(), db, time.Now(), RetentionOpts{BatchSize: 10},
	)
	require.NoError(t, err)
	require.EqualValues(t, 25, deleted)

	order := rec.deletes()
	require.Len(t, order, 6, "three batches, two deletes each")
	for i := 0; i < len(order); i += 2 {
		assert.Equal(t, "job_runs", order[i], "batch %d deletes audit rows first", i/2)
		assert.Equal(t, "jobs", order[i+1], "batch %d deletes jobs second", i/2)
	}
}
