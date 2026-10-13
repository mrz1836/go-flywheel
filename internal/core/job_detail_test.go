package core

import (
	"context"
	"reflect"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/mrz1836/go-foundation/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/datatypes"
	"gorm.io/gorm"
)

// selectList matches a statement's select list: everything between its leading
// SELECT and the first FROM.
var selectList = regexp.MustCompile(`(?is)^\s*SELECT\s+(.*?)\s+FROM\s`)

// payloadColumn matches the two columns a JobView must never load.
var payloadColumn = regexp.MustCompile(`(?i)\b(args|metadata)\b`)

// TestJobViewStillCarriesNoArgs guards the list reads: JobView has no field for
// a job's args or metadata, and the statements FindJob, ListJobs, and
// ListFinished run select neither column, so a full-detail read added beside
// them can never make a list heavier.
func TestJobViewStillCarriesNoArgs(t *testing.T) {
	t.Parallel()

	view := reflect.TypeOf(JobView{})
	for i := range view.NumField() {
		f := view.Field(i)
		assert.NotContains(t, []string{"Args", "Metadata"}, f.Name, "JobView must not carry %s", f.Name)
		tag := strings.Split(f.Tag.Get("json"), ",")[0]
		assert.NotContains(t, []string{"args", "metadata"}, tag, "JobView must not serialize %s", tag)
	}

	db := newDB(t)
	ctx := context.Background()
	at := time.Now().UTC().Add(-time.Minute)
	for i, state := range []JobState{StateAvailable, StateSucceeded, StateCancelled, StateDiscarded} {
		row := jobRow{
			ID: "guard-" + string(state), Kind: "guard", State: string(state), MaxAttempts: 1,
			CreatedAt: at, UpdatedAt: at, ScheduledAt: at,
			Args:     datatypes.JSON(`{"secret":"payload"}`),
			Metadata: datatypes.JSON(`{"request_id":"r"}`),
		}
		if state != StateAvailable {
			finalized := at.Add(time.Duration(i) * time.Second)
			row.FinalizedAt = &finalized
		}
		seedJob(t, db, row)
	}

	reads := map[string]func(db *gorm.DB){
		"FindJob": func(db *gorm.DB) {
			_, err := FindJob(ctx, db, "guard-available")
			require.NoError(t, err)
		},
		"ListJobs": func(db *gorm.DB) {
			views, err := ListJobs(ctx, db, ListJobsParams{})
			require.NoError(t, err)
			require.Len(t, views, 4)
		},
		"ListFinished": func(db *gorm.DB) {
			views, err := ListFinished(ctx, db, ListFinishedParams{})
			require.NoError(t, err)
			require.Len(t, views, 3)
		},
	}
	for name, read := range reads {
		for _, stmt := range captureSQL(t, db, read) {
			m := selectList.FindStringSubmatch(stmt)
			require.NotNil(t, m, "%s: a SELECT with a select list: %s", name, stmt)
			assert.NotContains(t, m[1], "*", "%s selects every column: %s", name, stmt)
			assert.Empty(t, payloadColumn.FindString(m[1]), "%s selects a payload column: %s", name, stmt)
		}
	}
}

// storedArgs reads a job's args as the database renders them, the bytes a read
// of the column returns.
func storedArgs(t *testing.T, db *gorm.DB, id string) string {
	t.Helper()
	expr := "CAST(args AS TEXT)"
	if db.Name() == "postgres" {
		expr = "args::text"
	}
	var out string
	require.NoError(t, db.Raw("SELECT "+expr+" FROM jobs WHERE id = ?", id).Scan(&out).Error)
	require.NotEmpty(t, out)
	return out
}

// claimAndStart claims the one job on queue and commits its run stub, as a
// Runner does before the worker body runs, and returns the claim and the run id.
func claimAndStart(t *testing.T, ctx context.Context, d Driver, queue string) (RawJob, string) { //nolint:revive // testing.T-first helper convention
	t.Helper()
	raw := claimOneFrom(t, d, ctx, queue)
	runID := models.NewID()
	require.NoError(t, d.InsertRunStub(ctx, runID, raw, time.Now(), "local", "exec"))
	return raw, runID
}

// getJobDetailSuite proves GetJob returns everything a job carries, as stored,
// on the database open returns.
func getJobDetailSuite(t *testing.T, open dbOpener) {
	t.Helper()

	t.Run("claimed_job_in_full", func(t *testing.T) {
		t.Parallel()
		db := open(t)
		ctx := context.Background()
		d := driverForTest(t, db)
		id, err := Enqueue(ctx, NewClient(db), "detail.full", []byte(`{"id": 9007199254740993}`), InsertOpts{
			Queue: "detail", Tags: []string{"a"}, Metadata: map[string]any{"source": "test"}, RequestID: "req-1",
			UniqueKey: "uk-1", UniqueActiveKey: "uak-1", Timeout: 30 * time.Second,
		})
		require.NoError(t, err)
		_, runID := claimAndStart(t, ctx, d, "detail")

		got, err := GetJob(ctx, db, id)
		require.NoError(t, err)
		stored := storedArgs(t, db, id)
		assert.Equal(t, stored, string(got.Args), "args are returned exactly as stored")
		assert.Contains(t, string(got.Args), "9007199254740993", "a large integer stays exact")
		assert.Equal(t, len(stored), got.ArgsBytes)
		assert.False(t, got.ArgsOmitted)
		assert.JSONEq(t, `{"request_id":"req-1","source":"test"}`, string(got.Metadata))
		assert.Equal(t, "uk-1", got.UniqueKey)
		assert.Equal(t, "uak-1", got.UniqueActiveKey)
		require.NotNil(t, got.TimeoutMs)
		assert.Equal(t, 30000, *got.TimeoutMs)
		row := jobRowByID(t, db, id)
		require.NotNil(t, got.LeasedUntil)
		require.NotNil(t, row.LeasedUntil)
		assert.WithinDuration(t, *row.LeasedUntil, *got.LeasedUntil, time.Millisecond)
		assert.Equal(t, id, got.ID)
		assert.Equal(t, "detail.full", got.Kind)
		assert.Equal(t, string(StateRunning), got.State)
		assert.Equal(t, []string{"a"}, got.Tags)
		require.NotNil(t, got.LatestRun, "the claimed attempt's run")
		assert.Equal(t, runID, got.LatestRun.ID)
		assert.Equal(t, got.Attempt, got.LatestRun.Attempt, "the run of the attempt the job names")
		assert.Equal(t, string(OutcomeStarted), got.LatestRun.Outcome)
		assert.False(t, got.CancelPending)
		assert.Nil(t, got.Barrier)
	})

	t.Run("never_claimed", func(t *testing.T) {
		t.Parallel()
		db := open(t)
		ctx := context.Background()
		id, err := Enqueue(ctx, NewClient(db), "detail.fresh", []byte(`{}`), InsertOpts{})
		require.NoError(t, err)

		got, err := GetJob(ctx, db, id)
		require.NoError(t, err)
		assert.Nil(t, got.LatestRun)
		assert.False(t, got.CancelPending)
		assert.Nil(t, got.LeasedUntil)
		assert.Nil(t, got.TimeoutMs)
		assert.Empty(t, got.UniqueKey)
		assert.Empty(t, got.UniqueActiveKey)
		assert.JSONEq(t, `{}`, string(got.Metadata))
		assert.Equal(t, []string{}, got.Tags)
	})

	t.Run("metadata_empty_or_null_reads_as_an_empty_object", func(t *testing.T) {
		t.Parallel()
		db := open(t)
		ctx := context.Background()
		id, err := Enqueue(ctx, NewClient(db), "detail.bare", []byte(`{}`), InsertOpts{RequestID: "req-gone"})
		require.NoError(t, err)
		if db.Name() == "postgres" {
			// A host's schema may allow a NULL metadata.
			require.NoError(t, db.Exec(`ALTER TABLE jobs ALTER COLUMN metadata DROP NOT NULL`).Error)
			require.NoError(t, db.Exec(`UPDATE jobs SET metadata = NULL WHERE id = ?`, id).Error)
		} else {
			require.NoError(t, db.Exec(`UPDATE jobs SET metadata = '' WHERE id = ?`, id).Error)
		}

		got, err := GetJob(ctx, db, id)
		require.NoError(t, err)
		assert.Equal(t, `{}`, string(got.Metadata))
	})

	t.Run("barrier_until_it_fires", func(t *testing.T) {
		t.Parallel()
		db := open(t)
		ctx := context.Background()
		d := driverForTest(t, db)
		ids := seedClaimable(t, db, 1)
		parent := claimOne(t, d, ctx)
		require.Equal(t, ids[0], parent.ID)
		finalizeJob(t, d, ctx, parent, Result{
			FollowUps: []FollowUp{{Kind: "detail.child", Parent: true, Queue: "children"}},
			Barrier: &Barrier{
				Kind: "detail.cont", Args: map[string]int{"x": 1}, Queue: "cont", ExecutorClass: "gpu", Priority: 7,
			},
		}, nil)

		got, err := GetJob(ctx, db, parent.ID)
		require.NoError(t, err)
		require.NotNil(t, got.Barrier, "a declared barrier that has not fired")
		assert.Equal(t, "detail.cont", got.Barrier.Kind)
		assert.Equal(t, "cont", got.Barrier.Queue)
		assert.Equal(t, ExecutorClass("gpu"), got.Barrier.ExecutorClass)
		assert.Equal(t, 7, got.Barrier.Priority)
		assert.JSONEq(t, `{"x":1}`, string(got.Barrier.Args))

		finalizeJob(t, d, ctx, claimOneFrom(t, d, ctx, "children"), Result{}, nil)
		got, err = GetJob(ctx, db, parent.ID)
		require.NoError(t, err)
		assert.Nil(t, got.Barrier, "a fired barrier is cleared")

		// A spec that does not decode reads as the barrier's kind alone.
		require.NoError(t, db.Exec(`UPDATE jobs SET barrier_kind = 'detail.cont', barrier_spec = '[1]' WHERE id = ?`,
			parent.ID).Error)
		got, err = GetJob(ctx, db, parent.ID)
		require.NoError(t, err)
		assert.Equal(t, &BarrierView{Kind: "detail.cont"}, got.Barrier)
	})

	t.Run("missing_or_soft_deleted", func(t *testing.T) {
		t.Parallel()
		db := open(t)
		ctx := context.Background()
		_, err := GetJob(ctx, db, "no-such-job")
		require.ErrorIs(t, err, ErrJobNotFound)

		id, err := Enqueue(ctx, NewClient(db), "detail.deleted", []byte(`{}`), InsertOpts{})
		require.NoError(t, err)
		require.NoError(t, db.Delete(&jobRow{ID: id}).Error)
		_, err = GetJobWithOptions(ctx, db, id, GetJobOpts{MaxArgsBytes: -1})
		require.ErrorIs(t, err, ErrJobNotFound)
	})
}

// TestGetJobReturnsArgsMetadataKeysAndTheLatestRun proves GetJob reads a job in
// full: its view, args as stored, metadata, keys, timeout, lease, an unfired
// barrier, and the run of the attempt it names.
func TestGetJobReturnsArgsMetadataKeysAndTheLatestRun(t *testing.T) {
	t.Parallel()
	getJobDetailSuite(t, sqliteOpener)
}

// bigArgs returns a JSON object of exactly n bytes.
func bigArgs(n int) []byte {
	const frame = len(`{"blob":""}`)
	return []byte(`{"blob":"` + strings.Repeat("x", n-frame) + `"}`)
}

// oversizedArgsSuite proves GetJob measures args in SQL and leaves a payload past
// its bound in the database: the statement it ran, re-run as captured, returns no
// args at all.
func oversizedArgsSuite(t *testing.T, db *gorm.DB) {
	t.Helper()
	ctx := context.Background()
	id, err := Enqueue(ctx, NewClient(db), "detail.big", bigArgs(2<<20), InsertOpts{})
	require.NoError(t, err)
	stored := storedArgs(t, db, id)

	var got JobDetail
	stmts := captureSQL(t, db, func(db *gorm.DB) {
		got, err = GetJob(ctx, db, id)
		require.NoError(t, err)
	})
	assert.Nil(t, got.Args, "a payload past the default bound is not loaded")
	assert.True(t, got.ArgsOmitted)
	assert.Equal(t, len(stored), got.ArgsBytes, "the payload is measured as stored")

	var read string
	for _, s := range stmts {
		if strings.Contains(s, "args_bytes") {
			read = s
		}
	}
	require.NotEmpty(t, read, "GetJob's statement measures args: %v", stmts)
	var rows []map[string]any
	require.NoError(t, db.Raw(read).Scan(&rows).Error)
	require.Len(t, rows, 1)
	args, ok := rows[0]["args"]
	require.True(t, ok, "the statement selects an args column")
	assert.Nil(t, args, "the payload never left the database")
	assert.EqualValues(t, len(stored), rows[0]["args_bytes"])

	got, err = GetJobWithOptions(ctx, db, id, GetJobOpts{MaxArgsBytes: len(stored)})
	require.NoError(t, err)
	assert.Equal(t, stored, string(got.Args), "a bound equal to the payload's length loads it")
	assert.False(t, got.ArgsOmitted)

	got, err = GetJobWithOptions(ctx, db, id, GetJobOpts{MaxArgsBytes: len(stored) - 1})
	require.NoError(t, err)
	assert.Nil(t, got.Args, "one byte less leaves it")
	assert.True(t, got.ArgsOmitted)
	assert.Equal(t, len(stored), got.ArgsBytes)

	got, err = GetJobWithOptions(ctx, db, id, GetJobOpts{MaxArgsBytes: -1})
	require.NoError(t, err)
	assert.Equal(t, stored, string(got.Args), "a negative bound loads args whatever their size")
	assert.False(t, got.ArgsOmitted)
}

// TestGetJobLeavesOversizedArgsInTheDatabase bounds args in SQL on SQLite.
func TestGetJobLeavesOversizedArgsInTheDatabase(t *testing.T) {
	t.Parallel()
	oversizedArgsSuite(t, newDB(t))
}

// TestGetJobReportsACancelPendingOnACancelledRunningJob proves CancelPending
// reads the job's state and its attempt's run together: true from the cancel of
// a running attempt until that attempt's own finalize, false for a job
// cancelled before any claim.
func TestGetJobReportsACancelPendingOnACancelledRunningJob(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	ctx := context.Background()
	d := driverForTest(t, db)

	id, err := Enqueue(ctx, NewClient(db), "detail.cancel", []byte(`{}`), InsertOpts{Queue: "cancel"})
	require.NoError(t, err)
	raw, runID := claimAndStart(t, ctx, d, "cancel")
	require.NoError(t, CancelJob(ctx, db, id))

	got, err := GetJob(ctx, db, id)
	require.NoError(t, err)
	assert.Equal(t, string(StateCancelled), got.State)
	require.NotNil(t, got.LatestRun)
	assert.Equal(t, string(OutcomeStarted), got.LatestRun.Outcome)
	assert.True(t, got.CancelPending, "the cancelled attempt has not returned")

	out, err := d.Finalize(ctx, raw, runID, Result{}, nil, time.Now())
	require.NoError(t, err)
	require.True(t, out.Superseded, "the cancel took the claim")
	got, err = GetJob(ctx, db, id)
	require.NoError(t, err)
	assert.Equal(t, string(StateCancelled), got.State)
	assert.False(t, got.CancelPending, "the attempt finalized")

	early, err := Enqueue(ctx, NewClient(db), "detail.cancel", []byte(`{}`), InsertOpts{Queue: "never"})
	require.NoError(t, err)
	require.NoError(t, CancelJob(ctx, db, early))
	got, err = GetJob(ctx, db, early)
	require.NoError(t, err)
	assert.Nil(t, got.LatestRun)
	assert.False(t, got.CancelPending, "a job cancelled before any claim has no attempt to wait on")
}
