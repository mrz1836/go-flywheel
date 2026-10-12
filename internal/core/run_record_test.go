package core

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

// recordArgs carries what a run-record worker should do with its attempt.
type recordArgs struct {
	// OutputBytes, when positive, returns a string output of that many bytes.
	OutputBytes int `json:"output_bytes"`
	// ErrorBytes, when positive, fails with a message of that many bytes.
	ErrorBytes int `json:"error_bytes"`
	// Panic panics from three calls deep.
	Panic bool `json:"panic"`
}

func (recordArgs) Kind() string { return "run_record.worker" }

// recordWorker does what its args ask, so one registration covers every
// run-record case.
type recordWorker struct{}

func (recordWorker) Kind() string              { return "run_record.worker" }
func (recordWorker) Classify(error) ErrorClass { return ErrorPermanent }

func (recordWorker) Work(_ context.Context, job *Job[recordArgs]) (Result, error) {
	switch {
	case job.Args.Panic:
		recordPanicOuter()
	case job.Args.ErrorBytes > 0:
		return Result{}, errors.New(strings.Repeat("e", job.Args.ErrorBytes))
	case job.Args.OutputBytes > 0:
		return Result{Output: strings.Repeat("o", job.Args.OutputBytes)}, nil
	}
	return Result{}, nil
}

// recordPanicOuter, recordPanicMiddle, and recordPanicInner are the three calls
// a panicking worker goes through, so a recorded stack must name the innermost.
func recordPanicOuter()  { recordPanicMiddle() }
func recordPanicMiddle() { recordPanicInner() }
func recordPanicInner()  { panic("boom") }

// runRecordJob enqueues one recordArgs job, runs it to idle on a runner over
// driver, and returns the job's id and its single stored run.
func runRecordJob(t *testing.T, db *gorm.DB, driver Driver, args recordArgs) (string, jobRunRow) {
	t.Helper()
	ctx := context.Background()
	reg := NewRegistry()
	Register[recordArgs](reg, recordWorker{})
	r, err := NewRunner(RunnerConfig{
		DB: db, Driver: driver, Registry: reg, Queues: []string{"default"},
		ExecutorClass: "local", ClaimAnyClass: true, Concurrency: 1, PollInterval: 2 * time.Millisecond,
	})
	require.NoError(t, err)

	id, err := Insert(ctx, NewClient(db), args, InsertOpts{MaxAttempts: 1})
	require.NoError(t, err)
	runToIdle(t, ctx, r)

	var run jobRunRow
	require.NoError(t, db.Model(&jobRunRow{}).Where("job_id = ?", id).First(&run).Error)
	return id, run
}

// decodeObject unmarshals a stored JSON object, failing the test on anything else.
func decodeObject(t *testing.T, raw []byte) map[string]any {
	t.Helper()
	var obj map[string]any
	require.NoError(t, json.Unmarshal(raw, &obj), "the stored value is a JSON object (%d bytes)", len(raw))
	return obj
}

// assertOversizedOutputIsStoredAsATruncationMarker runs a worker whose output
// marshals to more than the default cap and asserts the attempt still finalizes,
// with a small marker stored in its place that says how big the output was and
// keeps its head.
func assertOversizedOutputIsStoredAsATruncationMarker(t *testing.T, db *gorm.DB, driver Driver) {
	t.Helper()
	const size = 100_000
	id, run := runRecordJob(t, db, driver, recordArgs{OutputBytes: size})

	assert.Equal(t, string(StateSucceeded), jobState(t, db, id), "the attempt finalizes whatever its output's size")
	assert.Equal(t, string(OutcomeSuccess), run.Outcome)
	assert.Less(t, len(run.Output), 2048, "the stored output is a small marker, not the output")

	marker := decodeObject(t, run.Output)
	assert.Len(t, marker, 3, "the marker has exactly its three keys: %v", marker)
	assert.Equal(t, true, marker["$truncated"])
	assert.EqualValues(t, size+2, marker["$bytes"], "the size of the output as marshaled, quotes included")
	head, ok := marker["$head"].(string)
	require.True(t, ok, "the marker keeps the head of the marshaled output")
	assert.Equal(t, `"`+strings.Repeat("o", 1023), head, "the first 1,024 bytes of the marshaled output")

	view := latestRunView(t, db, id)
	assert.True(t, view.OutputTruncated, "the view reports the marker")
	assert.False(t, view.ErrorTruncated)
}

// assertLongErrorIsFlaggedTruncated runs a worker that fails with a
// 10,000-byte message and asserts the stored message is cut at the cap while the
// payload says it was cut, and how long it was; a short message carries no flag.
func assertLongErrorIsFlaggedTruncated(t *testing.T, db *gorm.DB, driver Driver) {
	t.Helper()
	const size = 10_000
	_, run := runRecordJob(t, db, driver, recordArgs{ErrorBytes: size})

	require.NotNil(t, run.ErrorMessage)
	assert.Len(t, *run.ErrorMessage, maxErrorMessage, "the stored message is cut at the cap")
	payload := decodeObject(t, run.ErrorPayload)
	assert.Equal(t, *run.ErrorMessage, payload["message"])
	assert.Equal(t, true, payload["truncated"], "the payload says the message was cut")
	assert.EqualValues(t, size, payload["message_bytes"], "and how long it was before the cut")

	view := latestRunView(t, db, run.JobID)
	assert.True(t, view.ErrorTruncated, "the view reports the cut")
	assert.False(t, view.OutputTruncated)

	_, short := runRecordJob(t, db, driver, recordArgs{ErrorBytes: 10})
	whole := decodeObject(t, short.ErrorPayload)
	assert.NotContains(t, whole, "truncated", "a whole message carries no flag")
	assert.NotContains(t, whole, "message_bytes")
	assert.False(t, latestRunView(t, db, short.JobID).ErrorTruncated, "nor does its view")
}

// assertRecoveredPanicKeepsItsStack runs a worker that panics from three calls
// deep and asserts the message stays the short panic text while the payload
// carries a bounded stack that names where it panicked.
func assertRecoveredPanicKeepsItsStack(t *testing.T, db *gorm.DB, driver Driver) {
	t.Helper()
	_, run := runRecordJob(t, db, driver, recordArgs{Panic: true})

	require.NotNil(t, run.ErrorMessage)
	assert.Equal(t, "jobs: worker panicked: boom", *run.ErrorMessage, "the message stays short")
	payload := decodeObject(t, run.ErrorPayload)
	assert.Equal(t, *run.ErrorMessage, payload["message"])
	stack, ok := payload["stack"].(string)
	require.True(t, ok, "the payload carries the recovered panic's stack: %v", payload)
	assert.Contains(t, stack, "recordPanicInner", "the stack names the frame that panicked")
	assert.Contains(t, stack, "recordPanicOuter")
	assert.LessOrEqual(t, len(stack), 16_384, "the stack is bounded")
	assert.True(t, utf8.ValidString(stack))

	view := latestRunView(t, db, run.JobID)
	assert.Equal(t, stack, view.ErrorStack, "the view reports the stack")
	assert.False(t, view.ErrorTruncated)
}

// assertJobRunViewCarriesTheStoredErrorPayload runs a failed attempt, a cut
// message, a panic, and a success, and expects each run's view to carry its
// error_payload exactly as stored, so a host can show the whole payload, and no
// payload at all for the success.
func assertJobRunViewCarriesTheStoredErrorPayload(t *testing.T, db *gorm.DB, driver Driver) {
	t.Helper()
	viewOf := func(args recordArgs) (jobRunRow, JobRunView, map[string]any) {
		t.Helper()
		id, run := runRecordJob(t, db, driver, args)
		view := latestRunView(t, db, id)
		require.NotEmpty(t, view.ErrorPayload, "a failed run's view carries its payload")
		assert.Equal(t, []byte(run.ErrorPayload), []byte(view.ErrorPayload), "the payload is the column as stored")
		return run, view, decodeObject(t, view.ErrorPayload)
	}

	failed, _, payload := viewOf(recordArgs{ErrorBytes: 10})
	assert.Equal(t, map[string]any{"message": *failed.ErrorMessage}, payload, "a whole message is the only key")

	_, _, payload = viewOf(recordArgs{ErrorBytes: 10_000})
	assert.Equal(t, true, payload["truncated"], "a cut message's payload says it was cut")
	assert.EqualValues(t, 10_000, payload["message_bytes"], "and how long it was")

	_, panicked, payload := viewOf(recordArgs{Panic: true})
	assert.Equal(t, "jobs: worker panicked: boom", payload["message"])
	assert.Equal(t, panicked.ErrorStack, payload["stack"], "a panic's payload carries its stack")

	id, _ := runRecordJob(t, db, driver, recordArgs{})
	succeeded := latestRunView(t, db, id)
	assert.Nil(t, succeeded.ErrorPayload, "a successful run has no payload")
	encoded, err := json.Marshal(succeeded)
	require.NoError(t, err)
	assert.NotContains(t, decodeObject(t, encoded), "error_payload", "and its JSON leaves the key out")
}

// latestRunView reads a job's newest run through the public view.
func latestRunView(t *testing.T, db *gorm.DB, jobID string) JobRunView {
	t.Helper()
	view, ok, err := LatestRun(context.Background(), db, jobID)
	require.NoError(t, err)
	require.True(t, ok, "the job has a run")
	return view
}

// assertMaxOutputBytesSetsTheCap runs outputs either side of a configured cap
// and of the default one, and expects the marker exactly when the marshaled
// output is larger than the cap in force.
func assertMaxOutputBytesSetsTheCap(t *testing.T, db *gorm.DB, newDriver func(DriverOpts) Driver) {
	t.Helper()
	cases := []struct {
		name     string
		opts     DriverOpts
		bytes    int // the string output's length; it marshals to two more
		marker   bool
		capLabel string
	}{
		{"under a configured cap", DriverOpts{MaxOutputBytes: 1024}, 1022, false, "1,024"},
		{"over a configured cap", DriverOpts{MaxOutputBytes: 1024}, 1023, true, "1,024"},
		{"at the default cap", DriverOpts{}, 65_534, false, "65,536"},
		{"over the default cap", DriverOpts{}, 65_535, true, "65,536"},
		{"a negative cap selects the default", DriverOpts{MaxOutputBytes: -1}, 65_534, false, "65,536"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			id, run := runRecordJob(t, db, newDriver(tc.opts), recordArgs{OutputBytes: tc.bytes})
			assert.Equal(t, string(StateSucceeded), jobState(t, db, id))
			view := latestRunView(t, db, id)
			assert.Equal(t, tc.marker, view.OutputTruncated, "a %d-byte output against a cap of %s", tc.bytes+2, tc.capLabel)
			if tc.marker {
				var marker TruncatedOutput
				require.NoError(t, json.Unmarshal(run.Output, &marker))
				assert.Equal(t, TruncatedOutput{Truncated: true, Bytes: tc.bytes + 2, Head: marker.Head}, marker)
				assert.LessOrEqual(t, len(marker.Head), 1024)
				return
			}
			var whole string
			require.NoError(t, json.Unmarshal(run.Output, &whole), "an output within the cap is stored as returned")
			assert.Len(t, whole, tc.bytes)
		})
	}
}

func TestMaxOutputBytesSetsTheCap(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	assertMaxOutputBytesSetsTheCap(t, db, func(o DriverOpts) Driver {
		d, err := NewSQLiteDriverWithOptions(db, SQLiteOpts{DriverOpts: o})
		require.NoError(t, err)
		return d
	})
}

// TestSeedRunRecordsOutputAndErrorsLikeAFinalize proves SeedRun stores what a
// finalize would: an output over the default cap as the marker, and a long
// error cut and flagged.
func TestSeedRunRecordsOutputAndErrorsLikeAFinalize(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	ctx := context.Background()
	seedJob(t, db, jobRow{ID: "seeded-record", Kind: "k", State: string(StateDiscarded), ScheduledAt: runRecordAnchor})

	_, err := SeedRun(ctx, db, RunSeed{
		JobID: "seeded-record", Attempt: 1, ExecutorID: "import", Outcome: OutcomeError,
		StartedAt: runRecordAnchor, Output: strings.Repeat("s", 70_000), ErrorMessage: strings.Repeat("m", 5_000),
	})
	require.NoError(t, err)

	view := latestRunView(t, db, "seeded-record")
	assert.True(t, view.OutputTruncated, "SeedRun applies the default output cap")
	assert.True(t, view.ErrorTruncated, "and flags a cut message")
	require.NotNil(t, view.Error)
	assert.Len(t, *view.Error, maxErrorMessage)
}

// TestJobRunViewReportsTruncationAndStack reads hand-written rows through
// ListRuns: the view flags an output that is exactly the marker and an error
// payload that says it was cut, and nothing else.
func TestJobRunViewReportsTruncationAndStack(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	ctx := context.Background()
	const job = "view-flags"
	seedJob(t, db, jobRow{ID: job, Kind: "k", State: string(StateRetryable), Attempt: 6, ScheduledAt: runRecordAnchor})

	rows := []struct {
		output, payload            string
		outputTruncated, errorFlag bool
		stack                      string
	}{
		{`{"$truncated": true, "$bytes": 100002, "$head": "\"ooo"}`, `{"message": "m", "truncated": true, "message_bytes": 9000}`, true, true, ""},
		{`{"$truncated": true, "$bytes": 5, "$head": "x", "mine": 1}`, `{"message": "short"}`, false, false, ""},
		{`{"$truncated": "yes", "$bytes": 5, "$head": "x"}`, `{"message": "m", "truncated": "yes"}`, false, false, ""},
		{`"a string that mentions $truncated"`, `not json`, false, false, ""},
		{``, ``, false, false, ""},
		{``, `{"message": "jobs: worker panicked: boom", "stack": "goroutine 7 [running]:\nmain.inner()"}`, false, false, "goroutine 7 [running]:\nmain.inner()"},
	}
	for i, r := range rows {
		run := jobRunRow{
			ID: job + "-" + string(rune('a'+i)), JobID: job, Attempt: i + 1, ExecutorID: "h",
			StartedAt: runRecordAnchor, Outcome: string(OutcomeError), CreatedAt: runRecordAnchor,
		}
		if r.output != "" {
			run.Output = []byte(r.output)
		}
		if r.payload != "" {
			run.ErrorPayload = []byte(r.payload)
		}
		seedRun(t, db, run)
	}

	views, err := ListRuns(ctx, db, job, ListRunsParams{})
	require.NoError(t, err)
	require.Len(t, views, len(rows))
	for _, v := range views {
		r := rows[v.Attempt-1]
		assert.Equal(t, r.outputTruncated, v.OutputTruncated, "attempt %d's output flag: %s", v.Attempt, r.output)
		assert.Equal(t, r.errorFlag, v.ErrorTruncated, "attempt %d's error flag: %s", v.Attempt, r.payload)
		assert.Equal(t, r.stack, v.ErrorStack, "attempt %d's stack: %s", v.Attempt, r.payload)
	}
}

// sqliteRecordDriver is the SQLite driver the run-record tests run on.
func sqliteRecordDriver(t *testing.T, db *gorm.DB) Driver {
	t.Helper()
	return NewSQLiteDriver(db)
}

func TestOversizedOutputIsStoredAsATruncationMarker(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	assertOversizedOutputIsStoredAsATruncationMarker(t, db, sqliteRecordDriver(t, db))
}

func TestLongErrorIsFlaggedTruncated(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	assertLongErrorIsFlaggedTruncated(t, db, sqliteRecordDriver(t, db))
}

func TestRecoveredPanicKeepsItsStack(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	assertRecoveredPanicKeepsItsStack(t, db, sqliteRecordDriver(t, db))
}

func TestJobRunViewCarriesTheStoredErrorPayload(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	assertJobRunViewCarriesTheStoredErrorPayload(t, db, sqliteRecordDriver(t, db))
}

// runRecordAnchor keeps the seeded-row timestamps of the view tests fixed.
//
//nolint:gochecknoglobals // shared deterministic test anchor
var runRecordAnchor = time.Date(2026, 7, 2, 9, 0, 0, 0, time.UTC)
