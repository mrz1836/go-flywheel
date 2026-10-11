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

	_, short := runRecordJob(t, db, driver, recordArgs{ErrorBytes: 10})
	whole := decodeObject(t, short.ErrorPayload)
	assert.NotContains(t, whole, "truncated", "a whole message carries no flag")
	assert.NotContains(t, whole, "message_bytes")
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
