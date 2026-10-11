package core

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

// strictArgs is an args type whose field a malformed payload cannot fill.
type strictArgs struct {
	N int `json:"n"`
}

// strictWorker records whether it ran, and classifies every error it is shown
// as transient: a classifier that would keep a decode failure retrying if the
// runtime asked it.
type strictWorker struct {
	ran        bool
	classified []error
}

func (*strictWorker) Kind() string { return "decode.strict" }

func (w *strictWorker) Work(context.Context, *Job[strictArgs]) (Result, error) {
	w.ran = true
	return Result{}, nil
}

func (w *strictWorker) Classify(err error) ErrorClass {
	w.classified = append(w.classified, err)
	return ErrorTransient
}

// undecodableArgsSuite enqueues args that cannot decode into the worker's type,
// with the default budget of 25 attempts, and runs one attempt: the job is
// discarded on it, its run is classified validation, and the run keeps the
// decode error's text, naming the kind.
func undecodableArgsSuite(t *testing.T, db *gorm.DB) {
	t.Helper()
	ctx := context.Background()
	worker := &strictWorker{}
	reg := NewRegistry()
	Register[strictArgs](reg, worker)
	r, err := NewRunner(RunnerConfig{
		DB: db, Driver: driverForTest(t, db), Registry: reg, Queues: []string{defaultQueue},
		ExecutorClass: "local", Concurrency: 1,
	})
	require.NoError(t, err)

	id, err := Enqueue(ctx, NewClient(db), "decode.strict", []byte(`{"n":"not a number"}`), InsertOpts{})
	require.NoError(t, err)

	claimed, err := r.pollOnce(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, claimed)

	assert.False(t, worker.ran, "the worker never runs on args it cannot receive")
	assert.Empty(t, worker.classified, "the worker's classifier is not asked about a decode failure")

	row := jobRowByID(t, db, id)
	assert.Equal(t, string(StateDiscarded), row.State, "args that cannot decode never will, so the job ends at once")
	assert.Equal(t, 1, row.Attempt)
	assert.Equal(t, defaultMaxAttempts, row.MaxAttempts, "the budget was there; the class is what ends the job")

	runs := runsOf(t, db, id)
	require.Len(t, runs, 1)
	assert.Equal(t, string(OutcomeError), runs[0].Outcome)
	require.NotNil(t, runs[0].ErrorClass)
	assert.Equal(t, string(ErrorValidation), *runs[0].ErrorClass)
	require.NotNil(t, runs[0].ErrorMessage)
	assert.Regexp(t, `^jobs: decode args for kind "decode\.strict": json: cannot unmarshal string `, *runs[0].ErrorMessage,
		"the run keeps the decode error's text")
	require.NotNil(t, runs[0].JobState)
	assert.Equal(t, string(StateDiscarded), *runs[0].JobState)
}

// TestUndecodableArgsDiscardOnTheFirstAttemptWhateverTheClassifierSays proves a
// decode failure is a validation error the worker's Classifier cannot override:
// the worker never ran, so it has nothing to decide.
func TestUndecodableArgsDiscardOnTheFirstAttemptWhateverTheClassifierSays(t *testing.T) {
	t.Parallel()
	undecodableArgsSuite(t, newDB(t))
}
