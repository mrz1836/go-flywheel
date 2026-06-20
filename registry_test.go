package flywheel

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// noopArgs is the minimal Args type a unit-only worker needs.
type noopArgs struct {
	Value string `json:"value"`
}

// noopWorker implements [Worker] with no side effects so registry lookup can
// be exercised end-to-end without a database.
type noopWorker struct {
	kind   string
	called bool
	got    noopArgs
}

func (w *noopWorker) Kind() string { return w.kind }
func (w *noopWorker) Work(_ context.Context, job *Job[noopArgs]) (Result, error) {
	w.called = true
	w.got = job.Args
	return Result{}, nil
}

func TestRegistryRegisterAndLookupRoundTripsTypedArgs(t *testing.T) {
	t.Parallel()
	reg := NewRegistry()
	w := &noopWorker{kind: "test.noop"}
	Register(reg, w)

	entry, ok := reg.lookup("test.noop")
	require.True(t, ok, "registered kind must be looked up")

	_, err := entry.dispatch(context.Background(), dispatchInput{
		ID:      "job-1",
		Kind:    "test.noop",
		Queue:   "default",
		RawArgs: []byte(`{"value":"hello"}`),
	})

	require.NoError(t, err)
	assert.True(t, w.called, "dispatch must invoke the underlying worker")
	assert.Equal(t, "hello", w.got.Value, "RawArgs JSON must decode into the worker's typed Args")
}

func TestRegistryLookupUnknownKindReportsMiss(t *testing.T) {
	t.Parallel()
	reg := NewRegistry()
	_, ok := reg.lookup("unregistered")
	assert.False(t, ok)
}

func TestRegistryDuplicateRegistrationPanics(t *testing.T) {
	t.Parallel()
	reg := NewRegistry()
	Register(reg, &noopWorker{kind: "dup"})

	assert.PanicsWithValue(t, `jobs: duplicate worker registration for kind "dup"`, func() {
		Register(reg, &noopWorker{kind: "dup"})
	})
}
