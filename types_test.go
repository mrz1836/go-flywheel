package flywheel

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestEnumWireValuesAreStable pins every enum's persisted string to the literal
// the schema has always stored. The wire values are the stable contract for the
// jobs and job_runs columns, so an existing row round-trips unchanged when a
// host adopts these enums in place of its own.
func TestEnumWireValuesAreStable(t *testing.T) {
	t.Parallel()

	assert.Equal(t, "available", string(StateAvailable))
	assert.Equal(t, "running", string(StateRunning))
	assert.Equal(t, "retryable", string(StateRetryable))
	assert.Equal(t, "scheduled", string(StateScheduled))
	assert.Equal(t, "succeeded", string(StateSucceeded))
	assert.Equal(t, "cancelled", string(StateCancelled))
	assert.Equal(t, "discarded", string(StateDiscarded))

	assert.Equal(t, "", string(AnyClass), "the wildcard executor class is the empty string")

	assert.Equal(t, "transient", string(ErrorTransient))
	assert.Equal(t, "permanent", string(ErrorPermanent))
	assert.Equal(t, "validation", string(ErrorValidation))
	assert.Equal(t, "timeout", string(ErrorTimeout))

	assert.Equal(t, "started", string(OutcomeStarted))
	assert.Equal(t, "success", string(OutcomeSuccess))
	assert.Equal(t, "error", string(OutcomeError))
	assert.Equal(t, "snooze", string(OutcomeSnooze))
	assert.Equal(t, "cancelled", string(OutcomeCancelled))
	assert.Equal(t, "timeout", string(OutcomeTimeout))
	assert.Equal(t, "crashed", string(OutcomeCrashed))
}

// TestEnumValid proves each enum's Valid method accepts its recognized values
// and rejects an invented one.
func TestEnumValid(t *testing.T) {
	t.Parallel()

	assert.True(t, StateAvailable.Valid())
	assert.False(t, JobState("invented").Valid())

	assert.True(t, ErrorTransient.Valid())
	assert.False(t, ErrorClass("invented").Valid())

	assert.True(t, OutcomeSuccess.Valid())
	assert.False(t, RunOutcome("invented").Valid())
}
