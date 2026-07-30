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
	assert.Equal(t, "paused", string(StatePaused))
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

// TestPausedIsNonTerminalButNotClaimable pins the one category the state machine
// gained with paused: a state that is neither claimable nor terminal. A runner
// must never claim a paused job, and a "still in flight" scope must still count
// it — so it belongs to nonTerminalStates and NonTerminalStates but not to
// claimableStates or TerminalStates.
func TestPausedIsNonTerminalButNotClaimable(t *testing.T) {
	t.Parallel()

	assert.NotContains(t, claimableStates, string(StatePaused),
		"a paused job must never be claimed")
	assert.Contains(t, nonTerminalStates, string(StatePaused),
		"a paused job is unfinished work, so RunUntilIdle keeps polling around it")

	nonTerminal := map[JobState]bool{}
	for _, s := range NonTerminalStates() {
		nonTerminal[s] = true
	}
	assert.True(t, nonTerminal[StatePaused], "paused is a non-terminal state")

	for _, terminal := range TerminalStates() {
		assert.NotEqual(t, StatePaused, terminal, "paused is not a terminal state")
	}
}

// TestEnumValid proves each enum's Valid method accepts its recognized values
// and rejects an invented one.
func TestEnumValid(t *testing.T) {
	t.Parallel()

	assert.True(t, StateAvailable.Valid())
	assert.True(t, StatePaused.Valid())
	assert.False(t, JobState("invented").Valid())

	assert.True(t, ErrorTransient.Valid())
	assert.False(t, ErrorClass("invented").Valid())

	assert.True(t, OutcomeSuccess.Valid())
	assert.False(t, RunOutcome("invented").Valid())
}
