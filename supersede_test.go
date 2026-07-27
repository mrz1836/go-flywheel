package flywheel

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSupersedeReplacesFinishForALostClaim is A8, and it is written to fail
// against observe-before-finalize.
//
// The defect it pins down is not that a supersede goes unreported. It is that
// the attempt is *mis*reported: the observer is told the outcome before the
// driver decides whether it stands, so a job whose work ran twice shows two
// successes and no signal at all. Asserting "one supersede" alone would pass
// with OnFinish still firing beside it, which is why the OnFinish assertion is
// the load-bearing one here.
func TestSupersedeReplacesFinishForALostClaim(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	driver := NewSQLiteDriver(db)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	worker := &leaseWorker{release: make(chan struct{})}
	reg := NewRegistry()
	Register(reg, worker)
	obs := &recordingObserver{}
	r := heartbeatRunner(t, db, driver, reg, func(c *RunnerConfig) { c.Observer = obs })

	id, err := Insert(ctx, NewClient(db), leaseArgs{V: "x"}, InsertOpts{})
	require.NoError(t, err)

	dispatched := make(chan struct{})
	go func() {
		defer close(dispatched)
		_, _ = r.pollOnce(ctx)
	}()

	// Take the claim away while the worker is mid-flight, then let it finish
	// successfully. Its finalize will match no row.
	require.Eventually(t, func() bool { return worker.entries.Load() == 1 },
		5*time.Second, 5*time.Millisecond, "the worker must be running before its claim is revoked")
	require.NoError(t, CancelJob(ctx, db, id))
	close(worker.release)
	<-dispatched

	_, starts, finishes, retries := obs.snapshot()
	supersedes := obs.snapshotSupersedes()

	require.Len(t, starts, 1, "the attempt started")
	// Asserted before the supersede count on purpose: this is the defect. An
	// OnFinish here is the runtime telling an operator the attempt succeeded and
	// then discarding that outcome.
	assert.Empty(t, finishes,
		"OnSupersede replaces OnFinish: an attempt that advanced nothing must not be counted as finished")
	assert.Empty(t, retries, "no retry was scheduled, so none is reported")
	require.Len(t, supersedes, 1, "exactly one supersede is reported for one lost claim")

	ev := supersedes[0]
	assert.Equal(t, id, ev.JobID)
	assert.Equal(t, "test.block", ev.Kind)
	assert.Equal(t, 1, ev.Attempt)
	assert.Equal(t, starts[0].RunID, ev.RunID, "the supersede names the run the start named")
	assert.Equal(t, OutcomeSuccess, ev.Outcome,
		"the event carries the outcome that was discarded, so no telemetry is lost")
	assert.Equal(t, StateCancelled, ev.State, "and the state the superseding claim left")
	assert.NotEmpty(t, ev.LeaseToken, "and the token the attempt held")
	assert.Positive(t, ev.Duration, "and how long the discarded work took")

	// The job itself is untouched by the attempt that lost its claim.
	assert.Equal(t, string(StateCancelled), jobState(t, db, id))
	assert.Nil(t, leaseToken(t, db, id), "the cancelled job holds no claim")
}

// TestSupersedeIsNotEmittedForAHeldClaim is the other side of the contract: an
// ordinary attempt reports exactly one OnFinish and no supersede. Without it,
// an implementation that emitted both events for every attempt would satisfy
// the test above.
func TestSupersedeIsNotEmittedForAHeldClaim(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	reg := NewRegistry()
	Register(reg, &successWorker{})
	obs := &recordingObserver{}
	r := newObservedRunner(t, db, reg, obs)
	ctx := context.Background()

	_, err := Insert(ctx, NewClient(db), successArgs{V: "x"}, InsertOpts{})
	require.NoError(t, err)
	runToIdle(t, ctx, r)

	_, starts, finishes, _ := obs.snapshot()
	require.Len(t, starts, 1)
	require.Len(t, finishes, 1, "a held claim finishes normally")
	assert.Equal(t, OutcomeSuccess, finishes[0].Outcome)
	assert.Empty(t, obs.snapshotSupersedes(), "nothing was superseded, so nothing is reported as such")
}

// TestSupersedeOnFinishReportsThePersistedOutcome closes the remaining half of
// the contract. OnSupersede covers the case where nothing was persisted; this covers
// the case where something was, and asserts the observer's view matches it
// rather than a plan computed independently.
//
// The retry path is the sharpest version: the runner and the driver each decided
// "retryable with a delay", and if those two derivations ever diverged, the
// observer would report a schedule the database does not have.
func TestSupersedeOnFinishReportsThePersistedOutcome(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	reg := NewRegistry()
	Register(reg, &retryWorker{failuresBefore: 1})
	obs := &recordingObserver{}
	r := newObservedRunner(t, db, reg, obs)
	ctx := context.Background()

	id, err := Insert(ctx, NewClient(db), retryArgs{V: "x"}, InsertOpts{})
	require.NoError(t, err)
	runToIdle(t, ctx, r)

	_, _, finishes, retries := obs.snapshot()
	require.Len(t, finishes, 2, "one failed attempt and one successful one")
	assert.Equal(t, OutcomeError, finishes[0].Outcome)
	assert.Equal(t, OutcomeSuccess, finishes[1].Outcome)
	require.Len(t, retries, 1)
	assert.Equal(t, ErrorTransient, retries[0].ErrorClass)
	assert.Empty(t, obs.snapshotSupersedes())

	// What the observer reported is what the audit trail holds.
	runs, err := ListRuns(ctx, db, id, ListRunsParams{})
	require.NoError(t, err)
	require.Len(t, runs, 2)
	got := []string{runs[0].Outcome, runs[1].Outcome}
	assert.ElementsMatch(t, []string{string(OutcomeError), string(OutcomeSuccess)}, got,
		"every outcome the observer reported was persisted")
	assert.Equal(t, string(StateSucceeded), jobState(t, db, id))
}
