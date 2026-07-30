//go:build loadtest

package loadtest

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestHarnessClockAdvancesForwardOnly pins the time-compression seam: the clock
// starts at its base, moves forward when advanced, and never moves backward — the
// property the advancer relies on to release retry generations in order.
func TestHarnessClockAdvancesForwardOnly(t *testing.T) {
	t.Parallel()
	c := newHarnessClock()
	ctx := context.Background()

	assert.Equal(t, simClockBase, c.Now(ctx), "a fresh clock reads its base")
	assert.Equal(t, time.Duration(0), c.span(), "a fresh clock has advanced nothing")

	c.advanceTo(simClockBase.Add(time.Hour))
	assert.Equal(t, simClockBase.Add(time.Hour), c.Now(ctx), "it moves to the advanced instant")
	assert.Equal(t, time.Hour, c.span(), "span tracks how far it has moved")

	c.advanceTo(simClockBase.Add(30 * time.Minute))
	assert.Equal(t, simClockBase.Add(time.Hour), c.Now(ctx),
		"advancing to an earlier instant is a no-op: the clock never runs backward")
}

// TestSimClockBaseSitsPastTheSeedEpoch guards the one invariant that makes the
// outage measurement claimable from sim-time zero: every bulk-seeded row's
// scheduled_at (anchored at seedEpoch) must already be in the past when the
// runners first read the simulated clock.
func TestSimClockBaseSitsPastTheSeedEpoch(t *testing.T) {
	t.Parallel()
	assert.True(t, simClockBase.After(seedEpoch),
		"the sim clock must start past the seed epoch, or seeded jobs are not yet due")
}

// TestOutageStateFailsInsideItsWindow pins the switch the worker reads: an
// un-begun outage never fails, and a begun one fails exactly until the simulated
// clock passes its end.
func TestOutageStateFailsInsideItsWindow(t *testing.T) {
	t.Parallel()
	var o outageState
	base := time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC)

	assert.False(t, o.failing(base), "no outage has begun, so nothing fails")
	assert.True(t, o.end().IsZero(), "an un-begun outage has no end instant")

	o.begin(base.Add(time.Hour))
	assert.True(t, o.failing(base), "an attempt inside the window fails")
	assert.True(t, o.failing(base.Add(59*time.Minute)), "still inside the window")
	assert.False(t, o.failing(base.Add(time.Hour)), "at the boundary the outage has lifted")
	assert.False(t, o.failing(base.Add(2*time.Hour)), "past the window nothing fails")
	assert.True(t, o.end().Equal(base.Add(time.Hour)), "end reports the lift instant")
}

// TestDownstreamOutageIsClockDrivenAndValidates covers the fault's contract: it
// carries the clock-driven marker, describes itself, and rejects the runs it
// cannot be measured in — a zero window, a mix that drains nothing, and simulated
// work that would sleep in wall time while the clock is compressed.
func TestDownstreamOutageIsClockDrivenAndValidates(t *testing.T) {
	t.Parallel()

	var f Fault = DownstreamOutage{For: 4 * time.Hour}
	_, ok := f.(clockDrivenFault)
	assert.True(t, ok, "DownstreamOutage must be recognized as clock-driven")

	assert.Contains(t, f.Describe(), "4h", "the description names the outage length")
	assert.InEpsilon(t, 0.5, f.At(), 1e-9, "At only satisfies the (0,1) config check")
	assert.Equal(t, 4*time.Hour, f.(DownstreamOutage).Window())

	base := Config{Mix: WorkloadDrainOnly}
	require.NoError(t, DownstreamOutage{For: time.Hour}.Validate(base))

	assert.Error(t, DownstreamOutage{For: 0}.Validate(base), "a zero window is a fault that never happens")
	assert.Error(t, DownstreamOutage{For: time.Hour}.Validate(Config{Mix: WorkloadEnqueueOnly}),
		"the enqueue mix starts no runner, so there is nothing to retry")
	assert.Error(t, DownstreamOutage{For: time.Hour}.Validate(Config{Mix: WorkloadDrainOnly, WorkDuration: time.Millisecond}),
		"simulated work would sleep in wall time while the clock jumped hours")
}

// TestBackoffReportRoundTripsThroughJSON proves the outage account and the two new
// config knobs survive the wire form the committed benchmark artifacts use.
func TestBackoffReportRoundTripsThroughJSON(t *testing.T) {
	t.Parallel()

	src := Report{
		Config: Config{
			DSN: testDSN, Jobs: 10_000, Mix: WorkloadDrainOnly, Indexes: IndexesFull,
			SampleInterval: time.Second, Timeout: 30 * time.Minute,
			Queue: "default", ExecutorClass: "loadtest",
			MaxAttempts: 8, MaxRetryBackoff: 30 * time.Minute,
		},
		Backoff: &BackoffReport{
			MaxRetryBackoff: "30m0s", OutageWindow: "4h0m0s", SimSpanCovered: "3m51s",
			Cohort: 10_000, MaxAttempts: 8, Attempts: 80_000, AttemptsPerJob: 8,
			TransientFailures: 80_000, Drained: 0, Discarded: 10_000,
		},
	}

	data, err := json.Marshal(src)
	require.NoError(t, err)

	var got Report
	require.NoError(t, json.Unmarshal(data, &got))

	require.NotNil(t, got.Backoff, "the backoff account must survive the round trip")
	assert.Equal(t, *src.Backoff, *got.Backoff)
	assert.Equal(t, 8, got.Config.MaxAttempts, "the seeded budget round-trips")
	assert.Equal(t, 30*time.Minute, got.Config.MaxRetryBackoff, "the cap round-trips")
}
