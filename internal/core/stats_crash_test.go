package core

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestStatsCountACrashDiscardAsADiscardAfterALateFinalize proves a crash that
// ended its job counts as one crashed attempt and one discard, never as a retry.
// It holds for the run as the sweep left it and for the same run after the
// attempt finished late: the late, superseded finalize writes its own outcome but
// keeps the state the sweep recorded. Each form is read raw and again from the
// rollup, and the two reads agree.
func TestStatsCountACrashDiscardAsADiscardAfterALateFinalize(t *testing.T) {
	t.Parallel()

	h := hourAt(rollupBase, 0)
	for _, tc := range []struct {
		name string
		run  histRun
	}{
		{
			name: "as the sweep left it",
			run: histRun{
				Kind: "crashy", Outcome: OutcomeCrashed, JobState: StateDiscarded,
				FinishedAt: h.Add(10 * time.Minute), DurationMs: -1, WaitMs: 5,
			},
		},
		{
			name: "after a late finalize",
			run: histRun{
				Kind: "crashy", Outcome: OutcomeSuccess, JobState: StateDiscarded, Superseded: true,
				FinishedAt: h.Add(10 * time.Minute), DurationMs: 900, WaitMs: 5, CostMicros: 7,
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			db := newDB(t)
			seedHistory(t, db, []histRun{tc.run})

			check := func(served string) {
				t.Helper()
				res, err := Stats(context.Background(), db, StatsParams{From: h, To: h.Add(time.Hour)})
				require.NoError(t, err)
				ks := mustKind(t, res, "crashy")
				assert.EqualValues(t, 1, ks.Attempts, "%s: one attempt", served)
				assert.EqualValues(t, 1, ks.Outcomes.Crashed, "%s: the attempt crashed", served)
				assert.Zero(t, ks.Outcomes.Success, "%s: the late outcome is not counted", served)
				assert.EqualValues(t, 1, ks.Discarded, "%s: the crash ended the job", served)
				assert.Zero(t, ks.Retries, "%s: nothing ran again", served)
				assert.Zero(t, ks.Superseded, "%s: the sweep, not a cancel, took the claim", served)
				assert.Zero(t, ks.Duration.Count, "%s: a crash has no success duration", served)
				assert.Zero(t, ks.CostMicros, "%s: the late cost is not counted", served)
			}

			check("raw")
			res := rollupAll(t, db, h.Add(2*time.Hour))
			require.Equal(t, []time.Time{h}, res.Rolled)
			check("rolled up")
		})
	}
}
