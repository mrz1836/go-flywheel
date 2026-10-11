package observers

import (
	"context"
	"log/slog"
	"testing"
	"time"

	flywheel "github.com/mrz1836/go-flywheel"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSlogLogsASweepDiscardAtError proves a crashed attempt the sweep reclaimed
// logs the line any failed attempt logs: at error when the crash ended its job,
// at warn when the job runs again, each with the job's fields and the outcome
// crashed, and both before the pass line. An info-level handler sees all three.
func TestSlogLogsASweepDiscardAtError(t *testing.T) {
	t.Parallel()
	h := &capturingHandler{level: slog.LevelInfo}
	obs := NewSlog(slog.New(h))

	obs.OnSweep(context.Background(), flywheel.SweepEvent{
		Duration:  3 * time.Millisecond,
		Reclaimed: 2,
		Discarded: 1,
		Jobs: []flywheel.SweptJob{
			{
				JobEvent:    flywheel.JobEvent{JobID: "j-last", RunID: "r-last", Kind: "k", Queue: "q", Attempt: 3},
				MaxAttempts: 3, State: flywheel.StateDiscarded,
			},
			{
				JobEvent:    flywheel.JobEvent{JobID: "j-left", RunID: "r-left", Kind: "k", Queue: "q", Attempt: 1},
				MaxAttempts: 3, State: flywheel.StateAvailable,
			},
		},
	})

	records := h.snapshot()
	require.Len(t, records, 3, "one line per reclaimed job, then the pass line")

	assert.Equal(t, slog.LevelError, records[0].Level)
	assert.Equal(t, msgJobDiscarded, records[0].Message)
	assert.Equal(t, map[string]string{
		"job_id": "j-last", "run_id": "r-last", "kind": "k", "queue": "q", "outcome": "crashed",
		"attempt": "3", "max_attempts": "3", "job_state": "discarded",
	}, stringAttrs(records[0]))

	assert.Equal(t, slog.LevelWarn, records[1].Level)
	assert.Equal(t, msgJobAttemptFailed, records[1].Message)
	assert.Equal(t, map[string]string{
		"job_id": "j-left", "run_id": "r-left", "kind": "k", "queue": "q", "outcome": "crashed",
		"attempt": "1", "max_attempts": "3", "job_state": "available",
	}, stringAttrs(records[1]))

	assert.Equal(t, slog.LevelInfo, records[2].Level)
	assert.Equal(t, "flywheel: lease sweep completed", records[2].Message)
	pass := attrsOf(records[2])
	assert.EqualValues(t, 2, pass["reclaimed"].Int64())
}

// stringAttrs renders a record's attributes as strings, for one comparison of
// every field a line carries.
func stringAttrs(r slog.Record) map[string]string {
	out := map[string]string{}
	for k, v := range attrsOf(r) {
		out[k] = v.String()
	}
	return out
}
