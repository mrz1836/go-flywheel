package core

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestIndexDriftErrorDescribesAnInvalidIndex pins the message an operator reads
// when a concurrent build failed part-way. Its definition usually matches, so
// the old "drifted, enable reconciliation" text pointed at the wrong problem and
// the wrong fix; the message must say the index is invalid and name the fix for
// its kind.
func TestIndexDriftErrorDescribesAnInvalidIndex(t *testing.T) {
	t.Parallel()
	const def = `CREATE INDEX jobs_finished ON jobs (state, finalized_at DESC, id DESC)`

	perf := (&IndexDriftError{Drift: []IndexDrift{
		{Name: "jobs_finished", Installed: def, Expected: def, Invalid: true},
	}}).Error()
	assert.Contains(t, perf, "jobs_finished: invalid")
	assert.Contains(t, perf, "flywheel migrate --concurrently")
	assert.NotContains(t, perf, "installed:", "an identical definition is not printed twice")
	assert.NotContains(t, perf, "enable reconciliation", "nothing in the report has drifted")

	correctness := (&IndexDriftError{Drift: []IndexDrift{
		{Name: "jobs_unique_key", Installed: "x", Expected: "x", Invalid: true},
	}}).Error()
	assert.Contains(t, correctness, "jobs_unique_key: invalid")
	assert.Contains(t, correctness, "Reconcile", "a unique index is rebuilt only through reconciliation")
	assert.NotContains(t, correctness, "--concurrently")

	mixed := (&IndexDriftError{Drift: []IndexDrift{
		{Name: "jobs_finished", Installed: def, Expected: def, Invalid: true},
		{Name: "jobs_state", Installed: "CREATE INDEX jobs_state ON jobs (state)", Expected: "CREATE INDEX jobs_state ON jobs (state) WHERE x"},
	}}).Error()
	assert.Contains(t, mixed, "2 installed index(es)")
	assert.Contains(t, mixed, "jobs_state: definition has drifted")
	assert.Contains(t, mixed, "installed: CREATE INDEX jobs_state ON jobs (state)")
	assert.Contains(t, mixed, "enable reconciliation")
}
