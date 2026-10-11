package flywheel_test

import (
	"bytes"
	"context"
	"testing"

	flywheel "github.com/mrz1836/go-flywheel"
	"github.com/mrz1836/go-flywheel/flywheeltest"
	"github.com/mrz1836/go-flywheel/observers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// queueHealthArgs is a job kind the queue-health gauge test enqueues.
type queueHealthArgs struct{}

func (queueHealthArgs) Kind() string { return "queue_health.gauge" }

// TestQueueHealthGaugesOmitSucceededJobs reads the queue-health gauges the way a
// /metrics scrape does, from SampleQueueHealth through WritePrometheus, over a
// table holding succeeded jobs: flywheel_queue_jobs carries a series for every
// state present but succeeded.
func TestQueueHealthGaugesOmitSucceededJobs(t *testing.T) {
	t.Parallel()
	db := flywheeltest.NewDB(t)
	ctx := context.Background()
	client := flywheel.NewClient(db)

	ids := make([]string, 3)
	for i := range ids {
		id, err := flywheel.Insert(ctx, client, queueHealthArgs{}, flywheel.InsertOpts{})
		require.NoError(t, err)
		ids[i] = id
	}
	require.NoError(t, db.Exec(`UPDATE jobs SET state = 'succeeded' WHERE id IN (?, ?)`, ids[0], ids[1]).Error)

	qh, err := flywheel.SampleQueueHealth(ctx, db)
	require.NoError(t, err)
	var out bytes.Buffer
	require.NoError(t, observers.WritePrometheus(&out, observers.NewMemRecorder(), qh))

	assert.Contains(t, out.String(), `flywheel_queue_jobs{state="available"} 1`)
	assert.NotContains(t, out.String(), `state="succeeded"`, "no gauge series counts succeeded jobs")
}
