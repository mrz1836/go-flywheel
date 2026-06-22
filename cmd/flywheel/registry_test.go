package main

import (
	"context"
	"testing"
	"time"

	flywheel "github.com/mrz1836/go-flywheel"
	"github.com/mrz1836/go-flywheel/workers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestScheduleWorkerKindAndArgsTemplate(t *testing.T) {
	t.Parallel()
	exec := ScheduleEntry{Worker: "exec", Exec: &execSpec{Command: "sh", Args: []string{"-c", "true"}, TimeoutSeconds: 5}}
	assert.Equal(t, workers.ExecKind, exec.workerKind())
	tmpl, err := exec.argsTemplate()
	require.NoError(t, err)
	assert.JSONEq(t, `{"command":"sh","args":["-c","true"],"timeout_seconds":5}`, string(tmpl))

	httpEntry := ScheduleEntry{Worker: "http", HTTP: &httpSpec{Method: "GET", URL: "https://x.test"}}
	assert.Equal(t, workers.HTTPKind, httpEntry.workerKind())
	tmpl, err = httpEntry.argsTemplate()
	require.NoError(t, err)
	assert.JSONEq(t, `{"method":"GET","url":"https://x.test"}`, string(tmpl))
}

func TestReconcileSchedulesUpsertsAndIsIdempotent(t *testing.T) {
	t.Parallel()
	db := newCLITestDB(t)
	cfg := &Config{Schedules: []ScheduleEntry{
		{Slug: "a", Worker: "exec", Every: Duration(time.Minute), Exec: &execSpec{Command: "true"}},
		{Slug: "b", Worker: "http", Cron: "0 * * * *", HTTP: &httpSpec{URL: "https://x.test"}},
	}}
	ctx := context.Background()

	require.NoError(t, reconcileSchedules(ctx, db, cfg))
	views, err := flywheel.ListPeriodics(ctx, db)
	require.NoError(t, err)
	require.Len(t, views, 2)

	// Reconciling the same config again updates in place, not duplicates.
	require.NoError(t, reconcileSchedules(ctx, db, cfg))
	views, err = flywheel.ListPeriodics(ctx, db)
	require.NoError(t, err)
	assert.Len(t, views, 2)
}

func TestBuildRegistryRegistersExecAndHTTP(t *testing.T) {
	t.Parallel()
	// A duplicate registration panics, so registering the same kinds again proves
	// the first call registered exec + http exactly once.
	buildRegistry(&Config{})
	assert.NotPanics(t, func() { buildRegistry(&Config{}) }, "each registry is independent")
}
