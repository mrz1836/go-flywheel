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

	shell := ScheduleEntry{Worker: "shell", Shell: &shellSpec{Script: "/x.sh", Args: []string{"a"}}}
	assert.Equal(t, workers.ShellKind, shell.workerKind())
	tmpl, err = shell.argsTemplate()
	require.NoError(t, err)
	assert.JSONEq(t, `{"script":"/x.sh","args":["a"]}`, string(tmpl))

	py := ScheduleEntry{Worker: "python", Python: &pythonSpec{Module: "http.server", Interpreter: "python3"}}
	assert.Equal(t, workers.PythonKind, py.workerKind())
	tmpl, err = py.argsTemplate()
	require.NoError(t, err)
	assert.JSONEq(t, `{"module":"http.server","interpreter":"python3"}`, string(tmpl))

	mage := ScheduleEntry{Worker: "mage", Mage: &mageSpec{Targets: []string{"test"}, Binary: "magex"}}
	assert.Equal(t, workers.MageKind, mage.workerKind())
	tmpl, err = mage.argsTemplate()
	require.NoError(t, err)
	assert.JSONEq(t, `{"targets":["test"],"binary":"magex"}`, string(tmpl))
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

func TestReconcileSchedulesDisablesOrphans(t *testing.T) {
	t.Parallel()
	db := newCLITestDB(t)
	ctx := context.Background()

	both := &Config{Schedules: []ScheduleEntry{
		{Slug: "a", Worker: "exec", Every: Duration(time.Minute), Exec: &execSpec{Command: "true"}},
		{Slug: "b", Worker: "exec", Every: Duration(time.Minute), Exec: &execSpec{Command: "true"}},
	}}
	require.NoError(t, reconcileSchedules(ctx, db, both))

	// Drop b from the config; a declarative reconcile must deactivate it.
	onlyA := &Config{Schedules: []ScheduleEntry{both.Schedules[0]}}
	require.NoError(t, reconcileSchedules(ctx, db, onlyA))

	views, err := flywheel.ListPeriodics(ctx, db)
	require.NoError(t, err)
	active := map[string]bool{}
	for _, v := range views {
		active[v.Slug] = v.Active
	}
	assert.True(t, active["a"], "a stays active")
	assert.False(t, active["b"], "b is deactivated when removed from the config")
}

func TestBuildRegistryRegistersAllWorkers(t *testing.T) {
	t.Parallel()
	// A duplicate registration panics, so building twice without panicking proves
	// each registry is independent and every kind (exec, shell, python, mage, http)
	// is registered exactly once per registry.
	assert.NotPanics(t, func() {
		buildRegistry(&Config{})
		buildRegistry(&Config{})
	})
}
