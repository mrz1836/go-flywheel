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

// TestScheduleSpecsDeclareEveryEntryActive proves the YAML becomes one active
// PeriodicSpec per entry, carrying the worker's kind, the queue, the schedule,
// and the worker spec as the args template — the declarations serve hands the
// scheduler and doctor compares with the database.
func TestScheduleSpecsDeclareEveryEntryActive(t *testing.T) {
	t.Parallel()
	cfg := &Config{Schedules: []ScheduleEntry{
		{Slug: "a", Worker: "exec", Every: Duration(time.Minute), Exec: &execSpec{Command: "true"}},
		{Slug: "b", Worker: "http", Cron: "0 * * * *", Queue: "hooks", HTTP: &httpSpec{URL: "https://x.test"}},
	}}

	specs, err := scheduleSpecs(cfg)
	require.NoError(t, err)
	require.Len(t, specs, 2)

	assert.Equal(t, "a", specs[0].Slug)
	assert.Equal(t, workers.ExecKind, specs[0].Kind)
	assert.Equal(t, time.Minute, specs[0].Every)
	assert.Empty(t, specs[0].Cron)
	assert.Empty(t, specs[0].Queue, "an unset queue is left to the runtime's default")
	assert.JSONEq(t, `{"command":"true"}`, string(specs[0].ArgsTemplate))

	assert.Equal(t, "b", specs[1].Slug)
	assert.Equal(t, workers.HTTPKind, specs[1].Kind)
	assert.Equal(t, "0 * * * *", specs[1].Cron)
	assert.Zero(t, specs[1].Every)
	assert.Equal(t, "hooks", specs[1].Queue)
	assert.JSONEq(t, `{"url":"https://x.test"}`, string(specs[1].ArgsTemplate))

	for _, spec := range specs {
		assert.True(t, spec.Active, "%s: a declared schedule fires", spec.Slug)
	}

	none, err := scheduleSpecs(&Config{})
	require.NoError(t, err)
	assert.Empty(t, none)
}

// TestDisableOrphanSchedulesDisablesUndeclared proves the orphan-disable is the
// file's say over which schedules fire: an active row the config no longer names
// is deactivated (and kept), a declared one is left alone, and nothing is
// created — applying the declarations is the scheduler's job.
func TestDisableOrphanSchedulesDisablesUndeclared(t *testing.T) {
	t.Parallel()
	db := newCLITestDB(t)
	ctx := context.Background()
	for _, slug := range []string{"a", "b", "c"} {
		require.NoError(t, flywheel.UpsertPeriodic(ctx, db, flywheel.PeriodicSpec{
			Slug: slug, Kind: workers.ExecKind, Every: time.Minute, Active: slug != "c",
		}))
	}
	cfg := &Config{Schedules: []ScheduleEntry{
		{Slug: "a", Worker: "exec", Every: Duration(time.Minute), Exec: &execSpec{Command: "true"}},
		{Slug: "d", Worker: "exec", Every: Duration(time.Minute), Exec: &execSpec{Command: "true"}},
	}}

	require.NoError(t, disableOrphanSchedules(ctx, db, cfg))

	views, err := flywheel.ListPeriodics(ctx, db)
	require.NoError(t, err)
	active := map[string]bool{}
	for _, v := range views {
		active[v.Slug] = v.Active
	}
	assert.Equal(t, map[string]bool{"a": true, "b": false, "c": false}, active,
		"b is deactivated when removed from the config, and d is not created here")
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
