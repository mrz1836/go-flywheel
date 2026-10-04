package main

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	flywheel "github.com/mrz1836/go-flywheel"
	"github.com/mrz1836/go-flywheel/workers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

// failPeriodicReads makes every read of job_periodics through db fail, leaving
// every other query alone.
func failPeriodicReads(t *testing.T, db *gorm.DB) {
	t.Helper()
	require.NoError(t, db.Callback().Query().Before("gorm:query").Register("test:fail_periodic_reads", func(tx *gorm.DB) {
		if tx.Statement.Table == "job_periodics" {
			_ = tx.AddError(errors.New("injected job_periodics read failure"))
		}
	}))
}

// cliTestDriver returns a SQLite Driver over db without the pragma check, for a
// test that hands doctor a handle the check would complain about — a closed one,
// or one whose schema doctor has yet to create.
func cliTestDriver(t *testing.T, db *gorm.DB) flywheel.Driver {
	t.Helper()
	driver, err := flywheel.NewSQLiteDriverWithOptions(db, flywheel.SQLiteOpts{SkipPragmaCheck: true})
	require.NoError(t, err)
	return driver
}

// TestScheduleReconcileConfig pins the runtime.schedule_reconcile knob: serve
// passes the configured value through to the scheduler, and the resolved cadence
// doctor and the start log report is one minute unset and off when negative.
func TestScheduleReconcileConfig(t *testing.T) {
	t.Parallel()
	tests := map[string]struct {
		yaml     string
		raw      time.Duration
		resolved time.Duration
	}{
		"unset":    {"runtime:\n  queues: [default]\n", 0, time.Minute},
		"negative": {"runtime:\n  schedule_reconcile: -1s\n", -time.Second, 0},
		"explicit": {"runtime:\n  schedule_reconcile: 30s\n", 30 * time.Second, 30 * time.Second},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			cfg, err := LoadConfig(writeConfigFile(t, tt.yaml))
			require.NoError(t, err)
			assert.Equal(t, tt.raw, cfg.Runtime.ScheduleReconcile.Std())
			assert.Equal(t, tt.resolved, cfg.Runtime.scheduleReconcileInterval())
		})
	}
}

// TestServeSchedulerConfigDeclaresTheConfiguredSchedules proves serve hands the
// scheduler the config's schedules as its declared periodics, every one active,
// and passes runtime.schedule_reconcile through as the reconcile interval.
func TestServeSchedulerConfigDeclaresTheConfiguredSchedules(t *testing.T) {
	t.Parallel()
	db := newCLITestDB(t)
	cfg := &Config{
		Runtime: defaultConfig().Runtime,
		Schedules: []ScheduleEntry{
			{Slug: "nightly", Worker: "exec", Cron: "0 2 * * *", Exec: &execSpec{Command: "true"}},
			{Slug: "ping", Worker: "http", Every: Duration(time.Minute), HTTP: &httpSpec{URL: "https://x.test"}},
		},
	}

	logger := newLogger(cfg)
	for name, tt := range map[string]struct {
		knob Duration
		want time.Duration
	}{
		"unset passes through":    {0, 0},
		"explicit passes through": {Duration(30 * time.Second), 30 * time.Second},
		"negative passes through": {Duration(-time.Second), -time.Second},
	} {
		cfg.Runtime.ScheduleReconcile = tt.knob
		sc, err := serveSchedulerConfig(cfg, db, flywheel.NewSQLiteDriver(db), logger, nil, time.Minute)
		require.NoError(t, err, name)
		assert.Same(t, logger, sc.Logger, "the scheduler logs through the daemon's logger")
		assert.Equal(t, tt.want, sc.ReconcileInterval, name)
		require.Len(t, sc.Periodics, 2, name)
		assert.Equal(t, "nightly", sc.Periodics[0].Slug)
		assert.Equal(t, workers.ExecKind, sc.Periodics[0].Kind)
		assert.Equal(t, "ping", sc.Periodics[1].Slug)
		assert.Equal(t, workers.HTTPKind, sc.Periodics[1].Kind)
		for _, p := range sc.Periodics {
			assert.True(t, p.Active, "%s: %s is declared active", name, p.Slug)
		}
	}
}

// TestRunServeRejectsAnInvalidScheduleBeforeWritingAnything proves serve
// refuses a malformed schedule before it touches the database. Building the Node
// validates the declarations and does no I/O, and it runs ahead of the migrate
// and the orphan-disable: a fresh database is left without a single table, and an
// existing one exactly as it was — the valid schedule beside the bad one not
// created, the orphan not disabled.
func TestRunServeRejectsAnInvalidScheduleBeforeWritingAnything(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cfg := &Config{
		Runtime: defaultConfig().Runtime,
		Schedules: []ScheduleEntry{
			{Slug: "good", Worker: "exec", Every: Duration(time.Minute), Exec: &execSpec{Command: "true"}},
			{Slug: "bad", Worker: "exec", Cron: "not a cron", Exec: &execSpec{Command: "true"}},
		},
	}
	assertRefused := func(t *testing.T, err error) {
		t.Helper()
		require.ErrorIs(t, err, flywheel.ErrValidation)
		assert.Contains(t, err.Error(), `jobs: scheduler config: periodic "bad": flywheel: parse cron "not a cron"`)
	}

	t.Run("a fresh database gets no schema", func(t *testing.T) {
		t.Parallel()
		db := newBareCLITestDB(t)
		assertRefused(t, runServe(ctx, cfg, db, flywheel.NewSQLiteDriver(db)))

		var tables int64
		require.NoError(t, db.Raw(`SELECT count(*) FROM sqlite_master WHERE type = 'table'`).Scan(&tables).Error)
		assert.Zero(t, tables, "serve refused before migrating")
	})

	t.Run("an existing database is left as it was", func(t *testing.T) {
		t.Parallel()
		db := newCLITestDB(t)
		require.NoError(t, flywheel.UpsertPeriodic(ctx, db, flywheel.PeriodicSpec{
			Slug: "legacy", Kind: workers.ExecKind, Every: time.Minute, Active: true,
		}))
		assertRefused(t, runServe(ctx, cfg, db, flywheel.NewSQLiteDriver(db)))

		views, err := flywheel.ListPeriodics(ctx, db)
		require.NoError(t, err)
		require.Len(t, views, 1, "neither declared schedule was created")
		assert.Equal(t, "legacy", views[0].Slug)
		assert.True(t, views[0].Active, "the orphan was not disabled by a serve that refused to start")
	})
}

// TestCLIServeRecreatesADeletedSchedule is the daemon end to end: serve applies a
// declared schedule, `flywheel schedule rm` deletes it and warns that serve will
// bring it back, and serve does.
func TestCLIServeRecreatesADeletedSchedule(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	cfg := writeCLIConfig(t, dir, "schedules:\n"+
		"  - slug: nightly\n"+
		"    cron: \"0 2 * * *\"\n"+
		"    worker: exec\n"+
		"    exec:\n"+
		"      command: \"true\"\n")
	loaded, err := LoadConfig(cfg)
	require.NoError(t, err)
	loaded.Runtime.ScheduleReconcile = Duration(20 * time.Millisecond)
	db, driver, err := openDB(loaded)
	require.NoError(t, err)
	t.Cleanup(func() { closeDB(db) })

	serveCtx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runServe(serveCtx, loaded, db, driver) }()

	listed := func() bool {
		out, lerr := runRoot(context.Background(), "--config", cfg, "schedule", "ls", "--json")
		return lerr == nil && strings.Contains(out, `"slug": "nightly"`)
	}
	require.Eventually(t, listed, 8*time.Second, 20*time.Millisecond, "serve applied the declared schedule")

	out, err := runRoot(context.Background(), "--config", cfg, "schedule", "rm", "nightly")
	require.NoError(t, err)
	assert.Contains(t, out, "removed nightly")
	assert.Contains(t, out, "warning: nightly is declared in "+cfg+"; flywheel serve re-creates it")

	require.Eventually(t, listed, 8*time.Second, 20*time.Millisecond, "serve re-created the deleted schedule")

	cancel()
	require.NoError(t, <-done)
}

// TestCLIScheduleRmWarnsOnlyForADeclaredSlug proves the warning is about the
// config: a slug the config declares is removed with a warning that serve will
// re-create it, and an ad-hoc one is removed quietly.
func TestCLIScheduleRmWarnsOnlyForADeclaredSlug(t *testing.T) {
	t.Parallel()
	cfg := writeCLIConfig(t, t.TempDir(), "schedules:\n"+
		"  - slug: nightly\n"+
		"    cron: \"0 2 * * *\"\n"+
		"    worker: exec\n"+
		"    exec:\n"+
		"      command: \"true\"\n")
	ctx := context.Background()
	_, err := runRoot(ctx, "--config", cfg, "migrate")
	require.NoError(t, err)
	for _, slug := range []string{"nightly", "adhoc"} {
		_, err = runRoot(ctx, "--config", cfg, "schedule", "add", slug, "exec", "--every", "1m", "--args", `{"command":"true"}`)
		require.NoError(t, err)
	}

	out, err := runRoot(ctx, "--config", cfg, "schedule", "rm", "nightly")
	require.NoError(t, err)
	assert.Contains(t, out, "removed nightly")
	assert.Contains(t, out,
		"warning: nightly is declared in "+cfg+"; flywheel serve re-creates it — remove it from the config to retire it")

	out, err = runRoot(ctx, "--config", cfg, "schedule", "rm", "adhoc")
	require.NoError(t, err)
	assert.Contains(t, out, "removed adhoc")
	assert.NotContains(t, out, "warning:", "a slug the config does not declare stays removed")
}

// doctorSchedulesConfig declares one schedule in each state the doctor report
// distinguishes, once seedDoctorSchedules has prepared the database.
func doctorSchedulesConfig() *Config {
	return &Config{
		DB:      DBConfig{SQLite: "/x/y.db"},
		Runtime: defaultConfig().Runtime,
		Schedules: []ScheduleEntry{
			{Slug: "nightly", Worker: "exec", Cron: "0 2 * * *", Exec: &execSpec{Command: "true"}},
			{Slug: "ping", Worker: "http", Every: Duration(time.Minute), HTTP: &httpSpec{URL: "https://x.test"}},
			{Slug: "sync", Worker: "shell", Every: Duration(24 * time.Hour), Shell: &shellSpec{Inline: "true"}},
			{Slug: "deps", Worker: "mage", Every: Duration(24 * time.Hour), Mage: &mageSpec{Targets: []string{"deps"}}},
		},
	}
}

// seedDoctorSchedules writes what serve would for cfg, then drifts it: ping is
// deleted, sync deactivated, deps given another cadence and moved off the default
// queue its config leaves unset, and two schedules the config does not declare
// are added — legacy active, retired inactive.
func seedDoctorSchedules(t *testing.T, db *gorm.DB, cfg *Config) {
	t.Helper()
	ctx := context.Background()
	specs, err := scheduleSpecs(cfg)
	require.NoError(t, err)
	for _, spec := range specs {
		require.NoError(t, flywheel.UpsertPeriodic(ctx, db, spec))
	}
	require.NoError(t, flywheel.DeletePeriodic(ctx, db, "ping"))
	require.NoError(t, flywheel.SetPeriodicActive(ctx, db, "sync", false))
	deps := specs[3]
	deps.Every = 12 * time.Hour
	deps.Queue = "elsewhere"
	require.NoError(t, flywheel.UpsertPeriodic(ctx, db, deps))
	for _, slug := range []string{"legacy", "retired"} {
		require.NoError(t, flywheel.UpsertPeriodic(ctx, db, flywheel.PeriodicSpec{
			Slug: slug, Kind: workers.ExecKind, Every: time.Hour, Active: slug == "legacy",
		}))
	}
}

// TestRunDoctorReportsDeclaredSchedulesAgainstTheDatabase proves doctor compares
// the config with job_periodics and says, per schedule, what is off and what
// serve does about it — without failing the check, since serve repairs all of it.
func TestRunDoctorReportsDeclaredSchedulesAgainstTheDatabase(t *testing.T) {
	t.Parallel()
	db := newCLITestDB(t)
	cfg := doctorSchedulesConfig()
	seedDoctorSchedules(t, db, cfg)

	var buf bytes.Buffer
	require.NoError(t, runDoctor(context.Background(), &buf, "cfg.yaml", cfg, db, cliTestDriver(t, db)))
	out := buf.String()
	assert.Contains(t, out, "  schedules:    4 declared (1 missing, 1 inactive, 1 differs)\n")
	assert.Contains(t, out, "    - nightly              exec  0 2 * * *\n", "a schedule in sync carries no note")
	assert.Contains(t, out, "    - ping                 http  every 1m0s  [not in database: serve creates it]\n")
	assert.Contains(t, out, "    - sync                 shell every 24h0m0s  [inactive: serve re-activates it on start]\n")
	assert.Contains(t, out,
		"    - deps                 mage  every 24h0m0s  [differs (schedule, queue): serve updates it on start]\n",
		"a queue the config leaves unset is compared with the default serve writes")
	assert.Contains(t, out, "  undeclared:   1 active (legacy): serve disables it on start\n",
		"an inactive undeclared row is already where serve leaves it, so it is not reported")
	assert.Contains(t, out, "  status:       OK", "drift is reported, not failed")
}

// TestRunDoctorReportsSchedulesInSync proves the quiet case reads as such, and
// that turning the re-creation off is called out on the same line.
func TestRunDoctorReportsSchedulesInSync(t *testing.T) {
	t.Parallel()
	db := newCLITestDB(t)
	ctx := context.Background()
	cfg := &Config{
		DB:      DBConfig{SQLite: "/x/y.db"},
		Runtime: defaultConfig().Runtime,
		Schedules: []ScheduleEntry{
			{Slug: "nightly", Worker: "exec", Cron: "0 2 * * *", Queue: "batch", Exec: &execSpec{Command: "true"}},
			{Slug: "ping", Worker: "http", Every: Duration(time.Minute), HTTP: &httpSpec{URL: "https://x.test"}},
		},
	}
	specs, err := scheduleSpecs(cfg)
	require.NoError(t, err)
	for _, spec := range specs {
		require.NoError(t, flywheel.UpsertPeriodic(ctx, db, spec))
	}

	var buf bytes.Buffer
	require.NoError(t, runDoctor(ctx, &buf, "cfg.yaml", cfg, db, cliTestDriver(t, db)))
	out := buf.String()
	assert.Contains(t, out, "  schedules:    2 declared, in sync\n"+
		"    - nightly              exec  0 2 * * *\n"+
		"    - ping                 http  every 1m0s\n", "no schedule carries a note")
	assert.NotContains(t, out, "undeclared:")

	cfg.Runtime.ScheduleReconcile = Duration(-time.Second)
	buf.Reset()
	require.NoError(t, runDoctor(ctx, &buf, "cfg.yaml", cfg, db, cliTestDriver(t, db)))
	assert.Contains(t, buf.String(), "  schedules:    2 declared, in sync; drift repair off\n")
}

// TestRunDoctorSurfacesAScheduleListFailure proves a failed read of the schedules
// fails the check rather than reporting every declared schedule missing.
func TestRunDoctorSurfacesAScheduleListFailure(t *testing.T) {
	t.Parallel()
	db := newCLITestDB(t)
	failPeriodicReads(t, db)

	var buf bytes.Buffer
	err := runDoctor(context.Background(), &buf, "cfg.yaml", doctorSchedulesConfig(), db, cliTestDriver(t, db))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "schedule check failed")
	assert.NotContains(t, buf.String(), "status:")
}

// TestScheduleDrift pins what doctor compares between a declared schedule and its
// row: the kind, the schedule in either form, and the queue — against the
// runtime's default queue when the config sets none, since that is what serve
// writes.
func TestScheduleDrift(t *testing.T) {
	t.Parallel()
	every := flywheel.PeriodicSpec{Kind: "exec", Every: time.Minute}
	cron := flywheel.PeriodicSpec{Kind: "exec", Cron: "0 2 * * *"}
	queued := flywheel.PeriodicSpec{Kind: "exec", Every: time.Minute, Queue: "batch"}
	row := func(kind, cronExpr string, interval int, queue string) flywheel.PeriodicView {
		return flywheel.PeriodicView{Kind: kind, Cron: cronExpr, IntervalSeconds: interval, Queue: queue}
	}

	tests := map[string]struct {
		spec flywheel.PeriodicSpec
		row  flywheel.PeriodicView
		want []string
	}{
		"interval in sync":                 {every, row("exec", "", 60, "periodic"), nil},
		"cron in sync":                     {cron, row("exec", "0 2 * * *", 0, "periodic"), nil},
		"kind differs":                     {every, row("http", "", 60, "periodic"), []string{"kind"}},
		"interval differs":                 {every, row("exec", "", 120, "periodic"), []string{"schedule"}},
		"cron differs":                     {cron, row("exec", "0 3 * * *", 0, "periodic"), []string{"schedule"}},
		"interval declared, cron row":      {every, row("exec", "* * * * *", 0, "periodic"), []string{"schedule"}},
		"cron declared, interval row":      {cron, row("exec", "", 60, "periodic"), []string{"schedule"}},
		"queue declared, same":             {queued, row("exec", "", 60, "batch"), nil},
		"queue declared, differs":          {queued, row("exec", "", 60, "periodic"), []string{"queue"}},
		"queue left to the default, same":  {every, row("exec", "", 60, "periodic"), nil},
		"queue left to the default, moved": {every, row("exec", "", 60, "elsewhere"), []string{"queue"}},
		"everything differs":               {queued, row("http", "0 2 * * *", 0, "periodic"), []string{"kind", "schedule", "queue"}},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, scheduleDrift(tt.spec, &tt.row))
		})
	}
}

// TestRunDoctorRefusesSchedulesServeWouldRefuse proves doctor validates the config
// exactly as serve does, and fails on what serve would refuse — a malformed cron,
// a sub-second interval — instead of reporting OK and "serve creates it". It
// validates before it touches the database, like serve: a fresh database gets no
// schema.
func TestRunDoctorRefusesSchedulesServeWouldRefuse(t *testing.T) {
	t.Parallel()
	tests := map[string]ScheduleEntry{
		"malformed cron":   {Slug: "bad", Worker: "exec", Cron: "not a cron", Exec: &execSpec{Command: "true"}},
		"sub-second every": {Slug: "bad", Worker: "exec", Every: Duration(500 * time.Millisecond), Exec: &execSpec{Command: "true"}},
	}
	for name, bad := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			db := newBareCLITestDB(t)
			cfg := &Config{
				DB:      DBConfig{SQLite: "/x/y.db"},
				Runtime: defaultConfig().Runtime,
				Schedules: []ScheduleEntry{
					{Slug: "good", Worker: "exec", Every: Duration(time.Minute), Exec: &execSpec{Command: "true"}},
					bad,
				},
			}

			var buf bytes.Buffer
			err := runDoctor(context.Background(), &buf, "cfg.yaml", cfg, db, cliTestDriver(t, db))
			require.ErrorIs(t, err, flywheel.ErrValidation)
			assert.Contains(t, err.Error(), `config: `)
			assert.Contains(t, err.Error(), `periodic "bad"`)
			assert.NotContains(t, buf.String(), "status:", "an invalid config is not reported OK")

			var tables int64
			require.NoError(t, db.Raw(`SELECT count(*) FROM sqlite_master WHERE type = 'table'`).Scan(&tables).Error)
			assert.Zero(t, tables, "doctor refused before migrating")
		})
	}
}

// TestCLIDoctorFailsOnAScheduleServeWouldRefuse is the same refusal through the
// command: a YAML schedule every 500ms passes the config's own checks, but serve
// rejects it, so `flywheel doctor` exits with the error rather than reporting OK.
func TestCLIDoctorFailsOnAScheduleServeWouldRefuse(t *testing.T) {
	t.Parallel()
	cfg := writeCLIConfig(t, t.TempDir(), "schedules:\n"+
		"  - slug: too-fast\n"+
		"    every: 500ms\n"+
		"    worker: exec\n"+
		"    exec:\n"+
		"      command: \"true\"\n")

	out, err := runRoot(context.Background(), "--config", cfg, "doctor")
	require.ErrorIs(t, err, flywheel.ErrValidation)
	assert.Contains(t, err.Error(), `config: `)
	assert.Contains(t, err.Error(), `periodic "too-fast": flywheel: every must be at least 1 second`)
	assert.NotContains(t, out, "status:")
}
