package main

import (
	"bytes"
	"context"
	"testing"
	"time"

	flywheel "github.com/mrz1836/go-flywheel"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRunServeFailsToMigrateOnClosedDB(t *testing.T) {
	t.Parallel()
	db := newCLITestDB(t)
	sqlDB, err := db.DB()
	require.NoError(t, err)
	require.NoError(t, sqlDB.Close())

	err = runServe(context.Background(), &Config{Runtime: defaultConfig().Runtime}, db, flywheel.NewSQLiteDriver(db))
	require.Error(t, err, "serve fails to migrate against a closed database")
	assert.Contains(t, err.Error(), "migrate")
}

func TestRunServeFailsToReconcileOnClosedDB(t *testing.T) {
	t.Parallel()
	// Migrate the DB first so Migrate succeeds, then close it so the reconcile step
	// (the next database touch) is the failure point.
	db := newCLITestDB(t)
	cfg := &Config{
		Runtime:   defaultConfig().Runtime,
		Schedules: []ScheduleEntry{{Slug: "x", Worker: "exec", Every: Duration(time.Minute), Exec: &execSpec{Command: "true"}}},
	}
	sqlDB, err := db.DB()
	require.NoError(t, err)
	require.NoError(t, sqlDB.Close())

	err = runServe(context.Background(), cfg, db, flywheel.NewSQLiteDriver(db))
	require.Error(t, err, "serve fails when it cannot reconcile schedules")
}

func TestRunServeFailsToBuildNodeWithNoQueues(t *testing.T) {
	t.Parallel()
	// Migrate and reconcile succeed against a live DB, but a runtime with no queues
	// makes NewNode reject the runner, exercising serve's node-build error branch.
	db := newCLITestDB(t)
	cfg := &Config{Runtime: RuntimeConfig{Queues: nil, Concurrency: 1}}
	err := runServe(context.Background(), cfg, db, flywheel.NewSQLiteDriver(db))
	require.Error(t, err, "a runtime with no queues cannot build a node")
}

func TestCLIServeFailsOnUnopenableDB(t *testing.T) {
	t.Parallel()
	cfg := unopenableConfig(t)
	_, err := runRoot(context.Background(), "--config", cfg, "serve")
	require.Error(t, err, "serve surfaces a database that cannot be opened")
}

func TestRunDoctorReportsUnreachableDatabase(t *testing.T) {
	t.Parallel()
	db := newCLITestDB(t)
	sqlDB, err := db.DB()
	require.NoError(t, err)
	require.NoError(t, sqlDB.Close())

	var buf bytes.Buffer
	err = runDoctor(context.Background(), &buf, "cfg.yaml", &Config{DB: DBConfig{SQLite: "x"}}, db)
	require.Error(t, err, "a closed database is reported as unreachable")
	assert.Contains(t, err.Error(), "database unreachable")
}

func TestRunDoctorHappyPathReportsOK(t *testing.T) {
	t.Parallel()
	db := newCLITestDB(t)
	var buf bytes.Buffer
	cfg := &Config{
		DB:      DBConfig{SQLite: "/x/y.db"},
		Runtime: defaultConfig().Runtime,
		Schedules: []ScheduleEntry{
			{Slug: "nightly", Worker: "exec", Cron: "0 2 * * *", Exec: &execSpec{Command: "true"}},
			{Slug: "ping", Worker: "http", Every: Duration(time.Minute), HTTP: &httpSpec{URL: "https://x.test"}},
		},
	}
	require.NoError(t, runDoctor(context.Background(), &buf, "cfg.yaml", cfg, db))
	out := buf.String()
	assert.Contains(t, out, "status:       OK")
	assert.Contains(t, out, "sqlite:", "the sqlite hardening line is printed for a sqlite config")
	assert.Contains(t, out, "nightly")
	assert.Contains(t, out, "every 1m0s", "an interval schedule shows its cadence")
}
