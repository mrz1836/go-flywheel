package main

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	flywheel "github.com/mrz1836/go-flywheel"
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStatusFormatLag(t *testing.T) {
	t.Parallel()
	assert.Equal(t, "none", formatLag(0), "no lag reads as none")
	assert.Equal(t, "none", formatLag(-time.Second), "a negative age clamps to none")
	assert.Equal(t, "5s", formatLag(5*time.Second))
	assert.Equal(t, "1m30s", formatLag(90*time.Second), "the lag renders as a rounded duration")
	assert.Equal(t, "2s", formatLag(2400*time.Millisecond), "sub-second precision is rounded away")
}

func TestStatusScheduleWhen(t *testing.T) {
	t.Parallel()
	assert.Equal(t, "0 2 * * *", scheduleWhen(flywheel.PeriodicView{Cron: "0 2 * * *"}), "a cron schedule shows its expression")
	assert.Equal(t, "every 1m0s", scheduleWhen(flywheel.PeriodicView{IntervalSeconds: 60}), "an interval schedule shows its cadence")
}

func TestStatusTruncateMessage(t *testing.T) {
	t.Parallel()
	assert.Equal(t, "short", truncateMessage("short"), "a short message is unchanged")
	long := strings.Repeat("x", 200)
	got := truncateMessage(long)
	assert.Len(t, []rune(got), statusMessageWidth, "a long message is capped to the display width")
	assert.True(t, strings.HasSuffix(got, "…"), "truncation is marked with an ellipsis")
}

func TestGatherStatusErrorsOnClosedDB(t *testing.T) {
	t.Parallel()
	db := newCLITestDB(t)
	sqlDB, err := db.DB()
	require.NoError(t, err)
	require.NoError(t, sqlDB.Close())

	cfg := &Config{DB: DBConfig{SQLite: "x"}}
	_, err = gatherStatus(context.Background(), db, cfg, time.Now())
	require.Error(t, err, "a broken database surfaces as a gather error, not a panic")
}

func TestRunStatusWatchReturnsRenderErrorWhenContextLive(t *testing.T) {
	t.Parallel()
	db := newCLITestDB(t)
	sqlDB, err := db.DB()
	require.NoError(t, err)
	require.NoError(t, sqlDB.Close())

	cmd := &cobra.Command{}
	cmd.SetContext(context.Background()) // live context: a render error must surface
	var buf bytes.Buffer
	cmd.SetOut(&buf)

	err = runStatusWatch(cmd, db, &Config{DB: DBConfig{SQLite: "x"}}, time.Second)
	require.Error(t, err, "a render failure on a live context returns the error rather than looping")
}
