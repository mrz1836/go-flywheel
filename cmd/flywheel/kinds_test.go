package main

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	flywheel "github.com/mrz1836/go-flywheel"
	"github.com/mrz1836/go-foundation/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// catalogArgs is the args type of the catalog command's test kinds.
type catalogArgs struct {
	UserID string `json:"user_id"`
}

// welcomeKindWorker declares a description and defaults for "mail.welcome".
type welcomeKindWorker struct{}

func (welcomeKindWorker) Kind() string { return "mail.welcome" }

func (welcomeKindWorker) Work(context.Context, *flywheel.Job[catalogArgs]) (flywheel.Result, error) {
	return flywheel.Result{}, nil
}

func (welcomeKindWorker) Description() string { return "Send the welcome email" }

func (welcomeKindWorker) Defaults() flywheel.InsertOpts {
	return flywheel.InsertOpts{Queue: "mail", MaxAttempts: 4, Timeout: 30 * time.Second}
}

// dailyKindWorker declares nothing about "report.daily".
type dailyKindWorker struct{}

func (dailyKindWorker) Kind() string { return "report.daily" }

func (dailyKindWorker) Work(context.Context, *flywheel.Job[catalogArgs]) (flywheel.Result, error) {
	return flywheel.Result{}, nil
}

// TestKindsCommandListsThePublishedCatalog proves `flywheel kinds` prints one
// line per published kind, sorted, with its queue, priority, attempts, timeout
// (or "-"), and publish time, and that --json prints the published records.
func TestKindsCommandListsThePublishedCatalog(t *testing.T) {
	t.Parallel()
	cfg := migratedCLIConfig(t)
	ctx := context.Background()

	out, err := runRoot(ctx, "--config", cfg, "kinds")
	require.NoError(t, err)
	assert.Equal(t, []string{"KIND", "QUEUE", "PRIORITY", "ATTEMPTS", "TIMEOUT", "PUBLISHED"},
		strings.Fields(strings.TrimSpace(out)), "an empty catalog prints its header alone")

	reg := flywheel.NewRegistry()
	flywheel.Register(reg, dailyKindWorker{})
	flywheel.Register(reg, welcomeKindWorker{})
	_, db, _, err := loadAndOpen(cfg)
	require.NoError(t, err)
	at := time.Date(2026, 1, 5, 9, 0, 0, 0, time.UTC)
	err = flywheel.PublishKindsWithOptions(models.WithClock(ctx, models.NewFixedClock(at)), db, reg,
		flywheel.PublishKindsOpts{Publisher: "worker-v1"})
	closeDB(db)
	require.NoError(t, err)

	out, err = runRoot(ctx, "--config", cfg, "kinds")
	require.NoError(t, err)
	lines := strings.Split(strings.TrimSpace(out), "\n")
	require.Len(t, lines, 3)
	assert.Equal(t, []string{"KIND", "QUEUE", "PRIORITY", "ATTEMPTS", "TIMEOUT", "PUBLISHED"}, strings.Fields(lines[0]))
	assert.Equal(t, []string{"mail.welcome", "mail", "100", "4", "30s", "2026-01-05", "09:00:00"}, strings.Fields(lines[1]))
	assert.Equal(t, []string{"report.daily", "default", "100", "25", "-", "2026-01-05", "09:00:00"}, strings.Fields(lines[2]))

	out, err = runRoot(ctx, "--config", cfg, "kinds", "--json")
	require.NoError(t, err)
	var records []flywheel.KindRecord
	require.NoError(t, json.Unmarshal([]byte(out), &records))
	require.Len(t, records, 2)
	assert.Equal(t, "mail.welcome", records[0].Kind)
	assert.Equal(t, "Send the welcome email", records[0].Description)
	assert.Equal(t, 30_000, records[0].TimeoutMs)
	assert.Equal(t, "worker-v1", records[0].PublishedBy)
	assert.True(t, at.Equal(records[0].FirstPublishedAt))
	assert.Equal(t, "report.daily", records[1].Kind)
}
