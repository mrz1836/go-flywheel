package main

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	flywheel "github.com/mrz1836/go-flywheel"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestLimitsCommandListsAndSetsOverrides proves `flywheel limits` lists each
// shared limiter with its overrides, as a table and as JSON, and `limits set`
// changes only the flags it is given, clears one with none, records the CLI's
// actor and reason, and refuses a call with no flag or a bad value.
func TestLimitsCommandListsAndSetsOverrides(t *testing.T) {
	t.Parallel()
	cfg := migratedCLIConfig(t)
	ctx := context.Background()
	_, db, _, err := loadAndOpen(cfg)
	require.NoError(t, err)
	defer closeDB(db)
	limiter, err := flywheel.NewDBLimiter(db, flywheel.DBLimiterConfig{MaxConcurrent: 4, HoldTTL: time.Minute})
	require.NoError(t, err)
	g, err := limiter.Acquire(ctx, "vendor", 1)
	require.NoError(t, err)
	limiter.Release(ctx, g)

	out, err := runRoot(ctx, "--config", cfg, "limits")
	require.NoError(t, err)
	lines := strings.Split(strings.TrimSpace(out), "\n")
	require.Len(t, lines, 2, out)
	assert.Equal(t, []string{"RESOURCE", "TOKENS", "HELD", "NEXT-EXPIRY", "MAX-CONCURRENT", "RATE", "BURST"},
		strings.Fields(lines[0]))
	assert.Equal(t, []string{"vendor", "0", "0", "-", "-", "-", "-"}, strings.Fields(lines[1]))

	out, err = runRoot(ctx, "--config", cfg, "limits", "set", "vendor", "--max-concurrent", "2", "--reason", "vendor throttling")
	require.NoError(t, err)
	assert.Equal(t, "limits set vendor: max_concurrent=2, rate=none, burst=none\n", out)
	out, err = runRoot(ctx, "--config", cfg, "limits", "set", "vendor", "--rate", "5")
	require.NoError(t, err)
	assert.Equal(t, "limits set vendor: max_concurrent=2, rate=5, burst=none\n", out, "a flag left out keeps its override")

	out, err = runRoot(ctx, "--config", cfg, "limits")
	require.NoError(t, err)
	lines = strings.Split(strings.TrimSpace(out), "\n")
	require.Len(t, lines, 2, out)
	assert.Equal(t, []string{"vendor", "0", "0", "-", "2", "5", "-"}, strings.Fields(lines[1]))
	out, err = runRoot(ctx, "--config", cfg, "limits", "--json")
	require.NoError(t, err)
	var infos []flywheel.LimiterInfo
	require.NoError(t, json.Unmarshal([]byte(out), &infos))
	require.Len(t, infos, 1)
	require.NotNil(t, infos[0].Overrides.MaxConcurrent)
	assert.Equal(t, 2, *infos[0].Overrides.MaxConcurrent)

	out, err = runRoot(ctx, "--config", cfg, "limits", "set", "vendor", "--max-concurrent", "none")
	require.NoError(t, err)
	assert.Equal(t, "limits set vendor: max_concurrent=none, rate=5, burst=none\n", out, "none clears an override")

	page, err := flywheel.ListAudit(ctx, db, flywheel.ListAuditParams{
		TargetType: flywheel.AuditTargetLimiter, TargetID: "vendor",
	})
	require.NoError(t, err)
	require.Len(t, page.Entries, 3)
	first := page.Entries[2]
	assert.Equal(t, flywheel.AuditLimitsSet, first.Action)
	assert.Equal(t, osUserName(), first.Actor.ID, "the operating-system user")
	assert.Equal(t, "cli", first.Actor.Source)
	assert.Equal(t, "vendor throttling", first.Actor.Reason)

	_, err = runRoot(ctx, "--config", cfg, "limits", "set", "vendor")
	require.Error(t, err, "set needs at least one flag")
	_, err = runRoot(ctx, "--config", cfg, "limits", "set", "vendor", "--rate", "0")
	require.Error(t, err, "a value is a positive integer or none")
	_, err = runRoot(ctx, "--config", cfg, "limits", "set", "typo", "--rate", "1")
	var verr *flywheel.ValidationError
	require.ErrorAs(t, err, &verr, "a resource no limiter has admitted against is refused")
	assert.Equal(t, "resource", verr.Field)
}

// TestQueuesConcurrencyCommandSetsAndClearsTheCap proves `flywheel queues
// concurrency` sets a queue's cap, which `queues --json` reports, records the
// CLI's actor and reason, clears it with none, and refuses a bad value.
func TestQueuesConcurrencyCommandSetsAndClearsTheCap(t *testing.T) {
	t.Parallel()
	cfg := migratedCLIConfig(t)
	ctx := context.Background()
	_, db, _, err := loadAndOpen(cfg)
	require.NoError(t, err)
	defer closeDB(db)

	out, err := runRoot(ctx, "--config", cfg, "queues", "concurrency", "mail", "3", "--reason", "downstream drowning")
	require.NoError(t, err)
	assert.Equal(t, "concurrency mail: 3\n", out)
	out, err = runRoot(ctx, "--config", cfg, "queues", "--json")
	require.NoError(t, err)
	var queues []flywheel.QueueInfo
	require.NoError(t, json.Unmarshal([]byte(out), &queues))
	require.Len(t, queues, 1)
	require.NotNil(t, queues[0].MaxConcurrency)
	assert.Equal(t, 3, *queues[0].MaxConcurrency)

	page, err := flywheel.ListAudit(ctx, db, flywheel.ListAuditParams{TargetType: flywheel.AuditTargetQueue, TargetID: "mail"})
	require.NoError(t, err)
	require.Len(t, page.Entries, 1)
	assert.Equal(t, flywheel.AuditLimitsSet, page.Entries[0].Action)
	assert.Equal(t, "cli", page.Entries[0].Actor.Source)
	assert.Equal(t, "downstream drowning", page.Entries[0].Actor.Reason)

	out, err = runRoot(ctx, "--config", cfg, "queues", "concurrency", "mail", "none")
	require.NoError(t, err)
	assert.Equal(t, "concurrency mail: none\n", out)
	queues = nil
	out, err = runRoot(ctx, "--config", cfg, "queues", "--json")
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal([]byte(out), &queues))
	assert.Nil(t, queues[0].MaxConcurrency, "none clears the cap")

	_, err = runRoot(ctx, "--config", cfg, "queues", "concurrency", "mail", "0")
	require.Error(t, err, "a cap is a positive integer or none")
	_, err = runRoot(ctx, "--config", cfg, "queues", "concurrency", "mail")
	require.Error(t, err, "concurrency names its cap")
}
