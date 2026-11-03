package core

import (
	"testing"
	"time"

	"github.com/mrz1836/go-flywheel/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSetRetentionSetsEveryRetentionField proves SetRetention copies each age
// and each rule of a host's retention block onto its SchedulerConfig field, and
// touches no other field.
func TestSetRetentionSetsEveryRetentionField(t *testing.T) {
	t.Parallel()
	cfg := SchedulerConfig{SweepInterval: time.Minute, RetentionInterval: 2 * time.Hour}

	cfg.SetRetention(config.RetentionConfig{
		Jobs: 30 * retentionDay,
		Rules: []config.RetentionRule{
			{State: "discarded", MaxAge: 90 * retentionDay},
			{Kind: "heartbeat", State: "succeeded", MaxAge: retentionDay},
		},
		Audit:        400 * retentionDay,
		QueueSamples: 14 * retentionDay,
		Nodes:        3 * retentionDay,
		Stats:        200 * retentionDay,
	})

	assert.Equal(t, 30*retentionDay, cfg.RetentionMaxAge)
	assert.Equal(t, []RetentionRule{
		{State: StateDiscarded, MaxAge: 90 * retentionDay},
		{Kind: "heartbeat", State: StateSucceeded, MaxAge: retentionDay},
	}, cfg.RetentionRules)
	assert.Equal(t, 400*retentionDay, cfg.AuditRetention)
	assert.Equal(t, 14*retentionDay, cfg.QueueSampleRetention)
	assert.Equal(t, 3*retentionDay, cfg.NodeRetention)
	assert.Equal(t, 200*retentionDay, cfg.StatsRetention)
	assert.Equal(t, time.Minute, cfg.SweepInterval, "no other field changes")
	assert.Equal(t, 2*time.Hour, cfg.RetentionInterval, "no other field changes")
}

// TestSetRetentionWithAZeroBlockLeavesTheDefaults proves a zero block sets every
// retention field to zero, the runtime's defaults (retention off), replacing
// what was there.
func TestSetRetentionWithAZeroBlockLeavesTheDefaults(t *testing.T) {
	t.Parallel()
	cfg := SchedulerConfig{
		RetentionMaxAge: retentionDay,
		RetentionRules:  []RetentionRule{{State: StateDiscarded, MaxAge: retentionDay}},
		AuditRetention:  retentionDay,
	}

	cfg.SetRetention(config.RetentionConfig{})

	assert.Zero(t, cfg.RetentionMaxAge)
	assert.Nil(t, cfg.RetentionRules)
	assert.Zero(t, cfg.AuditRetention)
	assert.Zero(t, cfg.QueueSampleRetention)
	assert.Zero(t, cfg.NodeRetention)
	assert.Zero(t, cfg.StatsRetention)
}

// TestASchedulerPrunesByItsConfiguredRetention proves a retention block, set
// through SetRetention, prunes as the same SchedulerConfig fields set by hand
// do: the rule keeps discarded jobs 90 days, and every other job goes after 30.
func TestASchedulerPrunesByItsConfiguredRetention(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	seedAged(t, db, "succeeded-40d", "k", StateSucceeded, 40*retentionDay)
	seedAged(t, db, "discarded-40d", "k", StateDiscarded, 40*retentionDay)
	seedAged(t, db, "discarded-100d", "k", StateDiscarded, 100*retentionDay)
	cfg := SchedulerConfig{DB: db, Client: NewClient(db)}
	cfg.SetRetention(config.RetentionConfig{
		Jobs:  30 * retentionDay,
		Rules: []config.RetentionRule{{State: "discarded", MaxAge: 90 * retentionDay}},
	})
	s := newSchedulerCfg(t, cfg)

	n, err := s.PruneRetention(fixedClockCtx(retentionNow))
	require.NoError(t, err)
	assert.EqualValues(t, 2, n)
	assertJobsLeft(t, db, []string{"discarded-40d"}, []string{"succeeded-40d", "discarded-100d"})
}
