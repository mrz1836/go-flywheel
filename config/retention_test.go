package config_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/mrz1836/go-flywheel/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// configured is a job block the runtime accepts, for the retention cases to
// build on.
func configured(retention config.RetentionConfig) *config.JobsConfig {
	return &config.JobsConfig{
		Queues: []string{"default"}, LeaseDuration: 30 * time.Second, Concurrency: 4, Retention: retention,
	}
}

func TestJobsConfig_ValidateRetention(t *testing.T) {
	t.Parallel()

	const day = 24 * time.Hour

	t.Run("a full retention block is valid", func(t *testing.T) {
		t.Parallel()

		require.NoError(t, configured(config.RetentionConfig{
			Jobs: 30 * day,
			Rules: []config.RetentionRule{
				{State: "discarded", MaxAge: 90 * day},
				{Kind: "metrics.flush", MaxAge: 7 * day},
				{Kind: "metrics.flush", State: "succeeded", MaxAge: day},
			},
			Audit: 400 * day, QueueSamples: 30 * day, Nodes: 7 * day, Stats: 400 * day,
		}).Validate())
	})

	t.Run("no retention block is valid", func(t *testing.T) {
		t.Parallel()

		require.NoError(t, configured(config.RetentionConfig{}).Validate())
	})

	t.Run("a negative age is rejected, by name", func(t *testing.T) {
		t.Parallel()

		err := configured(config.RetentionConfig{Jobs: 30 * day, Nodes: -time.Hour}).Validate()
		require.ErrorContains(t, err, "Retention.Nodes")
	})

	t.Run("rules without a default age are rejected", func(t *testing.T) {
		t.Parallel()

		err := configured(config.RetentionConfig{
			Rules: []config.RetentionRule{{State: "discarded", MaxAge: day}},
		}).Validate()
		require.ErrorContains(t, err, "need Retention.Jobs")
	})

	for name, tc := range map[string]struct {
		rule config.RetentionRule
		want string
	}{
		"a rule naming neither a kind nor a state": {config.RetentionRule{MaxAge: day}, "neither a kind nor a state"},
		"a rule naming a live state":               {config.RetentionRule{State: "running", MaxAge: day}, `"running"`},
		"a rule with no age":                       {config.RetentionRule{Kind: "k"}, "max_age"},
	} {
		t.Run(name+" is rejected", func(t *testing.T) {
			t.Parallel()

			err := configured(config.RetentionConfig{
				Jobs:  30 * day,
				Rules: []config.RetentionRule{{State: "discarded", MaxAge: day}, tc.rule},
			}).Validate()
			require.ErrorContains(t, err, "Retention.Rules[1]")
			require.ErrorContains(t, err, tc.want)
		})
	}

	t.Run("two rules with the same kind and state are rejected", func(t *testing.T) {
		t.Parallel()

		err := configured(config.RetentionConfig{
			Jobs: 30 * day,
			Rules: []config.RetentionRule{
				{Kind: "k", State: "succeeded", MaxAge: day},
				{Kind: "k", State: "succeeded", MaxAge: 2 * day},
			},
		}).Validate()
		require.ErrorContains(t, err, "Retention.Rules[1]")
		require.ErrorContains(t, err, "repeats")
	})
}

func TestJobsConfig_ConfiguredByRetentionAlone(t *testing.T) {
	t.Parallel()

	j := &config.JobsConfig{Retention: config.RetentionConfig{Audit: time.Hour}}
	assert.True(t, j.Configured(), "a retention setting is a job-runtime setting")
	require.Error(t, j.Validate(), "and, once set, the rest of the block is validated")
}

func TestJobsConfig_DecodesRetentionFromJSON(t *testing.T) {
	t.Parallel()

	raw := `{
		"queues": ["default"],
		"lease_duration": 30000000000,
		"concurrency": 4,
		"retention": {
			"jobs": 2592000000000000,
			"rules": [
				{"state": "discarded", "max_age": 7776000000000000},
				{"kind": "heartbeat", "state": "succeeded", "max_age": 86400000000000}
			],
			"audit": 34560000000000000
		}
	}`
	var j config.JobsConfig
	require.NoError(t, json.Unmarshal([]byte(raw), &j))
	require.NoError(t, j.Validate())

	const day = 24 * time.Hour
	assert.Equal(t, config.RetentionConfig{
		Jobs: 30 * day,
		Rules: []config.RetentionRule{
			{State: "discarded", MaxAge: 90 * day},
			{Kind: "heartbeat", State: "succeeded", MaxAge: day},
		},
		Audit: 400 * day,
	}, j.Retention)
}
