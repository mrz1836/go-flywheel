package config_test

import (
	"testing"
	"time"

	"github.com/mrz1836/go-flywheel/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestJobsConfig_Validate(t *testing.T) {
	t.Parallel()

	t.Run("fully configured job block is valid", func(t *testing.T) {
		t.Parallel()

		j := &config.JobsConfig{
			Queues:        []string{"default"},
			LeaseDuration: 30 * time.Second,
			Concurrency:   4,
		}
		require.NoError(t, j.Validate())
	})

	t.Run("empty queues is rejected", func(t *testing.T) {
		t.Parallel()

		j := &config.JobsConfig{LeaseDuration: time.Second, Concurrency: 1}
		require.Error(t, j.Validate())
	})

	t.Run("zero concurrency is rejected", func(t *testing.T) {
		t.Parallel()

		j := &config.JobsConfig{Queues: []string{"default"}, LeaseDuration: time.Second}
		require.Error(t, j.Validate())
	})

	t.Run("non-positive lease duration is rejected", func(t *testing.T) {
		t.Parallel()

		j := &config.JobsConfig{Queues: []string{"default"}, Concurrency: 1}
		require.Error(t, j.Validate())
	})
}

func TestJobsConfig_Configured(t *testing.T) {
	t.Parallel()

	t.Run("zero value is not configured", func(t *testing.T) {
		t.Parallel()

		j := &config.JobsConfig{}
		assert.False(t, j.Configured())
	})

	t.Run("any set field marks the block configured", func(t *testing.T) {
		t.Parallel()

		assert.True(t, (&config.JobsConfig{Queues: []string{"default"}}).Configured())
		assert.True(t, (&config.JobsConfig{Concurrency: 1}).Configured())
		assert.True(t, (&config.JobsConfig{LeaseDuration: time.Second}).Configured())
		assert.True(t, (&config.JobsConfig{PollInterval: time.Second}).Configured())
	})
}
