//go:build integration

package core

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

// TestConcurrentPausesOfOneQueueWriteOneEntryPostgres races pauses of one queue,
// none of them holding its row yet, then resumes of it: each race changes the
// queue once and writes one entry, and every call returns nil.
func TestConcurrentPausesOfOneQueueWriteOneEntryPostgres(t *testing.T) {
	t.Parallel()
	db := NewPostgresIsolatedDB(t)
	ctx := WithActor(context.Background(), queueOperator)

	race := func(call func(context.Context, *gorm.DB, string) error) {
		var wg sync.WaitGroup
		errs := make(chan error, 8)
		for range 8 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				errs <- call(ctx, db, "race")
			}()
		}
		wg.Wait()
		close(errs)
		for err := range errs {
			require.NoError(t, err)
		}
	}

	race(PauseQueue)
	row, ok := queueRow(t, db, "race")
	require.True(t, ok)
	require.NotNil(t, row.PausedAt)
	assert.Equal(t, "provider outage", row.PausedReason)
	entries := auditRows(t, db, "race")
	require.Len(t, entries, 1, "racing pauses write one entry")
	assert.Equal(t, string(AuditPaused), entries[0].Action)

	race(ResumeQueue)
	row, _ = queueRow(t, db, "race")
	assert.Nil(t, row.PausedAt)
	entries = auditRows(t, db, "race")
	require.Len(t, entries, 2, "racing resumes write one entry")
	assert.Equal(t, string(AuditResumed), entries[1].Action)
}
