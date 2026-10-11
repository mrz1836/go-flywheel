package core

import (
	"context"
	"testing"
	"time"

	"github.com/mrz1836/go-foundation/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestRetentionFreesAUniqueKey pins what InsertOpts.UniqueKey documents: the key
// collides with its job, terminal or not, for as long as the job's row exists,
// and retention's hard delete of that job frees it, so the next insert with the
// key lands as a new job.
func TestRetentionFreesAUniqueKey(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	c := NewClient(db)
	finished := time.Now().Add(-48 * time.Hour)
	past := models.WithClock(context.Background(), models.NewFixedClock(finished))

	first, err := Enqueue(past, c, "k", []byte(`{}`), InsertOpts{UniqueKey: "once"})
	require.NoError(t, err)
	require.NoError(t, CancelJob(past, db, first), "the holder reaches a terminal state")

	_, err = Enqueue(context.Background(), c, "k", []byte(`{}`), InsertOpts{UniqueKey: "once"})
	require.ErrorIs(t, err, ErrAlreadyEnqueued, "a terminal holder still holds the key")

	deleted, err := DeleteFinishedJobs(context.Background(), db, time.Now().Add(-24*time.Hour))
	require.NoError(t, err)
	require.EqualValues(t, 1, deleted, "retention deletes the holder")

	second, err := Enqueue(context.Background(), c, "k", []byte(`{}`), InsertOpts{UniqueKey: "once"})
	require.NoError(t, err, "with its holder deleted the key is free again")
	assert.NotEqual(t, first, second)
}
