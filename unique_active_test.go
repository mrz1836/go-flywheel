package flywheel

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

// assertUniqueActiveKeyEnforced exercises the unique-while-active contract end to
// end against db: a second active enqueue of the same key collides, a different
// key is unaffected, and once the holding job is terminal the key enqueues again.
// It is shared by the SQLite and (integration) Postgres suites so both dialects
// prove enforcement.
func assertUniqueActiveKeyEnforced(t *testing.T, db *gorm.DB) {
	t.Helper()
	ctx := context.Background()
	c := NewClient(db)

	id, err := Enqueue(ctx, c, "k", []byte(`{}`), InsertOpts{UniqueActiveKey: "subject-1"})
	require.NoError(t, err)

	_, err = Enqueue(ctx, c, "k", []byte(`{}`), InsertOpts{UniqueActiveKey: "subject-1"})
	require.ErrorIs(t, err, ErrAlreadyEnqueued, "a second active job for the same key is rejected")

	_, err = Enqueue(ctx, c, "k", []byte(`{}`), InsertOpts{UniqueActiveKey: "subject-2"})
	require.NoError(t, err, "a different active key is unaffected")

	// Once the holder reaches a terminal state the key frees up.
	require.NoError(t, CancelJob(ctx, db, id))
	_, err = Enqueue(ctx, c, "k", []byte(`{}`), InsertOpts{UniqueActiveKey: "subject-1"})
	require.NoError(t, err, "a terminal job no longer holds the active key")
}

func TestUniqueActiveKeyEnforcedSQLite(t *testing.T) {
	t.Parallel()
	assertUniqueActiveKeyEnforced(t, newDB(t))
}

// TestUniqueKeyRemainsForever proves the additive change leaves UniqueKey's
// unique-forever semantics intact: it collides even after the holder is terminal.
func TestUniqueKeyRemainsForever(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	ctx := context.Background()
	c := NewClient(db)

	id, err := Enqueue(ctx, c, "k", []byte(`{}`), InsertOpts{UniqueKey: "forever-1"})
	require.NoError(t, err)
	require.NoError(t, CancelJob(ctx, db, id))

	_, err = Enqueue(ctx, c, "k", []byte(`{}`), InsertOpts{UniqueKey: "forever-1"})
	require.ErrorIs(t, err, ErrAlreadyEnqueued, "unique_key collides forever, even after the job is terminal")
}

// TestUniqueActiveAndForeverKeysAreIndependent proves the two keys do not
// interfere: the same string used as UniqueActiveKey on one job and UniqueKey on
// another are separate constraints.
func TestUniqueActiveAndForeverKeysAreIndependent(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	ctx := context.Background()
	c := NewClient(db)

	_, err := Enqueue(ctx, c, "k", []byte(`{}`), InsertOpts{UniqueActiveKey: "shared"})
	require.NoError(t, err)
	_, err = Enqueue(ctx, c, "k", []byte(`{}`), InsertOpts{UniqueKey: "shared"})
	require.NoError(t, err, "the active-key and forever-key namespaces are independent")
}
