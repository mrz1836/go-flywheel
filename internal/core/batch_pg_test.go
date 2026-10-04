//go:build integration

package core

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// TestInsertManyInsertsAllRowsInInputOrderPostgres is A1 on Postgres: every row
// lands, IDs stay aligned to input order, and the batch is split into the
// expected number of statements.
func TestInsertManyInsertsAllRowsInInputOrderPostgres(t *testing.T) {
	t.Parallel()
	db := NewPostgresIsolatedDB(t)
	c := NewClient(db)
	ctx := context.Background()

	const n = 2500
	items := batchItems(n)

	res, err := InsertMany(ctx, c, items, BatchOpts{ChunkSize: 1000})
	require.NoError(t, err)
	assert.Equal(t, n, res.Inserted)
	assert.Equal(t, 0, res.Skipped)
	assert.Equal(t, 3, res.Chunks) // ceil(2500/1000)
	require.Len(t, res.IDs, n)

	var count int64
	require.NoError(t, db.Table("jobs").Count(&count).Error)
	assert.EqualValues(t, n, count)

	var rows []jobRow
	require.NoError(t, db.Find(&rows).Error)
	byID := make(map[string]string, len(rows))
	for _, r := range rows {
		byID[r.ID] = string(r.Args)
	}
	for i, id := range res.IDs {
		require.NotEmptyf(t, id, "IDs[%d]", i)
		assert.JSONEq(t, fmt.Sprintf(`{"n":%d}`, i), byID[id])
	}
}

// TestInsertManyHonorsCallerTransactionPostgres is A2, the non-negotiable trap:
// with BatchOpts.Tx set the batch opens no transaction of its own, so its rows
// are invisible to any other connection until the caller commits, a rolled-back
// caller transaction leaves nothing, and a committed one lands every row.
func TestInsertManyHonorsCallerTransactionPostgres(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	items := []BatchItem{
		{Kind: "b", Args: []byte(`{}`)},
		{Kind: "b", Args: []byte(`{}`)},
		{Kind: "b", Args: []byte(`{}`)},
	}

	t.Run("uncommitted rows are invisible to another connection, then commit reveals all", func(t *testing.T) {
		t.Parallel()
		db := NewPostgresIsolatedDB(t)
		c := NewClient(db)

		// Read from a second pool connection while the domain transaction is still
		// open. Under READ COMMITTED it sees only committed rows, so the batch's
		// uncommitted rows must not appear — this is the orphaned-job check.
		var seenMidTx int64 = -1
		err := db.Transaction(func(tx *gorm.DB) error {
			res, e := InsertMany(ctx, c, items, BatchOpts{Tx: tx})
			if e != nil {
				return e
			}
			require.Equal(t, len(items), res.Inserted)
			return db.Table("jobs").Count(&seenMidTx).Error
		})
		require.NoError(t, err)
		assert.EqualValues(t, 0, seenMidTx, "another connection must not see uncommitted batch rows")

		var afterCommit int64
		require.NoError(t, db.Table("jobs").Count(&afterCommit).Error)
		assert.EqualValues(t, len(items), afterCommit, "commit reveals every row at once")
	})

	t.Run("rolled-back caller transaction leaves nothing", func(t *testing.T) {
		t.Parallel()
		db := NewPostgresIsolatedDB(t)
		c := NewClient(db)

		errRollback := errors.New("roll back the domain txn")
		err := db.Transaction(func(tx *gorm.DB) error {
			res, e := InsertMany(ctx, c, items, BatchOpts{Tx: tx})
			require.NoError(t, e)
			require.Equal(t, len(items), res.Inserted)
			return errRollback
		})
		require.ErrorIs(t, err, errRollback)

		var count int64
		require.NoError(t, db.Table("jobs").Count(&count).Error)
		assert.EqualValues(t, 0, count, "a rolled-back caller txn must leave no orphaned job")
	})
}

// TestInsertManyReportsPartialProgressPostgres is A3 on Postgres: a row whose
// BeforeCreate fails in the third chunk leaves chunks 0–1 committed, names chunk 2
// in the error, and reports no id for a row that never landed.
func TestInsertManyReportsPartialProgressPostgres(t *testing.T) {
	t.Parallel()
	db := NewPostgresIsolatedDB(t)
	c := NewClient(db)
	ctx := context.Background()

	items := batchItems(250) // chunks of 100: [0,99] [100,199] [200,249]
	items[210].Kind = ""     // empty Kind → BeforeCreate ValidationError, in chunk 2

	res, err := InsertMany(ctx, c, items, BatchOpts{ChunkSize: 100})
	require.Error(t, err)
	assert.ErrorContains(t, err, "chunk 2")
	assert.ErrorIs(t, err, ErrValidation)

	assert.Equal(t, 2, res.Chunks)
	assert.Equal(t, 200, res.Inserted)
	var count int64
	require.NoError(t, db.Table("jobs").Count(&count).Error)
	assert.EqualValues(t, 200, count, "the failing chunk rolled back whole")

	require.Len(t, res.IDs, len(items), "IDs stays aligned to the input")
	for i, id := range res.IDs[:200] {
		assert.NotEmptyf(t, id, "IDs[%d] landed in a committed chunk", i)
	}
	assert.Equal(t, make([]string, len(items)-200), res.IDs[200:],
		"a row that never landed has no id: its chunk failed")
}

// TestInsertManySkipsDuplicatesPerRowPostgres is A4 on Postgres: 250 unique_key
// and 100 unique_active_key collisions → Inserted 650, Skipped 350, exactly the
// 350 colliding IDs empty, each naming its holder in ExistingIDs, no error.
func TestInsertManySkipsDuplicatesPerRowPostgres(t *testing.T) {
	t.Parallel()
	db := NewPostgresIsolatedDB(t)
	c := NewClient(db)
	ctx := context.Background()

	const (
		total         = 1000
		ukCollisions  = 250
		uakCollisions = 100
	)
	holders := make([]string, 0, ukCollisions+uakCollisions)
	for i := range ukCollisions {
		id, err := Enqueue(ctx, c, "pre", []byte(`{}`), InsertOpts{UniqueKey: fmt.Sprintf("uk-%d", i)})
		require.NoError(t, err)
		holders = append(holders, id)
	}
	for i := range uakCollisions {
		id, err := Enqueue(ctx, c, "pre", []byte(`{}`), InsertOpts{UniqueActiveKey: fmt.Sprintf("uak-%d", i)})
		require.NoError(t, err)
		holders = append(holders, id)
	}

	items := make([]BatchItem, total)
	for i := range items {
		opts := InsertOpts{}
		switch {
		case i < ukCollisions:
			opts.UniqueKey = fmt.Sprintf("uk-%d", i)
		case i < ukCollisions+uakCollisions:
			opts.UniqueActiveKey = fmt.Sprintf("uak-%d", i-ukCollisions)
		}
		items[i] = BatchItem{Kind: "batch.kind", Args: []byte(`{}`), Opts: opts}
	}

	res, err := InsertMany(ctx, c, items, BatchOpts{ChunkSize: 300})
	require.NoError(t, err)
	assert.Equal(t, total-ukCollisions-uakCollisions, res.Inserted, "650 land")
	assert.Equal(t, ukCollisions+uakCollisions, res.Skipped, "350 collide")

	emptyIDs := 0
	for i, id := range res.IDs {
		if id == "" {
			emptyIDs++
			assert.Less(t, i, ukCollisions+uakCollisions, "only the leading colliding rows should be empty")
		}
	}
	assert.Equal(t, ukCollisions+uakCollisions, emptyIDs)

	// Each skipped row names the pre-existing job holding its key; every row that
	// landed has no holder.
	require.Len(t, res.ExistingIDs, total, "ExistingIDs is aligned with IDs")
	assert.Equal(t, holders, res.ExistingIDs[:ukCollisions+uakCollisions])
	assert.Equal(t, make([]string, total-ukCollisions-uakCollisions), res.ExistingIDs[ukCollisions+uakCollisions:])
}

// TestInsertManyIntraBatchDuplicatesNameTheSiblingThatLandedPostgres proves an
// intra-batch duplicate names the sibling that landed on PostgreSQL, where one
// multi-row ON CONFLICT DO NOTHING statement inserts the rows in input order.
func TestInsertManyIntraBatchDuplicatesNameTheSiblingThatLandedPostgres(t *testing.T) {
	t.Parallel()
	assertInsertManyIntraBatchDuplicatesNameTheSibling(t, NewPostgresIsolatedDB(t))
}

// TestInsertManyTxNamesHoldersOnTheCallersTransactionPostgres proves the batch's
// holder read sees a holder still uncommitted on the caller's transaction, which
// no other connection can.
func TestInsertManyTxNamesHoldersOnTheCallersTransactionPostgres(t *testing.T) {
	t.Parallel()
	assertInsertManyTxNamesHolders(t, NewPostgresIsolatedDB(t))
}

// TestConflictChunkRowsAffectedIsUnusableForCountingPostgres is the RowsAffected
// characterization gate on Postgres — the second dialect where the SELECT-back
// must not be regressed back to RowsAffected.
func TestConflictChunkRowsAffectedIsUnusableForCountingPostgres(t *testing.T) {
	t.Parallel()
	db := NewPostgresIsolatedDB(t)
	ctx := context.Background()

	existing := buildRow(ctx, "k", []byte(`{}`), InsertOpts{UniqueKey: "dup"})
	require.NoError(t, db.Create(&existing).Error)

	rows := []jobRow{
		buildRow(ctx, "k", []byte(`{}`), InsertOpts{}),
		buildRow(ctx, "k", []byte(`{}`), InsertOpts{UniqueKey: "dup"}),
		buildRow(ctx, "k", []byte(`{}`), InsertOpts{}),
	}
	res := db.WithContext(ctx).Clauses(clause.OnConflict{DoNothing: true}).Create(&rows)
	require.NoError(t, res.Error)

	assert.EqualValues(t, len(rows), res.RowsAffected,
		"RowsAffected is pinned to len(chunk) by GORM's RETURNING write-back on Postgres too")

	var count int64
	require.NoError(t, db.Table("jobs").Where("kind = ?", "k").Count(&count).Error)
	assert.EqualValues(t, 3, count) // existing + 2 that landed
}
