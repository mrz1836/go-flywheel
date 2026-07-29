package flywheel

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

// batchItems builds n plain BatchItems whose args carry their input index, so a
// test can prove IDs stay aligned to input order.
func batchItems(n int) []BatchItem {
	items := make([]BatchItem, n)
	for i := range items {
		items[i] = BatchItem{Kind: "batch.kind", Args: fmt.Appendf(nil, `{"n":%d}`, i)}
	}
	return items
}

// TestChunkSizeFor pins the dialect-aware sizing: default for a non-positive
// request, pass-through in range, clamp above the dialect maximum, and the safe
// SQLite bounds for an unrecognized dialect.
func TestChunkSizeFor(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		dialect   string
		requested int
		want      int
	}{
		{"sqlite default", "sqlite", 0, defaultSQLiteChunk},
		{"sqlite negative", "sqlite", -5, defaultSQLiteChunk},
		{"sqlite in range", "sqlite", 100, 100},
		{"sqlite clamped", "sqlite", 1_000_000, maxSQLiteChunk},
		{"postgres default", "postgres", 0, defaultPostgresChunk},
		{"postgres in range", "postgres", 2000, 2000},
		{"postgres clamped", "postgres", 1_000_000, maxPostgresChunk},
		{"unknown dialect uses sqlite bounds", "mysql", 0, defaultSQLiteChunk},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, chunkSizeFor(tc.dialect, tc.requested))
		})
	}
}

// TestConflictChunkRowsAffectedIsUnusableForCounting is a characterization gate:
// it pins the trap that forces the SELECT-back. A conflicting []jobRow chunk
// reports RowsAffected == len(chunk), so anyone who "optimizes" attribution back
// to RowsAffected breaks the count — and this test fails to say so.
func TestConflictChunkRowsAffectedIsUnusableForCounting(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	ctx := context.Background()

	// A pre-existing row whose unique_key the middle batch row collides with.
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
		"RowsAffected is pinned to len(chunk) by GORM's RETURNING write-back; if this ever drops to the "+
			"real landed count, attribution may move back off the SELECT-back")

	// Ground truth: only two of the three landed.
	var count int64
	require.NoError(t, db.Table("jobs").Where("kind = ?", "k").Count(&count).Error)
	assert.EqualValues(t, 3, count) // existing + 2 that landed
}

// TestConflictChunkSkipsIntraBatchDuplicate is the second characterization gate:
// two rows sharing a unique_key in ONE statement. The old one-by-one loop got
// intra-batch dedup for free; the batch relies on the DB's conflict semantics, so
// this proves targetless ON CONFLICT DO NOTHING dedups within a single statement
// on glebarez SQLite.
func TestConflictChunkSkipsIntraBatchDuplicate(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	ctx := context.Background()

	rows := []jobRow{
		buildRow(ctx, "k", []byte(`{}`), InsertOpts{UniqueKey: "same"}),
		buildRow(ctx, "k", []byte(`{}`), InsertOpts{UniqueKey: "same"}),
	}
	landed, err := conflictInsertChunk(ctx, db, rows)
	require.NoError(t, err)
	assert.Len(t, landed, 1, "exactly one of two intra-batch duplicates must land")

	var count int64
	require.NoError(t, db.Table("jobs").Where("unique_key = ?", "same").Count(&count).Error)
	assert.EqualValues(t, 1, count)
}

// TestInsertManyInsertsAllRowsInInputOrder is A1's core on SQLite: every row
// lands, IDs stay aligned to input order, and the chunk count matches ChunkSize.
func TestInsertManyInsertsAllRowsInInputOrder(t *testing.T) {
	t.Parallel()
	db := newDB(t)
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

	// One query, then confirm each returned id maps to the row whose args carry
	// that id's input index.
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

// TestInsertManyDefaultChunkSizeIsDialectDefault proves ChunkSize <= 0 selects
// the SQLite default rather than issuing one statement for the whole batch.
func TestInsertManyDefaultChunkSizeIsDialectDefault(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	c := NewClient(db)

	items := batchItems(defaultSQLiteChunk*2 + 10)
	res, err := InsertMany(context.Background(), c, items, BatchOpts{})
	require.NoError(t, err)
	assert.Equal(t, len(items), res.Inserted)
	assert.Equal(t, 3, res.Chunks) // ceil((2*500+10)/500)
}

// TestInsertManySkipsDuplicatesPerRow is A4 on SQLite: 250 unique_key and 100
// unique_active_key collisions against pre-existing jobs → Inserted 650,
// Skipped 350, and exactly the 350 colliding IDs empty, with no error.
func TestInsertManySkipsDuplicatesPerRow(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	c := NewClient(db)
	ctx := context.Background()

	const (
		total         = 1000
		ukCollisions  = 250
		uakCollisions = 100
	)
	for i := range ukCollisions {
		_, err := Enqueue(ctx, c, "pre", []byte(`{}`), InsertOpts{UniqueKey: fmt.Sprintf("uk-%d", i)})
		require.NoError(t, err)
	}
	for i := range uakCollisions {
		_, err := Enqueue(ctx, c, "pre", []byte(`{}`), InsertOpts{UniqueActiveKey: fmt.Sprintf("uak-%d", i)})
		require.NoError(t, err)
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
}

// TestInsertManySkipDuplicatesFlag pins the one thing the flag governs: whether a
// collision is reported in Skipped. Both modes land the same rows and clear the
// same IDs.
func TestInsertManySkipDuplicatesFlag(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name        string
		skip        bool
		wantSkipped int
	}{
		{"reported by default", false, 1},
		{"dropped when SkipDuplicates", true, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			db := newDB(t)
			c := NewClient(db)
			ctx := context.Background()

			_, err := Enqueue(ctx, c, "pre", []byte(`{}`), InsertOpts{UniqueKey: "dup"})
			require.NoError(t, err)

			items := []BatchItem{
				{Kind: "b", Args: []byte(`{}`)},
				{Kind: "b", Args: []byte(`{}`), Opts: InsertOpts{UniqueKey: "dup"}},
				{Kind: "b", Args: []byte(`{}`)},
			}
			res, err := InsertMany(ctx, c, items, BatchOpts{SkipDuplicates: tc.skip})
			require.NoError(t, err)
			assert.Equal(t, 2, res.Inserted)
			assert.Equal(t, tc.wantSkipped, res.Skipped)
			assert.NotEmpty(t, res.IDs[0])
			assert.Empty(t, res.IDs[1], "the colliding row's id is cleared in both modes")
			assert.NotEmpty(t, res.IDs[2])
		})
	}
}

// TestInsertManyReportsPartialProgress is A3 on SQLite: a row whose BeforeCreate
// fails in the third chunk aborts that chunk before any SQL, leaves chunks 0–1
// committed, and names chunk 2 in the error.
func TestInsertManyReportsPartialProgress(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	c := NewClient(db)
	ctx := context.Background()

	items := batchItems(250) // chunks of 100: [0,99] [100,199] [200,249]
	items[210].Kind = ""     // empty Kind → BeforeCreate ValidationError, in chunk 2

	res, err := InsertMany(ctx, c, items, BatchOpts{ChunkSize: 100})
	require.Error(t, err)
	assert.ErrorContains(t, err, "chunk 2")
	assert.ErrorIs(t, err, ErrValidation)

	assert.Equal(t, 2, res.Chunks, "chunks 0 and 1 committed")
	assert.Equal(t, 200, res.Inserted)
	var count int64
	require.NoError(t, db.Table("jobs").Count(&count).Error)
	assert.EqualValues(t, 200, count, "the failing chunk rolled back whole")
}

// TestInsertManyHonorsCallerTransaction proves the Tx seam on SQLite: a
// rolled-back caller transaction leaves no orphaned job, and a committed one
// lands every row. The cross-connection invisibility is proven on Postgres (A2).
func TestInsertManyHonorsCallerTransaction(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	items := []BatchItem{{Kind: "b", Args: []byte(`{}`)}, {Kind: "b", Args: []byte(`{}`)}}

	t.Run("rollback leaves nothing", func(t *testing.T) {
		t.Parallel()
		db := newDB(t)
		c := NewClient(db)

		errRollback := errors.New("roll back the domain txn")
		err := db.Transaction(func(tx *gorm.DB) error {
			res, e := InsertMany(ctx, c, items, BatchOpts{Tx: tx})
			require.NoError(t, e)
			require.Equal(t, 2, res.Inserted)
			return errRollback
		})
		require.ErrorIs(t, err, errRollback)

		var count int64
		require.NoError(t, db.Table("jobs").Count(&count).Error)
		assert.EqualValues(t, 0, count, "a rolled-back caller txn must leave no orphaned job")
	})

	t.Run("commit lands every row", func(t *testing.T) {
		t.Parallel()
		db := newDB(t)
		c := NewClient(db)

		err := db.Transaction(func(tx *gorm.DB) error {
			_, e := InsertMany(ctx, c, items, BatchOpts{Tx: tx})
			return e
		})
		require.NoError(t, err)

		var count int64
		require.NoError(t, db.Table("jobs").Count(&count).Error)
		assert.EqualValues(t, 2, count)
	})
}

// TestInsertManyTypedReadsKindAndMarshals proves the generic form reads Kind from
// the args value and applies the shared opts to every row.
func TestInsertManyTypedReadsKindAndMarshals(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	c := NewClient(db)
	ctx := context.Background()

	args := []typedArgs{{Value: "a"}, {Value: "b"}, {Value: "c"}}
	res, err := InsertManyTyped(ctx, c, args, InsertOpts{Queue: "q"}, BatchOpts{})
	require.NoError(t, err)
	assert.Equal(t, 3, res.Inserted)

	var rows []jobRow
	require.NoError(t, db.Find(&rows).Error)
	require.Len(t, rows, 3)
	for _, r := range rows {
		assert.Equal(t, "test.typed", r.Kind)
		assert.Equal(t, "q", r.Queue)
	}
}

// TestInsertManyTypedRejectsKindlessArgs mirrors Insert's ErrMissingKind contract.
func TestInsertManyTypedRejectsKindlessArgs(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	c := NewClient(db)
	_, err := InsertManyTyped(context.Background(), c, []kindlessArgs{{Value: "x"}}, InsertOpts{}, BatchOpts{})
	require.ErrorIs(t, err, ErrMissingKind)
}

// TestInsertManyEmptyInputIsNoop confirms an empty batch neither errors nor writes.
func TestInsertManyEmptyInputIsNoop(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	c := NewClient(db)
	res, err := InsertMany(context.Background(), c, nil, BatchOpts{})
	require.NoError(t, err)
	assert.Equal(t, BatchResult{}, res)
}
