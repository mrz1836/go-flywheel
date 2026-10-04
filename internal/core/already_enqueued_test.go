package core

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/glebarez/sqlite"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/datatypes"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// TestAlreadyEnqueuedErrorMatchesTheSentinel pins the compatibility contract of
// the typed collision error: errors.Is and errors.As reach it directly and through
// a wrap, its message is the sentinel's unchanged, and it Unwraps to the sentinel —
// the walk a hand-rolled matcher (errorsIs in the integration suite) relies on.
func TestAlreadyEnqueuedErrorMatchesTheSentinel(t *testing.T) {
	t.Parallel()
	dup := &AlreadyEnqueuedError{ExistingID: "job-1", Key: "k"}

	for name, err := range map[string]error{
		"direct":  dup,
		"wrapped": fmt.Errorf("caller: %w", dup),
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			require.ErrorIs(t, err, ErrAlreadyEnqueued)
			var got *AlreadyEnqueuedError
			require.ErrorAs(t, err, &got)
			assert.Same(t, dup, got)
		})
	}

	assert.Equal(t, "jobs: already enqueued", dup.Error(), "the message is the sentinel's, unchanged")
	assert.Equal(t, ErrAlreadyEnqueued.Error(), dup.Error())
	assert.Same(t, ErrAlreadyEnqueued, errors.Unwrap(dup), "Unwrap reaches the sentinel itself")
}

// keyCase is one shape of keyed insert: which keys the colliding insert sets.
type keyCase struct {
	name string
	opts func(suffix string) InsertOpts
	// key is the key the collision reports for the given suffix.
	key func(suffix string) string
}

// keyCases covers the three keyed shapes an insert can take.
func keyCases() []keyCase {
	return []keyCase{
		{
			name: "unique key",
			opts: func(s string) InsertOpts { return InsertOpts{UniqueKey: "uk-" + s} },
			key:  func(s string) string { return "uk-" + s },
		},
		{
			name: "unique active key",
			opts: func(s string) InsertOpts { return InsertOpts{UniqueActiveKey: "uak-" + s} },
			key:  func(s string) string { return "uak-" + s },
		},
		{
			name: "both keys",
			opts: func(s string) InsertOpts { return InsertOpts{UniqueKey: "uk-" + s, UniqueActiveKey: "uak-" + s} },
			key:  func(s string) string { return "uk-" + s },
		},
	}
}

// requireHolder asserts err is a collision naming holder under key.
func requireHolder(t *testing.T, err error, holder, key string) {
	t.Helper()
	require.ErrorIs(t, err, ErrAlreadyEnqueued)
	var dup *AlreadyEnqueuedError
	require.ErrorAs(t, err, &dup)
	assert.Equal(t, holder, dup.ExistingID, "the collision names the job holding the key")
	assert.Equal(t, key, dup.Key)
}

// holderCase is one way a job can hold the key a later insert collides on.
type holderCase struct {
	name string
	// hold enqueues (and then shapes) the holder, returning its id.
	hold func(t *testing.T, ctx context.Context, db *gorm.DB, c *Client) string
	// insert is the colliding insert's options, and key the key it reports.
	insert InsertOpts
	key    string
}

// enqueueHolder returns a hold func that enqueues a job with opts, then applies
// shape (nil for none) to it.
func enqueueHolder(opts InsertOpts, shape func(t *testing.T, ctx context.Context, db *gorm.DB, id string)) func(
	t *testing.T, ctx context.Context, db *gorm.DB, c *Client,
) string {
	return func(t *testing.T, ctx context.Context, db *gorm.DB, c *Client) string {
		t.Helper()
		id, err := Enqueue(ctx, c, "holder", []byte(`{}`), opts)
		require.NoError(t, err)
		if shape != nil {
			shape(t, ctx, db, id)
		}
		return id
	}
}

// holderCases covers every way a job holds a key: a UniqueKey held by a live, a
// cancelled, and a soft-deleted job (neither unique index excludes deleted_at, so
// the read is Unscoped), a UniqueActiveKey held by an available and a paused job,
// and inserts that set both keys.
func holderCases() []holderCase {
	cancel := func(t *testing.T, ctx context.Context, db *gorm.DB, id string) {
		t.Helper()
		require.NoError(t, CancelJob(ctx, db, id))
	}
	softDelete := func(t *testing.T, _ context.Context, db *gorm.DB, id string) {
		t.Helper()
		require.NoError(t, db.Delete(&jobRow{}, "id = ?", id).Error)
	}
	pause := func(t *testing.T, _ context.Context, db *gorm.DB, id string) {
		t.Helper()
		require.NoError(t, db.Model(&jobRow{}).Where("id = ?", id).Update("state", string(StatePaused)).Error)
	}
	return []holderCase{
		{
			name:   "unique key held by a live job",
			hold:   enqueueHolder(InsertOpts{UniqueKey: "uk-live"}, nil),
			insert: InsertOpts{UniqueKey: "uk-live"}, key: "uk-live",
		},
		{
			name:   "unique key held by a cancelled job",
			hold:   enqueueHolder(InsertOpts{UniqueKey: "uk-cancelled"}, cancel),
			insert: InsertOpts{UniqueKey: "uk-cancelled"}, key: "uk-cancelled",
		},
		{
			name:   "unique key held by a soft-deleted job",
			hold:   enqueueHolder(InsertOpts{UniqueKey: "uk-deleted"}, softDelete),
			insert: InsertOpts{UniqueKey: "uk-deleted"}, key: "uk-deleted",
		},
		{
			name:   "unique active key held by an available job",
			hold:   enqueueHolder(InsertOpts{UniqueActiveKey: "uak-available"}, nil),
			insert: InsertOpts{UniqueActiveKey: "uak-available"}, key: "uak-available",
		},
		{
			name:   "unique active key held by a paused job",
			hold:   enqueueHolder(InsertOpts{UniqueActiveKey: "uak-paused"}, pause),
			insert: InsertOpts{UniqueActiveKey: "uak-paused"}, key: "uak-paused",
		},
		{
			name:   "both keys set, the unique key held",
			hold:   enqueueHolder(InsertOpts{UniqueKey: "uk-both"}, nil),
			insert: InsertOpts{UniqueKey: "uk-both", UniqueActiveKey: "uak-both-free"}, key: "uk-both",
		},
		{
			name:   "both keys set, only the unique active key held",
			hold:   enqueueHolder(InsertOpts{UniqueActiveKey: "uak-only"}, nil),
			insert: InsertOpts{UniqueKey: "uk-only-free", UniqueActiveKey: "uak-only"}, key: "uak-only",
		},
		{
			name: "both keys held by different jobs, the unique key holder named",
			hold: func(t *testing.T, ctx context.Context, db *gorm.DB, c *Client) string {
				t.Helper()
				enqueueHolder(InsertOpts{UniqueActiveKey: "uak-split"}, nil)(t, ctx, db, c)
				return enqueueHolder(InsertOpts{UniqueKey: "uk-split"}, nil)(t, ctx, db, c)
			},
			insert: InsertOpts{UniqueKey: "uk-split", UniqueActiveKey: "uak-split"}, key: "uk-split",
		},
	}
}

// assertCollisionNamesTheHolder proves every way a job can hold a key comes back
// named, on Insert and on Enqueue alike. It is shared by the SQLite and
// (integration) PostgreSQL suites.
func assertCollisionNamesTheHolder(t *testing.T, db *gorm.DB) {
	t.Helper()
	ctx := context.Background()
	c := NewClient(db)
	for _, tc := range holderCases() {
		holder := tc.hold(t, ctx, db, c)

		_, err := Enqueue(ctx, c, "dup", []byte(`{}`), tc.insert)
		requireHolder(t, err, holder, tc.key)

		_, err = Insert(ctx, c, typedArgs{Value: "dup"}, tc.insert)
		requireHolder(t, err, holder, tc.key)
	}
	var landed int64
	require.NoError(t, db.Model(&jobRow{}).Where("kind IN ?", []string{"dup", "test.typed"}).Count(&landed).Error)
	assert.Zero(t, landed, "no colliding insert landed")
}

func TestInsertCollisionNamesTheHolder(t *testing.T) {
	t.Parallel()
	assertCollisionNamesTheHolder(t, newDB(t))
}

// failHolderReads registers a Query callback on db that fails every read of the
// columns readKeyHolders selects — the holder lookup and nothing else, so the
// insert and its landing read-back still run.
func failHolderReads(t *testing.T, db *gorm.DB) error {
	t.Helper()
	errRead := errors.New("holder read failed")
	require.NoError(t, db.Callback().Query().Before("gorm:query").Register("test:fail_holder_read",
		func(tx *gorm.DB) {
			if slices.Contains(tx.Statement.Selects, "unique_active_key") {
				_ = tx.AddError(errRead)
			}
		}))
	return errRead
}

// TestInsertCollisionLookupFailureStillReportsTheCollision proves a failed holder
// read never masks the collision: the error is still ErrAlreadyEnqueued, with the
// key it collided on and no holder, on the write connection and on a caller's
// transaction alike. With both keys set and nothing read, the key reported is
// the first one set.
func TestInsertCollisionLookupFailureStillReportsTheCollision(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	t.Run("write connection", func(t *testing.T) {
		t.Parallel()
		db := newDB(t)
		c := NewClient(db)
		_, err := Enqueue(ctx, c, "holder", []byte(`{}`), InsertOpts{UniqueKey: "k"})
		require.NoError(t, err)
		errRead := failHolderReads(t, db)

		_, err = Enqueue(ctx, c, "dup", []byte(`{}`), InsertOpts{UniqueKey: "k"})
		requireHolder(t, err, "", "k")
		assert.NotErrorIs(t, err, errRead, "the read failure is not what the caller sees")
	})

	t.Run("caller transaction", func(t *testing.T) {
		t.Parallel()
		db := newDB(t)
		c := NewClient(db)
		_, err := Enqueue(ctx, c, "holder", []byte(`{}`), InsertOpts{UniqueActiveKey: "k"})
		require.NoError(t, err)
		_ = failHolderReads(t, db)

		var collision error
		require.NoError(t, db.Transaction(func(tx *gorm.DB) error {
			_, collision = Enqueue(ctx, c, "dup", []byte(`{}`), InsertOpts{UniqueActiveKey: "k", Tx: tx})
			return nil
		}))
		requireHolder(t, collision, "", "k")
	})

	t.Run("both keys set: the first key set is reported", func(t *testing.T) {
		t.Parallel()
		db := newDB(t)
		c := NewClient(db)
		_, err := Enqueue(ctx, c, "holder", []byte(`{}`), InsertOpts{UniqueActiveKey: "active"})
		require.NoError(t, err)
		_ = failHolderReads(t, db)

		_, err = Enqueue(ctx, c, "dup", []byte(`{}`), InsertOpts{UniqueKey: "unique", UniqueActiveKey: "active"})
		requireHolder(t, err, "", "unique")
	})
}

// assertTxCollisionLeavesTheTransactionUsable proves a collision on
// InsertOpts.Tx does not abort the caller's transaction: the caller's writes
// before and after the colliding insert both commit, and the collision still
// names the holder. On PostgreSQL a failed INSERT aborts the transaction it runs
// in (25P02 on every later statement), so this is the assertion a plain INSERT on
// Tx fails.
func assertTxCollisionLeavesTheTransactionUsable(t *testing.T, db *gorm.DB) {
	t.Helper()
	ctx := context.Background()
	c := NewClient(db)

	for i, tc := range keyCases() {
		suffix := string(rune('a' + i))
		holder, err := Enqueue(ctx, c, "holder", []byte(`{}`), tc.opts(suffix))
		require.NoError(t, err)

		var collision error
		err = db.Transaction(func(tx *gorm.DB) error {
			if _, e := Enqueue(ctx, c, "before."+suffix, []byte(`{}`), InsertOpts{Tx: tx}); e != nil {
				return e
			}
			opts := tc.opts(suffix)
			opts.Tx = tx
			_, collision = Enqueue(ctx, c, "dup", []byte(`{}`), opts)
			if _, e := Enqueue(ctx, c, "after."+suffix, []byte(`{}`), InsertOpts{Tx: tx}); e != nil {
				return e
			}
			return nil
		})
		require.ErrorIs(t, collision, ErrAlreadyEnqueued, "%s: the colliding insert reports the collision", tc.name)
		require.NoError(t, err, "%s: the caller's transaction stays usable after the collision and commits", tc.name)
		requireHolder(t, collision, holder, tc.key(suffix))

		for _, kind := range []string{"before." + suffix, "after." + suffix} {
			var n int64
			require.NoError(t, db.Model(&jobRow{}).Where("kind = ?", kind).Count(&n).Error)
			assert.EqualValues(t, 1, n, "%s: the caller's %s write committed", tc.name, kind)
		}
	}
	var dups int64
	require.NoError(t, db.Model(&jobRow{}).Where("kind = ?", "dup").Count(&dups).Error)
	assert.Zero(t, dups, "no colliding insert landed")
}

func TestInsertTxCollisionLeavesTheTransactionUsable(t *testing.T) {
	t.Parallel()
	assertTxCollisionLeavesTheTransactionUsable(t, newDB(t))
}

// assertTxCollisionNamesASibling proves the holder is read on the caller's
// transaction: a holder written earlier in the same, still-open transaction is
// named. On PostgreSQL that holder is invisible to any other connection until the
// commit, so a read on the write connection would come back empty.
func assertTxCollisionNamesASibling(t *testing.T, db *gorm.DB) {
	t.Helper()
	ctx := context.Background()
	c := NewClient(db)

	for i, tc := range keyCases() {
		suffix := fmt.Sprintf("sibling-%d", i)
		require.NoError(t, db.Transaction(func(tx *gorm.DB) error {
			opts := tc.opts(suffix)
			opts.Tx = tx
			sibling, err := Enqueue(ctx, c, "sibling", []byte(`{}`), opts)
			require.NoError(t, err)

			_, err = Enqueue(ctx, c, "dup", []byte(`{}`), opts)
			requireHolder(t, err, sibling, tc.key(suffix))
			return nil
		}), "%s: the transaction commits", tc.name)
	}
	var siblings int64
	require.NoError(t, db.Model(&jobRow{}).Where("kind = ?", "sibling").Count(&siblings).Error)
	assert.EqualValues(t, len(keyCases()), siblings, "every sibling committed, and no duplicate did")
}

func TestInsertTxCollisionNamesASiblingInTheSameTransaction(t *testing.T) {
	t.Parallel()
	assertTxCollisionNamesASibling(t, newDB(t))
}

// TestInsertTxCollisionReadsOnTheTransaction proves the holder read runs on the
// caller's transaction, not the client's write connection. On a single-connection
// SQLite pool the transaction holds the only connection, so a read on the write
// connection would wait for it until ctx expired — the read fails, ExistingID
// comes back empty, and this test fails instead of hanging.
func TestInsertTxCollisionReadsOnTheTransaction(t *testing.T) {
	t.Parallel()
	db := newSingleConnMemoryDB(t)
	c := NewClient(db)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	holder, err := Enqueue(ctx, c, "holder", []byte(`{}`), InsertOpts{UniqueKey: "k"})
	require.NoError(t, err)

	require.NoError(t, db.Transaction(func(tx *gorm.DB) error {
		_, err := Enqueue(ctx, c, "dup", []byte(`{}`), InsertOpts{UniqueKey: "k", Tx: tx})
		requireHolder(t, err, holder, "k")
		return nil
	}))
}

// TestInsertTxCostsOneReadOnlyWithAKey pins what a Tx insert costs: an unkeyed
// insert is a plain INSERT with no read at all, and a keyed one is an
// ON CONFLICT DO NOTHING insert plus exactly one primary-key read-back.
func TestInsertTxCostsOneReadOnlyWithAKey(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name         string
		opts         InsertOpts
		wantConflict bool
		wantReads    int
	}{
		{"unkeyed: a plain insert", InsertOpts{}, false, 0},
		{"keyed: conflict insert plus a primary-key read-back", InsertOpts{UniqueKey: "k"}, true, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			db := newDB(t)
			sqls, rec := countStatements(db)

			require.NoError(t, rec.Transaction(func(tx *gorm.DB) error {
				opts := tc.opts
				opts.Tx = tx
				id, err := Enqueue(context.Background(), NewClient(db), "k", []byte(`{}`), opts)
				require.NotEmpty(t, id)
				return err
			}))

			var inserts, reads []string
			for _, s := range *sqls {
				switch {
				case strings.HasPrefix(s, "INSERT"):
					inserts = append(inserts, s)
				case strings.HasPrefix(s, "SELECT"):
					reads = append(reads, s)
				}
			}
			require.Len(t, inserts, 1)
			assert.Equal(t, tc.wantConflict, strings.Contains(inserts[0], "ON CONFLICT"), inserts[0])
			require.Len(t, reads, tc.wantReads, "%v", reads)
			if tc.wantReads > 0 {
				assert.Contains(t, reads[0], "WHERE id IN", "the read-back is a primary-key read")
			}
		})
	}
}

// TestInsertTxKeyedSurfacesNonDuplicateError proves a keyed insert on Tx that
// fails for a reason other than a collision is surfaced as an insert error, not
// mistaken for one.
func TestInsertTxKeyedSurfacesNonDuplicateError(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	c := NewClient(db)

	var insertErr error
	require.NoError(t, db.Transaction(func(tx *gorm.DB) error {
		_, insertErr = Enqueue(context.Background(), c, "", []byte(`{}`), InsertOpts{UniqueKey: "k", Tx: tx})
		return nil
	}))
	require.ErrorIs(t, insertErr, ErrValidation, "the empty kind is rejected before any SQL")
	assert.NotErrorIs(t, insertErr, ErrAlreadyEnqueued)
	assert.True(t, strings.HasPrefix(insertErr.Error(), "jobs: insert: "), insertErr.Error())
}

// TestInsertTxKeyedReadBackFailureNamesTheInsert proves a failed read-back of a
// keyed single insert on Tx is reported as that insert's failure, in its own
// words, rather than as a batch's or as a collision.
func TestInsertTxKeyedReadBackFailureNamesTheInsert(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	errRead := errors.New("read failed")
	require.NoError(t, db.Callback().Query().Before("gorm:query").Register("test:fail_reads",
		func(tx *gorm.DB) { _ = tx.AddError(errRead) }))

	var insertErr error
	require.NoError(t, db.Transaction(func(tx *gorm.DB) error {
		_, insertErr = Enqueue(context.Background(), NewClient(db), "k", []byte(`{}`), InsertOpts{UniqueKey: "k", Tx: tx})
		return nil
	}))
	require.ErrorIs(t, insertErr, errRead)
	assert.NotErrorIs(t, insertErr, ErrAlreadyEnqueued)
	assert.EqualError(t, insertErr, "jobs: insert: read back the row: read failed")
}

// TestEnqueueBucketCollisionReadsNothing proves the Scheduler never pays for a
// holder read: a tick's collision is a no-op it has no use for the holder of.
func TestEnqueueBucketCollisionReadsNothing(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	sched := newScheduler(t, db)
	ctx := context.Background()
	def := jobPeriodicRow{Slug: "tick", Kind: "tick.k", Queue: "periodic", ArgsTemplate: datatypes.JSON("{}")}
	bucket := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)

	ok, err := sched.enqueueBucket(ctx, def, bucket)
	require.NoError(t, err)
	require.True(t, ok, "the first fire for a bucket enqueues")

	var reads atomic.Int32
	require.NoError(t, db.Callback().Query().Before("gorm:query").Register("test:count_reads",
		func(*gorm.DB) { reads.Add(1) }))

	ok, err = sched.enqueueBucket(ctx, def, bucket)
	require.NoError(t, err, "a redundant fire is a successful no-op")
	assert.False(t, ok, "the redundant fire enqueued nothing")
	assert.Zero(t, reads.Load(), "the collision issued no read")
}

// TestHolderLookupMirrorsTheUniqueIndexes pins readKeyHolders' predicate to the
// index definitions it mirrors, read from IndexSet rather than copied: the
// active-key index is partial on exactly the live states, spelled exactly as
// liveStatesSQL spells them — a planner matches the literal list, so a reordered
// or respelled one would leave the index unusable — and neither index excludes
// soft-deleted rows, which is why the read is Unscoped.
func TestHolderLookupMirrorsTheUniqueIndexes(t *testing.T) {
	t.Parallel()
	require.Equal(t, "('"+strings.Join(nonTerminalStateStrings(), "', '")+"')", liveStatesSQL())
	livePredicate := "unique_active_key IS NOT NULL AND state IN " + liveStatesSQL()

	for _, dialect := range []string{"sqlite", "postgres"} {
		set, err := IndexSet(dialect)
		require.NoError(t, err)
		ddl := map[string]string{}
		for _, idx := range set {
			ddl[idx.Name] = idx.DDL
		}
		require.Contains(t, ddl, "jobs_unique_key")
		require.Contains(t, ddl, "jobs_unique_active_key")

		assert.True(t, strings.HasSuffix(ddl["jobs_unique_key"], "WHERE unique_key IS NOT NULL"),
			"%s: jobs_unique_key carries no state condition: %s", dialect, ddl["jobs_unique_key"])
		assert.True(t, strings.HasSuffix(ddl["jobs_unique_active_key"], "WHERE "+livePredicate),
			"%s: jobs_unique_active_key is partial on exactly the live states: %s", dialect, ddl["jobs_unique_active_key"])
		for name, def := range ddl {
			if strings.HasPrefix(name, "jobs_unique") {
				assert.NotContains(t, def, "deleted_at", "%s: %s does not exclude soft-deleted rows", dialect, name)
			}
		}
	}
}

// TestReadKeyHoldersCountsAnActiveKeyOnlyOnALiveHolder proves the Go-side half
// of the active-key predicate: a row the unique_key half matched may carry a
// UniqueActiveKey it no longer holds (it is terminal), and that row must not be
// named as the active key's holder. The live job that does hold it is.
func TestReadKeyHoldersCountsAnActiveKeyOnlyOnALiveHolder(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	ctx := context.Background()
	c := NewClient(db)

	old, err := Enqueue(ctx, c, "k", []byte(`{}`), InsertOpts{UniqueKey: "uk", UniqueActiveKey: "uak"})
	require.NoError(t, err)
	require.NoError(t, CancelJob(ctx, db, old))

	h, err := readKeyHolders(ctx, db, []string{"uk"}, []string{"uak"})
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"uk": old}, h.byKey)
	assert.Empty(t, h.byActiveKey, "a cancelled job does not hold its active key")

	live, err := Enqueue(ctx, c, "k", []byte(`{}`), InsertOpts{UniqueActiveKey: "uak"})
	require.NoError(t, err)
	h, err = readKeyHolders(ctx, db, []string{"uk", "uk", ""}, []string{"uak"})
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"uk": old}, h.byKey)
	assert.Equal(t, map[string]string{"uak": live}, h.byActiveKey)

	sqls, rec := countStatements(db)
	h, err = readKeyHolders(ctx, rec, nil, []string{""})
	require.NoError(t, err)
	assert.Empty(t, h.byKey)
	assert.Empty(t, h.byActiveKey)
	assert.Empty(t, *sqls, "no key, no read")
}

// seedHolderPlanFixture fills jobs with enough keyed and unkeyed rows — some
// holding a unique key, some a live active key, some a terminal one — that a
// planner choosing a full scan over a usable index would be choosing badly, then
// refreshes the planner's statistics for jobs.
func seedHolderPlanFixture(t *testing.T, db *gorm.DB) {
	t.Helper()
	const n = 3000
	items := make([]BatchItem, n)
	for i := range items {
		opts := InsertOpts{}
		switch i % 3 {
		case 0:
			opts.UniqueKey = fmt.Sprintf("uk-%d", i)
		case 1:
			opts.UniqueActiveKey = fmt.Sprintf("uak-%d", i)
		}
		items[i] = BatchItem{Kind: "plan", Args: []byte(`{}`), Opts: opts}
	}
	_, err := InsertMany(context.Background(), NewClient(db), items, BatchOpts{})
	require.NoError(t, err)
	require.NoError(t, db.Model(&jobRow{}).Where("unique_active_key LIKE ?", "%7").
		Update("state", string(StateCancelled)).Error)
	// Only this test's jobs table: a bare ANALYZE on PostgreSQL walks every table in
	// the database, every parallel test's schema included, and its lock on each one
	// stalls their concurrent index builds.
	require.NoError(t, db.Exec("ANALYZE jobs").Error)
}

// holderReadStatement returns the statement readKeyHolders sends for keys and
// activeKeys with its bind variables still separate, so the planner sees the
// placeholders the driver sends rather than values a logger inlined. Which
// predicates are bound and which are literal is exactly what decides whether a
// partial index is usable.
func holderReadStatement(t *testing.T, db *gorm.DB, keys, activeKeys []string) (string, []any) {
	t.Helper()
	var rows []jobRow
	stmt := keyHoldersQuery(db.Session(&gorm.Session{DryRun: true}), keys, activeKeys).Find(&rows).Statement
	require.NoError(t, stmt.Error)
	return stmt.SQL.String(), stmt.Vars
}

// holderPlanCase is one shape of the holder read and the indexes its plan must
// use.
type holderPlanCase struct {
	name         string
	keys, active []string
	indexes      []string
}

// holderPlanCases covers the three shapes of the holder read.
func holderPlanCases() []holderPlanCase {
	return []holderPlanCase{
		{"unique key only", []string{"uk-0"}, nil, []string{"jobs_unique_key"}},
		{"active key only", nil, []string{"uak-1"}, []string{"jobs_unique_active_key"}},
		{"both keys", []string{"uk-0"}, []string{"uak-1"}, []string{"jobs_unique_key", "jobs_unique_active_key"}},
	}
}

// TestHolderReadReachesTheUniqueIndexes pins the holder read's plan on SQLite: a
// lookup through each partial unique index, OR-ed when both keys are set, never a
// scan of jobs. SQLite uses a partial index only when the query's WHERE repeats
// the index's predicate term for term, so the live states must be SQL literals; a
// bound list leaves jobs_unique_active_key unusable.
func TestHolderReadReachesTheUniqueIndexes(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	seedHolderPlanFixture(t, db)

	for _, tc := range holderPlanCases() {
		query, vars := holderReadStatement(t, db, tc.keys, tc.active)
		plan := sqlitePlan(t, db, query, vars...)
		for _, idx := range tc.indexes {
			assert.Contains(t, plan, "USING INDEX "+idx+" ", "%s: the read uses %s\n%s", tc.name, idx, plan)
		}
		if len(tc.indexes) > 1 {
			assert.Contains(t, plan, "MULTI-INDEX OR", "%s\n%s", tc.name, plan)
		}
		assert.NotContains(t, plan, "SCAN jobs", "%s: no scan of jobs\n%s", tc.name, plan)
	}
}

// newTranslateErrorDB opens a migrated in-memory SQLite database with GORM's
// TranslateError on, which replaces a driver's unique violation with the bare
// gorm.ErrDuplicatedKey before the runtime sees it.
func newTranslateErrorDB(t *testing.T) *gorm.DB {
	t.Helper()
	dsn := fmt.Sprintf("file:flywheel-translate-%d?mode=memory&cache=shared", dbSeq.Add(1))
	db, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{
		TranslateError: true,
		Logger:         logger.Default.LogMode(logger.Silent),
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		if sqlDB, derr := db.DB(); derr == nil {
			_ = sqlDB.Close()
		}
	})
	require.NoError(t, Migrate(db))
	return db
}

// TestCollisionsAreRecognizedUnderTranslateError proves every duplicate-key
// mapping recognizes the collision when a host opens its database with
// TranslateError, where it arrives as gorm.ErrDuplicatedKey rather than the
// driver's error.
func TestCollisionsAreRecognizedUnderTranslateError(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	t.Run("enqueue", func(t *testing.T) {
		t.Parallel()
		db := newTranslateErrorDB(t)
		c := NewClient(db)
		holder, err := Enqueue(ctx, c, "k", []byte(`{}`), InsertOpts{UniqueKey: "k"})
		require.NoError(t, err)
		_, err = Enqueue(ctx, c, "k", []byte(`{}`), InsertOpts{UniqueKey: "k"})
		requireHolder(t, err, holder, "k")
	})

	t.Run("forced retry", func(t *testing.T) {
		t.Parallel()
		db := newTranslateErrorDB(t)
		c := NewClient(db)
		first, err := Enqueue(ctx, c, "k", []byte(`{}`), InsertOpts{UniqueActiveKey: "subject"})
		require.NoError(t, err)
		require.NoError(t, CancelJob(ctx, db, first))
		holder, err := Enqueue(ctx, c, "k", []byte(`{}`), InsertOpts{UniqueActiveKey: "subject"})
		require.NoError(t, err)
		requireHolder(t, RetryJobWithOptions(ctx, db, first, RetryOpts{Force: true}), holder, "subject")
	})

	t.Run("insert child", func(t *testing.T) {
		t.Parallel()
		db := newTranslateErrorDB(t)
		d := NewSQLiteDriver(db)
		fu := FollowUp{Kind: "child", Args: map[string]int{"i": 1}, UniqueKey: "child-key"}
		require.NoError(t, d.InsertChild(ctx, db, fu, "parent"))
		require.ErrorIs(t, d.InsertChild(ctx, db, fu, "parent"), ErrAlreadyEnqueued)
	})

	t.Run("seed run", func(t *testing.T) {
		t.Parallel()
		db := newTranslateErrorDB(t)
		seed := RunSeed{JobID: "j", Attempt: 1, ExecutorID: "e"}
		_, err := SeedRun(ctx, db, seed)
		require.NoError(t, err)
		_, err = SeedRun(ctx, db, seed)
		require.ErrorIs(t, err, ErrRunAlreadyRecorded)
	})
}

// TestInsertCollisionWithAHolderThatFinishesBeforeTheRead drives the race the
// error's empty ExistingID exists for: the live job holding the active key
// reaches a terminal state between the insert's collision and the holder read.
// A Query callback finishes the holder just before that read runs. The read
// succeeds and finds no holder, so ExistingID is empty and Key is the active key
// — the key that freed — even when the insert also set a UniqueKey nobody holds.
func TestInsertCollisionWithAHolderThatFinishesBeforeTheRead(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		opts InsertOpts
	}{
		{"active key only", InsertOpts{UniqueActiveKey: "subject"}},
		{"both keys, only the active key held", InsertOpts{UniqueKey: "free", UniqueActiveKey: "subject"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			db := newDB(t)
			ctx := context.Background()
			c := NewClient(db)
			holder, err := Enqueue(ctx, c, "holder", []byte(`{}`), InsertOpts{UniqueActiveKey: "subject"})
			require.NoError(t, err)

			var finished atomic.Bool
			require.NoError(t, db.Callback().Query().Before("gorm:query").Register("test:finish_holder",
				func(tx *gorm.DB) {
					if slices.Contains(tx.Statement.Selects, "unique_active_key") && finished.CompareAndSwap(false, true) {
						_ = db.Exec("UPDATE jobs SET state = ? WHERE id = ?", string(StateSucceeded), holder).Error
					}
				}))

			_, err = Enqueue(ctx, c, "dup", []byte(`{}`), tc.opts)
			require.True(t, finished.Load(), "the holder finished before the read")
			requireHolder(t, err, "", "subject")

			_, err = Enqueue(ctx, c, "dup", []byte(`{}`), tc.opts)
			require.NoError(t, err, "the key freed when its holder finished, so the next insert lands")
		})
	}
}
