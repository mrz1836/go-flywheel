//go:build integration

package core

import (
	"context"
	"database/sql"
	"regexp"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

// TestInsertCollisionNamesTheHolderPostgres names every kind of holder on
// PostgreSQL, where the soft-deleted holder proves the read is Unscoped against
// the real partial indexes.
func TestInsertCollisionNamesTheHolderPostgres(t *testing.T) {
	t.Parallel()
	assertCollisionNamesTheHolder(t, NewPostgresIsolatedDB(t))
}

// TestInsertTxCollisionLeavesTheTransactionUsablePostgres is the dialect where
// the guarantee bites: a failed INSERT aborts a PostgreSQL transaction, so a
// collision written as a plain INSERT on InsertOpts.Tx leaves every later
// statement failing with 25P02 and the caller's commit rolled back.
func TestInsertTxCollisionLeavesTheTransactionUsablePostgres(t *testing.T) {
	t.Parallel()
	assertTxCollisionLeavesTheTransactionUsable(t, NewPostgresIsolatedDB(t))
}

// TestInsertTxCollisionNamesASiblingInTheSameTransactionPostgres proves the
// holder read runs on the caller's transaction: the sibling is uncommitted, so
// no other connection could see it.
func TestInsertTxCollisionNamesASiblingInTheSameTransactionPostgres(t *testing.T) {
	t.Parallel()
	assertTxCollisionNamesASibling(t, NewPostgresIsolatedDB(t))
}

// TestInsertUniqueKeyRaceNamesTheWinnerPostgres races 50 inserts of one key and
// proves every loser names the winner. It is deterministic under READ COMMITTED:
// a loser's insert waits on the winner's transaction, and only fails (or, on a
// caller's transaction, skips its row) once the winner has committed, so the
// holder read that follows always sees it.
//
// The cases run one after another on a pool capped at 20 connections, so the race
// stays within a shared server's connection limit while 50 racers still contend.
func TestInsertUniqueKeyRaceNamesTheWinnerPostgres(t *testing.T) {
	t.Parallel()
	const racers = 50

	for _, tc := range []struct {
		name string
		opts InsertOpts
		onTx bool
		key  string
	}{
		{name: "unique key", opts: InsertOpts{UniqueKey: "race"}, key: "race"},
		{name: "unique active key", opts: InsertOpts{UniqueActiveKey: "race"}, key: "race"},
		{name: "unique key, each racer on its own transaction", opts: InsertOpts{UniqueKey: "race"}, onTx: true, key: "race"},
		{name: "unique active key, each racer on its own transaction", opts: InsertOpts{UniqueActiveKey: "race"}, onTx: true, key: "race"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db := NewPostgresIsolatedDB(t)
			sqlDB, err := db.DB()
			require.NoError(t, err)
			sqlDB.SetMaxOpenConns(20)
			c := NewClient(db)
			ctx := context.Background()

			ids := make([]string, racers)
			errs := make([]error, racers)
			start := make(chan struct{})
			var wg sync.WaitGroup
			for i := range racers {
				wg.Add(1)
				go func() {
					defer wg.Done()
					<-start
					if !tc.onTx {
						ids[i], errs[i] = Enqueue(ctx, c, "race", []byte(`{}`), tc.opts)
						return
					}
					txErr := db.Transaction(func(tx *gorm.DB) error {
						opts := tc.opts
						opts.Tx = tx
						ids[i], errs[i] = Enqueue(ctx, c, "race", []byte(`{}`), opts)
						return nil
					})
					if txErr != nil {
						errs[i] = txErr
					}
				}()
			}
			close(start)
			wg.Wait()

			var winner string
			for i := range racers {
				if errs[i] == nil {
					require.Empty(t, winner, "exactly one racer lands")
					winner = ids[i]
				}
			}
			require.NotEmpty(t, winner, "one racer lands")

			for i := range racers {
				if ids[i] == winner {
					continue
				}
				var dup *AlreadyEnqueuedError
				require.ErrorAs(t, errs[i], &dup, "racer %d", i)
				assert.Equal(t, winner, dup.ExistingID, "racer %d names the winner", i)
				assert.Equal(t, tc.key, dup.Key)
			}
		})
	}
}

// postgresGenericPlan returns the generic plan PostgreSQL builds for query — the
// plan a prepared statement is cached with and reused under for any parameter
// values, so nothing in it can depend on what was bound. vars are the statement's
// string parameters, passed to EXECUTE as literals. Sequential scans are switched
// off so that, on a fixture this small, a usable index is always the one chosen:
// an index the plan does not name is an index the planner could not prove usable.
func postgresGenericPlan(t *testing.T, db *gorm.DB, query string, vars []any) string {
	t.Helper()
	args := make([]string, len(vars))
	for i, v := range vars {
		s, ok := v.(string)
		require.True(t, ok, "parameter %d is a %T", i, v)
		args[i] = "'" + strings.ReplaceAll(s, "'", "''") + "'"
	}
	var plan strings.Builder
	require.NoError(t, db.Transaction(func(tx *gorm.DB) error {
		for _, stmt := range []string{
			"SET LOCAL plan_cache_mode = force_generic_plan",
			"SET LOCAL enable_seqscan = off",
			"PREPARE holder_read AS " + query,
		} {
			if err := tx.Exec(stmt).Error; err != nil {
				return err
			}
		}
		rows, err := tx.Raw("EXPLAIN (COSTS OFF) EXECUTE holder_read(" + strings.Join(args, ", ") + ")").Rows()
		if err != nil {
			return err
		}
		defer func() { _ = rows.Close() }()
		for rows.Next() {
			var line string
			if err := rows.Scan(&line); err != nil {
				return err
			}
			plan.WriteString(line + "\n")
		}
		if err := rows.Err(); err != nil {
			return err
		}
		return tx.Exec("DEALLOCATE holder_read").Error
	}))
	return plan.String()
}

// TestHolderReadReachesTheUniqueIndexesPostgres pins the holder read's generic
// plan: PostgreSQL can use jobs_unique_active_key only when it can prove the
// query's state condition implies the index's, which a bound state list never
// lets a generic plan do.
func TestHolderReadReachesTheUniqueIndexesPostgres(t *testing.T) {
	t.Parallel()
	db := NewPostgresIsolatedDB(t)
	seedHolderPlanFixture(t, db)

	for _, tc := range holderPlanCases() {
		query, vars := holderReadStatement(t, db, tc.keys, tc.active)
		plan := postgresGenericPlan(t, db, query, vars)
		for _, idx := range tc.indexes {
			// "Index Scan using <idx> on jobs", or "Bitmap Index Scan on <idx>"
			// under a BitmapOr when both halves are present.
			scan := regexp.MustCompile(`Index (Only )?Scan (using|on) ` + idx + `\b`)
			assert.Regexp(t, scan, plan, "%s: the read uses %s\n%s", tc.name, idx, plan)
		}
		assert.NotContains(t, plan, "Seq Scan", "%s: no scan of jobs\n%s", tc.name, plan)
	}
}

// TestInsertTxCollisionUnderRepeatableReadPostgres pins what a collision on a
// REPEATABLE READ transaction does. When the holder committed after the
// transaction took its snapshot, ON CONFLICT DO NOTHING cannot skip a row its
// snapshot does not see, and PostgreSQL raises a serialization failure (SQLSTATE
// 40001) — the error such a caller already retries. The retried transaction's
// snapshot sees the holder, so it gets the collision, named, and commits.
func TestInsertTxCollisionUnderRepeatableReadPostgres(t *testing.T) {
	t.Parallel()
	repeatableRead := &sql.TxOptions{Isolation: sql.LevelRepeatableRead}

	for _, tc := range keyCases() {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			db := NewPostgresIsolatedDB(t)
			c := NewClient(db)
			ctx := context.Background()
			opts := tc.opts("rr")

			// The first attempt takes its snapshot, then the holder commits.
			tx := db.Begin(repeatableRead)
			require.NoError(t, tx.Error)
			var n int64
			require.NoError(t, tx.Model(&jobRow{}).Count(&n).Error)
			holder, err := Enqueue(ctx, c, "holder", []byte(`{}`), opts)
			require.NoError(t, err)

			txOpts := opts
			txOpts.Tx = tx
			_, err = Enqueue(ctx, c, "dup", []byte(`{}`), txOpts)
			require.ErrorContains(t, err, "(SQLSTATE 40001)", "a serialization failure, not a collision")
			assert.NotErrorIs(t, err, ErrAlreadyEnqueued)
			require.NoError(t, tx.Rollback().Error)

			// The retry sees the holder.
			var collision error
			require.NoError(t, db.Transaction(func(tx *gorm.DB) error {
				txOpts.Tx = tx
				_, collision = Enqueue(ctx, c, "dup", []byte(`{}`), txOpts)
				_, e := Enqueue(ctx, c, "after", []byte(`{}`), InsertOpts{Tx: tx})
				return e
			}, repeatableRead), "the retried transaction commits")
			requireHolder(t, collision, holder, tc.key("rr"))
		})
	}
}
