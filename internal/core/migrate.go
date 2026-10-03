package core

import (
	"context"
	"database/sql/driver"
	"fmt"
	"time"

	"gorm.io/gorm"
)

// Models returns the runtime's row structs so a consumer can drive schema
// generation from a single source of truth — the same structs Migrate uses.
//
// This is the table half of the host-owned install mode: a host that prefers
// versioned SQL (e.g. an Atlas/atlas-provider-gorm flow) points its loader at
// these models instead of re-declaring the columns. The runtime keeps the row
// structs unexported on purpose; Models exposes them as a stable []any without
// widening the package's typed API surface.
//
// It is not the whole install. Pair it with [InstallIndexes] for the index half:
// the indexes are not expressible as struct tags, so they are not in what a
// loader reads from these models, and four of them are correctness-bearing. See
// [Migrate] for the contract that decides between the two modes.
func Models() []any {
	return []any{
		&jobRow{}, &jobRunRow{}, &jobPeriodicRow{},
		&limiterBucketRow{}, &limiterHoldRow{},
		&jobRunFinishRow{}, &jobStatsHourlyRow{}, &jobStatsProgressRow{},
	}
}

// Migrate is the library-owned install: it brings up the runtime's tables — the
// three job tables (jobs, job_runs, job_periodics), the two limiter tables
// (limiter_buckets, limiter_holds) the DBLimiter uses, and the three stats tables
// (job_run_finishes, the finish log every finalize writes; job_stats_hourly, the
// rollup; and job_stats_progress, the hours the rollup covers) — with their NOT-NULL constraints, column defaults, and the jobs
// soft-delete column, plus the partial/unique indexes GORM AutoMigrate cannot
// express. A host in this mode calls Migrate(db) and nothing else. The limiter
// tables are additive: a host that never constructs a DBLimiter simply leaves
// them empty.
//
// # Choosing an install mode
//
// The runtime owns eight tables and there are two ways to install them. They are
// not layers. A host picks exactly one; running both means two migration
// authorities against one database.
//
//	Question                      Library-owned            Host-owned
//	----------------------------  -----------------------  -------------------------------
//	Who creates the tables?       Migrate(db)              your loader, from Models
//	Who creates the indexes?      Migrate(db)              you, from IndexSet/InstallIndexes
//	Who owns migration history?   nobody (AutoMigrate      your migration tool
//	                              is declarative)
//	Runtime runs DDL at startup?  yes, every start         no
//	Co-located host schema safe?  only if the host's       yes — the tables are in
//	                              tooling excludes the     your loader
//	                              runtime tables
//	Pick this when                the database is the      the runtime's tables share a
//	                              runtime's alone          database with an app schema
//
// The last row is the rule: a shared database means host-owned. A migration tool
// that cannot see the runtime tables will propose dropping them, and a runtime
// that runs its own DDL inside a versioned schema is a second migration
// authority with no coordination.
//
// The half a host most often misses is the indexes. Models yields tables and
// columns; it does not yield indexes, because every one of them carries a WHERE
// predicate or spans columns a GORM struct tag cannot express. Four are
// correctness-bearing (see [IndexSet]), so a host-owned schema installed without
// [InstallIndexes] is one in which idempotent enqueue silently does not work.
//
// # Using Migrate
//
// Call it against a bare SQLite or PostgreSQL database the runtime is the only
// writer of.
//
// The indexes it applies are IndexSet(db.Name()), in that order, through the
// same apply path [InstallIndexes] uses, and the storage parameters are
// StorageParameterSet(db.Name()) through the path [InstallStorageParameters]
// uses. On PostgreSQL those tune the jobs table for its update rate; on SQLite
// there are none to apply.
//
// Migrate is idempotent against an up-to-date schema: AutoMigrate is a no-op and
// every index already matches, so repeated calls do nothing. Against a database
// whose index definition has drifted from the runtime's it fails loudly by
// default — with an IndexDriftError naming the drift — rather than leaving the
// stale index in place; set MigrateOpts.Reconcile to rebuild it instead. See
// MigrateOpts.Reconcile for the lock that takes.
//
// Migrate is MigrateWithOptions(db, MigrateOpts{}).
func Migrate(db *gorm.DB) error {
	return MigrateWithOptions(db, MigrateOpts{})
}

// MigrateOpts configures MigrateWithOptions. The zero value is the library-owned
// install: the runtime brings up its own tables and indexes against a database it
// is the only writer of.
//
// Upgrading a live PostgreSQL database that running workers are writing to wants
// Concurrently and LockTimeout set — see the upgrade runbook in
// docs/INTEGRATING.md. Its fields mirror IndexOpts exactly.
type MigrateOpts struct {
	// Reconcile drops and recreates any index whose installed definition has
	// drifted from the runtime's, rather than failing with an IndexDriftError. It
	// is off by default, uniform with InstallIndexes: the rebuild takes a table-wide
	// ACCESS EXCLUSIVE lock, and a Migrate on every start that could take one
	// uninvited is a stall under load. A drifted database therefore fails Migrate
	// loudly by default — recoverable — until a host either sets this or corrects
	// the index by hand. See IndexOpts.Reconcile for the lock this takes.
	//
	// An absent index is still created and a matching one still left alone; this
	// governs only what happens to a drifted one.
	Reconcile bool

	// Concurrently creates absent indexes with CREATE INDEX CONCURRENTLY on
	// PostgreSQL, so adding an index to a loaded table never blocks its writers.
	// See IndexOpts.Concurrently.
	Concurrently bool

	// LockTimeout, when positive, bounds how long each schema statement waits for
	// its table lock on PostgreSQL. A statement that cannot get its lock fails the
	// Migrate instead of queueing every later query on the table behind it; retry
	// it. Zero waits indefinitely. See IndexOpts.LockTimeout.
	//
	// Each step commits on its own: every table's AutoMigrate, every storage
	// parameter statement, and every plain index build runs in its own short
	// transaction under SET LOCAL lock_timeout. That is what keeps the timeout from
	// lengthening the very stall it guards against — a lock is held only for the
	// statement that needed it, so the ACCESS EXCLUSIVE lock an ADD COLUMN takes on
	// job_runs is released the moment job_runs is done, not held while the rest of
	// the schema is migrated. A Migrate that fails part-way therefore leaves the
	// steps before the failure applied; every step is additive and idempotent, so
	// the retry completes it.
	LockTimeout time.Duration
}

// MigrateWithOptions installs the schema per opts: AutoMigrate over Models,
// StorageParameterSet's statements, then IndexSet's statements in order.
//
// A host whose migration tool already created the tables wants InstallIndexes
// and InstallStorageParameters instead: together they apply everything Migrate
// does except the tables themselves.
//
// The index step reconciles by definition, not by name: it creates an absent
// index, leaves a matching one alone, and — with opts.Reconcile unset — fails
// with an IndexDriftError on one whose definition has drifted rather than
// silently keeping the stale index. Set opts.Reconcile to rebuild it in place.
//
// # Upgrading
//
// Against a database an older release installed, Migrate is the upgrade: the
// columns and tables a release adds are all additive — defaulted or nullable — so
// AutoMigrate adds them without rewriting a row (on PostgreSQL 11+ a constant
// default is a catalog change only), and binaries still running the older
// release keep working against the result. Migrate, then deploy.
//
// # Concurrent callers
//
// On PostgreSQL the whole migration runs on one connection holding a
// session-level advisory lock scoped to the target schema (see
// withMigrationLock), so two processes migrating the same schema at once — two
// `flywheel serve` instances starting together — run one after the other: the
// second finds the schema current and does nothing. Without it both would race
// the same CREATE TABLE and one would fail with a duplicate-object error. SQLite
// has a single writer and takes no lock.
func MigrateWithOptions(db *gorm.DB, opts MigrateOpts) error {
	if db == nil {
		return fmt.Errorf("flywheel: Migrate: db is nil")
	}
	ctx := context.Background()
	if err := withMigrationLock(ctx, db, func(conn *gorm.DB) error {
		return migrateSchema(ctx, conn, opts)
	}); err != nil {
		return fmt.Errorf("flywheel: Migrate: %w", err)
	}
	return nil
}

// migrateSchema applies the tables, then the storage parameters, then the
// indexes. The order is load-bearing rather than tidy: fillfactor governs only
// pages written after it is set, so applying it after rows exist leaves every
// existing page at the old target. On SQLite the storage step is a no-op.
//
// Each table's AutoMigrate and each storage statement runs on its own, so a
// LockTimeout scopes one statement's lock wait and that statement's locks are
// released when it commits — see MigrateOpts.LockTimeout.
//
// A table that already carries every column its model declares is not passed to
// AutoMigrate at all. Every schema change the runtime makes is additive, so such
// a table has nothing to gain; and GORM's PostgreSQL migrator compares a column
// default it read back unquoted ({}) against the tag's quoted literal ('{}'), so
// on every run it re-issues ALTER COLUMN ... SET DEFAULT for each jsonb column —
// an ACCESS EXCLUSIVE lock on jobs at every `flywheel serve` start, which queues
// behind any long transaction on the table and stalls every claim queued behind
// it. With the skip, a Migrate of an up-to-date schema runs no DDL at all.
func migrateSchema(ctx context.Context, db *gorm.DB, opts MigrateOpts) error {
	for _, model := range Models() {
		gaps, err := inspectSchema(ctx, db, model)
		if err != nil {
			return fmt.Errorf("inspect %T: %w", model, err)
		}
		if len(gaps) == 0 {
			continue
		}
		if err := withLockTimeout(db, opts.LockTimeout, func(tx *gorm.DB) error {
			return tx.AutoMigrate(model)
		}); err != nil {
			return fmt.Errorf("automigrate %T: %w", model, err)
		}
	}
	if err := applyStorageParameters(ctx, db, opts.LockTimeout); err != nil {
		return err
	}
	return applyIndexes(ctx, db, IndexOpts(opts))
}

// migrationLockClass is the first key of the advisory lock that serializes
// migrations of one schema on PostgreSQL: "flym" as an int32. It is distinct
// from statsLockClass ("flyw"), which serializes writers of one stats rollup
// hour, so the two can never contend; the second key is the target schema's
// name hashed with hashtext, so migrations of different schemas in one database
// do not wait on each other.
const migrationLockClass = 0x666c796d

// migrationLockPoll is how long a migration waiting on another one sleeps between
// attempts to take the lock.
const migrationLockPoll = 100 * time.Millisecond

// withMigrationLock runs fn on one pinned connection while holding the
// migration advisory lock for db's current schema, on PostgreSQL. SQLite, and a
// db that is already a transaction (its caller owns the serialization), run fn
// against db directly.
//
// Three details make it safe:
//
//   - fn receives the pinned connection and must run every statement through it.
//     Taking a second pooled connection while the first is held would deadlock a
//     pool capped at one connection.
//   - The lock is taken with pg_try_advisory_lock in a poll loop, never with the
//     blocking pg_advisory_lock. A blocked pg_advisory_lock is a statement in
//     flight holding a snapshot, and CREATE INDEX CONCURRENTLY in the migration
//     that owns the lock waits for every older snapshot to end — so the waiter
//     and the owner would wait on each other until PostgreSQL broke the deadlock
//     by failing one. Between attempts the waiter holds no snapshot at all.
//   - The lock is session-scoped, so it outlives fn's own transactions. It is
//     released explicitly, and then the connection is discarded rather than
//     returned to the pool, which ends the session — so even a failed release
//     cannot leave a pooled connection holding the lock and blocking every later
//     migration of the schema. Discarding it matters for a second reason: the
//     driver caches each statement's plan per connection, and a `SELECT *` this
//     connection prepared before an ALTER TABLE added columns would fail the next
//     time it ran there ("cached plan must not change result type").
func withMigrationLock(ctx context.Context, db *gorm.DB, fn func(conn *gorm.DB) error) (err error) {
	if db.Name() != "postgres" {
		return fn(db)
	}
	if _, inTx := db.Statement.ConnPool.(gorm.TxCommitter); inTx {
		return fn(db)
	}
	sqlDB, err := db.DB()
	if err != nil {
		return fmt.Errorf("migration lock: %w", err)
	}
	conn, err := sqlDB.Conn(ctx)
	if err != nil {
		return fmt.Errorf("migration lock: pin connection: %w", err)
	}
	defer func() {
		// Discard, never pool: see the doc comment. Returning driver.ErrBadConn
		// from Raw is database/sql's way to close a connection instead of reusing it.
		_ = conn.Raw(func(any) error { return driver.ErrBadConn })
		_ = conn.Close()
	}()

	// A fresh session whose statements all go to the pinned connection. NewDB
	// gives every call from it a clean statement, and Context clones the
	// statement first, so the caller's handle is never touched.
	pinned := db.Session(&gorm.Session{NewDB: true, Context: ctx})
	pinned.Statement.ConnPool = conn

	if err := acquireMigrationLock(ctx, pinned); err != nil {
		return err
	}
	defer func() {
		// Release before the connection is discarded, so the next migration can
		// take the lock at once rather than when the server notices the session
		// ended. A failed release is no leak — the discard ends the session — so
		// it fails the migration only when nothing else did.
		unlockErr := pinned.Exec(`SELECT pg_advisory_unlock(?, hashtext(COALESCE(current_schema(), '')))`,
			migrationLockClass).Error
		if err == nil && unlockErr != nil {
			err = fmt.Errorf("migration lock: release: %w", unlockErr)
		}
	}()
	return fn(pinned)
}

// acquireMigrationLock polls pg_try_advisory_lock until it takes the migration
// lock or ctx ends. See withMigrationLock for why it never blocks in the server.
func acquireMigrationLock(ctx context.Context, conn *gorm.DB) error {
	for {
		var got bool
		if err := conn.Raw(`SELECT pg_try_advisory_lock(?, hashtext(COALESCE(current_schema(), '')))`,
			migrationLockClass).Scan(&got).Error; err != nil {
			return fmt.Errorf("migration lock: acquire: %w", err)
		}
		if got {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("migration lock: acquire: %w", ctx.Err())
		case <-time.After(migrationLockPoll):
		}
	}
}

// withLockTimeout runs fn against db directly, or — on PostgreSQL with a positive
// timeout — inside a transaction that scopes the timeout with SET LOCAL, so it
// can never leak onto a pooled connection another caller reuses.
func withLockTimeout(db *gorm.DB, timeout time.Duration, fn func(tx *gorm.DB) error) error {
	if timeout <= 0 || db.Name() != "postgres" {
		return fn(db)
	}
	return db.Transaction(func(tx *gorm.DB) error {
		if err := setLocalLockTimeout(tx, timeout); err != nil {
			return err
		}
		return fn(tx)
	})
}
