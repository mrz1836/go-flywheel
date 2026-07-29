package flywheel

import (
	"context"
	"encoding/json"
	"fmt"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// jobRowColumns is the number of columns jobRow binds on an INSERT — the
// bind-parameter cost of one row, and therefore the divisor that turns a
// dialect's parameter ceiling into a rows-per-statement ceiling. It is an upper
// bound (a NULL optional column may not be bound at all), so a chunk sized from
// it can never exceed the driver's real parameter cap.
const jobRowColumns = 22

// Bulk-enqueue chunk sizing. The defaults sit at roughly a third of each
// dialect's rows-per-statement ceiling, so adding a jobRow column cannot silently
// push a default-configured caller over the driver's bind-parameter limit. The
// maxima are the ceiling itself: a requested ChunkSize above one is clamped down
// to it rather than rejected.
const (
	// defaultPostgresChunk is the default rows-per-INSERT on PostgreSQL, whose wire
	// protocol caps a statement at 65535 bind parameters (65535/22 ≈ 2978 rows).
	defaultPostgresChunk = 1000
	// defaultSQLiteChunk is the default rows-per-INSERT on SQLite, whose
	// SQLITE_MAX_VARIABLE_NUMBER is 32766 (32766/22 ≈ 1489 rows).
	defaultSQLiteChunk = 500

	// maxPostgresChunk and maxSQLiteChunk are the hard per-statement ceilings a
	// requested ChunkSize is clamped down to — the bind-parameter limit divided by
	// jobRow's column count.
	maxPostgresChunk = 65535 / jobRowColumns
	maxSQLiteChunk   = 32766 / jobRowColumns
)

// BatchItem is one job in a bulk enqueue. Kind names the worker; Args is the
// pre-marshaled payload, as Enqueue takes it; Opts carries this row's per-job
// options. Opts.Tx is ignored here — the transaction is chosen once for the whole
// batch by BatchOpts.
type BatchItem struct {
	Kind string
	Args []byte
	Opts InsertOpts
}

// BatchOpts configures one bulk enqueue.
type BatchOpts struct {
	// ChunkSize is the number of rows per INSERT statement. Zero selects the
	// dialect default, chosen so a chunk stays well inside the driver's
	// bind-parameter ceiling. A value above the dialect maximum is clamped down,
	// not rejected: the caller asked for throughput, not for a specific statement
	// shape.
	ChunkSize int
	// Tx, when set, writes every chunk on the caller's transaction (outbox). The
	// batch opens no transaction of its own, so the caller's commit is the only
	// durability boundary — all rows land, or none do.
	Tx *gorm.DB
	// SkipDuplicates, when true, drops rows whose unique key collides with an
	// existing job without reporting them: BatchResult.Skipped stays zero. The
	// default (false) counts each collision in BatchResult.Skipped. Either way the
	// row is skipped, its IDs entry is empty, and the collision is never an error —
	// the flag governs only whether the skip is reported.
	SkipDuplicates bool
}

// BatchResult reports what a bulk enqueue did.
type BatchResult struct {
	// IDs are the created job ids in input order. A skipped row's entry is empty.
	IDs []string
	// Inserted and Skipped partition the input: Inserted counts rows that landed,
	// Skipped counts rows dropped by a unique-key collision (and only when
	// BatchOpts.SkipDuplicates is false).
	Inserted, Skipped int
	// Chunks is the number of INSERT statements that landed rows. On a mid-batch
	// failure it is the count of chunks committed before the failing one, whose
	// index the returned error names.
	Chunks int
}

// InsertMany enqueues many jobs in bounded chunks. It is the bulk counterpart to
// Enqueue: same row defaults, same unique-key semantics, same ErrAlreadyEnqueued
// meaning — applied per row rather than to the call.
//
// A unique-key collision is never fatal. The colliding row is skipped, its entry
// in IDs is left empty, and the batch continues; with BatchOpts.SkipDuplicates
// false (the default) the skip is counted in BatchResult.Skipped. This matches
// the single-row contract, where a collision is a successful no-op the caller
// distinguishes by the returned error.
//
// When opts.Tx is set every chunk runs on that transaction and the batch opens
// none of its own, so the caller's commit is the only durability boundary — all
// rows land or none do, and a rolled-back caller transaction leaves no orphaned
// job. Without opts.Tx each chunk commits independently, so a mid-batch failure
// leaves the chunks before it committed; the returned BatchResult reports exactly
// how many rows landed and the error wraps the failing chunk's index. A caller
// that needs all-or-nothing supplies opts.Tx.
func InsertMany(ctx context.Context, c *Client, items []BatchItem, opts BatchOpts) (BatchResult, error) {
	if len(items) == 0 {
		return BatchResult{}, nil
	}

	// The handle picks the dialect and, when opts.Tx is set, the transaction every
	// chunk runs on. When Tx is set the batch opens NO transaction of its own:
	// committing a chunk on c.writeDB while the caller's domain transaction is
	// still open would orphan a job row after the caller rolls back.
	handle := c.writeDB
	if opts.Tx != nil {
		handle = opts.Tx
	}
	chunkSize := chunkSizeFor(handle.Name(), opts.ChunkSize)

	rows := make([]jobRow, len(items))
	ids := make([]string, len(items))
	for i := range items {
		rows[i] = buildRow(ctx, items[i].Kind, items[i].Args, items[i].Opts)
		ids[i] = rows[i].ID
	}

	res := BatchResult{IDs: ids}
	chunkIdx := 0
	for start := 0; start < len(rows); start += chunkSize {
		end := min(start+chunkSize, len(rows))
		landed, err := c.insertChunk(ctx, handle, opts.Tx != nil, rows[start:end])
		if err != nil {
			// Partial progress: chunks before this one are committed (or, under
			// opts.Tx, stand or fall with the caller's commit). Report what landed
			// and name the failing chunk.
			return res, fmt.Errorf("jobs: insert many: chunk %d: %w", chunkIdx, err)
		}
		res.Chunks++
		for i := start; i < end; i++ {
			if _, ok := landed[ids[i]]; ok {
				res.Inserted++
				continue
			}
			// The row collided with an existing unique key and did not land. Clear
			// its id; count it as skipped unless the caller opted to drop silently.
			ids[i] = ""
			if !opts.SkipDuplicates {
				res.Skipped++
			}
		}
		chunkIdx++
	}
	return res, nil
}

// InsertManyTyped is the generic form of InsertMany: it marshals each args value
// and reads the kind from it, exactly as Insert does for one job, then applies
// opts to every row and delegates to InsertMany.
func InsertManyTyped[A Args](
	ctx context.Context, c *Client, args []A, opts InsertOpts, batch BatchOpts,
) (BatchResult, error) {
	items := make([]BatchItem, len(args))
	for i := range args {
		namer, ok := any(args[i]).(kindNamer)
		if !ok {
			return BatchResult{}, ErrMissingKind
		}
		payload, err := json.Marshal(args[i])
		if err != nil {
			return BatchResult{}, fmt.Errorf("jobs: marshal args: %w", err)
		}
		items[i] = BatchItem{Kind: namer.Kind(), Args: payload, Opts: opts}
	}
	return InsertMany(ctx, c, items, batch)
}

// insertChunk runs one chunk through the shared conflict-insert atom under the
// batch's transaction policy.
//
// When onTx is true the chunk runs directly on the caller's handle and opens NO
// transaction of its own — the structural guarantee that a batch sharing a
// caller's transaction can never commit a row on a separate connection and orphan
// it. When onTx is false each chunk is wrapped in its own transaction on the
// write handle, so a mid-batch failure leaves prior chunks committed.
func (c *Client) insertChunk(
	ctx context.Context, handle *gorm.DB, onTx bool, chunk []jobRow,
) (map[string]struct{}, error) {
	if onTx {
		return conflictInsertChunk(ctx, handle, chunk)
	}
	var landed map[string]struct{}
	err := handle.Transaction(func(tx *gorm.DB) error {
		var chunkErr error
		landed, chunkErr = conflictInsertChunk(ctx, tx, chunk)
		return chunkErr
	})
	if err != nil {
		return nil, err
	}
	return landed, nil
}

// conflictInsertChunk inserts one chunk of pre-built job rows on db with
// ON CONFLICT DO NOTHING, then reports which rows actually landed. It is the one
// batching primitive both InsertMany and the finalize follow-up fan-out share;
// the chunk-iteration and transaction policy wrap it per caller.
//
// The insert is targetless (clause.OnConflict{DoNothing: true}) because both
// unique indexes are partial — jobs_unique_key and jobs_unique_active_key each
// carry a WHERE predicate — and a bare-column conflict target cannot name a
// partial index's arbiter. A targetless DO NOTHING covers any unique violation on
// the table, which is exactly the per-row "skip a duplicate" semantics wanted,
// and it renders on both PostgreSQL and SQLite (≥ 3.35).
//
// A row is only ever skipped by the conflict when it carries a unique key: its id
// is a freshly minted value that cannot pre-exist, so the PK never collides. When
// no row in the chunk carries a unique_key or unique_active_key, every row landed
// and the read-back is skipped entirely — which is the whole cost of a bulk
// enqueue at scale, because the read-back's id-IN over a mid-load table dominates
// the insert by an order of magnitude while the insert itself is cheap.
//
// When the chunk does carry unique keys, landing is read back rather than counted
// from RowsAffected, which is unusable here: jobRow's metadata and tags are
// datatypes.JSON columns with a default: tag, so GORM auto-appends
// RETURNING metadata, tags and scans it back — a path that skips every row whose
// returning fields are already non-zero, and buildRow always sets tags.
// RowsAffected therefore equals len(rows) whether a row conflicted or not. The
// same-handle read-back sees rows still uncommitted in the caller's transaction,
// and a skipped row — whose app-minted id was never inserted — is simply absent.
func conflictInsertChunk(ctx context.Context, db *gorm.DB, rows []jobRow) (map[string]struct{}, error) {
	if len(rows) == 0 {
		return map[string]struct{}{}, nil
	}
	if err := db.WithContext(ctx).
		Clauses(clause.OnConflict{DoNothing: true}).
		Create(&rows).Error; err != nil {
		return nil, err
	}

	landed := make(map[string]struct{}, len(rows))
	if !chunkHasUniqueKey(rows) {
		for i := range rows {
			landed[rows[i].ID] = struct{}{}
		}
		return landed, nil
	}

	ids := make([]string, len(rows))
	for i := range rows {
		ids[i] = rows[i].ID
	}
	var got []string
	if err := db.WithContext(ctx).Model(&jobRow{}).
		Where("id IN ?", ids).Pluck("id", &got).Error; err != nil {
		return nil, fmt.Errorf("jobs: batch: read back landed ids: %w", err)
	}
	for _, id := range got {
		landed[id] = struct{}{}
	}
	return landed, nil
}

// chunkHasUniqueKey reports whether any row in the chunk carries a unique key of
// either kind — the only rows a targetless ON CONFLICT DO NOTHING can skip, and so
// the only reason a chunk needs its landings read back.
func chunkHasUniqueKey(rows []jobRow) bool {
	for i := range rows {
		if rows[i].UniqueKey != nil || rows[i].UniqueActiveKey != nil {
			return true
		}
	}
	return false
}

// chunkSizeFor resolves the rows-per-statement for a bulk insert on the given
// dialect: a non-positive request selects the dialect default, and a request
// above the dialect maximum is clamped down to it (documented, not errored). An
// unrecognized dialect gets the smaller SQLite bounds, which are safe on any
// backend the runtime supports.
func chunkSizeFor(dialect string, requested int) int {
	def, hardMax := defaultSQLiteChunk, maxSQLiteChunk
	if dialect == "postgres" {
		def, hardMax = defaultPostgresChunk, maxPostgresChunk
	}
	switch {
	case requested <= 0:
		return def
	case requested > hardMax:
		return hardMax
	default:
		return requested
	}
}
