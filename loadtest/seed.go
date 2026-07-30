//go:build loadtest

package loadtest

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math/rand/v2"
	"time"

	flywheel "github.com/mrz1836/go-flywheel"
	"gorm.io/datatypes"
	"gorm.io/gorm"
)

// bulkBatch is how many rows one CreateInBatches statement carries.
//
// PostgreSQL's wire protocol caps a statement at 65535 bound parameters, and a
// jobs row binds 13 of them, so anything over ~5000 rows per batch fails
// outright. A thousand keeps a wide margin and is past the point where larger
// batches stop helping.
const bulkBatch = 1000

// seedEpoch is the timestamp the bulk path's minted ids and scheduled_at values
// are anchored to.
//
// It is a constant rather than time.Now() because everything the bulk path
// writes must derive from the config: a run seeded from the wall clock produces
// a different byte sequence every time, and then "two runs with equal Config
// produce byte-identical workloads" is not a property anyone can check.
var seedEpoch = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC) //nolint:gochecknoglobals // fixed reproducibility anchor

// bulkJobRow is the harness's own mapping of the jobs table, used only by the
// bulk seed path.
//
// The runtime keeps its row structs unexported, so a bulk insert cannot go
// through them. This is a deliberate second mapping of the same table, and the
// hazard it creates — drifting from the runtime's row shape as columns are
// added — is met head-on by TestSeedBulkMatchesEnqueueRowShape, which inserts
// one row down each path and diffs every column the database reports.
//
// It also bypasses jobRow.BeforeCreate, which is the point: BeforeCreate mints
// an id and applies the producer defaults, and the bulk path needs to control
// both. The same test is what proves the bypass reproduces the hook rather than
// skipping it.
type bulkJobRow struct {
	ID            string         `gorm:"column:id;primaryKey"`
	CreatedAt     time.Time      `gorm:"column:created_at"`
	UpdatedAt     time.Time      `gorm:"column:updated_at"`
	Metadata      datatypes.JSON `gorm:"column:metadata"`
	Kind          string         `gorm:"column:kind"`
	Queue         string         `gorm:"column:queue"`
	Args          datatypes.JSON `gorm:"column:args"`
	Priority      int            `gorm:"column:priority"`
	State         string         `gorm:"column:state"`
	Attempt       int            `gorm:"column:attempt"`
	MaxAttempts   int            `gorm:"column:max_attempts"`
	ScheduledAt   time.Time      `gorm:"column:scheduled_at"`
	ExecutorClass string         `gorm:"column:executor_class"`
	ParentJobID   *string        `gorm:"column:parent_job_id"`
	Tags          datatypes.JSON `gorm:"column:tags"`
}

// TableName binds bulkJobRow to the jobs table.
func (bulkJobRow) TableName() string { return "jobs" }

// seedAPI inserts the workload one row at a time through the producer API.
//
// It is the seed path for the mixes whose measurement *is* the insert rate. Its
// row ids come from the runtime's own models.NewID — random-bit UUIDv7, with no
// injection seam — so this path's ids are not reproducible across runs even
// though its workload is. That is a property of the code under measurement, and
// replacing it with something reproducible would mean measuring something other
// than what a host runs.
func seedAPI(ctx context.Context, db *gorm.DB, cfg Config, specs []jobSpec, onInsert func()) error {
	client := flywheel.NewClient(db)

	for i, spec := range specs {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("loadtest: seed interrupted after %d jobs: %w", i, err)
		}
		payload, err := marshalArgs(specToArgs(spec, cfg.Mix))
		if err != nil {
			return err
		}
		if _, err := flywheel.Enqueue(ctx, client, loadKind, payload, flywheel.InsertOpts{
			Queue:         cfg.Queue,
			Priority:      spec.Priority,
			ExecutorClass: flywheel.ExecutorClass(cfg.ExecutorClass),
		}); err != nil {
			return fmt.Errorf("loadtest: seed job %d: %w", i, err)
		}
		onInsert()
	}
	return nil
}

// seedInsertMany inserts the workload through the real bulk producer API,
// flywheel.InsertMany, in chunks of chunkSize.
//
// It is the seed path whose measurement is the batch-enqueue rate — the
// counterpart to seedAPI's single-row rate — so it goes through the exported API
// a host calls, not the harness's own reduced bulk struct (seedBulk). That is the
// point of the batched enqueue benchmark: it times the runtime's own producer
// path, chunk boundaries, ON CONFLICT clause and SELECT-back included, at each
// candidate chunk size.
func seedInsertMany(
	ctx context.Context, db *gorm.DB, cfg Config, specs []jobSpec, chunkSize int, onInsert func(int),
) error {
	client := flywheel.NewClient(db)

	items := make([]flywheel.BatchItem, len(specs))
	for i, spec := range specs {
		payload, err := marshalArgs(specToArgs(spec, cfg.Mix))
		if err != nil {
			return err
		}
		items[i] = flywheel.BatchItem{
			Kind: loadKind,
			Args: payload,
			Opts: flywheel.InsertOpts{
				Queue:         cfg.Queue,
				Priority:      spec.Priority,
				ExecutorClass: flywheel.ExecutorClass(cfg.ExecutorClass),
			},
		}
	}

	res, err := flywheel.InsertMany(ctx, client, items, flywheel.BatchOpts{ChunkSize: chunkSize})
	if err != nil {
		return fmt.Errorf("loadtest: batch seed: %w", err)
	}
	onInsert(res.Inserted)
	return nil
}

// seedTerminal writes count already-finalized jobs, each with a finished
// job_runs row, backdated by age.
//
// It exists so a retention pass has something to delete. The ordinary bulk path
// writes state 'available' with no finalized_at and no job_runs at all, so a
// retention sweep against a freshly seeded run removes zero rows however long
// it runs — which would look like retention working against an empty backlog
// rather than like a measurement of nothing.
//
// The rows are written server-side from generate_series rather than through the
// bulk path: this is fixture, not workload, it is never claimed, and its insert
// rate is not part of any number the run reports.
func seedTerminal(ctx context.Context, db *gorm.DB, cfg Config, count int, age time.Duration) error {
	if count <= 0 {
		return nil
	}
	finalized := time.Now().UTC().Add(-age)

	if err := db.WithContext(ctx).Exec(
		`
		INSERT INTO jobs (id, created_at, updated_at, metadata, kind, queue, args, priority,
		                  state, attempt, max_attempts, scheduled_at, finalized_at,
		                  executor_class, tags)
		SELECT 'lt-terminal-' || g, ?, ?, '{}'::jsonb, ?, ?, '{}'::jsonb, 100,
		       'succeeded', 1, 25, ?, ?, ?, '[]'::jsonb
		FROM generate_series(0, ?) AS g`,
		finalized, finalized, loadKind, cfg.Queue, finalized, finalized, cfg.ExecutorClass, count-1,
	).Error; err != nil {
		return fmt.Errorf("loadtest: seed terminal jobs: %w", err)
	}

	if err := db.WithContext(ctx).Exec(
		`
		INSERT INTO job_runs (id, job_id, attempt, executor_class, executor_id, started_at,
		                      finished_at, outcome, enqueued_children, created_at)
		SELECT 'lt-trun-' || g, 'lt-terminal-' || g, 1, ?, 'seed', ?, ?, 'success', 0, ?
		FROM generate_series(0, ?) AS g`,
		cfg.ExecutorClass, finalized, finalized, finalized, count-1,
	).Error; err != nil {
		return fmt.Errorf("loadtest: seed terminal runs: %w", err)
	}
	return nil
}

// seedBulk inserts the workload in batches.
//
// It is setup, not measurement: for a drain run the number that matters is the
// claim and finalize rate against a full queue, and 100k single-row inserts
// would add 30–100 seconds of untimed overhead to every benchmark iteration.
//
// Its ids are harness-minted, deterministic, and monotone. Monotone matters
// beyond reproducibility: production ids are UUIDv7, which are time-ordered, so
// every insert lands at the right-most leaf of the primary-key btree. Random ids
// would scatter inserts across the whole index and produce page-split behavior
// no production database sees, which would corrupt every storage number in the
// report.
func seedBulk(ctx context.Context, db *gorm.DB, cfg Config, specs []jobSpec, parentID string, onInsert func(int)) error {
	return seedBulkFrom(ctx, db, cfg, specs, 0, parentID, onInsert)
}

// maxAttemptsFor resolves the seeded jobs' retry budget: the configured override
// when set, otherwise the runtime's own default of 25 — the value the bulk path
// wrote before MaxAttempts was configurable.
func maxAttemptsFor(cfg Config) int {
	if cfg.MaxAttempts > 0 {
		return cfg.MaxAttempts
	}
	return 25
}

// seedBulkFrom is seedBulk with an explicit generation offset, so a run that
// seeds more than once does not mint the same ids twice.
//
// The offset shifts both the id timestamps and the PCG stream. Shifting the
// timestamp alone would not be enough — two generations an equal number of
// microseconds apart would still collide on the random tail — and shifting the
// stream alone would break the monotone id property that keeps every insert at
// the right-most leaf of the primary-key btree, which is the whole reason these
// ids are minted rather than random.
//
// generation 0 is exactly what seedBulk always produced, so a run that seeds
// once is byte-identical to before.
func seedBulkFrom(
	ctx context.Context, db *gorm.DB, cfg Config, specs []jobSpec, generation int, parentID string, onInsert func(int),
) error {
	ids := rand.New(rand.NewPCG( //nolint:gosec // reproducibility, not security
		uint64(cfg.Seed), streamID+uint64(generation), //nolint:gosec // a non-negative counter
	))
	// Each generation starts a full second past the previous one's last id, which
	// is far more than any generation's microsecond-per-row span at these sizes.
	base := seedEpoch.Add(time.Duration(generation) * time.Second)
	maxAttempts := maxAttemptsFor(cfg)

	// A non-empty parentID seeds every row as a leaf child of that parent (the
	// replay cohort's shape), so it is threaded onto each row rather than left nil.
	var parent *string
	if parentID != "" {
		p := parentID
		parent = &p
	}

	rows := make([]bulkJobRow, len(specs))
	for i, spec := range specs {
		payload, err := marshalArgs(specToArgs(spec, cfg.Mix))
		if err != nil {
			return err
		}
		at := base.Add(time.Duration(i) * time.Microsecond)
		rows[i] = bulkJobRow{
			ID:            newMonotoneID(at, ids),
			CreatedAt:     at,
			UpdatedAt:     at,
			Metadata:      datatypes.JSON(`{}`),
			Kind:          loadKind,
			Queue:         cfg.Queue,
			Args:          datatypes.JSON(payload),
			Priority:      spec.Priority,
			State:         string(flywheel.StateAvailable),
			Attempt:       0,
			MaxAttempts:   maxAttempts,
			ScheduledAt:   at,
			ExecutorClass: cfg.ExecutorClass,
			ParentJobID:   parent,
			Tags:          datatypes.JSON(`[]`),
		}
	}

	for start := 0; start < len(rows); start += bulkBatch {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("loadtest: bulk seed interrupted after %d jobs: %w", start, err)
		}
		end := min(start+bulkBatch, len(rows))
		batch := rows[start:end]
		if err := db.WithContext(ctx).CreateInBatches(batch, bulkBatch).Error; err != nil {
			return fmt.Errorf("loadtest: bulk seed rows %d-%d: %w", start, end, err)
		}
		onInsert(len(batch))
	}
	return nil
}

// specToArgs renders a generated spec into the worker's args for the given mix.
// A barrier-mix parent — a spec that fans out children — carries the barrier flag
// so its worker declares the continuation; every other spec, including the mix's
// dynamically-enqueued children and continuations, carries it false.
func specToArgs(spec jobSpec, mix Workload) loadArgs {
	return loadArgs{
		N:         spec.N,
		WorkNanos: spec.WorkNanos,
		Payload:   spec.Payload,
		Children:  spec.Children,
		Barrier:   mix == WorkloadBarrier && spec.Children > 0,
		Fail:      spec.Fail,
	}
}

// newMonotoneID mints a UUIDv7 identifier from a timestamp and a deterministic
// random source.
//
// The shape matters as much as the determinism. The runtime's ids are UUIDv7, so
// they sort by creation time and every insert appends near the right-most leaf
// of the primary-key index. An id that was merely deterministic but randomly
// ordered would scatter inserts across the whole index, and the bloat and WAL
// numbers this harness reports would describe a database nobody runs.
//
// # Why rand_a carries sub-millisecond precision
//
// The timestamp field is milliseconds, and a bulk seed inserts thousands of rows
// inside one. Filling rand_a with random bits — which is what a general-purpose
// UUIDv7 does, including the runtime's own — leaves ids within a millisecond
// unordered relative to each other, and the naive version of this function was
// exactly that: not monotone, caught by its own test.
//
// So rand_a holds the microsecond offset within the millisecond instead, which
// is RFC 9562's "replace leftmost random bits with increased clock precision"
// method. The result is strictly ordered wherever the caller supplies distinct
// microseconds. It is a stronger ordering than the runtime's own ids have, and
// the difference is confined to a single millisecond — inside which every id
// shares a 48-bit prefix and lands on the same handful of leaf pages either way,
// so the index write pattern this exists to reproduce is unchanged.
func newMonotoneID(at time.Time, rng *rand.Rand) string {
	var b [16]byte

	// 48-bit big-endian millisecond timestamp.
	ms := uint64(at.UnixMilli()) //nolint:gosec // a positive timestamp, bounded by the epoch constant
	b[0] = byte(ms >> 40)
	b[1] = byte(ms >> 32)
	b[2] = byte(ms >> 24)
	b[3] = byte(ms >> 16)
	b[4] = byte(ms >> 8)
	b[5] = byte(ms)

	// Version 7 plus the 12-bit rand_a field, holding 0..999 microseconds.
	sub := uint16(at.UnixMicro() % 1000) //nolint:gosec // bounded by the modulus
	b[6] = 0x70 | byte(sub>>8)
	b[7] = byte(sub)

	// The remaining 8 bytes are deterministic randomness, with the variant bits
	// stamped over the first of them.
	binary.BigEndian.PutUint64(b[8:16], rng.Uint64())
	b[8] = (b[8] & 0x3f) | 0x80 // RFC 9562 variant

	hexBuf := make([]byte, 32)
	hex.Encode(hexBuf, b[:])
	return string(hexBuf[0:8]) + "-" + string(hexBuf[8:12]) + "-" + string(hexBuf[12:16]) + "-" +
		string(hexBuf[16:20]) + "-" + string(hexBuf[20:32])
}
