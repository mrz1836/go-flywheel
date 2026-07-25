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
		payload, err := marshalArgs(specToArgs(spec))
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
func seedBulk(ctx context.Context, db *gorm.DB, cfg Config, specs []jobSpec, onInsert func(int)) error {
	ids := rand.New(rand.NewPCG(uint64(cfg.Seed), streamID)) //nolint:gosec // reproducibility, not security

	rows := make([]bulkJobRow, len(specs))
	for i, spec := range specs {
		payload, err := marshalArgs(specToArgs(spec))
		if err != nil {
			return err
		}
		at := seedEpoch.Add(time.Duration(i) * time.Microsecond)
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
			MaxAttempts:   25,
			ScheduledAt:   at,
			ExecutorClass: cfg.ExecutorClass,
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

// specToArgs renders a generated spec into the worker's args.
func specToArgs(spec jobSpec) loadArgs {
	return loadArgs{
		N:         spec.N,
		WorkNanos: spec.WorkNanos,
		Payload:   spec.Payload,
		Children:  spec.Children,
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
