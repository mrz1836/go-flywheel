//go:build loadtest

package loadtest

import (
	"context"
	"fmt"
	"math/rand/v2"
	"sort"
	"strings"
	"testing"
	"time"
)

// columnsThatMayDiffer are the jobs columns the two seed paths are allowed to
// disagree on, each for a stated reason. Everything else must match, which is
// what makes the diff below meaningful.
//
//nolint:gochecknoglobals // shared expectation fixture for the row-shape test
var columnsThatMayDiffer = map[string]string{
	"id":           "the API path mints a random UUIDv7; the bulk path mints a deterministic one",
	"created_at":   "the API path stamps the wall clock; the bulk path stamps the fixed seed epoch",
	"updated_at":   "same",
	"scheduled_at": "same",
	"args":         "the two rows carry different job ordinals",
}

// TestSeedBulkMatchesEnqueueRowShape is the guard on the harness's second
// mapping of the jobs table.
//
// The runtime keeps its row structs unexported, so the bulk path cannot go
// through them and declares its own bulkJobRow instead. That bypasses
// jobRow.BeforeCreate, which is deliberate — the bulk path controls its own ids
// and timestamps — but it means the two paths could silently produce different
// rows, and a benchmark seeded with rows production never writes measures
// nothing.
//
// So this inserts one row down each path and diffs every column the database
// reports, with an explicit allowlist for the ones that must differ. A column
// added to the runtime's jobRow that the bulk path does not set fails it: the
// API row would carry BeforeCreate's default and the bulk row would carry the
// database's.
func TestSeedBulkMatchesEnqueueRowShape(t *testing.T) {
	dsn := requireDSN(t)
	ctx := context.Background()

	h, err := newHarness(ctx, Config{DSN: dsn, Jobs: 1})
	if err != nil {
		t.Fatalf("newHarness: %v", err)
	}
	defer func() { _ = h.Close(ctx) }()

	specs := []jobSpec{{N: 1, WorkNanos: 1000, Priority: 100, Payload: "abc"}}
	if err := seedAPI(ctx, h.work, h.cfg, specs, func() {}); err != nil {
		t.Fatalf("seedAPI: %v", err)
	}
	bulkSpecs := []jobSpec{{N: 2, WorkNanos: 1000, Priority: 100, Payload: "abc"}}
	if err := seedBulk(ctx, h.work, h.cfg, bulkSpecs, "", func(int) {}); err != nil {
		t.Fatalf("seedBulk: %v", err)
	}

	rows := readAllJobs(t, h)
	if len(rows) != 2 {
		t.Fatalf("expected exactly two rows, got %d", len(rows))
	}
	// The API row is the one whose args name job 1.
	apiRow, bulkRow := rows[0], rows[1]
	if !containsOrdinal(apiRow["args"], 1) {
		apiRow, bulkRow = bulkRow, apiRow
	}

	if len(apiRow) != len(bulkRow) {
		t.Fatalf("the two rows report different column sets: %d vs %d", len(apiRow), len(bulkRow))
	}
	for _, col := range sortedKeys(apiRow) {
		if reason, allowed := columnsThatMayDiffer[col]; allowed {
			t.Logf("column %s may differ: %s", col, reason)
			continue
		}
		a, b := render(apiRow[col]), render(bulkRow[col])
		if a != b {
			t.Errorf("column %s: enqueue path wrote %q, bulk path wrote %q — the bulk path is not "+
				"reproducing the row the runtime writes", col, a, b)
		}
	}
}

// TestSeedBulkMintsMonotoneIDs proves the bulk path's ids sort in insertion
// order.
//
// It is not a stylistic preference. Production ids are UUIDv7, so they are
// time-ordered and every insert lands at the right-most leaf of the primary-key
// btree. Ids that were deterministic but randomly ordered would scatter inserts
// across the index and produce page-split behavior no production database sees,
// which would corrupt every storage number in the report.
func TestSeedBulkMintsMonotoneIDs(t *testing.T) {
	t.Parallel()

	rng := rand.New(rand.NewPCG(1, streamID)) //nolint:gosec // reproducibility, not security
	prev := ""
	for i := range 5000 {
		id := newMonotoneID(seedEpoch.Add(time.Duration(i)*time.Microsecond), rng)
		if len(id) != 36 {
			t.Fatalf("id %q is %d chars, want a 36-char UUID", id, len(id))
		}
		if id[14] != '7' {
			t.Fatalf("id %q is not version 7: production ids are UUIDv7 and the write pattern depends on it", id)
		}
		if id <= prev {
			t.Fatalf("id %q does not sort after %q: inserts would scatter across the primary-key index", id, prev)
		}
		prev = id
	}
}

// TestSeedBulkIsReproducible proves two identical configs mint identical ids, so
// a bulk-seeded run is byte-identical end to end and not merely
// workload-identical.
func TestSeedBulkIsReproducible(t *testing.T) {
	t.Parallel()

	mint := func(seed int64) []string {
		rng := rand.New(rand.NewPCG(uint64(seed), streamID)) //nolint:gosec // reproducibility, not security
		out := make([]string, 20)
		for i := range out {
			out[i] = newMonotoneID(seedEpoch.Add(time.Duration(i)*time.Microsecond), rng)
		}
		return out
	}

	first, second, other := mint(7), mint(7), mint(8)
	for i := range first {
		if first[i] != second[i] {
			t.Fatalf("id %d differs across runs with the same seed: %q vs %q", i, first[i], second[i])
		}
		if first[i] == other[i] {
			t.Fatalf("id %d is identical under a different seed: the seed is not being used", i)
		}
	}
}

// TestSeedPathsBothLandEveryRow proves neither path loses rows at a batch
// boundary — the bulk path inserts in batches of bulkBatch, and an off-by-one
// there would silently under-seed a benchmark.
func TestSeedPathsBothLandEveryRow(t *testing.T) {
	dsn := requireDSN(t)
	ctx := context.Background()

	// One row over two batches, so the boundary is exercised in both directions.
	const rows = bulkBatch + 1

	h, err := newHarness(ctx, Config{DSN: dsn, Jobs: rows})
	if err != nil {
		t.Fatalf("newHarness: %v", err)
	}
	defer func() { _ = h.Close(ctx) }()

	specs := generate(h.cfg)
	if len(specs) != rows {
		t.Fatalf("generate produced %d specs, want %d", len(specs), rows)
	}
	inserted := 0
	if err := seedBulk(ctx, h.work, h.cfg, specs, "", func(n int) { inserted += n }); err != nil {
		t.Fatalf("seedBulk: %v", err)
	}
	if inserted != rows {
		t.Errorf("the callback reported %d inserts, want %d", inserted, rows)
	}

	var count int64
	if err := h.probe.Raw(`SELECT count(*) FROM jobs`).Scan(&count).Error; err != nil {
		t.Fatalf("count jobs: %v", err)
	}
	if count != rows {
		t.Errorf("the table holds %d rows, want %d", count, rows)
	}
}

// readAllJobs reads every jobs row as a column map, ordered by args so the
// result is stable.
func readAllJobs(t *testing.T, h *Harness) []map[string]any {
	t.Helper()
	var rows []map[string]any
	if err := h.probe.Raw(`SELECT * FROM jobs ORDER BY args::text`).Scan(&rows).Error; err != nil {
		t.Fatalf("read jobs: %v", err)
	}
	return rows
}

// containsOrdinal reports whether an args value names job ordinal n.
func containsOrdinal(args any, n int) bool {
	return strings.Contains(render(args), fmt.Sprintf(`"n":%d,`, n))
}

// sortedKeys returns a map's keys in order, so a column diff reads the same way
// every run.
func sortedKeys(m map[string]any) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// render normalizes a scanned column value for comparison.
func render(v any) string {
	switch t := v.(type) {
	case nil:
		return "<nil>"
	case []byte:
		return string(t)
	case string:
		return t
	default:
		return fmt.Sprintf("%v", t)
	}
}
