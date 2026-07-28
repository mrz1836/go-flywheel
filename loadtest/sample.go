//go:build loadtest

package loadtest

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// pgstattupleEvery is how many samples pass between pgstattuple calls.
//
// Even the approximate form walks the visibility map, and the exact form is a
// full heap scan. Calling either every second against 100k rows would put a
// measurable load on the run it is measuring, which is the one thing a sampler
// must not do. Once every ten samples is enough to see a bloat trajectory.
const pgstattupleEvery = 10

// sampledTables are the relations the sampler reports on.
//
//nolint:gochecknoglobals // the runtime's own three tables, fixed
var sampledTables = []string{"jobs", "job_runs"}

// sampleset accumulates storage samples and the run-level peaks derived from
// them.
//
// The peaks are tracked here rather than recomputed from the series at report
// time for the same reason peakRSS always was: a caller that trims or downsamples
// the series would silently change the peak, and a peak is the one statistic that
// must survive the series it came from.
type sampleset struct {
	mu              sync.Mutex
	samples         []StorageSample
	peakRSS         uint64
	peakXactAge     float64
	peakLockWaits   int64
	longestLockWait float64
}

// add records one sample and tracks the running peaks.
func (s *sampleset) add(sample StorageSample) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.samples = append(s.samples, sample)
	if sample.RSS > s.peakRSS {
		s.peakRSS = sample.RSS
	}
	if sample.MaxXactAge > s.peakXactAge {
		s.peakXactAge = sample.MaxXactAge
	}
	if sample.LockWaits > s.peakLockWaits {
		s.peakLockWaits = sample.LockWaits
	}
	if sample.LongestLockWait > s.longestLockWait {
		s.longestLockWait = sample.LongestLockWait
	}
}

// peaks returns the run-level contention peaks the samples observed.
func (s *sampleset) peaks() (xactAge float64, lockWaits int64, longestLockWait float64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.peakXactAge, s.peakLockWaits, s.longestLockWait
}

// all returns a copy of the collected samples.
func (s *sampleset) all() []StorageSample {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.samples) == 0 {
		return nil
	}
	return append([]StorageSample(nil), s.samples...)
}

// peak returns the highest RSS any sample observed.
func (s *sampleset) peak() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.peakRSS
}

// runSampler samples the target's storage and contention state until ctx ends.
//
// # Every statement runs in autocommit, and that is load-bearing
//
// A transaction holds a snapshot, and a held snapshot blocks autovacuum from
// removing dead tuples newer than it. A sampler wrapped in a transaction would
// therefore suppress the exact behavior it exists to observe: dead-tuple counts
// would climb, autovacuum counts would stall, and the report would describe a
// database that was being prevented from vacuuming by its own instrument.
//
// This is worth stating loudly because the refactor that breaks it is an
// attractive one — wrapping these five queries in db.Transaction to get a
// consistent view across them. Do not. A consistent view is precisely what must
// not be taken here, and the sampler tolerates the small inconsistency between
// its statements instead.
//
// It runs on the probe pool, which is one connection, never shared with the
// runners, and deliberately outside any fault gate: a fault the harness cannot
// see through is a fault the report cannot describe.
func (h *Harness) runSampler(ctx context.Context) {
	ticker := time.NewTicker(h.cfg.SampleInterval)
	defer ticker.Stop()

	h.noteRSSMechanism()
	bloat := h.pgstattupleAvailable(ctx)

	var prevWAL int64
	var n int
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		sample, wal := h.sampleOnce(ctx, prevWAL, bloat && n%pgstattupleEvery == 0)
		if ctx.Err() != nil {
			// Cancelled mid-sample. Its queries failed part-way through, so the
			// maps are partly empty — and an empty map reads as a zero, which
			// would make the run's final sample report no table size and no
			// scans at all. Discard it rather than publish it.
			return
		}
		prevWAL = wal
		n++
		h.samples.add(sample)
	}
}

// finalSample takes one last reading after the runners have stopped.
//
// It exists for two reasons. The periodic series ends whenever the drain did,
// which is rarely on a tick boundary, so without this the report's last sample
// can be most of an interval stale. And PostgreSQL's cumulative statistics are
// reported by each backend rather than written live, so the scan and tuple
// counters only settle once the runners' connections have gone idle — which is
// exactly the moment this runs.
func (h *Harness) finalSample(ctx context.Context) {
	sample, _ := h.sampleOnce(ctx, 0, false)
	h.samples.add(sample)
}

// sampleOnce takes one sample, returning it and the absolute WAL position it
// read, so the next call can report a delta.
func (h *Harness) sampleOnce(ctx context.Context, prevWAL int64, withBloat bool) (StorageSample, int64) {
	sample := StorageSample{At: time.Now()}

	// A failure caused by the run ending is not a finding: the sampler is
	// cancelled at shutdown by design, and collecting those errors would put a
	// context-canceled entry in the report of every successful run.
	collect := func(err error) {
		if err != nil && ctx.Err() == nil {
			h.errs.add(err)
		}
	}

	collect(h.sampleTableStats(ctx, &sample))
	collect(h.sampleSizes(ctx, &sample))

	wal, err := h.sampleWAL(ctx)
	if err != nil {
		collect(err)
		wal = prevWAL
	}
	if prevWAL > 0 && wal >= prevWAL {
		sample.WALBytes = wal - prevWAL
	}

	locks, longestWait, err := h.sampleLockWaits(ctx)
	collect(err)
	sample.LockWaits = locks
	sample.LongestLockWait = longestWait

	xactAge, err := h.sampleXactAge(ctx)
	collect(err)
	sample.MaxXactAge = xactAge

	if withBloat {
		collect(h.sampleBloat(ctx, &sample))
	}

	if rss, ok := currentRSS(); ok {
		sample.RSS = rss
	}
	return sample, wal
}

// sampleTableStats reads the tuple, vacuum, and scan counters.
//
// SeqScans and IdxScans are the reason this query is worth its cost. They turn
// an index-condition comparison from an inference about timing into direct
// evidence about the plan: a claim that used jobs_ready increments idx_scan, and
// one that could not increments seq_scan. No amount of latency measurement says
// that as plainly.
func (h *Harness) sampleTableStats(ctx context.Context, sample *StorageSample) error {
	type row struct {
		Relname          string
		NLiveTup         int64
		NDeadTup         int64
		AutovacuumCount  int64
		AutoanalyzeCount int64
		SeqScan          int64
		IdxScan          int64
	}
	var rows []row
	// idx_scan is NULL until the table has been scanned through an index at
	// least once, which is exactly the correctness-only condition's normal state.
	if err := h.probe.WithContext(ctx).Raw(`
		SELECT relname, n_live_tup, n_dead_tup, autovacuum_count, autoanalyze_count,
		       coalesce(seq_scan, 0) AS seq_scan, coalesce(idx_scan, 0) AS idx_scan
		FROM pg_stat_user_tables
		WHERE schemaname = current_schema()`).Scan(&rows).Error; err != nil {
		return fmt.Errorf("loadtest: sample table stats: %w", err)
	}

	sample.LiveTuples = make(map[string]int64, len(rows))
	sample.DeadTuples = make(map[string]int64, len(rows))
	sample.AutovacuumCount = make(map[string]int64, len(rows))
	sample.AutoanalyzeCount = make(map[string]int64, len(rows))
	sample.SeqScans = make(map[string]int64, len(rows))
	sample.IdxScans = make(map[string]int64, len(rows))
	for _, r := range rows {
		sample.LiveTuples[r.Relname] = r.NLiveTup
		sample.DeadTuples[r.Relname] = r.NDeadTup
		sample.AutovacuumCount[r.Relname] = r.AutovacuumCount
		sample.AutoanalyzeCount[r.Relname] = r.AutoanalyzeCount
		sample.SeqScans[r.Relname] = r.SeqScan
		sample.IdxScans[r.Relname] = r.IdxScan
	}
	return nil
}

// sampleSizes reads total relation and index sizes.
func (h *Harness) sampleSizes(ctx context.Context, sample *StorageSample) error {
	type row struct {
		Relname    string
		TableBytes int64
		IndexBytes int64
	}
	var rows []row
	if err := h.probe.WithContext(ctx).Raw(`
		SELECT relname,
		       pg_total_relation_size(relid) AS table_bytes,
		       pg_indexes_size(relid)        AS index_bytes
		FROM pg_stat_user_tables
		WHERE schemaname = current_schema()`).Scan(&rows).Error; err != nil {
		return fmt.Errorf("loadtest: sample relation sizes: %w", err)
	}

	sample.TableBytes = make(map[string]int64, len(rows))
	sample.IndexBytes = make(map[string]int64, len(rows))
	for _, r := range rows {
		sample.TableBytes[r.Relname] = r.TableBytes
		sample.IndexBytes[r.Relname] = r.IndexBytes
	}
	return nil
}

// sampleWAL reads the cluster's WAL byte counter.
//
// The cast is required, not stylistic: pg_stat_wal.wal_bytes is numeric, not
// bigint, and scanning it into an int64 without it fails. Verified against the
// server this harness targets rather than assumed from the documentation.
func (h *Harness) sampleWAL(ctx context.Context) (int64, error) {
	var wal int64
	if err := h.probe.WithContext(ctx).
		Raw(`SELECT wal_bytes::bigint FROM pg_stat_wal`).Scan(&wal).Error; err != nil {
		return 0, fmt.Errorf("loadtest: sample WAL: %w", err)
	}
	return wal, nil
}

// sampleLockWaits counts ungranted lock requests at this instant and reports
// the longest one's wait, scoped to this database.
//
// The bare `count(*) FROM pg_locks WHERE NOT granted` it replaces read zero in
// every published report, and the reason is structural rather than lucky: with
// one sweeper there is no second transaction to contend with, and a count of a
// transient condition sampled once a second sees almost nothing regardless.
// Joining pg_stat_activity turns it from a count of a condition into a
// measurement of a duration — how long the longest blocked backend has been
// waiting — which is the quantity a lock-contention claim actually needs, and
// which is non-zero for as long as the contention lasts rather than only at the
// instant it is sampled.
//
// It also scopes to the current database. The count it replaces was
// cluster-wide, so a sibling database's contention read as this run's.
func (h *Harness) sampleLockWaits(ctx context.Context) (int64, float64, error) {
	var row struct {
		Waiters     int64
		LongestWait float64
	}
	if err := h.probe.WithContext(ctx).Raw(`
		SELECT count(*) AS waiters,
		       coalesce(max(extract(epoch from (now() - a.state_change))), 0) AS longest_wait
		FROM pg_locks l
		JOIN pg_stat_activity a ON a.pid = l.pid
		WHERE NOT l.granted
		  AND a.datname = current_database()
		  AND a.backend_type = 'client backend'
		  AND a.pid <> pg_backend_pid()`).Scan(&row).Error; err != nil {
		return 0, 0, fmt.Errorf("loadtest: sample lock waits: %w", err)
	}
	return row.Waiters, row.LongestWait, nil
}

// sampleXactAge reports the age in seconds of the longest-running transaction
// open against this database, excluding the sampler's own backend.
//
// It is the instrument the bounded-maintenance claim is measured against, and
// its guarantee is worth stating precisely because it is one-sided. A sampler at
// interval I is *guaranteed* to observe any transaction living longer than I, at
// an age of at least L-I; a transaction shorter than I may be missed entirely.
//
// That makes it exactly the right instrument for the unbounded sweep it is meant
// to characterize — a transaction measured in seconds cannot hide from a
// one-second sampler — and the wrong one for the bounded sweep that replaced it,
// whose batches are far shorter than any sample interval. For the bounded side
// the client-side Sweep latency is exact, and the report says which number is
// which rather than presenting them as interchangeable.
// It counts client backends only, and that exclusion is load-bearing rather
// than tidy. An autovacuum worker appears in pg_stat_activity with an
// xact_start like any other backend, and on a churning table it runs for
// minutes: measured on a 1M-row drain leaving ~1.2M dead tuples, an autovacuum
// worker produced a 110-second "longest transaction" while the sweep's own
// batches were finishing in single-digit milliseconds. Including it does not
// make the number conservative, it makes it answer a different question — one
// AutovacuumCount already answers — and it would be published as evidence about
// maintenance transactions it has nothing to do with.
func (h *Harness) sampleXactAge(ctx context.Context) (float64, error) {
	var age float64
	if err := h.probe.WithContext(ctx).Raw(`
		SELECT coalesce(max(extract(epoch from (now() - xact_start))), 0)
		FROM pg_stat_activity
		WHERE datname = current_database()
		  AND xact_start IS NOT NULL
		  AND backend_type = 'client backend'
		  AND pid <> pg_backend_pid()`).Scan(&age).Error; err != nil {
		return 0, fmt.Errorf("loadtest: sample transaction age: %w", err)
	}
	return age, nil
}

// sampleBloat reads approximate free and dead-tuple percentages.
//
// pgstattuple_approx walks the visibility map; the exact pgstattuple is a full
// heap scan, and calling that against 100k rows on a one-second cadence would
// corrupt the run it measures.
func (h *Harness) sampleBloat(ctx context.Context, sample *StorageSample) error {
	sample.FreePercent = make(map[string]float64, len(sampledTables))
	sample.DeadTuplePercent = make(map[string]float64, len(sampledTables))

	for _, table := range sampledTables {
		var row struct {
			ApproxFreePercent float64
			DeadTuplePercent  float64
		}
		if err := h.probe.WithContext(ctx).Raw(
			`SELECT approx_free_percent, dead_tuple_percent FROM pgstattuple_approx(?::regclass)`, table,
		).Scan(&row).Error; err != nil {
			return fmt.Errorf("loadtest: sample bloat for %s: %w", table, err)
		}
		sample.FreePercent[table] = row.ApproxFreePercent
		sample.DeadTuplePercent[table] = row.DeadTuplePercent
	}
	return nil
}

// pgstattupleAvailable reports whether the extension is installed, and records
// the answer either way.
//
// The distinction between available and installed matters: on the server this
// harness targets, pgstattuple ships with the distribution but is not created in
// the database. Absent, the two percent maps are omitted and the note says so —
// reporting zero free space and zero dead tuples would be a measurement nobody
// took.
func (h *Harness) pgstattupleAvailable(ctx context.Context) bool {
	var n int64
	if err := h.probe.WithContext(ctx).
		Raw(`SELECT count(*) FROM pg_extension WHERE extname = 'pgstattuple'`).Scan(&n).Error; err != nil {
		h.errs.add(fmt.Errorf("loadtest: check for pgstattuple: %w", err))
		return false
	}
	if n == 0 {
		h.notes.add(
			"pgstattuple is not installed in this database, so FreePercent and DeadTuplePercent are " +
				"absent rather than zero. CREATE EXTENSION pgstattuple to collect them.",
		)
		return false
	}
	h.notes.add(
		"Bloat percentages come from pgstattuple_approx (visibility-map based) every %d samples, "+
			"not from the exact pgstattuple, whose full heap scan would perturb the run it measures.",
		pgstattupleEvery,
	)
	return true
}

// noteSamplingCaveats records what the sampled numbers do and do not mean.
//
// Two of them read as zero under conditions a reader would misinterpret, and a
// number that reads as good news when it means "not observed" is worse than an
// absent one.
func (h *Harness) noteSamplingCaveats() {
	h.notes.add(
		"LockWaits is an instantaneous sample of pg_locks: it counts requests ungranted at the moment " +
			"of the sample, and lock waits under normal contention are far shorter than the sampling " +
			"interval. A run of zeroes means the sampler did not catch one, not that there was no " +
			"contention. LongestLockWait is the companion that survives sampling: it measures how long " +
			"the longest blocked backend has been waiting, which stays non-zero for the duration of a " +
			"wait rather than only at the instant it is sampled. Both are scoped to this database.",
	)
	h.notes.add(
		"MaxXactAge is sampled server-side from pg_stat_activity, and its guarantee is one-sided: a " +
			"sampler at interval I always observes a transaction living longer than I, at an age of at " +
			"least L-I, and may miss one shorter than I entirely. It is therefore the right instrument " +
			"for an unbounded maintenance transaction and the wrong one for a bounded batch. For " +
			"bounded maintenance the exact figure is Sweep.Max, which is client-side, is not bucketed, " +
			"and covers exactly one transaction per call - at the cost of including pool acquisition, " +
			"which the server-side age excludes.",
	)
	h.notes.add(
		"MaxXactAge and the lock-wait pair count client backends only. An autovacuum worker carries an " +
			"xact_start like any other backend and runs for minutes on a churning table, so including " +
			"it would report vacuum duration as maintenance-transaction age - a quantity " +
			"AutovacuumCount already reports, in the one field meant to answer whether a maintenance " +
			"pass held a snapshot open.",
	)
	h.notes.add(
		"MaxXactAge and the lock-wait pair are scoped to the database, not to this run's schema. The " +
			"harness isolates a run by schema within one database, so a concurrent run against the same " +
			"database - or an interactive query against it - is visible in these three numbers. Use a " +
			"dedicated database for a run whose contention figures are being published.",
	)
	h.notes.add(
		"WALBytes is the delta in pg_stat_wal, which is cluster-wide: PostgreSQL offers no per-database " +
			"breakdown, so any other activity on the same server is included.",
	)
}

// noteRSSMechanism records which RSS source ran and what it measures.
//
// The second half matters more than the first. "Process RSS" in a benchmark
// report reads as the database server's memory, and this is the harness client's
// — a different process, often on a different machine.
func (h *Harness) noteRSSMechanism() {
	h.notes.add(
		"RSS is the harness client process, not the PostgreSQL server. Mechanism: %s.",
		rssMechanism(),
	)
}
