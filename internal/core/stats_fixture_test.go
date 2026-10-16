package core

import (
	"context"
	"fmt"
	"math"
	"math/rand/v2"
	"sort"
	"testing"
	"time"

	"github.com/mrz1836/go-foundation/models"
	"github.com/stretchr/testify/require"
	"gorm.io/datatypes"
	"gorm.io/gorm"
)

// dbOpener builds a database carrying the full runtime schema, so a stats
// suite runs unchanged against either dialect.
type dbOpener func(t *testing.T) *gorm.DB

// sqliteOpener is the SQLite dbOpener.
func sqliteOpener(t *testing.T) *gorm.DB {
	t.Helper()
	return newDB(t)
}

// histRun describes one finished run to seed directly, bypassing the runtime so
// a test controls exactly which hour it lands in.
type histRun struct {
	ID    string // minted when empty
	JobID string // minted when empty: at EnqueuedAt when that is set, else now
	// EnqueuedAt is when the job was inserted, as its id records it; zero mints
	// the id at the wall clock, as the runtime does.
	EnqueuedAt time.Time
	Kind       string
	Queue      string
	Outcome    RunOutcome
	JobState   JobState // the state the attempt applied; empty records none
	FinishedAt time.Time
	DurationMs int // negative records none (a crash)
	WaitMs     int // negative records none
	Superseded bool
	CostMicros int64
	// ErrorClass and ErrorMessage are the attempt's error, stored when set, and
	// Fingerprint its stored error_fingerprint; empty stores none, as a run an
	// older release wrote has none.
	ErrorClass   ErrorClass
	ErrorMessage string
	Fingerprint  string
	// Legacy writes the row as an older binary did: kind and queue left at the
	// column default, for the read path to resolve through the job.
	Legacy bool
}

// seedHistory writes each run and a terminal job for it, in batches. The job
// carries the run's kind and queue, and is discarded at the run's attempt when
// the run discarded it, so a legacy row's discard is resolvable. A legacy run's
// finish-log entry is written the way an upgraded database gets it: by
// BackfillRunFinishes.
func seedHistory(t testing.TB, db *gorm.DB, runs []histRun) {
	t.Helper()
	writeHistory(t, db, runs)
	for i := range runs {
		if runs[i].Legacy {
			_, err := BackfillRunFinishes(context.Background(), db)
			require.NoError(t, err)
			break
		}
	}
}

// writeHistory is seedHistory without the backfill: a legacy run is left as an
// older binary left it, with no finish-log entry.
func writeHistory(t testing.TB, db *gorm.DB, runs []histRun) {
	t.Helper()
	jobs := make([]jobRow, 0, len(runs))
	rows := make([]jobRunRow, 0, len(runs))
	for i := range runs {
		r := runs[i]
		if r.ID == "" {
			r.ID = models.NewID()
		}
		switch {
		case r.JobID != "":
		case !r.EnqueuedAt.IsZero():
			r.JobID = mintIDAt(r.EnqueuedAt)
		default:
			r.JobID = models.NewID()
		}
		if r.Queue == "" {
			r.Queue = defaultQueue
		}
		state := StateSucceeded
		if r.JobState == StateDiscarded {
			state = StateDiscarded
		}
		finalized := r.FinishedAt
		jobs = append(jobs, jobRow{
			ID: r.JobID, Kind: r.Kind, Queue: r.Queue, Args: datatypes.JSON("{}"), State: string(state),
			Attempt: 1, MaxAttempts: 1, CreatedAt: r.FinishedAt.Add(-time.Hour), UpdatedAt: r.FinishedAt,
			ScheduledAt: r.FinishedAt.Add(-time.Hour), FinalizedAt: &finalized,
		})
		finished := r.FinishedAt
		row := jobRunRow{
			ID: r.ID, JobID: r.JobID, Attempt: 1, Kind: r.Kind, Queue: r.Queue, ExecutorID: "seed",
			StartedAt:  r.FinishedAt.Add(-time.Duration(max(r.DurationMs, 0)) * time.Millisecond),
			FinishedAt: &finished, Outcome: string(r.Outcome), Superseded: r.Superseded,
		}
		if r.Legacy {
			row.Kind, row.Queue = "", ""
		}
		if r.DurationMs >= 0 {
			d := r.DurationMs
			row.DurationMs = &d
		}
		if r.WaitMs >= 0 {
			w := r.WaitMs
			row.QueueWaitMs = &w
		}
		if r.JobState != "" {
			s := string(r.JobState)
			row.JobState = &s
		}
		if r.CostMicros != 0 {
			c := r.CostMicros
			row.CostMicros = &c
		}
		if r.ErrorClass != "" {
			c := string(r.ErrorClass)
			row.ErrorClass = &c
		}
		if r.ErrorMessage != "" {
			m := r.ErrorMessage
			row.ErrorMessage = &m
		}
		if r.Fingerprint != "" {
			fp := r.Fingerprint
			row.ErrorFingerprint = &fp
		}
		rows = append(rows, row)
	}
	require.NoError(t, db.CreateInBatches(jobs, 200).Error)
	require.NoError(t, db.CreateInBatches(rows, 200).Error)
	// The finish log, written the way the runtime writes it — read back off the
	// run rows — for every run this release would have finalized.
	require.NoError(t, db.Exec(fmt.Sprintf(finishLogInsert, "job_runs", " AND kind <> ''")).Error)
}

// finishEntries reads every job_run_finishes entry for a run.
func finishEntries(t testing.TB, db *gorm.DB, runID string) []jobRunFinishRow {
	t.Helper()
	var rows []jobRunFinishRow
	require.NoError(t, db.Where("run_id = ?", runID).Find(&rows).Error)
	return rows
}

// okRun is a successful run of kind at finishedAt taking durMs.
func okRun(kind string, finishedAt time.Time, durMs int) histRun {
	return histRun{
		Kind: kind, Outcome: OutcomeSuccess, JobState: StateSucceeded,
		FinishedAt: finishedAt, DurationMs: durMs, WaitMs: 10,
	}
}

// discardRun is a failed run of kind at finishedAt that discarded its job.
func discardRun(kind string, finishedAt time.Time) histRun {
	return histRun{
		Kind: kind, Outcome: OutcomeError, JobState: StateDiscarded,
		FinishedAt: finishedAt, DurationMs: 50, WaitMs: 10,
	}
}

// randomHistory builds n runs spread over [from, from+span), across four kinds
// and two queues, with every outcome, superseded and crashed rows, and legacy
// rows mixed in — the oracle's input. A crashed row records the state the sweep
// applied, available or discarded, and some crashed rows finished late: their
// superseded finalize wrote its own outcome beside the state the sweep recorded.
// Each job was inserted up to three hours before its run finished, so the
// enqueue counts fall in other hours than the finishes, some before the history.
// Every error and timeout carries a class and a message of one of four shapes,
// and one in five stores a fingerprint of the worker's own; a legacy row stores
// none.
//
//nolint:gocognit // one random draw per column, with the combinations each outcome allows
func randomHistory(rng *rand.Rand, n int, from time.Time, span time.Duration) []histRun {
	kinds := []string{"alpha", "beta", "gamma", "delta"}
	queues := []string{"default", "bulk"}
	outcomes := []RunOutcome{
		OutcomeSuccess, OutcomeSuccess, OutcomeSuccess, OutcomeSuccess,
		OutcomeError, OutcomeTimeout, OutcomeSnooze, OutcomeCancelled, OutcomeCrashed,
	}
	runs := make([]histRun, n)
	for i := range runs {
		lateCrash := false
		o := outcomes[rng.IntN(len(outcomes))]
		r := histRun{
			Kind:       kinds[rng.IntN(len(kinds))],
			Queue:      queues[rng.IntN(len(queues))],
			Outcome:    o,
			FinishedAt: from.Add(time.Duration(rng.Int64N(int64(span)))),
			DurationMs: int(math.Round(math.Exp(5 + 1.5*rng.NormFloat64()))),
			WaitMs:     rng.IntN(5000),
			CostMicros: rng.Int64N(1000),
			Superseded: rng.IntN(25) == 0,
			Legacy:     rng.IntN(10) == 0,
		}
		r.EnqueuedAt = r.FinishedAt.Add(-time.Duration(rng.Int64N(int64(3 * time.Hour))))
		switch o {
		case OutcomeSuccess:
			r.JobState = StateSucceeded
		case OutcomeError, OutcomeTimeout:
			r.JobState = StateRetryable
			if rng.IntN(3) == 0 {
				r.JobState = StateDiscarded
			}
		case OutcomeSnooze:
			r.JobState = StateScheduled
		case OutcomeCancelled:
			r.JobState = StateCancelled
		case OutcomeCrashed:
			r.JobState = StateAvailable
			if rng.IntN(3) == 0 {
				// The crash spent the job's last attempt, so the sweep discarded it.
				r.JobState = StateDiscarded
			}
			r.DurationMs = -1
			if rng.IntN(4) == 0 {
				// The crashed attempt finished after the sweep: its superseded
				// finalize recorded its own outcome, duration, and cost, and kept
				// the state the sweep had recorded.
				lateCrash = true
				r.Outcome = []RunOutcome{OutcomeSuccess, OutcomeError}[rng.IntN(2)]
				r.DurationMs = 1 + rng.IntN(5000)
				r.Superseded, r.Legacy = true, false
			}
		}
		if r.Superseded && !lateCrash {
			r.JobState = ""
		}
		if r.Legacy {
			// An older binary wrote neither the job state nor the wait.
			r.JobState, r.WaitMs = legacyState(r.JobState), -1
		}
		if rng.IntN(20) == 0 {
			// A row that never recorded a wait (written before the column, or by
			// a custom Driver).
			r.WaitMs = -1
		}
		if r.Outcome == OutcomeError || r.Outcome == OutcomeTimeout {
			r.ErrorClass = []ErrorClass{ErrorTransient, ErrorPermanent}[rng.IntN(2)]
			if r.Outcome == OutcomeTimeout {
				r.ErrorClass = ErrorTimeout
			}
			r.ErrorMessage = randomErrorMessage(rng)
			if rng.IntN(5) == 0 && !r.Legacy {
				r.Fingerprint = fmt.Sprintf("%016x", 0xfeed0+rng.IntN(3))
			}
		}
		runs[i] = r
	}
	return runs
}

// randomErrorMessage is a failure's message in one of four shapes, its values
// drawn at random, so each shape normalizes to one text.
func randomErrorMessage(rng *rand.Rand) string {
	switch rng.IntN(4) {
	case 0:
		return fmt.Sprintf("upstream returned %d for item %d", 500+rng.IntN(4), rng.IntN(10_000))
	case 1:
		return fmt.Sprintf("dial tcp 10.0.%d.%d:5432: connect: connection refused", rng.IntN(255), rng.IntN(255))
	case 2:
		return fmt.Sprintf("context deadline exceeded after %ds waiting on 'warehouse-%d'", rng.IntN(60), rng.IntN(9))
	default:
		return fmt.Sprintf(`row %d rejected: field "amount" = "%d.%02d USD"`, rng.IntN(5000), rng.IntN(100), rng.IntN(100))
	}
}

// legacyState keeps only what the seeder needs to make a legacy row's discard
// resolvable — its job's terminal state — and drops the column value itself.
func legacyState(s JobState) JobState {
	if s == StateDiscarded {
		return StateDiscarded
	}
	return ""
}

// rawKindStats is the oracle: the window aggregated entirely from raw job_runs
// by the one aggregate function, merged per kind exactly as Stats merges, with no
// rollup involved.
func rawKindStats(t *testing.T, db *gorm.DB, from, to time.Time, f statsFilter) ([]KindStats, KindStats) {
	t.Helper()
	return rawGroupedStats(t, db, from, to, f, StatsByKind)
}

// rawGroupedStats is the oracle for one grouping: the raw aggregate merged per
// kind, per queue, or per kind and queue, each entry naming its group, sorted
// as Stats sorts.
func rawGroupedStats(
	t *testing.T, db *gorm.DB, from, to time.Time, f statsFilter, groupBy StatsGroupBy,
) ([]KindStats, KindStats) {
	t.Helper()
	aggs, err := aggregateRuns(fixedClockCtx(to), db, from, to, f)
	require.NoError(t, err)
	byGroup := map[[2]string]*runAgg{}
	total := newRunAgg("", "")
	for _, a := range aggs {
		key := [2]string{a.Kind, ""}
		switch groupBy {
		case StatsByQueue:
			key = [2]string{"", a.Queue}
		case StatsByKindQueue:
			key = [2]string{a.Kind, a.Queue}
		}
		k, ok := byGroup[key]
		if !ok {
			k = newRunAgg(key[0], key[1])
			byGroup[key] = k
		}
		k.merge(a)
		total.merge(a)
	}
	out := make([]KindStats, 0, len(byGroup))
	for key, a := range byGroup {
		ks := kindStatsFromAgg(a)
		ks.Queue = key[1]
		out = append(out, ks)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Kind != out[j].Kind {
			return out[i].Kind < out[j].Kind
		}
		return out[i].Queue < out[j].Queue
	})
	return out, kindStatsFromAgg(total)
}

// rollupAll rolls every closed hour as of now, however many there are.
func rollupAll(t testing.TB, db *gorm.DB, now time.Time) RollupResult {
	t.Helper()
	res, err := rollupPass(fixedClockCtx(now), db, now, rollupConfig{
		grace: defaultStatsRollupGrace, maxHours: 100_000, retention: defaultStatsRetention,
	})
	require.NoError(t, err)
	return res
}

// statsRows reads every stored rollup row with rolled_at zeroed, for
// comparisons that should not depend on when a row was written.
func statsRows(t testing.TB, db *gorm.DB) []jobStatsHourlyRow {
	t.Helper()
	var rows []jobStatsHourlyRow
	require.NoError(t, db.Order("bucket_start_unix, kind, queue").Find(&rows).Error)
	for i := range rows {
		rows[i].RolledAt = time.Time{}
	}
	return rows
}

// groupStatsRows reads the stored (kind, queue) group rows — the total rows the
// rollup writes beside them filtered out — with rolled_at zeroed.
func groupStatsRows(t testing.TB, db *gorm.DB) []jobStatsHourlyRow {
	t.Helper()
	var out []jobStatsHourlyRow
	for _, r := range statsRows(t, db) {
		if r.Kind != statsTotalKey && r.Queue != statsTotalKey {
			out = append(out, r)
		}
	}
	return out
}

// hourAt is the start of the UTC hour n hours after base.
func hourAt(base time.Time, n int) time.Time {
	return floorHour(base).Add(time.Duration(n) * time.Hour)
}

// mustKind returns the KindStats for kind, failing the test when absent.
func mustKind(t *testing.T, res StatsResult, kind string) KindStats {
	t.Helper()
	for _, k := range res.Kinds {
		if k.Kind == kind {
			return k
		}
	}
	t.Fatalf("kind %q not in result: %v", kind, res.Kinds)
	return KindStats{}
}
