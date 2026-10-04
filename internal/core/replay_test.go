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

	"github.com/mrz1836/go-foundation/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/datatypes"
	"gorm.io/gorm"
)

// seedTerminalChildrenBulk bulk-inserts count children of parent in the given
// terminal state, each finalized at fin and sitting at attempt == max_attempts ==
// maxAttempts — the shape a discarded-at-the-ceiling cohort has, so a replay test
// can assert the budget reset as well as the accounting.
func seedTerminalChildrenBulk(
	t testing.TB, db *gorm.DB, parent string, count int, state JobState, maxAttempts int, fin time.Time,
) {
	t.Helper()
	rows := make([]jobRow, count)
	p := parent
	f := fin
	for i := range rows {
		rows[i] = jobRow{
			ID: fmt.Sprintf("%s-%s-%d", parent, state, i), Kind: "leaf",
			State: string(state), ParentJobID: &p, Args: datatypes.JSON("{}"),
			ScheduledAt: fin, Attempt: maxAttempts, MaxAttempts: maxAttempts, FinalizedAt: &f,
		}
	}
	require.NoError(t, db.CreateInBatches(&rows, 200).Error)
}

// TestReplayGuardsSucceededWithoutForce is A3: naming StateSucceeded without Force
// returns ErrJobTerminal and changes nothing — a bulk replay must never re-run
// succeeded work by accident.
func TestReplayGuardsSucceededWithoutForce(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	ctx := context.Background()
	base := time.Now().UTC().Truncate(time.Second)

	seedTerminalChildrenBulk(t, db, "P", 5, StateSucceeded, 3, base)

	res, err := ReplayByParent(ctx, db, "P", ReplayOpts{States: []JobState{StateSucceeded}})
	require.ErrorIs(t, err, ErrJobTerminal)
	assert.Equal(t, ScopeResult{}, res, "a refused replay reports a zero result")
	assert.EqualValues(t, 5, countChildrenInState(t, db, "P", StateSucceeded), "the refused replay changed nothing")
}

// TestReplayGuardsSucceededWithForce proves the escape hatch: with Force, naming
// StateSucceeded replays succeeded work deliberately.
func TestReplayGuardsSucceededWithForce(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	ctx := context.Background()
	base := time.Now().UTC().Truncate(time.Second)

	seedTerminalChildrenBulk(t, db, "P", 5, StateSucceeded, 3, base)

	res, err := ReplayByParent(ctx, db, "P", ReplayOpts{
		RetryOpts: RetryOpts{Force: true}, States: []JobState{StateSucceeded},
	})
	require.NoError(t, err)
	assert.EqualValues(t, 5, res.Changed)
	assert.EqualValues(t, 5, countChildrenInState(t, db, "P", StateAvailable), "Force replays succeeded work")
}

// TestReplayUnboundedIsRefused is A3: an unscoped Replay with neither Kinds nor
// FailedSince returns ErrReplayUnbounded and changes nothing.
func TestReplayUnboundedIsRefused(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	ctx := context.Background()
	seedTerminalChildrenBulk(t, db, "P", 5, StateDiscarded, 3, time.Now().UTC())

	res, err := Replay(ctx, db, ReplayOpts{})
	require.ErrorIs(t, err, ErrReplayUnbounded)
	assert.Equal(t, ScopeResult{}, res)
	assert.EqualValues(t, 5, countChildrenInState(t, db, "P", StateDiscarded), "the refused replay changed nothing")
}

// TestReplayByParentAccountsForEveryFinishedOrRunningChild is A4: over a mixed
// cohort, ScopeResult accounts for every finished-or-finishing child. It also pins
// the default (discarded only) and the budget reset.
func TestReplayByParentAccountsForEveryFinishedOrRunningChild(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	base := time.Now().UTC().Truncate(time.Second)
	fin := base.Add(-time.Hour)
	ctx := models.WithClock(context.Background(), models.NewFixedClock(base))

	p := "P"
	seedTerminalChildrenBulk(t, db, p, 300, StateDiscarded, 3, fin)
	seedTerminalChildrenBulk(t, db, p, 50, StateSucceeded, 3, fin)
	seedTerminalChildrenBulk(t, db, p, 20, StateCancelled, 3, fin)
	seedChildrenBulk(t, db, p, 10, StateRunning, base)
	seedChildrenBulk(t, db, p, 15, StateAvailable, base) // neither terminal nor running

	res, err := ReplayByParent(ctx, db, p, ReplayOpts{RetryOpts: RetryOpts{ResetAttempts: true}})
	require.NoError(t, err)

	assert.EqualValues(t, 300, res.Changed, "only the discarded children are replayed by default")
	assert.EqualValues(t, 70, res.SkippedTerminal, "succeeded + cancelled are terminal but untargeted")
	assert.EqualValues(t, 10, res.SkippedRunning, "a running attempt is never interrupted")

	// A4: the buckets account for every child that was terminal or running.
	universe := int64(300 + 50 + 20 + 10)
	assert.EqualValues(t, universe, res.Changed+res.SkippedTerminal+res.SkippedRunning,
		"Changed + SkippedTerminal + SkippedRunning == the finished-or-running universe")

	assert.EqualValues(t, 315, countChildrenInState(t, db, p, StateAvailable),
		"the 300 replayed children join the 15 already available")
	assert.EqualValues(t, 0, countChildrenInState(t, db, p, StateDiscarded))
	assert.EqualValues(t, 50, countChildrenInState(t, db, p, StateSucceeded), "no succeeded child re-ran")

	row := jobRowByID(t, db, "P-discarded-0")
	assert.Equal(t, 3, row.Attempt, "attempt is not rewound")
	assert.Equal(t, 6, row.MaxAttempts, "the budget is restored as headroom (attempt 3 + old max 3)")
	assert.Nil(t, row.FinalizedAt, "the discard's finalization is cleared")
}

// TestReplayByParentDefaultsToDiscardedOnly proves the empty-States default is not
// "everything": a succeeded sibling is left alone with no Force in sight.
func TestReplayByParentDefaultsToDiscardedOnly(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	ctx := context.Background()
	base := time.Now().UTC().Truncate(time.Second)

	p := "P"
	seedTerminalChildrenBulk(t, db, p, 4, StateDiscarded, 3, base)
	seedTerminalChildrenBulk(t, db, p, 6, StateSucceeded, 3, base)

	res, err := ReplayByParent(ctx, db, p, ReplayOpts{})
	require.NoError(t, err)
	assert.EqualValues(t, 4, res.Changed, "the empty default replays discarded jobs only")
	assert.EqualValues(t, 6, res.SkippedTerminal)
	assert.EqualValues(t, 6, countChildrenInState(t, db, p, StateSucceeded), "succeeded work is untouched")
}

// TestReplayFailedSinceWindowsTheCohort proves FailedSince narrows the replay to an
// incident window, leaving older failures discarded.
func TestReplayFailedSinceWindowsTheCohort(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	ctx := context.Background()
	base := time.Now().UTC().Truncate(time.Second)

	p := "P"
	old := base.Add(-2 * time.Hour)
	recent := base.Add(-time.Minute)
	for i := range 5 {
		f := old
		seedJob(t, db, jobRow{
			ID: fmt.Sprintf("old-%d", i), Kind: "leaf", ParentJobID: &p, State: string(StateDiscarded),
			Attempt: 3, MaxAttempts: 3, FinalizedAt: &f, ScheduledAt: base,
		})
	}
	for i := range 5 {
		f := recent
		seedJob(t, db, jobRow{
			ID: fmt.Sprintf("new-%d", i), Kind: "leaf", ParentJobID: &p, State: string(StateDiscarded),
			Attempt: 3, MaxAttempts: 3, FinalizedAt: &f, ScheduledAt: base,
		})
	}

	res, err := ReplayByParent(ctx, db, p, ReplayOpts{FailedSince: base.Add(-time.Hour)})
	require.NoError(t, err)
	assert.EqualValues(t, 5, res.Changed, "only the failures within the window are replayed")
	assert.EqualValues(t, 5, countChildrenInState(t, db, p, StateDiscarded), "older failures are left alone")
}

// TestReplayKindsBoundsAnUnscopedReplay proves Kinds is a valid bound for the
// lineage-unscoped Replay and restricts it to the named kinds.
func TestReplayKindsBoundsAnUnscopedReplay(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	ctx := context.Background()
	base := time.Now().UTC().Truncate(time.Second)

	for i := range 5 {
		f := base
		seedJob(t, db, jobRow{
			ID: fmt.Sprintf("alpha-%d", i), Kind: "alpha", State: string(StateDiscarded),
			Attempt: 3, MaxAttempts: 3, FinalizedAt: &f, ScheduledAt: base,
		})
		seedJob(t, db, jobRow{
			ID: fmt.Sprintf("beta-%d", i), Kind: "beta", State: string(StateDiscarded),
			Attempt: 3, MaxAttempts: 3, FinalizedAt: &f, ScheduledAt: base,
		})
	}

	res, err := Replay(ctx, db, ReplayOpts{Kinds: []string{"alpha"}})
	require.NoError(t, err)
	assert.EqualValues(t, 5, res.Changed, "only the alpha kind is replayed")

	var betaDiscarded int64
	require.NoError(t, db.Model(&jobRow{}).
		Where("kind = ? AND state = ?", "beta", string(StateDiscarded)).Count(&betaDiscarded).Error)
	assert.EqualValues(t, 5, betaDiscarded, "the beta kind is untouched")
}

// TestReplayBatchingBoundsEveryTransaction is the batching contract: with BatchSize
// B and N eligible children the replay uses ceil(N/B) batches.
func TestReplayBatchingBoundsEveryTransaction(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	ctx := context.Background()

	seedTerminalChildrenBulk(t, db, "P", 2500, StateDiscarded, 3, time.Now().UTC())
	res, err := ReplayByParent(ctx, db, "P", ReplayOpts{BatchSize: 1000})
	require.NoError(t, err)
	assert.EqualValues(t, 2500, res.Changed)
	assert.Equal(t, 3, res.Batches, "2500 children in batches of 1000 is three transactions")
}

func TestReplayBatchSizeDefaultsWhenNonPositive(t *testing.T) {
	t.Parallel()
	assert.Equal(t, defaultScopeBatchSize, ReplayOpts{}.batchSize())
	assert.Equal(t, defaultScopeBatchSize, ReplayOpts{BatchSize: -1}.batchSize())
	assert.Equal(t, 250, ReplayOpts{BatchSize: 250}.batchSize())
}

// TestReplayWithACancelledContextDoesNoWork mirrors the scoped controls' contract:
// a replay entered under a dead context opens no batch transaction, changes
// nothing, and names the progress made.
func TestReplayWithACancelledContextDoesNoWork(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	seedTerminalChildrenBulk(t, db, "P", 20, StateDiscarded, 3, time.Now().UTC())

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	res, err := ReplayByParent(ctx, db, "P", ReplayOpts{BatchSize: 5})
	require.ErrorIs(t, err, context.Canceled)
	assert.EqualValues(t, 0, res.Changed)
	assert.Contains(t, err.Error(), "cancelled after 0 changed", "the error names the progress made")
	assert.EqualValues(t, 20, countChildrenInState(t, db, "P", StateDiscarded), "nothing was touched")
}

func TestReplayByParentNoMatchIsZeroResult(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	res, err := ReplayByParent(context.Background(), db, "nobody", ReplayOpts{})
	require.NoError(t, err)
	assert.Equal(t, ScopeResult{}, res, "a parent with no matching children yields a zero result, not an error")
}

func TestReplayNilDBIsAnError(t *testing.T) {
	t.Parallel()
	_, err := ReplayByParent(context.Background(), nil, "P", ReplayOpts{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "db is nil")
}

// assertStaggerDeciles seeds a cohort of discarded children, replays it across a
// 10-minute window in several batches, and asserts each decile of that window holds
// exactly a tenth of the cohort. Because the placement is deterministic — job i of n
// lands at base + Stagger*i/n over a global index spanning batches — the assertion
// is on exact per-decile counts, not a distribution shape. It is shared by the
// SQLite and Postgres stagger tests so the parity claim is one body run twice.
func assertStaggerDeciles(t *testing.T, db *gorm.DB) {
	t.Helper()
	const (
		n      = 1000
		window = 10 * time.Minute
	)
	base := time.Now().UTC().Truncate(time.Second)
	ctx := models.WithClock(context.Background(), models.NewFixedClock(base))

	seedTerminalChildrenBulk(t, db, "P", n, StateDiscarded, 3, base.Add(-time.Hour))

	res, err := ReplayByParent(ctx, db, "P", ReplayOpts{
		RetryOpts: RetryOpts{ResetAttempts: true}, Stagger: window, BatchSize: 250,
	})
	require.NoError(t, err)
	require.EqualValues(t, n, res.Changed)
	require.Equal(t, 4, res.Batches, "a 1000-job cohort in batches of 250 spans four transactions")

	// Read every replayed child's scheduled_at and bucket it into one of ten deciles
	// of the window, measured from base (now, since Delay is zero). A global index
	// that restarted per batch would pile four cohorts into the first 2.5 minutes;
	// the flat decile counts are what prove it does not.
	var scheduled []time.Time
	require.NoError(t, db.Model(&jobRow{}).
		Where("parent_job_id = ?", "P").Pluck("scheduled_at", &scheduled).Error)
	require.Len(t, scheduled, n)

	deciles := make([]int, 10)
	bucket := window / 10
	for _, s := range scheduled {
		offset := s.Sub(base)
		require.GreaterOrEqual(t, offset, time.Duration(0), "no job lands before base")
		require.Less(t, offset, window, "no job lands at or after the full window")
		deciles[int(offset/bucket)]++
	}
	for d, count := range deciles {
		assert.Equalf(t, n/10, count, "decile %d holds exactly a tenth of the cohort", d)
	}
}

// TestStaggerDistributesUniformly is A5 on SQLite.
func TestStaggerDistributesUniformly(t *testing.T) {
	t.Parallel()
	assertStaggerDeciles(t, newDB(t))
}

// TestStaggerZeroLeavesTheCohortImmediatelyClaimable is the fast path: with no
// Stagger every replayed job lands at base (now + Delay), immediately claimable.
func TestStaggerZeroLeavesTheCohortImmediatelyClaimable(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	base := time.Now().UTC().Truncate(time.Second)
	ctx := models.WithClock(context.Background(), models.NewFixedClock(base))

	seedTerminalChildrenBulk(t, db, "P", 50, StateDiscarded, 3, base.Add(-time.Hour))
	_, err := ReplayByParent(ctx, db, "P", ReplayOpts{})
	require.NoError(t, err)

	var scheduled []time.Time
	require.NoError(t, db.Model(&jobRow{}).
		Where("parent_job_id = ?", "P").Pluck("scheduled_at", &scheduled).Error)
	for _, s := range scheduled {
		assert.True(t, s.Equal(base), "every job is immediately claimable at base")
	}
}

// --- replay error branches --------------------------------------------------

// TestReplaySurfacesErrors covers replay's guard and count-read failure exits: a
// nil db, a dead context on entry (no work, progress named), and a failed
// running-count read. ReplayByParent is used because Replay's own unbounded guard
// short-circuits before reaching the shared engine.
func TestReplaySurfacesErrors(t *testing.T) {
	t.Parallel()

	t.Run("a nil db is rejected", func(t *testing.T) {
		t.Parallel()
		_, err := ReplayByParent(context.Background(), nil, "P", ReplayOpts{})
		require.ErrorContains(t, err, "db is nil")
	})

	t.Run("a dead context does no work", func(t *testing.T) {
		t.Parallel()
		db := newDB(t)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_, err := ReplayByParent(ctx, db, "P", ReplayOpts{})
		require.ErrorIs(t, err, context.Canceled)
		require.ErrorContains(t, err, "cancelled after 0 changed", "the error names the progress made")
	})

	t.Run("a failed running-count read is surfaced", func(t *testing.T) {
		t.Parallel()
		db := newDB(t)
		closeDB(t, db)
		_, err := ReplayByParent(context.Background(), db, "P", ReplayOpts{})
		require.ErrorContains(t, err, "count running")
	})
}

// --- replay and the unique active key ---------------------------------------

// replayEntry is one replay entry point bound to the keyed-replay cohort: kind
// "leaf" under parent "P". Jobs seeded as kind "other" under "Q" sit outside it
// for both entry points, which is where a test puts a holder it must not replay.
type replayEntry struct {
	name string
	run  func(ctx context.Context, db *gorm.DB, opts ReplayOpts) (ScopeResult, error)
}

// replayEntries returns ReplayByParent scoped to P and Replay bounded to the leaf
// kind, so every keyed-replay case holds both to the same behavior.
func replayEntries() []replayEntry {
	return []replayEntry{
		{name: "ReplayByParent", run: func(ctx context.Context, db *gorm.DB, opts ReplayOpts) (ScopeResult, error) {
			return ReplayByParent(ctx, db, "P", opts)
		}},
		{name: "Replay", run: func(ctx context.Context, db *gorm.DB, opts ReplayOpts) (ScopeResult, error) {
			opts.Kinds = []string{"leaf"}
			return Replay(ctx, db, opts)
		}},
	}
}

// runKeyedReplayCase runs check once per entry point, each on a fresh database
// from open.
func runKeyedReplayCase(
	t *testing.T, open func(testing.TB) *gorm.DB, check func(*testing.T, *gorm.DB, replayEntry),
) {
	t.Helper()
	for _, e := range replayEntries() {
		t.Run(e.name, func(t *testing.T) {
			t.Parallel()
			check(t, open(t), e)
		})
	}
}

// seedKeyedJob writes one job of kind under parent in state, carrying key as its
// UniqueActiveKey (none when key is empty). A terminal job is finalized an hour
// ago at its attempt ceiling, the shape a discarded job has.
func seedKeyedJob(t testing.TB, db *gorm.DB, id, kind, parent string, state JobState, key string) {
	t.Helper()
	now := time.Now().UTC().Truncate(time.Second)
	p := parent
	row := jobRow{
		ID: id, Kind: kind, ParentJobID: &p, State: string(state),
		Attempt: 3, MaxAttempts: 3, ScheduledAt: now,
	}
	if key != "" {
		k := key
		row.UniqueActiveKey = &k
	}
	if slices.Contains(TerminalStates(), state) {
		fin := now.Add(-time.Hour)
		row.FinalizedAt = &fin
	}
	seedJob(t, db, row)
}

// assertReplayKeepsOneSiblingPerActiveKey replays two discarded siblings that
// share a UniqueActiveKey beside an unkeyed one. The index allows one live job
// per key, so the first sibling in id order goes live, the other stays
// discarded, and the replay completes.
func assertReplayKeepsOneSiblingPerActiveKey(t *testing.T, db *gorm.DB, e replayEntry) {
	t.Helper()
	seedKeyedJob(t, db, "P-1", "leaf", "P", StateDiscarded, "K")
	seedKeyedJob(t, db, "P-2", "leaf", "P", StateDiscarded, "K")
	seedKeyedJob(t, db, "P-3", "leaf", "P", StateDiscarded, "")

	res, err := e.run(context.Background(), db, ReplayOpts{})
	require.NoError(t, err, "the replay completes (result so far: %+v)", res)
	assert.EqualValues(t, 2, res.Changed, "one sibling per key goes live, and the unkeyed job")
	assert.EqualValues(t, 1, res.SkippedActiveKey, "the second sibling is skipped for its key")
	assert.Equal(t, 1, res.Batches)
	assert.Equal(t, string(StateAvailable), jobState(t, db, "P-1"), "the first sibling in id order goes live")
	assert.Equal(t, string(StateDiscarded), jobState(t, db, "P-2"), "its sibling stays terminal")
	assert.Equal(t, string(StateAvailable), jobState(t, db, "P-3"))
}

func TestReplayKeepsOneSiblingPerActiveKey(t *testing.T) {
	t.Parallel()
	runKeyedReplayCase(t, newDB, assertReplayKeepsOneSiblingPerActiveKey)
}

// assertReplaySkipsAJobWhoseActiveKeyIsHeld proves a live job outside the cohort
// blocks the replay of a terminal job carrying its key, in every live state the
// index names, and when that holder is soft-deleted — the index has no
// deleted_at condition. A terminal job holds nothing, so its key replays.
func assertReplaySkipsAJobWhoseActiveKeyIsHeld(t *testing.T, db *gorm.DB, e replayEntry) {
	t.Helper()
	for _, s := range NonTerminalStates() {
		seedKeyedJob(t, db, "Q-"+string(s), "other", "Q", s, "K-"+string(s))
		seedKeyedJob(t, db, "P-"+string(s), "leaf", "P", StateDiscarded, "K-"+string(s))
	}
	seedKeyedJob(t, db, "Q-deleted", "other", "Q", StateAvailable, "K-deleted")
	require.NoError(t, db.Delete(&jobRow{ID: "Q-deleted"}).Error)
	seedKeyedJob(t, db, "P-deleted", "leaf", "P", StateDiscarded, "K-deleted")
	seedKeyedJob(t, db, "Q-succeeded", "other", "Q", StateSucceeded, "K-free")
	seedKeyedJob(t, db, "P-free", "leaf", "P", StateDiscarded, "K-free")

	res, err := e.run(context.Background(), db, ReplayOpts{})
	require.NoError(t, err, "the replay completes (result so far: %+v)", res)
	assert.EqualValues(t, 1, res.Changed, "only the job whose key no live job holds goes live")
	assert.EqualValues(t, len(NonTerminalStates())+1, res.SkippedActiveKey, "one skip per held key")
	assert.Zero(t, res.SkippedRunning, "a holder outside the cohort is in no bucket")
	for _, s := range NonTerminalStates() {
		assert.Equal(t, string(StateDiscarded), jobState(t, db, "P-"+string(s)), "a %s holder blocks the replay", s)
		assert.Equal(t, string(s), jobState(t, db, "Q-"+string(s)), "the holder is untouched")
	}
	assert.Equal(t, string(StateDiscarded), jobState(t, db, "P-deleted"), "a soft-deleted live job still holds its key")
	assert.Equal(t, string(StateAvailable), jobState(t, db, "P-free"), "a terminal job holds no key")
}

func TestReplaySkipsAJobWhoseActiveKeyIsHeld(t *testing.T) {
	t.Parallel()
	runKeyedReplayCase(t, newDB, assertReplaySkipsAJobWhoseActiveKeyIsHeld)
}

// assertReplayOfALiveJobIsNotBlockedByItsOwnKey replays with States naming a
// live state. A selected retryable job already holds its key, and must not read
// as blocked by itself; the discarded job sharing that key is the one held.
func assertReplayOfALiveJobIsNotBlockedByItsOwnKey(t *testing.T, db *gorm.DB, e replayEntry) {
	t.Helper()
	seedKeyedJob(t, db, "P-1", "leaf", "P", StateDiscarded, "K")
	seedKeyedJob(t, db, "P-2", "leaf", "P", StateRetryable, "K")

	res, err := e.run(context.Background(), db, ReplayOpts{States: []JobState{StateDiscarded, StateRetryable}})
	require.NoError(t, err)
	assert.EqualValues(t, 1, res.Changed, "the retryable job goes to available on its own key")
	assert.EqualValues(t, 1, res.SkippedActiveKey, "the discarded job's key is held by the retryable one")
	assert.Equal(t, string(StateDiscarded), jobState(t, db, "P-1"))
	assert.Equal(t, string(StateAvailable), jobState(t, db, "P-2"))
}

func TestReplayOfALiveJobIsNotBlockedByItsOwnKey(t *testing.T) {
	t.Parallel()
	runKeyedReplayCase(t, newDB, assertReplayOfALiveJobIsNotBlockedByItsOwnKey)
}

// assertReplayKeepsOneJobPerActiveKeyAcrossBatches replays a cohort whose shared
// keys span transactions, one job per batch. A job replayed in an earlier batch
// holds its key once that batch commits, so a later job with the key stays
// discarded instead of failing the replay part-way.
func assertReplayKeepsOneJobPerActiveKeyAcrossBatches(t *testing.T, db *gorm.DB, e replayEntry) {
	t.Helper()
	for i, key := range []string{"K", "K", "", "K", "J", "J"} {
		seedKeyedJob(t, db, fmt.Sprintf("P-%d", i+1), "leaf", "P", StateDiscarded, key)
	}

	res, err := e.run(context.Background(), db, ReplayOpts{BatchSize: 1})
	require.NoError(t, err, "the replay completes (result so far: %+v)", res)
	assert.EqualValues(t, 3, res.Changed, "the first job per key, and the unkeyed one")
	assert.EqualValues(t, 3, res.SkippedActiveKey, "each later job with a replayed key is skipped")
	assert.Equal(t, 3, res.Batches, "a batch that only skipped changed no rows")
	for id, want := range map[string]JobState{
		"P-1": StateAvailable, "P-2": StateDiscarded, "P-3": StateAvailable,
		"P-4": StateDiscarded, "P-5": StateAvailable, "P-6": StateDiscarded,
	} {
		assert.Equal(t, string(want), jobState(t, db, id), id)
	}
}

func TestReplayKeepsOneJobPerActiveKeyAcrossBatches(t *testing.T) {
	t.Parallel()
	runKeyedReplayCase(t, newDB, assertReplayKeepsOneJobPerActiveKeyAcrossBatches)
}

// assertReplayAccountsForAKeyedCohort replays a mixed cohort in batches of four:
// unkeyed jobs, keys shared within the cohort and across its batches, keys a live
// job holds inside and outside it, untargeted terminal jobs, a running job, and
// pending ones. Every job that was terminal or running lands in exactly one
// bucket.
func assertReplayAccountsForAKeyedCohort(t *testing.T, db *gorm.DB, e replayEntry) {
	t.Helper()
	seed := func(id string, state JobState, key string) {
		seedKeyedJob(t, db, id, "leaf", "P", state, key)
	}
	for i := range 10 {
		seed(fmt.Sprintf("P-unkeyed-%02d", i), StateDiscarded, "")
	}
	// Three discarded jobs per key: the first in id order goes live, and the two
	// after it — one of them in the next batch — are skipped.
	for _, key := range []string{"a", "b", "c"} {
		for n := 1; n <= 3; n++ {
			seed(fmt.Sprintf("P-shared-%s-%d", key, n), StateDiscarded, "shared-"+key)
		}
	}
	seedKeyedJob(t, db, "Q-holder", "other", "Q", StateScheduled, "held-outside")
	seed("P-held-outside", StateDiscarded, "held-outside")
	seed("P-running", StateRunning, "held-running")
	seed("P-held-running", StateDiscarded, "held-running")
	seed("P-available", StateAvailable, "held-available")
	seed("P-held-available", StateDiscarded, "held-available")
	seed("P-pending", StateAvailable, "")
	for i := range 4 {
		seed(fmt.Sprintf("P-succeeded-%d", i), StateSucceeded, "shared-a") // a terminal job holds no key
	}
	for i := range 3 {
		seed(fmt.Sprintf("P-cancelled-%d", i), StateCancelled, "")
	}

	res, err := e.run(context.Background(), db, ReplayOpts{BatchSize: 4})
	require.NoError(t, err)
	assert.EqualValues(t, 13, res.Changed, "the 10 unkeyed jobs and the first job per shared key")
	assert.EqualValues(t, 9, res.SkippedActiveKey, "6 later jobs on a shared key, 3 on a key a live job holds")
	assert.EqualValues(t, 7, res.SkippedTerminal, "succeeded and cancelled are terminal but untargeted")
	assert.EqualValues(t, 1, res.SkippedRunning)
	assert.Equal(t, 6, res.Batches, "22 discarded jobs in batches of four")

	universe := int64(22 + 4 + 3 + 1)
	assert.Equal(t, universe, res.Changed+res.SkippedTerminal+res.SkippedRunning+res.SkippedActiveKey,
		"Changed + SkippedTerminal + SkippedRunning + SkippedActiveKey == the finished-or-running universe")

	for _, key := range []string{"a", "b", "c"} {
		assert.Equal(t, string(StateAvailable), jobState(t, db, "P-shared-"+key+"-1"))
		assert.Equal(t, string(StateDiscarded), jobState(t, db, "P-shared-"+key+"-2"))
		assert.Equal(t, string(StateDiscarded), jobState(t, db, "P-shared-"+key+"-3"))
	}
	assert.EqualValues(t, 9, countChildrenInState(t, db, "P", StateDiscarded))
	assert.EqualValues(t, 15, countChildrenInState(t, db, "P", StateAvailable), "13 replayed + 2 already available")
}

func TestReplayAccountsForAKeyedCohort(t *testing.T) {
	t.Parallel()
	runKeyedReplayCase(t, newDB, assertReplayAccountsForAKeyedCohort)
}

// assertReplayWindowsAKeyedCohort replays an incident window over keyed jobs, the
// shape of an incident recovery. A job finalized before the window is not
// targeted: it stays discarded, holds nothing, and is out of the accounting, even
// though it comes first in id order on a key the window's jobs share. Within the
// window the first job per key goes live, and a key a live job holds is skipped.
func assertReplayWindowsAKeyedCohort(t *testing.T, db *gorm.DB, e replayEntry) {
	t.Helper()
	now := time.Now().UTC().Truncate(time.Second)
	seedKeyedJob(t, db, "P-a0", "leaf", "P", StateDiscarded, "A")
	require.NoError(t, db.Model(&jobRow{}).Where("id = ?", "P-a0").
		Update("finalized_at", now.Add(-3*time.Hour)).Error)
	seedKeyedJob(t, db, "P-a1", "leaf", "P", StateDiscarded, "A")
	seedKeyedJob(t, db, "P-a2", "leaf", "P", StateDiscarded, "A")
	seedKeyedJob(t, db, "Q-holder", "other", "Q", StateRunning, "H")
	seedKeyedJob(t, db, "P-h", "leaf", "P", StateDiscarded, "H")
	seedKeyedJob(t, db, "P-s", "leaf", "P", StateSucceeded, "")
	seedKeyedJob(t, db, "P-u", "leaf", "P", StateDiscarded, "")

	res, err := e.run(context.Background(), db, ReplayOpts{FailedSince: now.Add(-2 * time.Hour)})
	require.NoError(t, err)
	assert.EqualValues(t, 2, res.Changed, "the first windowed job on A, and the unkeyed one")
	assert.EqualValues(t, 2, res.SkippedActiveKey, "the second windowed job on A, and the one whose key is held")
	assert.EqualValues(t, 1, res.SkippedTerminal, "the succeeded job")
	assert.Zero(t, res.SkippedRunning, "the holder is outside the cohort")
	inScope := int64(6) // P-a0, P-a1, P-a2, P-h, P-s, P-u
	assert.Equal(t, inScope-1, res.Changed+res.SkippedTerminal+res.SkippedRunning+res.SkippedActiveKey,
		"the sum leaves out only the targeted job finalized before the window")

	for id, want := range map[string]JobState{
		"P-a0": StateDiscarded, "P-a1": StateAvailable, "P-a2": StateDiscarded,
		"P-h": StateDiscarded, "P-u": StateAvailable,
	} {
		assert.Equal(t, string(want), jobState(t, db, id), id)
	}
}

func TestReplayWindowsAKeyedCohort(t *testing.T) {
	t.Parallel()
	runKeyedReplayCase(t, newDB, assertReplayWindowsAKeyedCohort)
}

// assertStaggerKeepsSlotsAcrossActiveKeySkips staggers a cohort in which every
// tenth job shares its key with the job before it. A skipped job keeps its slot
// unused, so every replayed job lands exactly where it would have without the
// skips — base + Stagger*i/n for its index i in the cohort — inside the window.
func assertStaggerKeepsSlotsAcrossActiveKeySkips(t *testing.T, db *gorm.DB, e replayEntry) {
	t.Helper()
	const (
		n      = 100
		window = 10 * time.Minute
	)
	base := time.Now().UTC().Truncate(time.Second)
	ctx := models.WithClock(context.Background(), models.NewFixedClock(base))
	for i := range n {
		key := fmt.Sprintf("key-%03d", i)
		if i%10 == 9 {
			key = fmt.Sprintf("key-%03d", i-1)
		}
		seedKeyedJob(t, db, fmt.Sprintf("P-%03d", i), "leaf", "P", StateDiscarded, key)
	}

	res, err := e.run(ctx, db, ReplayOpts{Stagger: window, BatchSize: 25})
	require.NoError(t, err)
	assert.EqualValues(t, n-10, res.Changed)
	assert.EqualValues(t, 10, res.SkippedActiveKey)

	var rows []jobRow
	require.NoError(t, db.Where("parent_job_id = ?", "P").Order("id").Find(&rows).Error)
	require.Len(t, rows, n)
	for i, row := range rows {
		if i%10 == 9 {
			assert.Equal(t, string(StateDiscarded), row.State, "%s shares a key with the job before it", row.ID)
			continue
		}
		require.Equal(t, string(StateAvailable), row.State, row.ID)
		want := base.Add(window * time.Duration(i) / n)
		assert.True(t, row.ScheduledAt.Equal(want), "%s lands in slot %d: got %s, want %s",
			row.ID, i, row.ScheduledAt, want)
	}
}

func TestStaggerKeepsSlotsAcrossActiveKeySkips(t *testing.T) {
	t.Parallel()
	runKeyedReplayCase(t, newDB, assertStaggerKeepsSlotsAcrossActiveKeySkips)
}

// assertReplayUpdateRechecksTheActiveKey takes a job's key between its batch's
// SELECT and UPDATE, the window a concurrent enqueue or forced retry has: just
// before the UPDATE runs, a terminal job carrying the key returns to available on
// the batch's own transaction. The UPDATE's guard reads the new holder and leaves
// the job terminal instead of failing the batch. The SELECT had read the key as
// free, so the job counts in no bucket, like any re-guard skip.
func assertReplayUpdateRechecksTheActiveKey(t *testing.T, db *gorm.DB) {
	t.Helper()
	seedKeyedJob(t, db, "Q-1", "other", "Q", StateSucceeded, "K")
	seedKeyedJob(t, db, "P-1", "leaf", "P", StateDiscarded, "K")
	seedKeyedJob(t, db, "P-2", "leaf", "P", StateDiscarded, "")

	var fired atomic.Bool
	require.NoError(t, db.Callback().Update().Before("gorm:update").Register("test:take_key",
		func(tx *gorm.DB) {
			if fired.Swap(true) {
				return
			}
			if _, err := tx.Statement.ConnPool.ExecContext(tx.Statement.Context,
				`UPDATE jobs SET state = 'available', finalized_at = NULL WHERE id = 'Q-1'`); err != nil {
				_ = tx.AddError(err)
			}
		}))

	res, err := ReplayByParent(context.Background(), db, "P", ReplayOpts{})
	require.NoError(t, err, "the guard skips the job rather than failing the batch")
	require.True(t, fired.Load())
	assert.EqualValues(t, 1, res.Changed, "only the unkeyed job goes live")
	assert.Zero(t, res.SkippedActiveKey, "the SELECT read the key as free")
	assert.Equal(t, string(StateDiscarded), jobState(t, db, "P-1"))
	assert.Equal(t, string(StateAvailable), jobState(t, db, "Q-1"), "the job that took the key keeps it")
}

func TestReplayUpdateRechecksTheActiveKey(t *testing.T) {
	t.Parallel()
	assertReplayUpdateRechecksTheActiveKey(t, newDB(t))
}

// loseKeyRaces makes each replay UPDATE on db whose 1-based attempt number lose
// reports fail as an UPDATE that loses a key race does: with a real unique
// violation of jobs_unique_active_key. Just before it, a statement on the batch's
// own transaction tries to put two jobs outside the cohort live on one key, and
// the UPDATE fails with the driver's error, or with gorm.ErrDuplicatedKey on a
// database opened with TranslateError, which GORM substitutes for any error it
// records. It returns a counter of the UPDATEs attempted.
func loseKeyRaces(t *testing.T, db *gorm.DB, lose func(attempt int32) bool) *atomic.Int32 {
	t.Helper()
	seedKeyedJob(t, db, "Q-race-1", "other", "Q", StateSucceeded, "race")
	seedKeyedJob(t, db, "Q-race-2", "other", "Q", StateSucceeded, "race")
	var updates atomic.Int32
	require.NoError(t, db.Callback().Update().Before("gorm:update").Register("test:lose_key_race",
		func(tx *gorm.DB) {
			if !lose(updates.Add(1)) {
				return
			}
			_, err := tx.Statement.ConnPool.ExecContext(tx.Statement.Context,
				`UPDATE jobs SET state = 'available' WHERE id IN ('Q-race-1', 'Q-race-2')`)
			if err == nil {
				err = errors.New("test: two live jobs on one key did not violate the index")
			}
			_ = tx.AddError(err)
		}))
	return &updates
}

// keyRaceDB opens a database a lost key race reaches the replay on, in one of
// two forms: the driver's own unique violation, or gorm.ErrDuplicatedKey when
// translated is set, because the handle was opened with TranslateError.
type keyRaceDB struct {
	name       string
	open       func(*testing.T) *gorm.DB
	translated bool
}

// keyRaceDBs returns both forms on SQLite.
func keyRaceDBs() []keyRaceDB {
	return []keyRaceDB{
		{name: "driver error", open: func(t *testing.T) *gorm.DB { return newDB(t) }},
		{name: "TranslateError", open: newTranslateErrorDB, translated: true},
	}
}

// assertReplayRetriesALostKeyRace proves a batch whose UPDATE lost a key race
// runs again: it rolled back whole, so the rerun counts each job once. It is
// shared by the SQLite and (integration) PostgreSQL suites.
func assertReplayRetriesALostKeyRace(t *testing.T, db *gorm.DB) {
	t.Helper()
	seedTerminalChildrenBulk(t, db, "P", 5, StateDiscarded, 3, time.Now().UTC())
	updates := loseKeyRaces(t, db, func(attempt int32) bool { return attempt == 1 })

	res, err := ReplayByParent(context.Background(), db, "P", ReplayOpts{})
	require.NoError(t, err)
	assert.EqualValues(t, 2, updates.Load(), "the batch ran again once")
	assert.EqualValues(t, 5, res.Changed)
	assert.Equal(t, 1, res.Batches)
	assert.EqualValues(t, 5, countChildrenInState(t, db, "P", StateAvailable))
}

// assertReplayStopsRetryingALostKeyRace proves the retry is bounded: when every
// rerun loses too, the replay returns the duplicate-key error after
// replayBatchRetries reruns, with nothing changed. translated says the handle
// has TranslateError, so the error is gorm.ErrDuplicatedKey rather than the
// driver's.
func assertReplayStopsRetryingALostKeyRace(t *testing.T, db *gorm.DB, translated bool) {
	t.Helper()
	seedTerminalChildrenBulk(t, db, "P", 5, StateDiscarded, 3, time.Now().UTC())
	updates := loseKeyRaces(t, db, func(int32) bool { return true })

	res, err := ReplayByParent(context.Background(), db, "P", ReplayOpts{})
	require.Error(t, err)
	assert.True(t, isDuplicateKey(err), "the last lost race is returned: %v", err)
	if translated {
		assert.ErrorIs(t, err, gorm.ErrDuplicatedKey, "TranslateError substituted GORM's sentinel")
	} else {
		assert.NotErrorIs(t, err, gorm.ErrDuplicatedKey, "the driver's own error")
	}
	assert.ErrorContains(t, err, "flywheel: replay by parent: ")
	assert.EqualValues(t, replayBatchRetries+1, updates.Load())
	assert.Equal(t, ScopeResult{}, res)
	assert.EqualValues(t, 5, countChildrenInState(t, db, "P", StateDiscarded))
}

func TestReplayRetriesABatchThatLosesAKeyRace(t *testing.T) {
	t.Parallel()
	for _, tc := range keyRaceDBs() {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assertReplayRetriesALostKeyRace(t, tc.open(t))
		})
	}
}

func TestReplayStopsRetryingAKeyRaceItKeepsLosing(t *testing.T) {
	t.Parallel()
	for _, tc := range keyRaceDBs() {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assertReplayStopsRetryingALostKeyRace(t, tc.open(t), tc.translated)
		})
	}
}

// sqlStateError carries a SQLSTATE the way pgconn.PgError does, without the
// driver.
type sqlStateError string

func (e sqlStateError) Error() string    { return "sqlstate " + string(e) }
func (e sqlStateError) SQLState() string { return string(e) }

// TestRetryableBatchErrorClassifiesTheLostRaces pins which batch failures a
// replay reruns: the duplicate key in either form, the deadlock, and the
// serialization failure, wrapped or not — and nothing else.
func TestRetryableBatchErrorClassifiesTheLostRaces(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		err  error
		want bool
	}{
		{"duplicate key, wrapped by the driver classifier", fmt.Errorf("x: %w", models.ErrDuplicateKey), true},
		{"TranslateError's duplicate key", gorm.ErrDuplicatedKey, true},
		{"deadlock", sqlStateError("40P01"), true},
		{"wrapped deadlock", fmt.Errorf("batch: %w", sqlStateError("40P01")), true},
		{"serialization failure", sqlStateError("40001"), true},
		{"foreign key violation", sqlStateError("23503"), false},
		{"lock timeout", sqlStateError("55P03"), false},
		{"cancelled", context.Canceled, false},
		{"plain error", errors.New("boom"), false},
	} {
		assert.Equal(t, tc.want, retryableBatchError(tc.err), tc.name)
	}
}

// TestReplayResetsTheRaceBudgetPerBatch proves the retry budget belongs to one
// batch. With BatchSize 1 the UPDATEs fail on attempts 1 to replayBatchRetries
// and on the one after the first batch succeeds: the first batch spends its whole
// budget and lands on its last rerun, and the second batch's failure is its first,
// so it reruns and the replay completes.
func TestReplayResetsTheRaceBudgetPerBatch(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	seedTerminalChildrenBulk(t, db, "P", 2, StateDiscarded, 3, time.Now().UTC())
	updates := loseKeyRaces(t, db, func(attempt int32) bool {
		return attempt <= replayBatchRetries || attempt == replayBatchRetries+2
	})

	res, err := ReplayByParent(context.Background(), db, "P", ReplayOpts{BatchSize: 1})
	require.NoError(t, err, "the second batch starts with a fresh budget")
	assert.EqualValues(t, replayBatchRetries+3, updates.Load(), "every failed UPDATE ran again")
	assert.EqualValues(t, 2, res.Changed)
	assert.Equal(t, 2, res.Batches)
}

// holderLookup is one statement a replay issued that carries the holder lookup,
// as GORM sent it, with its bound values.
type holderLookup struct {
	sql  string
	vars []any
}

// recordHolderLookups records every statement on db that carries the holder
// lookup — the batch's SELECT and its UPDATE — so a plan test explains exactly
// what the runtime runs rather than SQL written to resemble it.
func recordHolderLookups(t *testing.T, db *gorm.DB) *[]holderLookup {
	t.Helper()
	var issued []holderLookup
	record := func(tx *gorm.DB) {
		if sql := tx.Statement.SQL.String(); strings.Contains(sql, " AS holder ") {
			issued = append(issued, holderLookup{sql: sql, vars: slices.Clone(tx.Statement.Vars)})
		}
	}
	require.NoError(t, db.Callback().Query().After("gorm:query").Register("test:record_select", record))
	require.NoError(t, db.Callback().Update().After("gorm:update").Register("test:record_update", record))
	return &issued
}

// TestReplayHolderLookupReachesTheActiveKeyIndexSQLite proves the holder lookup
// in the replay's SELECT and in its UPDATE's guard probes jobs_unique_active_key
// instead of scanning jobs once per row. It explains the two statements a replay
// actually issues, recorded with their bound values. The literal state list in
// activeKeyHeld is what allows the probe: SQLite matches a partial index's WHERE
// literally, and with the states bound as parameters the same query scans.
func TestReplayHolderLookupReachesTheActiveKeyIndexSQLite(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	seedKeyedJob(t, db, "P-1", "leaf", "P", StateDiscarded, "K")
	issued := recordHolderLookups(t, db)

	_, err := ReplayByParent(context.Background(), db, "P", ReplayOpts{})
	require.NoError(t, err)
	require.Len(t, *issued, 2, "the batch's SELECT and its UPDATE carry the holder lookup")
	for _, st := range *issued {
		plan := sqlitePlan(t, db, st.sql, st.vars...)
		assert.Contains(t, plan, "holder USING INDEX jobs_unique_active_key", "%s\n%s", st.sql, plan)
		assert.NotContains(t, plan, "SCAN holder", "%s\n%s", st.sql, plan)
	}
}
