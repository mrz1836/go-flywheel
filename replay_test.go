package flywheel

import (
	"context"
	"fmt"
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
