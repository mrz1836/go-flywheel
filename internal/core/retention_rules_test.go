package core

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

// retentionNow is the retention-rule tests' clock reading.
//
//nolint:gochecknoglobals // a fixed instant the tests share
var retentionNow = time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)

// retentionDay is one day.
const retentionDay = 24 * time.Hour

// seedAged seeds a terminal job of kind, finalized age before retentionNow,
// with one run, and returns its id.
func seedAged(t testing.TB, db *gorm.DB, id, kind string, state JobState, age time.Duration) string {
	t.Helper()
	at := retentionNow.Add(-age)
	seedJob(t, db, jobRow{
		ID: id, Kind: kind, State: string(state), FinalizedAt: &at, CreatedAt: at, UpdatedAt: at, ScheduledAt: at,
	})
	seedRun(t, db, jobRunRow{
		ID: "run-" + id, JobID: id, Attempt: 1, ExecutorID: "h", StartedAt: at, Outcome: string(OutcomeSuccess),
		CreatedAt: at,
	})
	return id
}

// pruneAt runs one retention pass at retentionNow with a 30-day default and
// opts, and returns how many jobs it deleted.
func pruneAt(t *testing.T, db *gorm.DB, opts RetentionOpts) int64 {
	t.Helper()
	n, err := DeleteFinishedJobsWithOptions(fixedClockCtx(retentionNow), db, retentionNow.Add(-30*retentionDay), opts)
	require.NoError(t, err)
	return n
}

// assertJobsLeft asserts each of kept is still there and each of gone is
// deleted with its run.
func assertJobsLeft(t *testing.T, db *gorm.DB, kept, gone []string) {
	t.Helper()
	for _, id := range kept {
		assert.True(t, jobExists(t, db, id), "%s is kept", id)
	}
	for _, id := range gone {
		assert.False(t, jobExists(t, db, id), "%s is deleted", id)
		assert.Zero(t, runCount(t, db, id), "%s's run is deleted with it", id)
	}
}

// retentionRuleKeepsDiscardedJobsLongerSuite proves a state rule against the
// default, through the database open returns: with a discarded rule of 90 days
// over a 30-day default, discarded jobs outlive the others; without rules,
// every finished job takes the default, as retention always has.
func retentionRuleKeepsDiscardedJobsLongerSuite(t *testing.T, open func(t *testing.T) *gorm.DB) {
	t.Helper()
	seed := func(t *testing.T) *gorm.DB {
		t.Helper()
		db := open(t)
		seedAged(t, db, "succeeded-40d", "k", StateSucceeded, 40*retentionDay)
		seedAged(t, db, "cancelled-40d", "k", StateCancelled, 40*retentionDay)
		seedAged(t, db, "discarded-40d", "k", StateDiscarded, 40*retentionDay)
		seedAged(t, db, "discarded-100d", "k", StateDiscarded, 100*retentionDay)
		seedAged(t, db, "succeeded-10d", "k", StateSucceeded, 10*retentionDay)
		return db
	}

	t.Run("a discarded rule keeps discarded jobs longer", func(t *testing.T) {
		t.Parallel()
		db := seed(t)
		n := pruneAt(t, db, RetentionOpts{Rules: []RetentionRule{{State: StateDiscarded, MaxAge: 90 * retentionDay}}})
		assert.EqualValues(t, 3, n)
		assertJobsLeft(t, db,
			[]string{"discarded-40d", "succeeded-10d"},
			[]string{"succeeded-40d", "cancelled-40d", "discarded-100d"})
	})

	t.Run("without rules every finished job takes the default", func(t *testing.T) {
		t.Parallel()
		db := seed(t)
		assert.EqualValues(t, 4, pruneAt(t, db, RetentionOpts{}))
		assertJobsLeft(t, db,
			[]string{"succeeded-10d"},
			[]string{"succeeded-40d", "cancelled-40d", "discarded-40d", "discarded-100d"})
	})
}

// TestARetentionRuleKeepsDiscardedJobsLonger runs the suite on SQLite.
func TestARetentionRuleKeepsDiscardedJobsLonger(t *testing.T) {
	t.Parallel()
	retentionRuleKeepsDiscardedJobsLongerSuite(t, func(t *testing.T) *gorm.DB {
		t.Helper()
		return newDB(t)
	})
}

// TestTheMostSpecificRetentionRuleWins proves which rule decides a job: one
// naming its kind and state, then one naming its kind, then one naming its
// state, else the default, whatever order the rules are given in.
func TestTheMostSpecificRetentionRuleWins(t *testing.T) {
	t.Parallel()

	t.Run("kind and state, then kind, then state, then the default", func(t *testing.T) {
		t.Parallel()
		db := newDB(t)
		seedAged(t, db, "k-discarded-10d", "k", StateDiscarded, 10*retentionDay)
		seedAged(t, db, "k-succeeded-50d", "k", StateSucceeded, 50*retentionDay)
		seedAged(t, db, "k-succeeded-70d", "k", StateSucceeded, 70*retentionDay)
		seedAged(t, db, "other-discarded-50d", "other", StateDiscarded, 50*retentionDay)
		seedAged(t, db, "other-discarded-100d", "other", StateDiscarded, 100*retentionDay)
		seedAged(t, db, "other-succeeded-40d", "other", StateSucceeded, 40*retentionDay)
		seedAged(t, db, "other-succeeded-20d", "other", StateSucceeded, 20*retentionDay)

		// Given least specific first: the order given decides nothing.
		n := pruneAt(t, db, RetentionOpts{Rules: []RetentionRule{
			{State: StateDiscarded, MaxAge: 90 * retentionDay},
			{Kind: "k", MaxAge: 60 * retentionDay},
			{Kind: "k", State: StateDiscarded, MaxAge: 7 * retentionDay},
		}})
		assert.EqualValues(t, 4, n)
		assertJobsLeft(t, db,
			[]string{"k-succeeded-50d", "other-discarded-50d", "other-succeeded-20d"},
			[]string{"k-discarded-10d", "k-succeeded-70d", "other-discarded-100d", "other-succeeded-40d"})
	})

	t.Run("a kind rule beats a state rule", func(t *testing.T) {
		t.Parallel()
		db := newDB(t)
		seedAged(t, db, "noisy-discarded-70d", "noisy", StateDiscarded, 70*retentionDay)
		seedAged(t, db, "other-discarded-70d", "other", StateDiscarded, 70*retentionDay)

		n := pruneAt(t, db, RetentionOpts{Rules: []RetentionRule{
			{State: StateDiscarded, MaxAge: 90 * retentionDay},
			{Kind: "noisy", MaxAge: 60 * retentionDay},
		}})
		assert.EqualValues(t, 1, n)
		assertJobsLeft(t, db, []string{"other-discarded-70d"}, []string{"noisy-discarded-70d"})
	})
}

// TestRetentionRulesAreValidatedBeforeAnyDelete proves a bad rule is refused,
// naming it, before anything is deleted.
func TestRetentionRulesAreValidatedBeforeAnyDelete(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name  string
		rules []RetentionRule
		field string
	}{
		{"a live state", []RetentionRule{{State: StateRunning, MaxAge: retentionDay}}, "rules[0].state"},
		{"an unknown state", []RetentionRule{{State: "finished", MaxAge: retentionDay}}, "rules[0].state"},
		{"a zero max age", []RetentionRule{{Kind: "k", MaxAge: 0}}, "rules[0].max_age"},
		{"a negative max age", []RetentionRule{{Kind: "k", MaxAge: -retentionDay}}, "rules[0].max_age"},
		{"neither a kind nor a state", []RetentionRule{{MaxAge: retentionDay}}, "rules[0]"},
		{"a repeated kind", []RetentionRule{
			{Kind: "k", MaxAge: retentionDay}, {Kind: "k", MaxAge: 2 * retentionDay},
		}, "rules[1]"},
		{"a repeated kind and state", []RetentionRule{
			{State: StateDiscarded, MaxAge: 90 * retentionDay},
			{Kind: "k", State: StateDiscarded, MaxAge: retentionDay},
			{Kind: "k", State: StateDiscarded, MaxAge: 2 * retentionDay},
		}, "rules[2]"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			db := newDB(t)
			seedAged(t, db, "old", "k", StateSucceeded, 100*retentionDay)
			n, err := DeleteFinishedJobsWithOptions(fixedClockCtx(retentionNow), db,
				retentionNow.Add(-30*retentionDay), RetentionOpts{Rules: tc.rules})
			var verr *ValidationError
			require.ErrorAs(t, err, &verr)
			assert.Equal(t, tc.field, verr.Field)
			assert.Zero(t, n)
			assert.True(t, jobExists(t, db, "old"), "nothing is deleted")
		})
	}
}

// writeStatsWatermark records that the stats rollup covers the hours up to
// to, from long before it, as a rollup pass would.
func writeStatsWatermark(t *testing.T, db *gorm.DB, to time.Time) {
	t.Helper()
	from := to.Add(-365 * retentionDay).Unix()
	require.NoError(t, db.Create(&jobStatsProgressRow{
		ID: statsProgressID, CoveredFromUnix: from, CoveredToUnix: to.Unix(), RetainFromUnix: from,
		Version: 1, UpdatedAt: retentionNow,
	}).Error)
}

// TestTheStatsHoldCapsEveryRuleCutoff proves the stats-rollup hold binds every
// rule, as it binds the default: a rule shorter than the hold deletes nothing
// the rollup has yet to count.
func TestTheStatsHoldCapsEveryRuleCutoff(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	writeStatsWatermark(t, db, retentionNow.Add(-50*retentionDay).Truncate(time.Hour))
	seedAged(t, db, "discarded-20d", "k", StateDiscarded, 20*retentionDay)
	seedAged(t, db, "discarded-60d", "k", StateDiscarded, 60*retentionDay)
	seedAged(t, db, "succeeded-40d", "k", StateSucceeded, 40*retentionDay)
	seedAged(t, db, "succeeded-55d", "k", StateSucceeded, 55*retentionDay)

	n := pruneAt(t, db, RetentionOpts{
		HoldForStatsRollup: true,
		Rules:              []RetentionRule{{State: StateDiscarded, MaxAge: 7 * retentionDay}},
	})
	assert.EqualValues(t, 2, n)
	assertJobsLeft(t, db,
		[]string{"discarded-20d", "succeeded-40d"},
		[]string{"discarded-60d", "succeeded-55d"})
}

// TestNewSchedulerRefusesBadRetentionRules proves a Scheduler checks its rules
// when it is built: rules without RetentionMaxAge, which turns retention on, a
// rule retention itself would refuse, and, with the stats rollup on, a rule
// shorter than one hour plus StatsRollupGrace, as RetentionMaxAge is.
func TestNewSchedulerRefusesBadRetentionRules(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	build := func(cfg SchedulerConfig) error {
		cfg.DB, cfg.Client, cfg.Driver = db, NewClient(db), NewSQLiteDriver(db)
		_, err := NewSchedulerWithConfig(cfg)
		return err
	}
	discarded := []RetentionRule{{State: StateDiscarded, MaxAge: 90 * retentionDay}}

	err := build(SchedulerConfig{RetentionRules: discarded})
	require.ErrorIs(t, err, ErrValidation, "rules without RetentionMaxAge")
	assert.Contains(t, err.Error(), "RetentionRules need RetentionMaxAge")

	err = build(SchedulerConfig{
		RetentionMaxAge: 30 * retentionDay, RetentionRules: []RetentionRule{{State: StateRunning, MaxAge: retentionDay}},
	})
	var verr *ValidationError
	require.ErrorAs(t, err, &verr, "a rule retention would refuse")
	assert.Equal(t, "rules[0].state", verr.Field)
	assert.Contains(t, err.Error(), "jobs: scheduler config:")

	err = build(SchedulerConfig{
		RetentionMaxAge: 30 * retentionDay, StatsRollupInterval: time.Minute,
		RetentionRules: []RetentionRule{discarded[0], {Kind: "noisy", MaxAge: time.Hour}},
	})
	require.ErrorIs(t, err, ErrValidation, "a rule the rollup could not count before it deletes")
	assert.Contains(t, err.Error(), "RetentionRules[1]")

	require.NoError(t, build(SchedulerConfig{
		RetentionMaxAge: 30 * retentionDay, StatsRollupInterval: time.Minute,
		RetentionRules: []RetentionRule{discarded[0], {Kind: "noisy", MaxAge: 2 * time.Hour}},
	}))
	require.NoError(t, build(SchedulerConfig{
		RetentionMaxAge: 30 * retentionDay, RetentionRules: []RetentionRule{{Kind: "noisy", MaxAge: time.Minute}},
	}), "without the rollup a short rule is the host's call")
}

// TestSchedulerPrunesByRetentionRule proves a Scheduler's retention pass applies
// its rules beside RetentionMaxAge.
func TestSchedulerPrunesByRetentionRule(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	seedAged(t, db, "succeeded-40d", "k", StateSucceeded, 40*retentionDay)
	seedAged(t, db, "discarded-40d", "k", StateDiscarded, 40*retentionDay)
	seedAged(t, db, "discarded-100d", "k", StateDiscarded, 100*retentionDay)
	s := newSchedulerCfg(t, SchedulerConfig{
		DB: db, Client: NewClient(db), RetentionMaxAge: 30 * retentionDay,
		RetentionRules: []RetentionRule{{State: StateDiscarded, MaxAge: 90 * retentionDay}},
	})

	n, err := s.PruneRetention(fixedClockCtx(retentionNow))
	require.NoError(t, err)
	assert.EqualValues(t, 2, n)
	assertJobsLeft(t, db, []string{"discarded-40d"}, []string{"succeeded-40d", "discarded-100d"})
}
