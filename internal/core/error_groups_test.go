package core

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

// storedFingerprintF is a fingerprint a run stores as written, standing in for
// one a worker's ErrorFingerprinter produced.
const storedFingerprintF = "0123456789abcdef"

// failRun is a failed attempt of kind at at: class and message stored, the
// fingerprint stored when fp is set, and the job discarded when discarded.
func failRun(id, kind string, at time.Time, class ErrorClass, message, fp string, discarded bool) histRun {
	r := histRun{
		ID: id, Kind: kind, Outcome: OutcomeError, JobState: StateRetryable, FinishedAt: at, DurationMs: 40, WaitMs: 5,
		ErrorClass: class, ErrorMessage: message, Fingerprint: fp,
	}
	if class == ErrorTimeout {
		r.Outcome = OutcomeTimeout
	}
	if discarded {
		r.JobState = StateDiscarded
	}
	return r
}

// errorGroupRows reads every stored error-group row in key order, rolled_at
// zeroed.
func errorGroupRows(t testing.TB, db *gorm.DB) []jobErrorGroupHourlyRow {
	t.Helper()
	var rows []jobErrorGroupHourlyRow
	require.NoError(t, db.Order("bucket_start_unix, kind, fingerprint").Find(&rows).Error)
	for i := range rows {
		rows[i].RolledAt = time.Time{}
	}
	return rows
}

// groupRow is the stored row a test expects.
func groupRow(
	hour time.Time, kind, fp string, class ErrorClass, failures, discarded int64, first, last, sample string,
) jobErrorGroupHourlyRow {
	return jobErrorGroupHourlyRow{
		BucketStartUnix: hour.Unix(), Kind: kind, Fingerprint: fp, ErrorClass: string(class),
		Failures: failures, Discarded: discarded, FirstRunID: &first, LastRunID: &last, SampleMessage: sample,
	}
}

// TestTheRollupWritesErrorGroupsWithTheStatsHour proves the rollup stores an
// hour's failures as its error groups: one row per kind and fingerprint — the
// stored one, or one computed from the message for a run that stores none, a
// legacy run's kind resolved through its job — counting failures and discards,
// naming the first and last failing runs and the last one's message, and
// leaving out superseded attempts, crashes, snoozes, and successes. The groups
// are written in the stats hour's transaction, so an hour whose write fails
// keeps neither, and they are pruned with the stats rows.
func TestTheRollupWritesErrorGroupsWithTheStatsHour(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	h, h2, old := hourAt(rollupBase, 1), hourAt(rollupBase, 2), hourAt(rollupBase, -72)
	at := func(m int) time.Time { return h.Add(time.Duration(m) * time.Minute) }
	legacy := failRun("eg-b1", "beta", at(25), ErrorPermanent, "bad row 7", "", true)
	legacy.Legacy = true
	superseded := failRun("eg-a9", "alpha", at(35), ErrorTransient, "upstream returned 503 for item 9", storedFingerprintF, false)
	superseded.Superseded, superseded.JobState = true, ""
	snoozed := histRun{
		Kind: "alpha", Outcome: OutcomeSnooze, JobState: StateScheduled, FinishedAt: at(50), DurationMs: 5,
		WaitMs: 5, ErrorMessage: "deferred 3",
	}
	crashed := histRun{Kind: "alpha", Outcome: OutcomeCrashed, JobState: StateAvailable, FinishedAt: at(40), DurationMs: -1, WaitMs: 5}
	seedHistory(t, db, []histRun{
		failRun("eg-a1", "alpha", at(10), ErrorTransient, "upstream returned 503 for item 1", storedFingerprintF, false),
		failRun("eg-a2", "alpha", at(20), ErrorTransient, "upstream returned 503 for item 2", storedFingerprintF, true),
		failRun("eg-a3", "alpha", at(30), ErrorTransient, "upstream returned 503 for item 3", storedFingerprintF, false),
		failRun("eg-a4", "alpha", at(15), ErrorTimeout, "deadline after 30s", "", false),
		legacy, superseded, snoozed, crashed, okRun("alpha", at(45), 100),
		failRun("eg-h2", "alpha", h2.Add(5*time.Minute), ErrorTransient, "upstream returned 502 for item 4", "", false),
		failRun("eg-old", "alpha", old.Add(5*time.Minute), ErrorTransient, "upstream returned 500", "", false),
	})
	now := hourAt(rollupBase, 3).Add(10 * time.Minute)
	ctx := fixedClockCtx(now)

	_, err := rollupHour(ctx, db, h, now, false, coverageStep{})
	require.NoError(t, err)
	want := []jobErrorGroupHourlyRow{
		groupRow(h, "alpha", storedFingerprintF, ErrorTransient, 3, 1, "eg-a1", "eg-a3", "upstream returned 503 for item 3"),
		groupRow(h, "alpha", ErrorFingerprint("alpha", ErrorTimeout, "deadline after 30s"), ErrorTimeout, 1, 0,
			"eg-a4", "eg-a4", "deadline after 30s"),
		groupRow(h, "beta", ErrorFingerprint("beta", ErrorPermanent, "bad row 7"), ErrorPermanent, 1, 1,
			"eg-b1", "eg-b1", "bad row 7"),
	}
	sort.Slice(want, func(i, j int) bool {
		if want[i].Kind != want[j].Kind {
			return want[i].Kind < want[j].Kind
		}
		return want[i].Fingerprint < want[j].Fingerprint
	})
	assert.Equal(t, want, errorGroupRows(t, db), "the hour's failures, grouped")

	// A failed write rolls back the whole hour: the stats rows are inserted first,
	// and a refused group insert takes them with it.
	require.NoError(t, db.Exec(`CREATE TRIGGER refuse_error_groups BEFORE INSERT ON job_error_groups_hourly `+
		`BEGIN SELECT RAISE(ABORT, 'refused'); END`).Error)
	_, err = rollupHour(ctx, db, h2, now, false, coverageStep{})
	require.Error(t, err, "the group insert is refused")
	assert.Empty(t, hourStatsRows(t, db, h2), "the hour keeps no stats rows")
	require.NoError(t, db.Exec(`DROP TRIGGER refuse_error_groups`).Error)
	assert.Len(t, errorGroupRows(t, db), 3, "and no group rows")

	rollupAll(t, db, now)
	var oldGroups int64
	require.NoError(t, db.Model(&jobErrorGroupHourlyRow{}).Where("bucket_start_unix = ?", old.Unix()).
		Count(&oldGroups).Error)
	require.EqualValues(t, 1, oldGroups, "an old hour inside the retention has its group")
	oldStats := int64(len(hourStatsRows(t, db, old)))
	require.Positive(t, oldStats)

	res, err := rollupPass(ctx, db, now, rollupConfig{
		grace: defaultStatsRollupGrace, maxHours: 100_000, retention: 48 * time.Hour,
	})
	require.NoError(t, err)
	assert.Equal(t, oldStats+1, res.Pruned, "the prune counts both tables' rows")
	assert.Empty(t, hourStatsRows(t, db, old))
	for _, r := range errorGroupRows(t, db) {
		assert.NotEqual(t, old.Unix(), r.BucketStartUnix, "the old hour's group is pruned with its stats")
	}
}

// onlyGroup reads a window's error groups and returns the one group, failing
// the test when there is not exactly one.
func onlyGroup(t *testing.T, db *gorm.DB, p ErrorGroupsParams) ErrorGroup {
	t.Helper()
	res, err := ErrorGroups(context.Background(), db, p)
	require.NoError(t, err)
	require.Len(t, res.Groups, 1)
	return res.Groups[0]
}

// TestErrorGroupMessageIsTheNormalizedSample proves a group's sample is its
// latest failure's message as stored, cut at 512 bytes, and its Message that
// sample normalized — whether the latest failure is read raw or from a rolled
// hour.
func TestErrorGroupMessageIsTheNormalizedSample(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	h0, h1, h3 := hourAt(rollupBase, 0), hourAt(rollupBase, 1), hourAt(rollupBase, 3)
	long := "upstream returned 503 for item 42: " + strings.Repeat("payload ", 80)
	require.Greater(t, len(long), maxSampleMessage)
	seedHistory(t, db, []histRun{
		failRun("ms-a1", "alpha", h0.Add(5*time.Minute), ErrorTransient, "upstream returned 500 for item 1", storedFingerprintF, false),
		failRun("ms-a2", "alpha", h3.Add(5*time.Minute), ErrorTransient, long, storedFingerprintF, false),
		failRun("ms-b1", "beta", h0.Add(6*time.Minute), ErrorPermanent, "row 9 rejected", storedFingerprintF, false),
		failRun("ms-b2", "beta", h1.Add(7*time.Minute), ErrorPermanent, `row 10 rejected: field "amount"`, storedFingerprintF, false),
	})
	rollupAll(t, db, hourAt(rollupBase, 2).Add(10*time.Minute))
	window := ErrorGroupsParams{From: h0, To: h3.Add(time.Hour)}

	window.Kind = "alpha"
	raw := onlyGroup(t, db, window)
	assert.Equal(t, truncate(long, maxSampleMessage), raw.SampleMessage, "the latest message, read raw, cut at 512 bytes")
	assert.Equal(t, NormalizeErrorMessage(raw.SampleMessage), raw.Message)
	assert.True(t, strings.HasPrefix(raw.Message, "upstream returned <n> for item <n>: payload payload"),
		"the sample with its values masked: %q", raw.Message)
	assert.LessOrEqual(t, len(raw.Message), maxSampleMessage)

	window.Kind = "beta"
	rolled := onlyGroup(t, db, window)
	assert.Equal(t, `row 10 rejected: field "amount"`, rolled.SampleMessage, "the latest message, from a rolled hour")
	assert.Equal(t, "row <n> rejected: field <q>", rolled.Message)
}

// TestErrorGroupsCountOnlyFinalizedFailures proves a group counts the failures
// whose outcome was applied — errors and timeouts not superseded — across
// rolled hours and the raw tail, with its discards, its retries, and its
// latest failure's class and message; superseded attempts, the sweep's
// crashes, snoozes that carry an error, and successes are no failures. So a
// group never counts more than the kind's error and timeout counters.
func TestErrorGroupsCountOnlyFinalizedFailures(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	var runs []histRun
	n := 0
	fail := func(hour, minute int, discarded bool) {
		n++
		runs = append(runs, failRun(fmt.Sprintf("cf-%02d", n), "alpha", hourAt(rollupBase, hour).Add(time.Duration(minute)*time.Minute),
			ErrorTransient, fmt.Sprintf("upstream returned 503 for item %d", n), storedFingerprintF, discarded))
	}
	fail(0, 5, true)
	fail(0, 10, false)
	fail(1, 5, false)
	fail(1, 10, false)
	fail(2, 5, true)
	fail(2, 10, false)
	fail(4, 5, false)
	fail(4, 10, true)
	fail(4, 15, false)
	fail(4, 20, false)
	superseded := failRun("cf-sup", "alpha", hourAt(rollupBase, 1).Add(20*time.Minute), ErrorTransient,
		"upstream returned 503 for item 99", storedFingerprintF, false)
	superseded.Superseded, superseded.JobState = true, ""
	lateTimeout := failRun("cf-sup2", "alpha", hourAt(rollupBase, 4).Add(30*time.Minute), ErrorTimeout,
		"upstream returned 503 for item 98", storedFingerprintF, false)
	lateTimeout.Superseded, lateTimeout.JobState = true, ""
	runs = append(runs, superseded, lateTimeout,
		histRun{
			Kind: "alpha", Outcome: OutcomeCrashed, JobState: StateAvailable, FinishedAt: hourAt(rollupBase, 2).Add(20 * time.Minute),
			DurationMs: -1, WaitMs: 5,
		},
		histRun{
			Kind: "alpha", Outcome: OutcomeSnooze, JobState: StateScheduled, FinishedAt: hourAt(rollupBase, 4).Add(25 * time.Minute),
			DurationMs: 5, WaitMs: 5, ErrorMessage: "upstream returned 503 for item 97", Fingerprint: storedFingerprintF,
		},
		okRun("alpha", hourAt(rollupBase, 4).Add(26*time.Minute), 100))
	seedHistory(t, db, runs)
	rollupAll(t, db, hourAt(rollupBase, 4).Add(10*time.Minute))
	window := ErrorGroupsParams{From: hourAt(rollupBase, 0), To: hourAt(rollupBase, 5)}

	res, err := ErrorGroups(context.Background(), db, window)
	require.NoError(t, err)
	require.Len(t, res.Groups, 1)
	g := res.Groups[0]
	assert.Equal(t, "alpha", g.Kind)
	assert.Equal(t, storedFingerprintF, g.Fingerprint)
	assert.Equal(t, string(ErrorTransient), g.ErrorClass)
	assert.EqualValues(t, 10, g.Failures, "the superseded attempts, the crash, the snooze, and the success are no failures")
	assert.EqualValues(t, 3, g.Discarded)
	assert.EqualValues(t, 7, g.Retried)
	assert.Equal(t, "upstream returned 503 for item 10", g.SampleMessage, "the latest failure's message")
	assert.Equal(t, hourAt(rollupBase, 4), res.Coverage.RolledThrough, "four hours rolled, the fifth read raw")

	stats, err := Stats(context.Background(), db, StatsParams{From: window.From, To: window.To})
	require.NoError(t, err)
	assert.Equal(t, stats.Total.Outcomes.Error+stats.Total.Outcomes.Timeout, g.Failures,
		"a group counts what the stats count as errors and timeouts")
}

// TestErrorGroupsReportFirstAndLastSeen proves a group's first sighting reaches
// back past the window into the retained rollup hours, its last is the latest
// failure in the window, each exact while its run is kept, and the first falls
// back to the start of its rolled hour once retention has deleted the run.
func TestErrorGroupsReportFirstAndLastSeen(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	first := hourAt(rollupBase, 1).Add(10 * time.Minute)
	middle := hourAt(rollupBase, 3).Add(5 * time.Minute)
	last := hourAt(rollupBase, 5).Add(20 * time.Minute)
	seedHistory(t, db, []histRun{
		failRun("fs-first", "alpha", first, ErrorTransient, "upstream returned 503 for item 1", storedFingerprintF, false),
		failRun("fs-middle", "alpha", middle, ErrorTransient, "upstream returned 503 for item 2", storedFingerprintF, false),
		failRun("fs-last", "alpha", last, ErrorTransient, "upstream returned 503 for item 3", storedFingerprintF, false),
	})
	rollupAll(t, db, hourAt(rollupBase, 5).Add(30*time.Minute))
	window := ErrorGroupsParams{From: hourAt(rollupBase, 2).Add(30 * time.Minute), To: hourAt(rollupBase, 6)}

	g := onlyGroup(t, db, window)
	assert.EqualValues(t, 2, g.Failures, "the first failure is before the window")
	assert.Equal(t, "fs-first", g.FirstRunID, "the first sighting reaches back into the rolled hours")
	assert.True(t, first.Equal(g.FirstSeen), "exact while the run is kept: %s", g.FirstSeen)
	assert.Equal(t, "fs-last", g.LastRunID, "the last is the window's latest failure, read raw")
	assert.True(t, last.Equal(g.LastSeen), "%s", g.LastSeen)

	window.To = hourAt(rollupBase, 5)
	g = onlyGroup(t, db, window)
	assert.Equal(t, "fs-middle", g.LastRunID, "a last failure in a rolled hour")
	assert.True(t, middle.Equal(g.LastSeen), "exact from its run: %s", g.LastSeen)

	require.NoError(t, db.Exec("DELETE FROM job_run_finishes WHERE run_id = ?", "fs-first").Error)
	require.NoError(t, db.Exec("DELETE FROM job_runs WHERE id = ?", "fs-first").Error)
	g = onlyGroup(t, db, window)
	assert.Equal(t, "fs-first", g.FirstRunID, "the rollup keeps the id after retention deletes the run")
	assert.True(t, hourAt(rollupBase, 1).Equal(g.FirstSeen), "and the time falls back to its hour: %s", g.FirstSeen)
}

// TestErrorGroupsOrderFilterAndLimit proves the order — most failures first,
// then kind, then fingerprint — the kind and fingerprint filters, the limit's
// default and cap, and the hourly counts across every hour the window touches.
func TestErrorGroupsOrderFilterAndLimit(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	h := hourAt(rollupBase, 0)
	fp := func(n int) string { return fmt.Sprintf("%016x", 0xabc000+n) }
	failAt := func(id, kind, fingerprint string, at time.Time) histRun {
		return failRun(id, kind, at, ErrorTransient, "boom", fingerprint, false)
	}

	t.Run("order, filters, and hourly counts", func(t *testing.T) {
		t.Parallel()
		db := newDB(t)
		var runs []histRun
		add := func(kind, fingerprint string, minutes ...int) {
			for _, m := range minutes {
				runs = append(runs, failAt(fmt.Sprintf("of-%s-%s-%d", kind, fingerprint[12:], m), kind, fingerprint,
					h.Add(time.Duration(m)*time.Minute)))
			}
		}
		add("beta", fp(3), 40, 41, 42, 43, 44)
		add("alpha", fp(2), 45, 46, 47)
		add("alpha", fp(1), 48, 49, 50)
		add("gamma", fp(4), 51)
		add("delta", fp(5), 10, 35, 65, 70, 185)
		seedHistory(t, db, runs)
		window := ErrorGroupsParams{From: h.Add(30 * time.Minute), To: h.Add(3*time.Hour + 15*time.Minute)}

		res, err := ErrorGroups(ctx, db, window)
		require.NoError(t, err)
		var keys []string
		for _, g := range res.Groups {
			keys = append(keys, fmt.Sprintf("%s/%s/%d", g.Kind, g.Fingerprint, g.Failures))
		}
		assert.Equal(t, []string{
			"beta/" + fp(3) + "/5", "delta/" + fp(5) + "/4", "alpha/" + fp(1) + "/3", "alpha/" + fp(2) + "/3",
			"gamma/" + fp(4) + "/1",
		}, keys, "most failures first, then kind, then fingerprint")
		for _, g := range res.Groups {
			assert.Nil(t, g.Hourly, "no hourly counts unless asked")
		}

		window.Kind = "alpha"
		res, err = ErrorGroups(ctx, db, window)
		require.NoError(t, err)
		assert.Len(t, res.Groups, 2, "one kind's groups")

		window.Kind, window.Fingerprint = "", fp(3)
		res, err = ErrorGroups(ctx, db, window)
		require.NoError(t, err)
		require.Len(t, res.Groups, 1, "one group")
		assert.Equal(t, "beta", res.Groups[0].Kind)

		window.Fingerprint, window.Limit = "", 2
		res, err = ErrorGroups(ctx, db, window)
		require.NoError(t, err)
		assert.Len(t, res.Groups, 2, "the limit keeps the largest")
		assert.Equal(t, "beta", res.Groups[0].Kind)

		window.Limit, window.WithHourly, window.Kind = 0, true, "delta"
		res, err = ErrorGroups(ctx, db, window)
		require.NoError(t, err)
		require.Len(t, res.Groups, 1)
		assert.Equal(t, []int64{1, 2, 0, 1}, res.Groups[0].Hourly,
			"one count per UTC hour the window touches; the failure before From is outside it")
	})

	t.Run("the limit's default and cap", func(t *testing.T) {
		t.Parallel()
		db := newDB(t)
		runs := make([]histRun, 0, 210)
		for i := range 210 {
			runs = append(runs, failAt(fmt.Sprintf("lim-%03d", i), "many", fp(100+i), h.Add(time.Duration(i)*time.Second)))
		}
		seedHistory(t, db, runs)
		window := ErrorGroupsParams{From: h, To: h.Add(time.Hour)}
		res, err := ErrorGroups(ctx, db, window)
		require.NoError(t, err)
		assert.Len(t, res.Groups, 50, "zero reads 50")
		window.Limit = 1000
		res, err = ErrorGroups(ctx, db, window)
		require.NoError(t, err)
		assert.Len(t, res.Groups, MaxReadLimit, "and above the cap reads the cap")
	})
}

// TestErrorGroupsRefuseABadRequest proves each refusal names its field before
// anything is read — the database here has no tables — and a window too long
// to read raw with nothing rolled is ErrStatsNotRolledUp, as for Stats.
func TestErrorGroupsRefuseABadRequest(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	bare := newBareSQLite(t)
	day := 24 * time.Hour
	for _, tc := range []struct {
		name  string
		p     ErrorGroupsParams
		field string
	}{
		{name: "no From", p: ErrorGroupsParams{To: rollupBase}, field: "window"},
		{name: "no To", p: ErrorGroupsParams{From: rollupBase}, field: "window"},
		{name: "To before From", p: ErrorGroupsParams{From: rollupBase, To: rollupBase.Add(-time.Hour)}, field: "window"},
		{
			name: "a short fingerprint", p: ErrorGroupsParams{From: rollupBase, To: rollupBase.Add(time.Hour), Fingerprint: "0123"},
			field: "fingerprint",
		},
		{name: "an uppercase fingerprint", p: ErrorGroupsParams{
			From: rollupBase, To: rollupBase.Add(time.Hour),
			Fingerprint: "0123456789ABCDEF",
		}, field: "fingerprint"},
		{name: "hourly counts over eight days", p: ErrorGroupsParams{
			From: rollupBase, To: rollupBase.Add(8 * day),
			WithHourly: true,
		}, field: "with_hourly"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, err := ErrorGroups(ctx, bare, tc.p)
			require.ErrorIs(t, err, ErrValidation)
			var ve *ValidationError
			require.True(t, errors.As(err, &ve))
			assert.Equal(t, tc.field, ve.Field)
		})
	}

	db := newDB(t)
	seedHistory(t, db, []histRun{
		failRun("rr-1", "alpha", rollupBase.Add(-20*day), ErrorTransient, "boom", "", false),
		failRun("rr-2", "alpha", rollupBase, ErrorTransient, "boom", "", false),
	})
	_, err := ErrorGroups(ctx, db, ErrorGroupsParams{From: rollupBase.Add(-30 * day), To: rollupBase.Add(time.Hour)})
	require.ErrorIs(t, err, ErrStatsNotRolledUp)
	res, err := ErrorGroups(ctx, db, ErrorGroupsParams{
		From: rollupBase.Add(-30 * day), To: rollupBase.Add(time.Hour), WithHourly: false, MaxRawSpan: -1,
	})
	require.NoError(t, err, "a negative MaxRawSpan lifts the cap")
	require.Len(t, res.Groups, 1)
	assert.EqualValues(t, 2, res.Groups[0].Failures)
}
