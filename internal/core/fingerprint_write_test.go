package core

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/mrz1836/go-foundation/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

// mailArgs and mailWorker drive the worker-supplied fingerprint test: every
// attempt fails with a message that names its recipient, wrapping an error
// that names the failure by the upstream's code.
type mailArgs struct{ To string }

func (mailArgs) Kind() string { return "send_mail" }

type mailWorker struct{}

func (mailWorker) Kind() string { return "send_mail" }

func (mailWorker) Work(_ context.Context, job *Job[mailArgs]) (Result, error) {
	return Result{}, fmt.Errorf("deliver to %s: %w", job.Args.To,
		customFingerprintError{code: "smtp-421", message: "421 4.7.0 " + job.Args.To + " is rate limited"})
}

// storedFingerprint reads a run's error_fingerprint, nil when it is NULL.
func storedFingerprint(t testing.TB, db *gorm.DB, runID string) *string {
	t.Helper()
	var row jobRunRow
	require.NoError(t, db.Where("id = ?", runID).First(&row).Error)
	return row.ErrorFingerprint
}

// TestAWorkerSuppliedFingerprintWins proves a worker that names its failure
// puts every attempt that returns the name in one group, whatever each message
// says: two attempts with different messages store one fingerprint, the hash of
// the worker's value.
func TestAWorkerSuppliedFingerprintWins(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	ctx := context.Background()
	reg := NewRegistry()
	Register[mailArgs](reg, mailWorker{})
	var jobs []string
	for _, to := range []string{"billing", "support"} {
		id, err := Insert(ctx, NewClient(db), mailArgs{To: to}, InsertOpts{MaxAttempts: 1})
		require.NoError(t, err)
		jobs = append(jobs, id)
	}

	runToIdle(t, ctx, rwRunner(t, db, reg))

	var messages []string
	for _, id := range jobs {
		var row jobRunRow
		require.NoError(t, db.Where("job_id = ?", id).First(&row).Error)
		require.NotNil(t, row.ErrorMessage)
		messages = append(messages, *row.ErrorMessage)
		require.NotNil(t, row.ErrorFingerprint, "a failed attempt stores its fingerprint")
		assert.Equal(t, "b988885cd8179df6", *row.ErrorFingerprint, "the hash of the worker's value")
	}
	assert.NotEqual(t, messages[0], messages[1], "the two attempts failed with different messages")
	assert.NotEqual(t, ErrorFingerprint("send_mail", ErrorTransient, messages[0]),
		ErrorFingerprint("send_mail", ErrorTransient, messages[1]), "which alone would be two groups")
}

// seedFingerprintJob seeds a running job of kind "ingest" holding a claim and
// its started run stub, through d, and returns the raw job and the run's id.
func seedFingerprintJob(t *testing.T, db *gorm.DB, d Driver, id string, now time.Time) (RawJob, string) {
	t.Helper()
	token := models.NewID()
	leased := now.Add(time.Minute)
	raw := RawJob{ID: id, Kind: "ingest", Queue: defaultQueue, Attempt: 1, MaxAttempts: 3, LeaseToken: token}
	seedJob(t, db, jobRow{
		ID: raw.ID, Kind: raw.Kind, Queue: raw.Queue, State: string(StateRunning), LeaseToken: &token,
		Attempt: 1, MaxAttempts: 3, CreatedAt: now, UpdatedAt: now, ScheduledAt: now, LeasedUntil: &leased,
	})
	runID := models.NewID()
	require.NoError(t, d.InsertRunStub(context.Background(), runID, raw, now, ExecutorClass("local"), "h1"))
	return raw, runID
}

// finalizeFingerprintsSuite proves the finalize stores a fingerprint exactly
// for an attempt whose outcome is error or timeout — superseded or not — over
// the job's kind, the attempt's class, and the stored message, and leaves it
// NULL for a success, a snooze that carries an error, a cancel, and the sweep's
// crash.
func finalizeFingerprintsSuite(t *testing.T, db *gorm.DB) {
	t.Helper()
	d := driverForTest(t, db)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)
	snooze := time.Minute
	long := "payload " + strings.Repeat("7", 5000) + " and a tail the store cuts off"

	for _, tc := range []struct {
		name      string
		result    Result
		workErr   error
		supersede bool
		want      string // "" for NULL
	}{
		{name: "success", want: ""},
		{name: "a snooze carrying an error", result: Result{Snooze: &snooze}, workErr: errors.New("deferred 3"), want: ""},
		{name: "a cancel", result: Result{Cancel: true}, workErr: errors.New("gave up"), want: ""},
		{
			name:    "an error",
			workErr: &classifiedError{cause: errors.New("upstream returned 503 for item 42"), class: ErrorTransient},
			want:    ErrorFingerprint("ingest", ErrorTransient, "upstream returned 503 for item 42"),
		},
		{
			name:    "a timeout",
			workErr: &classifiedError{cause: errors.New("deadline after 30s"), class: ErrorTimeout},
			want:    ErrorFingerprint("ingest", ErrorTimeout, "deadline after 30s"),
		},
		{
			name:      "a superseded error",
			workErr:   &classifiedError{cause: errors.New("upstream returned 502 for item 7"), class: ErrorTransient},
			supersede: true,
			want:      ErrorFingerprint("ingest", ErrorTransient, "upstream returned 502 for item 7"),
		},
		{
			name:    "an error longer than the stored message",
			workErr: &classifiedError{cause: errors.New(long), class: ErrorPermanent},
			want:    ErrorFingerprint("ingest", ErrorPermanent, truncate(long, maxErrorMessage)),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			raw, runID := seedFingerprintJob(t, db, d, "fp-"+strings.ReplaceAll(tc.name, " ", "-"), now)
			if tc.supersede {
				require.NoError(t, CancelJob(ctx, db, raw.ID))
			}
			out, err := d.Finalize(ctx, raw, runID, tc.result, tc.workErr, now.Add(time.Second))
			require.NoError(t, err)
			assert.Equal(t, tc.supersede, out.Superseded)

			got := storedFingerprint(t, db, runID)
			if tc.want == "" {
				assert.Nil(t, got, "no fingerprint for a %s", tc.name)
				return
			}
			require.NotNil(t, got, "a fingerprint for %s", tc.name)
			assert.Equal(t, tc.want, *got)
		})
	}

	t.Run("a crash", func(t *testing.T) {
		raw, runID := seedFingerprintJob(t, db, d, "fp-crash", now)
		swept, err := d.Sweep(ctx, now.Add(time.Hour))
		require.NoError(t, err)
		require.Equal(t, 1, swept)
		var row jobRunRow
		require.NoError(t, db.Where("id = ?", runID).First(&row).Error)
		assert.Equal(t, string(OutcomeCrashed), row.Outcome, "%s's stub was crashed", raw.ID)
		assert.Nil(t, row.ErrorFingerprint, "a crash stores no fingerprint")
	})
}

// TestFinalizeFingerprintsOnlyErrorsAndTimeouts runs the suite on SQLite.
func TestFinalizeFingerprintsOnlyErrorsAndTimeouts(t *testing.T) {
	t.Parallel()
	finalizeFingerprintsSuite(t, newDB(t))
}

// TestSeedRunFingerprintsAFailedRun proves a seeded failure is fingerprinted as
// a real one is, over its job's kind and the stored message; a seed carries no
// error class, so the class hashed is empty. Any other outcome stores none.
func TestSeedRunFingerprintsAFailedRun(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	ctx := context.Background()
	jobID, err := Enqueue(ctx, NewClient(db), "import", []byte(`{}`), InsertOpts{})
	require.NoError(t, err)
	finished := time.Now().UTC().Truncate(time.Second)

	for i, tc := range []struct {
		outcome RunOutcome
		message string
		want    string
	}{
		{outcome: OutcomeError, message: "row 1,204 rejected: field \"amount\"", want: ErrorFingerprint("import", "", "row 1,204 rejected: field \"amount\"")},
		{outcome: OutcomeTimeout, message: "gave up after 30s", want: ErrorFingerprint("import", "", "gave up after 30s")},
		{outcome: OutcomeSuccess, message: "", want: ""},
		{outcome: OutcomeSnooze, message: "deferred", want: ""},
	} {
		runID, err := SeedRun(ctx, db, RunSeed{
			JobID: jobID, Attempt: i + 1, ExecutorID: "seed", Outcome: tc.outcome,
			StartedAt: finished.Add(-time.Second), FinishedAt: &finished, ErrorMessage: tc.message,
		})
		require.NoError(t, err)
		got := storedFingerprint(t, db, runID)
		if tc.want == "" {
			assert.Nil(t, got, "a seeded %s stores no fingerprint", tc.outcome)
			continue
		}
		require.NotNil(t, got, "a seeded %s stores a fingerprint", tc.outcome)
		assert.Equal(t, tc.want, *got)
	}
}

// TestRunViewFingerprintsAnOlderRunOnRead proves JobRunView reports a stored
// fingerprint as stored, computes one for a failed run an older release wrote
// without it, from the kind the run records, and reports none for a run whose
// kind is unrecorded or that did not fail.
func TestRunViewFingerprintsAnOlderRunOnRead(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	ctx := context.Background()
	jobID, err := Enqueue(ctx, NewClient(db), "sync", []byte(`{}`), InsertOpts{})
	require.NoError(t, err)
	at := time.Now().UTC().Truncate(time.Second)
	class := string(ErrorPermanent)
	message := "HTTP 503 from https://api.example.com/v2/items?id=42"
	stored := "0123456789abcdef"

	write := func(attempt int, kind, outcome string, fingerprint *string) {
		t.Helper()
		row := jobRunRow{
			ID: models.NewID(), JobID: jobID, Attempt: attempt, Kind: kind, ExecutorID: "old-binary",
			StartedAt: at, FinishedAt: &at, Outcome: outcome, CreatedAt: at, ErrorFingerprint: fingerprint,
		}
		if outcome != string(OutcomeSuccess) {
			row.ErrorClass, row.ErrorMessage = &class, &message
		}
		require.NoError(t, db.Create(&row).Error)
	}
	write(1, "sync", string(OutcomeError), nil)
	write(2, "", string(OutcomeError), nil)
	write(3, "sync", string(OutcomeTimeout), &stored)
	write(4, "sync", string(OutcomeSuccess), nil)

	views, err := ListRuns(ctx, db, jobID, ListRunsParams{})
	require.NoError(t, err)
	byAttempt := map[int]string{}
	for _, v := range views {
		byAttempt[v.Attempt] = v.ErrorFingerprint
	}
	assert.Equal(t, map[int]string{
		1: ErrorFingerprint("sync", ErrorPermanent, message),
		2: "",
		3: stored,
		4: "",
	}, byAttempt)
}
