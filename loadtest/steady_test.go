//go:build loadtest

package loadtest

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

// TestDurationMustBeBelowTimeout is the guard for the trap that would otherwise
// waste a whole depth run.
//
// Timeout defaults to 30 minutes and Run wraps the entire run in it, so a
// -duration of two hours against an unset -timeout dies at the 30-minute mark
// with a non-zero exit and a truncated artifact — after having already spent
// thirty minutes. Rejecting the configuration up front costs a message.
func TestDurationMustBeBelowTimeout(t *testing.T) {
	t.Parallel()

	_, err := Config{
		DSN: testDSN, Jobs: 1, Mix: WorkloadSteady,
		Duration: 2 * time.Hour, // Timeout defaults to 30m
	}.validate()
	if !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("a Duration past the Timeout must be rejected, got %v", err)
	}
	for _, want := range []string{"2h0m0s", "30m0s", "-timeout"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the rejection must name %q, got: %s", want, err)
		}
	}

	if _, err := (Config{
		DSN: testDSN, Jobs: 1, Mix: WorkloadSteady,
		Duration: 20 * time.Minute, Timeout: 90 * time.Minute,
	}).validate(); err != nil {
		t.Errorf("a Duration comfortably below the Timeout is valid: %v", err)
	}
}

// TestDurationRequiresTheSteadyMix pins the flag to the only mix it means
// anything for: a fixed population drains when it is empty, not when a clock
// says so.
func TestDurationRequiresTheSteadyMix(t *testing.T) {
	t.Parallel()

	_, err := Config{
		DSN: testDSN, Jobs: 1, Mix: WorkloadDrainOnly,
		Duration: time.Minute, Timeout: time.Hour,
	}.validate()
	if !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("Duration outside the steady mix must be rejected, got %v", err)
	}
}

// TestZeroDurationPreservesTodaysBehavior proves the new field is inert when
// unset, which is what makes every previously committed report still
// reproducible.
func TestZeroDurationPreservesTodaysBehavior(t *testing.T) {
	t.Parallel()

	got, err := Config{DSN: testDSN, Jobs: 1, Mix: WorkloadSteady}.validate()
	if err != nil {
		t.Fatalf("validate: %v", err)
	}
	if got.Duration != 0 {
		t.Errorf("a zero Duration must stay zero, got %s", got.Duration)
	}
	if got.Storage != StorageDefault {
		t.Errorf("the default storage condition must be %q, got %q", StorageDefault, got.Storage)
	}
}

// TestRetentionWithoutABacklogIsRejected keeps a run from reporting a working
// retention sweep that had nothing to delete.
//
// The bulk seed path writes no finalized_at and no job_runs, so a drain run with
// -retention set would prune exactly zero rows and the artifact would look like
// a successful measurement.
func TestRetentionWithoutABacklogIsRejected(t *testing.T) {
	t.Parallel()

	_, err := Config{
		DSN: testDSN, Jobs: 1, Mix: WorkloadDrainOnly, RetentionMaxAge: time.Hour,
	}.validate()
	if !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("retention with nothing to prune must be rejected, got %v", err)
	}
	if !strings.Contains(err.Error(), "TerminalSeed") {
		t.Errorf("the rejection must name the remedy, got: %s", err)
	}

	// With a seeded backlog it is fine.
	if _, err := (Config{
		DSN: testDSN, Jobs: 1, Mix: WorkloadDrainOnly,
		RetentionMaxAge: time.Hour, TerminalSeed: 100,
	}).validate(); err != nil {
		t.Errorf("retention against a seeded backlog is valid: %v", err)
	}
}

// TestTerminalSeedAgeDefaults proves a seeded backlog is old enough for any
// plausible window without the caller having to say so.
func TestTerminalSeedAgeDefaults(t *testing.T) {
	t.Parallel()

	got, err := Config{DSN: testDSN, Jobs: 1, TerminalSeed: 10}.validate()
	if err != nil {
		t.Fatalf("validate: %v", err)
	}
	if got.TerminalSeedAge != defaultTerminalSeedAge {
		t.Errorf("TerminalSeedAge must default to %s, got %s", defaultTerminalSeedAge, got.TerminalSeedAge)
	}
}

// TestStorageConditionValidity proves the enum recognizes exactly its declared
// values. The string is persisted in committed reports, so a typo that validated
// would produce an artifact naming a condition that does not exist.
func TestStorageConditionValidity(t *testing.T) {
	t.Parallel()

	for _, c := range []StorageCondition{StorageDefault, StorageTuned} {
		if !c.Valid() {
			t.Errorf("StorageCondition %q must be valid", c)
		}
	}
	for _, c := range []StorageCondition{"", "tuned-ish", "FILLFACTOR"} {
		if c.Valid() {
			t.Errorf("StorageCondition %q must not be valid", c)
		}
	}
	if _, err := (Config{DSN: testDSN, Jobs: 1, Storage: "nope"}).validate(); !errors.Is(err, ErrInvalidConfig) {
		t.Errorf("an unknown storage condition must be rejected, got %v", err)
	}
}

// TestStorageTuningIsInstalledAndReported proves the tuned condition reaches the
// database and that the report records what was installed rather than what was
// requested.
//
// The distinction follows the precedent the index condition set: a report that
// only echoed its own Config could not tell a real condition from a schema that
// silently came up wrong.
func TestStorageTuningIsInstalledAndReported(t *testing.T) {
	t.Parallel()
	dsn := requireDSN(t)
	ctx := context.Background()

	for _, tc := range []struct {
		name      string
		condition StorageCondition
		wantJobs  string
	}{
		{"default leaves no fingerprint", StorageDefault, ""},
		{"tuned sets fillfactor and autovacuum", StorageTuned, "fillfactor=80"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h, err := newHarness(ctx, Config{
				DSN: dsn, Jobs: 1, Runners: 1, Workers: 1, Storage: tc.condition,
			})
			if err != nil {
				t.Fatalf("newHarness: %v", err)
			}
			t.Cleanup(func() { _ = h.Close(ctx) })

			opts, err := installedStorage(ctx, h.probe)
			if err != nil {
				t.Fatalf("installedStorage: %v", err)
			}
			if !strings.Contains(opts["jobs"], tc.wantJobs) {
				t.Errorf("jobs reloptions = %q, want it to contain %q", opts["jobs"], tc.wantJobs)
			}
			// job_runs is append-only, so neither setting has anything to act on
			// and neither condition should touch it.
			if opts["job_runs"] != "" {
				t.Errorf("job_runs must carry no storage parameters, got %q", opts["job_runs"])
			}
		})
	}
}

// TestSeedTerminalWritesAPrunableBacklog proves the fixture retention needs is
// actually prunable: finalized, backdated, and carrying its audit rows.
func TestSeedTerminalWritesAPrunableBacklog(t *testing.T) {
	t.Parallel()
	dsn := requireDSN(t)
	ctx := context.Background()

	cfg := Config{DSN: dsn, Jobs: 1, Runners: 1, Workers: 1}
	h, err := newHarness(ctx, cfg)
	if err != nil {
		t.Fatalf("newHarness: %v", err)
	}
	t.Cleanup(func() { _ = h.Close(ctx) })

	const count = 250
	normalized, err := cfg.validate()
	if err != nil {
		t.Fatalf("validate: %v", err)
	}
	if err := seedTerminal(ctx, h.work, normalized, count, defaultTerminalSeedAge); err != nil {
		t.Fatalf("seedTerminal: %v", err)
	}

	var jobs, runs int64
	if err := h.work.Raw(
		`SELECT count(*) FROM jobs WHERE state = 'succeeded' AND finalized_at IS NOT NULL`,
	).Scan(&jobs).Error; err != nil {
		t.Fatalf("count jobs: %v", err)
	}
	if err := h.work.Raw(`SELECT count(*) FROM job_runs`).Scan(&runs).Error; err != nil {
		t.Fatalf("count runs: %v", err)
	}
	if jobs != count {
		t.Errorf("seeded terminal jobs = %d, want %d", jobs, count)
	}
	if runs != count {
		t.Errorf("seeded terminal runs = %d, want %d — retention prunes both tables", runs, count)
	}
}

// TestSeedBulkGenerationsDoNotCollide is the guard for the closed loop's id
// space. A replenishing run seeds many times, and every generation before this
// change minted the same ids from the same epoch and the same PCG stream — so
// the second top-up would have failed on the primary key.
func TestSeedBulkGenerationsDoNotCollide(t *testing.T) {
	t.Parallel()
	dsn := requireDSN(t)
	ctx := context.Background()

	cfg, err := Config{DSN: dsn, Jobs: 50, Runners: 1, Workers: 1}.validate()
	if err != nil {
		t.Fatalf("validate: %v", err)
	}
	h, err := newHarness(ctx, cfg)
	if err != nil {
		t.Fatalf("newHarness: %v", err)
	}
	t.Cleanup(func() { _ = h.Close(ctx) })

	specs := generate(cfg)
	for generation := range 3 {
		if err := seedBulkFrom(ctx, h.work, cfg, specs, generation, "", func(int) {}); err != nil {
			t.Fatalf("generation %d: %v", generation, err)
		}
	}

	var total int64
	if err := h.work.Raw(`SELECT count(*) FROM jobs`).Scan(&total).Error; err != nil {
		t.Fatalf("count: %v", err)
	}
	if want := int64(3 * len(specs)); total != want {
		t.Errorf("three generations inserted %d rows, want %d — ids collided", total, want)
	}
}
