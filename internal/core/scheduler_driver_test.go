package core

import (
	"context"
	"testing"
	"time"

	"github.com/mrz1836/go-foundation/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

// countingDriver counts the Driver calls a Scheduler makes, which is the only
// way to prove the sweep reaches the injected seam rather than one the
// Scheduler built for itself. It delegates everything it does not count.
type countingDriver struct {
	Driver
	sweeps int
}

// Sweep records the call and delegates.
func (d *countingDriver) Sweep(ctx context.Context, now time.Time) (int, error) {
	d.sweeps++
	return d.Driver.Sweep(ctx, now)
}

// TestSchedulerSweepRoutesThroughTheInjectedDriver is the regression gate for
// the defect this change exists to fix: the Scheduler used to construct a
// baseDriver inline, so a host that wrapped its Driver for metrics, tracing, or
// fault injection saw every claim and finalize and never saw a sweep.
func TestSchedulerSweepRoutesThroughTheInjectedDriver(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	counting := &countingDriver{Driver: NewSQLiteDriver(db)}

	sched, err := NewSchedulerWithConfig(SchedulerConfig{
		DB: db, Client: NewClient(db), Driver: counting,
	})
	require.NoError(t, err)

	reclaimed, err := sched.Sweep(context.Background())
	require.NoError(t, err)

	assert.Equal(t, 1, counting.sweeps, "the sweep must reach the injected driver")
	assert.Equal(t, 0, reclaimed, "an empty queue reclaims nothing")
}

// TestSchedulerSweepReclaimsThroughTheInjectedDriver pairs the call-count
// assertion above with a behavioral one: the wrapper is not merely called, its
// return value is what the Scheduler reports.
func TestSchedulerSweepReclaimsThroughTheInjectedDriver(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	now := time.Now().UTC().Truncate(time.Second)
	seedExpiredLease(t, db, "expired-1", now.Add(-time.Hour))

	counting := &countingDriver{Driver: NewSQLiteDriver(db)}
	sched, err := NewSchedulerWithConfig(SchedulerConfig{
		DB: db, Client: NewClient(db), Driver: counting,
	})
	require.NoError(t, err)

	reclaimed, err := sched.Sweep(clockCtx(context.Background(), models.NewFixedClock(now)))
	require.NoError(t, err)

	assert.Equal(t, 1, counting.sweeps)
	assert.Equal(t, 1, reclaimed, "the count the Scheduler reports is the injected driver's")
}

// TestNewSchedulerWithConfigRequiresADriver holds the line the helper
// newSchedulerCfg papers over for every other test in the package.
func TestNewSchedulerWithConfigRequiresADriver(t *testing.T) {
	t.Parallel()
	db := newDB(t)

	_, err := NewSchedulerWithConfig(SchedulerConfig{DB: db, Client: NewClient(db)})
	require.ErrorIs(t, err, errSchedulerNeedsDriver)
}

// TestNewSchedulerWithConfigRequiresDBAndClient proves the constructor is the
// single authority on the config, not just on its new field.
func TestNewSchedulerWithConfigRequiresDBAndClient(t *testing.T) {
	t.Parallel()
	db := newDB(t)

	_, err := NewSchedulerWithConfig(SchedulerConfig{Client: NewClient(db), Driver: NewSQLiteDriver(db)})
	require.ErrorIs(t, err, errSchedulerNeedsDB)

	_, err = NewSchedulerWithConfig(SchedulerConfig{DB: db, Driver: NewSQLiteDriver(db)})
	require.ErrorIs(t, err, errSchedulerNeedsClient)
}

// TestNewSchedulerSelectsTheDriverFromTheDialect proves the two-argument
// shorthand still works without a Driver argument, by selecting one.
func TestNewSchedulerSelectsTheDriverFromTheDialect(t *testing.T) {
	t.Parallel()
	db := newDB(t)

	sched, err := NewScheduler(db, NewClient(db))
	require.NoError(t, err)
	require.NotNil(t, sched)

	assert.IsType(t, &sqliteDriver{}, sched.driver, "a sqlite dialect selects the sqlite driver")
}

// TestNewSchedulerRejectsAnUnsupportedDialect proves the dialect gate reuses the
// runtime's existing sentinel rather than inventing a second one, so a scheduler
// over an unsupported dialect fails the way Migrate and IndexSet already do.
//
// It goes through driverForDialect rather than a fake gorm.Dialector: the
// interface is eight methods, seven of which would be panics, and a test fixture
// that large obscures the one line it exists to exercise.
func TestNewSchedulerRejectsAnUnsupportedDialect(t *testing.T) {
	t.Parallel()
	db := newDB(t)

	_, err := driverForDialect("mysql", db)
	require.ErrorIs(t, err, ErrUnsupportedDialect)
	assert.Contains(t, err.Error(), "mysql", "the error names the offending dialect")
}

// TestDriverForDialectSelectsBothSupportedDialects covers the postgres arm,
// which no SQLite fixture reaches, without needing a live server: selection is a
// constructor call, not a connection.
func TestDriverForDialectSelectsBothSupportedDialects(t *testing.T) {
	t.Parallel()
	db := newDB(t)

	pg, err := driverForDialect("postgres", db)
	require.NoError(t, err)
	assert.IsType(t, &postgresDriver{}, pg)

	lite, err := driverForDialect("sqlite", db)
	require.NoError(t, err)
	assert.IsType(t, &sqliteDriver{}, lite)
}

// TestNewSchedulerRejectsANilDB proves the dialect selection does not panic on
// the one input it cannot read a dialect from.
func TestNewSchedulerRejectsANilDB(t *testing.T) {
	t.Parallel()

	_, err := NewScheduler(nil, nil)
	require.ErrorIs(t, err, errSchedulerNeedsDB)
}

// seedExpiredLease writes one running job whose lease expired at expiredAt,
// together with the started run stub a live attempt would have committed.
func seedExpiredLease(t testing.TB, db *gorm.DB, id string, expiredAt time.Time) {
	t.Helper()
	token := "token-" + id
	require.NoError(t, db.Create(&jobRow{
		ID: id, CreatedAt: expiredAt, UpdatedAt: expiredAt,
		Kind: "sweep.test", Queue: "default", Args: []byte(`{}`),
		Priority: 100, State: string(StateRunning), Attempt: 1, MaxAttempts: 25,
		ScheduledAt: expiredAt, LeasedUntil: &expiredAt, LeaseToken: &token,
		Tags: []byte(`[]`), Metadata: []byte(`{}`),
	}).Error)
	require.NoError(t, db.Create(&jobRunRow{
		ID: "run-" + id, JobID: id, Attempt: 1, ExecutorClass: "local",
		ExecutorID: "exec-1", StartedAt: expiredAt,
		Outcome: string(OutcomeStarted), CreatedAt: expiredAt,
	}).Error)
}
