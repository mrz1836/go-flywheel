package core

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mrz1836/go-foundation/models"
)

// anomalyRecencyWindow bounds which newly rolled hours the anomaly log
// evaluates: an hour that ended longer ago than this was rolled as part of a
// catch-up over old history, and warning about it now would page someone for the
// past.
const anomalyRecencyWindow = 6 * time.Hour

// slowRunScanLimit bounds the running attempts one heartbeat compares to their
// baselines. The page is longest-running first, so the slow ones come first.
const slowRunScanLimit = 1000

// backfillRecheck is how long after its first backfill a Scheduler backfills
// once more. The first pass covers history; the second covers the runs an older
// binary finalized after the first — the window a rolling deploy keeps both
// binaries running — which is minutes, not hours.
const backfillRecheck = 10 * time.Minute

// statsState is the Scheduler's in-memory stats state, shared by the rollup
// activity and the heartbeat, which run on different goroutines.
type statsState struct {
	// backfills counts the BackfillRunFinishes passes this Scheduler completed,
	// and firstBackfill is when the first one did: the rollup backfills on its
	// first pass and once more backfillRecheck later, then never again.
	backfills     atomic.Int32
	firstBackfill atomic.Int64

	mu sync.Mutex
	// baselines is the per-kind baseline cache the heartbeat compares running
	// attempts to, refreshed after every rollup pass; loaded marks it populated.
	baselines map[string]Baseline
	loaded    bool
	// warned is the set of run ids already logged as slow, so each slow attempt
	// warns once. It is pruned to the attempts still running on every pulse,
	// which keeps it as small as the running set.
	warned map[string]struct{}
}

// configureStats resolves the stats fields of cfg onto s, applying defaults, and
// rejects a retention window too short for the rollup to count a run before
// retention deletes it.
func (s *Scheduler) configureStats(cfg SchedulerConfig) error {
	if cfg.StatsRollupInterval <= 0 {
		return nil
	}
	s.statsInterval = cfg.StatsRollupInterval
	s.statsAnomalyLog = cfg.StatsAnomalyLog
	s.statsCfg = rollupConfig{
		grace:     cfg.StatsRollupGrace,
		maxHours:  cfg.StatsMaxHoursPerPass,
		retention: cfg.StatsRetention,
	}
	if s.statsCfg.grace <= 0 {
		s.statsCfg.grace = defaultStatsRollupGrace
	}
	if s.statsCfg.maxHours <= 0 {
		s.statsCfg.maxHours = defaultStatsMaxHoursPerPass
	}
	if s.statsCfg.retention <= 0 {
		s.statsCfg.retention = defaultStatsRetention
	}
	if s.retentionMaxAge > 0 && s.retentionMaxAge < time.Hour+s.statsCfg.grace {
		return fmt.Errorf(
			"jobs: scheduler config: RetentionMaxAge %s is shorter than one hour plus StatsRollupGrace (%s), "+
				"so retention would delete runs before the stats rollup could count them: %w",
			s.retentionMaxAge, s.statsCfg.grace, ErrValidation,
		)
	}
	s.stats.warned = map[string]struct{}{}
	return nil
}

// checkStatsSchema is the stats rollup's startup schema probe: with the rollup
// enabled, the rollup table and the job_runs columns it aggregates must exist,
// or Run fails before its first pass rather than logging the same failure every
// tick. See probeSchema for why an unreachable database is not a failure here.
func (s *Scheduler) checkStatsSchema(ctx context.Context) error {
	if s.statsInterval <= 0 {
		return nil
	}
	if _, err := probeSchema(ctx, s.db, &jobRunRow{}, &jobStatsHourlyRow{}); err != nil {
		return fmt.Errorf("jobs: scheduler schema check: %w", err)
	}
	return nil
}

// RollupStats runs one stats rollup pass: it rolls up every closed hour of
// job_runs since the watermark into job_stats_hourly, at most
// StatsMaxHoursPerPass of them, and prunes rollup rows older than
// StatsRetention.
//
// It also completes an upgrade on its own. Its first call backfills the finish
// log for runs an older release finalized (BackfillRunFinishes, which on SQLite
// first re-stamps their timestamps in UTC), and a call ten minutes later does it
// once more, for runs an older binary finalized while a rolling deploy kept both
// running. After that it never scans job_runs again.
//
// It parallels Tick, Sweep, and PruneRetention: the stats activity calls it on
// every StatsRollupInterval tick, and a host that drives its own maintenance
// loop can call it directly. It runs whether or not the activity is enabled,
// with the defaults filled in for any stats field left zero.
func (s *Scheduler) RollupStats(ctx context.Context) (RollupResult, error) {
	if err := s.backfillIfDue(ctx); err != nil {
		return RollupResult{}, fmt.Errorf("jobs: stats rollup: %w", err)
	}
	cfg := s.statsCfg
	if cfg.grace <= 0 {
		cfg.grace = defaultStatsRollupGrace
	}
	if cfg.maxHours <= 0 {
		cfg.maxHours = defaultStatsMaxHoursPerPass
	}
	if cfg.retention <= 0 {
		cfg.retention = defaultStatsRetention
	}
	res, err := rollupPass(ctx, s.db, models.ClockFrom(ctx).Now(ctx), cfg)
	if err != nil {
		return res, fmt.Errorf("jobs: stats rollup: %w", err)
	}
	return res, nil
}

// backfillIfDue runs BackfillRunFinishes on the first rollup pass and once more
// backfillRecheck after it. The recheck is timed on the wall clock, not the
// context clock: it bounds a real deploy's overlap, and a test driving a fixed
// clock must not be able to suppress it.
func (s *Scheduler) backfillIfDue(ctx context.Context) error {
	switch n := s.stats.backfills.Load(); {
	case n == 0:
	case n == 1 && time.Since(time.Unix(0, s.stats.firstBackfill.Load())) >= backfillRecheck:
	default:
		return nil
	}
	if _, err := BackfillRunFinishes(ctx, s.db); err != nil {
		return err
	}
	if s.stats.backfills.Add(1) == 1 {
		s.stats.firstBackfill.Store(time.Now().UnixNano())
	}
	return nil
}

// rollupOnce is the stats activity's pass: roll up, refresh the baseline cache
// the heartbeat reads, and — when enabled and caught up — log anomaly onsets
// for the hours just rolled.
func (s *Scheduler) rollupOnce(ctx context.Context) {
	res, err := s.RollupStats(ctx)
	if err != nil {
		s.logMaintenanceError(ctx, "jobs: stats rollup failed", err)
		return
	}
	if !res.CaughtUp {
		// Saying so is what tells an operator why Stats still returns
		// ErrStatsNotRolledUp for a long window: the backfill is in progress.
		s.logger.InfoContext(ctx, "jobs: stats rollup catching up",
			"hours", res.Hours, "rolled_through", res.Watermark)
	}
	s.refreshBaselines(ctx)
	if s.statsAnomalyLog && res.CaughtUp {
		s.logAnomalies(ctx, res.Rolled)
	}
}

// refreshBaselines reloads the per-kind baseline cache. A failed read keeps the
// previous cache: a stale baseline is better than none for a heartbeat field.
func (s *Scheduler) refreshBaselines(ctx context.Context) {
	list, err := Baselines(ctx, s.db, 0)
	if err != nil {
		s.logMaintenanceError(ctx, "jobs: stats baseline refresh failed", err)
		return
	}
	s.stats.mu.Lock()
	s.stats.baselines = baselineMap(list)
	s.stats.loaded = true
	s.stats.mu.Unlock()
}

// cachedBaselines returns the baseline cache, loading it on first use so a
// heartbeat that fires before the first rollup pass still has one.
func (s *Scheduler) cachedBaselines(ctx context.Context) map[string]Baseline {
	s.stats.mu.Lock()
	loaded := s.stats.loaded
	s.stats.mu.Unlock()
	if !loaded {
		s.refreshBaselines(ctx)
	}
	s.stats.mu.Lock()
	defer s.stats.mu.Unlock()
	return s.stats.baselines
}

// countSlowRunning counts running attempts past their kind's slow line and warns
// once for each that is new. A read failure is logged and counts zero: the
// heartbeat must keep pulsing through a transient error.
func (s *Scheduler) countSlowRunning(ctx context.Context) int {
	baselines := s.cachedBaselines(ctx)
	if len(baselines) == 0 {
		return 0
	}
	running, err := listRunning(ctx, s.db, ListRunningParams{Limit: slowRunScanLimit}, baselines)
	if err != nil {
		s.logger.ErrorContext(ctx, "jobs: slow-run check failed", "error", err)
		return 0
	}
	s.stats.mu.Lock()
	defer s.stats.mu.Unlock()
	still := make(map[string]struct{}, len(s.stats.warned))
	slow := 0
	for i := range running {
		r := running[i]
		if !r.Slow {
			continue
		}
		slow++
		still[r.RunID] = struct{}{}
		if _, seen := s.stats.warned[r.RunID]; seen {
			continue
		}
		s.logger.WarnContext(ctx, "jobs: run is far slower than its kind's baseline",
			"job_id", r.JobID, "run_id", r.RunID, "kind", r.Kind, "queue", r.Queue,
			"attempt", r.Attempt, "elapsed", r.Elapsed.String(),
			"baseline_p99", r.BaselineP99.String(),
			"threshold", slowRunThreshold(*r.BaselineP99).String())
	}
	s.stats.warned = still
	return slow
}

// logAnomalies evaluates each newly rolled, recent hour and warns once for each
// anomaly's onset. Each hour is rolled once, so each (kind, signal, hour) is
// evaluated once, which is what deduplicates the warnings.
func (s *Scheduler) logAnomalies(ctx context.Context, rolled []time.Time) {
	now := models.ClockFrom(ctx).Now(ctx)
	for _, hour := range rolled {
		if now.Sub(hour.Add(time.Hour)) > anomalyRecencyWindow {
			continue
		}
		found, err := Anomalies(ctx, s.db, AnomalyParams{Hour: hour})
		if err != nil {
			s.logMaintenanceError(ctx, "jobs: stats anomaly check failed", err)
			return
		}
		for _, a := range found {
			if !a.Onset {
				continue
			}
			s.logger.WarnContext(ctx, "jobs: stats anomaly",
				"kind", a.Kind, "signal", string(a.Signal), "hour", a.Hour,
				"metric", a.Metric, "recent", a.Recent, "baseline", a.Baseline,
				"ratio", a.Ratio, "samples", a.Samples, "baseline_samples", a.BaselineSamples)
		}
	}
}
