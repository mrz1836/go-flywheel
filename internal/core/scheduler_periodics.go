package core

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/mrz1836/go-foundation/models"
)

// defaultReconcileInterval is the cadence of the declared-periodic reconcile when
// SchedulerConfig.ReconcileInterval is zero, and the cadence at which a start-up
// apply that failed is retried whatever the interval. A pass with nothing missing
// is one indexed read that writes nothing, so a minute bounds how long a deleted
// schedule stays gone at negligible cost.
const defaultReconcileInterval = time.Minute

// configurePeriodics validates and copies cfg's declared periodics onto s and
// resolves the reconcile interval.
//
// Every declaration is validated here, at construction, rather than when Run
// first applies it: a malformed declaration is a wiring bug the host fixes in
// source, and a Node that accepted one would log the same failure every minute
// instead of refusing to start. The specs are deep-copied, so a caller that
// reuses its slice or its ArgsTemplate buffers cannot change what this Scheduler
// declares.
func (s *Scheduler) configurePeriodics(cfg SchedulerConfig) error {
	periodics := make([]PeriodicSpec, len(cfg.Periodics))
	seen := make(map[string]struct{}, len(cfg.Periodics))
	for i, spec := range cfg.Periodics {
		if err := spec.validate(); err != nil {
			if spec.Slug == "" {
				return fmt.Errorf("jobs: scheduler config: Periodics[%d]: %w", i, err)
			}
			return fmt.Errorf("jobs: scheduler config: periodic %q: %w", spec.Slug, err)
		}
		if _, dup := seen[spec.Slug]; dup {
			return fmt.Errorf("jobs: scheduler config: periodic %q is declared more than once: %w",
				spec.Slug, ErrValidation)
		}
		seen[spec.Slug] = struct{}{}
		spec.ArgsTemplate = bytes.Clone(spec.ArgsTemplate)
		periodics[i] = spec
	}
	s.periodics = periodics
	s.reconcileInterval = resolveReconcileInterval(cfg.ReconcileInterval)
	return nil
}

// resolveReconcileInterval resolves SchedulerConfig.ReconcileInterval: an
// explicit positive value as given, zero to the one-minute default, and a
// negative value to zero, which turns the reconcile pass off.
func resolveReconcileInterval(d time.Duration) time.Duration {
	switch {
	case d < 0:
		return 0
	case d == 0:
		return defaultReconcileInterval
	default:
		return d
	}
}

// reconcileCadence is the reconcile activity's tick interval: the configured
// interval, or the default when the pass is off and the activity runs only to
// retry a start-up apply that failed.
func (s *Scheduler) reconcileCadence() time.Duration {
	if s.reconcileInterval > 0 {
		return s.reconcileInterval
	}
	return defaultReconcileInterval
}

// reconcileEnabled reports whether this Scheduler runs the reconcile activity:
// it declares periodics, and either the reconcile pass is on or a declaration
// Run could not apply at start is still waiting to be retried.
func (s *Scheduler) reconcileEnabled() bool {
	return len(s.periodics) > 0 && (s.reconcileInterval > 0 || s.hasPendingPeriodics())
}

// reconcileFinished reports whether the reconcile activity has no work left for
// the rest of Run: the pass is off and nothing is waiting to be retried. It is
// the activity's done hook, so its goroutine exits once the retry has landed.
func (s *Scheduler) reconcileFinished() bool {
	return s.reconcileInterval <= 0 && !s.hasPendingPeriodics()
}

// hasPendingPeriodics reports whether a declaration Run could not apply is still
// waiting to be retried.
func (s *Scheduler) hasPendingPeriodics() bool {
	return len(s.pendingSlugs()) > 0
}

// pendingSlugs snapshots the slugs of the declarations Run could not apply yet,
// or nil when there are none.
func (s *Scheduler) pendingSlugs() map[string]struct{} {
	s.periodicsMu.Lock()
	defer s.periodicsMu.Unlock()
	return slugSet(s.pendingPeriodics)
}

// slugSet returns the set of specs' slugs, or nil for no specs.
func slugSet(specs []PeriodicSpec) map[string]struct{} {
	if len(specs) == 0 {
		return nil
	}
	set := make(map[string]struct{}, len(specs))
	for i := range specs {
		set[specs[i].Slug] = struct{}{}
	}
	return set
}

// applyDeclaredPeriodics is Run's start-up apply: it upserts every declared
// periodic with UpsertPeriodic's semantics, so a missing definition is inserted,
// a changed one updated, and an unchanged one keeps its next_run_at cursor.
//
// It never fails Run. A declaration that cannot be applied — the database is
// unreachable as the Node starts — is logged and kept pending, and the reconcile
// activity retries it, for the same reason an unreachable database does not fail
// the schema check: an outage must not turn into a crash.
func (s *Scheduler) applyDeclaredPeriodics(ctx context.Context) {
	if len(s.periodics) == 0 {
		return
	}
	s.periodicsMu.Lock()
	s.pendingPeriodics = slices.Clone(s.periodics)
	s.periodicsMu.Unlock()
	s.applyPendingPeriodics(ctx)
}

// applyPendingPeriodics upserts each declaration still pending and keeps only the
// ones that failed, returning the slugs still pending (nil when none is).
//
// Only the failures are retried, never the whole declaration set. A retry
// re-sends Active, so re-applying a declaration that already landed would undo
// an operator's SetPeriodicActive(false) on every pass for as long as some other
// declaration kept failing. Once a declaration has been applied it belongs to the
// insert-only reconcile pass, which never touches an existing row.
//
// A failure is logged at error with how many declarations are still pending, and
// the success that follows one is logged at info. A clean start logs nothing.
func (s *Scheduler) applyPendingPeriodics(ctx context.Context) map[string]struct{} {
	s.periodicsMu.Lock()
	defer s.periodicsMu.Unlock()
	if len(s.pendingPeriodics) == 0 {
		return nil
	}

	var (
		failed []PeriodicSpec
		errs   []error
	)
	for i, spec := range s.pendingPeriodics {
		if ctx.Err() != nil {
			// Shutting down: what is left stays pending, and a cancellation is not
			// a failure worth logging.
			failed = append(failed, s.pendingPeriodics[i:]...)
			break
		}
		if err := UpsertPeriodic(ctx, s.db, spec); err != nil {
			failed = append(failed, spec)
			errs = append(errs, err)
		}
	}
	s.pendingPeriodics = failed

	if len(failed) > 0 {
		s.applyFailed = true
		if len(errs) > 0 {
			s.logMaintenanceError(ctx, "jobs: declared periodic apply failed", errors.Join(errs...),
				"pending", len(failed), "retry_in", s.reconcileCadence().String())
		}
		return slugSet(failed)
	}
	if s.applyFailed {
		s.applyFailed = false
		s.logger.InfoContext(ctx, "jobs: declared periodics applied", "periodics", len(s.periodics))
	}
	return nil
}

// reconcileOnce is the reconcile activity's pass. It first retries any
// declaration the start-up apply could not land, then — when the pass is on —
// runs the insert-only reconcile over every other declaration.
//
// A declaration still pending is left out of the reconcile. It was never
// created, so re-creating it there would log drift where there was only a failed
// apply; and one that keeps failing must hold back only itself, not the
// re-creation of every other declared slug.
func (s *Scheduler) reconcileOnce(ctx context.Context) {
	pending := s.applyPendingPeriodics(ctx)
	if s.reconcileInterval <= 0 {
		return
	}
	if _, err := s.reconcilePeriodics(ctx, pending); err != nil {
		s.logMaintenanceError(ctx, "jobs: periodic reconcile failed", err)
	}
}

// ReconcilePeriodics re-creates every declared periodic whose row is missing and
// reports how many it re-created. It only ever inserts: a declared definition that
// exists — active or not, edited by hand or not — is left exactly as it is. A
// re-created definition is written from its declaration with next_run_at at its
// first fire time after now, so it neither fires at once nor backfills what it
// missed, and each is logged once, at warn.
//
// It is safe beside other Schedulers declaring the same periodics: the insert does
// nothing when the slug exists (job_periodics' slug index is unique), and only the
// Scheduler whose insert landed counts and logs it.
//
// The pass reads the declared slugs in one indexed query and writes only what is
// missing, so a pass with nothing missing writes nothing. A failure to re-create
// one definition does not stop the others; every failure is returned, joined, with
// the count of those that landed.
//
// It parallels Tick and Sweep: the reconcile activity calls it every
// ReconcileInterval, and a host driving its own maintenance loop can call it
// directly. It does not apply a changed declaration — Run does that on start —
// and it skips a declaration Run's start-up apply has not landed yet, which that
// apply's retry owns: one that was never created is never reported as re-created.
// It returns (0, nil) without touching the database when nothing is left to check.
func (s *Scheduler) ReconcilePeriodics(ctx context.Context) (int, error) {
	return s.reconcilePeriodics(ctx, s.pendingSlugs())
}

// reconcilePeriodics is ReconcilePeriodics over every declaration whose slug is
// not in skip.
func (s *Scheduler) reconcilePeriodics(ctx context.Context, skip map[string]struct{}) (int, error) {
	specs := make([]PeriodicSpec, 0, len(s.periodics))
	slugs := make([]string, 0, len(s.periodics))
	for i := range s.periodics {
		if _, pending := skip[s.periodics[i].Slug]; pending {
			continue
		}
		specs = append(specs, s.periodics[i])
		slugs = append(slugs, s.periodics[i].Slug)
	}
	if len(slugs) == 0 {
		return 0, nil
	}
	var present []string
	if err := s.db.WithContext(ctx).Model(&jobPeriodicRow{}).
		Where("slug IN ?", slugs).Pluck("slug", &present).Error; err != nil {
		return 0, fmt.Errorf("jobs: reconcile periodics: %w", err)
	}
	exists := make(map[string]struct{}, len(present))
	for _, slug := range present {
		exists[slug] = struct{}{}
	}

	now := models.ClockFrom(ctx).Now(ctx)
	created := 0
	var errs []error
	for _, spec := range specs {
		if _, ok := exists[spec.Slug]; ok {
			continue
		}
		if err := ctx.Err(); err != nil {
			errs = append(errs, fmt.Errorf("jobs: reconcile periodics: %w", err))
			break
		}
		row, inserted, err := insertPeriodic(ctx, s.db, spec, now)
		if err != nil {
			errs = append(errs, fmt.Errorf("jobs: reconcile periodic %q: %w", spec.Slug, err))
			continue
		}
		if !inserted {
			// Another Scheduler re-created it between the read and this insert; the
			// one whose insert landed reports it.
			continue
		}
		created++
		s.logger.WarnContext(ctx, "jobs: declared periodic was missing and has been re-created",
			"slug", spec.Slug, "kind", spec.Kind, "next_run_at", row.NextRunAt)
	}
	return created, errors.Join(errs...)
}
