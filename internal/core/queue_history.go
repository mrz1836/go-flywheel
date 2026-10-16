package core

import (
	"context"
	"fmt"
	"time"

	"github.com/mrz1836/go-foundation/models"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// MaxHistoryPoints is the most buckets QueueDepthHistory returns: a day of
// minutes.
const MaxHistoryPoints = 1440

// defaultQueueSampleRetention is how long queue samples are kept when
// SchedulerConfig.QueueSampleRetention is zero.
const defaultQueueSampleRetention = 30 * 24 * time.Hour

// QueueHistoryParams scopes a QueueDepthHistory read.
type QueueHistoryParams struct {
	// Queue is the queue to read. Required.
	Queue string
	// From and To bound the history: samples taken in [From, To). From is
	// required; a zero To selects the context clock's now.
	From, To time.Time
	// Step is the bucket width, a positive whole number of minutes. Zero selects
	// the smallest whole minute that keeps the points at or under
	// MaxHistoryPoints.
	Step time.Duration
}

// QueueDepthPoint is one bucket of a queue's depth history: what the samples
// taken in [Start, Start + Step) saw at most.
type QueueDepthPoint struct {
	Start time.Time `json:"start"`
	// Samples is how many samples fell in the bucket; 0 when none did — the
	// sampler was off, or the queue held no live job — and every count is then
	// zero.
	Samples int `json:"samples"`
	// Ready, ScheduledAhead, Running, Paused, and OldestReadyAge are each the
	// bucket's maximum over its samples, so a spike shows rather than averaging
	// away. Each sample is QueueDepth's reading at its instant.
	Ready          int64         `json:"ready"`
	ScheduledAhead int64         `json:"scheduled_ahead"`
	Running        int64         `json:"running"`
	Paused         int64         `json:"paused"`
	OldestReadyAge time.Duration `json:"oldest_ready_age"`
}

// QueueDepthHistory returns a queue's depth over [From, To), one point per
// bucket, empty buckets included, so a chart needs no gap filling. It reads the
// samples a Scheduler with SchedulerConfig.QueueSampleInterval set writes to
// job_queue_samples — one primary-key range read — and keeps each count's
// maximum per bucket. Buckets start on Step boundaries of the Unix epoch, the
// first at or before From, so two reads of overlapping windows agree on their
// buckets. Without sampling, every point has zero Samples.
//
// An empty Queue, a zero From, a To not after From, a Step that is not a
// positive whole number of minutes, and a Step that gives more than
// MaxHistoryPoints buckets are refused with a ValidationError naming queue,
// from, window, or step, before anything is read.
func QueueDepthHistory(ctx context.Context, db *gorm.DB, p QueueHistoryParams) ([]QueueDepthPoint, error) {
	if db == nil {
		return nil, fmt.Errorf("flywheel: QueueDepthHistory: db is nil")
	}
	if p.Queue == "" {
		return nil, newValidationError("queue", "is required")
	}
	if p.From.IsZero() {
		return nil, newValidationError("from", "is required")
	}
	from, to := p.From.UTC(), p.To.UTC()
	if p.To.IsZero() {
		to = models.ClockFrom(ctx).Now(ctx).UTC()
	}
	if !from.Before(to) {
		return nil, newValidationError("window", "requires From before To")
	}
	step, err := historyStep(from, to, p.Step)
	if err != nil {
		return nil, err
	}

	start := epochFloor(from, step)
	points := make([]QueueDepthPoint, 0, historyBuckets(start, to, step))
	for b := start; b.Before(to); b = b.Add(step) {
		points = append(points, QueueDepthPoint{Start: b})
	}
	rows, err := db.WithContext(ctx).Raw(`SELECT sampled_at, ready, scheduled_ahead, running, paused, oldest_ready_ms `+
		`FROM job_queue_samples WHERE queue = ? AND sampled_at >= ? AND sampled_at < ? ORDER BY sampled_at`,
		p.Queue, from, to).Rows()
	if err != nil {
		return nil, fmt.Errorf("flywheel: QueueDepthHistory: %w", err)
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var (
			s       jobQueueSampleRow
			sampled any
		)
		if err := rows.Scan(&sampled, &s.Ready, &s.ScheduledAhead, &s.Running, &s.Paused, &s.OldestReadyMs); err != nil {
			return nil, fmt.Errorf("flywheel: QueueDepthHistory: scan: %w", err)
		}
		if s.SampledAt, err = scannedTime(sampled); err != nil {
			return nil, fmt.Errorf("flywheel: QueueDepthHistory: %w", err)
		}
		i := int(s.SampledAt.Sub(start) / step)
		if i < 0 || i >= len(points) {
			continue
		}
		pt := &points[i]
		pt.Samples++
		pt.Ready = max(pt.Ready, s.Ready)
		pt.ScheduledAhead = max(pt.ScheduledAhead, s.ScheduledAhead)
		pt.Running = max(pt.Running, s.Running)
		pt.Paused = max(pt.Paused, s.Paused)
		pt.OldestReadyAge = max(pt.OldestReadyAge, time.Duration(s.OldestReadyMs)*time.Millisecond)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("flywheel: QueueDepthHistory: %w", err)
	}
	return points, nil
}

// historyStep resolves a history's bucket width for [from, to): a positive
// step must be a whole number of minutes, and a zero one is the smallest whole
// minute that keeps the buckets at or under MaxHistoryPoints. Either is refused
// when it gives more.
func historyStep(from, to time.Time, step time.Duration) (time.Duration, error) {
	switch {
	case step < 0 || step%time.Minute != 0:
		return 0, newValidationError("step", fmt.Sprintf("%s is not a positive whole number of minutes", step))
	case step == 0:
		span := to.Sub(from)
		perPoint := time.Duration(MaxHistoryPoints) * time.Minute
		step = max(time.Minute, (span+perPoint-1)/perPoint/time.Minute*time.Minute)
		for historyBuckets(epochFloor(from, step), to, step) > MaxHistoryPoints {
			step += time.Minute
		}
		return step, nil
	}
	if n := historyBuckets(epochFloor(from, step), to, step); n > MaxHistoryPoints {
		return 0, newValidationError("step",
			fmt.Sprintf("%s gives %d points over the window, more than %d", step, n, MaxHistoryPoints))
	}
	return step, nil
}

// epochFloor returns the last step boundary of the Unix epoch at or before t,
// in UTC.
func epochFloor(t time.Time, step time.Duration) time.Time {
	ns, s := t.UnixNano(), int64(step)
	return time.Unix(0, ns-((ns%s)+s)%s).UTC()
}

// historyBuckets counts the buckets of step from start that begin before to.
func historyBuckets(start, to time.Time, step time.Duration) int {
	return int((to.Sub(start) + step - 1) / step)
}

// sampleQueuesOnce writes one job_queue_samples row per queue QueueDepths
// reports, at the context clock's now in UTC — a repeat at the same instant,
// from a second Scheduler, is kept once — then deletes the samples older than
// QueueSampleRetention. A failure is logged and swallowed, like every
// maintenance pass's.
func (s *Scheduler) sampleQueuesOnce(ctx context.Context) {
	if err := s.sampleQueues(ctx); err != nil {
		s.logMaintenanceError(ctx, "jobs: queue sample failed", err)
	}
}

// sampleQueues is sampleQueuesOnce's work, returning its error.
func (s *Scheduler) sampleQueues(ctx context.Context) error {
	now := models.ClockFrom(ctx).Now(ctx).UTC()
	depths, err := QueueDepths(ctx, s.db)
	if err != nil {
		return err
	}
	if len(depths) > 0 {
		rows := make([]jobQueueSampleRow, len(depths))
		for i, d := range depths {
			rows[i] = jobQueueSampleRow{
				Queue: d.Queue, SampledAt: now, Ready: d.Ready, ScheduledAhead: d.ScheduledAhead,
				Running: d.Running, Paused: d.Paused, OldestReadyMs: d.OldestReadyAge.Milliseconds(),
			}
		}
		if err := s.db.WithContext(ctx).Clauses(clause.OnConflict{DoNothing: true}).Create(&rows).Error; err != nil {
			return fmt.Errorf("write queue samples: %w", err)
		}
	}
	if err := s.db.WithContext(ctx).Where("sampled_at < ?", now.Add(-s.queueSampleRetention)).
		Delete(&jobQueueSampleRow{}).Error; err != nil {
		return fmt.Errorf("prune queue samples: %w", err)
	}
	return nil
}
