package core

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/mrz1836/go-foundation/models"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// QueueInfo is one queue's depth, whether an operator has paused it, and its
// concurrency cap.
type QueueInfo struct {
	// QueueDepth is the queue's point-in-time depth, as QueueDepths reads it.
	// Its Paused field counts the queue's paused jobs; IsPaused is the queue's
	// own pause.
	QueueDepth
	// IsPaused reports that PauseQueue paused the queue and no ResumeQueue has
	// resumed it since, so no Runner claims its jobs.
	IsPaused bool `json:"is_paused"`
	// PausedAt is when the queue was paused, in UTC; nil while it is not.
	PausedAt *time.Time `json:"paused_at,omitempty"`
	// PausedBy is the type, id, and reason of the actor who paused the queue;
	// nil while it is not paused.
	PausedBy *Actor `json:"paused_by,omitempty"`
	// MaxConcurrency is the cap SetQueueConcurrency set: the most jobs a Runner
	// serving the queue runs at once. nil is no cap.
	MaxConcurrency *int `json:"max_concurrency,omitempty"`
}

// PauseQueue pauses queue for every Runner that claims from it: each leaves the
// queue out of its claims within one RunnerConfig.QueueStateRefresh of the
// pause. An attempt already running on the queue finishes, and its jobs keep
// their states — pausing a queue holds it, it does not drain it. A queue no job
// has used yet can be paused, and stays paused for the jobs that arrive.
//
// It writes the pause into the queue's job_queues row, creating it: when, and
// the type, id, and reason of the context's actor (WithActor; the system when
// there is none). In the same transaction — a savepoint when db is a caller's
// transaction — it writes one AuditPaused entry for the queue (AuditTargetQueue)
// to the control history and runs the context's audit hook. Pausing a paused
// queue changes nothing, its first pause's time and actor included, and records
// nothing; concurrent pauses of one queue record it once.
//
// An empty queue name is refused with a *ValidationError naming "queue", and an
// actor the history can't record with a *ValidationError naming its field, each
// before anything is written.
func PauseQueue(ctx context.Context, db *gorm.DB, queue string) error {
	if db == nil {
		return fmt.Errorf("flywheel: PauseQueue: db is nil")
	}
	actor, err := resolveActor(ctx)
	if err != nil {
		return err
	}
	if queue == "" {
		return newValidationError("queue", "is required")
	}
	err = db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		now := models.ClockFrom(ctx).Now(ctx).UTC()
		if err := tx.Clauses(clause.OnConflict{Columns: []clause.Column{{Name: "queue"}}, DoNothing: true}).
			Create(&jobQueueRow{Queue: queue, UpdatedAt: now}).Error; err != nil {
			return err
		}
		res := tx.Model(&jobQueueRow{}).Where("queue = ? AND paused_at IS NULL", queue).Updates(map[string]any{
			"paused_at":      now,
			"paused_by_type": string(actor.Type),
			"paused_by_id":   actor.ID,
			"paused_reason":  actor.Reason,
			"updated_at":     now,
		})
		if res.Error != nil {
			return res.Error
		}
		if res.RowsAffected == 0 {
			return nil
		}
		return recordAudit(ctx, tx, actor, now,
			[]auditChange{{target: AuditTargetQueue, targetID: queue, action: AuditPaused}})
	})
	if err != nil {
		return fmt.Errorf("flywheel: pause queue %q: %w", queue, err)
	}
	return nil
}

// ResumeQueue resumes a queue PauseQueue paused: every Runner that claims from
// it claims its jobs again within one RunnerConfig.QueueStateRefresh. It clears
// the pause from the queue's job_queues row and keeps the row, so ListQueues
// keeps listing the queue.
//
// In the same transaction — a savepoint when db is a caller's transaction — it
// writes one AuditResumed entry for the queue to the control history,
// attributed to the context's actor, and runs the context's audit hook.
// Resuming a queue that isn't paused, or that has no row, changes nothing and
// records nothing. It refuses an empty name and an actor the history can't
// record as PauseQueue does.
func ResumeQueue(ctx context.Context, db *gorm.DB, queue string) error {
	if db == nil {
		return fmt.Errorf("flywheel: ResumeQueue: db is nil")
	}
	actor, err := resolveActor(ctx)
	if err != nil {
		return err
	}
	if queue == "" {
		return newValidationError("queue", "is required")
	}
	err = db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		now := models.ClockFrom(ctx).Now(ctx).UTC()
		res := tx.Model(&jobQueueRow{}).Where("queue = ? AND paused_at IS NOT NULL", queue).Updates(map[string]any{
			"paused_at":      nil,
			"paused_by_type": "",
			"paused_by_id":   "",
			"paused_reason":  "",
			"updated_at":     now,
		})
		if res.Error != nil {
			return res.Error
		}
		if res.RowsAffected == 0 {
			return nil
		}
		return recordAudit(ctx, tx, actor, now,
			[]auditChange{{target: AuditTargetQueue, targetID: queue, action: AuditResumed}})
	})
	if err != nil {
		return fmt.Errorf("flywheel: resume queue %q: %w", queue, err)
	}
	return nil
}

// SetQueueConcurrency caps how many of queue's jobs a Runner serving it runs at
// once, or clears the cap when n is nil. Every Runner serving the queue applies
// it within one RunnerConfig.QueueStateRefresh, even with every slot busy, and
// a Runner with QueueStateRefresh negative never reads it. A lowered cap
// interrupts nothing: running jobs finish, and the Runner claims nothing more
// until fewer of them run than the cap. A cap above a Runner's Concurrency
// changes nothing there.
//
// The cap is per Runner, not fleet-wide: ten Runners serving a queue capped at
// 2 run up to twenty of its jobs, and a Runner serving several queues runs at
// most the smallest of their caps, in total. A shared DBLimiter is the
// fleet-wide gate (see SetLimiterBudget); a queue that needs a cap of its own
// gets a Runner of its own.
//
// It writes the cap into the queue's job_queues row, creating it when absent
// (clearing a queue that has no row writes nothing), and keeps any pause. In
// the same transaction — a savepoint when db is a caller's transaction — it
// writes one AuditLimitsSet entry for the queue (AuditTargetQueue) to the
// control history, with the cap before and after as "from" and "to" (null for
// none), attributed to the context's actor (WithActor; the system when there
// is none), and runs the context's audit hook. Setting the cap already set
// changes nothing and records nothing.
//
// An empty queue name is refused with a *ValidationError naming "queue", an n
// below 1 with one naming "max_concurrency", and an actor the history can't
// record with one naming its field, each before anything is written.
func SetQueueConcurrency(ctx context.Context, db *gorm.DB, queue string, n *int) error {
	if db == nil {
		return fmt.Errorf("flywheel: SetQueueConcurrency: db is nil")
	}
	actor, err := resolveActor(ctx)
	if err != nil {
		return err
	}
	if queue == "" {
		return newValidationError("queue", "is required")
	}
	if n != nil && *n < 1 {
		return newValidationError("max_concurrency", "must be at least 1")
	}
	err = db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		now := models.ClockFrom(ctx).Now(ctx).UTC()
		if n != nil {
			if err := tx.Clauses(clause.OnConflict{Columns: []clause.Column{{Name: "queue"}}, DoNothing: true}).
				Create(&jobQueueRow{Queue: queue, UpdatedAt: now}).Error; err != nil {
				return err
			}
		}
		q := quietMissing(tx).Model(&jobQueueRow{}).Select("queue", "max_concurrency").Where("queue = ?", queue)
		if tx.Name() == "postgres" {
			q = q.Clauses(clause.Locking{Strength: "UPDATE"})
		}
		var row jobQueueRow
		if err := q.Take(&row).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return nil // no row and no cap to set: nothing to clear
			}
			return err
		}
		before := row.MaxConcurrency
		if (before == nil) == (n == nil) && (n == nil || *before == *n) {
			return nil
		}
		if err := tx.Model(&jobQueueRow{}).Where("queue = ?", queue).
			Updates(map[string]any{"max_concurrency": n, "updated_at": now}).Error; err != nil {
			return err
		}
		return recordAudit(ctx, tx, actor, now, []auditChange{{
			target: AuditTargetQueue, targetID: queue, action: AuditLimitsSet,
			details: map[string]any{"from": before, "to": n},
		}})
	})
	if err != nil {
		return fmt.Errorf("flywheel: set queue concurrency %q: %w", queue, err)
	}
	return nil
}

// ListQueues returns every queue that holds a non-terminal job or has a
// job_queues row (a queue an operator has paused or capped, now or before),
// sorted by name: each with QueueDepths' figures, zero for a queue no job
// holds, its pause, and its concurrency cap. It returns every such queue,
// uncapped: a host's queues are bounded by its configuration, not by its data.
//
// It costs what QueueDepths costs, plus one read of job_queues, which holds a
// row per queue ever paused. Like QueueDepths it is a sample, not a
// transaction.
func ListQueues(ctx context.Context, db *gorm.DB) ([]QueueInfo, error) {
	if db == nil {
		return nil, fmt.Errorf("flywheel: ListQueues: db is nil")
	}
	depths, err := QueueDepths(ctx, db)
	if err != nil {
		return nil, fmt.Errorf("flywheel: ListQueues: %w", err)
	}
	var rows []jobQueueRow
	if err := db.WithContext(ctx).Order("queue").Find(&rows).Error; err != nil {
		return nil, fmt.Errorf("flywheel: ListQueues: queue state: %w", err)
	}

	byName := make(map[string]*QueueInfo, len(depths)+len(rows))
	for _, d := range depths {
		byName[d.Queue] = &QueueInfo{QueueDepth: d}
	}
	for _, row := range rows {
		info, ok := byName[row.Queue]
		if !ok {
			info = &QueueInfo{QueueDepth: QueueDepth{Queue: row.Queue}}
			byName[row.Queue] = info
		}
		info.MaxConcurrency = row.MaxConcurrency
		if row.PausedAt == nil {
			continue
		}
		at := row.PausedAt.UTC()
		info.IsPaused = true
		info.PausedAt = &at
		info.PausedBy = &Actor{Type: ActorType(row.PausedByType), ID: row.PausedByID, Reason: row.PausedReason}
	}

	out := make([]QueueInfo, 0, len(byName))
	for _, info := range byName {
		out = append(out, *info)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Queue < out[j].Queue })
	return out, nil
}
