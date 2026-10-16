package core

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/mrz1836/go-foundation/ctxutil"
	"github.com/mrz1836/go-foundation/models"
	"gorm.io/datatypes"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// ActorType is the kind of principal that ran a control.
type ActorType string

// The actor types a control records.
const (
	// ActorUser is a person: an operator at a console, an API caller, or the CLI.
	ActorUser ActorType = "user"
	// ActorAgent is an automated principal acting for someone: a bot, a script,
	// an assistant.
	ActorAgent ActorType = "agent"
	// ActorSystem is the runtime, or the host, acting on its own. A control
	// called with no actor on its context is recorded as the system.
	ActorSystem ActorType = "system"
)

// Valid reports whether t is one of ActorUser, ActorAgent, and ActorSystem.
func (t ActorType) Valid() bool {
	switch t {
	case ActorUser, ActorAgent, ActorSystem:
		return true
	default:
		return false
	}
}

// The longest reason and source an Actor carries. A control refuses an actor
// past either, rather than record a truncated one.
const (
	// MaxReasonBytes is the longest Actor.Reason, in bytes.
	MaxReasonBytes = 1024
	// MaxActorSourceBytes is the longest Actor.Source, in bytes.
	MaxActorSourceBytes = 64
)

// Actor is who ran a control, and why. A host puts it on the request's context
// once, with WithActor, and every control called with that context records it
// in the history (job_audit). The runtime requires no reason: whether an action
// needs one is the host's policy, and the runtime records what it is given.
type Actor struct {
	// Type is the kind of principal. It must be one of the ActorType constants.
	Type ActorType `json:"type"`
	// ID is the host's id for the principal: a user id, a key id, a bot's name.
	ID string `json:"id,omitempty"`
	// Reason is why the control ran, as the principal gave it, at most
	// MaxReasonBytes bytes.
	Reason string `json:"reason,omitempty"`
	// Source is the channel the principal used, a label the host chooses (an
	// operation name, "cli"), at most MaxActorSourceBytes bytes.
	Source string `json:"source,omitempty"`
	// RequestID ties the entry to the request that caused it. Empty takes the
	// context's request id (go-foundation's ctxutil.RequestIDFrom).
	RequestID string `json:"request_id,omitempty"`
}

// actorKey is the context key WithActor stores the actor under.
type actorKey struct{}

// WithActor returns a copy of ctx carrying a, the actor every control called
// with it records. Each control writes one job_audit entry for every job or
// schedule it changes, in the transaction that makes the change (each batch's,
// for a batched control), recording the state the job was in when the control
// read it under its row lock; a call that changes nothing writes nothing. A
// control called with a context that carries no actor is recorded as
// ActorSystem, with the context's request id.
//
// A control refuses an actor whose Type is not valid, or whose Reason or Source
// is longer than its limit, with a *ValidationError naming the field, before it
// reads or writes anything; the error never quotes the value.
func WithActor(ctx context.Context, a Actor) context.Context {
	return context.WithValue(ctx, actorKey{}, a)
}

// ActorFrom returns the actor WithActor put on ctx and true, or the zero Actor
// and false when ctx carries none.
func ActorFrom(ctx context.Context) (Actor, bool) {
	a, ok := ctx.Value(actorKey{}).(Actor)
	return a, ok
}

// AuditTarget is the kind of thing an audit entry is about.
type AuditTarget string

// The targets an entry names. Its TargetID is the target's stable name: a job's
// id, the queue name, a periodic's slug, the node id, or the limiter resource.
const (
	// AuditTargetJob is a job, named by its id.
	AuditTargetJob AuditTarget = "job"
	// AuditTargetQueue is a queue, named by its name.
	AuditTargetQueue AuditTarget = "queue"
	// AuditTargetPeriodic is a periodic definition, named by its slug, so a
	// schedule deleted and declared again under its slug keeps one history.
	AuditTargetPeriodic AuditTarget = "periodic"
	// AuditTargetNode is a node, named by its id.
	AuditTargetNode AuditTarget = "node"
	// AuditTargetLimiter is a limiter, named by its resource.
	AuditTargetLimiter AuditTarget = "limiter"
)

// AuditAction is what a control did to its target.
type AuditAction string

// The actions the controls record.
const (
	// AuditRetried is a job returned to available by RetryJob or
	// RetryJobWithOptions.
	AuditRetried AuditAction = "retried"
	// AuditReplayed is a job returned to available by Replay or ReplayByParent.
	AuditReplayed AuditAction = "replayed"
	// AuditCancelled is a job cancelled by CancelJob or CancelByParent.
	AuditCancelled AuditAction = "cancelled"
	// AuditPaused is a job paused by PauseByParent or PauseJob, or a queue
	// paused by PauseQueue.
	AuditPaused AuditAction = "paused"
	// AuditResumed is a job resumed by ResumeByParent or ResumeJob, or a queue
	// resumed by ResumeQueue.
	AuditResumed AuditAction = "resumed"
	// AuditActivated is a periodic definition SetPeriodicActive turned on.
	AuditActivated AuditAction = "activated"
	// AuditDeactivated is a periodic definition SetPeriodicActive turned off.
	AuditDeactivated AuditAction = "deactivated"
	// AuditDeleted is a periodic definition DeletePeriodic removed, or a
	// finished job DeleteJob removed.
	AuditDeleted AuditAction = "deleted"
	// AuditRescheduled is a waiting job RescheduleJob moved to another time.
	AuditRescheduled AuditAction = "rescheduled"
	// AuditReprioritized is a waiting job SetJobPriority gave another priority.
	AuditReprioritized AuditAction = "reprioritized"
	// AuditDrainRequested is a node RequestDrain asked to stop taking work.
	AuditDrainRequested AuditAction = "drain_requested"
)

// AuditEntry is one entry of the control history: one change one control made
// to one target.
type AuditEntry struct {
	// ID is the entry's id.
	ID string `json:"id"`
	// CreatedAt is when the control made the change, in UTC: a clock reading the
	// control took in the transaction that made it.
	CreatedAt time.Time `json:"created_at"`
	// TargetType is the kind of thing the change was made to.
	TargetType AuditTarget `json:"target_type"`
	// TargetID is the target's stable name.
	TargetID string `json:"target_id"`
	// Action is what the control did.
	Action AuditAction `json:"action"`
	// Actor is who ran the control, and why.
	Actor Actor `json:"actor"`
	// Details is a JSON object: the state a job changed from (from_state), and
	// what the call was told to do. Its keys depend on the action.
	Details json.RawMessage `json:"details"`
}

// AuditHook is a host's function run in a control's transaction, after the
// control has written its entries, with those entries. It writes a host's own
// record of the change on tx, so that record commits or rolls back with the
// change. Its error rolls the transaction back, and the control returns it.
type AuditHook func(ctx context.Context, tx *gorm.DB, entries []AuditEntry) error

// auditHookKey is the context key WithAuditHook stores the hook under.
type auditHookKey struct{}

// WithAuditHook returns a copy of ctx carrying hook, which every control called
// with it runs once per transaction that wrote entries, on that transaction,
// after the entries, with exactly those entries. A batched control runs it once
// per batch. The hook's error rolls that transaction back — the change and its
// entries with it — and the control returns it, so errors.Is finds it; a batched
// control keeps the batches it committed before. A transaction that wrote no
// entry runs no hook. A nil hook clears one ctx already carries. A host that
// already wraps each control in a transaction of its own, and writes its record
// there, needs no hook.
func WithAuditHook(ctx context.Context, hook AuditHook) context.Context {
	return context.WithValue(ctx, auditHookKey{}, hook)
}

// auditHookFrom returns the hook WithAuditHook put on ctx, or nil.
func auditHookFrom(ctx context.Context) AuditHook {
	hook, _ := ctx.Value(auditHookKey{}).(AuditHook)
	return hook
}

// auditInsertChunk is the most entries one INSERT writes: eleven binds a row,
// 5,500 a statement, under SQLite's 32,766 and PostgreSQL's 65,535 whatever
// batch size a caller chose.
const auditInsertChunk = 500

// resolveActor returns the actor a control records: the context's, or
// ActorSystem when it carries none, an empty RequestID replaced by the
// context's request id. It refuses an actor the history can't record with a
// *ValidationError, before the control runs any SQL. The error never quotes
// the value, which can hold anything a person typed.
func resolveActor(ctx context.Context) (Actor, error) {
	actor, ok := ActorFrom(ctx)
	if !ok {
		actor = Actor{Type: ActorSystem}
	}
	switch {
	case !actor.Type.Valid():
		return Actor{}, newValidationError("actor.type", "is not one of user, agent, or system")
	case len(actor.Reason) > MaxReasonBytes:
		return Actor{}, newValidationError("actor.reason", fmt.Sprintf("is longer than %d bytes", MaxReasonBytes))
	case len(actor.Source) > MaxActorSourceBytes:
		return Actor{}, newValidationError("actor.source", fmt.Sprintf("is longer than %d bytes", MaxActorSourceBytes))
	}
	if actor.RequestID == "" {
		actor.RequestID = ctxutil.RequestIDFrom(ctx)
	}
	return actor, nil
}

// auditChange is one change a control made, before recordAudit writes it.
type auditChange struct {
	target   AuditTarget
	targetID string
	action   AuditAction
	details  map[string]any // nil is written as {}
}

// recordAudit writes one job_audit row per change on tx, attributed to actor
// and stamped at (in UTC), then runs the context's audit hook on tx with the
// entries it wrote. With no change it writes nothing and runs no hook.
func recordAudit(ctx context.Context, tx *gorm.DB, actor Actor, at time.Time, changes []auditChange) error {
	if len(changes) == 0 {
		return nil
	}
	at = at.UTC()
	rows := make([]jobAuditRow, len(changes))
	entries := make([]AuditEntry, len(changes))
	for i, c := range changes {
		details := []byte("{}")
		if len(c.details) > 0 {
			raw, err := json.Marshal(c.details)
			if err != nil {
				return fmt.Errorf("flywheel: record audit entries: %w", err)
			}
			details = raw
		}
		rows[i] = jobAuditRow{
			ID: models.NewID(), CreatedAt: at, TargetType: string(c.target), TargetID: c.targetID,
			Action: string(c.action), ActorType: string(actor.Type), ActorID: actor.ID, Reason: actor.Reason,
			Source: actor.Source, RequestID: actor.RequestID, Details: datatypes.JSON(details),
		}
		entries[i] = AuditEntry{
			ID: rows[i].ID, CreatedAt: at, TargetType: c.target, TargetID: c.targetID, Action: c.action,
			Actor: actor, Details: json.RawMessage(details),
		}
	}
	if err := tx.WithContext(ctx).CreateInBatches(&rows, auditInsertChunk).Error; err != nil {
		return fmt.Errorf("flywheel: record audit entries: %w", err)
	}
	if hook := auditHookFrom(ctx); hook != nil {
		if err := hook(ctx, tx, entries); err != nil {
			return fmt.Errorf("flywheel: audit hook: %w", err)
		}
	}
	return nil
}

// lockJob reads the job a control is about to change, on tx, locking its row on
// PostgreSQL (SQLite serializes writers). A concurrent change therefore commits
// first, and the control decides and records from what it then reads. A missing
// or soft-deleted job is ErrJobNotFound.
func lockJob(ctx context.Context, tx *gorm.DB, id string) (jobRow, error) {
	q := quietMissing(tx).WithContext(ctx).Model(&jobRow{}).
		Select("id", "state", "kind", "parent_job_id", "priority", "scheduled_at", "attempt").
		Where("id = ?", id)
	if tx.Name() == "postgres" {
		q = q.Clauses(clause.Locking{Strength: "UPDATE"})
	}
	var job jobRow
	if err := q.Take(&job).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return jobRow{}, ErrJobNotFound
		}
		return jobRow{}, fmt.Errorf("flywheel: read job %q: %w", id, err)
	}
	return job, nil
}

// updateLockedJob applies upd to job, on tx, guarded on the state lockJob read.
// The row is locked, so the guard always matches; a miss means the row changed
// under the lock, and the control fails rather than record a change it didn't
// make.
func updateLockedJob(tx *gorm.DB, job jobRow, upd map[string]any) error {
	res := tx.Model(&jobRow{}).Where("id = ? AND state = ?", job.ID, job.State).Updates(upd)
	if res.Error != nil {
		return res.Error
	}
	if res.RowsAffected != 1 {
		return fmt.Errorf("flywheel: job %q changed while it was locked", job.ID)
	}
	return nil
}

// lockJobStates reads, on tx, the state of each job in ids, in id order, locking
// the rows the same way lockJob does, for a batch about to update them. The id
// order is the lock order, so two batches never lock in opposite orders. A job
// that is gone or soft-deleted is absent from the map.
func lockJobStates(ctx context.Context, tx *gorm.DB, ids []string) (map[string]JobState, error) {
	q := tx.WithContext(ctx).Model(&jobRow{}).Select("id", "state").Where("id IN ?", ids).Order("id")
	if tx.Name() == "postgres" {
		q = q.Clauses(clause.Locking{Strength: "UPDATE"})
	}
	var rows []jobRow
	if err := q.Find(&rows).Error; err != nil {
		return nil, fmt.Errorf("lock jobs: %w", err)
	}
	states := make(map[string]JobState, len(rows))
	for _, r := range rows {
		states[r.ID] = JobState(r.State)
	}
	return states, nil
}

// updateJobsReturningIDs runs upd on the jobs rows where and args select, on tx,
// and returns the ids it changed, in id order, read back with RETURNING id. Its
// error is the update's own, unwrapped, so a caller still recognizes a duplicate
// key or a deadlock. A database that reports changed rows but returns no ids (a
// GORM handle configured without RETURNING) fails the call, so a batch never
// records less than it changed.
func updateJobsReturningIDs(tx *gorm.DB, where string, args []any, upd map[string]any) ([]string, error) {
	var changed []jobRow
	res := tx.Model(&changed).Clauses(clause.Returning{Columns: []clause.Column{{Name: "id"}}}).
		Where(where, args...).Updates(upd)
	if res.Error != nil {
		return nil, res.Error
	}
	if res.RowsAffected > int64(len(changed)) {
		return nil, fmt.Errorf(
			"flywheel: the database changed %d jobs but returned none of their ids; its GORM dialector must render RETURNING",
			res.RowsAffected,
		)
	}
	ids := make([]string, len(changed))
	for i, r := range changed {
		ids[i] = r.ID
	}
	slices.Sort(ids)
	return ids, nil
}

// controlMiss is the error a control returns for a job in a state it can't act
// on: ErrJobRunning for a running job, ErrJobTerminal for a terminal one, and
// ErrJobNotTerminal otherwise, for a job still waiting, which only DeleteJob
// refuses.
func controlMiss(state JobState) error {
	switch {
	case state == StateRunning:
		return ErrJobRunning
	case slices.Contains(TerminalStates(), state):
		return ErrJobTerminal
	default:
		return ErrJobNotTerminal
	}
}
