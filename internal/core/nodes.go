package core

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/mrz1836/go-foundation/models"
	"gorm.io/gorm"
)

// ErrNodeNotFound is returned by RequestDrain when no node that has not stopped
// has the id.
var ErrNodeNotFound = errors.New("flywheel: node not found")

// defaultNodeStaleAfter is how long a node may go without a heartbeat before
// ListNodes flags it stale, when ListNodesParams.StaleAfter is zero.
const defaultNodeStaleAfter = time.Minute

// defaultNodeListLimit is how many nodes ListNodes returns when
// ListNodesParams.Limit is zero or negative.
const defaultNodeListLimit = 100

// NodeRunner is one runner of a node, as the node last reported it.
type NodeRunner struct {
	// Queues are the queues the runner claims from.
	Queues []string `json:"queues"`
	// ExecutorClass is the routing label the runner serves.
	ExecutorClass ExecutorClass `json:"executor_class"`
	// Concurrency is the runner's slot count.
	Concurrency int `json:"concurrency"`
	// InFlight is how many of its slots were running a job at the node's last
	// heartbeat.
	InFlight int `json:"in_flight"`
}

// NodeInfo is one process lifetime of a Node that registers itself
// (NodeConfig.RegistryInterval): what it runs, and how recently it said so.
//
// A run belongs to the node whose ExecutorID equals the run's executor_id
// (JobRunView.ExecutorID) and whose span — [StartedAt, StoppedAt], or
// [StartedAt, HeartbeatAt] while it has not stopped — covers the run's
// started_at. Both halves are needed: a host and process id repeat across
// restarts, so an ExecutorID alone can match several nodes. The rule holds for
// one registering Node per process, since two in one process share an
// executor identity and overlap in time.
//
// A node that dies without stopping writes no StoppedAt: it reads Stale from
// StaleAfter after its last heartbeat until node retention
// (SchedulerConfig.NodeRetention) prunes its row.
type NodeInfo struct {
	// ID is minted when the node starts: one per process lifetime.
	ID string `json:"id"`
	// ExecutorID is the node's host:pid, the job_runs.executor_id of its runs.
	ExecutorID string `json:"executor_id"`
	// Host is the node's hostname.
	Host string `json:"host"`
	// Version and Commit are what the host set in NodeConfig, empty when unset.
	Version string `json:"version,omitempty"`
	Commit  string `json:"commit,omitempty"`
	// StartedAt is when the node started; HeartbeatAt when it last checked in.
	// Both are UTC.
	StartedAt   time.Time `json:"started_at"`
	HeartbeatAt time.Time `json:"heartbeat_at"`
	// StoppedAt is when the node's Run returned, in UTC; nil while it runs, and
	// for a node that died without stopping.
	StoppedAt *time.Time `json:"stopped_at,omitempty"`
	// Stale reports a node that has not stopped and whose heartbeat is older
	// than ListNodesParams.StaleAfter, by the context's clock.
	Stale bool `json:"stale"`
	// Scheduler reports that the node runs a Scheduler.
	Scheduler bool `json:"scheduler"`
	// Draining reports that the node saw a drain request and stopped its
	// runners' claims; Drained, that it is draining with nothing in flight.
	Draining bool `json:"draining"`
	Drained  bool `json:"drained"`
	// DrainRequestedAt is when RequestDrain asked the node to drain, in UTC.
	DrainRequestedAt *time.Time `json:"drain_requested_at,omitempty"`
	// Runners are the node's runners, as of its last heartbeat.
	Runners []NodeRunner `json:"runners"`
	// Kinds is every kind its runners' registries register, sorted.
	Kinds []string `json:"kinds"`
}

// ListNodesParams bounds a ListNodes call. Its zero value lists up to 100
// nodes that have not stopped, flagging those silent for over a minute as
// stale.
type ListNodesParams struct {
	// StoppedSince also lists the nodes that stopped at or after it. Zero lists
	// only the nodes that have not stopped.
	StoppedSince time.Time
	// StaleAfter is how long a node may go without a heartbeat before it reads
	// Stale. Zero (or negative) selects one minute; keep it well above the
	// nodes' RegistryInterval, or a healthy node reads stale between beats.
	StaleAfter time.Duration
	// Limit caps how many nodes are returned. Zero (or negative) selects 100;
	// above MaxReadLimit reads MaxReadLimit.
	Limit int
}

// ListNodes returns the registered nodes that have not stopped, then, with a
// non-zero StoppedSince, those that stopped at or after it; each group newest
// start first (then by id), at most Limit. A node is Stale when it has not
// stopped and its heartbeat is older than StaleAfter by the context's clock.
//
// It reads job_nodes alone, which holds one row per registering process
// lifetime within node retention; the stale flag is computed from the rows it
// returns. A node pruned by retention is gone, so StoppedSince reaches no
// further back than SchedulerConfig.NodeRetention.
func ListNodes(ctx context.Context, db *gorm.DB, p ListNodesParams) ([]NodeInfo, error) {
	if db == nil {
		return nil, fmt.Errorf("flywheel: ListNodes: db is nil")
	}
	limit := p.Limit
	switch {
	case limit <= 0:
		limit = defaultNodeListLimit
	case limit > MaxReadLimit:
		limit = MaxReadLimit
	}
	staleAfter := p.StaleAfter
	if staleAfter <= 0 {
		staleAfter = defaultNodeStaleAfter
	}
	now := models.ClockFrom(ctx).Now(ctx)

	q := db.WithContext(ctx).Model(&jobNodeRow{})
	if p.StoppedSince.IsZero() {
		q = q.Where("stopped_at IS NULL")
	} else {
		q = q.Where("stopped_at IS NULL OR stopped_at >= ?", p.StoppedSince.UTC())
	}
	var rows []jobNodeRow
	if err := q.Order("CASE WHEN stopped_at IS NULL THEN 0 ELSE 1 END, started_at DESC, id DESC").
		Limit(limit).Find(&rows).Error; err != nil {
		return nil, fmt.Errorf("flywheel: ListNodes: %w", err)
	}

	out := make([]NodeInfo, len(rows))
	for i, row := range rows {
		info, err := nodeInfoFromRow(row)
		if err != nil {
			return nil, fmt.Errorf("flywheel: ListNodes: node %q: %w", row.ID, err)
		}
		info.Stale = info.StoppedAt == nil && now.Sub(info.HeartbeatAt) > staleAfter
		out[i] = info
	}
	return out, nil
}

// nodeInfoFromRow maps a job_nodes row to its NodeInfo, decoding its runners
// and kinds and deriving Drained; Stale is the caller's, since it needs a clock.
func nodeInfoFromRow(row jobNodeRow) (NodeInfo, error) {
	info := NodeInfo{
		ID: row.ID, ExecutorID: row.ExecutorID, Host: row.Host, Version: row.Version, Commit: row.CommitHash,
		StartedAt: row.StartedAt.UTC(), HeartbeatAt: row.HeartbeatAt.UTC(), StoppedAt: utcPtr(row.StoppedAt),
		Scheduler: row.Scheduler, Draining: row.Draining, DrainRequestedAt: utcPtr(row.DrainRequestedAt),
		Runners: []NodeRunner{}, Kinds: []string{},
	}
	if len(row.Runners) > 0 {
		if err := json.Unmarshal(row.Runners, &info.Runners); err != nil {
			return NodeInfo{}, fmt.Errorf("decode runners: %w", err)
		}
	}
	if len(row.Kinds) > 0 {
		if err := json.Unmarshal(row.Kinds, &info.Kinds); err != nil {
			return NodeInfo{}, fmt.Errorf("decode kinds: %w", err)
		}
	}
	if info.Draining {
		info.Drained = true
		for _, r := range info.Runners {
			if r.InFlight > 0 {
				info.Drained = false
				break
			}
		}
	}
	return info, nil
}

// utcPtr returns t in UTC, or nil for a nil t.
func utcPtr(t *time.Time) *time.Time {
	if t == nil {
		return nil
	}
	u := t.UTC()
	return &u
}

// RequestDrain asks a registered node to stop taking work: within one
// heartbeat (NodeConfig.RegistryInterval) the node stops every runner's claims
// and reports Draining, then Drained once nothing is in flight. It keeps
// running — its health server and its Scheduler — until its context ends, and
// since a stopped Runner cannot restart, it takes work again only after a
// restart.
//
// It stamps the node's drain_requested_at and writes one AuditDrainRequested
// entry for the node (AuditTargetNode) to the control history, attributed to the
// context's actor (WithActor; the system when there is none), in one
// transaction — a savepoint when db is a caller's transaction — that also runs
// the context's audit hook. A drain already requested changes nothing and
// records nothing. A node that has stopped, or an id no node has, is
// ErrNodeNotFound.
//
// An empty id is refused with a *ValidationError naming "node_id", and an actor
// the history can't record with a *ValidationError naming its field, each
// before anything is written.
func RequestDrain(ctx context.Context, db *gorm.DB, nodeID string) error {
	if db == nil {
		return fmt.Errorf("flywheel: RequestDrain: db is nil")
	}
	actor, err := resolveActor(ctx)
	if err != nil {
		return err
	}
	if nodeID == "" {
		return newValidationError("node_id", "is required")
	}
	err = db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		now := models.ClockFrom(ctx).Now(ctx).UTC()
		res := tx.Model(&jobNodeRow{}).
			Where("id = ? AND stopped_at IS NULL AND drain_requested_at IS NULL", nodeID).
			Update("drain_requested_at", now)
		if res.Error != nil {
			return res.Error
		}
		if res.RowsAffected == 1 {
			return recordAudit(ctx, tx, actor, now,
				[]auditChange{{target: AuditTargetNode, targetID: nodeID, action: AuditDrainRequested}})
		}
		var rows []jobNodeRow
		if err := tx.Select("id", "stopped_at").Where("id = ?", nodeID).Limit(1).Find(&rows).Error; err != nil {
			return err
		}
		if len(rows) == 0 || rows[0].StoppedAt != nil {
			return ErrNodeNotFound
		}
		return nil
	})
	if err != nil && !errors.Is(err, ErrNodeNotFound) {
		return fmt.Errorf("flywheel: request drain of node %q: %w", nodeID, err)
	}
	return err
}
