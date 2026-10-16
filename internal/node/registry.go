package node

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	core "github.com/mrz1836/go-flywheel/internal/core"
	"github.com/mrz1836/go-foundation/models"
	"gorm.io/datatypes"
	"gorm.io/gorm"
)

// stopWriteTimeout bounds the write that stamps a node's stop time, which runs
// after the caller's context has ended.
const stopWriteTimeout = 5 * time.Second

// Node-row retention, applied by a registering Node that runs a Scheduler.
const (
	// defaultNodeRetention is how long a node's row is kept after its last
	// heartbeat when SchedulerConfig.NodeRetention is zero.
	defaultNodeRetention = 7 * 24 * time.Hour
	// nodePruneInterval is how often the prune runs after the one at start.
	nodePruneInterval = time.Hour
	// nodePruneBatch is the most rows one prune statement deletes.
	nodePruneBatch = 1000
)

// nodeRow is job_nodes as a registering Node writes it. It is the node's own
// model of the runtime's table, kept column for column with the one Models
// returns, which TestTheNodeWritesEveryColumnTheRuntimeDeclares holds it to.
// Its times are UTC.
type nodeRow struct {
	ID               string         `gorm:"column:id;primaryKey"`
	ExecutorID       string         `gorm:"column:executor_id;not null"`
	Host             string         `gorm:"column:host;not null;default:''"`
	Version          string         `gorm:"column:version;not null;default:''"`
	CommitHash       string         `gorm:"column:commit_hash;not null;default:''"`
	StartedAt        time.Time      `gorm:"column:started_at;not null"`
	HeartbeatAt      time.Time      `gorm:"column:heartbeat_at;not null"`
	StoppedAt        *time.Time     `gorm:"column:stopped_at"`
	Scheduler        bool           `gorm:"column:scheduler;not null;default:false"`
	Draining         bool           `gorm:"column:draining;not null;default:false"`
	DrainRequestedAt *time.Time     `gorm:"column:drain_requested_at"`
	Runners          datatypes.JSON `gorm:"column:runners;type:jsonb;not null;default:'[]'"`
	Kinds            datatypes.JSON `gorm:"column:kinds;type:jsonb;not null;default:'[]'"`
}

// TableName binds nodeRow to the job_nodes table.
func (nodeRow) TableName() string { return "job_nodes" }

// registration is a registering Node's row and its upkeep: the insert at
// start, a heartbeat every interval that also answers a drain request, the
// prune of old rows when the node runs a Scheduler, and the stop time when Run
// returns. A failed write is logged and retried at the next beat; it never
// stops the node or its runners.
type registration struct {
	db        *gorm.DB
	logger    *slog.Logger
	interval  time.Duration
	runners   []*core.Runner
	configs   []core.RunnerConfig
	id        string
	executor  string
	host      string
	version   string
	commit    string
	scheduler bool
	kinds     datatypes.JSON
	// retention is how long another node's row is kept after its last
	// heartbeat; zero when the node runs no Scheduler, which prunes nothing.
	retention time.Duration

	// mu guards what the beats change: whether the row has been written, when
	// the node started (which a re-inserted row keeps), whether it is draining,
	// and when it last pruned.
	mu        sync.Mutex
	inserted  bool
	startedAt time.Time
	draining  bool
	lastPrune time.Time
}

// newRegistration builds a Node's registration, or nil when cfg leaves the node
// unregistered. Its executor identity follows the runtime's rule — host:pid,
// with "unknown" for a host that cannot be read — so it is the job_runs.executor_id
// its runners write.
func newRegistration(cfg NodeConfig, runners []*core.Runner, logger *slog.Logger) (*registration, error) {
	if cfg.RegistryInterval <= 0 {
		return nil, nil //nolint:nilnil // a nil registration is an unregistered node
	}
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "unknown"
	}
	var kinds []string
	for _, rc := range cfg.Runners {
		for _, k := range rc.Registry.Kinds() {
			if !slices.Contains(kinds, k.Kind) {
				kinds = append(kinds, k.Kind)
			}
		}
	}
	slices.Sort(kinds)
	if kinds == nil {
		kinds = []string{}
	}
	rawKinds, err := json.Marshal(kinds)
	if err != nil {
		return nil, fmt.Errorf("flywheel: node registry: encode kinds: %w", err)
	}
	var retention time.Duration
	if cfg.Scheduler != nil {
		retention = cfg.Scheduler.NodeRetention
		if retention <= 0 {
			retention = defaultNodeRetention
		}
	}
	return &registration{
		db: cfg.Runners[0].DB, logger: logger, interval: cfg.RegistryInterval,
		runners: runners, configs: cfg.Runners,
		id: models.NewID(), executor: host + ":" + strconv.Itoa(os.Getpid()), host: host,
		version: cfg.Version, commit: cfg.Commit, scheduler: cfg.Scheduler != nil,
		kinds: datatypes.JSON(rawKinds), retention: retention,
	}, nil
}

// checkSchema refuses a database whose job_nodes lacks a table or column the
// runtime declares, before any component starts. An inspection that cannot run
// is not a verdict: it is logged, and the row's writes retry at each beat.
func (g *registration) checkSchema(ctx context.Context) error {
	drift, err := core.InspectSchema(ctx, g.db)
	if err != nil {
		g.logger.WarnContext(ctx, "flywheel: node registry schema check failed", "error", err)
		return nil
	}
	var missing []string
	for _, d := range drift {
		if d.Table == "job_nodes" {
			missing = append(missing, d.String())
		}
	}
	if len(missing) == 0 {
		return nil
	}
	return fmt.Errorf(
		"flywheel: node registry: %w: missing %s; migrate before deploying this binary: run Migrate "+
			"(`flywheel migrate`) on a library-owned install, or apply the upgrade DDL in docs/INTEGRATING.md "+
			"on a host-owned one",
		core.ErrSchemaOutdated, strings.Join(missing, ", "),
	)
}

// start writes the node's row: started and heartbeating now.
func (g *registration) start(ctx context.Context) {
	now := models.ClockFrom(ctx).Now(ctx).UTC()
	g.mu.Lock()
	defer g.mu.Unlock()
	g.startedAt = now
	g.insert(ctx, now)
}

// insert writes the row as of now, recording whether it landed. The caller
// holds mu.
func (g *registration) insert(ctx context.Context, now time.Time) {
	row := nodeRow{
		ID: g.id, ExecutorID: g.executor, Host: g.host, Version: g.version, CommitHash: g.commit,
		StartedAt: g.startedAt, HeartbeatAt: now, Scheduler: g.scheduler, Draining: g.draining,
		Runners: g.runnersJSON(), Kinds: g.kinds,
	}
	if err := g.db.WithContext(ctx).Create(&row).Error; err != nil {
		g.logger.WarnContext(ctx, "flywheel: node registry heartbeat failed", "node_id", g.id, "error", err)
		return
	}
	g.inserted = true
}

// run prunes old rows once, when the node runs a Scheduler, then heartbeats
// the row every interval until ctx ends.
func (g *registration) run(ctx context.Context) error {
	g.mu.Lock()
	g.prune(ctx)
	g.mu.Unlock()
	ticker := time.NewTicker(g.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			g.beat(ctx)
		}
	}
}

// beat writes one heartbeat. It first answers a drain request: once one is
// recorded on the row, it stops every runner's claims and reports draining
// from this beat on. Then it writes the time, the runners' in-flight counts,
// and the drain state, re-inserting the row when the update finds none, and
// prunes old rows when an hour has passed since the last prune.
func (g *registration) beat(ctx context.Context) {
	now := models.ClockFrom(ctx).Now(ctx).UTC()
	g.mu.Lock()
	defer g.mu.Unlock()
	if !g.inserted {
		g.insert(ctx, now)
		return
	}
	if !g.draining {
		g.answerDrain(ctx)
	}
	res := g.db.WithContext(ctx).Model(&nodeRow{}).Where("id = ?", g.id).Updates(map[string]any{
		"heartbeat_at": now,
		"runners":      g.runnersJSON(),
		"draining":     g.draining,
	})
	switch {
	case res.Error != nil:
		if ctx.Err() == nil {
			g.logger.WarnContext(ctx, "flywheel: node registry heartbeat failed", "node_id", g.id, "error", res.Error)
		}
	case res.RowsAffected == 0:
		g.insert(ctx, now)
	}
	if time.Since(g.lastPrune) >= nodePruneInterval {
		g.prune(ctx)
	}
}

// answerDrain reads whether a drain has been requested and, if so, stops every
// runner: each finishes what it has in flight and claims nothing more, and since
// a stopped Runner cannot restart, the node takes work again only after a
// restart. The caller holds mu.
func (g *registration) answerDrain(ctx context.Context) {
	var rows []nodeRow
	if err := g.db.WithContext(ctx).Select("drain_requested_at").Where("id = ?", g.id).Limit(1).
		Find(&rows).Error; err != nil {
		if ctx.Err() == nil {
			g.logger.WarnContext(ctx, "flywheel: node registry heartbeat failed", "node_id", g.id, "error", err)
		}
		return
	}
	if len(rows) == 0 || rows[0].DrainRequestedAt == nil {
		return
	}
	for _, r := range g.runners {
		r.Stop()
	}
	g.draining = true
	g.logger.InfoContext(ctx, "flywheel: node draining: a drain was requested; its runners claim nothing more",
		"node_id", g.id)
}

// prune deletes the rows of nodes whose heartbeat is older than the retention,
// in id-ordered batches of at most nodePruneBatch, until a batch comes up short
// or ctx ends. A node that runs no Scheduler prunes nothing. A failure is
// logged, and the prune runs again an hour later. The caller holds mu.
func (g *registration) prune(ctx context.Context) {
	if g.retention <= 0 {
		return
	}
	g.lastPrune = time.Now()
	cutoff := models.ClockFrom(ctx).Now(ctx).UTC().Add(-g.retention)
	for ctx.Err() == nil {
		batch := g.db.Model(&nodeRow{}).Select("id").
			Where("heartbeat_at < ? AND id <> ?", cutoff, g.id).Order("id").Limit(nodePruneBatch)
		res := g.db.WithContext(ctx).Where("id IN (?)", batch).Delete(&nodeRow{})
		if res.Error != nil {
			if ctx.Err() == nil {
				g.logger.WarnContext(ctx, "flywheel: node registry prune failed", "node_id", g.id, "error", res.Error)
			}
			return
		}
		if res.RowsAffected < nodePruneBatch {
			return
		}
	}
}

// stop stamps the node's stop time, with its runners' last in-flight counts, on
// a context of its own bounded by stopWriteTimeout: ctx has ended by now. A
// failure is logged.
func (g *registration) stop(ctx context.Context) {
	writeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), stopWriteTimeout)
	defer cancel()
	now := models.ClockFrom(ctx).Now(ctx).UTC()
	g.mu.Lock()
	defer g.mu.Unlock()
	if err := g.db.WithContext(writeCtx).Model(&nodeRow{}).Where("id = ?", g.id).Updates(map[string]any{
		"heartbeat_at": now,
		"stopped_at":   now,
		"runners":      g.runnersJSON(),
		"draining":     g.draining,
	}).Error; err != nil {
		g.logger.WarnContext(ctx, "flywheel: node registry stop failed", "node_id", g.id, "error", err)
	}
}

// runnersJSON reports each runner's queues, executor class, slot count, and the
// jobs it has in flight now.
func (g *registration) runnersJSON() datatypes.JSON {
	out := make([]core.NodeRunner, len(g.runners))
	for i, r := range g.runners {
		cfg := g.configs[i]
		out[i] = core.NodeRunner{
			Queues: cfg.Queues, ExecutorClass: cfg.ExecutorClass, Concurrency: max(cfg.Concurrency, 1),
			InFlight: r.InFlight(),
		}
	}
	raw, err := json.Marshal(out)
	if err != nil {
		return datatypes.JSON("[]")
	}
	return datatypes.JSON(raw)
}
