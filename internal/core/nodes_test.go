package core

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/mrz1836/go-foundation/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/datatypes"
	"gorm.io/gorm"
)

// nodesNow is the fixed clock reading the node tests run at.
//
//nolint:gochecknoglobals // shared fixture
var nodesNow = time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

// seedNode writes a job_nodes row as a registering Node would, defaulting its
// executor identity, host, and JSON columns.
func seedNode(t testing.TB, db *gorm.DB, row jobNodeRow) {
	t.Helper()
	if row.ExecutorID == "" {
		row.ExecutorID = "host-" + row.ID + ":42"
	}
	if row.Host == "" {
		row.Host = "host-" + row.ID
	}
	if len(row.Runners) == 0 {
		row.Runners = datatypes.JSON(`[{"queues":["default"],"executor_class":"","concurrency":4,"in_flight":0}]`)
	}
	if len(row.Kinds) == 0 {
		row.Kinds = datatypes.JSON(`["mail.welcome","report.daily"]`)
	}
	require.NoError(t, db.Create(&row).Error)
}

// nodeIDs lists the ids of nodes, in order.
func nodeIDs(nodes []NodeInfo) []string {
	ids := make([]string, len(nodes))
	for i, n := range nodes {
		ids[i] = n.ID
	}
	return ids
}

// nodeByID returns the node of nodes with id, failing the test without one.
func nodeByID(t *testing.T, nodes []NodeInfo, id string) NodeInfo {
	t.Helper()
	for _, n := range nodes {
		if n.ID == id {
			return n
		}
	}
	t.Fatalf("no node %q in %v", id, nodeIDs(nodes))
	return NodeInfo{}
}

// TestListNodesFlagsAStaleNode proves Stale is a node that has not stopped and
// whose heartbeat is older than StaleAfter (one minute when zero), by the
// context's clock, and never a stopped node; and that each row reads back whole:
// its runners and kinds from JSON, and Drained only for a draining node with
// nothing in flight.
func TestListNodesFlagsAStaleNode(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	ctx := clockCtx(context.Background(), models.NewFixedClock(nodesNow))
	started := nodesNow.Add(-3 * time.Hour)
	requested := nodesNow.Add(-5 * time.Minute)
	stoppedAt := nodesNow.Add(-time.Hour)
	seedNode(t, db, jobNodeRow{ID: "silent", StartedAt: started, HeartbeatAt: nodesNow.Add(-2 * time.Minute)})
	seedNode(t, db, jobNodeRow{
		ID: "beating", StartedAt: started, HeartbeatAt: nodesNow.Add(-30 * time.Second),
		Version: "1.4.0", CommitHash: "abc1234", Scheduler: true,
		Runners: datatypes.JSON(`[{"queues":["default"],"executor_class":"","concurrency":4,"in_flight":3},` +
			`{"queues":["mail"],"executor_class":"ecs","concurrency":2,"in_flight":0}]`),
	})
	seedNode(t, db, jobNodeRow{
		ID: "stopped", StartedAt: started.Add(-time.Hour), HeartbeatAt: stoppedAt, StoppedAt: &stoppedAt,
	})
	seedNode(t, db, jobNodeRow{
		ID: "draining-busy", StartedAt: started, HeartbeatAt: nodesNow, Draining: true, DrainRequestedAt: &requested,
		Runners: datatypes.JSON(`[{"queues":["a"],"executor_class":"","concurrency":1,"in_flight":1}]`),
	})
	seedNode(t, db, jobNodeRow{
		ID: "draining-idle", StartedAt: started, HeartbeatAt: nodesNow, Draining: true, DrainRequestedAt: &requested,
	})

	nodes, err := ListNodes(ctx, db, ListNodesParams{})
	require.NoError(t, err)
	assert.True(t, nodeByID(t, nodes, "silent").Stale, "two minutes without a heartbeat is stale by default")
	beating := nodeByID(t, nodes, "beating")
	assert.False(t, beating.Stale, "thirty seconds is not")
	assert.Equal(t, NodeInfo{
		ID: "beating", ExecutorID: "host-beating:42", Host: "host-beating", Version: "1.4.0", Commit: "abc1234",
		StartedAt: started, HeartbeatAt: nodesNow.Add(-30 * time.Second), Scheduler: true,
		Runners: []NodeRunner{
			{Queues: []string{"default"}, Concurrency: 4, InFlight: 3},
			{Queues: []string{"mail"}, ExecutorClass: "ecs", Concurrency: 2, InFlight: 0},
		},
		Kinds: []string{"mail.welcome", "report.daily"},
	}, beating)
	busy := nodeByID(t, nodes, "draining-busy")
	assert.True(t, busy.Draining)
	assert.False(t, busy.Drained, "a draining node with a job in flight is not drained")
	require.NotNil(t, busy.DrainRequestedAt)
	assert.True(t, requested.Equal(*busy.DrainRequestedAt))
	assert.True(t, nodeByID(t, nodes, "draining-idle").Drained, "with nothing in flight it is")
	assert.False(t, beating.Drained, "a node that is not draining is not drained")

	nodes, err = ListNodes(ctx, db, ListNodesParams{StaleAfter: 10 * time.Second})
	require.NoError(t, err)
	assert.True(t, nodeByID(t, nodes, "beating").Stale, "a shorter StaleAfter flags it")
	assert.False(t, nodeByID(t, nodes, "draining-idle").Stale, "a heartbeat at now is never stale")

	nodes, err = ListNodes(ctx, db, ListNodesParams{StoppedSince: stoppedAt.Add(-time.Minute), StaleAfter: time.Second})
	require.NoError(t, err)
	stopped := nodeByID(t, nodes, "stopped")
	assert.False(t, stopped.Stale, "a stopped node is never stale, however old its heartbeat")
	require.NotNil(t, stopped.StoppedAt)
	assert.True(t, stoppedAt.Equal(*stopped.StoppedAt))
}

// TestListNodesListsStoppedNodesBackToStoppedSince proves ListNodes lists the
// nodes that have not stopped, then those that stopped at or after a non-zero
// StoppedSince, each group newest start first; and that Limit selects 100 when
// zero or negative and reads at most MaxReadLimit.
func TestListNodesListsStoppedNodesBackToStoppedSince(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	ctx := clockCtx(context.Background(), models.NewFixedClock(nodesNow))
	stoppedLong := nodesNow.Add(-4 * time.Hour)
	stoppedLately := nodesNow.Add(-30 * time.Minute)
	seedNode(t, db, jobNodeRow{ID: "live-older", StartedAt: nodesNow.Add(-3 * time.Hour), HeartbeatAt: nodesNow})
	seedNode(t, db, jobNodeRow{ID: "live-newer", StartedAt: nodesNow.Add(-time.Hour), HeartbeatAt: nodesNow})
	seedNode(t, db, jobNodeRow{
		ID: "gone-long", StartedAt: nodesNow.Add(-5 * time.Hour), HeartbeatAt: stoppedLong, StoppedAt: &stoppedLong,
	})
	seedNode(t, db, jobNodeRow{
		ID: "gone-lately", StartedAt: nodesNow.Add(-2 * time.Hour), HeartbeatAt: stoppedLately, StoppedAt: &stoppedLately,
	})

	nodes, err := ListNodes(ctx, db, ListNodesParams{})
	require.NoError(t, err)
	assert.Equal(t, []string{"live-newer", "live-older"}, nodeIDs(nodes), "a zero StoppedSince lists no stopped node")

	nodes, err = ListNodes(ctx, db, ListNodesParams{StoppedSince: nodesNow.Add(-time.Hour)})
	require.NoError(t, err)
	assert.Equal(t, []string{"live-newer", "live-older", "gone-lately"}, nodeIDs(nodes),
		"stopped nodes come after the nodes that have not stopped")

	nodes, err = ListNodes(ctx, db, ListNodesParams{StoppedSince: stoppedLong})
	require.NoError(t, err)
	assert.Equal(t, []string{"live-newer", "live-older", "gone-lately", "gone-long"}, nodeIDs(nodes),
		"a node that stopped exactly at StoppedSince is listed")

	many := newDB(t)
	for i := range MaxReadLimit + 50 {
		seedNode(t, many, jobNodeRow{
			ID: fmt.Sprintf("node-%03d", i), StartedAt: nodesNow.Add(-time.Duration(i) * time.Second), HeartbeatAt: nodesNow,
		})
	}
	for _, tc := range []struct {
		limit, want int
	}{{0, 100}, {-1, 100}, {3, 3}, {MaxReadLimit + 1, MaxReadLimit}} {
		nodes, err := ListNodes(ctx, many, ListNodesParams{Limit: tc.limit})
		require.NoError(t, err)
		assert.Len(t, nodes, tc.want, "Limit %d", tc.limit)
	}

	_, err = ListNodes(ctx, nil, ListNodesParams{})
	require.Error(t, err)
}

// TestRequestDrainRefusesAStoppedOrUnknownNode proves RequestDrain stamps
// drain_requested_at on a node that has not stopped with one entry, attributed
// to the context's actor; that a repeated request changes nothing and records
// nothing; that a stopped or unknown node is ErrNodeNotFound; and that an empty
// id, an actor the history can't record, and a failing audit hook each leave
// everything as it was.
func TestRequestDrainRefusesAStoppedOrUnknownNode(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	// Five hours behind UTC, so a time stamped in UTC is told apart.
	at := time.Date(2026, 1, 1, 7, 0, 0, 0, time.FixedZone("UTC-5", -5*60*60))
	retire := Actor{Type: ActorUser, ID: "u1", Reason: "retire host"}
	ctx := clockCtx(WithActor(context.Background(), retire), models.NewFixedClock(at))
	stoppedAt := nodesNow.Add(-time.Hour)
	seedNode(t, db, jobNodeRow{ID: "live", StartedAt: nodesNow.Add(-time.Hour), HeartbeatAt: nodesNow})
	seedNode(t, db, jobNodeRow{ID: "stopped", StartedAt: nodesNow.Add(-2 * time.Hour), HeartbeatAt: stoppedAt, StoppedAt: &stoppedAt})
	drainRequested := func(id string) *time.Time {
		var row jobNodeRow
		require.NoError(t, db.Where("id = ?", id).Take(&row).Error)
		return row.DrainRequestedAt
	}

	require.NoError(t, RequestDrain(ctx, db, "live"))
	got := drainRequested("live")
	require.NotNil(t, got)
	assert.True(t, at.Equal(*got))
	assert.Equal(t, time.UTC, got.Location(), "stored in UTC")
	entry := onlyAuditRow(t, db, "live")
	assert.Equal(t, string(AuditTargetNode), entry.TargetType)
	assert.Equal(t, string(AuditDrainRequested), entry.Action)
	assert.Equal(t, "u1", entry.ActorID)
	assert.Equal(t, "retire host", entry.Reason)

	later := clockCtx(WithActor(context.Background(), retire), models.NewFixedClock(at.Add(time.Hour)))
	require.NoError(t, RequestDrain(later, db, "live"), "a repeated request is not an error")
	assert.True(t, at.Equal(*drainRequested("live")), "and keeps the first request's time")
	assert.Len(t, auditRows(t, db, "live"), 1, "and records nothing")

	require.ErrorIs(t, RequestDrain(ctx, db, "stopped"), ErrNodeNotFound, "a stopped node cannot drain")
	assert.Nil(t, drainRequested("stopped"))
	require.ErrorIs(t, RequestDrain(ctx, db, "no-such-node"), ErrNodeNotFound)
	assert.Empty(t, auditRows(t, db, "stopped"))
	assert.Empty(t, auditRows(t, db, "no-such-node"))

	var ve *ValidationError
	require.ErrorAs(t, RequestDrain(ctx, db, ""), &ve)
	assert.Equal(t, "node_id", ve.Field)
	seedNode(t, db, jobNodeRow{ID: "kept", StartedAt: nodesNow.Add(-time.Hour), HeartbeatAt: nodesNow})
	for _, bad := range invalidActors() {
		assertRefusedActor(t, RequestDrain(WithActor(context.Background(), bad.actor), db, "kept"), bad)
	}
	hookErr := errors.New("host record failed")
	failing := WithAuditHook(ctx, func(context.Context, *gorm.DB, []AuditEntry) error { return hookErr })
	require.ErrorIs(t, RequestDrain(failing, db, "kept"), hookErr)
	assert.Nil(t, drainRequested("kept"), "neither a refused actor nor a failed hook requests the drain")
	assert.Empty(t, auditRows(t, db, "kept"))

	require.Error(t, RequestDrain(ctx, nil, "live"))
}
