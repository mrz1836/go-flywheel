package main

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	flywheel "github.com/mrz1836/go-flywheel"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// cliActorLabel is how history and the queue table print the CLI's actor.
func cliActorLabel() string {
	if osUserName() == "" {
		return "user"
	}
	return "user:" + osUserName()
}

// TestNodesAndQueuesCommands proves `flywheel queues` lists every queue with its
// depth and pause, `queues pause` and `queues resume` record the CLI's actor and
// reason, `flywheel nodes` lists the registered nodes and, with
// --stopped-since, the stopped ones, and `nodes drain` requests a drain, each
// with --json where it lists.
func TestNodesAndQueuesCommands(t *testing.T) {
	t.Parallel()
	cfg := migratedCLIConfig(t)
	ctx := context.Background()
	_, db, _, err := loadAndOpen(cfg)
	require.NoError(t, err)
	defer closeDB(db)
	_, err = flywheel.Enqueue(ctx, flywheel.NewClient(db), "report.daily", []byte(`{}`), flywheel.InsertOpts{})
	require.NoError(t, err)

	out, err := runRoot(ctx, "--config", cfg, "queues", "pause", "mail", "--reason", "provider outage")
	require.NoError(t, err)
	assert.Equal(t, "paused mail\n", out)
	out, err = runRoot(ctx, "--config", cfg, "queues")
	require.NoError(t, err)
	lines := strings.Split(strings.TrimSpace(out), "\n")
	require.Len(t, lines, 3, out)
	assert.Equal(t, []string{"QUEUE", "READY", "AHEAD", "RUNNING", "PAUSED-JOBS", "PAUSED", "BY", "REASON"},
		strings.Fields(lines[0]))
	assert.Equal(t, []string{"default", "1", "0", "0", "0", "-", "-", "-"}, strings.Fields(lines[1]))
	assert.Equal(t, []string{"mail", "0", "0", "0", "0", "yes", cliActorLabel(), "provider", "outage"},
		strings.Fields(lines[2]))

	page, err := flywheel.ListAudit(ctx, db, flywheel.ListAuditParams{
		TargetType: flywheel.AuditTargetQueue, TargetID: "mail",
	})
	require.NoError(t, err)
	require.Len(t, page.Entries, 1)
	assert.Equal(t, flywheel.AuditPaused, page.Entries[0].Action)
	assert.Equal(t, flywheel.ActorUser, page.Entries[0].Actor.Type)
	assert.Equal(t, osUserName(), page.Entries[0].Actor.ID, "the operating-system user")
	assert.Equal(t, "cli", page.Entries[0].Actor.Source)
	assert.Equal(t, "provider outage", page.Entries[0].Actor.Reason)

	out, err = runRoot(ctx, "--config", cfg, "queues", "--json")
	require.NoError(t, err)
	var queues []flywheel.QueueInfo
	require.NoError(t, json.Unmarshal([]byte(out), &queues))
	require.Len(t, queues, 2)
	assert.True(t, queues[1].IsPaused)
	out, err = runRoot(ctx, "--config", cfg, "queues", "resume", "mail", "--reason", "recovered")
	require.NoError(t, err)
	assert.Equal(t, "resumed mail\n", out)
	_, err = runRoot(ctx, "--config", cfg, "queues", "pause")
	require.Error(t, err, "pause names its queue")

	out, err = runRoot(ctx, "--config", cfg, "nodes")
	require.NoError(t, err)
	assert.Equal(t, []string{"ID", "HOST", "VERSION", "STARTED", "HEARTBEAT", "STATE", "RUNNERS"},
		strings.Fields(strings.TrimSpace(out)), "no node registered prints the header alone")

	node, err := flywheel.NewNode(flywheel.NodeConfig{
		Runners: []flywheel.RunnerConfig{{
			DB: db, Driver: flywheel.NewSQLiteDriver(db), Registry: flywheel.NewRegistry(),
			Queues: []string{"idle"}, Concurrency: 1, // a queue with no job, so nothing is ever in flight
		}},
		Version: "1.4.0", RegistryInterval: 20 * time.Millisecond,
	})
	require.NoError(t, err)
	nodeCtx, stopNode := context.WithCancel(ctx)
	stopped := make(chan error, 1)
	go func() { stopped <- node.Run(nodeCtx) }()
	var nodes []flywheel.NodeInfo
	require.Eventually(t, func() bool {
		nodes, err = flywheel.ListNodes(ctx, db, flywheel.ListNodesParams{})
		return err == nil && len(nodes) == 1
	}, 5*time.Second, 5*time.Millisecond)
	id := nodes[0].ID

	out, err = runRoot(ctx, "--config", cfg, "nodes")
	require.NoError(t, err)
	lines = strings.Split(strings.TrimSpace(out), "\n")
	require.Len(t, lines, 2, out)
	fields := strings.Fields(lines[1])
	assert.Equal(t, id, fields[0])
	assert.Equal(t, nodes[0].Host, fields[1])
	assert.Equal(t, "1.4.0", fields[2])
	assert.Equal(t, "live", fields[7], "the state, after the two time columns")
	assert.Equal(t, "idle:0/1", fields[8], "each runner's queues, then its jobs in flight of its slots")

	out, err = runRoot(ctx, "--config", cfg, "nodes", "drain", id, "--reason", "retire host")
	require.NoError(t, err)
	assert.Equal(t, "drain requested "+id+"\n", out)
	page, err = flywheel.ListAudit(ctx, db, flywheel.ListAuditParams{TargetType: flywheel.AuditTargetNode, TargetID: id})
	require.NoError(t, err)
	require.Len(t, page.Entries, 1)
	assert.Equal(t, flywheel.AuditDrainRequested, page.Entries[0].Action)
	assert.Equal(t, "retire host", page.Entries[0].Actor.Reason)
	assert.Equal(t, "cli", page.Entries[0].Actor.Source)
	require.Eventually(t, func() bool {
		out, err = runRoot(ctx, "--config", cfg, "nodes")
		return err == nil && strings.Contains(out, " drained ")
	}, 5*time.Second, 5*time.Millisecond, "the node reports drained")

	stopNode()
	require.NoError(t, <-stopped)
	out, err = runRoot(ctx, "--config", cfg, "nodes")
	require.NoError(t, err)
	assert.Len(t, strings.Split(strings.TrimSpace(out), "\n"), 1, "a stopped node is not listed by default")
	out, err = runRoot(ctx, "--config", cfg, "nodes", "--stopped-since", "1h")
	require.NoError(t, err)
	lines = strings.Split(strings.TrimSpace(out), "\n")
	require.Len(t, lines, 2, out)
	assert.Equal(t, "stopped", strings.Fields(lines[1])[7])
	out, err = runRoot(ctx, "--config", cfg, "nodes", "--stopped-since", "1h", "--json")
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal([]byte(out), &nodes))
	require.Len(t, nodes, 1)
	assert.Equal(t, id, nodes[0].ID)

	_, err = runRoot(ctx, "--config", cfg, "nodes", "drain", id, "--reason", "again")
	require.ErrorIs(t, err, flywheel.ErrNodeNotFound, "a stopped node cannot drain")
}
