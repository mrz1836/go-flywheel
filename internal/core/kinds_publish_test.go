package core

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/mrz1836/go-foundation/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

// rewordedWorker is describedWorker as a later release declares it: the same
// kind, with another description.
type rewordedWorker struct{ describedWorker }

func (rewordedWorker) Description() string { return "Send the welcome email, in the user's language" }

// publishedAt returns ctx carrying a fixed clock at at.
func publishedAt(ctx context.Context, at time.Time) context.Context {
	return clockCtx(ctx, models.NewFixedClock(at))
}

// assertSameInstant asserts want and got are the same instant, whatever zone the
// database reads a time back in.
func assertSameInstant(t testing.TB, want, got time.Time, msg string) {
	t.Helper()
	assert.True(t, want.Equal(got), "%s: want %s, got %s", msg, want, got)
}

// publishKindsSuite publishes a catalog, publishes it again from a later
// release under another publisher, and checks every column of each row: a
// republished kind takes the new metadata, publisher, and publish time and keeps
// its first publish time, and a kind the later release does not register keeps
// its row as it was.
func publishKindsSuite(t *testing.T, db *gorm.DB) {
	t.Helper()
	ctx := context.Background()
	t1 := time.Date(2026, 1, 5, 9, 0, 0, 0, time.UTC)
	t2 := t1.Add(36 * time.Hour)

	require.NoError(t, PublishKinds(publishedAt(ctx, t1), db, catalogRegistry()))
	records, err := ListKinds(ctx, db)
	require.NoError(t, err)
	require.Len(t, records, 2)
	for i, info := range catalogRegistry().Kinds() {
		rec := records[i]
		assert.Equal(t, info.Kind, rec.Kind, "sorted by kind")
		assert.Equal(t, info.Description, rec.Description)
		if info.ArgsSchema == nil {
			assert.Nil(t, rec.ArgsSchema)
			assert.Nil(t, rec.ExampleArgs)
		} else {
			assert.JSONEq(t, string(info.ArgsSchema), string(rec.ArgsSchema))
			assert.JSONEq(t, string(info.ExampleArgs), string(rec.ExampleArgs))
		}
		rec.ArgsSchema, rec.ExampleArgs, info.ArgsSchema, info.ExampleArgs = nil, nil, nil, nil
		assert.Equal(t, info, rec.KindInfo, "every KindInfo field round-trips")
		assert.Equal(t, executorIdentity(), rec.PublishedBy, "the process publishes as host:pid by default")
		assertSameInstant(t, t1, rec.FirstPublishedAt, rec.Kind+" first published")
		assertSameInstant(t, t1, rec.PublishedAt, rec.Kind+" published")
	}

	later := NewRegistry()
	Register(later, rewordedWorker{})
	require.NoError(t, PublishKindsWithOptions(publishedAt(ctx, t2), db, later, PublishKindsOpts{Publisher: "worker-v2"}))
	records, err = ListKinds(ctx, db)
	require.NoError(t, err)
	require.Len(t, records, 2, "a republish never deletes a kind")

	welcome := records[0]
	assert.Equal(t, "mail.welcome", welcome.Kind)
	assert.Equal(t, "Send the welcome email, in the user's language", welcome.Description, "the metadata is the last publisher's")
	assert.Equal(t, "worker-v2", welcome.PublishedBy)
	assertSameInstant(t, t1, welcome.FirstPublishedAt, "the first publish time never moves")
	assertSameInstant(t, t2, welcome.PublishedAt, "the publish time is the last publish's")

	daily := records[1]
	assert.Equal(t, "report.daily", daily.Kind, "a kind the later release does not register stays listed")
	assert.Equal(t, executorIdentity(), daily.PublishedBy)
	assertSameInstant(t, t1, daily.PublishedAt, "with its old publish time")
}

// TestPublishKindsKeepsFirstPublishedAt proves a publish writes every
// registered kind's KindInfo with its publisher and publish time, and a later
// publish updates them and keeps each kind's first publish time.
func TestPublishKindsKeepsFirstPublishedAt(t *testing.T) {
	t.Parallel()
	publishKindsSuite(t, newDB(t))
}

// TestPublishKindsNeverDeletesAKind proves a kind the publishing registry no
// longer registers keeps its row, its old publish time, and its publisher, and
// that publishing an empty registry writes and deletes nothing.
func TestPublishKindsNeverDeletesAKind(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	ctx := context.Background()
	t1 := time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)
	require.NoError(t, PublishKindsWithOptions(publishedAt(ctx, t1), db, catalogRegistry(), PublishKindsOpts{Publisher: "v1"}))

	only := NewRegistry()
	Register(only, describedWorker{})
	require.NoError(t, PublishKindsWithOptions(publishedAt(ctx, t1.Add(time.Hour)), db, only, PublishKindsOpts{Publisher: "v2"}))
	require.NoError(t, PublishKinds(publishedAt(ctx, t1.Add(2*time.Hour)), db, NewRegistry()))

	records, err := ListKinds(ctx, db)
	require.NoError(t, err)
	require.Len(t, records, 2)
	assert.Equal(t, "report.daily", records[1].Kind)
	assert.Equal(t, "v1", records[1].PublishedBy)
	assertSameInstant(t, t1, records[1].PublishedAt, "the unregistered kind keeps its old publish time")
	assert.Equal(t, "v2", records[0].PublishedBy)

	require.Error(t, PublishKinds(ctx, nil, catalogRegistry()), "a nil database is refused")
	require.Error(t, PublishKinds(ctx, db, nil), "a nil registry is refused")
}

// numberedWorker is a worker whose kind is set per value, so one type
// registers any number of kinds.
type numberedWorker struct{ kind string }

func (w numberedWorker) Kind() string { return w.kind }

func (numberedWorker) Work(context.Context, *Job[signupArgs]) (Result, error) { return Result{}, nil }

// TestListKindsReturnsEveryKindSortedByKind proves ListKinds returns every
// published kind, sorted by kind, past MaxReadLimit: a catalog is bounded by a
// host's code, and a cut one would hide a kind.
func TestListKindsReturnsEveryKindSortedByKind(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	ctx := context.Background()
	const total = MaxReadLimit + 50
	reg := NewRegistry()
	for i := total - 1; i >= 0; i-- {
		Register(reg, numberedWorker{kind: fmt.Sprintf("catalog.kind_%03d", i)})
	}
	require.NoError(t, PublishKinds(ctx, db, reg))

	records, err := ListKinds(ctx, db)
	require.NoError(t, err)
	require.Len(t, records, total, "every kind, past MaxReadLimit")
	for i, rec := range records {
		assert.Equal(t, fmt.Sprintf("catalog.kind_%03d", i), rec.Kind)
	}
	encoded, err := json.Marshal(records[0])
	require.NoError(t, err)
	assert.Contains(t, string(encoded), `"published_by":`, "a record carries its publish fields in JSON")
	assert.Contains(t, string(encoded), `"first_published_at":`)
}

// TestTheRuntimeStartsWithoutTheKindTable proves the catalog is the only thing
// that needs job_kinds: on a schema without it, as a release before the catalog
// left it, a Runner runs a job and a Scheduler with the stats on runs until its
// context ends, while PublishKinds and ListKinds fail, naming the table.
func TestTheRuntimeStartsWithoutTheKindTable(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	require.NoError(t, db.Exec("DROP TABLE job_kinds").Error)
	ctx := context.Background()

	reg := NewRegistry()
	Register[statsWriteArgs](reg, statsWriteWorker{})
	id, err := Insert(ctx, NewClient(db), statsWriteArgs{}, InsertOpts{})
	require.NoError(t, err)
	r, err := NewRunner(RunnerConfig{
		DB: db, Driver: driverForTest(t, db), Registry: reg, Queues: []string{defaultQueue}, ClaimAnyClass: true,
	})
	require.NoError(t, err)
	require.NoError(t, r.RunUntilIdle(ctx), "a Runner starts")
	assert.Equal(t, string(StateSucceeded), jobState(t, db, id), "and runs the job")

	runFor, cancel := context.WithTimeout(ctx, 300*time.Millisecond)
	defer cancel()
	s := newSchedulerCfg(t, SchedulerConfig{
		DB: db, Client: NewClient(db), Driver: driverForTest(t, db), StatsRollupInterval: time.Minute,
	})
	err = s.Run(runFor)
	require.ErrorIs(t, err, context.DeadlineExceeded, "a Scheduler with the stats on runs until its context ends")
	require.NotErrorIs(t, err, ErrSchemaOutdated)

	err = PublishKinds(ctx, db, catalogRegistry())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "job_kinds", "a publish names the missing table")
	_, err = ListKinds(ctx, db)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "job_kinds", "and so does a read")
}
