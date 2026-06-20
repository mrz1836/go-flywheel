package flywheel

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

// typedArgs implements kindNamer so Insert can read the kind from the args.
type typedArgs struct {
	Value string `json:"value"`
}

func (typedArgs) Kind() string { return "test.typed" }

// kindlessArgs intentionally lacks Kind() — Insert must reject it.
type kindlessArgs struct {
	Value string
}

func TestInsertTypedArgsWritesRowAndReturnsID(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	c := NewClient(db)

	id, err := Insert(context.Background(), c, typedArgs{Value: "hello"}, InsertOpts{})
	require.NoError(t, err)
	require.NotEmpty(t, id)

	var count int64
	require.NoError(t, db.Table("jobs").Where("id = ?", id).Count(&count).Error)
	assert.EqualValues(t, 1, count)
}

func TestInsertWithoutKindFunctionReturnsErrMissingKind(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	c := NewClient(db)

	_, err := Insert(context.Background(), c, kindlessArgs{Value: "x"}, InsertOpts{})
	require.ErrorIs(t, err, ErrMissingKind)
}

func TestInsertUniqueKeyCollisionReturnsErrAlreadyEnqueued(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	c := NewClient(db)

	_, err := Insert(context.Background(), c, typedArgs{Value: "a"}, InsertOpts{UniqueKey: "k"})
	require.NoError(t, err)

	_, err = Insert(context.Background(), c, typedArgs{Value: "b"}, InsertOpts{UniqueKey: "k"})
	require.ErrorIs(t, err, ErrAlreadyEnqueued)
}

func TestEnqueueArbitraryKindWritesAvailableRowWithDefaults(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	c := NewClient(db)

	id, err := Enqueue(context.Background(), c, "seed.kind", []byte(`{"k":"v"}`), InsertOpts{})
	require.NoError(t, err)
	require.NotEmpty(t, id)

	var row jobRow
	require.NoError(t, db.Where("id = ?", id).First(&row).Error)
	assert.Equal(t, "seed.kind", row.Kind)
	assert.Equal(t, string(StateAvailable), row.State)
	assert.Equal(t, defaultQueue, row.Queue)
	assert.Equal(t, defaultPriority, row.Priority)
	assert.Equal(t, defaultMaxAttempts, row.MaxAttempts)
	assert.JSONEq(t, `{"k":"v"}`, string(row.Args))
}

func TestEnqueueHonorsUniqueKeyAndQueueOpts(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	c := NewClient(db)

	id, err := Enqueue(context.Background(), c, "seed.kind", []byte(`{}`), InsertOpts{UniqueKey: "uk-1", Queue: "custom"})
	require.NoError(t, err)

	var row jobRow
	require.NoError(t, db.Where("id = ?", id).First(&row).Error)
	require.NotNil(t, row.UniqueKey)
	assert.Equal(t, "uk-1", *row.UniqueKey)
	assert.Equal(t, "custom", row.Queue)

	_, err = Enqueue(context.Background(), c, "seed.kind", []byte(`{}`), InsertOpts{UniqueKey: "uk-1"})
	require.ErrorIs(t, err, ErrAlreadyEnqueued)
}

func TestInsertTxOverrideWritesOnSuppliedConnection(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	c := NewClient(db)

	err := db.Transaction(func(tx *gorm.DB) error {
		_, e := Insert(context.Background(), c, typedArgs{Value: "in-tx"}, InsertOpts{Tx: tx})
		return e
	})
	require.NoError(t, err)

	var count int64
	require.NoError(t, db.Table("jobs").Count(&count).Error)
	assert.EqualValues(t, 1, count, "Insert with Tx must still land exactly one row")
}
