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
