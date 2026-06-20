package flywheel

import (
	"errors"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	sqlite3 "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

func TestWrapDBError_Nil(t *testing.T) {
	assert.NoError(t, WrapDBError(nil))
}

func TestWrapDBError_RecordNotFound(t *testing.T) {
	got := WrapDBError(gorm.ErrRecordNotFound)
	assert.ErrorIs(t, got, ErrNotFound)
}

func TestWrapDBError_PostgresUniqueViolation(t *testing.T) {
	pgErr := &pgconn.PgError{Code: pgCodeUniqueViolation, Message: "duplicate key value"}

	got := WrapDBError(pgErr)
	assert.ErrorIs(t, got, ErrDuplicateKey, "pg 23505 must map to ErrDuplicateKey")
}

func TestWrapDBError_PostgresForeignKeyViolation(t *testing.T) {
	pgErr := &pgconn.PgError{Code: pgCodeForeignKeyViolation, Message: "fk violation"}

	got := WrapDBError(pgErr)
	assert.ErrorIs(t, got, ErrForeignKey, "pg 23503 must map to ErrForeignKey")
}

func TestWrapDBError_PostgresOtherCode(t *testing.T) {
	pgErr := &pgconn.PgError{Code: "42P01", Message: "undefined table"}

	got := WrapDBError(pgErr)
	assert.ErrorIs(t, got, ErrDatabaseError)
}

func TestWrapDBError_SqliteUniqueViolation(t *testing.T) {
	sqliteErr := sqlite3.Error{
		Code:         sqlite3.ErrConstraint,
		ExtendedCode: sqlite3.ErrConstraintUnique,
	}

	got := WrapDBError(sqliteErr)
	assert.ErrorIs(t, got, ErrDuplicateKey, "sqlite unique extended code must map to ErrDuplicateKey")
}

func TestWrapDBError_SqlitePrimaryKeyViolation(t *testing.T) {
	sqliteErr := sqlite3.Error{
		Code:         sqlite3.ErrConstraint,
		ExtendedCode: sqlite3.ErrConstraintPrimaryKey,
	}

	got := WrapDBError(sqliteErr)
	assert.ErrorIs(t, got, ErrDuplicateKey)
}

func TestWrapDBError_MessageFallbackUnique(t *testing.T) {
	// A driver error that does not unwrap to a typed value must still be
	// classified by message so duplicate-key idempotency survives.
	got := WrapDBError(errors.New("UNIQUE constraint failed: jobs.unique_key"))
	assert.ErrorIs(t, got, ErrDuplicateKey)
}

func TestWrapDBError_MessageFallbackPostgresText(t *testing.T) {
	got := WrapDBError(fmt.Errorf("pq: duplicate key value violates unique constraint %q", "jobs_unique_key"))
	assert.ErrorIs(t, got, ErrDuplicateKey)
}

func TestWrapDBError_UnknownErrorIsDatabaseError(t *testing.T) {
	got := WrapDBError(errors.New("something else went wrong"))
	require.Error(t, got)
	assert.ErrorIs(t, got, ErrDatabaseError)
}
