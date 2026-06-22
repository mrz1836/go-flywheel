package flywheel

import (
	"errors"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

func TestWrapDBError_Nil(t *testing.T) {
	t.Parallel()
	assert.NoError(t, WrapDBError(nil))
}

func TestWrapDBError(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		in   error
		want error
		msg  string
	}{
		{
			name: "record not found",
			in:   gorm.ErrRecordNotFound,
			want: ErrNotFound,
		},
		{
			name: "postgres unique violation",
			in:   &pgconn.PgError{Code: pgCodeUniqueViolation, Message: "duplicate key value"},
			want: ErrDuplicateKey,
			msg:  "pg 23505 must map to ErrDuplicateKey",
		},
		{
			name: "postgres foreign key violation",
			in:   &pgconn.PgError{Code: pgCodeForeignKeyViolation, Message: "fk violation"},
			want: ErrForeignKey,
			msg:  "pg 23503 must map to ErrForeignKey",
		},
		{
			name: "postgres other code",
			in:   &pgconn.PgError{Code: "42P01", Message: "undefined table"},
			want: ErrDatabaseError,
		},
		{
			// A driver error that does not unwrap to a typed value must still be
			// classified by message so duplicate-key idempotency survives.
			name: "message fallback unique",
			in:   errors.New("UNIQUE constraint failed: jobs.unique_key"),
			want: ErrDuplicateKey,
		},
		{
			name: "message fallback postgres text",
			in:   fmt.Errorf("pq: duplicate key value violates unique constraint %q", "jobs_unique_key"),
			want: ErrDuplicateKey,
		},
		{
			name: "unknown error is database error",
			in:   errors.New("something else went wrong"),
			want: ErrDatabaseError,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := WrapDBError(tt.in)
			assert.ErrorIs(t, got, tt.want, tt.msg)
		})
	}
}

// TestWrapDBErrorClassifiesRealSQLiteUnique drives a genuine modernc UNIQUE
// violation through WrapDBError. modernc's *sqlite.Error has unexported fields
// (it cannot be constructed by hand), so the typed-error branch is verified
// against a real constraint failure rather than a literal.
func TestWrapDBErrorClassifiesRealSQLiteUnique(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	require.NoError(t, db.Exec(`CREATE TABLE u (k text)`).Error)
	require.NoError(t, db.Exec(`CREATE UNIQUE INDEX ux ON u (k)`).Error)
	require.NoError(t, db.Exec(`INSERT INTO u (k) VALUES ('x')`).Error)

	err := db.Exec(`INSERT INTO u (k) VALUES ('x')`).Error
	require.Error(t, err)
	assert.ErrorIs(t, WrapDBError(err), ErrDuplicateKey, "a real modernc UNIQUE violation maps to ErrDuplicateKey")
}
