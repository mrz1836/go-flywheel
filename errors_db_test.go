package flywheel

import (
	"errors"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	sqlite3 "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
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
			name: "sqlite unique violation",
			in:   sqlite3.Error{Code: sqlite3.ErrConstraint, ExtendedCode: sqlite3.ErrConstraintUnique},
			want: ErrDuplicateKey,
			msg:  "sqlite unique extended code must map to ErrDuplicateKey",
		},
		{
			name: "sqlite primary key violation",
			in:   sqlite3.Error{Code: sqlite3.ErrConstraint, ExtendedCode: sqlite3.ErrConstraintPrimaryKey},
			want: ErrDuplicateKey,
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
