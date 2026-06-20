package flywheel

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// auditErrorClassArgs is the args type for the audit-trail tests.
type auditErrorClassArgs struct{ V string }

func (auditErrorClassArgs) Kind() string { return "test.auditerror" }

// auditErrorWorker fails on every attempt with a permanent classification so
// the run row ends with ErrorClass=permanent and ErrorMessage populated.
type auditErrorWorker struct{}

func (auditErrorWorker) Kind() string              { return "test.auditerror" }
func (auditErrorWorker) Classify(error) ErrorClass { return ErrorPermanent }
func (auditErrorWorker) Work(context.Context, *Job[auditErrorClassArgs]) (Result, error) {
	return Result{}, &classifiedError{cause: errOnPurpose, class: ErrorPermanent}
}

var errOnPurpose = errors.New("audit-test-purposeful-failure")

// Each attempt must produce exactly one job_runs row, never reuse one. The
// runner pre-allocates a stub on dispatch and finalizes it in place.
func TestRunnerAuditOneRunRowPerAttempt(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	reg := NewRegistry()
	Register(reg, auditErrorWorker{})
	r := newRunner(t, db, reg)

	id, err := Insert(context.Background(), NewClient(db), auditErrorClassArgs{V: "x"},
		InsertOpts{MaxAttempts: 1})
	require.NoError(t, err)

	runToIdle(t, context.Background(), r)

	var rows []struct {
		Outcome      string
		Attempt      int
		ErrorClass   *string
		ErrorMessage *string
	}
	require.NoError(t, db.Table("job_runs").
		Select("outcome, attempt, error_class, error_message").
		Where("job_id = ?", id).Order("attempt ASC").Scan(&rows).Error)

	require.Len(t, rows, 1, "with MaxAttempts=1 and a permanent failure we expect one append-only row")
	assert.Equal(t, "error", rows[0].Outcome)
	require.NotNil(t, rows[0].ErrorClass)
	assert.Equal(t, "permanent", *rows[0].ErrorClass)
	require.NotNil(t, rows[0].ErrorMessage)
	assert.Contains(t, *rows[0].ErrorMessage, "audit-test-purposeful-failure")
}

func TestRunnerAuditRowsAreAppendOnlyAcrossAttempts(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	w := &retryWorker{failuresBefore: 2}
	reg := NewRegistry()
	Register(reg, w)
	r := newRunner(t, db, reg)

	id, err := Insert(context.Background(), NewClient(db), retryArgs{V: "x"}, InsertOpts{MaxAttempts: 5})
	require.NoError(t, err)

	runToIdle(t, context.Background(), r)

	var attempts []int
	require.NoError(t, db.Table("job_runs").
		Select("attempt").Where("job_id = ?", id).Order("attempt ASC").Scan(&attempts).Error)

	assert.Equal(t, []int{1, 2, 3}, attempts, "each attempt mints its own row; no in-place rewrite")
}
