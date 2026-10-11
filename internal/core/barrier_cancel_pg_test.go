//go:build integration

package core

import (
	"testing"

	"gorm.io/gorm"
)

// TestCancellingTheLastPendingChildFiresItsParentsBarrierPostgres runs the cancel
// cases on PostgreSQL, where the barrier check locks the parent row.
func TestCancellingTheLastPendingChildFiresItsParentsBarrierPostgres(t *testing.T) {
	t.Parallel()
	cancelBarrierSuite(t, func(t *testing.T) *gorm.DB { return NewPostgresIsolatedDB(t) })
}
