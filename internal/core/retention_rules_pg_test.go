//go:build integration

package core

import (
	"testing"

	"gorm.io/gorm"
)

// TestARetentionRuleKeepsDiscardedJobsLongerPostgres runs the suite on
// PostgreSQL, where the rule's cutoffs are bound against timestamptz.
func TestARetentionRuleKeepsDiscardedJobsLongerPostgres(t *testing.T) {
	t.Parallel()
	retentionRuleKeepsDiscardedJobsLongerSuite(t, func(t *testing.T) *gorm.DB {
		t.Helper()
		return NewPostgresIsolatedDB(t)
	})
}
