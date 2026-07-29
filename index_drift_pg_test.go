//go:build integration

package flywheel

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestInspectIndexesReportsNoDriftOnAFreshInstallPostgres is the PostgreSQL half
// of the parity check: the dialect that rewrites the definition most on the way
// into the catalog (schema qualifier, USING btree, = ANY (ARRAY[...]), ::text)
// must still read back as no drift, or the normalizer is wrong.
func TestInspectIndexesReportsNoDriftOnAFreshInstallPostgres(t *testing.T) {
	t.Parallel()
	db := NewPostgresIsolatedDB(t)

	drift, err := InspectIndexes(context.Background(), db)
	require.NoError(t, err)
	assert.Empty(t, drift, "a freshly migrated PostgreSQL schema is at parity with the runtime")
}
