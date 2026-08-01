package core

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestInspectStorageParametersReturnsNoDriftOnSQLite proves the parity check is a
// clean no-op on the dialect with no storage parameters, so a host can call it
// unconditionally the way it calls InstallStorageParameters.
func TestInspectStorageParametersReturnsNoDriftOnSQLite(t *testing.T) {
	t.Parallel()
	db := newDB(t)

	drift, err := InspectStorageParameters(context.Background(), db)
	require.NoError(t, err, "SQLite has no storage parameters and that is not drift")
	assert.Empty(t, drift)
}

// TestInspectStorageParametersRejectsANilDB matches the guard on the other
// install and inspect entry points.
func TestInspectStorageParametersRejectsANilDB(t *testing.T) {
	t.Parallel()
	_, err := InspectStorageParameters(context.Background(), nil)
	require.Error(t, err)
}
