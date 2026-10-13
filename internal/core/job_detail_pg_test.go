//go:build integration

package core

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestGetJobReturnsArgsMetadataKeysAndTheLatestRunPostgres reads a job in full on
// PostgreSQL, where args are jsonb and a host's schema may allow a NULL metadata.
func TestGetJobReturnsArgsMetadataKeysAndTheLatestRunPostgres(t *testing.T) {
	t.Parallel()
	getJobDetailSuite(t, postgresOpener)
}

// TestGetJobLeavesOversizedArgsInTheDatabasePostgres bounds args in SQL on
// PostgreSQL, where the length is the jsonb value's text rendering, the bytes a
// read of the column would send.
func TestGetJobLeavesOversizedArgsInTheDatabasePostgres(t *testing.T) {
	t.Parallel()
	db := NewPostgresIsolatedDB(t)
	oversizedArgsSuite(t, db)

	id, err := Enqueue(context.Background(), NewClient(db), "detail.measured", bigArgs(4096), InsertOpts{})
	require.NoError(t, err)
	var octets int
	require.NoError(t, db.Raw(`SELECT octet_length(args::text) FROM jobs WHERE id = ?`, id).Scan(&octets).Error)
	got, err := GetJob(context.Background(), db, id)
	require.NoError(t, err)
	assert.Equal(t, octets, got.ArgsBytes)
}
