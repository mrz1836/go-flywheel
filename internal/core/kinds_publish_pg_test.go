//go:build integration

package core

import "testing"

// TestPublishKindsKeepsFirstPublishedAtPostgres runs the publish suite on
// PostgreSQL, where the upsert is INSERT … ON CONFLICT (kind) DO UPDATE and the
// args schema and example are jsonb.
func TestPublishKindsKeepsFirstPublishedAtPostgres(t *testing.T) {
	t.Parallel()
	publishKindsSuite(t, NewPostgresIsolatedDB(t))
}
