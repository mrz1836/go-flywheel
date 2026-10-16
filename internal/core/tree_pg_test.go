//go:build integration

package core

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestJobTreeIsTheSameOnBothDialectsPostgres walks the dialect fixture on
// PostgreSQL and on SQLite and proves every walk returns the same result.
func TestJobTreeIsTheSameOnBothDialectsPostgres(t *testing.T) {
	t.Parallel()
	pg := NewPostgresIsolatedDB(t)
	lite := newDB(t)
	pgWalks := walkTreeDialectFixture(t, pg, seedTreeDialectFixture(t, pg))
	liteWalks := walkTreeDialectFixture(t, lite, seedTreeDialectFixture(t, lite))
	for _, w := range treeDialectWalks {
		assert.Equal(t, liteWalks[w.name], pgWalks[w.name], w.name)
	}
}
