package flywheel

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewID_IsValidUUIDv7(t *testing.T) {
	t.Parallel()
	id := NewID()

	parsed, err := uuid.Parse(id)
	require.NoError(t, err, "NewID must produce a parseable UUID")
	assert.Equal(t, uuid.Version(7), parsed.Version(), "NewID must produce a v7 UUID")
}

func TestNewID_IsLexicographicallySortable(t *testing.T) {
	t.Parallel()
	// v7 IDs embed a millisecond timestamp, so a later ID must sort >= an
	// earlier one. Generate a batch and assert non-decreasing order.
	const n = 50

	prev := NewID()
	for range n {
		next := NewID()
		assert.LessOrEqual(t, prev, next, "v7 IDs must be lexicographically non-decreasing in creation order")
		prev = next
	}
}

func TestNewID_IsUnique(t *testing.T) {
	t.Parallel()
	const n = 1000

	seen := make(map[string]struct{}, n)
	for range n {
		id := NewID()
		_, dup := seen[id]
		require.False(t, dup, "NewID must not collide")
		seen[id] = struct{}{}
	}
}
