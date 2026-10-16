package core

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mintIDAt mints a UUIDv7 whose time field is t's millisecond and whose tail
// is random: the id the runtime would have minted for a job inserted at t.
func mintIDAt(t time.Time) string {
	var tail [10]byte
	if _, err := rand.Read(tail[:]); err != nil {
		panic(err)
	}
	tail[0] = 0x70 | tail[0]&0x0f // version 7
	tail[2] = 0x80 | tail[2]&0x3f // the RFC 9562 variant
	h := fmt.Sprintf("%012x", t.UnixMilli()) + hex.EncodeToString(tail[:])
	return h[:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:]
}

// TestIdTimeReadsTheInsertTimeBack proves idTime is idFloor's inverse to the
// millisecond, for the floor itself and for any id of that millisecond, and
// that an id without a UUIDv7's leading time field reads as no time.
func TestIdTimeReadsTheInsertTimeBack(t *testing.T) {
	t.Parallel()
	at := time.Date(2026, 9, 1, 13, 45, 12, 345_678_901, time.UTC)
	want := at.Truncate(time.Millisecond)

	for _, id := range []string{idFloor(at), mintIDAt(at)} {
		got, ok := idTime(id)
		require.True(t, ok, id)
		assert.True(t, want.Equal(got), "%s reads back as %s, want %s", id, got, want)
		assert.Equal(t, time.UTC, got.Location())
	}
	assert.Less(t, idFloor(at), mintIDAt(at), "an id of a millisecond sorts at or after its floor")
	assert.Less(t, mintIDAt(at), idFloor(at.Add(time.Millisecond)), "and before the next millisecond's")

	for _, bad := range []string{"", "job-1", "0190e9a2", "0190e9a2-7c1", "0190e9a2x7c1d-7b3e", "0190E9A2-7C1D-7b3e-8f00-1234567890ab"} {
		_, ok := idTime(bad)
		assert.False(t, ok, "%q has no readable time", bad)
	}
}
