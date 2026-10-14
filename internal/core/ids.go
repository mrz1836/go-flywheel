package core

import (
	"fmt"
	"time"
)

// maxIDMillis is the largest millisecond a UUIDv7's 48-bit time field holds.
const maxIDMillis = 1<<48 - 1

// idFloor returns the smallest UUIDv7-shaped id of t's millisecond:
// xxxxxxxx-xxxx-0000-0000-000000000000, its first 48 bits the millisecond Unix
// time in lowercase hex. Every id the runtime mints in that millisecond sorts at
// or after it, and every id of an earlier millisecond sorts before it, as text
// and as a uuid alike, so a created window applies as a range of the primary key.
// A time before 1970, or past the field's range, clamps to its end.
func idFloor(t time.Time) string {
	ms := min(max(t.UnixMilli(), 0), maxIDMillis)
	h := fmt.Sprintf("%012x", ms)
	return h[:8] + "-" + h[8:] + "-0000-0000-000000000000"
}
