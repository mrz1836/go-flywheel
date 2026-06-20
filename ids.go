package flywheel

import "github.com/google/uuid"

// NewID returns a freshly minted UUID v7 string. v7 IDs embed a millisecond
// timestamp, so they sort lexicographically in creation order — which lets
// callers paginate by id alone instead of composite cursors.
//
// If the underlying RNG fails (a process-level fault, not a runtime condition
// on supported platforms), NewID panics.
func NewID() string {
	id, err := uuid.NewV7()
	if err != nil {
		panic("flywheel: UUID v7 generation failed: " + err.Error())
	}

	return id.String()
}
