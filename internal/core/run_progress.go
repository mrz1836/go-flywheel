package core

import (
	"bytes"
	"encoding/json"
	"math"
	"slices"
	"sync"
)

// The bounds a RunProgress is kept within.
const (
	// MaxProgressMessageBytes is the longest RunProgress.Message kept: a longer
	// one is cut at this many bytes, on a rune boundary.
	MaxProgressMessageBytes = 256
	// MaxProgressCounters is the most RunProgress.Counters kept: past it, the
	// counters whose keys sort first are kept, so which ones survive does not
	// depend on the order the worker added them.
	MaxProgressCounters = 8
)

// maxProgressCounterKeyBytes is the longest counter key kept; a longer key, or
// an empty one, is dropped.
const maxProgressCounterKeyBytes = 32

// RunProgress is a running attempt's report of how far along it is, set by its
// worker with Job.SetProgress and written by the attempt's heartbeat. Every
// field is optional.
type RunProgress struct {
	// Percent is how much of the work is done, from 0 to 100: a value outside
	// that range is clamped to it, and NaN is dropped.
	Percent *float64 `json:"percent,omitempty"`
	// Message says what the attempt is doing, cut at MaxProgressMessageBytes on
	// a rune boundary.
	Message string `json:"message,omitempty"`
	// Counters are named tallies (rows copied, pages fetched): at most
	// MaxProgressCounters of them, each key 1 to 32 bytes. An empty or longer
	// key is dropped, and past the limit the keys that sort first are kept.
	Counters map[string]int64 `json:"counters,omitempty"`
}

// SetProgress records how far along the attempt is. It keeps a normalized copy
// of p in memory, replacing the last one, and returns at once: the attempt's
// next heartbeat writes it to the attempt's run, at most once a
// RunnerConfig.HeartbeatInterval and only when it changed, so a worker may call
// it as often as it likes. JobRunView.Progress and RunningJob.Progress read it
// back.
//
// It is safe for concurrent use. A run shorter than one heartbeat writes no
// progress, nor does an attempt whose Runner renews no lease (a negative
// HeartbeatInterval, or a driver that issues no lease token), nor one whose
// claim is gone. On a Job the Runner did not build — one a test constructs —
// it does nothing.
func (j *Job[A]) SetProgress(p RunProgress) {
	if j == nil || j.progress == nil {
		return
	}
	j.progress.set(p)
}

// progressSink is one attempt's progress between its worker and its heartbeat:
// the value last set, and enough bookkeeping to write it only when it changed.
// version counts the sets; written is the version the last successful write
// carried, and lastWritten its JSON, so a set that repeats the written value is
// no change.
type progressSink struct {
	mu          sync.Mutex
	value       RunProgress
	version     uint64
	written     uint64
	lastWritten []byte
}

// newProgressSink returns an empty sink: nothing set, nothing to write.
func newProgressSink() *progressSink { return &progressSink{} }

// set normalizes p and keeps it as the value to write next.
func (s *progressSink) set(p RunProgress) {
	n := normalizeProgress(p)
	s.mu.Lock()
	s.value = n
	s.version++
	s.mu.Unlock()
}

// latest returns the value last set.
func (s *progressSink) latest() RunProgress {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.value
}

// pending returns the value to write, as JSON, and the version it carries. ok
// is false when nothing was set since the last write, or what was set encodes
// to what the last write stored.
func (s *progressSink) pending() (raw []byte, version uint64, ok bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.version == s.written {
		return nil, 0, false
	}
	raw, err := json.Marshal(s.value)
	if err != nil || bytes.Equal(raw, s.lastWritten) {
		// A normalized value always encodes; a value that repeats the last write
		// is not a change.
		s.written = s.version
		return nil, 0, false
	}
	return raw, s.version, true
}

// wrote records that raw, the value of version, reached the database. A set
// that landed after pending keeps a later version, so the next heartbeat
// writes it.
func (s *progressSink) wrote(raw []byte, version uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastWritten = raw
	s.written = max(s.written, version)
}

// normalizeProgress applies RunProgress's bounds, copying what it keeps so a
// worker that reuses its map cannot change a stored value.
func normalizeProgress(p RunProgress) RunProgress {
	out := RunProgress{Message: truncate(p.Message, MaxProgressMessageBytes)}
	if p.Percent != nil && !math.IsNaN(*p.Percent) {
		pct := min(max(*p.Percent, 0), 100)
		out.Percent = &pct
	}
	keys := make([]string, 0, len(p.Counters))
	for k := range p.Counters {
		if k != "" && len(k) <= maxProgressCounterKeyBytes {
			keys = append(keys, k)
		}
	}
	if len(keys) == 0 {
		return out
	}
	slices.Sort(keys)
	keys = keys[:min(len(keys), MaxProgressCounters)]
	out.Counters = make(map[string]int64, len(keys))
	for _, k := range keys {
		out.Counters[k] = p.Counters[k]
	}
	return out
}

// decodeProgress reads a stored progress. An empty value, or one that does not
// decode, is none: a run's progress is a report, and a row a host wrote by hand
// must not fail the page that reads it.
func decodeProgress(raw []byte) *RunProgress {
	if len(raw) == 0 || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return nil
	}
	var p RunProgress
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil
	}
	return &p
}
