//go:build loadtest

package main

import (
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/mrz1836/go-flywheel/loadtest"
)

// The fault grammar is `name[@fraction][:duration]`:
//
//	none                      no fault
//	kill-worker               at the name's default fraction
//	kill-worker@0.4           at 40% drained
//	pause-database@0.5:60s    at 50% drained, for a minute
//
// Both modifiers are optional and order-independent in meaning but not in
// syntax: the fraction attaches to the name, the duration follows the colon.
// The grammar is a superset of the two forms the scenarios were already written
// in — `name@fraction` and `name:duration` — so a command line written before
// the flag existed still parses.
const (
	// defaultFaultFraction is where a fault fires when the command line names no
	// point.
	//
	// Config.validate requires the fraction to be strictly inside (0,1) — a fault
	// at 0 fires before the run starts and one at 1 fires after it ends — so
	// there is no "unset" value to pass through, and every name needs a default
	// that is a real point in the drain. Half way is the one point that is
	// equally far from both ends.
	defaultFaultFraction = 0.5
	// defaultPauseWindow is how long pause-database lasts when the command line
	// gives no duration. Long enough that a drain visibly stalls, short enough
	// that a scenario still finishes.
	defaultPauseWindow = 30 * time.Second
	// faultNone is the spelling for "no fault", so a scenario script can pass the
	// flag unconditionally.
	faultNone = "none"
)

// faultBuilder turns a parsed fault spec into a Fault.
type faultBuilder func(spec faultSpec) (loadtest.Fault, error)

// faultSpec is one parsed `-fault` value.
type faultSpec struct {
	name     string
	fraction float64
	// window is the duration after the colon, or zero when none was given.
	window time.Duration
}

// faultBuilders maps a command-line name to its constructor.
//
// It is a map rather than a switch so the error message can list what is
// available, which is the difference between "unknown fault" and a usable
// message.
//
//nolint:gochecknoglobals // static, read-only name→constructor registry
var faultBuilders = map[string]faultBuilder{
	"kill-worker": func(spec faultSpec) (loadtest.Fault, error) {
		if spec.window != 0 {
			return nil, fmt.Errorf("scenario: -fault kill-worker takes no duration: the kill is permanent")
		}
		return loadtest.KillWorker{Fraction: spec.fraction}, nil
	},
	"pause-database": func(spec faultSpec) (loadtest.Fault, error) {
		window := spec.window
		if window == 0 {
			window = defaultPauseWindow
		}
		return loadtest.PauseDatabase{Fraction: spec.fraction, For: window}, nil
	},
}

// faultNames returns the accepted names, sorted, for a usage or error message.
func faultNames() []string {
	names := slices.Sorted(maps.Keys(faultBuilders))
	return append([]string{faultNone}, names...)
}

// parseFault maps a `-fault` value onto a Fault. An empty value or "none"
// yields a nil Fault, which is a run with no fault rather than an error.
func parseFault(value string) (loadtest.Fault, error) {
	spec, err := parseFaultSpec(value)
	if err != nil {
		return nil, err
	}
	if spec.name == "" {
		return nil, nil //nolint:nilnil // no fault is a valid configuration, not a failure
	}
	build, ok := faultBuilders[spec.name]
	if !ok {
		return nil, fmt.Errorf("scenario: -fault %q is not one of %s", spec.name, strings.Join(faultNames(), ", "))
	}
	return build(spec)
}

// parseFaultSpec splits `name[@fraction][:duration]` into its parts, applying
// the default fraction. A name of "" means no fault.
func parseFaultSpec(value string) (faultSpec, error) {
	value = strings.TrimSpace(value)
	if value == "" || value == faultNone {
		return faultSpec{}, nil
	}

	spec := faultSpec{fraction: defaultFaultFraction}

	// The duration comes off first: a fraction cannot contain a colon, but a
	// duration can contain the characters a fraction uses.
	head := value
	if name, window, found := strings.Cut(value, ":"); found {
		head = name
		d, err := time.ParseDuration(window)
		if err != nil {
			return faultSpec{}, fmt.Errorf("scenario: -fault %q: %q is not a duration", value, window)
		}
		if d <= 0 {
			return faultSpec{}, fmt.Errorf("scenario: -fault %q: a duration of %s is a fault that never happens", value, d)
		}
		spec.window = d
	}

	if name, fraction, found := strings.Cut(head, "@"); found {
		head = name
		f, err := strconv.ParseFloat(fraction, 64)
		if err != nil {
			return faultSpec{}, fmt.Errorf("scenario: -fault %q: %q is not a fraction", value, fraction)
		}
		// Bounds are the harness's to enforce — Config.validate rejects anything
		// outside (0,1) with the reasoning attached — so this only parses.
		spec.fraction = f
	}

	spec.name = head
	if spec.name == "" {
		return faultSpec{}, fmt.Errorf("scenario: -fault %q names no fault", value)
	}
	return spec, nil
}
