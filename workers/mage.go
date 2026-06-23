package workers

import (
	"context"
	"errors"

	flywheel "github.com/mrz1836/go-flywheel"
)

// MageKind is the job kind MageWorker handles.
const MageKind = "mage"

// defaultMageBinary is the build tool MageWorker runs when no binary is configured.
// It defaults to magex (mage-x), which ships 150+ built-in commands and needs no
// magefile; set "mage" (or an absolute path) for vanilla mage.
const defaultMageBinary = "magex"

// MageArgs is the JSON payload for a mage job: the magex/mage targets to run.
type MageArgs struct {
	// Targets are the commands (and any params) passed to the build tool, e.g.
	// ["test"], ["lint"], or ["version:bump", "push=true", "bump=patch"]. At least
	// one target is required.
	Targets []string `json:"targets"`
	// Binary is the build tool to invoke. Empty defaults to "magex"; set "mage" (or
	// an absolute path) to use a different one.
	Binary string `json:"binary,omitempty"`
	// Dir is the module root the tool runs in (where .mage.yaml / go.mod live).
	// Empty uses the daemon's working directory.
	Dir string `json:"dir,omitempty"`
	// Env is extra environment passed to the child, merged over the allowlisted
	// host environment.
	Env map[string]string `json:"env,omitempty"`
	// TimeoutSeconds, when > 0, bounds the run.
	TimeoutSeconds int `json:"timeout_seconds,omitempty"`
}

// Kind names the mage job kind so flywheel.Insert/Enqueue can target it.
func (MageArgs) Kind() string { return MageKind }

// MageWorker runs magex (or mage) targets durably and records their captured
// streams, exit code, and timing into the audit trail, with flywheel's retry ladder
// on failure. It is a thin, typed convenience over the shared exec engine for the
// Go-native task runner this project already builds with.
type MageWorker struct {
	// Binary overrides the build tool for every job this worker runs. Empty resolves
	// per job from MageArgs.Binary, then "magex".
	Binary string
	// EnvAllowlist names the host environment variables passed through to the child.
	// Nil uses a small safe default (PATH, HOME, SHELL, LANG, TMPDIR); an explicit
	// empty slice passes no host environment at all.
	EnvAllowlist []string
	// MaxOutputBytes caps the captured stdout/stderr stored in ExecOutput. Zero uses
	// a 64 KiB default.
	MaxOutputBytes int
	// PermanentExitCodes are exit codes treated as permanent failures (no retry).
	PermanentExitCodes []int
}

// Kind names the mage job kind.
func (MageWorker) Kind() string { return MageKind }

// Work runs the targets and captures the outcome. The captured streams and exit
// code are always present in Result.Output (an ExecOutput).
func (w MageWorker) Work(ctx context.Context, job *flywheel.Job[MageArgs]) (flywheel.Result, error) {
	inv, err := job.Args.invocation(w.Binary)
	if err != nil {
		return flywheel.Result{}, err
	}
	out, runErr := runProcess(ctx, inv, w.EnvAllowlist, w.MaxOutputBytes)
	return flywheel.Result{Output: out}, runErr
}

// Classify reuses the shared exec classification: a missing build tool or a
// configured permanent exit code is permanent; every other failure is transient.
func (w MageWorker) Classify(err error) flywheel.ErrorClass {
	return classify(err, w.PermanentExitCodes)
}

// invocation translates the mage args into a resolved execInvocation. binaryOverride
// (the worker's Binary) wins over MageArgs.Binary, which wins over the magex default.
// At least one target is required.
func (a MageArgs) invocation(binaryOverride string) (execInvocation, error) {
	if len(a.Targets) == 0 {
		return execInvocation{}, errors.New("mage: at least one target is required")
	}
	binary := binaryOverride
	if binary == "" {
		binary = a.Binary
	}
	if binary == "" {
		binary = defaultMageBinary
	}
	return execInvocation{
		Command:        binary,
		Args:           append([]string{}, a.Targets...),
		Env:            a.Env,
		Dir:            a.Dir,
		TimeoutSeconds: a.TimeoutSeconds,
	}, nil
}
