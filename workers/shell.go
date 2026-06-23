package workers

import (
	"context"
	"errors"

	flywheel "github.com/mrz1836/go-flywheel"
)

// ShellKind is the job kind ShellWorker handles.
const ShellKind = "shell"

// defaultShell is the interpreter ShellWorker uses when ShellArgs.Shell is empty.
const defaultShell = "sh"

// ShellArgs is the JSON payload for a shell job: a script to run and how. Provide
// exactly one of Script (a path to a .sh file) or Inline (script text run with the
// shell's -c flag).
type ShellArgs struct {
	// Script is the path to a shell script file to execute. Mutually exclusive with
	// Inline. The file needs no executable bit: it runs as `<shell> <script> <args...>`.
	Script string `json:"script,omitempty"`
	// Inline is shell script text run as `<shell> -c <inline>`. Mutually exclusive
	// with Script.
	Inline string `json:"inline,omitempty"`
	// Args are positional arguments passed to the script (seen as $1, $2, ... — and
	// for an inline snippet, $0 is the shell name).
	Args []string `json:"args,omitempty"`
	// Shell is the interpreter. Empty defaults to "sh"; set "bash" (etc.) for
	// shell-specific features.
	Shell string `json:"shell,omitempty"`
	// Env is extra environment passed to the child, merged over the allowlisted
	// host environment.
	Env map[string]string `json:"env,omitempty"`
	// Dir is the working directory for the script. Empty uses the daemon's.
	Dir string `json:"dir,omitempty"`
	// Stdin, when non-empty, is written to the script's standard input.
	Stdin string `json:"stdin,omitempty"`
	// TimeoutSeconds, when > 0, bounds the run; the worker kills it and the attempt
	// records a timeout outcome.
	TimeoutSeconds int `json:"timeout_seconds,omitempty"`
}

// Kind names the shell job kind so flywheel.Insert/Enqueue can target it.
func (ShellArgs) Kind() string { return ShellKind }

// ShellWorker runs a shell script durably — a .sh file or an inline snippet — and
// records its captured streams, exit code, and timing into the audit trail, with
// flywheel's retry ladder applied on failure. It is a thin, typed convenience over
// the shared exec engine; the run is recorded under kind "shell" so the queue reads
// naturally.
type ShellWorker struct {
	// EnvAllowlist names the host environment variables passed through to the child.
	// Nil uses a small safe default (PATH, HOME, SHELL, LANG, TMPDIR); an explicit
	// empty slice passes no host environment at all.
	EnvAllowlist []string
	// MaxOutputBytes caps the captured stdout/stderr stored in ExecOutput. Zero uses
	// a 64 KiB default.
	MaxOutputBytes int
	// PermanentExitCodes are exit codes treated as permanent failures (no retry). By
	// default every non-zero exit is transient and retried.
	PermanentExitCodes []int
}

// Kind names the shell job kind.
func (ShellWorker) Kind() string { return ShellKind }

// Work runs the script and captures its outcome. The captured streams and exit
// code are always present in Result.Output (an ExecOutput); a non-zero exit or a
// run failure is returned as an error.
func (w ShellWorker) Work(ctx context.Context, job *flywheel.Job[ShellArgs]) (flywheel.Result, error) {
	inv, err := job.Args.invocation()
	if err != nil {
		return flywheel.Result{}, err
	}
	out, runErr := runProcess(ctx, inv, w.EnvAllowlist, w.MaxOutputBytes)
	return flywheel.Result{Output: out}, runErr
}

// Classify reuses the shared exec classification: a missing shell or a configured
// permanent exit code is permanent; every other failure is transient.
func (w ShellWorker) Classify(err error) flywheel.ErrorClass {
	return classify(err, w.PermanentExitCodes)
}

// invocation translates the shell args into a resolved execInvocation. It requires
// exactly one of Script or Inline.
func (a ShellArgs) invocation() (execInvocation, error) {
	shell := a.Shell
	if shell == "" {
		shell = defaultShell
	}
	var args []string
	switch {
	case a.Script != "" && a.Inline != "":
		return execInvocation{}, errors.New("shell: set exactly one of script or inline, not both")
	case a.Script != "":
		args = append([]string{a.Script}, a.Args...)
	case a.Inline != "":
		// `<shell> -c <inline> [<shell> <args...>]`: when Args are present the shell
		// name becomes $0 so the positionals line up as $1, $2, ... in the snippet.
		args = []string{"-c", a.Inline}
		if len(a.Args) > 0 {
			args = append(args, shell)
			args = append(args, a.Args...)
		}
	default:
		return execInvocation{}, errors.New("shell: script or inline is required")
	}
	return execInvocation{
		Command:        shell,
		Args:           args,
		Env:            a.Env,
		Dir:            a.Dir,
		Stdin:          a.Stdin,
		TimeoutSeconds: a.TimeoutSeconds,
	}, nil
}
