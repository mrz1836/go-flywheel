package workers

import (
	"context"
	"errors"
	"os/exec"

	flywheel "github.com/mrz1836/go-flywheel"
)

// PythonKind is the job kind PythonWorker handles.
const PythonKind = "python"

// defaultPythonInterpreters are tried in order when no interpreter is configured:
// a modern system has python3; some have only python.
var defaultPythonInterpreters = []string{"python3", "python"}

// PythonArgs is the JSON payload for a python job. Provide exactly one of Script
// (a path to a .py file), Module (run with -m), or Inline (run with -c).
type PythonArgs struct {
	// Script is the path to a .py file: `<interpreter> <script> <args...>`.
	Script string `json:"script,omitempty"`
	// Module is an importable module run with `<interpreter> -m <module> <args...>`.
	Module string `json:"module,omitempty"`
	// Inline is a snippet run with `<interpreter> -c <inline> <args...>`.
	Inline string `json:"inline,omitempty"`
	// Args are arguments passed after the script/module/snippet (seen in sys.argv).
	Args []string `json:"args,omitempty"`
	// Interpreter is the python binary. Empty resolves to python3, falling back to
	// python; set an absolute path to pin a virtualenv interpreter.
	Interpreter string `json:"interpreter,omitempty"`
	// Env is extra environment passed to the child, merged over the allowlisted
	// host environment.
	Env map[string]string `json:"env,omitempty"`
	// Dir is the working directory. Empty uses the daemon's.
	Dir string `json:"dir,omitempty"`
	// Stdin, when non-empty, is written to the program's standard input.
	Stdin string `json:"stdin,omitempty"`
	// TimeoutSeconds, when > 0, bounds the run.
	TimeoutSeconds int `json:"timeout_seconds,omitempty"`
}

// Kind names the python job kind so flywheel.Insert/Enqueue can target it.
func (PythonArgs) Kind() string { return PythonKind }

// PythonWorker runs a Python script durably and records its captured streams, exit
// code, and timing into the audit trail, with flywheel's retry ladder on failure.
// It is a thin, typed convenience over the shared exec engine that resolves the
// interpreter (python3, falling back to python) so a schedule reads naturally.
type PythonWorker struct {
	// Interpreter overrides the python binary for every job this worker runs. Empty
	// resolves per job from PythonArgs.Interpreter, then python3/python on PATH.
	Interpreter string
	// EnvAllowlist names the host environment variables passed through to the child.
	// Nil uses a small safe default (PATH, HOME, SHELL, LANG, TMPDIR); an explicit
	// empty slice passes no host environment at all.
	EnvAllowlist []string
	// MaxOutputBytes caps the captured stdout/stderr stored in ExecOutput. Zero uses
	// a 64 KiB default.
	MaxOutputBytes int
	// PermanentExitCodes are exit codes treated as permanent failures (no retry).
	PermanentExitCodes []int
	// lookPath resolves an interpreter name to a path; nil uses exec.LookPath. It is
	// the injectable seam that makes interpreter resolution testable.
	lookPath func(string) (string, error)
}

// Kind names the python job kind.
func (PythonWorker) Kind() string { return PythonKind }

// Work runs the program and captures its outcome. The captured streams and exit
// code are always present in Result.Output (an ExecOutput).
func (w PythonWorker) Work(ctx context.Context, job *flywheel.Job[PythonArgs]) (flywheel.Result, error) {
	inv, err := job.Args.invocation(w.resolveInterpreter)
	if err != nil {
		return flywheel.Result{}, err
	}
	out, runErr := runProcess(ctx, inv, w.EnvAllowlist, w.MaxOutputBytes)
	return flywheel.Result{Output: out}, runErr
}

// Classify reuses the shared exec classification: a missing interpreter or a
// configured permanent exit code is permanent; every other failure is transient.
func (w PythonWorker) Classify(err error) flywheel.ErrorClass {
	return classify(err, w.PermanentExitCodes)
}

// resolveInterpreter chooses the interpreter for a job: an explicit per-job value
// wins, then the worker's Interpreter, then the first of python3/python found on
// PATH. When none resolve it returns "python3" so the run fails with a clear
// command-not-found error that classify marks permanent.
func (w PythonWorker) resolveInterpreter(jobInterp string) string {
	if jobInterp != "" {
		return jobInterp
	}
	if w.Interpreter != "" {
		return w.Interpreter
	}
	look := w.lookPath
	if look == nil {
		look = exec.LookPath
	}
	for _, name := range defaultPythonInterpreters {
		if _, err := look(name); err == nil {
			return name
		}
	}
	return defaultPythonInterpreters[0]
}

// invocation translates the python args into a resolved execInvocation, using
// resolve to pick the interpreter. It requires exactly one of Script, Module, or
// Inline.
func (a PythonArgs) invocation(resolve func(string) string) (execInvocation, error) {
	var head []string
	switch {
	case nonEmptyCount(a.Script, a.Module, a.Inline) != 1:
		return execInvocation{}, errors.New("python: set exactly one of script, module, or inline")
	case a.Script != "":
		head = []string{a.Script}
	case a.Module != "":
		head = []string{"-m", a.Module}
	default:
		head = []string{"-c", a.Inline}
	}
	return execInvocation{
		Command:        resolve(a.Interpreter),
		Args:           append(head, a.Args...),
		Env:            a.Env,
		Dir:            a.Dir,
		Stdin:          a.Stdin,
		TimeoutSeconds: a.TimeoutSeconds,
	}, nil
}

// nonEmptyCount counts the non-empty strings in vals.
func nonEmptyCount(vals ...string) int {
	n := 0
	for _, v := range vals {
		if v != "" {
			n++
		}
	}
	return n
}
