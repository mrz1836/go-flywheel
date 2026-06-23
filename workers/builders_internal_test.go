package workers

import (
	"errors"
	"os/exec"
	"testing"

	flywheel "github.com/mrz1836/go-flywheel"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// These white-box tests assert the exact command line each worker builds (no
// process is spawned) plus the shared interpreter resolution and classification —
// the logic that is fully deterministic and carries most of the coverage.

func TestShellInvocation(t *testing.T) {
	t.Parallel()

	inv, err := ShellArgs{Script: "run.sh", Args: []string{"a", "b"}}.invocation()
	require.NoError(t, err)
	assert.Equal(t, "sh", inv.Command)
	assert.Equal(t, []string{"run.sh", "a", "b"}, inv.Args)

	inv, err = ShellArgs{Inline: "echo hi"}.invocation()
	require.NoError(t, err)
	assert.Equal(t, "sh", inv.Command)
	assert.Equal(t, []string{"-c", "echo hi"}, inv.Args)

	// Inline with args: the shell name becomes $0 so the positionals are $1, $2...
	inv, err = ShellArgs{Inline: "echo $1", Args: []string{"x"}, Shell: "bash", Dir: "/tmp", Stdin: "in", TimeoutSeconds: 9}.invocation()
	require.NoError(t, err)
	assert.Equal(t, "bash", inv.Command)
	assert.Equal(t, []string{"-c", "echo $1", "bash", "x"}, inv.Args)
	assert.Equal(t, "/tmp", inv.Dir)
	assert.Equal(t, "in", inv.Stdin)
	assert.Equal(t, 9, inv.TimeoutSeconds)

	_, err = ShellArgs{}.invocation()
	require.Error(t, err, "neither script nor inline")
	_, err = ShellArgs{Script: "x", Inline: "y"}.invocation()
	require.Error(t, err, "both script and inline")
}

func TestPythonInvocation(t *testing.T) {
	t.Parallel()
	resolve := func(string) string { return "PY" }

	inv, err := PythonArgs{Script: "x.py", Args: []string{"--flag"}}.invocation(resolve)
	require.NoError(t, err)
	assert.Equal(t, "PY", inv.Command)
	assert.Equal(t, []string{"x.py", "--flag"}, inv.Args)

	inv, err = PythonArgs{Module: "http.server", Args: []string{"8000"}}.invocation(resolve)
	require.NoError(t, err)
	assert.Equal(t, []string{"-m", "http.server", "8000"}, inv.Args)

	inv, err = PythonArgs{Inline: "print(1)"}.invocation(resolve)
	require.NoError(t, err)
	assert.Equal(t, []string{"-c", "print(1)"}, inv.Args)

	_, err = PythonArgs{}.invocation(resolve)
	require.Error(t, err, "no source")
	_, err = PythonArgs{Script: "a", Module: "b"}.invocation(resolve)
	require.Error(t, err, "two sources")
}

func TestPythonResolveInterpreter(t *testing.T) {
	t.Parallel()

	// An explicit per-job interpreter wins over everything.
	assert.Equal(t, "/venv/bin/python", PythonWorker{Interpreter: "pypy"}.resolveInterpreter("/venv/bin/python"))

	// The worker's Interpreter is next.
	assert.Equal(t, "pypy", PythonWorker{Interpreter: "pypy"}.resolveInterpreter(""))

	// Otherwise lookPath decides: python3 wins when present.
	found := func(want string) func(string) (string, error) {
		return func(n string) (string, error) {
			if n == want {
				return "/usr/bin/" + n, nil
			}
			return "", errors.New("not found")
		}
	}
	assert.Equal(t, "python3", PythonWorker{lookPath: found("python3")}.resolveInterpreter(""))
	assert.Equal(t, "python", PythonWorker{lookPath: found("python")}.resolveInterpreter(""))

	// None present: fall back to python3 so the run fails with a clear error.
	none := func(string) (string, error) { return "", errors.New("not found") }
	assert.Equal(t, "python3", PythonWorker{lookPath: none}.resolveInterpreter(""))
}

func TestMageInvocation(t *testing.T) {
	t.Parallel()

	// Default binary is magex.
	inv, err := MageArgs{Targets: []string{"test"}}.invocation("")
	require.NoError(t, err)
	assert.Equal(t, "magex", inv.Command)
	assert.Equal(t, []string{"test"}, inv.Args)

	// MageArgs.Binary is used when no worker override is set.
	inv, err = MageArgs{Targets: []string{"build"}, Binary: "mage", Dir: "/repo", TimeoutSeconds: 30}.invocation("")
	require.NoError(t, err)
	assert.Equal(t, "mage", inv.Command)
	assert.Equal(t, "/repo", inv.Dir)
	assert.Equal(t, 30, inv.TimeoutSeconds)

	// The worker override wins over MageArgs.Binary.
	inv, err = MageArgs{Targets: []string{"x"}, Binary: "mage"}.invocation("magex")
	require.NoError(t, err)
	assert.Equal(t, "magex", inv.Command)

	// Multiple targets and params pass through verbatim.
	inv, err = MageArgs{Targets: []string{"version:bump", "push=true", "bump=patch"}}.invocation("")
	require.NoError(t, err)
	assert.Equal(t, []string{"version:bump", "push=true", "bump=patch"}, inv.Args)

	_, err = MageArgs{}.invocation("")
	require.Error(t, err, "no targets")
}

func TestClassifyAndHelpers(t *testing.T) {
	t.Parallel()

	assert.Equal(t, flywheel.ErrorTransient, classify(errors.New("boom"), nil))
	assert.Equal(t, flywheel.ErrorPermanent, classify(&exec.Error{Name: "x", Err: errors.New("nf")}, nil), "missing binary is permanent")
	assert.Equal(t, flywheel.ErrorPermanent, classify(&execExitError{command: "x", code: 2}, []int{2}), "listed exit code is permanent")
	assert.Equal(t, flywheel.ErrorTransient, classify(&execExitError{command: "x", code: 3}, []int{2}), "unlisted exit code is transient")

	assert.Equal(t, 0, nonEmptyCount("", ""))
	assert.Equal(t, 2, nonEmptyCount("a", "", "b"))
}
