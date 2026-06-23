package workers

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"slices"
	"strings"
	"time"

	flywheel "github.com/mrz1836/go-flywheel"
)

// ExecKind is the job kind ExecWorker handles.
const ExecKind = "exec"

// ExecArgs is the JSON payload for an exec job: the command to run and how. A
// schedule entry's args template or an Enqueue payload provides these fields.
type ExecArgs struct {
	// Command is the binary or script to run. An absolute path is recommended;
	// a bare name is resolved against the worker's PATH (see ExecWorker env).
	Command string `json:"command"`
	// Args are the command's arguments.
	Args []string `json:"args,omitempty"`
	// Env is extra environment passed to the child, merged over the allowlisted
	// host environment.
	Env map[string]string `json:"env,omitempty"`
	// Dir is the working directory for the command. Empty uses the daemon's.
	Dir string `json:"dir,omitempty"`
	// Stdin, when non-empty, is written to the command's standard input.
	Stdin string `json:"stdin,omitempty"`
	// TimeoutSeconds, when > 0, bounds the command's run time; the worker kills it
	// and the attempt records a timeout outcome.
	TimeoutSeconds int `json:"timeout_seconds,omitempty"`
}

// Kind names the exec job kind so flywheel.Insert/Enqueue can target it.
func (ExecArgs) Kind() string { return ExecKind }

// ExecOutput is the structured Result.Output ExecWorker persists to
// job_runs.output: the exit code, captured (capped) streams, and timing.
type ExecOutput struct {
	ExitCode   int    `json:"exit_code"`
	Stdout     string `json:"stdout"`
	Stderr     string `json:"stderr"`
	DurationMs int64  `json:"duration_ms"`
}

// ExecWorker runs a shell command or binary durably: it captures stdout, stderr,
// and the exit code into the audit trail, and turns a non-zero exit into a
// retryable error so flywheel's backoff ladder applies. It is the drop-in,
// zero-Go replacement for a crontab line — strictly better than cron because each
// run is retried, recorded, and overlap-protected by the lease.
type ExecWorker struct {
	// EnvAllowlist names the host environment variables passed through to the
	// child. A nil slice uses a small safe default (PATH, HOME, SHELL, LANG,
	// TMPDIR); an explicit empty slice passes no host environment at all.
	EnvAllowlist []string
	// MaxOutputBytes caps the captured stdout/stderr stored in ExecOutput.
	// Zero uses a 64 KiB default.
	MaxOutputBytes int
	// PermanentExitCodes are exit codes treated as permanent failures (no retry).
	// By default every non-zero exit is transient and retried.
	PermanentExitCodes []int
}

// Kind names the exec job kind.
func (ExecWorker) Kind() string { return ExecKind }

// Work runs the command and captures its outcome. A non-zero exit or a run
// failure is returned as an error (recorded on the run row); the captured streams
// and exit code are always present in Result.Output.
func (w ExecWorker) Work(ctx context.Context, job *flywheel.Job[ExecArgs]) (flywheel.Result, error) {
	a := job.Args
	if a.Command == "" {
		return flywheel.Result{}, errors.New("exec: command is required")
	}
	out, err := runProcess(ctx, execInvocation(a), w.EnvAllowlist, w.MaxOutputBytes)
	return flywheel.Result{Output: out}, err
}

// Classify makes an unresolvable command (not found / not executable) and any
// configured PermanentExitCodes permanent; every other failure stays transient
// and retries. It satisfies flywheel's optional Classifier interface.
func (w ExecWorker) Classify(err error) flywheel.ErrorClass {
	return classify(err, w.PermanentExitCodes)
}

// execInvocation is a fully-resolved process to run: the command and its
// arguments, plus the environment, working directory, stdin, and timeout that
// shape the run. The shell, python, and mage workers each translate their typed
// args into one of these and hand it to runProcess, so every worker kind shares a
// single execution path, output-capture policy, and error-shaping contract.
type execInvocation struct {
	Command        string
	Args           []string
	Env            map[string]string
	Dir            string
	Stdin          string
	TimeoutSeconds int
}

// runProcess executes inv and captures its outcome. It bounds the run with
// TimeoutSeconds (when > 0), captures capped stdout/stderr and the exit code into
// an ExecOutput, and shapes failures the way the Runner expects: a fired deadline
// becomes the context error (classified as a timeout), a non-zero exit becomes an
// *execExitError carrying the code, and a start failure (e.g. command not found)
// is wrapped so classify can mark it permanent. allow is the host-env allowlist
// (nil uses the safe default, empty passes nothing); maxOut caps each captured
// stream (<= 0 uses the 64 KiB default). The ExecOutput is always returned — even
// on error — so the audit trail records what the command produced.
func runProcess(ctx context.Context, inv execInvocation, allow []string, maxOut int) (ExecOutput, error) {
	if inv.TimeoutSeconds > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, time.Duration(inv.TimeoutSeconds)*time.Second)
		defer cancel()
	}
	if maxOut <= 0 {
		maxOut = defaultMaxOutputBytes
	}
	stdout := &cappedBuffer{cap: maxOut}
	stderr := &cappedBuffer{cap: maxOut}

	cmd := exec.CommandContext(ctx, inv.Command, inv.Args...) //nolint:gosec // workers intentionally run operator-configured commands
	cmd.Env = buildEnv(allow, inv.Env)
	cmd.Dir = inv.Dir
	if inv.Stdin != "" {
		cmd.Stdin = strings.NewReader(inv.Stdin)
	}
	cmd.Stdout = stdout
	cmd.Stderr = stderr

	start := time.Now()
	runErr := cmd.Run()
	out := ExecOutput{
		Stdout:     stdout.String(),
		Stderr:     stderr.String(),
		DurationMs: time.Since(start).Milliseconds(),
	}
	if cmd.ProcessState != nil {
		out.ExitCode = cmd.ProcessState.ExitCode()
	}
	if runErr == nil {
		return out, nil
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		// The deadline/cancel fired and killed the process; surface the ctx error
		// so the Runner classifies it as a timeout.
		return out, fmt.Errorf("exec %q timed out: %w", inv.Command, ctxErr)
	}
	var exitErr *exec.ExitError
	if errors.As(runErr, &exitErr) {
		return out, &execExitError{command: inv.Command, code: out.ExitCode}
	}
	return out, fmt.Errorf("exec %q failed to start: %w", inv.Command, runErr)
}

// classify maps a runProcess error to a flywheel.ErrorClass: an unresolvable
// command (not found / not executable) and any exit code listed in permanent are
// permanent failures (no retry); every other failure stays transient and retries.
// The exec, shell, python, and mage workers share it via their Classify method.
func classify(err error, permanent []int) flywheel.ErrorClass {
	var notFound *exec.Error
	if errors.As(err, &notFound) {
		return flywheel.ErrorPermanent
	}
	var exitErr *execExitError
	if errors.As(err, &exitErr) && slices.Contains(permanent, exitErr.code) {
		return flywheel.ErrorPermanent
	}
	return flywheel.ErrorTransient
}

// buildEnv assembles a child environment from the allowlisted host variables plus
// the per-job extras (extras win on conflict). A nil allowlist uses the safe
// default set (PATH, HOME, SHELL, LANG, TMPDIR); an explicit empty allowlist
// passes no host environment at all.
func buildEnv(allow []string, extra map[string]string) []string {
	if allow == nil {
		allow = defaultEnvAllowlist()
	}
	env := make([]string, 0, len(allow)+len(extra))
	for _, key := range allow {
		if v, ok := os.LookupEnv(key); ok {
			env = append(env, key+"="+v)
		}
	}
	for k, v := range extra {
		env = append(env, k+"="+v)
	}
	return env
}

// defaultEnvAllowlist is the safe default set of host environment variables
// passed through to a child command.
func defaultEnvAllowlist() []string {
	return []string{"PATH", "HOME", "SHELL", "LANG", "TMPDIR"}
}

// execExitError carries a command's non-zero exit code so Classify can decide
// whether that code is permanent.
type execExitError struct {
	command string
	code    int
}

// Error renders the command and its exit code.
func (e *execExitError) Error() string {
	return fmt.Sprintf("exec %q exited with code %d", e.command, e.code)
}

// ExitCode returns the command's exit code.
func (e *execExitError) ExitCode() int { return e.code }

// cappedBuffer is an io.Writer that retains at most cap bytes, discarding the
// rest while still reporting a full write so the child process is never blocked.
type cappedBuffer struct {
	buf bytes.Buffer
	cap int
}

// Write appends up to the remaining cap; bytes beyond the cap are discarded.
func (c *cappedBuffer) Write(p []byte) (int, error) {
	if remaining := c.cap - c.buf.Len(); remaining > 0 {
		if len(p) > remaining {
			c.buf.Write(p[:remaining])
		} else {
			c.buf.Write(p)
		}
	}
	return len(p), nil
}

// String returns the retained output.
func (c *cappedBuffer) String() string { return c.buf.String() }
