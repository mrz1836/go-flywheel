package config

import (
	"errors"
	"fmt"
)

// Sentinel errors for job-runtime configuration. Use errors.Is() to check for
// these in callers. They are self-contained so the config package validates
// without importing any external foundation package.
var (
	errMissingJobQueues        = errors.New("missing required field: JobsConfig.Queues (set JOB_QUEUES)")
	errInvalidJobConcurrency   = errors.New("JobsConfig.Concurrency must be >= 1")
	errInvalidJobLeaseDuration = errors.New("JobsConfig.LeaseDuration must be greater than zero")
)

// Validate validates the JobsConfig. The job runtime requires at least one
// queue, a positive concurrency, and a positive lease duration. Validation is
// strict: callers that host the job runtime invoke it directly to fail fast on
// a missing setting. An outer config should only invoke it when
// JobsConfig.Configured reports the block is in use.
func (j *JobsConfig) Validate() error {
	if len(j.Queues) == 0 {
		return errMissingJobQueues
	}
	if j.Concurrency < 1 {
		return fmt.Errorf("%w: got %d", errInvalidJobConcurrency, j.Concurrency)
	}
	if j.LeaseDuration <= 0 {
		return fmt.Errorf("%w: got %s", errInvalidJobLeaseDuration, j.LeaseDuration)
	}
	return nil
}
