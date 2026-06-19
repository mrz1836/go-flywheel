// Package config holds the job-runtime configuration that travels with the
// flywheel runtime. A host that runs the runtime (a worker daemon or a
// scheduled lambda) populates a JobsConfig and validates it before starting.
package config

import "time"

// JobsConfig contains background-job runtime settings shared by the hosts that
// run the job runtime. It is opt-in: a zero-value JobsConfig means the host
// runs no job runtime, so a host's outer config skips it. Once any field is
// set, every field is validated.
type JobsConfig struct {
	// Queues are the logical queues this host claims work from.
	Queues []string `json:"queues" env:"JOB_QUEUES"`

	// LeaseDuration is the visibility timeout on a claimed job.
	LeaseDuration time.Duration `json:"lease_duration" env:"JOB_LEASE_DURATION"`

	// Concurrency is the number of jobs claimed and run per poll.
	Concurrency int `json:"concurrency" env:"JOB_CONCURRENCY"`

	// PollInterval is the optional pause between empty polls.
	PollInterval time.Duration `json:"poll_interval" env:"JOB_POLL_INTERVAL"`
}

// Configured reports whether any job-runtime setting is present. A zero-value
// JobsConfig (no host running the job runtime) reports false.
func (j *JobsConfig) Configured() bool {
	return len(j.Queues) > 0 || j.LeaseDuration != 0 || j.Concurrency != 0 || j.PollInterval != 0
}
