package config

import (
	"errors"
	"fmt"
	"time"
)

// RetentionConfig is how long a host's Scheduler keeps what it prunes: finished
// jobs, with their runs and captured log lines, by default and by rule; the
// control history; queue-depth samples; stopped nodes' registry rows; and the
// hourly stats rollups. Each field sets the SchedulerConfig field named beside
// it (SchedulerConfig.SetRetention applies them all), and its zero means what
// that field's zero means. A zero RetentionConfig leaves every one at the
// runtime's default: retention off, and the control history kept for good.
//
// Ages are durations: nanoseconds in JSON, as every duration in JobsConfig is,
// and a Go duration string such as "720h" in an environment variable. Rules
// come from the config file alone; no environment variable carries them.
type RetentionConfig struct {
	// Jobs is how long a finished job is kept after it finalized
	// (SchedulerConfig.RetentionMaxAge). Zero turns retention off, so finished
	// jobs are kept for good.
	Jobs time.Duration `json:"jobs" env:"JOB_RETENTION_JOBS"`

	// Rules keep the finished jobs each matches for its own MaxAge instead of
	// Jobs (SchedulerConfig.RetentionRules): a kind, a terminal state, or both.
	// The most specific rule that matches a job decides: one naming its kind and
	// state, then one naming its kind, then one naming its state. They need Jobs.
	Rules []RetentionRule `json:"rules"`

	// Audit is how long the control history (job_audit) keeps an entry
	// (SchedulerConfig.AuditRetention). Zero keeps every entry.
	Audit time.Duration `json:"audit" env:"JOB_RETENTION_AUDIT"`

	// QueueSamples is how long queue-depth samples are kept
	// (SchedulerConfig.QueueSampleRetention). Zero selects 30 days.
	QueueSamples time.Duration `json:"queue_samples" env:"JOB_RETENTION_QUEUE_SAMPLES"`

	// Nodes is how long a stopped node's registry row is kept after its last
	// heartbeat (SchedulerConfig.NodeRetention). Zero selects 7 days.
	Nodes time.Duration `json:"nodes" env:"JOB_RETENTION_NODES"`

	// Stats is how long the hourly stats rollups are kept
	// (SchedulerConfig.StatsRetention). Zero selects 400 days.
	Stats time.Duration `json:"stats" env:"JOB_RETENTION_STATS"`
}

// RetentionRule keeps the finished jobs it matches for MaxAge after they
// finalized, in place of RetentionConfig.Jobs. It names a kind, a terminal
// state (succeeded, cancelled, or discarded), or both; an empty Kind matches
// every kind and an empty State every terminal state.
type RetentionRule struct {
	// Kind is the job kind the rule matches.
	Kind string `json:"kind"`

	// State is the terminal state the rule matches.
	State string `json:"state"`

	// MaxAge is how long the jobs the rule matches are kept after finalizing.
	MaxAge time.Duration `json:"max_age"`
}

// Sentinel errors for a retention block. Use errors.Is() to check for them.
var (
	errNegativeRetentionAge  = errors.New("JobsConfig.Retention ages must not be negative")
	errRetentionRulesNeedAge = errors.New(
		"JobsConfig.Retention.Rules need Retention.Jobs: a rule changes how long retention keeps " +
			"the jobs it matches, and Jobs is what turns retention on",
	)
	errRetentionRuleTarget   = errors.New("names neither a kind nor a state")
	errRetentionRuleState    = errors.New("state must be succeeded, cancelled, or discarded")
	errRetentionRuleMaxAge   = errors.New("max_age must be greater than zero")
	errRetentionRuleRepeated = errors.New("repeats the kind and state of an earlier rule")
)

// terminalStates are the states a finished job ends in, the ones a rule may
// name.
var terminalStates = map[string]bool{"succeeded": true, "cancelled": true, "discarded": true}

// configured reports whether any retention setting is present.
func (r *RetentionConfig) configured() bool {
	return r.Jobs != 0 || len(r.Rules) > 0 || r.Audit != 0 || r.QueueSamples != 0 || r.Nodes != 0 || r.Stats != 0
}

// validate checks the block on its own terms: no negative age, and rules the
// Scheduler would accept. It cannot see the stats rollup's grace, so a rule
// shorter than one hour plus that grace is left to NewSchedulerWithConfig.
func (r *RetentionConfig) validate() error {
	ages := []struct {
		name string
		age  time.Duration
	}{
		{"Jobs", r.Jobs}, {"Audit", r.Audit}, {"QueueSamples", r.QueueSamples}, {"Nodes", r.Nodes}, {"Stats", r.Stats},
	}
	for _, a := range ages {
		if a.age < 0 {
			return fmt.Errorf("%w: Retention.%s is %s", errNegativeRetentionAge, a.name, a.age)
		}
	}
	if len(r.Rules) > 0 && r.Jobs == 0 {
		return errRetentionRulesNeedAge
	}
	type target struct{ kind, state string }
	seen := make(map[target]bool, len(r.Rules))
	for i, rule := range r.Rules {
		field := fmt.Sprintf("JobsConfig.Retention.Rules[%d]", i)
		switch {
		case rule.Kind == "" && rule.State == "":
			return fmt.Errorf("%s %w", field, errRetentionRuleTarget)
		case rule.State != "" && !terminalStates[rule.State]:
			return fmt.Errorf("%s %w: got %q", field, errRetentionRuleState, rule.State)
		case rule.MaxAge <= 0:
			return fmt.Errorf("%s %w: got %s", field, errRetentionRuleMaxAge, rule.MaxAge)
		}
		key := target{kind: rule.Kind, state: rule.State}
		if seen[key] {
			return fmt.Errorf("%s %w", field, errRetentionRuleRepeated)
		}
		seen[key] = true
	}
	return nil
}
