package core

import "github.com/mrz1836/go-flywheel/config"

// SetRetention sets the Scheduler's retention from a host's configuration
// (config.JobsConfig.Retention): RetentionMaxAge from Jobs, RetentionRules from
// Rules, AuditRetention from Audit, QueueSampleRetention from QueueSamples,
// NodeRetention from Nodes, and StatsRetention from Stats. It sets every one of
// them, so a zero in r sets that field to zero, which means what the field's
// own documentation says. It changes no other field and checks nothing:
// config.JobsConfig.Validate checks the block, and NewSchedulerWithConfig checks
// the rules against the stats rollup's grace.
func (c *SchedulerConfig) SetRetention(r config.RetentionConfig) {
	c.RetentionMaxAge = r.Jobs
	c.RetentionRules = nil
	for _, rule := range r.Rules {
		c.RetentionRules = append(c.RetentionRules, RetentionRule{
			Kind:   rule.Kind,
			State:  JobState(rule.State),
			MaxAge: rule.MaxAge,
		})
	}
	c.AuditRetention = r.Audit
	c.QueueSampleRetention = r.QueueSamples
	c.NodeRetention = r.Nodes
	c.StatsRetention = r.Stats
}
