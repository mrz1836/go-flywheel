package main

import (
	"fmt"
	"strings"
	"time"

	flywheel "github.com/mrz1836/go-flywheel"
	"github.com/spf13/cobra"
)

// day is the unit a retention age prints in when it is a whole number of them.
const day = 24 * time.Hour

// parseRetentionRule reads one --rule: comma-separated key=value pairs, kind,
// state, and keep (a duration, which may end in d or w), keep required and at
// least one of kind and state. The library validates the rest: a state that is
// not terminal, or a keep that is not positive, is refused when the rules are
// used.
func parseRetentionRule(spec string) (flywheel.RetentionRule, error) {
	var (
		rule flywheel.RetentionRule
		seen = map[string]bool{}
	)
	for _, pair := range strings.Split(spec, ",") {
		key, value, ok := strings.Cut(strings.TrimSpace(pair), "=")
		if !ok || key == "" || value == "" {
			return flywheel.RetentionRule{}, fmt.Errorf("--rule %q: want key=value pairs: kind, state, and keep", spec)
		}
		if seen[key] {
			return flywheel.RetentionRule{}, fmt.Errorf("--rule %q: %s is given twice", spec, key)
		}
		seen[key] = true
		switch key {
		case "kind":
			rule.Kind = value
		case "state":
			rule.State = flywheel.JobState(value)
		case "keep":
			age, err := parseHumanDuration(value)
			if err != nil {
				return flywheel.RetentionRule{}, fmt.Errorf("--rule %q: keep: %w", spec, err)
			}
			rule.MaxAge = age
		default:
			return flywheel.RetentionRule{}, fmt.Errorf("--rule %q: unknown key %q (want kind, state, or keep)", spec, key)
		}
	}
	switch {
	case !seen["keep"]:
		return flywheel.RetentionRule{}, fmt.Errorf("--rule %q: keep is required", spec)
	case rule.Kind == "" && rule.State == "":
		return flywheel.RetentionRule{}, fmt.Errorf("--rule %q: give a kind, a state, or both", spec)
	}
	return rule, nil
}

// parseRetentionRules reads every --rule given.
func parseRetentionRules(specs []string) ([]flywheel.RetentionRule, error) {
	rules := make([]flywheel.RetentionRule, 0, len(specs))
	for _, spec := range specs {
		rule, err := parseRetentionRule(spec)
		if err != nil {
			return nil, err
		}
		rules = append(rules, rule)
	}
	return rules, nil
}

// formatRetentionRule writes a rule back in the form --rule takes.
func formatRetentionRule(rule flywheel.RetentionRule) string {
	var parts []string
	if rule.Kind != "" {
		parts = append(parts, "kind="+rule.Kind)
	}
	if rule.State != "" {
		parts = append(parts, "state="+string(rule.State))
	}
	return strings.Join(append(parts, "keep="+ageCell(rule.MaxAge)), ",")
}

// ageCell prints a retention age in days when it is a whole number of them,
// and as a Go duration otherwise.
func ageCell(age time.Duration) string {
	if age > 0 && age%day == 0 {
		return fmt.Sprintf("%dd", age/day)
	}
	return age.String()
}

// addRuleFlag registers the repeatable --rule flag on cmd.
func addRuleFlag(cmd *cobra.Command, specs *[]string) {
	cmd.Flags().StringArrayVar(specs, "rule", nil,
		"keep the finished jobs a rule matches for its own age: kind=<kind>,state=<state>,keep=<duration> "+
			"(keep and a kind or a state required; repeat for several; the most specific match decides)")
}
