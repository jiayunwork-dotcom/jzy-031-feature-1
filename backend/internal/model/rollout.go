// Rollout is the gray-release ("canary") state of one rule. While a rollout
// exists, traffic matching the rule is split deterministically: a stable
// percentage of judging subjects is evaluated against NewRule, the rest keeps
// being evaluated against the rule's persisted (old) content. The two
// versions own fully independent quota counters.
//
// Invariants:
//   - Percent is 1..99 for a live row. 0 means "abort" and 100 means
//     "promote to full"; both delete the row immediately, so a rule without
//     an in-progress rollout is always an ordinary single-version rule.
//   - NewRule is validated with exactly the same Rule.Validate rules as a
//     normally persisted rule: an illegal new version can never enter
//     evaluation just because only part of the traffic sees it.
package model

import (
	"fmt"
	"time"
)

// Rollout is persisted in the rollouts table, one row per gray-released rule.
type Rollout struct {
	RuleID    string    `json:"rule_id"`
	Percent   int       `json:"percent"`
	NewRule   Rule      `json:"new_rule"`
	StartedAt time.Time `json:"started_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

// ValidatePercent accepts the legal adjustment range 0..100 inclusive:
// 0 aborts the rollout, 100 promotes it, 1..99 resize the canary bucket.
// Anything outside (and non-numeric input rejected earlier at the JSON
// boundary) is refused with an operator-facing reason.
func ValidatePercent(p int) error {
	if p < 0 || p > 100 {
		return fmt.Errorf("rollout percent must be between 0 and 100, got %d", p)
	}
	return nil
}

// ValidateStart checks a request to begin (or replace) a rollout: the
// percentage must actually split traffic (1..99; 0 is a no-op, 100 is a
// normal full update) and the new version must itself be a legal rule.
func (r *Rollout) ValidateStart() error {
	if r.Percent < 1 || r.Percent > 99 {
		return fmt.Errorf("rollout percent when starting a rollout must be between 1 and 99, got %d (use a plain update for 100, abort for 0)", r.Percent)
	}
	nr := r.NewRule
	nr.ID = r.RuleID
	if err := nr.Validate(); err != nil {
		return fmt.Errorf("new (canary) rule version is invalid: %w", err)
	}
	return nil
}
