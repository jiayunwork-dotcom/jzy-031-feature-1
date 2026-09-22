// Package canary is the standalone gray-rollout (canary) layer. It owns three
// things and nothing else:
//
//   - the deterministic bucketing of a "judgement subject" (a client, or the
//     combination of dimension values a rule declares) into one of 100 fixed
//     buckets;
//   - the pure, reproducible mapping (rule, subject, percent) -> rule version;
//   - the Rollout state describing one in-flight change (percent plus the old
//     and new rule contents) and its validation.
//
// Bucketing uses FNV-1a over a canonical string, so every gateway instance —
// and every point in time while the percent is unchanged — computes the exact
// same answer without coordinating. Because "canary" is defined as
// bucket < percent, raising the percent only adds subjects to the new side;
// subjects already there can never be reshuffled back.
package canary

import (
	"fmt"
	"hash/fnv"
	"strings"
	"time"

	"ratelimit-gateway/internal/model"
)

// Version identifies which content a request is judged against while a rollout
// is in flight.
type Version string

const (
	// VersionStable is the pre-change rule (the old version).
	VersionStable Version = "stable"
	// VersionCanary is the new, partially rolled-out rule.
	VersionCanary Version = "canary"
)

// MaxPercent is the inclusive upper bound of a rollout percent.
const MaxPercent = 100

// Subject builds the stable bucketing identity for a request under a rule:
// the values of every dimension the rule declares, in declared order. The
// length-prefixed join makes distinct value combinations unambiguous. When the
// rule declares no dimensions the client id is used, then a fixed marker, so a
// subject always exists.
//
// The base (stable) rule's dimensions anchor the subject for the whole life of
// a rollout — even when the canary content declares different dimensions — so a
// subject's bucket cannot move between the two versions.
func Subject(rule *model.Rule, rctx model.RequestContext) string {
	var b strings.Builder
	if len(rule.Dimensions) == 0 {
		b.WriteString("c=")
		b.WriteString(nonEmpty(rctx.ClientID))
		return b.String()
	}
	for i, dim := range rule.Dimensions {
		if i > 0 {
			b.WriteString("|")
		}
		b.WriteString(dim)
		b.WriteString("=")
		b.WriteString(nonEmpty(dimValue(rctx, dim)))
	}
	return b.String()
}

func nonEmpty(v string) string {
	if strings.TrimSpace(v) == "" {
		return "_"
	}
	return v
}

func dimValue(rctx model.RequestContext, dim string) string {
	switch dim {
	case model.DimClient:
		return rctx.ClientID
	case model.DimAPI:
		return rctx.APIPath
	case model.DimGroup:
		return rctx.Group
	}
	return ""
}

// Bucket maps a subject under a rule to one of 100 deterministic buckets in
// [0,99]. The rule id is part of the hash input so two rules rolling out at the
// same percent can still partition their subjects independently.
func Bucket(ruleID, subject string) uint32 {
	h := fnv.New64a()
	var b [8]byte
	writeStr := func(s string) {
		n := len(s)
		b[0] = byte(n)
		b[1] = byte(n >> 8)
		b[2] = byte(n >> 16)
		b[3] = byte(n >> 24)
		_, _ = h.Write(b[:4])
		_, _ = h.Write([]byte(s))
	}
	writeStr(ruleID)
	writeStr(subject)
	return uint32(h.Sum64() % 100)
}

// SelectVersion is the pure, reproducible assignment. Given the same rule id,
// subject and percent every instance returns the same version.
//
//	percent <= 0   -> everything stays on the old version
//	percent >= 100 -> the new version takes all traffic
//	otherwise      -> buckets [0,percent) go canary, the rest stay stable
//
// Since the canary set is the prefix [0,percent), increasing the percent only
// migrates more subjects onto the canary: a subject that was canary at 10% is
// still canary at 50%.
func SelectVersion(ruleID, subject string, percent int) Version {
	if percent <= 0 {
		return VersionStable
	}
	if percent >= MaxPercent {
		return VersionCanary
	}
	if Bucket(ruleID, subject) < uint32(percent) {
		return VersionCanary
	}
	return VersionStable
}

// Rollout is one in-flight gray change: the rule it belongs to, the current
// percent of traffic judged by the canary content, and both versions' content.
type Rollout struct {
	RuleID    string     `json:"rule_id"`
	Percent   int        `json:"percent"`
	Canary    model.Rule `json:"canary"`
	Old       model.Rule `json:"old"`
	CreatedAt time.Time  `json:"created_at"`
	UpdatedAt time.Time  `json:"updated_at"`
}

// Active reports whether any traffic is currently judged by the new version.
// A percent of 0 means the change is parked (fully reverted to old behavior)
// but its content is retained so the rollout can resume later.
func (r *Rollout) Active() bool { return r != nil && r.Percent > 0 }

// VersionFor resolves the version for a request under this rollout.
func (r *Rollout) VersionFor(rctx model.RequestContext) Version {
	return SelectVersion(r.RuleID, Subject(&r.Old, rctx), r.Percent)
}

// ValidatePercent rejects out-of-range percentages with an operator-facing
// reason.
func ValidatePercent(percent int) error {
	if percent < 0 || percent > MaxPercent {
		return fmt.Errorf("rollout percent must be an integer between 0 and 100, got %d", percent)
	}
	return nil
}

// Validate guards the rollout boundaries: a legal percent and a fully legal
// new rule. A bad canary must never enter judgement just because only part of
// the traffic sees it — all existing rule validation applies to the new
// content too.
func (r *Rollout) Validate() error {
	if strings.TrimSpace(r.RuleID) == "" {
		return fmt.Errorf("rollout must reference a rule")
	}
	if err := ValidatePercent(r.Percent); err != nil {
		return err
	}
	if r.Canary.ID != "" && r.Canary.ID != r.RuleID {
		return fmt.Errorf("canary rule id %q does not match rollout rule %q", r.Canary.ID, r.RuleID)
	}
	if err := r.Canary.Validate(); err != nil {
		return fmt.Errorf("new (canary) rule is invalid: %w", err)
	}
	if err := r.Old.Validate(); err != nil {
		return fmt.Errorf("old (stable) rule snapshot is invalid: %w", err)
	}
	return nil
}
