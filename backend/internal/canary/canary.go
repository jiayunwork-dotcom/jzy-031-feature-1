// Package canary is the deterministic traffic-splitting layer for gray
// releases. It contains no I/O and no per-request randomness: the verdict
// "new version or old version" is a pure function of (rule ID, rollout
// percentage, judging subject). Every gateway instance — and every moment in
// time — therefore computes the same assignment.
//
// The assignment is also monotone when the percentage grows. Subjects are
// hashed onto the fixed 100-point bucket ring 0..99 and the new version owns
// the lowest `percent` buckets. Raising 10 -> 50 only admits buckets 10..49;
// nobody already in the new bucket is reshuffled out.
package canary

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"sort"
	"strings"

	"ratelimit-gateway/internal/model"
)

// Version is which rule content a request is evaluated against.
type Version string

const (
	VersionOld Version = "old"
	VersionNew Version = "new"
)

// bucketCount is the fixed granularity of the split: percentages are integer
// points on a 100-bucket ring.
const bucketCount = 100

// SelectVersion assigns one request to a version.
//
// Callers pass the stable (old) rule and the live rollout. The subject is
// derived from the dimensions the old rule declares so the split is keyed on
// exactly the values the rule is attributed by (e.g. client_id, or the
// client_id+api_path combination). A rule that declares no dimensions falls
// back to the request's client identity, and finally to a global subject.
func SelectVersion(rule *model.Rule, percent int, rctx model.RequestContext) Version {
	if percent <= 0 {
		return VersionOld
	}
	if percent >= 100 {
		return VersionNew
	}
	b := Bucket(Subject(rule, rctx))
	if b < percent {
		return VersionNew
	}
	return VersionOld
}

// Subject renders the judging subject as the ordered tuple of dimension
// values declared by the rule. It is exported so callers (tests, the
// dashboard's "which side am I on" view) can reproduce the exact key.
func Subject(rule *model.Rule, rctx model.RequestContext) string {
	dims := make([]string, len(rule.Dimensions))
	copy(dims, rule.Dimensions)
	sort.Strings(dims)

	var b strings.Builder
	b.WriteString("rule=")
	b.WriteString(rule.ID)
	b.WriteString("|")
	first := true
	for _, d := range dims {
		v := dimValue(rctx, d)
		if !first {
			b.WriteString("|")
		}
		first = false
		b.WriteString(d)
		b.WriteString("=")
		b.WriteString(v)
	}
	// A rule that declares no dimensions has no per-request tuple at all;
	// attribute by client id so different clients can still split, and fall
	// back to one global subject otherwise.
	if len(dims) == 0 {
		if rctx.ClientID != "" {
			b.WriteString("client_id=")
			b.WriteString(rctx.ClientID)
		} else {
			b.WriteString("_global")
		}
	}
	return b.String()
}

// Bucket maps a subject onto the fixed 0..99 ring. SHA-256 gives a uniform,
// stable distribution with no host-specific behavior (unlike Go's map hash)
// and the same input always yields the same bucket on every instance.
func Bucket(subject string) int {
	sum := sha256.Sum256([]byte(subject))
	// big-endian first 8 bytes keeps the result architecture-independent.
	h := binary.BigEndian.Uint64(sum[:8])
	return int(h % bucketCount)
}

// Assign is the fully explicit form used by tests and tooling: given a rule,
// a percentage and a subject string, report the side and the raw bucket.
func Assign(ruleID string, percent int, subject string) (Version, int, error) {
	if err := model.ValidatePercent(percent); err != nil {
		return VersionOld, 0, err
	}
	if !strings.HasPrefix(subject, "rule="+ruleID+"|") {
		// Guard against accidentally passing a subject computed for another
		// rule: the rule id is what isolates two rules' bucket rings.
		return VersionOld, 0, fmt.Errorf("subject %q was not derived for rule %q", subject, ruleID)
	}
	b := Bucket(subject)
	if b < percent {
		return VersionNew, b, nil
	}
	return VersionOld, b, nil
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
