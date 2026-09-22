package canary_test

import (
	"strings"
	"testing"

	"ratelimit-gateway/internal/canary"
	"ratelimit-gateway/internal/model"
)

func testRule() *model.Rule {
	return &model.Rule{
		ID: "rule-1", Name: "r", Enabled: true,
		Algorithm:  model.AlgoTokenBucket,
		Dimensions: []string{model.DimClient, model.DimAPI},
		Levels: map[model.Level]model.RuleLevel{
			model.LevelClient: {Threshold: 10, Burst: 5},
		},
	}
}

func rc(client, api string) model.RequestContext {
	return model.RequestContext{ClientID: client, APIPath: api}
}

// TestSubjectDeterministic: the same rule + request yields the same canonical
// subject string on every call.
func TestSubjectDeterministic(t *testing.T) {
	r := testRule()
	a := canary.Subject(r, rc("c1", "/p"))
	b := canary.Subject(r, rc("c1", "/p"))
	if a != b {
		t.Fatalf("subject not stable: %q vs %q", a, b)
	}
	// different dimension values -> different subjects
	if canary.Subject(r, rc("c1", "/p")) == canary.Subject(r, rc("c1", "/q")) {
		t.Fatal("distinct api values must produce distinct subjects")
	}
	if canary.Subject(r, rc("c1", "/p")) == canary.Subject(r, rc("c2", "/p")) {
		t.Fatal("distinct clients must produce distinct subjects")
	}
}

// TestBucketInRange: every bucket is in [0,99].
func TestBucketInRange(t *testing.T) {
	for i := 0; i < 5000; i++ {
		s := "client-" + itoa(i%26) + "-" + itoa(i)
		b := canary.Bucket("rule-x", s)
		if b >= 100 {
			t.Fatalf("bucket out of range: %d", b)
		}
	}
}

// TestStableWhilePercentFixed: while the percent does not change a subject
// stays on one side, call after call — the exact "not new then old" guarantee.
func TestStableWhilePercentFixed(t *testing.T) {
	r := testRule()
	first := map[string]canary.Version{}
	for i := 0; i < 2000; i++ {
		subj := canary.Subject(r, rc("c"+itoa(i), "/orders"))
		v := canary.SelectVersion(r.ID, subj, 37)
		if prev, ok := first[subj]; ok && prev != v {
			t.Fatalf("subject %s flipped versions at fixed percent", subj)
		}
		first[subj] = v
	}
}

// TestMonotonicExpansion: raising the percent never removes a subject from the
// canary side; the set at 10 is a strict subset of that at 50 which is a
// subset of 100. No reshuffling.
func TestMonotonicExpansion(t *testing.T) {
	r := testRule()
	canaryAt := func(pct int) map[string]bool {
		set := map[string]bool{}
		for i := 0; i < 3000; i++ {
			subj := canary.Subject(r, rc("c"+itoa(i), "/orders"))
			if canary.SelectVersion(r.ID, subj, pct) == canary.VersionCanary {
				set[subj] = true
			}
		}
		return set
	}
	s10, s50, s100 := canaryAt(10), canaryAt(50), canaryAt(100)
	for subj := range s10 {
		if !s50[subj] {
			t.Fatalf("subject %s was canary at 10%% but dropped at 50%%", subj)
		}
		if !s100[subj] {
			t.Fatalf("subject %s was canary at 10%% but dropped at 100%%", subj)
		}
	}
	for subj := range s50 {
		if !s100[subj] {
			t.Fatalf("subject %s was canary at 50%% but dropped at 100%%", subj)
		}
	}
	// counts should be (approximately) proportional and strictly increasing
	if len(s10) == 0 || len(s50) <= len(s10) || len(s100) != 3000 {
		t.Fatalf("expected monotonic growth, got %d/%d/%d", len(s10), len(s50), len(s100))
	}
}

// TestZeroAndHundred: 0% puts everyone on stable, 100% everyone on canary.
func TestZeroAndHundred(t *testing.T) {
	r := testRule()
	for i := 0; i < 200; i++ {
		subj := canary.Subject(r, rc("c"+itoa(i), "/p"))
		if canary.SelectVersion(r.ID, subj, 0) != canary.VersionStable {
			t.Fatal("0% must keep every subject on stable")
		}
		if canary.SelectVersion(r.ID, subj, -5) != canary.VersionStable {
			t.Fatal("negative percent must clamp to all stable")
		}
		if canary.SelectVersion(r.ID, subj, 100) != canary.VersionCanary {
			t.Fatal("100% must put every subject on canary")
		}
		if canary.SelectVersion(r.ID, subj, 1000) != canary.VersionCanary {
			t.Fatal(">100 must clamp to all canary")
		}
	}
}

// TestCrossInstanceReproducible: independent "decision paths" (plain function
// calls with no shared state) always agree for the same (rule, subject, pct).
func TestCrossInstanceReproducible(t *testing.T) {
	const ruleID, subj = "rule-abc", "client_id=42|api_path=/x"
	for _, pct := range []int{1, 10, 33, 50, 99} {
		got := canary.SelectVersion(ruleID, subj, pct)
		for rep := 0; rep < 50; rep++ {
			if canary.SelectVersion(ruleID, subj, pct) != got {
				t.Fatalf("independent path disagreed at pct %d", pct)
			}
			if canary.Bucket(ruleID, subj) != canary.Bucket(ruleID, subj) {
				t.Fatal("bucket not reproducible")
			}
		}
	}
}

// TestRuleIDIsolatesPartitions: the same subject under two rules can differ,
// proving rule id is part of the hash (independent rollouts).
func TestRuleIDIsolatesPartitions(t *testing.T) {
	subj := "client_id=c1"
	diff := false
	for _, id := range []string{"a", "b", "c", "d", "e"} {
		for _, id2 := range []string{"a", "b", "c", "d", "e"} {
			if canary.SelectVersion(id, subj, 50) != canary.SelectVersion(id2, subj, 50) {
				diff = true
			}
		}
	}
	if !diff {
		t.Fatal("expected rule id to influence bucketing across rules")
	}
}

// TestValidatePercentBounds: only integers 0..100 accepted.
func TestValidatePercentBounds(t *testing.T) {
	for _, p := range []int{0, 1, 50, 99, 100} {
		if err := canary.ValidatePercent(p); err != nil {
			t.Fatalf("percent %d should be valid: %v", p, err)
		}
	}
	for _, p := range []int{-1, -100, 101, 1000} {
		err := canary.ValidatePercent(p)
		if err == nil || !strings.Contains(err.Error(), "0 and 100") {
			t.Fatalf("percent %d must be rejected with a range reason, got %v", p, err)
		}
	}
}

// TestRolloutValidateRejectsBadCanary: a malformed new rule is rejected even
// though only part of the traffic would see it — existing validation applies.
func TestRolloutValidateRejectsBadCanary(t *testing.T) {
	old := testRule()
	badCanary := *old
	badCanary.Algorithm = "magic"
	ro := &canary.Rollout{RuleID: old.ID, Percent: 10, Canary: badCanary, Old: *old}
	if err := ro.Validate(); err == nil ||
		!strings.Contains(strings.ToLower(err.Error()), "canary") {
		t.Fatalf("bad canary must be rejected mentioning canary, got %v", err)
	}

	// bad percent rejected
	ro2 := &canary.Rollout{RuleID: old.ID, Percent: 150, Canary: *old, Old: *old}
	if err := ro2.Validate(); err == nil {
		t.Fatal("percent 150 must be rejected")
	}
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	neg := i < 0
	if neg {
		i = -i
	}
	var b [12]byte
	pos := len(b)
	for i > 0 {
		pos--
		b[pos] = byte('0' + i%10)
		i /= 10
	}
	if neg {
		pos--
		b[pos] = '-'
	}
	return string(b[pos:])
}
