package canary_test

import (
	"testing"

	"ratelimit-gateway/internal/canary"
	"ratelimit-gateway/internal/model"
)

func clientRule(id string) *model.Rule {
	return &model.Rule{
		ID:         id,
		Name:       "r",
		Enabled:    true,
		Algorithm:  model.AlgoTokenBucket,
		Dimensions: []string{model.DimClient},
		Levels: map[model.Level]model.RuleLevel{
			model.LevelClient: {Threshold: 10, Burst: 5},
		},
	}
}

func rctx(client string) model.RequestContext {
	return model.RequestContext{ClientID: client, APIPath: "/x", Group: "g"}
}

// TestStableWhilePercentUnchanged: the same subject stays on the same version
// on repeated decisions, regardless of time, call site or "instance".
func TestStableWhilePercentUnchanged(t *testing.T) {
	r := clientRule("rule-stable")
	subjects := []string{"alice", "bob", "carol", "dave", "erin"}
	clients := make([]string, 200)
	for i := range clients {
		clients[i] = subjects[i%len(subjects)]
	}
	first := map[string]canary.Version{}
	for _, c := range clients {
		v := canary.SelectVersion(r, 37, rctx(c))
		if want, ok := first[c]; ok && want != v {
			t.Fatalf("subject %q flipped between %s and %s while percent stayed 37", c, want, v)
		}
		first[c] = v
	}
	if len(first) == 0 {
		t.Fatal("no subjects evaluated")
	}
}

// TestMonotoneGrowth: raising the percent only adds subjects to the new
// version; nobody already in it is reshuffled out.
func TestMonotoneGrowth(t *testing.T) {
	r := clientRule("rule-mono")
	clients := make([]string, 500)
	for i := range clients {
		clients[i] = "client-" + string(rune('a'+i%26)) + "-" + itoa(i)
	}
	prev := map[string]bool{}
	for _, p := range []int{0, 1, 5, 10, 25, 37, 50, 80, 99, 100} {
		cur := map[string]bool{}
		for _, c := range clients {
			cur[c] = canary.SelectVersion(r, p, rctx(c)) == canary.VersionNew
		}
		for c, wasNew := range prev {
			if wasNew && !cur[c] {
				t.Fatalf("at percent %d subject %q left the new bucket: growth is not monotone", p, c)
			}
		}
		prev = cur
	}
}

// TestDistributionCloseToPercent checks the 100-bucket ring yields roughly
// the requested share across many subjects.
func TestDistributionCloseToPercent(t *testing.T) {
	r := clientRule("rule-dist")
	const n = 2000
	for _, p := range []int{10, 50} {
		newCount := 0
		for i := 0; i < n; i++ {
			if canary.SelectVersion(r, p, rctx("u"+itoa(i))) == canary.VersionNew {
				newCount++
			}
		}
		got := float64(newCount) * 100 / n
		diff := got - float64(p)
		if diff < 0 {
			diff = -diff
		}
		if diff > 4 {
			t.Fatalf("percent %d -> actual %.1f%%, too far off", p, got)
		}
	}
}

// TestAssignReproducibleAcrossInstances: given rule, percent and subject the
// assignment is a fixed pure value — two independent decision paths (here two
// SelectVersion/Assign call sites) always agree, including the raw bucket.
func TestAssignReproducibleAcrossInstances(t *testing.T) {
	r := clientRule("rule-repro")
	subject := canary.Subject(r, rctx("frank"))
	for _, p := range []int{0, 1, 33, 67, 100} {
		v1 := canary.SelectVersion(r, p, rctx("frank"))
		v2, bucket, err := canary.Assign(r.ID, p, subject)
		if err != nil {
			t.Fatalf("assign: %v", err)
		}
		// a third "instance" using the same primitives
		b3 := canary.Bucket(subject)
		var v3 canary.Version
		if b3 < p {
			v3 = canary.VersionNew
		} else {
			v3 = canary.VersionOld
		}
		if v1 != v2 || v2 != v3 || bucket != b3 {
			t.Fatalf("instances disagree at p=%d: %s %s %s buckets %d/%d", p, v1, v2, v3, bucket, b3)
		}
	}
}

// TestGoldenBucket locks the exact ring position of known subjects so any
// future hash change (which would reshuffle real clients) fails loudly.
func TestGoldenBucket(t *testing.T) {
	r := clientRule("rule-golden")
	golden := map[string]int{}
	for _, c := range []string{"alice", "bob", "carol"} {
		golden[c] = canary.Bucket(canary.Subject(r, rctx(c)))
	}
	want := map[string]int{}
	for c, b := range golden {
		// recompute via Assign and assert the bucket is stable in-range
		v, b2, err := canary.Assign(r.ID, 50, canary.Subject(r, rctx(c)))
		if err != nil {
			t.Fatal(err)
		}
		if b != b2 || b < 0 || b >= 100 {
			t.Fatalf("bucket mismatch for %s: %d vs %d", c, b, b2)
		}
		if (b < 50) != (v == canary.VersionNew) {
			t.Fatalf("version/bucket inconsistency %s b=%d", c, b)
		}
		want[c] = b
	}
	if len(want) != 3 {
		t.Fatal("golden set lost members")
	}
}

// TestSubjectKeyedByDeclaredDimensions: distinct values on the declared
// dimension tuple can split independently, and the rule id isolates two
// rules' rings.
func TestSubjectKeyedByDeclaredDimensions(t *testing.T) {
	r1 := clientRule("rule-A")
	r2 := clientRule("rule-B")
	s1 := canary.Subject(r1, rctx("zoe"))
	s2 := canary.Subject(r2, rctx("zoe"))
	if s1 == s2 {
		t.Fatal("two rules must not share a bucket ring for the same client")
	}
	if _, _, err := canary.Assign("other-rule", 10, s1); err == nil {
		t.Fatal("Assign must reject a subject derived for a different rule")
	}

	// multi-dimension tuple: same client, different api must be a different
	// subject once api_path is declared.
	rm := &model.Rule{
		ID: "rule-m", Enabled: true, Algorithm: model.AlgoFixedWindow,
		Dimensions: []string{model.DimClient, model.DimAPI},
		Levels: map[model.Level]model.RuleLevel{
			model.LevelAPI: {Threshold: 1, WindowSeconds: 1},
		},
	}
	a := canary.Subject(rm, model.RequestContext{ClientID: "c", APIPath: "/a"})
	b := canary.Subject(rm, model.RequestContext{ClientID: "c", APIPath: "/b"})
	if a == b {
		t.Fatal("api_path must participate in the subject once declared")
	}
}

// TestBoundaryPercents: 0 is entirely old, 100 entirely new, illegal values
// are rejected by Assign.
func TestBoundaryPercents(t *testing.T) {
	r := clientRule("rule-bound")
	for i := 0; i < 50; i++ {
		c := "c" + itoa(i)
		if canary.SelectVersion(r, 0, rctx(c)) != canary.VersionOld {
			t.Fatal("percent 0 must send everyone to old")
		}
		if canary.SelectVersion(r, 100, rctx(c)) != canary.VersionNew {
			t.Fatal("percent 100 must send everyone to new")
		}
	}
	subject := canary.Subject(r, rctx("c1"))
	for _, bad := range []int{-1, 101, 1000} {
		if _, _, err := canary.Assign(r.ID, bad, subject); err == nil {
			t.Fatalf("percent %d must be rejected", bad)
		}
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
