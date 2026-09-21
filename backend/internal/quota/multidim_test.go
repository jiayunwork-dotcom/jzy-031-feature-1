package quota_test

import (
	"context"
	"testing"

	"ratelimit-gateway/internal/model"
)

func bucketRule(name string, algo model.Algorithm, dims []string, threshold, burst int64) *model.Rule {
	return &model.Rule{
		Name: name, Enabled: true, Algorithm: algo, Dimensions: dims,
		Levels: map[model.Level]model.RuleLevel{
			model.LevelClient: {Threshold: threshold, Burst: burst},
		},
	}
}

func windowRule(name string, algo model.Algorithm, dims []string, limit, win int64) *model.Rule {
	return &model.Rule{
		Name: name, Enabled: true, Algorithm: algo, Dimensions: dims,
		Levels: map[model.Level]model.RuleLevel{
			model.LevelClient: {Threshold: limit, WindowSeconds: win},
		},
	}
}

// TestMultiDimANDIsolation: a rule keyed on (client_id, api_path) counts each
// AND-combination independently. Client A exhausting /orders must not affect
// client A on /pay, nor client B on /orders.
func TestMultiDimANDIsolation(t *testing.T) {
	rules, chk, _ := newChecker(t)
	ctx := context.Background()
	r, err := rules.Create(ctx, bucketRule("combo", model.AlgoTokenBucket,
		[]string{model.DimClient, model.DimAPI}, 100, 2))
	if err != nil {
		t.Fatal(err)
	}
	_ = r

	Aorders := model.RequestContext{ClientID: "A", APIPath: "/orders"}
	Apay := model.RequestContext{ClientID: "A", APIPath: "/pay"}
	Borders := model.RequestContext{ClientID: "B", APIPath: "/orders"}

	// exhaust A on /orders
	if v, _ := chk.Decide(ctx, Aorders); !v.Allowed {
		t.Fatal("A /orders 1")
	}
	if v, _ := chk.Decide(ctx, Aorders); !v.Allowed {
		t.Fatal("A /orders 2")
	}
	if v, _ := chk.Decide(ctx, Aorders); v.Allowed {
		t.Fatal("A /orders 3 must be rejected")
	}

	// independent counters: other combinations untouched
	if v, _ := chk.Decide(ctx, Apay); !v.Allowed {
		t.Fatal("A /pay must use an independent counter")
	}
	if v, _ := chk.Decide(ctx, Borders); !v.Allowed {
		t.Fatal("B /orders must use an independent counter")
	}
}

// TestMultiDimMissingValueExcluded: a request lacking a dimension the rule
// declares does not match and is not counted against it.
func TestMultiDimMissingValueExcluded(t *testing.T) {
	rules, chk, _ := newChecker(t)
	ctx := context.Background()
	if _, err := rules.Create(ctx, windowRule("needs-group", model.AlgoFixedWindow,
		[]string{model.DimGroup, model.DimClient}, 1, 5)); err != nil {
		t.Fatal(err)
	}
	// no group => rule does not apply
	v, err := chk.Decide(ctx, model.RequestContext{ClientID: "A", APIPath: "/"})
	if err != nil {
		t.Fatal(err)
	}
	if !v.Allowed {
		t.Fatal("request without group must not be matched by a group-keyed rule")
	}
	// with group, the rule applies
	v, _ = chk.Decide(ctx, model.RequestContext{ClientID: "A", Group: "g1"})
	if !v.Allowed {
		t.Fatal("first grouped request allowed")
	}
	v, _ = chk.Decide(ctx, model.RequestContext{ClientID: "A", Group: "g1"})
	if v.Allowed {
		t.Fatal("second grouped request exceeds limit 1")
	}
}

// TestMatcherValueAllowList: a matcher restricts which dimension values count.
func TestMatcherValueAllowList(t *testing.T) {
	rules, chk, _ := newChecker(t)
	ctx := context.Background()
	r := windowRule("vip-only", model.AlgoFixedWindow, []string{model.DimClient}, 100, 5)
	r.Matchers = map[string][]string{model.DimClient: {"vip"}}
	if _, err := rules.Create(ctx, r); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		v, _ := chk.Decide(ctx, model.RequestContext{ClientID: "normal"})
		if !v.Allowed {
			t.Fatal("non-vip traffic must not match vip-only rule")
		}
	}
	// vip gets the rule (limit 100, so still allowed here)
	v, _ := chk.Decide(ctx, model.RequestContext{ClientID: "vip"})
	if !v.Allowed {
		t.Fatal("vip must match the rule")
	}
}
