package quota_test

import (
	"context"
	"testing"

	"ratelimit-gateway/internal/model"
)

func fourLevelRule(algo model.Algorithm, global, group, api, client int64, win int64) *model.Rule {
	levels := map[model.Level]model.RuleLevel{}
	add := func(lv model.Level, v int64) {
		if algo == model.AlgoTokenBucket || algo == model.AlgoLeakyBucket {
			levels[lv] = model.RuleLevel{Threshold: 1000, Burst: v}
		} else {
			levels[lv] = model.RuleLevel{Threshold: v, WindowSeconds: win}
		}
	}
	add(model.LevelGlobal, global)
	add(model.LevelGroup, group)
	add(model.LevelAPI, api)
	add(model.LevelClient, client)
	return &model.Rule{
		Name: "four-level", Enabled: true, Algorithm: algo,
		Dimensions: []string{model.DimClient, model.DimAPI, model.DimGroup},
		Levels:     levels,
	}
}

// TestFourLevelCascade: whichever of the four coexisting quotas is tightest
// is the one that stops the request, and the verdict names that exact level.
func TestFourLevelCascade(t *testing.T) {
	cases := []struct {
		name     string
		global   int64
		group    int64
		api      int64
		client   int64
		stopAt   model.Level
		requests int // number of requests to fire before expecting a deny
	}{
		{"global_tightest", 2, 100, 100, 100, model.LevelGlobal, 2},
		{"group_tightest", 100, 2, 100, 100, model.LevelGroup, 2},
		{"api_tightest", 100, 100, 2, 100, model.LevelAPI, 2},
		{"client_tightest", 100, 100, 100, 2, model.LevelClient, 2},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rules, chk, _ := newChecker(t)
			ctx := context.Background()
			if _, err := rules.Create(ctx, fourLevelRule(model.AlgoFixedWindow,
				tc.global, tc.group, tc.api, tc.client, 5)); err != nil {
				t.Fatal(err)
			}
			rc := model.RequestContext{ClientID: "c1", APIPath: "/p", Group: "g1"}
			for i := 0; i < tc.requests; i++ {
				v, err := chk.Decide(ctx, rc)
				if err != nil {
					t.Fatal(err)
				}
				if !v.Allowed {
					t.Fatalf("request %d unexpectedly denied", i+1)
				}
			}
			v, _ := chk.Decide(ctx, rc)
			if v.Allowed {
				t.Fatalf("request %d should be denied by %s", tc.requests+1, tc.stopAt)
			}
			if v.Level != tc.stopAt {
				t.Fatalf("deny level = %s, want %s", v.Level, tc.stopAt)
			}
			if v.RuleID == "" || v.Reason == "" {
				t.Fatal("verdict must name the rule and explain the reason")
			}
		})
	}
}

// TestCascadeDistinctGroups: the global quota is shared by everyone; once the
// global level is exhausted even a brand-new client with full client quota is
// rejected at the global level.
func TestCascadeGlobalShared(t *testing.T) {
	rules, chk, _ := newChecker(t)
	ctx := context.Background()
	if _, err := rules.Create(ctx, fourLevelRule(model.AlgoFixedWindow, 2, 100, 100, 100, 5)); err != nil {
		t.Fatal(err)
	}
	chk.Decide(ctx, model.RequestContext{ClientID: "c1", APIPath: "/p", Group: "g1"})
	chk.Decide(ctx, model.RequestContext{ClientID: "c2", APIPath: "/p", Group: "g2"})
	// fresh client c3, but global is already exhausted
	v, _ := chk.Decide(ctx, model.RequestContext{ClientID: "c3", APIPath: "/p", Group: "g3"})
	if v.Allowed || v.Level != model.LevelGlobal {
		t.Fatalf("fresh client must still be stopped at global level, got allowed=%v level=%s", v.Allowed, v.Level)
	}
}
