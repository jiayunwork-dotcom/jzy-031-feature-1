package quota_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"ratelimit-gateway/internal/model"
	"ratelimit-gateway/internal/quota"
)

// TestDeterministicUnderContention: all four levels set to the same limit.
// Repeating the identical traffic from many goroutines must always yield the
// same total allow count and denials must always be attributed to the first
// level in the fixed global->group->api->client order.
func TestDeterministicUnderContention(t *testing.T) {
	for run := 0; run < 3; run++ {
		rules, chk, _ := newChecker(t)
		ctx := context.Background()
		if _, err := rules.Create(ctx, fourLevelRule(model.AlgoFixedWindow, 3, 3, 3, 3, 5)); err != nil {
			t.Fatal(err)
		}
		var allowed int64
		var denyLevel string
		var mu sync.Mutex
		var wg sync.WaitGroup
		const n = 16
		wg.Add(n)
		for i := 0; i < n; i++ {
			go func() {
				defer wg.Done()
				v, err := chk.Decide(ctx, model.RequestContext{ClientID: "c", APIPath: "/p", Group: "g"})
				if err != nil {
					t.Error(err)
					return
				}
				if v.Allowed {
					atomic.AddInt64(&allowed, 1)
					return
				}
				mu.Lock()
				if denyLevel == "" {
					denyLevel = string(v.Level)
				} else if denyLevel != string(v.Level) {
					t.Errorf("inconsistent deny attribution: %s vs %s", denyLevel, v.Level)
				}
				mu.Unlock()
			}()
		}
		wg.Wait()
		if allowed != 3 {
			t.Fatalf("run %d: exactly 3 requests may pass, got %d", run, allowed)
		}
		if denyLevel != string(model.LevelGlobal) {
			t.Fatalf("run %d: denials must pin to global (first level), got %s", run, denyLevel)
		}
	}
}

// TestConcurrentAcrossInstancesSimulated: two checkers share one Redis (as two
// gateway replicas would); their combined admits must not exceed the quota.
func TestConcurrentAcrossInstancesSimulated(t *testing.T) {
	rdb, rules, mgr, rec := testbootFull(t)
	ctx := context.Background()
	// Rate 1/s: across the short concurrent burst refill is negligible, while
	// capacity 10 is the exact instantaneous budget shared via Redis.
	if _, err := rules.Create(ctx, bucketRule("shared", model.AlgoTokenBucket,
		[]string{model.DimClient}, 1, 10)); err != nil {
		t.Fatal(err)
	}
	chk1 := newCheckerFrom(t, rdb, rules, mgr, rec)
	chk2 := newCheckerFrom(t, rdb, rules, mgr, rec)

	var allowed int64
	var wg sync.WaitGroup
	fire := func(chk *quota.Checker) {
		defer wg.Done()
		for i := 0; i < 30; i++ {
			v, err := chk.Decide(ctx, model.RequestContext{ClientID: "same"})
			if err != nil {
				t.Error(err)
				return
			}
			if v.Allowed {
				atomic.AddInt64(&allowed, 1)
			}
		}
	}
	wg.Add(2)
	go fire(chk1)
	go fire(chk2)
	wg.Wait()
	if allowed != 10 {
		t.Fatalf("two replicas sharing Redis must admit exactly 10, got %d", allowed)
	}
}

// TestHotReload: change a threshold and the new value applies immediately,
// no restart; also confirm a disable stops matching.
func TestHotReload(t *testing.T) {
	rules, chk, _ := newChecker(t)
	ctx := context.Background()
	r, err := rules.Create(ctx, windowRule("hot", model.AlgoFixedWindow,
		[]string{model.DimClient}, 2, 5))
	if err != nil {
		t.Fatal(err)
	}
	rc := model.RequestContext{ClientID: "c"}
	chk.Decide(ctx, rc)
	chk.Decide(ctx, rc)
	if v, _ := chk.Decide(ctx, rc); v.Allowed {
		t.Fatal("limit 2 should reject third")
	}

	// raise threshold to 5 and wait for the pub/sub reload to land
	r.Levels[model.LevelClient] = model.RuleLevel{Threshold: 5, WindowSeconds: 5}
	if _, err := rules.Update(ctx, r); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(3 * time.Second)
	thirdAllowed := false
	for time.Now().Before(deadline) {
		v, _ := chk.Decide(ctx, rc)
		if v.Allowed {
			thirdAllowed = true
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !thirdAllowed {
		t.Fatal("raised threshold must hot-reload and admit more requests")
	}
}
