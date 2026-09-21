package engine_test

import (
	"context"
	"testing"
	"time"

	"ratelimit-gateway/internal/engine"
	"ratelimit-gateway/internal/model"
	"ratelimit-gateway/internal/testboot"
)

// drain admits exactly n requests against a key (fresh bucket assumed).
func drain(t *testing.T, mgr *engine.Manager, algo model.Algorithm, key string, cfg model.RuleLevel, n int) {
	t.Helper()
	ctx := context.Background()
	for i := 0; i < n; i++ {
		d, err := mgr.Try(ctx, algo, key, cfg)
		if err != nil {
			t.Fatal(err)
		}
		if !d.Allowed {
			t.Fatalf("drain request %d/%d unexpectedly rejected", i+1, n)
		}
	}
}

// admitN fires n requests as fast as possible and returns how many passed.
func admitN(t *testing.T, mgr *engine.Manager, algo model.Algorithm, key string, cfg model.RuleLevel, n int) int {
	t.Helper()
	ctx := context.Background()
	passed := 0
	for i := 0; i < n; i++ {
		d, err := mgr.Try(ctx, algo, key, cfg)
		if err != nil {
			t.Fatal(err)
		}
		if d.Allowed {
			passed++
		}
	}
	return passed
}

// TestTokenBucketBurst: a fresh bucket holds the full capacity, so an
// instantaneous burst of `capacity` is admitted; requests beyond capacity are
// rejected until the constant refill earns another token.
func TestTokenBucketBurst(t *testing.T) {
	_, mgr := testboot.RedisOnly(t)
	ctx := context.Background()
	key := "tb:burst"
	cfg := model.RuleLevel{Threshold: 10, Burst: 5} // 10 tokens/s, capacity 5

	for i := 0; i < 5; i++ {
		d, err := mgr.Try(ctx, model.AlgoTokenBucket, key, cfg)
		if err != nil {
			t.Fatal(err)
		}
		if !d.Allowed {
			t.Fatalf("request %d in burst should be admitted (capacity 5)", i+1)
		}
	}
	if d, _ := mgr.Try(ctx, model.AlgoTokenBucket, key, cfg); d.Allowed {
		t.Fatalf("request beyond bucket capacity must be rejected")
	}

	// rate 10/s => after ~120ms one token is back
	time.Sleep(120 * time.Millisecond)
	if d, _ := mgr.Try(ctx, model.AlgoTokenBucket, key, cfg); !d.Allowed {
		t.Fatalf("after refill delay one token should be available")
	}
}

// TestTokenBucketRefillRate verifies the constant refill capped at capacity.
func TestTokenBucketRefillRate(t *testing.T) {
	_, mgr := testboot.RedisOnly(t)
	key := "tb:rate"
	cfg := model.RuleLevel{Threshold: 5, Burst: 5}
	drain(t, mgr, model.AlgoTokenBucket, key, cfg, 5)

	time.Sleep(1050 * time.Millisecond)
	admitted := admitN(t, mgr, model.AlgoTokenBucket, key, cfg, 6)
	if admitted != 5 {
		t.Fatalf("after 1s at rate 5/s expect 5 refilled (capped at cap), got %d", admitted)
	}
}
