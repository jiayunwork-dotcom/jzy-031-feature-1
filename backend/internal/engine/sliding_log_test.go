package engine_test

import (
	"context"
	"testing"
	"time"

	"ratelimit-gateway/internal/model"
	"ratelimit-gateway/internal/testboot"
)

// TestSlidingLogPreciseEviction: only timestamps strictly inside the trailing
// window count. After exactly the window length the recorded hits are evicted
// and the budget frees up fully.
func TestSlidingLogPreciseEviction(t *testing.T) {
	_, mgr := testboot.RedisOnly(t)
	ctx := context.Background()
	key := "sl:evict"
	cfg := model.RuleLevel{Threshold: 2, WindowSeconds: 1}

	if d, _ := mgr.Try(ctx, model.AlgoSlidingLog, key, cfg); !d.Allowed {
		t.Fatal("hit 1")
	}
	if d, _ := mgr.Try(ctx, model.AlgoSlidingLog, key, cfg); !d.Allowed {
		t.Fatal("hit 2")
	}
	if d, _ := mgr.Try(ctx, model.AlgoSlidingLog, key, cfg); d.Allowed {
		t.Fatal("third hit within 1s window must be rejected")
	}

	// Wait beyond the window: both records evict precisely.
	time.Sleep(1080 * time.Millisecond)
	if d, _ := mgr.Try(ctx, model.AlgoSlidingLog, key, cfg); !d.Allowed {
		t.Fatalf("after the window elapses the log must be empty, remaining=%d", d.Remaining)
	}
	if d, _ := mgr.Try(ctx, model.AlgoSlidingLog, key, cfg); !d.Allowed {
		t.Fatal("second hit in fresh window should pass")
	}
}

// TestSlidingLogGradualEviction: a request admitted long ago evicts before a
// recent one, freeing exactly one slot.
func TestSlidingLogGradualEviction(t *testing.T) {
	_, mgr := testboot.RedisOnly(t)
	ctx := context.Background()
	key := "sl:grad"
	cfg := model.RuleLevel{Threshold: 2, WindowSeconds: 1}

	if d, _ := mgr.Try(ctx, model.AlgoSlidingLog, key, cfg); !d.Allowed {
		t.Fatal("first")
	}
	time.Sleep(550 * time.Millisecond)
	if d, _ := mgr.Try(ctx, model.AlgoSlidingLog, key, cfg); !d.Allowed {
		t.Fatal("second")
	}
	if d, _ := mgr.Try(ctx, model.AlgoSlidingLog, key, cfg); d.Allowed {
		t.Fatal("window holds both hits, third must reject")
	}
	// At ~1050ms the first hit (t=0) has aged out; the second (t=550) stays.
	time.Sleep(550 * time.Millisecond)
	if d, _ := mgr.Try(ctx, model.AlgoSlidingLog, key, cfg); !d.Allowed {
		t.Fatal("after oldest evicts exactly one slot frees")
	}
	if d, _ := mgr.Try(ctx, model.AlgoSlidingLog, key, cfg); d.Allowed {
		t.Fatal("second slot still occupied by the t=550 hit")
	}
}
