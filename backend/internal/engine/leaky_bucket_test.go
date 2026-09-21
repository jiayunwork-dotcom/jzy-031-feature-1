package engine_test

import (
	"context"
	"testing"

	"ratelimit-gateway/internal/model"
	"ratelimit-gateway/internal/testboot"
)

// TestLeakyBucketBurstCappedAndPaced: the queue accepts at most capacity at
// once, rejects the overflow, and spreads the accepted requests' releases at
// the constant leak rate. Each accepted request carries its FIFO release
// delay; consecutive releases are evenly spaced.
func TestLeakyBucketBurstCappedAndPaced(t *testing.T) {
	_, mgr := testboot.RedisOnly(t)
	ctx := context.Background()
	key := "lb:shape"
	cfg := model.RuleLevel{Threshold: 10, Burst: 3} // leaks 10/s, queue 3

	var releases []int64
	admitted := 0
	for i := 0; i < 6; i++ {
		d, err := mgr.Try(ctx, model.AlgoLeakyBucket, key, cfg)
		if err != nil {
			t.Fatal(err)
		}
		if d.Allowed {
			admitted++
			releases = append(releases, d.ResetInMs)
		}
	}
	if admitted != 3 {
		t.Fatalf("leaky bucket queue capacity is 3, burst admitted %d", admitted)
	}
	// FIFO release delays must be paced ~100ms apart (1/rate): the smoothing
	// the token bucket does not impose on output.
	want := []int64{100, 200, 300}
	for i, got := range releases {
		if abs(got-want[i]) > 20 {
			t.Fatalf("release pacing wrong at slot %d: got %dms want ~%dms (all=%v)", i, got, want[i], releases)
		}
	}
}

// TestLeakyOverflowRejected: once the queue is full, arrivals are refused
// until enough water leaks to free a whole slot.
func TestLeakyOverflowRejected(t *testing.T) {
	_, mgr := testboot.RedisOnly(t)
	ctx := context.Background()
	key := "lb:over"
	cfg := model.RuleLevel{Threshold: 100, Burst: 2}

	if d, _ := mgr.Try(ctx, model.AlgoLeakyBucket, key, cfg); !d.Allowed {
		t.Fatal("slot 1")
	}
	if d, _ := mgr.Try(ctx, model.AlgoLeakyBucket, key, cfg); !d.Allowed {
		t.Fatal("slot 2")
	}
	if d, _ := mgr.Try(ctx, model.AlgoLeakyBucket, key, cfg); d.Allowed {
		t.Fatal("third request must be rejected while the queue is full")
	}
}
