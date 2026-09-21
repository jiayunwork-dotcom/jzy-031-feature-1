package engine_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"

	"ratelimit-gateway/internal/model"
	"ratelimit-gateway/internal/testboot"
)

// TestConcurrentNoOversell: hammers one bucket from many goroutines (which
// would also map to many gateway instances sharing Redis). The atomic Lua
// scripts guarantee the number admitted never exceeds the capacity/limit.
func TestConcurrentNoOversell(t *testing.T) {
	algoCases := []struct {
		name model.Algorithm
		cfg  model.RuleLevel
		cap  int64
	}{
		// 1/s refill/leak and a 60s window mean the test only fails if the
		// counter exceeds instantaneous capacity; legitimate sub-second
		// refill is allowed for but asserted below as a tight upper bound.
		{"token_bucket", model.RuleLevel{Threshold: 1, Burst: 20}, 20},
		{"leaky_bucket", model.RuleLevel{Threshold: 1, Burst: 20}, 20},
		{"fixed_window", model.RuleLevel{Threshold: 20, WindowSeconds: 60}, 20},
		{"sliding_window", model.RuleLevel{Threshold: 20, WindowSeconds: 60}, 20},
		{"sliding_log", model.RuleLevel{Threshold: 20, WindowSeconds: 60}, 20},
	}
	for _, tc := range algoCases {
		t.Run(string(tc.name), func(t *testing.T) {
			_, mgr := testboot.RedisOnly(t)
			ctx := context.Background()
			key := "conc:" + string(tc.name)

			const goroutines = 64
			const per = 10
			var allowed int64
			var wg sync.WaitGroup
			wg.Add(goroutines)
			for g := 0; g < goroutines; g++ {
				go func() {
					defer wg.Done()
					for i := 0; i < per; i++ {
						d, err := mgr.Try(ctx, tc.name, key, tc.cfg)
						if err != nil {
							t.Error(err)
							return
						}
						if d.Allowed {
							atomic.AddInt64(&allowed, 1)
						}
					}
				}()
			}
			wg.Wait()
			// Core invariant: never exceed instantaneous capacity. Window
			// algorithms (60s window) cannot refill during the burst, so they
			// must admit exactly 20. Bucket algorithms refill at 1/s, so allow
			// for at most a small refill but still reject any real oversell.
			if tc.name == model.AlgoTokenBucket || tc.name == model.AlgoLeakyBucket {
				if allowed < tc.cap || allowed > tc.cap+2 {
					t.Fatalf("%s: concurrent admits = %d, want %d..%d (initial capacity %d, tiny refill tolerated, oversell forbidden)",
						tc.name, allowed, tc.cap, tc.cap+2, tc.cap)
				}
			} else if allowed != tc.cap {
				t.Fatalf("%s: concurrent admits = %d, want exactly %d (no oversell)",
					tc.name, allowed, tc.cap)
			}
		})
	}
}
