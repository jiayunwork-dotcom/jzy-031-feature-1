package engine_test

import (
	"context"
	"testing"
	"time"

	"ratelimit-gateway/internal/engine"
	"ratelimit-gateway/internal/model"
	"ratelimit-gateway/internal/testboot"
)

// TestFixedWindowBoundaryDoubleBurst: exhaust the limit at the tail of one
// window; right after the boundary the counter resets and another full limit
// is instantly spendable — the classic ~2x seam.
func TestFixedWindowBoundaryDoubleBurst(t *testing.T) {
	_, mgr := testboot.RedisOnly(t)
	key := "fw:seam"
	cfg := model.RuleLevel{Threshold: 100, WindowSeconds: 1}

	fireWin := fireAtWindowTail(t, mgr, model.AlgoFixedWindow, key, cfg, 100)

	// Sleep genuinely into the following window.
	sleepPastBoundary(t, fireWin)
	if got := admitN(t, mgr, model.AlgoFixedWindow, key, cfg, 100); got != 100 {
		t.Fatalf("fixed window must reset at boundary (allow ~2x seam), admitted %d", got)
	}
}

// TestSlidingWindowNoDoubleBurst: same pattern, but the weighted previous
// window still counts at the boundary, so the second burst is blocked.
func TestSlidingWindowNoDoubleBurst(t *testing.T) {
	_, mgr := testboot.RedisOnly(t)
	key := "sw:seam"
	cfg := model.RuleLevel{Threshold: 100, WindowSeconds: 1}

	fireWin := fireAtWindowTail(t, mgr, model.AlgoSlidingWindow, key, cfg, 100)
	sleepPastBoundary(t, fireWin)

	// Previous window is weighted ~0.9, so only a handful of new requests
	// fit; nowhere near another full 100.
	allowed := admitN(t, mgr, model.AlgoSlidingWindow, key, cfg, 100)
	if allowed > 15 {
		t.Fatalf("sliding window must not allow a 2x boundary burst, admitted %d", allowed)
	}
	if allowed < 1 {
		t.Fatalf("sliding window should still allow a few requests as the weight fades, got %d", allowed)
	}
}

// fireAtWindowTail waits until late in a whole-second window and fires n
// requests, returning that window's start (epoch ms).
func fireAtWindowTail(t *testing.T, mgr *engine.Manager, algo model.Algorithm, key string, cfg model.RuleLevel, n int) int64 {
	t.Helper()
	// wait until we are inside the final 300ms of the current second
	var winStart int64
	for {
		now := time.Now().UnixMilli()
		winStart = (now / 1000) * 1000
		if now-winStart > 650 {
			break
		}
		time.Sleep(15 * time.Millisecond)
	}
	if got := admitN(t, mgr, algo, key, cfg, n); got != n {
		t.Fatalf("tail-of-window burst should admit %d, got %d", n, got)
	}
	return winStart
}

func sleepPastBoundary(t *testing.T, windowStartMs int64) {
	t.Helper()
	for {
		now := time.Now().UnixMilli()
		curStart := (now / 1000) * 1000
		if curStart > windowStartMs {
			time.Sleep(40 * time.Millisecond) // settle just inside new window
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestFixedWindowMidWindowReject confirms ordinary in-window limiting.
func TestFixedWindowMidWindowReject(t *testing.T) {
	_, mgr := testboot.RedisOnly(t)
	ctx := context.Background()
	key := "fw:mid"
	cfg := model.RuleLevel{Threshold: 3, WindowSeconds: 1}
	if got := admitN(t, mgr, model.AlgoFixedWindow, key, cfg, 10); got != 3 {
		t.Fatalf("fixed window admits only 3 per window, got %d", got)
	}
	// peek reflects zero remaining without consuming
	d, err := mgr.Peek(ctx, model.AlgoFixedWindow, key, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if d.Allowed || d.Remaining != 0 {
		t.Fatalf("peek should report exhausted window, got %+v", d)
	}
}
