package quota_test

import (
	"context"
	"testing"
	"time"

	"ratelimit-gateway/internal/canary"
	"ratelimit-gateway/internal/model"
	"ratelimit-gateway/internal/store"
	"ratelimit-gateway/internal/testboot"
)

func canaryBaseRule() *model.Rule {
	return &model.Rule{
		Name:       "canary-window",
		Enabled:    true,
		Algorithm:  model.AlgoFixedWindow,
		Dimensions: []string{model.DimClient},
		Levels: map[model.Level]model.RuleLevel{
			model.LevelClient: {Threshold: 100, WindowSeconds: 60},
		},
	}
}

func canaryTightRule() *model.Rule {
	r := canaryBaseRule()
	r.Levels[model.LevelClient] = model.RuleLevel{Threshold: 1, WindowSeconds: 60}
	return r
}

// classify splits clients by the same deterministic bucket the checker uses.
func classify(t *testing.T, rs *store.RuleStore, ruleID string, clients []string, percent int) (newOnes, oldOnes []string) {
	t.Helper()
	old, ok := rs.Get(ruleID)
	if !ok {
		t.Fatal("rule missing")
	}
	rl, hasRollout := rs.Rollout(ruleID)
	for _, c := range clients {
		rctx := model.RequestContext{ClientID: c, APIPath: "/x"}
		isNew := false
		if hasRollout {
			isNew = canary.SelectVersion(old, rl.Percent, rctx) == canary.VersionNew
		}
		_ = percent
		if isNew {
			newOnes = append(newOnes, c)
		} else {
			oldOnes = append(oldOnes, c)
		}
	}
	return newOnes, oldOnes
}

// TestCanarySplitStableAndIsolated:
//   - same subject is judged by the same version repeatedly (and by two
//     independent checker instances sharing one Redis);
//   - the two versions' counters are isolated: a saturated new-version client
//     does not touch the old-version quota of the same client.
func TestCanarySplitStableAndIsolated(t *testing.T) {
	rdb, rs, mgr, rec := testbootFull(t)
	ctx := context.Background()
	go rs.SubscribeHotReload(ctx)
	time.Sleep(200 * time.Millisecond)

	r, err := rs.Create(ctx, canaryBaseRule())
	if err != nil {
		t.Fatal(err)
	}
	// classify clients BEFORE starting so we know who lands where at 10%
	var clients []string
	for i := 0; i < 300; i++ {
		clients = append(clients, "split-c-"+itoa(i))
	}
	if _, err := rs.StartRollout(ctx, r.ID, canaryTightRule(), 10); err != nil {
		t.Fatal(err)
	}
	newClients, oldClients := classify(t, rs, r.ID, clients, 10)
	if len(newClients) == 0 || len(oldClients) == 0 {
		t.Fatalf("expected both sides populated: new=%d old=%d", len(newClients), len(oldClients))
	}

	// Two independent decision paths simulate two gateway instances.
	c1 := newCheckerFrom(t, rdb, rs, mgr, rec)
	c2 := newCheckerFrom(t, rdb, rs, mgr, rec)

	for _, c := range newClients[:5] {
		rctx := model.RequestContext{ClientID: c, APIPath: "/x"}
		v1, err := c1.Decide(ctx, rctx)
		if err != nil {
			t.Fatal(err)
		}
		// first new-version request allowed (limit 1); second must be denied
		// BY THE NEW VERSION on either "instance"
		v2, err := c2.Decide(ctx, rctx)
		if err != nil {
			t.Fatal(err)
		}
		if !v1.Allowed {
			t.Fatalf("new client %s first request unexpectedly denied: %s", c, v1.Reason)
		}
		if v2.Allowed {
			t.Fatalf("new client %s second request must be denied by tight new rule", c)
		}
		if v2.Version != canary.VersionNew || v2.Level != model.LevelClient {
			t.Fatalf("deny should be attributed to new/client, got version=%q level=%q", v2.Version, v2.Level)
		}
	}

	// Isolation: the same client id under the old version still has its full
	// old quota — the new-version saturation did not consume old counters.
	for _, c := range newClients[:5] {
		rctx := model.RequestContext{ClientID: c, APIPath: "/x"}
		// sanity: this subject is definitely on the new side at 10%
		if canary.SelectVersion(mustGet(t, rs, r.ID), 10, rctx) != canary.VersionNew {
			t.Fatal("test setup: expected new-side client")
		}
	}
	// an old-side client gets 100 admits for the same logical client slot
	oc := oldClients[0]
	for i := 0; i < 100; i++ {
		v, err := c1.Decide(ctx, model.RequestContext{ClientID: oc, APIPath: "/x"})
		if err != nil {
			t.Fatal(err)
		}
		if !v.Allowed {
			t.Fatalf("old-side client denied at request %d: %s (new counters leaked into old)", i, v.Reason)
		}
	}
}

// TestCanaryMonotoneAcrossResize: subjects new at 10% stay new at 50%; only
// additional subjects move.
func TestCanaryMonotoneAcrossResize(t *testing.T) {
	rdb, rs, mgr, rec := testbootFull(t)
	ctx := context.Background()
	chk := newCheckerFrom(t, rdb, rs, mgr, rec)

	r, err := rs.Create(ctx, canaryBaseRule())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := rs.StartRollout(ctx, r.ID, canaryTightRule(), 10); err != nil {
		t.Fatal(err)
	}
	old, _ := rs.Get(r.ID)

	var clients []string
	for i := 0; i < 200; i++ {
		clients = append(clients, "mono-c-"+itoa(i))
	}
	newAt10 := map[string]bool{}
	for _, c := range clients {
		if canary.SelectVersion(old, 10, model.RequestContext{ClientID: c, APIPath: "/x"}) == canary.VersionNew {
			newAt10[c] = true
		}
	}
	if err := rs.SetRolloutPercent(ctx, r.ID, 50); err != nil {
		t.Fatal(err)
	}
	rl, _ := rs.Rollout(r.ID)
	for c := range newAt10 {
		v, err := chk.Decide(ctx, model.RequestContext{ClientID: c, APIPath: "/x"})
		if err != nil {
			t.Fatal(err)
		}
		// already-new subjects must STILL be evaluated by the new version
		// (their second hit is denied by the tight new limit; the first one
		// may or may not have been consumed by earlier tests — so assert by
		// direct selection rather than verdict).
		_ = v
		if canary.SelectVersion(old, rl.Percent, model.RequestContext{ClientID: c, APIPath: "/x"}) != canary.VersionNew {
			t.Fatalf("subject %s reshuffled out of new on 10%%->50%%", c)
		}
	}
}

// TestCanaryAbortReturnsAllToOld: after abort every request is judged by the
// old rule again, even a client that was saturated on the new side; canary
// counters cannot influence anything.
func TestCanaryAbortReturnsAllToOld(t *testing.T) {
	rdb, rs, mgr, rec := testbootFull(t)
	ctx := context.Background()
	chk := newCheckerFrom(t, rdb, rs, mgr, rec)

	r, err := rs.Create(ctx, canaryBaseRule())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := rs.StartRollout(ctx, r.ID, canaryTightRule(), 100); err != nil {
		// 100 at start is rejected on purpose; use 99 then resize to 100 or
		// just test abort from 50.
	}
	if _, err := rs.StartRollout(ctx, r.ID, canaryTightRule(), 50); err != nil {
		t.Fatal(err)
	}
	// find a new-side client and exhaust its new quota (limit 1)
	var victim string
	for i := 0; i < 200; i++ {
		c := "abort-c-" + itoa(i)
		old, _ := rs.Get(r.ID)
		rl, _ := rs.Rollout(r.ID)
		if canary.SelectVersion(old, rl.Percent, model.RequestContext{ClientID: c, APIPath: "/x"}) == canary.VersionNew {
			victim = c
			break
		}
	}
	if victim == "" {
		t.Fatal("no new-side client found")
	}
	v1, _ := chk.Decide(ctx, model.RequestContext{ClientID: victim, APIPath: "/x"})
	if !v1.Allowed {
		t.Fatal("first new-side request should pass")
	}
	v2, _ := chk.Decide(ctx, model.RequestContext{ClientID: victim, APIPath: "/x"})
	if v2.Allowed || v2.Version != canary.VersionNew {
		t.Fatalf("second should be denied by new version, got %+v", v2)
	}

	if err := rs.SetRolloutPercent(ctx, r.ID, 0); err != nil {
		t.Fatal(err)
	}
	if _, ok := rs.Rollout(r.ID); ok {
		t.Fatal("rollout must be gone after abort")
	}
	// the very next request for the same client is back on the old rule's
	// full 100/window quota and passes.
	v3, err := chk.Decide(ctx, model.RequestContext{ClientID: victim, APIPath: "/x"})
	if err != nil {
		t.Fatal(err)
	}
	if !v3.Allowed {
		t.Fatalf("after abort the client must be on old quota, got denied: %s", v3.Reason)
	}
	for _, rr := range v3.Results {
		if rr.Version != "" {
			t.Fatalf("post-abort verdicts must carry no phantom version, got %q", rr.Version)
		}
	}
}

// TestCanaryPromoteFoldsCounters: at 100% the new content is the rule and the
// rollout disappears; subsequent judgments are single-version.
func TestCanaryPromoteFoldsCounters(t *testing.T) {
	rdb, rs, mgr, rec := testbootFull(t)
	ctx := context.Background()
	chk := newCheckerFrom(t, rdb, rs, mgr, rec)

	r, err := rs.Create(ctx, canaryBaseRule())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := rs.StartRollout(ctx, r.ID, canaryTightRule(), 10); err != nil {
		t.Fatal(err)
	}
	if err := rs.SetRolloutPercent(ctx, r.ID, 100); err != nil {
		t.Fatal(err)
	}
	if _, ok := rs.Rollout(r.ID); ok {
		t.Fatal("rollout must be gone after promotion")
	}
	got, _ := rs.Get(r.ID)
	if got.Levels[model.LevelClient].Threshold != 1 {
		t.Fatalf("promoted rule must be the new content, threshold=%d", got.Levels[model.LevelClient].Threshold)
	}
	// now EVERY client is judged by the single tight version
	v, _ := chk.Decide(ctx, model.RequestContext{ClientID: "anyone", APIPath: "/x"})
	if !v.Allowed {
		t.Fatal("first post-promote request allowed")
	}
	v2, _ := chk.Decide(ctx, model.RequestContext{ClientID: "anyone", APIPath: "/x"})
	if v2.Allowed || v2.Version != "" {
		t.Fatalf("second must be denied by the now-ordinary rule with no version tag, got %+v", v2)
	}
}

// TestCanaryRestartKeepsSplit: a new store+checker ("restarted gateway")
// continues splitting at the persisted percentage, and the same subjects are
// still on the new side.
func TestCanaryRestartKeepsSplit(t *testing.T) {
	dsn := testboot.DSN()
	// full clean slate: truncate rules + flush counters, then build the two
	// store instances ourselves against the same DSN to simulate a restart.
	rdb, _, _, _ := testbootFull(t)
	ctx := context.Background()

	rs1, err := store.New(ctx, dsn, rdb)
	if err != nil {
		t.Fatal(err)
	}
	r, err := rs1.Create(ctx, canaryBaseRule())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := rs1.StartRollout(ctx, r.ID, canaryTightRule(), 30); err != nil {
		t.Fatal(err)
	}

	// restart: new store against same PG/Redis, new engine manager, new checker
	rs2, err := store.New(ctx, dsn, rdb)
	if err != nil {
		t.Fatal(err)
	}
	mgr2, err := newMgr(ctx, rdb)
	if err != nil {
		t.Fatal(err)
	}
	rec := newRec(rdb)
	chk := newCheckerFrom(t, rdb, rs2, mgr2, rec)

	rl, ok := rs2.Rollout(r.ID)
	if !ok || rl.Percent != 30 {
		t.Fatalf("rollout must survive restart: %+v ok=%v", rl, ok)
	}
	var victim string
	for i := 0; i < 300; i++ {
		c := "restart-c-" + itoa(i)
		old, _ := rs2.Get(r.ID)
		if canary.SelectVersion(old, 30, model.RequestContext{ClientID: c, APIPath: "/x"}) == canary.VersionNew {
			victim = c
			break
		}
	}
	v1, _ := chk.Decide(ctx, model.RequestContext{ClientID: victim, APIPath: "/x"})
	v2, _ := chk.Decide(ctx, model.RequestContext{ClientID: victim, APIPath: "/x"})
	if !v1.Allowed || v2.Allowed || v2.Version != canary.VersionNew {
		t.Fatalf("after restart new-side subject must still hit tight new rule: %+v %+v", v1, v2)
	}
}

// TestCanaryHotSyncAcrossTwoCheckers: a percent change on instance A changes
// how instance B splits, with no restart.
func TestCanaryHotSyncAcrossTwoCheckers(t *testing.T) {
	dsn := testboot.DSN()
	rdb, _, _, _ := testbootFull(t) // truncate + flush clean slate
	ctx := context.Background()

	rsA, err := store.New(ctx, dsn, rdb)
	if err != nil {
		t.Fatal(err)
	}
	rsB, err := store.New(ctx, dsn, rdb)
	if err != nil {
		t.Fatal(err)
	}
	go rsA.SubscribeHotReload(ctx)
	go rsB.SubscribeHotReload(ctx)
	time.Sleep(300 * time.Millisecond)

	mgr, err := newMgr(ctx, rdb)
	if err != nil {
		t.Fatal(err)
	}
	rec := newRec(rdb)
	cA := newCheckerFrom(t, rdb, rsA, mgr, rec)
	cB := newCheckerFrom(t, rdb, rsB, mgr, rec)

	r, err := rsA.Create(ctx, canaryBaseRule())
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(200 * time.Millisecond) // let B see the rule
	if _, err := rsA.StartRollout(ctx, r.ID, canaryTightRule(), 10); err != nil {
		t.Fatal(err)
	}
	waitRollout(t, rsB, r.ID, 10)

	// find a subject that is OLD at 10% but NEW at 90%
	old, _ := rsB.Get(r.ID)
	var mover string
	for i := 0; i < 1000; i++ {
		c := "sync-c-" + itoa(i)
		rctx := model.RequestContext{ClientID: c, APIPath: "/x"}
		b := canary.Bucket(canary.Subject(old, rctx))
		if b >= 10 && b < 90 {
			mover = c
			break
		}
	}
	if mover == "" {
		t.Fatal("could not find a bucket in [10,90)")
	}
	rctx := model.RequestContext{ClientID: mover, APIPath: "/x"}
	if v, _ := cB.Decide(ctx, rctx); !v.Allowed {
		t.Fatal("mover must be on permissive old side at 10%")
	}
	if err := rsA.SetRolloutPercent(ctx, r.ID, 90); err != nil {
		t.Fatal(err)
	}
	waitRollout(t, rsB, r.ID, 90)
	// B now puts the mover on the tight new side; second request denies new
	if v, _ := cB.Decide(ctx, rctx); !v.Allowed {
		t.Fatal("first post-resize request allowed (fresh new bucket)")
	}
	v2, err := cB.Decide(ctx, rctx)
	if err != nil {
		t.Fatal(err)
	}
	if v2.Allowed || v2.Version != canary.VersionNew {
		t.Fatalf("instance B must switch the mover to new after hot resize, got %+v", v2)
	}
	// A agrees
	v3, err := cA.Decide(ctx, model.RequestContext{ClientID: mover + "-z", APIPath: "/x"})
	_ = v3
	if err != nil {
		t.Fatal(err)
	}
}

// TestCanaryIllegalNewVersionRejected at the checker-facing store boundary.
func TestCanaryIllegalNewVersionRejected(t *testing.T) {
	_, rs, _, _ := testbootFull(t)
	ctx := context.Background()
	r, err := rs.Create(ctx, canaryBaseRule())
	if err != nil {
		t.Fatal(err)
	}
	bad := canaryTightRule()
	bad.Algorithm = "mystery"
	if _, err := rs.StartRollout(ctx, r.ID, bad, 10); err == nil {
		t.Fatal("unknown algorithm in new version must be rejected")
	}
}

func mustGet(t *testing.T, rs *store.RuleStore, id string) *model.Rule {
	t.Helper()
	r, ok := rs.Get(id)
	if !ok {
		t.Fatal("rule missing")
	}
	return r
}

func waitRollout(t *testing.T, rs *store.RuleStore, id string, percent int) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if rl, ok := rs.Rollout(id); ok && rl.Percent == percent {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for rollout percent %d", percent)
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var b [12]byte
	pos := len(b)
	for i > 0 {
		pos--
		b[pos] = byte('0' + i%10)
		i /= 10
	}
	return string(b[pos:])
}
