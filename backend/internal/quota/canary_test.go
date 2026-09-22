package quota_test

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"ratelimit-gateway/internal/canary"
	"ratelimit-gateway/internal/engine"
	"ratelimit-gateway/internal/model"
	"ratelimit-gateway/internal/quota"
	"ratelimit-gateway/internal/stats"
	"ratelimit-gateway/internal/store"
	"ratelimit-gateway/internal/testboot"
)

// oldWindowRule is a generous stable rule; canary is a tight one, both fixed
// window with a long window so tests never straddle a window boundary.
func oldWindowRule(name string) *model.Rule {
	return &model.Rule{
		Name: name, Enabled: true, Algorithm: model.AlgoFixedWindow,
		Dimensions: []string{model.DimClient},
		Levels: map[model.Level]model.RuleLevel{
			model.LevelClient: {Threshold: 1000, WindowSeconds: 120},
		},
	}
}

func canaryWindowRule(name string, threshold int64) *model.Rule {
	r := oldWindowRule(name)
	r.Levels[model.LevelClient] = model.RuleLevel{Threshold: threshold, WindowSeconds: 120}
	return r
}

func rcFor(client string) model.RequestContext {
	return model.RequestContext{ClientID: client, APIPath: "/p"}
}

// expectedVersion predicts the assignment the checker must make, using the
// standalone canary layer (so the test is independent of the checker itself).
func expectedVersion(t *testing.T, ruleID string, ro *canary.Rollout, client string) string {
	t.Helper()
	subj := canary.Subject(&ro.Old, rcFor(client))
	if canary.SelectVersion(ruleID, subj, ro.Percent) == canary.VersionCanary {
		return stats.VersionCanary
	}
	return stats.VersionStable
}

// pickClients finds one subject assigned canary and one assigned stable at the
// given percent, so tests can target each side deterministically.
func pickClients(t *testing.T, ro *canary.Rollout) (canaryClient, stableClient string) {
	t.Helper()
	for i := 0; i < 10000; i++ {
		c := "c" + strconv.Itoa(i)
		subj := canary.Subject(&ro.Old, rcFor(c))
		v := canary.SelectVersion(ro.RuleID, subj, ro.Percent)
		if v == canary.VersionCanary && canaryClient == "" {
			canaryClient = c
		}
		if v == canary.VersionStable && stableClient == "" {
			stableClient = c
		}
		if canaryClient != "" && stableClient != "" {
			return canaryClient, stableClient
		}
	}
	t.Fatalf("could not find subjects on both sides at percent %d", ro.Percent)
	return "", ""
}

// TestCanaryVersionReportedOnVerdict: each verdict reveals which version
// judged it, and that assignment matches the standalone deterministic layer.
func TestCanaryVersionReportedOnVerdict(t *testing.T) {
	s := newStack(t)
	ctx := context.Background()
	base, err := s.rules.Create(ctx, oldWindowRule("gr"))
	if err != nil {
		t.Fatal(err)
	}
	ro, err := s.rollouts.Start(ctx, base, canaryWindowRule("gr", 5), 10)
	if err != nil {
		t.Fatal(err)
	}
	chk := s.checker()

	for i := 0; i < 200; i++ {
		c := "u" + strconv.Itoa(i)
		want := expectedVersion(t, base.ID, ro, c)
		v, err := chk.Decide(ctx, rcFor(c))
		if err != nil {
			t.Fatal(err)
		}
		if v.Version != want {
			t.Fatalf("client %s version = %q want %q", c, v.Version, want)
		}
		if len(v.Results) != 1 || v.Results[0].Version != want {
			t.Fatalf("per-rule result version mismatch for %s", c)
		}
	}
}

// TestSubjectStableAtFixedPercent: the same subject is judged by the same
// version on every request while the percent is unchanged.
func TestSubjectStableAtFixedPercent(t *testing.T) {
	s := newStack(t)
	ctx := context.Background()
	base, _ := s.rules.Create(ctx, oldWindowRule("stable"))
	if _, err := s.rollouts.Start(ctx, base, canaryWindowRule("stable", 5), 25); err != nil {
		t.Fatal(err)
	}
	chk := s.checker()
	for _, c := range []string{"alpha", "beta", "gamma", "delta", "epsilon"} {
		first := ""
		for i := 0; i < 20; i++ {
			v, err := chk.Decide(ctx, rcFor(c))
			if err != nil {
				t.Fatal(err)
			}
			if first == "" {
				first = v.Version
			} else if v.Version != first {
				t.Fatalf("subject %s flipped from %s to %s at fixed percent", c, first, v.Version)
			}
		}
	}
}

// TestMonotonicExpansionAtDecisionLayer: raising the percent only adds
// subjects to canary; existing canary subjects never move back to stable.
func TestMonotonicExpansionAtDecisionLayer(t *testing.T) {
	s := newStack(t)
	ctx := context.Background()
	base, _ := s.rules.Create(ctx, oldWindowRule("expand"))
	if _, err := s.rollouts.Start(ctx, base, canaryWindowRule("expand", 500), 10); err != nil {
		t.Fatal(err)
	}
	chk := s.checker()

	versionsAt := func(pct int) map[string]string {
		if _, err := s.rollouts.SetPercent(ctx, base.ID, pct); err != nil {
			t.Fatal(err)
		}
		out := map[string]string{}
		for i := 0; i < 400; i++ {
			c := "m" + strconv.Itoa(i)
			subj := canary.Subject(base, rcFor(c))
			out[c] = string(canary.SelectVersion(base.ID, subj, pct))
		}
		return out
	}
	v10 := versionsAt(10)
	v50 := versionsAt(50)
	for c, side := range v10 {
		if side == string(canary.VersionCanary) && v50[c] != string(canary.VersionCanary) {
			t.Fatalf("subject %s canary@10%% moved to %s@50%%", c, v50[c])
		}
	}
	// The checker's live verdicts agree with the predicted assignment at 50%.
	for i := 0; i < 100; i++ {
		c := "m" + strconv.Itoa(i)
		v, err := chk.Decide(ctx, rcFor(c))
		if err != nil {
			t.Fatal(err)
		}
		if v.Version != v50[c] {
			t.Fatalf("checker version %s != predicted %s for %s", v.Version, v50[c], c)
		}
	}
}

// TestVersionCountersIsolated: the canary's tight quota rejecting a canary
// client never consumes the stable quota of the same client, and vice versa.
func TestVersionCountersIsolated(t *testing.T) {
	s := newStack(t)
	ctx := context.Background()
	base, _ := s.rules.Create(ctx, oldWindowRule("iso"))
	ro, err := s.rollouts.Start(ctx, base, canaryWindowRule("iso", 3), 50)
	if err != nil {
		t.Fatal(err)
	}
	chk := s.checker()
	canaryC, stableC := pickClients(t, ro)

	// Exhaust the CANARY quota (3) of the canary client.
	for i := 0; i < 3; i++ {
		if v, _ := chk.Decide(ctx, rcFor(canaryC)); !v.Allowed || v.Version != stats.VersionCanary {
			t.Fatalf("canary client req %d should be allowed by canary", i+1)
		}
	}
	if v, _ := chk.Decide(ctx, rcFor(canaryC)); v.Allowed {
		t.Fatal("4th canary request must be rejected by the tight canary quota")
	}

	// The same client on the STABLE side is a different subject in practice;
	// to prove counter isolation per identity, drive a stable subject hard: it
	// must be unaffected by the canary bucket the canary client exhausted.
	for i := 0; i < 30; i++ {
		if v, _ := chk.Decide(ctx, rcFor(stableC)); !v.Allowed {
			t.Fatalf("stable client must keep passing (stable quota 1000), denied at %d: %s", i, v.Reason)
		}
		if got, _ := chk.Decide(ctx, rcFor(stableC)); got.Version != stats.VersionStable {
			t.Fatal("stable client must remain on stable version")
		}
	}

	// Key-level proof: stable and canary buckets for the SAME identity are
	// different Redis keys and hold independent state.
	stableKey := "rl:{" + base.ID + "}:client:c=" + canaryC
	canaryKey := "rl:{" + base.ID + "}:canary:client:c=" + canaryC
	if stableKey == canaryKey {
		t.Fatal("version keys collide")
	}
	if n, err := s.rdb.Exists(ctx, canaryKey).Result(); err != nil || n != 1 {
		t.Fatalf("canary bucket key must exist, n=%d err=%v", n, err)
	}
	// The canary client's stable key was never touched (it always routed canary).
	if n, _ := s.rdb.Exists(ctx, stableKey).Result(); n != 0 {
		t.Fatalf("canary traffic must not touch stable key, exists=%d", n)
	}
}

// TestAbortRevertsAllTrafficToStable: after abort every request uses the old
// (live) rule; canary counters can no longer influence any decision.
func TestAbortRevertsAllTrafficToStable(t *testing.T) {
	s := newStack(t)
	ctx := context.Background()
	base, _ := s.rules.Create(ctx, oldWindowRule("abort"))
	if _, err := s.rollouts.Start(ctx, base, canaryWindowRule("abort", 1), 100); err != nil {
		t.Fatal(err)
	}
	chk := s.checker()
	canaryC := "everyone-canary"

	// at 100% everything is canary; canary quota (1) rejects the 2nd.
	if v, _ := chk.Decide(ctx, rcFor(canaryC)); v.Version != stats.VersionCanary {
		t.Fatal("at 100% all traffic must be canary")
	}
	if v, _ := chk.Decide(ctx, rcFor(canaryC)); v.Allowed {
		t.Fatal("tight canary quota must reject")
	}

	if err := s.rollouts.Abort(ctx, base.ID); err != nil {
		t.Fatal(err)
	}
	// Immediately all traffic goes back to the generous stable rule; the
	// exhausted canary counter cannot block it.
	for i := 0; i < 50; i++ {
		v, err := chk.Decide(ctx, rcFor(canaryC))
		if err != nil {
			t.Fatal(err)
		}
		if v.Version != "" && v.Version != stats.VersionStable {
			t.Fatalf("after abort version must be stable, got %q", v.Version)
		}
		if !v.Allowed {
			t.Fatalf("after abort the old generous rule must allow, denied: %s", v.Reason)
		}
	}
}

// TestZeroPercentParksRollout: percent 0 behaves like abort for traffic (all
// stable) but keeps the canary content so it can be raised again.
func TestZeroPercentParksRollout(t *testing.T) {
	s := newStack(t)
	ctx := context.Background()
	base, _ := s.rules.Create(ctx, oldWindowRule("park"))
	if _, err := s.rollouts.Start(ctx, base, canaryWindowRule("park", 1), 0); err != nil {
		t.Fatal(err)
	}
	chk := s.checker()
	for i := 0; i < 20; i++ {
		v, _ := chk.Decide(ctx, rcFor("p"+strconv.Itoa(i)))
		if v.Version == stats.VersionCanary {
			t.Fatal("at 0% no traffic may be judged by canary")
		}
	}
	ro, ok := s.rollouts.Get(base.ID)
	if !ok {
		t.Fatal("rollout content must be retained at 0%")
	}
	if ro.Percent != 0 {
		t.Fatalf("percent = %d want 0", ro.Percent)
	}
}

// TestCrossInstanceAssignmentConsistent: two independent decision paths (each
// its own Checker and its own hot-synced RolloutStore) assign every subject to
// the same version, before and after a percent change on one of them.
func TestCrossInstanceAssignmentConsistent(t *testing.T) {
	dsn := testboot.DSN()
	// shared Redis but do NOT flush the second client; both stores + checkers
	// model two gateway replicas behind one Redis/Postgres.
	rdb1, rules1, ros1, mgr1, rec1 := testboot.FullStack(t)
	rdb2 := redis.NewClient(rdb1.Options())
	ctx := context.Background()

	rules2, err := store.New(ctx, dsn, rdb2)
	if err != nil {
		t.Fatal(err)
	}
	go rules2.SubscribeHotReload(ctx)
	ros2, err := store.NewRolloutStore(ctx, rules2.PgPool(), rdb2)
	if err != nil {
		t.Fatal(err)
	}
	go ros2.SubscribeHotReload(ctx)
	mgr2, err := engine.NewManager(ctx, rdb2)
	if err != nil {
		t.Fatal(err)
	}
	chk1 := quota.New(rules1, ros1, mgr1, rdb1, rec1)
	chk2 := quota.New(rules2, ros2, mgr2, rdb2, stats.New(rdb2))

	base, _ := rules1.Create(ctx, oldWindowRule("replicas"))
	if _, err := ros1.Start(ctx, base, canaryWindowRule("replicas", 500), 10); err != nil {
		t.Fatal(err)
	}
	mustSyncRollout(t, ros2, base.ID, 10)
	mustSyncRule(t, rules2, base.ID)

	for i := 0; i < 100; i++ {
		c := "r" + strconv.Itoa(i)
		v1, _ := chk1.Decide(ctx, rcFor(c))
		v2, _ := chk2.Decide(ctx, rcFor(c))
		if v1.Version != v2.Version {
			t.Fatalf("replicas disagree for %s at 10%%: %s vs %s", c, v1.Version, v2.Version)
		}
	}

	// instance 1 raises the percent; instance 2 must immediately follow.
	if _, err := ros1.SetPercent(ctx, base.ID, 70); err != nil {
		t.Fatal(err)
	}
	mustSyncRollout(t, ros2, base.ID, 70)
	for i := 0; i < 100; i++ {
		c := "r" + strconv.Itoa(i)
		v1, _ := chk1.Decide(ctx, rcFor(c))
		v2, _ := chk2.Decide(ctx, rcFor(c))
		if v1.Version != v2.Version {
			t.Fatalf("replicas disagree for %s after raise: %s vs %s", c, v1.Version, v2.Version)
		}
	}
}

// TestRolloutRestartContinuesSharding: after reopening stores (a restart) the
// checker resumes sharding at the persisted percent with both rule versions.
func TestRolloutRestartContinuesSharding(t *testing.T) {
	// Start from a clean (truncated) stack so no earlier test's rollout can
	// leak into the verdict, then reopen stores to simulate a process restart.
	s := newStack(t)
	dsn := testboot.DSN()
	rdb := s.rdb
	ctx := context.Background()

	rules1 := s.rules
	ros1 := s.rollouts
	base, _ := rules1.Create(ctx, oldWindowRule("restart-shard"))
	ro, err := ros1.Start(ctx, base, canaryWindowRule("restart-shard", 500), 42)
	if err != nil {
		t.Fatal(err)
	}

	// restart: brand-new stores opened against the same durable Postgres/Redis
	rules2, err := store.New(ctx, dsn, rdb)
	if err != nil {
		t.Fatal(err)
	}
	ros2, err := store.NewRolloutStore(ctx, rules2.PgPool(), rdb)
	if err != nil {
		t.Fatal(err)
	}
	mgr, err := engine.NewManager(ctx, rdb)
	if err != nil {
		t.Fatal(err)
	}
	chk := quota.New(rules2, ros2, mgr, rdb, stats.New(rdb))

	reloaded, ok := ros2.Get(base.ID)
	if !ok || reloaded.Percent != 42 {
		t.Fatalf("rollout not restored, got %+v", reloaded)
	}
	canaryC, stableC := pickClients(t, ro)
	if v, _ := chk.Decide(ctx, rcFor(canaryC)); v.Version != stats.VersionCanary {
		t.Fatalf("canary subject must stay canary after restart, got %s", v.Version)
	}
	if v, _ := chk.Decide(ctx, rcFor(stableC)); v.Version != stats.VersionStable {
		t.Fatalf("stable subject must stay stable after restart, got %s", v.Version)
	}
}

func mustSyncRollout(t *testing.T, ros *store.RolloutStore, id string, pct int) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if ro, ok := ros.Get(id); ok && ro.Percent == pct {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("rollout did not sync to percent %d", pct)
}

func mustSyncRule(t *testing.T, rs *store.RuleStore, id string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if _, ok := rs.Get(id); ok {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("rule %s did not hot-sync to the second instance", id)
}
