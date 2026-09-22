package api_test

import (
	"encoding/json"
	"net/http"
	"testing"
)

func windowRuleBody(name string, threshold int) map[string]any {
	return map[string]any{
		"name": name, "enabled": true, "algorithm": "fixed_window",
		"dimensions": []string{"client_id"},
		"levels": map[string]any{
			"client": map[string]any{"threshold": threshold, "window_seconds": 60},
		},
	}
}

func createRule(t *testing.T, h http.Handler, body map[string]any) string {
	t.Helper()
	code, out := postJSON(t, h, "/api/rules", body)
	if code != http.StatusCreated {
		t.Fatalf("create rule: %d %v", code, out)
	}
	return out["id"].(string)
}

// TestAPIRolloutRejectsInvalidPercent: out of range / non-numeric percent is a
// 400 carrying a reason.
func TestAPIRolloutRejectsInvalidPercent(t *testing.T) {
	h := newRouter(t)
	id := createRule(t, h, windowRuleBody("pct", 100))

	start := func(raw map[string]any) (int, map[string]any) {
		return postJSON(t, h, "/api/rules/"+id+"/rollout", raw)
	}

	// 150% rejected
	if code, b := start(map[string]any{"percent": 150, "canary": windowRuleBody("pct", 10)}); code != http.StatusBadRequest {
		t.Fatalf("percent 150 must be 400, got %d %v", code, b)
	} else if msg, _ := b["error"].(string); msg == "" {
		t.Fatal("400 must explain the percent bounds")
	}

	// negative percent rejected
	if code, b := start(map[string]any{"percent": -10, "canary": windowRuleBody("pct", 10)}); code != http.StatusBadRequest {
		t.Fatalf("negative percent must be 400, got %d %v", code, b)
	}

	// non-numeric percent rejected by strict JSON decoding
	raw := `{"percent":"ten","canary":` + mustJSON(windowRuleBody("pct", 10)) + `}`
	req := newJSONReq(http.MethodPost, "/api/rules/"+id+"/rollout", raw)
	w := serveReq(h, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("string percent must be 400, got %d body=%s", w.Code, w.Body.String())
	}

	// missing percent rejected
	if code, b := start(map[string]any{"canary": windowRuleBody("pct", 10)}); code != http.StatusBadRequest {
		t.Fatalf("missing percent must be 400, got %d %v", code, b)
	}

	// illegal canary content (unknown algorithm) rejected even at 10%
	badCanary := windowRuleBody("pct", 10)
	badCanary["algorithm"] = "bogus"
	if code, b := start(map[string]any{"percent": 10, "canary": badCanary}); code != http.StatusBadRequest {
		t.Fatalf("illegal canary must be 400, got %d %v", code, b)
	}
}

// TestAPIRolloutLifecycle end-to-end over HTTP: start at 10%, adjust to 0 and
// 100, promote; and a second rule aborted returns all traffic to stable.
func TestAPIRolloutLifecycle(t *testing.T) {
	h := newRouter(t)
	id := createRule(t, h, windowRuleBody("life", 1000))

	// start rollout at 10%
	code, body := postJSON(t, h, "/api/rules/"+id+"/rollout",
		map[string]any{"percent": 10, "canary": windowRuleBody("life", 1)})
	if code != http.StatusCreated {
		t.Fatalf("start: %d %v", code, body)
	}
	if body["percent"].(float64) != 10 {
		t.Fatalf("percent = %v", body["percent"])
	}

	// list shows the rollout
	if c, b := getJSON(t, h, "/api/rollouts"); c != http.StatusOK || len(b["rollouts"].([]any)) != 1 {
		t.Fatalf("rollout list wrong: %d %v", c, b)
	}

	// adjust to 0 then 100 via PUT
	if c, b := putJSON(t, h, "/api/rules/"+id+"/rollout", map[string]any{"percent": 0}); c != http.StatusOK || b["percent"].(float64) != 0 {
		t.Fatalf("set 0: %d %v", c, b)
	}
	if c, b := putJSON(t, h, "/api/rules/"+id+"/rollout", map[string]any{"percent": 100}); c != http.StatusOK || b["percent"].(float64) != 100 {
		t.Fatalf("set 100: %d %v", c, b)
	}

	// promote -> canary (threshold 1) becomes the live rule and rollout is gone
	if c, b := postJSONEmpty(t, h, "/api/rules/"+id+"/rollout/promote"); c != http.StatusOK {
		t.Fatalf("promote: %d %v", c, b)
	}
	if c, b := getJSON(t, h, "/api/rollouts"); c != http.StatusOK || len(b["rollouts"].([]any)) != 0 {
		t.Fatalf("rollout must be gone after promote: %v", b)
	}
	if c, b := getJSON(t, h, "/api/rules/"+id); c != http.StatusOK {
		t.Fatalf("get rule: %d", c)
	} else {
		lv := b["levels"].(map[string]any)["client"].(map[string]any)
		if lv["threshold"].(float64) != 1 {
			t.Fatalf("live rule threshold must be the canary's 1, got %v", lv["threshold"])
		}
	}

	// cannot promote a non-existent rollout
	if c, _ := postJSONEmpty(t, h, "/api/rules/"+id+"/rollout/promote"); c != http.StatusNotFound {
		t.Fatalf("promote after completion must 404, got %d", c)
	}
}

// TestAPIAbortRevertsTraffic: with a 100% rollout rejecting under the tight
// canary, aborting instantly makes the generous old rule admit traffic.
func TestAPIAbortRevertsTraffic(t *testing.T) {
	h := newRouter(t)
	id := createRule(t, h, windowRuleBody("abort-http", 1000))
	if c, b := postJSON(t, h, "/api/rules/"+id+"/rollout",
		map[string]any{"percent": 100, "canary": windowRuleBody("abort-http", 1)}); c != http.StatusCreated {
		t.Fatalf("start: %d %v", c, b)
	}

	req := map[string]any{"client_id": "abort-c", "api_path": "/p"}
	if c, _ := postJSON(t, h, "/api/gateway/check", req); c != http.StatusOK {
		t.Fatal("first canary request allowed")
	}
	if c, _ := postJSON(t, h, "/api/gateway/check", req); c != http.StatusTooManyRequests {
		t.Fatal("tight canary (1) must reject the second")
	}

	if c, b := deleteJSON(t, h, "/api/rules/"+id+"/rollout"); c != http.StatusOK {
		t.Fatalf("abort: %d %v", c, b)
	}
	// instant revert: old quota (1000) admits; version no longer canary
	for i := 0; i < 10; i++ {
		if c, b := postJSON(t, h, "/api/gateway/check", req); c != http.StatusOK {
			t.Fatalf("after abort old rule must admit, got %d %v", c, b)
		} else if b["version"] == "canary" {
			t.Fatalf("after abort version must not be canary, got %v", b["version"])
		}
	}
}

// TestAPIReplaySplitsByVersion: replaying many distinct subjects shows traffic
// landing in both versions, and replaying the same batch is stable.
func TestAPIReplaySplitsByVersion(t *testing.T) {
	h := newRouter(t)
	id := createRule(t, h, windowRuleBody("split", 100000))
	if c, b := postJSON(t, h, "/api/rules/"+id+"/rollout",
		map[string]any{"percent": 50, "canary": windowRuleBody("split", 100000)}); c != http.StatusCreated {
		t.Fatalf("start: %d %v", c, b)
	}

	play := func() map[string]any {
		c, b := postJSON(t, h, "/api/tools/replay", map[string]any{
			"count": 400, "batch": 50, "client_id": "u", "api_path": "/p",
			"distinct_clients": 100,
		})
		if c != http.StatusOK {
			t.Fatalf("replay: %d %v", c, b)
		}
		return b
	}
	first := play()
	byVersion := first["by_version"].(map[string]any)
	if _, ok := byVersion["canary"]; !ok {
		t.Fatalf("expected traffic in both versions, got %v", byVersion)
	}
	if _, ok := byVersion["stable"]; !ok {
		t.Fatalf("expected stable traffic too, got %v", byVersion)
	}

	// Replaying the same batch must assign the exact same subjects identically:
	// derive the per-index version list and compare with a second replay.
	versionsOf := func(b map[string]any) []string {
		items := b["items"].([]any)
		out := make([]string, len(items))
		for i, it := range items {
			m := it.(map[string]any)
			v, _ := m["version"].(string)
			if v == "" {
				v = "stable"
			}
			out[i] = v
		}
		return out
	}
	a := versionsOf(first)
	b := versionsOf(play())
	if len(a) != len(b) {
		t.Fatal("replay length changed")
	}
	for i := range a {
		if a[i] != b[i] {
			t.Fatalf("subject at index %d assigned %s then %s; split must be stable across replays", i, a[i], b[i])
		}
	}
}

func mustJSON(v any) string {
	b, _ := json.Marshal(v)
	return string(b)
}
