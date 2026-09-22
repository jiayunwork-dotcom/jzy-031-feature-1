package api_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"ratelimit-gateway/internal/api"
	"ratelimit-gateway/internal/quota"
	"ratelimit-gateway/internal/testboot"
)

func newRouter(t *testing.T) http.Handler {
	t.Helper()
	rdb, rules, rollouts, mgr, rec := testboot.FullStack(t)
	checker := quota.New(rules, rollouts, mgr, rdb, rec)
	return api.NewServer(testCfg(), rules, rollouts, mgr, rec, checker, rdb).Router()
}

func postJSON(t *testing.T, h http.Handler, path string, body any) (int, map[string]any) {
	t.Helper()
	raw, _ := json.Marshal(body)
	req := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(raw))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	var out map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &out)
	return w.Code, out
}

func getJSON(t *testing.T, h http.Handler, path string) (int, map[string]any) {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, path, nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	var out map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &out)
	return w.Code, out
}

// TestGatewayEndToEnd: create a token-bucket rule, send a burst through the
// real HTTP ingress; capacity requests get 200 and the next gets 429 naming
// the rule and level.
func TestGatewayEndToEnd(t *testing.T) {
	h := newRouter(t)

	rule := map[string]any{
		"name": "http-burst", "enabled": true, "algorithm": "token_bucket",
		"dimensions": []string{"client_id"},
		"levels": map[string]any{
			"client": map[string]any{"threshold": 10, "burst": 3},
		},
	}
	code, body := postJSON(t, h, "/api/rules", rule)
	if code != http.StatusCreated {
		t.Fatalf("create rule: %d %v", code, body)
	}

	req := map[string]any{"client_id": "c1", "api_path": "/x"}
	for i := 0; i < 3; i++ {
		if c, b := postJSON(t, h, "/api/gateway/check", req); c != http.StatusOK {
			t.Fatalf("burst request %d: status %d body %v", i+1, c, b)
		}
	}
	c, b := postJSON(t, h, "/api/gateway/check", req)
	if c != http.StatusTooManyRequests {
		t.Fatalf("4th request must be 429, got %d", c)
	}
	if b["level"] != "client" || b["rule_name"] != "http-burst" {
		t.Fatalf("429 must name level=client and rule, got %v", b)
	}
	if reason, _ := b["reason"].(string); reason == "" {
		t.Fatal("429 must include a human-readable reason")
	}
}

// TestAPIRejectsInvalidRule ensures bad payloads get 400 with a reason.
func TestAPIRejectsInvalidRule(t *testing.T) {
	h := newRouter(t)
	rule := map[string]any{
		"name": "bad", "algorithm": "totally_made_up",
		"dimensions": []string{"client_id"},
		"levels":     map[string]any{"client": map[string]any{"threshold": 1, "burst": 1}},
	}
	c, b := postJSON(t, h, "/api/rules", rule)
	if c != http.StatusBadRequest {
		t.Fatalf("unknown algorithm must 400, got %d", c)
	}
	if _, ok := b["error"].(string); !ok {
		t.Fatalf("400 must carry an error reason, got %v", b)
	}
}

// TestAPIRejectsUnknownLevel rejects a typo in the level name.
func TestAPIRejectsUnknownLevel(t *testing.T) {
	h := newRouter(t)
	rule := map[string]any{
		"name": "bad-level", "algorithm": "token_bucket",
		"dimensions": []string{"client_id"},
		"levels":     map[string]any{"tenant": map[string]any{"threshold": 1, "burst": 1}},
	}
	if c, b := postJSON(t, h, "/api/rules", rule); c != http.StatusBadRequest {
		t.Fatalf("unknown level must 400, got %d %v", c, b)
	}
}

// TestReplayEndpoint returns per-request verdicts usable to draw sequences.
func TestReplayEndpoint(t *testing.T) {
	h := newRouter(t)
	postJSON(t, h, "/api/rules", map[string]any{
		"name": "replay", "algorithm": "fixed_window",
		"dimensions": []string{"client_id"},
		"levels":     map[string]any{"client": map[string]any{"threshold": 5, "window_seconds": 10}},
	})
	c, b := postJSON(t, h, "/api/tools/replay", map[string]any{
		"count": 12, "batch": 12, "client_id": "c1", "api_path": "/p",
	})
	if c != http.StatusOK {
		t.Fatalf("replay: %d %v", c, b)
	}
	if b["allowed"].(float64) != 5 || b["denied"].(float64) != 7 {
		t.Fatalf("replay totals wrong: %v", b)
	}
	items := b["items"].([]any)
	if len(items) != 12 {
		t.Fatalf("replay must return every verdict, got %d", len(items))
	}
}
