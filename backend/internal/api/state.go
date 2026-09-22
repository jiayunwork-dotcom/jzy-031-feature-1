package api

import (
	"context"
	"net/http"
	"sort"
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"

	"ratelimit-gateway/internal/canary"
	"ratelimit-gateway/internal/model"
)

// redisStateReader is the subset of *redis.Client the state endpoint needs.
type redisStateReader interface {
	Scan(ctx context.Context, cursor uint64, match string, count int64) *redis.ScanCmd
	HGetAll(ctx context.Context, key string) *redis.MapStringStringCmd
	ZCard(ctx context.Context, key string) *redis.IntCmd
}

// activeKey describes one live bucket/window for the dashboard.
type activeKey struct {
	Level   string `json:"level"`
	Version string `json:"version"`
	Key     string `json:"key"`
	Remaining int64 `json:"remaining"`
	ResetInMs int64 `json:"reset_in_ms"`
	Kind    string `json:"kind"`
}

// ruleState scans Redis for a rule's active buckets and peeks each one.
// Query params client_id / api_path / group let the dashboard pick which
// concrete dimension combination to read. During a gray rollout both
// versions' levels and active keys are returned separately.
func (s *Server) ruleState(c *gin.Context) {
	id := c.Param("id")
	rule, ok := s.rules.Get(id)
	if !ok {
		c.JSON(http.StatusNotFound, gin.H{"error": "rule not found"})
		return
	}
	rctx := model.RequestContext{
		ClientID: c.Query("client_id"),
		APIPath:  c.DefaultQuery("api_path", "/"),
		Group:    c.Query("group"),
	}

	resp := gin.H{
		"rule": s.toDTO(rule),
		"selected": gin.H{
			"old": s.checker.PeekRule(c.Request.Context(), rule, "", rctx),
		},
	}
	if _, has := s.rules.Rollout(id); has {
		resp["selected"].(gin.H)["new"] = s.checker.PeekRule(c.Request.Context(), rule, canary.VersionNew, rctx)
	}

	// Plus enumerate all active physical keys of this rule from Redis,
	// including the canary namespace.
	live, err := s.scanActive(c.Request.Context(), id, rule)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	resp["active_keys"] = live

	c.JSON(http.StatusOK, resp)
}

func (s *Server) scanActive(ctx context.Context, ruleID string, rule *model.Rule) ([]activeKey, error) {
	pattern := "rl:{" + ruleID + "}:*"
	var (
		cursor uint64
		keys   []string
	)
	for {
		batch, next, err := s.redis.Scan(ctx, cursor, pattern, 200).Result()
		if err != nil {
			return nil, err
		}
		keys = append(keys, batch...)
		cursor = next
		if cursor == 0 {
			break
		}
	}
	sort.Strings(keys)

	out := make([]activeKey, 0, len(keys))
	for _, k := range keys {
		ak := activeKey{Key: k, Kind: string(rule.Algorithm)}
		// key forms:
		//   rl:{id}:<level>:...          -> old/canonical
		//   rl:{id}:canary:<level>:...   -> new
		rest := strings.TrimPrefix(k, "rl:{"+ruleID+"}:")
		if strings.HasPrefix(rest, "canary:") {
			ak.Version = "new"
			rest = strings.TrimPrefix(rest, "canary:")
		} else {
			ak.Version = "old"
		}
		if i := strings.IndexByte(rest, ':'); i >= 0 {
			ak.Level = rest[:i]
		}
		cfgRule := rule
		if rl, has := s.rules.Rollout(ruleID); has && ak.Version == "new" {
			cfgRule = &rl.NewRule
		}
		switch cfgRule.Algorithm {
		case model.AlgoSlidingLog:
			n, err := s.redis.ZCard(ctx, k).Result()
			if err == nil {
				cfg := cfgRule.Levels[model.Level(ak.Level)]
				ak.Remaining = cfg.Threshold - n
			}
		default:
			m, err := s.redis.HGetAll(ctx, k).Result()
			if err == nil {
				ak.Remaining = interpretRemaining(cfgRule, ak.Level, m)
			}
		}
		out = append(out, ak)
	}
	return out, nil
}

func interpretRemaining(rule *model.Rule, level string, m map[string]string) int64 {
	cfg, ok := rule.Levels[model.Level(level)]
	if !ok {
		return 0
	}
	switch rule.Algorithm {
	case model.AlgoTokenBucket:
		return int64(atof(m["tokens"]))
	case model.AlgoLeakyBucket:
		var f float64
		if v, ok := m["level"]; ok {
			f = atof(v)
		}
		return cfg.Burst - int64(f+0.999999)
	case model.AlgoFixedWindow:
		return cfg.Threshold - int64(atof(m["c"]))
	case model.AlgoSlidingWindow:
		return cfg.Threshold - int64(atof(m["ca"])) - int64(atof(m["cb"]))
	}
	return 0
}

func atof(v string) float64 {
	if v == "" {
		return 0
	}
	f, _ := strconv.ParseFloat(v, 64)
	return f
}
