// Package api exposes the HTTP surface: the gateway ingress that performs
// real rate-limit decisions, rule CRUD, and the live observability API.
package api

import (
	"net/http"

	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"

	"ratelimit-gateway/internal/config"
	"ratelimit-gateway/internal/engine"
	"ratelimit-gateway/internal/matcher"
	"ratelimit-gateway/internal/model"
	"ratelimit-gateway/internal/quota"
	"ratelimit-gateway/internal/stats"
	"ratelimit-gateway/internal/store"
)

type Server struct {
	cfg      config.Config
	rules    *store.RuleStore
	rollouts *store.RolloutStore
	engines  *engine.Manager
	checker  *quota.Checker
	rec      *stats.Recorder
	match    *matcher.Matcher
	redis    redisStateReader
}

// NewServer wires the router. The redis client is passed as an interface so
// the state endpoints can SCAN keys.
func NewServer(cfg config.Config, rules *store.RuleStore, rollouts *store.RolloutStore,
	engines *engine.Manager, rec *stats.Recorder, checker *quota.Checker, rsr redisStateReader) *Server {
	s := &Server{
		cfg: cfg, rules: rules, rollouts: rollouts, engines: engines, rec: rec,
		checker: checker, match: matcher.New(), redis: rsr,
	}
	return s
}

func (s *Server) Router() *gin.Engine {
	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	r.Use(gin.Recovery())
	r.Use(cors.New(cors.Config{
		AllowAllOrigins: true,
		AllowMethods:    []string{"GET", "POST", "PUT", "DELETE", "OPTIONS"},
		AllowHeaders:    []string{"*"},
	}))

	api := r.Group("/api")
	{
		api.GET("/health", s.health)

		// rule configuration
		api.GET("/rules", s.listRules)
		api.POST("/rules", s.createRule)
		api.GET("/rules/:id", s.getRule)
		api.PUT("/rules/:id", s.updateRule)
		api.DELETE("/rules/:id", s.deleteRule)

		// gray rollout (canary) control
		api.GET("/rollouts", s.listRollouts)
		api.POST("/rules/:id/rollout", s.startRollout)
		api.PUT("/rules/:id/rollout", s.updateRollout)
		api.POST("/rules/:id/rollout/promote", s.promoteRollout)
		api.DELETE("/rules/:id/rollout", s.abortRollout)

		// the real gateway ingress: a decision per request
		api.POST("/gateway/check", s.gatewayCheck)

		// observability
		api.GET("/metrics/timeseries", s.timeSeries)
		api.GET("/metrics/denies", s.recentDenies)
		api.GET("/rules/:id/state", s.ruleState)
		api.GET("/overview", s.overview)

		// traffic replay / load generation from the browser
		api.POST("/tools/replay", s.replay)
	}
	return r
}

func (s *Server) health(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"status": "ok"})
}

// gatewayRequest is what the browser (or any client) posts per request.
type gatewayRequest struct {
	ClientID string `json:"client_id"`
	APIPath  string `json:"api_path"`
	Group    string `json:"group"`
}

func (s *Server) gatewayCheck(c *gin.Context) {
	var req gatewayRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request body: " + err.Error()})
		return
	}
	if req.APIPath == "" {
		req.APIPath = "/"
	}
	v, err := s.checker.Decide(c.Request.Context(), model.RequestContext{
		ClientID: req.ClientID, APIPath: req.APIPath, Group: req.Group,
	})
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	if v.Allowed {
		c.JSON(http.StatusOK, gin.H{
			"allowed": true,
			"version": v.Version,
			"results": v.Results,
		})
		return
	}
	c.JSON(http.StatusTooManyRequests, gin.H{
		"allowed":   false,
		"rule_id":   v.RuleID,
		"rule_name": v.RuleName,
		"level":     string(v.Level),
		"version":   v.Version,
		"reason":    v.Reason,
		"results":   v.Results,
	})
}
