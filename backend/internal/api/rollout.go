package api

import (
	"errors"
	"net/http"

	"github.com/gin-gonic/gin"

	"ratelimit-gateway/internal/store"
)

// rolloutPayload starts or reconfigures a gray rollout. Canary is a full rule
// body (the new content) and is required to start; percent is the share of
// traffic judged by it. On adjustment only percent is needed.
type rolloutPayload struct {
	// Percent is a pointer so an absent field is distinguishable from an
	// explicit 0 (which means "park the change, all traffic back to old").
	Percent *int         `json:"percent"`
	Canary  *rulePayload `json:"canary,omitempty"`
}

// rolloutView is what the dashboard reads: the percent plus both versions.
type rolloutView struct {
	RuleID  string      `json:"rule_id"`
	Percent int         `json:"percent"`
	Canary  interface{} `json:"canary"`
	Old     interface{} `json:"old"`
}

func (s *Server) listRollouts(c *gin.Context) {
	out := make([]gin.H, 0)
	for _, ro := range s.rollouts.List() {
		out = append(out, gin.H{
			"rule_id": ro.RuleID, "percent": ro.Percent,
			"canary": ro.Canary, "old": ro.Old,
		})
	}
	c.JSON(http.StatusOK, gin.H{"rollouts": out})
}

// startRollout begins a gray change: snapshot the live rule as "old" and store
// the submitted body as "canary" at the requested percent.
func (s *Server) startRollout(c *gin.Context) {
	id := c.Param("id")
	old, ok := s.rules.Get(id)
	if !ok {
		c.JSON(http.StatusNotFound, gin.H{"error": "rule not found"})
		return
	}
	var p rolloutPayload
	if err := decodeStrict(c, &p); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request body: " + err.Error()})
		return
	}
	if p.Percent == nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "percent is required and must be an integer 0..100"})
		return
	}
	if p.Canary == nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "starting a rollout requires the new rule content in \"canary\""})
		return
	}
	cnry, err := toModel(p.Canary)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	cnry.ID = id
	ro, err := s.rollouts.Start(c.Request.Context(), old, cnry, *p.Percent)
	if err != nil {
		writeRolloutErr(c, err)
		return
	}
	c.JSON(http.StatusCreated, gin.H{
		"rule_id": ro.RuleID, "percent": ro.Percent, "canary": ro.Canary, "old": ro.Old,
	})
}

// updateRollout changes the percent (10 -> 50 -> 100, or back to 0) and/or
// replaces the canary content. At 100% it does not auto-promote; the operator
// promotes explicitly once satisfied, and aborts to revert instantly.
func (s *Server) updateRollout(c *gin.Context) {
	id := c.Param("id")
	var p rolloutPayload
	if err := decodeStrict(c, &p); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request body: " + err.Error()})
		return
	}
	if _, ok := s.rollouts.Get(id); !ok {
		c.JSON(http.StatusNotFound, gin.H{"error": "no in-flight rollout for rule"})
		return
	}

	if p.Canary != nil {
		cnry, err := toModel(p.Canary)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
		cnry.ID = id
		if _, err := s.rollouts.UpdateCanary(c.Request.Context(), id, cnry); err != nil {
			writeRolloutErr(c, err)
			return
		}
	}

	if p.Percent == nil {
		ro, _ := s.rollouts.Get(id)
		c.JSON(http.StatusOK, gin.H{
			"rule_id": ro.RuleID, "percent": ro.Percent, "canary": ro.Canary, "old": ro.Old,
		})
		return
	}
	updated, err := s.rollouts.SetPercent(c.Request.Context(), id, *p.Percent)
	if err != nil {
		writeRolloutErr(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{
		"rule_id": updated.RuleID, "percent": updated.Percent,
		"canary": updated.Canary, "old": updated.Old,
	})
}

// abortRollout (DELETE) ends the change immediately: all traffic is judged by
// the old rule and the canary counters never influence a decision again.
func (s *Server) abortRollout(c *gin.Context) {
	id := c.Param("id")
	if err := s.rollouts.Abort(c.Request.Context(), id); err != nil {
		writeRolloutErr(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"aborted": id})
}

// promoteRollout finalizes a 100% rollout: the canary becomes the single rule.
func (s *Server) promoteRollout(c *gin.Context) {
	id := c.Param("id")
	r, err := s.rollouts.Promote(c.Request.Context(), s.rules, id)
	if err != nil {
		writeRolloutErr(c, err)
		return
	}
	c.JSON(http.StatusOK, r)
}

func writeRolloutErr(c *gin.Context, err error) {
	var inv store.ErrInvalid
	if errors.As(err, &inv) {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid rollout: " + inv.Error()})
		return
	}
	var nf store.ErrNotFound
	if errors.As(err, &nf) {
		c.JSON(http.StatusNotFound, gin.H{"error": nf.Error()})
		return
	}
	c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
}
