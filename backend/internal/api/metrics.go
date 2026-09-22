package api

import (
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
)

func (s *Server) timeSeries(c *gin.Context) {
	ruleID := c.Query("rule_id")
	version := c.DefaultQuery("version", "stable")
	if version != "stable" && version != "canary" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "version must be \"stable\" or \"canary\""})
		return
	}
	secs, _ := strconv.ParseInt(c.DefaultQuery("seconds", "60"), 10, 64)
	if secs <= 0 || secs > 3600 {
		secs = 60
	}
	pts, err := s.rec.TimeSeries(c.Request.Context(), ruleID, version, secs)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"points": pts})
}

func (s *Server) recentDenies(c *gin.Context) {
	ruleID := c.Query("rule_id")
	version := c.Query("version") // "" = both versions
	if version != "" && version != "stable" && version != "canary" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "version must be \"stable\" or \"canary\""})
		return
	}
	limit, _ := strconv.ParseInt(c.DefaultQuery("limit", "20"), 10, 64)
	evs, err := s.rec.RecentDenies(c.Request.Context(), ruleID, version, limit)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"denies": evs})
}

func (s *Server) overview(c *gin.Context) {
	secs := int64(30)
	pts, err := s.rec.TimeSeries(c.Request.Context(), "_all", "stable", secs)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	var lastAllow, lastDeny int64
	if len(pts) > 0 {
		last := pts[len(pts)-1]
		lastAllow, lastDeny = last.Allow, last.Deny
	}
	c.JSON(http.StatusOK, gin.H{
		"rules":         s.rules.List(),
		"rollouts":      s.rollouts.List(),
		"allow_per_sec": lastAllow,
		"deny_per_sec":  lastDeny,
		"points":        pts,
	})
}
