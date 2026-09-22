package api

import (
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
)

func (s *Server) timeSeries(c *gin.Context) {
	ruleID := c.Query("rule_id")
	version := canonicalVersion(c.Query("version"))
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
	version := canonicalVersion(c.Query("version"))
	limit, _ := strconv.ParseInt(c.DefaultQuery("limit", "20"), 10, 64)
	evs, err := s.rec.RecentDenies(c.Request.Context(), ruleID, version, limit)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"denies": evs})
}

// canonicalVersion maps the query parameter to the stats key namespace:
// ""/"all" -> the plain aggregate series, "old"/"new" -> a rollout side.
func canonicalVersion(v string) string {
	switch v {
	case "old", "new":
		return v
	default:
		return ""
	}
}

func (s *Server) overview(c *gin.Context) {
	secs := int64(30)
	pts, err := s.rec.TimeSeries(c.Request.Context(), "_all", "", secs)
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
		"allow_per_sec": lastAllow,
		"deny_per_sec":  lastDeny,
		"points":        pts,
	})
}
