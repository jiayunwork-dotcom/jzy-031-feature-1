package api

import (
	"context"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/gin-gonic/gin"

	"ratelimit-gateway/internal/model"
)

// replayReq replays / generates traffic from the browser without having to
// hand-craft hundreds of POSTs. Requests are fired in batches of `batch`
// (so batch=N means an N-wide instantaneous burst), with `interval_ms`
// between batches, and every verdict is returned with its timestamp so the
// UI can draw the admission sequence.
type replayReq struct {
	Count      int    `json:"count"`
	Batch      int    `json:"batch"`
	IntervalMs int    `json:"interval_ms"`
	ClientID   string `json:"client_id"`
	APIPath    string `json:"api_path"`
	Group      string `json:"group"`
	// DistinctClients > 0 spreads the load across that many deterministic
	// subjects (client-0..client-N-1) instead of hammering one client, so a
	// gray rollout's proportional split is directly observable. The mapping
	// is stable: replaying the same batch assigns the same subjects to the
	// same versions.
	DistinctClients int `json:"distinct_clients"`
}

type replayItem struct {
	Seq     int    `json:"seq"`
	Allowed bool   `json:"allowed"`
	TimeMs  int64  `json:"time_ms"`
	RuleID  string `json:"rule_id,omitempty"`
	Level   string `json:"level,omitempty"`
	Version string `json:"version,omitempty"`
	Reason  string `json:"reason,omitempty"`
}

func (s *Server) replay(c *gin.Context) {
	var req replayReq
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if req.Count <= 0 || req.Count > 2000 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "count must be in 1..2000"})
		return
	}
	if req.Batch <= 0 {
		req.Batch = 1
	}
	if req.APIPath == "" {
		req.APIPath = "/"
	}

	base := model.RequestContext{ClientID: req.ClientID, APIPath: req.APIPath, Group: req.Group}
	if req.DistinctClients < 0 {
		req.DistinctClients = 0
	}
	results := make([]replayItem, req.Count)

	rctxFor := func(i int) model.RequestContext {
		if req.DistinctClients <= 0 {
			return base
		}
		rc := base
		// Deterministic spread across N subjects; the same index always maps
		// to the same client, so replays are reproducible.
		rc.ClientID = req.ClientID + "-" + strconv.Itoa(i%req.DistinctClients)
		return rc
	}

	for start := 0; start < req.Count; start += req.Batch {
		n := req.Batch
		if start+n > req.Count {
			n = req.Count - start
		}
		var wg sync.WaitGroup
		wg.Add(n)
		for j := 0; j < n; j++ {
			i := start + j
			go func() {
				defer wg.Done()
				v, err := s.checker.Decide(context.Background(), rctxFor(i))
				item := replayItem{Seq: i, TimeMs: time.Now().UnixMilli(), Version: v.Version}
				if err != nil {
					item.Reason = err.Error()
				} else {
					item.Allowed = v.Allowed
					item.RuleID = v.RuleID
					item.Level = string(v.Level)
					item.Reason = v.Reason
				}
				results[i] = item
			}()
		}
		wg.Wait()
		if start+n < req.Count && req.IntervalMs > 0 {
			time.Sleep(time.Duration(req.IntervalMs) * time.Millisecond)
		}
	}

	var allowed, denied int
	versionTotals := map[string]map[string]int{}
	for _, r := range results {
		v := r.Version
		if v == "" {
			v = "stable"
		}
		if versionTotals[v] == nil {
			versionTotals[v] = map[string]int{}
		}
		if r.Allowed {
			allowed++
			versionTotals[v]["allowed"]++
		} else {
			denied++
			versionTotals[v]["denied"]++
		}
	}
	c.JSON(http.StatusOK, gin.H{
		"total": len(results), "allowed": allowed, "denied": denied,
		"by_version": versionTotals,
		"items":      results,
	})
}
