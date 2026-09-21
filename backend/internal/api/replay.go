package api

import (
	"context"
	"net/http"
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
}

type replayItem struct {
	Seq     int    `json:"seq"`
	Allowed bool   `json:"allowed"`
	TimeMs  int64  `json:"time_ms"`
	RuleID  string `json:"rule_id,omitempty"`
	Level   string `json:"level,omitempty"`
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

	rctx := model.RequestContext{ClientID: req.ClientID, APIPath: req.APIPath, Group: req.Group}
	results := make([]replayItem, req.Count)

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
				v, err := s.checker.Decide(context.Background(), rctx)
				item := replayItem{Seq: i, TimeMs: time.Now().UnixMilli()}
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
	for _, r := range results {
		if r.Allowed {
			allowed++
		} else {
			denied++
		}
	}
	c.JSON(http.StatusOK, gin.H{
		"total": len(results), "allowed": allowed, "denied": denied,
		"items": results,
	})
}
