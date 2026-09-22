package api

import (
	"context"
	"net/http"
	"sync"
	"time"

	"github.com/gin-gonic/gin"

	"ratelimit-gateway/internal/canary"
	"ratelimit-gateway/internal/model"
)

// replayReq replays / generates traffic from the browser without having to
// hand-craft hundreds of POSTs. Requests are fired in batches of `batch`
// (so batch=N means an N-wide instantaneous burst), with `interval_ms`
// between batches, and every verdict is returned with its timestamp so the
// UI can draw the admission sequence.
//
// To make gray-rollout splits visible, clients may give a `clients` list
// (or a clients_prefix + clients_count range): each batch round-robins
// through the distinct subjects so the canary bucket assignment is stable
// per subject and both versions accumulate on their own curves.
type replayReq struct {
	Count      int      `json:"count"`
	Batch      int      `json:"batch"`
	IntervalMs int      `json:"interval_ms"`
	ClientID   string   `json:"client_id"`
	APIPath    string   `json:"api_path"`
	Group      string   `json:"group"`
	Clients    []string `json:"clients"`
}

type replayItem struct {
	Seq      int    `json:"seq"`
	Allowed  bool   `json:"allowed"`
	TimeMs   int64  `json:"time_ms"`
	ClientID string `json:"client_id,omitempty"`
	RuleID   string `json:"rule_id,omitempty"`
	Version  string `json:"version,omitempty"`
	Level    string `json:"level,omitempty"`
	Reason   string `json:"reason,omitempty"`
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
	clients := req.Clients
	if len(clients) == 0 {
		if req.ClientID != "" {
			clients = []string{req.ClientID}
		}
	}

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
			clientID := req.ClientID
			if len(clients) > 0 {
				clientID = clients[i%len(clients)]
			}
			rctx := model.RequestContext{ClientID: clientID, APIPath: req.APIPath, Group: req.Group}
			go func() {
				defer wg.Done()
				v, err := s.checker.Decide(context.Background(), rctx)
				item := replayItem{Seq: i, TimeMs: time.Now().UnixMilli(), ClientID: clientID}
				if err != nil {
					item.Reason = err.Error()
				} else {
					item.Allowed = v.Allowed
					item.RuleID = v.RuleID
					item.Version = string(v.Version)
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

	// On an allowed request the top-level version is empty (it is carried
	// per rule in results); recover the version of the first matching rule
	// so the replay cells can be colored by version.
	fillAllowedVersions(results, req, s)

	var allowed, denied int
	byVersion := map[string]map[string]int{}
	for _, r := range results {
		if r.Allowed {
			allowed++
		} else {
			denied++
		}
		v := r.Version
		if v == "" {
			v = "_"
		}
		if byVersion[v] == nil {
			byVersion[v] = map[string]int{}
		}
		if r.Allowed {
			byVersion[v]["allow"]++
		} else {
			byVersion[v]["deny"]++
		}
	}
	c.JSON(http.StatusOK, gin.H{
		"total": len(results), "allowed": allowed, "denied": denied,
		"by_version": byVersion,
		"items":      results,
	})
}

// fillAllowedVersions annotates allowed replay items with the version of the
// first rule that evaluated them, so the UI can show new/old split even when
// nothing was rejected.
func fillAllowedVersions(results []replayItem, req replayReq, s *Server) {
	for i := range results {
		if results[i].Version != "" {
			continue
		}
		clients := req.Clients
		if len(clients) == 0 && req.ClientID != "" {
			clients = []string{req.ClientID}
		}
		clientID := req.ClientID
		if len(clients) > 0 {
			clientID = clients[i%len(clients)]
		}
		rctx := model.RequestContext{ClientID: clientID, APIPath: req.APIPath, Group: req.Group}
		for _, r := range s.rules.List() {
			if !r.Enabled {
				continue
			}
			rl, has := s.rules.Rollout(r.ID)
			if !has {
				continue
			}
			eff := r
			ver := canary.VersionOld
			if canary.SelectVersion(r, rl.Percent, rctx) == canary.VersionNew {
				eff = &rl.NewRule
				ver = canary.VersionNew
			}
			if eff.Enabled && s.match.Matches(eff, rctx) {
				results[i].Version = string(ver)
				break
			}
		}
	}
}
