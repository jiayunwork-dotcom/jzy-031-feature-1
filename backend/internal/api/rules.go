package api

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"

	"ratelimit-gateway/internal/model"
	"ratelimit-gateway/internal/store"
)

// rulePayload is the wire representation. Levels are carried as a generic
// object so unknown level names can be rejected with a clear message.
type rulePayload struct {
	ID         string                    `json:"id,omitempty"`
	Name       string                    `json:"name"`
	Enabled    *bool                     `json:"enabled"`
	Algorithm  string                    `json:"algorithm"`
	Dimensions []string                  `json:"dimensions"`
	Levels     map[string]ruleLevelInput `json:"levels"`
	Matchers   map[string][]string       `json:"matchers"`
}

type ruleLevelInput struct {
	Threshold     int64 `json:"threshold"`
	Burst         int64 `json:"burst"`
	WindowSeconds int64 `json:"window_seconds"`
}

func toModel(p *rulePayload) (*model.Rule, error) {
	r := &model.Rule{
		ID:         p.ID,
		Name:       p.Name,
		Enabled:    true,
		Algorithm:  model.Algorithm(p.Algorithm),
		Dimensions: p.Dimensions,
		Levels:     map[model.Level]model.RuleLevel{},
		Matchers:   p.Matchers,
	}
	if p.Enabled != nil {
		r.Enabled = *p.Enabled
	}
	for lv, in := range p.Levels {
		level := model.Level(lv)
		switch level {
		case model.LevelGlobal, model.LevelGroup, model.LevelAPI, model.LevelClient:
		default:
			return nil, errors.New("unknown quota level: " + lv)
		}
		r.Levels[level] = model.RuleLevel{
			Threshold:     in.Threshold,
			Burst:         in.Burst,
			WindowSeconds: in.WindowSeconds,
		}
	}
	return r, nil
}

func (s *Server) listRules(c *gin.Context) {
	c.JSON(http.StatusOK, s.rules.List())
}

func (s *Server) getRule(c *gin.Context) {
	r, ok := s.rules.Get(c.Param("id"))
	if !ok {
		c.JSON(http.StatusNotFound, gin.H{"error": "rule not found"})
		return
	}
	c.JSON(http.StatusOK, r)
}

func (s *Server) createRule(c *gin.Context) {
	var p rulePayload
	if err := decodeStrict(c, &p); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	r, err := toModel(&p)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	created, err := s.rules.Create(c.Request.Context(), r)
	if err != nil {
		writeRuleErr(c, err)
		return
	}
	c.JSON(http.StatusCreated, created)
}

func (s *Server) updateRule(c *gin.Context) {
	var p rulePayload
	if err := decodeStrict(c, &p); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	p.ID = c.Param("id")
	r, err := toModel(&p)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	// While a gray rollout is in flight the live rule is the frozen "old"
	// snapshot; the operator must change the canary (or abort) rather than
	// silently moving the baseline the rollout compares against.
	if ro, ok := s.rollouts.Get(p.ID); ok {
		c.JSON(http.StatusConflict, gin.H{
			"error": "rule has an in-flight rollout at " +
				strconv.Itoa(ro.Percent) + "%; adjust the canary, promote it at 100%, or abort it before editing the live rule",
		})
		return
	}
	updated, err := s.rules.Update(c.Request.Context(), r)
	if err != nil {
		writeRuleErr(c, err)
		return
	}
	c.JSON(http.StatusOK, updated)
}

func (s *Server) deleteRule(c *gin.Context) {
	if err := s.rules.Delete(c.Request.Context(), c.Param("id")); err != nil {
		writeRuleErr(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"deleted": c.Param("id")})
}

func writeRuleErr(c *gin.Context, err error) {
	var inv store.ErrInvalid
	if errors.As(err, &inv) {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid rule: " + inv.Error()})
		return
	}
	var nf store.ErrNotFound
	if errors.As(err, &nf) {
		c.JSON(http.StatusNotFound, gin.H{"error": nf.Error()})
		return
	}
	c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
}

// decodeStrict rejects unknown fields so e.g. a misspelled algorithm can never
// be silently dropped and let the gateway run with a bad rule.
func decodeStrict(c *gin.Context, dst any) error {
	dec := json.NewDecoder(c.Request.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		return err
	}
	return nil
}
