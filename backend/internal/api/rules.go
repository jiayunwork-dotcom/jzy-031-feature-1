package api

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/gin-gonic/gin"

	"ratelimit-gateway/internal/canary"
	"ratelimit-gateway/internal/model"
	"ratelimit-gateway/internal/store"
)

// rulePayload is the wire representation. Levels are carried as a generic
// object so unknown level names can be rejected with a clear message.
type rulePayload struct {
	ID        string                    `json:"id,omitempty"`
	Name      string                    `json:"name"`
	Enabled   *bool                     `json:"enabled"`
	Algorithm string                    `json:"algorithm"`
	// RolloutPercent optionally starts a gray rollout when updating an
	// existing rule: 1..99 splits traffic, a missing/null field means a plain
	// edit. 0 and 100 are not accepted here (use the dedicated rollout
	// endpoints to abort / promote).
	RolloutPercent *int                     `json:"rollout_percent,omitempty"`
	Dimensions     []string                 `json:"dimensions"`
	Levels         map[string]ruleLevelInput `json:"levels"`
	Matchers       map[string][]string       `json:"matchers"`
}

type ruleLevelInput struct {
	Threshold     int64 `json:"threshold"`
	Burst         int64 `json:"burst"`
	WindowSeconds int64 `json:"window_seconds"`
}

// ruleDTO pairs the rule with its in-progress rollout for the UI.
type ruleDTO struct {
	*model.Rule
	Rollout *model.Rollout `json:"rollout,omitempty"`
}

func (s *Server) toDTO(r *model.Rule) ruleDTO {
	dto := ruleDTO{Rule: r}
	if rl, ok := s.rules.Rollout(r.ID); ok {
		dto.Rollout = rl
	}
	return dto
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
	rs := s.rules.List()
	out := make([]ruleDTO, 0, len(rs))
	for _, r := range rs {
		out = append(out, s.toDTO(r))
	}
	c.JSON(http.StatusOK, out)
}

func (s *Server) getRule(c *gin.Context) {
	r, ok := s.rules.Get(c.Param("id"))
	if !ok {
		c.JSON(http.StatusNotFound, gin.H{"error": "rule not found"})
		return
	}
	c.JSON(http.StatusOK, s.toDTO(r))
}

func (s *Server) createRule(c *gin.Context) {
	var p rulePayload
	if err := decodeStrict(c, &p); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if p.RolloutPercent != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "rollout_percent can only be set when updating an existing rule; create the rule first, then gray-release an edit"})
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
	c.JSON(http.StatusCreated, s.toDTO(created))
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

	// Gray release: validate the new content against the SAME rules as a
	// normal rule before any traffic can be evaluated by it.
	if p.RolloutPercent != nil {
		if _, ok := s.rules.Get(p.ID); !ok {
			c.JSON(http.StatusNotFound, gin.H{"error": "rule not found"})
			return
		}
		rl, err := s.rules.StartRollout(c.Request.Context(), p.ID, r, *p.RolloutPercent)
		if err != nil {
			writeRuleErr(c, err)
			return
		}
		stored, _ := s.rules.Get(p.ID)
		c.JSON(http.StatusOK, gin.H{
			"rule":    s.toDTO(stored),
			"rollout": rl,
		})
		return
	}

	updated, err := s.rules.Update(c.Request.Context(), r)
	if err != nil {
		writeRuleErr(c, err)
		return
	}
	c.JSON(http.StatusOK, s.toDTO(updated))
}

// rolloutReq adjusts an in-progress rollout: percent 1..99 resizes the canary
// bucket, 100 promotes, 0 aborts.
type rolloutReq struct {
	Percent *int `json:"percent"`
}

func (s *Server) setRollout(c *gin.Context) {
	var req rolloutReq
	if err := decodeStrict(c, &req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if req.Percent == nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "percent is required and must be an integer between 0 and 100"})
		return
	}
	id := c.Param("id")
	if err := s.rules.SetRolloutPercent(c.Request.Context(), id, *req.Percent); err != nil {
		writeRuleErr(c, err)
		return
	}
	r, ok := s.rules.Get(id)
	if !ok {
		c.JSON(http.StatusNotFound, gin.H{"error": "rule not found"})
		return
	}
	c.JSON(http.StatusOK, s.toDTO(r))
}

func (s *Server) abortRollout(c *gin.Context) {
	if err := s.rules.AbortRollout(c.Request.Context(), c.Param("id")); err != nil {
		writeRuleErr(c, err)
		return
	}
	r, ok := s.rules.Get(c.Param("id"))
	if !ok {
		c.JSON(http.StatusNotFound, gin.H{"error": "rule not found"})
		return
	}
	c.JSON(http.StatusOK, s.toDTO(r))
}

// rolloutPreview answers which version a subject gets at a given percentage,
// without consuming any quota — useful for the UI and for debugging "why did
// these two clients see different behavior".
type rolloutPreviewReq struct {
	Percent  *int   `json:"percent"`
	ClientID string `json:"client_id"`
	APIPath  string `json:"api_path"`
	Group    string `json:"group"`
}

func (s *Server) rolloutPreview(c *gin.Context) {
	r, ok := s.rules.Get(c.Param("id"))
	if !ok {
		c.JSON(http.StatusNotFound, gin.H{"error": "rule not found"})
		return
	}
	var req rolloutPreviewReq
	if c.Request.ContentLength > 0 {
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
	}
	percent := 0
	if req.Percent != nil {
		percent = *req.Percent
	} else if rl, has := s.rules.Rollout(r.ID); has {
		percent = rl.Percent
	}
	if err := model.ValidatePercent(percent); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	rctx := model.RequestContext{ClientID: req.ClientID, APIPath: req.APIPath, Group: req.Group}
	subject := canary.Subject(r, rctx)
	bucket := canary.Bucket(subject)
	c.JSON(http.StatusOK, gin.H{
		"subject": subject,
		"bucket":  bucket,
		"percent": percent,
		"version": canary.SelectVersion(r, percent, rctx),
	})
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
	var conflict store.ErrConflict
	if errors.As(err, &conflict) {
		c.JSON(http.StatusConflict, gin.H{"error": conflict.Error()})
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
