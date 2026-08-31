package handler

import (
	"context"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
)

// Pinger reports database reachability.
type Pinger interface {
	PingContext(ctx context.Context) error
}

// Health serves liveness and readiness probes.
type Health struct {
	db Pinger
}

// NewHealth builds the health handler.
func NewHealth(db Pinger) *Health {
	return &Health{db: db}
}

// Live reports process liveness.
//
// @Summary Liveness probe
// @Tags health
// @Produce json
// @Success 200 {object} map[string]string
// @Router /health/live [get]
func (h *Health) Live(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"status": "ok"})
}

// Ready reports readiness: the database must respond to a ping.
//
// @Summary Readiness probe
// @Tags health
// @Produce json
// @Success 200 {object} map[string]string
// @Failure 503 {object} map[string]string
// @Router /health/ready [get]
func (h *Health) Ready(c *gin.Context) {
	ctx, cancel := context.WithTimeout(c.Request.Context(), 2*time.Second)
	defer cancel()
	if err := h.db.PingContext(ctx); err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"status": "unavailable"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"status": "ok"})
}
