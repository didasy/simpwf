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

// ConsumerMonitor reports whether one broker input consumer is healthy. It
// is implemented by *inputtransport.ConsumerStatus; the narrow interface
// keeps handler from importing inputtransport.
type ConsumerMonitor interface {
	Healthy() bool
}

// Health serves liveness and readiness probes.
type Health struct {
	db        Pinger
	consumers map[string]ConsumerMonitor
}

// NewHealth builds the health handler.
func NewHealth(db Pinger) *Health {
	return &Health{db: db}
}

// WithConsumers registers broker input consumer monitors for the readiness
// gate. It returns h for chaining at the NewHealth call site.
func (h *Health) WithConsumers(monitors map[string]ConsumerMonitor) *Health {
	h.consumers = monitors
	return h
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

// Ready reports readiness: the database must respond to a ping and every
// registered broker input consumer must be healthy.
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
	dbOK := h.db.PingContext(ctx) == nil
	consumersOK := true
	for _, m := range h.consumers {
		if !m.Healthy() {
			consumersOK = false
			break
		}
	}
	if dbOK && consumersOK {
		c.JSON(http.StatusOK, gin.H{"status": "ok"})
		return
	}
	if len(h.consumers) == 0 {
		c.JSON(http.StatusServiceUnavailable, gin.H{"status": "unavailable"})
		return
	}
	// Names + health flags only, never error text: dial errors can embed
	// DSN credentials.
	detail := make(map[string]any, len(h.consumers))
	for name, m := range h.consumers {
		detail[name] = gin.H{"healthy": m.Healthy()}
	}
	c.JSON(http.StatusServiceUnavailable, gin.H{"status": "unavailable", "consumers": detail})
}
