package handler

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
)

// stubMonitor stands in for *inputtransport.ConsumerStatus. err carries a
// planted marker proving the 503 body never echoes error text.
type stubMonitor struct {
	healthy bool
	err     string
}

func (s stubMonitor) Healthy() bool { return s.healthy }

func readyRequest(t *testing.T, h *Health) *httptest.ResponseRecorder {
	t.Helper()
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodGet, "/health/ready", nil)
	h.Ready(c)
	return w
}

func TestReadyConsumersOK(t *testing.T) {
	h := NewHealth(fakePinger{}).WithConsumers(map[string]ConsumerMonitor{
		"redis":    stubMonitor{healthy: true},
		"rabbitmq": stubMonitor{healthy: true},
	})
	w := readyRequest(t, h)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	if got := w.Body.String(); got != `{"status":"ok"}` {
		t.Errorf("body = %s, want exact %s", got, `{"status":"ok"}`)
	}
}

func TestReadyConsumerDown(t *testing.T) {
	h := NewHealth(fakePinger{}).WithConsumers(map[string]ConsumerMonitor{
		"redis":    stubMonitor{healthy: false, err: "CREDENTIAL-MARKER-amqp://user:secret@host/"},
		"rabbitmq": stubMonitor{healthy: true},
	})
	w := readyRequest(t, h)
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", w.Code)
	}
	var body struct {
		Status    string `json:"status"`
		Consumers map[string]struct {
			Healthy bool `json:"healthy"`
		} `json:"consumers"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body.Status != "unavailable" {
		t.Errorf("status = %q, want unavailable", body.Status)
	}
	redis, ok := body.Consumers["redis"]
	if !ok {
		t.Fatalf("consumers = %v, want a redis entry", body.Consumers)
	}
	if redis.Healthy {
		t.Error("redis.healthy = true, want false")
	}
	rabbit, ok := body.Consumers["rabbitmq"]
	if !ok || !rabbit.Healthy {
		t.Errorf("rabbitmq = %+v, want a healthy entry", body.Consumers["rabbitmq"])
	}
	if strings.Contains(w.Body.String(), "CREDENTIAL-MARKER") {
		t.Errorf("body %s leaks error text, want names + health flags only", w.Body.String())
	}
}

func TestReadyDBDownWithConsumers(t *testing.T) {
	h := NewHealth(fakePinger{err: errors.New("connection refused")}).WithConsumers(map[string]ConsumerMonitor{
		"redis": stubMonitor{healthy: true},
	})
	w := readyRequest(t, h)
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", w.Code)
	}
	if !strings.Contains(w.Body.String(), `"redis"`) {
		t.Errorf("body %s misses the consumer detail", w.Body.String())
	}
}

// No consumers registered: today's behavior byte-identical.
func TestReadyNoConsumersUnchanged(t *testing.T) {
	h := NewHealth(fakePinger{})
	if w := readyRequest(t, h); w.Code != http.StatusOK || w.Body.String() != `{"status":"ok"}` {
		t.Errorf("db ok: status = %d body = %s, want 200 {\"status\":\"ok\"}", w.Code, w.Body.String())
	}
	h = NewHealth(fakePinger{err: errors.New("connection refused")})
	if w := readyRequest(t, h); w.Code != http.StatusServiceUnavailable || w.Body.String() != `{"status":"unavailable"}` {
		t.Errorf("db down: status = %d body = %s, want 503 {\"status\":\"unavailable\"}", w.Code, w.Body.String())
	}
}
