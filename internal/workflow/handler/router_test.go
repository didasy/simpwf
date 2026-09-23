package handler

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/simpwf/workflow-engine/pkg/configuration"
)

func TestRouterHealthRoutes(t *testing.T) {
	r := NewRouter(Deps{Health: NewHealth(fakePinger{})})

	for _, path := range []string{"/health/live", "/health/ready"} {
		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, path, nil)
		r.ServeHTTP(w, req)
		if w.Code != http.StatusOK {
			t.Errorf("GET %s status = %d, want 200", path, w.Code)
		}
	}
}

func TestRouterUnknownRoute(t *testing.T) {
	r := NewRouter(Deps{Health: NewHealth(fakePinger{})})
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/nope", nil)
	r.ServeHTTP(w, req)
	if w.Code != http.StatusNotFound {
		t.Errorf("unknown route status = %d, want 404", w.Code)
	}
}

func TestRouterSwaggerRoutesWhenEnabled(t *testing.T) {
	r := NewRouter(Deps{
		Health:         NewHealth(fakePinger{}),
		SwaggerEnabled: true,
	})

	for _, path := range []string{"/swagger/index.html", "/swagger/doc.json"} {
		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, path, nil)
		r.ServeHTTP(w, req)
		if w.Code != http.StatusOK {
			t.Errorf("GET %s status = %d, want 200", path, w.Code)
		}
	}

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/swagger/doc.json", nil)
	r.ServeHTTP(w, req)
	var document map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &document); err != nil {
		t.Fatalf("decode Swagger document: %v", err)
	}
	if document["swagger"] != "2.0" {
		t.Errorf("swagger version = %v, want 2.0", document["swagger"])
	}
}

func TestRouterSwaggerRoutesWhenDisabled(t *testing.T) {
	r := NewRouter(Deps{Health: NewHealth(fakePinger{})})
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/swagger/index.html", nil)
	r.ServeHTTP(w, req)
	if w.Code != http.StatusNotFound {
		t.Errorf("GET /swagger/index.html status = %d, want 404", w.Code)
	}
}

func TestRouterStatisticsRoutesRegistered(t *testing.T) {
	r := NewRouter(Deps{Health: NewHealth(fakePinger{}), Statistics: &fakeStatisticsSvc{}})
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/statistics", nil)
	r.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("GET /v1/statistics status = %d, want 200", w.Code)
	}
}

func authConfigForTest() configuration.Auth {
	return configuration.Auth{Enabled: true, APIToken: "test-token"}
}

func TestRouterStatisticsRoutesRequireAuth(t *testing.T) {
	r := NewRouter(Deps{
		Health:     NewHealth(fakePinger{}),
		Statistics: &fakeStatisticsSvc{},
		Auth:       authConfigForTest(),
	})
	for _, path := range []string{"/v1/statistics"} {
		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, path, nil)
		r.ServeHTTP(w, req)
		if w.Code != http.StatusUnauthorized {
			t.Errorf("GET %s status = %d, want 401", path, w.Code)
		}
	}
}

func TestRouterStatisticsBadQuery(t *testing.T) {
	r := NewRouter(Deps{Health: NewHealth(fakePinger{}), Statistics: &fakeStatisticsSvc{}})
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/statistics?order=total", nil)
	r.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", w.Code)
	}
}
