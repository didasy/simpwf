package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/simpwf/workflow-engine/internal/workflow/auth"
	"github.com/simpwf/workflow-engine/internal/workflow/model"
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

func TestRouterSwaggerBearerAuthDocumentsPrefix(t *testing.T) {
	// BearerAuth is type apiKey on the Authorization header, so swagger-ui
	// sends the Authorize input verbatim with no Bearer prefix auto-added.
	// The description must therefore tell the user to type the prefix.
	r := NewRouter(Deps{
		Health:         NewHealth(fakePinger{}),
		SwaggerEnabled: true,
	})

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/swagger/doc.json", nil)
	r.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("GET /swagger/doc.json status = %d, want 200", w.Code)
	}
	var document map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &document); err != nil {
		t.Fatalf("decode Swagger document: %v", err)
	}
	defs, ok := document["securityDefinitions"].(map[string]any)
	if !ok {
		t.Fatal("swagger document has no securityDefinitions")
	}
	bearer, ok := defs["BearerAuth"].(map[string]any)
	if !ok {
		t.Fatal("swagger document has no BearerAuth securityDefinition")
	}
	desc, _ := bearer["description"].(string)
	if !strings.Contains(desc, "Bearer ") || !strings.Contains(strings.ToLower(desc), "prefix") {
		t.Errorf("BearerAuth description must tell the user to type the Bearer prefix, got %q", desc)
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

// fakeAuthSvc is an in-memory auth service: it satisfies the identity
// resolver for the router tests without touching a database.
type fakeAuthSvc struct{}

func (f *fakeAuthSvc) ResolvePrincipal(_ context.Context, p auth.Principal) (auth.Principal, error) {
	if p.UserID == "" {
		p.UserID = testSystemUserID
	}
	return p, nil
}

func (f *fakeAuthSvc) SeedRoles(context.Context) error { return nil }

func (f *fakeAuthSvc) ListRoles(context.Context) ([]model.Role, map[string][]string, error) {
	return []model.Role{{Name: "auditor"}}, map[string][]string{"auditor": {auth.ActionRolesRead}}, nil
}

func (f *fakeAuthSvc) GetRole(_ context.Context, name string) (model.Role, []string, error) {
	if name != "auditor" {
		return model.Role{}, nil, fmt.Errorf("%w: role %s", model.ErrNotFound, name)
	}
	return model.Role{Name: "auditor"}, []string{auth.ActionRolesRead}, nil
}

func (f *fakeAuthSvc) Me(_ context.Context, p auth.Principal) (model.User, error) {
	return model.User{ID: p.UserID, Name: p.Name, Email: p.Email}, nil
}

func authConfigForTest() configuration.Auth {
	return configuration.Auth{Enabled: true, APIToken: "test-token"}
}

func TestRouterStatisticsRoutesRequireAuth(t *testing.T) {
	r := NewRouter(Deps{
		Health:       NewHealth(fakePinger{}),
		Statistics:   &fakeStatisticsSvc{},
		AuthSettings: authConfigForTest(),
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

// With only the API token configured, a valid token is the service
// principal and satisfies any resource-action gate.
func TestRouterStatisticsAcceptsServiceToken(t *testing.T) {
	r := NewRouter(Deps{
		Health:       NewHealth(fakePinger{}),
		Statistics:   &fakeStatisticsSvc{},
		AuthSettings: authConfigForTest(),
	})
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/statistics", nil)
	req.Header.Set(APIKeyHeader, "test-token")
	r.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("status = %d, want 200", w.Code)
	}
}

// The login contract is public: a frontend reads it before it holds a token.
func TestRouterAuthConfigIsPublic(t *testing.T) {
	r := NewRouter(Deps{
		Health:       NewHealth(fakePinger{}),
		Auth:         &fakeAuthSvc{},
		AuthSettings: authConfigForTest(),
		Catalog:      auth.NewCatalog(map[string][]string{"finance": {auth.ActionInputDeliver}}),
	})
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/auth/config", nil)
	r.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("status = %d, want 200", w.Code)
	}
}

// /v1/auth/me is authenticated but carries no resource-action gate: any
// caller may ask who it is.
func TestRouterAuthMeRequiresCredentialOnly(t *testing.T) {
	r := NewRouter(Deps{
		Health:       NewHealth(fakePinger{}),
		Auth:         &fakeAuthSvc{},
		AuthSettings: authConfigForTest(),
	})
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/auth/me", nil)
	r.ServeHTTP(w, req)
	if w.Code != http.StatusUnauthorized {
		t.Errorf("status without token = %d, want 401", w.Code)
	}

	w = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/v1/auth/me", nil)
	req.Header.Set(APIKeyHeader, "test-token")
	r.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("status with token = %d, want 200", w.Code)
	}
}

// A caller with no roles is still a caller, so roles is an empty array
// rather than null: a frontend can iterate it without a nil check.
func TestRouterAuthMeReportsEmptyRolesAsArray(t *testing.T) {
	r := NewRouter(Deps{
		Health:       NewHealth(fakePinger{}),
		Auth:         &fakeAuthSvc{},
		AuthSettings: authConfigForTest(),
	})
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/auth/me", nil)
	req.Header.Set(APIKeyHeader, "test-token")
	r.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	var body struct {
		Roles *[]string `json:"roles"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode /v1/auth/me: %v", err)
	}
	if body.Roles == nil {
		t.Fatalf(`roles = null, want [] (body: %s)`, w.Body.String())
	}
	if len(*body.Roles) != 0 {
		t.Errorf("roles = %v, want an empty array", *body.Roles)
	}
}

// A credential that authenticated but never resolved to a user is an
// authentication failure, not a missing resource: 401, not 404.
func TestRouterAuthMeUnresolvedPrincipalIsUnauthorized(t *testing.T) {
	r := NewRouter(Deps{
		Health:       NewHealth(fakePinger{}),
		Auth:         &unresolvedAuthSvc{err: fmt.Errorf("%w: principal is not resolved to a user", model.ErrInvalid)},
		AuthSettings: authConfigForTest(),
	})
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/auth/me", nil)
	req.Header.Set(APIKeyHeader, "test-token")
	r.ServeHTTP(w, req)
	if w.Code != http.StatusUnauthorized {
		t.Errorf("status = %d, want 401 (body: %s)", w.Code, w.Body.String())
	}
}

// unresolvedAuthSvc authenticates the caller but resolves it to no user.
type unresolvedAuthSvc struct {
	fakeAuthSvc
	err error
}

func (u *unresolvedAuthSvc) Me(context.Context, auth.Principal) (model.User, error) {
	return model.User{}, u.err
}

func TestRouterRoleRoutesRequireRolesRead(t *testing.T) {
	// A caller with a role that grants nothing is refused the catalog; a
	// role that grants roles:read is admitted.
	catalog := auth.NewCatalog(map[string][]string{
		"auditor": {auth.ActionRolesRead},
	})
	r := NewRouter(Deps{
		Health:       NewHealth(fakePinger{}),
		Auth:         &fakeAuthSvc{},
		AuthSettings: configuration.Auth{},
		OIDC:         stubVerifier{principal: auth.Principal{Subject: "s1", Issuer: testIssuer, UserID: testSystemUserID, Roles: []string{"auditor"}}},
		Catalog:      catalog,
	})

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/roles", nil)
	req.Header.Set(BearerHeader, "Bearer signed")
	r.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("auditor GET /v1/roles status = %d, want 200", w.Code)
	}

	r = NewRouter(Deps{
		Health:       NewHealth(fakePinger{}),
		Auth:         &fakeAuthSvc{},
		AuthSettings: configuration.Auth{},
		OIDC:         stubVerifier{principal: auth.Principal{Subject: "s1", Issuer: testIssuer, UserID: testSystemUserID, Roles: []string{"finance"}}},
		Catalog:      catalog,
	})
	w = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/v1/roles", nil)
	req.Header.Set(BearerHeader, "Bearer signed")
	r.ServeHTTP(w, req)
	if w.Code != http.StatusForbidden {
		t.Errorf("finance GET /v1/roles status = %d, want 403", w.Code)
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

// Authentication that is switched on but cannot authenticate anyone is a
// misconfiguration, and the only safe reading of it is closed. Skipping the
// middleware because the token is missing would serve the whole API to the
// internet, so a tokenless run stays 401 rather than turning into an open
// deployment by accident.
func TestRouterEnabledWithoutTokenFailsClosed(t *testing.T) {
	r := NewRouter(Deps{
		Health:       NewHealth(fakePinger{}),
		Instances:    &fakeInstanceSvc{},
		AuthSettings: configuration.Auth{Enabled: true},
	})
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/workflow/instance", nil)
	r.ServeHTTP(w, req)
	if w.Code != http.StatusUnauthorized {
		t.Errorf("status without a token = %d, want 401 (body: %s)", w.Code, w.Body.String())
	}

	// Even a presented token is refused: nothing can match it.
	w = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/v1/workflow/instance", nil)
	req.Header.Set(APIKeyHeader, "anything")
	r.ServeHTTP(w, req)
	if w.Code != http.StatusUnauthorized {
		t.Errorf("status with a token = %d, want 401 (body: %s)", w.Code, w.Body.String())
	}
}

func TestRouterScheduleRoutesRequireSchedulesActions(t *testing.T) {
	catalog := auth.NewCatalog(map[string][]string{
		"reader": {auth.ActionSchedulesRead},
		"writer": {auth.ActionSchedulesWrite},
	})
	routerFor := func(roles ...string) *gin.Engine {
		return NewRouter(Deps{
			Health:       NewHealth(fakePinger{}),
			Schedules:    &fakeScheduleSvc{},
			Auth:         &fakeAuthSvc{},
			AuthSettings: configuration.Auth{},
			OIDC:         stubVerifier{principal: auth.Principal{Subject: "s1", Issuer: testIssuer, UserID: testSystemUserID, Roles: roles}},
			Catalog:      catalog,
		})
	}
	bearer := map[string]string{BearerHeader: "Bearer x"}

	// Reads need schedules:read.
	w := performJSON(routerFor("reader"), http.MethodGet, "/v1/workflow/schedules", "", bearer)
	if w.Code != http.StatusOK {
		t.Errorf("reader GET list status = %d, want 200", w.Code)
	}
	w = performJSON(routerFor("writer"), http.MethodGet, "/v1/workflow/schedules", "", bearer)
	if w.Code != http.StatusForbidden {
		t.Errorf("writer GET list status = %d, want 403", w.Code)
	}
	w = performJSON(routerFor("reader"), http.MethodGet, "/v1/workflow/schedules/"+scheduleID, "", bearer)
	if w.Code != http.StatusOK {
		t.Errorf("reader GET one status = %d, want 200", w.Code)
	}
	w = performJSON(routerFor("writer"), http.MethodGet, "/v1/workflow/schedules/"+scheduleID, "", bearer)
	if w.Code != http.StatusForbidden {
		t.Errorf("writer GET one status = %d, want 403", w.Code)
	}

	// Writes need schedules:write.
	body := `{"workflow_definition_id":"22222222-2222-7222-8222-222222222222","crontab":"*/5 * * * *"}`
	w = performJSON(routerFor("writer"), http.MethodPost, "/v1/workflow/schedules", body, bearer)
	if w.Code != http.StatusCreated {
		t.Errorf("writer POST status = %d, want 201", w.Code)
	}
	w = performJSON(routerFor("reader"), http.MethodPost, "/v1/workflow/schedules", body, bearer)
	if w.Code != http.StatusForbidden {
		t.Errorf("reader POST status = %d, want 403", w.Code)
	}
	w = performJSON(routerFor("reader"), http.MethodPost, "/v1/workflow/schedules/"+scheduleID+"/pause", "", bearer)
	if w.Code != http.StatusForbidden {
		t.Errorf("reader POST pause status = %d, want 403", w.Code)
	}
	w = performJSON(routerFor("reader"), http.MethodPost, "/v1/workflow/schedules/"+scheduleID+"/resume", "", bearer)
	if w.Code != http.StatusForbidden {
		t.Errorf("reader POST resume status = %d, want 403", w.Code)
	}
	w = performJSON(routerFor("writer"), http.MethodDelete, "/v1/workflow/schedules/"+scheduleID, "", bearer)
	if w.Code != http.StatusNoContent {
		t.Errorf("writer DELETE status = %d, want 204", w.Code)
	}
	w = performJSON(routerFor("reader"), http.MethodDelete, "/v1/workflow/schedules/"+scheduleID, "", bearer)
	if w.Code != http.StatusForbidden {
		t.Errorf("reader DELETE status = %d, want 403", w.Code)
	}

	// No credential at all is 401, not 403.
	w = performJSON(routerFor("reader"), http.MethodGet, "/v1/workflow/schedules", "", nil)
	if w.Code != http.StatusUnauthorized {
		t.Errorf("anonymous GET status = %d, want 401", w.Code)
	}
}
