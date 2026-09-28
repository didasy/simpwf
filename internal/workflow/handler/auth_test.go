package handler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/simpwf/workflow-engine/internal/workflow/auth"
	"github.com/simpwf/workflow-engine/internal/workflow/model"
)

const testSystemUserID = "11111111-1111-7111-8111-111111111111"

// testIssuer is the issuer a stubbed token claims. A verified bearer token
// must name a subject and an issuer, so every test principal carries both.
const testIssuer = "https://issuer.test"

// tokenAuth is the API-token-only configuration used by the middleware
// tests: a valid token is the service principal, nothing else authenticates.
func tokenAuth(token string) AuthConfig {
	return AuthConfig{Enabled: true, APIToken: token, SystemUserID: testSystemUserID}
}

func TestRequireAuthPassesWithValidToken(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.GET("/protected", RequireAuth(tokenAuth("secret")), func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"ok": true})
	})

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/protected", nil)
	req.Header.Set(APIKeyHeader, "secret")
	r.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
}

func TestRequireAuthRejectsMissingToken(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.GET("/protected", RequireAuth(tokenAuth("secret")), func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"ok": true})
	})

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/protected", nil)
	r.ServeHTTP(w, req)
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", w.Code)
	}
}

func TestRequireAuthRejectsWrongToken(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.GET("/protected", RequireAuth(tokenAuth("secret")), func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"ok": true})
	})

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/protected", nil)
	req.Header.Set(APIKeyHeader, "wrong")
	r.ServeHTTP(w, req)
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", w.Code)
	}
}

// With OIDC on, the bearer token is what a browser sends, so a blanket
// "bearer token" message is the right default. But a caller who did present
// an API token and got it wrong is being told to fix a header they did set,
// and the message has to name the one they actually sent.
func TestCredentialErrorNamesTheCredentialPresented(t *testing.T) {
	tests := []struct {
		name       string
		apiToken   string
		authHeader string
		want       string
	}{
		{"no credential at all", "", "", "bearer token"},
		{"a wrong API token", "wrong", "", "API token"},
		{"a wrong bearer token", "", "Bearer nope", "bearer token"},
		// Both are wrong: the API token is the one a service caller is
		// most likely to be debugging, so it is the one named.
		{"both credentials wrong", "wrong", "Bearer nope", "API token"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r := gin.New()
			r.GET("/protected", RequireAuth(AuthConfig{
				Enabled:      true,
				APIToken:     "secret",
				Verifier:     stubVerifier{},
				SystemUserID: testSystemUserID,
			}), func(c *gin.Context) {
				c.JSON(http.StatusOK, gin.H{"ok": true})
			})
			w := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodGet, "/protected", nil)
			if tc.apiToken != "" {
				req.Header.Set(APIKeyHeader, tc.apiToken)
			}
			if tc.authHeader != "" {
				req.Header.Set(BearerHeader, tc.authHeader)
			}
			r.ServeHTTP(w, req)
			if w.Code != http.StatusUnauthorized {
				t.Fatalf("status = %d, want 401", w.Code)
			}
			detail := problemDetail(t, w)
			if !strings.Contains(detail, tc.want) {
				t.Errorf("detail = %q, want it to name %q", detail, tc.want)
			}
		})
	}
}

// problemDetail reads the detail field of an RFC 7807 problem response.
func problemDetail(t *testing.T, w *httptest.ResponseRecorder) string {
	t.Helper()
	var body struct {
		Detail string `json:"detail"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode problem: %v (body: %s)", err, w.Body.String())
	}
	return body.Detail
}

// A valid API token is the service principal, so it must reach a route that
// demands a resource-action a user could not hold.
func TestRequireAuthServicePrincipalCarriesWildcard(t *testing.T) {
	gin.SetMode(gin.TestMode)
	catalog := auth.NewCatalog(map[string][]string{"reader": {auth.ActionInputDeliver}})
	r := gin.New()
	r.GET("/protected",
		RequireAuth(tokenAuth("secret")),
		RequirePermission(auth.ActionInputDeliver, catalog),
		func(c *gin.Context) {
			p, ok := PrincipalFrom(c)
			if !ok {
				t.Errorf("principal missing from request context")
				return
			}
			if !p.Service || p.UserID != testSystemUserID {
				t.Errorf("principal = %+v, want service principal %s", p, testSystemUserID)
			}
			c.JSON(http.StatusOK, gin.H{"ok": true})
		})

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/protected", nil)
	req.Header.Set(APIKeyHeader, "secret")
	r.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
}

// stubVerifier stands in for the OIDC verifier so the middleware's bearer
// path can be tested without a provider.
type stubVerifier struct {
	principal auth.Principal
	err       error
}

func (s stubVerifier) Verify(_ context.Context, _ string) (auth.Principal, error) {
	return s.principal, s.err
}

func (s stubVerifier) Config() auth.AuthConfig {
	return auth.AuthConfig{Enabled: true, Issuer: "https://issuer.test"}
}

func oidcAuth(verifier auth.Verifier, resolver IdentityResolver) AuthConfig {
	return AuthConfig{
		Verifier:         verifier,
		IdentityResolver: resolver,
		Catalog:          auth.NewCatalog(map[string][]string{"finance": {auth.ActionInputDeliver}}),
		SystemUserID:     testSystemUserID,
	}
}

func TestRequireAuthAcceptsBearerToken(t *testing.T) {
	gin.SetMode(gin.TestMode)
	verifier := stubVerifier{principal: auth.Principal{Subject: "s1", Issuer: testIssuer, Roles: []string{"finance"}}}
	r := gin.New()
	r.GET("/protected",
		RequireAuth(oidcAuth(verifier, nil)),
		RequirePermission(auth.ActionInputDeliver, auth.NewCatalog(map[string][]string{"finance": {auth.ActionInputDeliver}})),
		func(c *gin.Context) { c.JSON(http.StatusOK, gin.H{"ok": true}) })

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/protected", nil)
	req.Header.Set(BearerHeader, "Bearer signed.jwt.value")
	r.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
}

func TestRequireAuthRejectsBadBearerToken(t *testing.T) {
	gin.SetMode(gin.TestMode)
	verifier := stubVerifier{err: errors.New("expired")}
	r := gin.New()
	r.GET("/protected", RequireAuth(oidcAuth(verifier, nil)), func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"ok": true})
	})

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/protected", nil)
	req.Header.Set(BearerHeader, "Bearer signed.jwt.value")
	r.ServeHTTP(w, req)
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", w.Code)
	}
}

// An authenticated caller whose roles do not carry the action is 403, not
// 401: the credential was accepted, the request is simply not allowed.
func TestRequirePermissionRefusesAuthenticatedCaller(t *testing.T) {
	gin.SetMode(gin.TestMode)
	verifier := stubVerifier{principal: auth.Principal{Subject: "s1", Issuer: testIssuer, Roles: []string{"viewer"}}}
	catalog := auth.NewCatalog(map[string][]string{"finance": {auth.ActionInputDeliver}})
	r := gin.New()
	r.GET("/protected",
		RequireAuth(oidcAuth(verifier, nil)),
		RequirePermission(auth.ActionInputDeliver, catalog),
		func(c *gin.Context) { c.JSON(http.StatusOK, gin.H{"ok": true}) })

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/protected", nil)
	req.Header.Set(BearerHeader, "Bearer signed.jwt.value")
	r.ServeHTTP(w, req)
	if w.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want 403", w.Code)
	}
}

// A role absent from the catalog grants nothing, so an unknown claim role
// denies by default.
func TestRequirePermissionUnknownRoleDenies(t *testing.T) {
	gin.SetMode(gin.TestMode)
	verifier := stubVerifier{principal: auth.Principal{Subject: "s1", Issuer: testIssuer, Roles: []string{"wizard"}}}
	catalog := auth.NewCatalog(map[string][]string{"finance": {auth.ActionInputDeliver}})
	r := gin.New()
	r.GET("/protected",
		RequireAuth(oidcAuth(verifier, nil)),
		RequirePermission(auth.ActionInputDeliver, catalog),
		func(c *gin.Context) { c.JSON(http.StatusOK, gin.H{"ok": true}) })

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/protected", nil)
	req.Header.Set(BearerHeader, "Bearer signed.jwt.value")
	r.ServeHTTP(w, req)
	if w.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want 403", w.Code)
	}
}

// The identity resolver runs on the verified principal so the handler sees
// a user id, and a resolver failure must not leak a 200.
type stubResolver struct {
	called bool
	err    error
}

func (s *stubResolver) ResolvePrincipal(_ context.Context, p auth.Principal) (auth.Principal, error) {
	s.called = true
	if s.err != nil {
		return p, s.err
	}
	p.UserID = "22222222-2222-7222-8222-222222222222"
	return p, nil
}

func TestRequireAuthResolvesIdentity(t *testing.T) {
	gin.SetMode(gin.TestMode)
	verifier := stubVerifier{principal: auth.Principal{Subject: "s1", Issuer: testIssuer, Roles: []string{"finance"}}}
	resolver := &stubResolver{}
	r := gin.New()
	r.GET("/protected", RequireAuth(oidcAuth(verifier, resolver)), func(c *gin.Context) {
		p, _ := PrincipalFrom(c)
		c.JSON(http.StatusOK, gin.H{"user_id": p.UserID})
	})

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/protected", nil)
	req.Header.Set(BearerHeader, "Bearer signed.jwt.value")
	r.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	if !resolver.called {
		t.Error("identity resolver was not called")
	}
	if body := w.Body.String(); !contains(body, "22222222-2222-7222-8222-222222222222") {
		t.Errorf("body = %s, want the resolved user id", body)
	}
}

func TestRequireAuthResolverFailureIsServerError(t *testing.T) {
	gin.SetMode(gin.TestMode)
	verifier := stubVerifier{principal: auth.Principal{Subject: "s1", Issuer: testIssuer}}
	r := gin.New()
	r.GET("/protected", RequireAuth(oidcAuth(verifier, &stubResolver{err: errors.New("db down")})), func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"ok": true})
	})

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/protected", nil)
	req.Header.Set(BearerHeader, "Bearer signed.jwt.value")
	r.ServeHTTP(w, req)
	if w.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", w.Code)
	}
}

// A token that verifies but carries no subject or issuer identifies nobody.
// Failing closed is the only safe reading: a resolvable identity is what
// the audit trail and the JIT user row are built from.
func TestRequireAuthRejectsTokenWithoutIdentity(t *testing.T) {
	cases := map[string]auth.Principal{
		"no subject": {Issuer: "https://issuer.test", Roles: []string{"finance"}},
		"no issuer":  {Subject: "s1", Roles: []string{"finance"}},
		"neither":    {Roles: []string{"finance"}},
	}
	for name, principal := range cases {
		t.Run(name, func(t *testing.T) {
			gin.SetMode(gin.TestMode)
			r := gin.New()
			r.GET("/protected", RequireAuth(oidcAuth(stubVerifier{principal: principal}, nil)), func(c *gin.Context) {
				c.JSON(http.StatusOK, gin.H{"ok": true})
			})

			w := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodGet, "/protected", nil)
			req.Header.Set(BearerHeader, "Bearer signed.jwt.value")
			r.ServeHTTP(w, req)
			if w.Code != http.StatusUnauthorized {
				t.Fatalf("status = %d, want 401 for a token with no identity", w.Code)
			}
		})
	}
}

// A resolver that cannot map the identity is a client error about the
// token, not a server fault: the credential named nobody to persist.
func TestRequireAuthMissingIdentityResolverErrorIsUnauthorized(t *testing.T) {
	gin.SetMode(gin.TestMode)
	verifier := stubVerifier{principal: auth.Principal{Subject: "s1", Issuer: testIssuer}}
	r := gin.New()
	r.GET("/protected", RequireAuth(oidcAuth(verifier, &stubResolver{err: fmt.Errorf("missing subject: %w", model.ErrInvalid)})), func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"ok": true})
	})

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/protected", nil)
	req.Header.Set(BearerHeader, "Bearer signed.jwt.value")
	r.ServeHTTP(w, req)
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", w.Code)
	}
}

func TestBearerToken(t *testing.T) {
	cases := map[string]string{
		"Bearer abc.def.ghi": "abc.def.ghi",
		"bearer abc":         "abc",
		"BEARER  abc  ":      "abc",
		"Basic abc":          "",
		"abc":                "",
		"":                   "",
		"Bearer":             "",
	}
	for header, want := range cases {
		if got := bearerToken(header); got != want {
			t.Errorf("bearerToken(%q) = %q, want %q", header, got, want)
		}
	}
}

func contains(haystack, needle string) bool {
	return len(haystack) >= len(needle) && (haystack == needle || indexOf(haystack, needle) >= 0)
}

func indexOf(haystack, needle string) int {
	for i := 0; i+len(needle) <= len(haystack); i++ {
		if haystack[i:i+len(needle)] == needle {
			return i
		}
	}
	return -1
}
