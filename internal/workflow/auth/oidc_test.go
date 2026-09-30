package auth_test

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"errors"
	"math/big"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/go-jose/go-jose/v4"
	"github.com/simpwf/workflow-engine/internal/workflow/auth"
	"github.com/simpwf/workflow-engine/pkg/configuration"
)

// oidcFixture is a mock provider: it serves the discovery document and a
// JWKS containing one RSA key, and signs tokens with that key. It is enough
// to exercise signature, issuer, audience, and expiry checks end to end
// without a real identity provider.
type oidcFixture struct {
	server   *httptest.Server
	key      *rsa.PrivateKey
	keyID    string
	issuer   string
	audience string
}

func newOIDCFixture(t *testing.T) *oidcFixture {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate rsa key: %v", err)
	}
	f := &oidcFixture{key: key, keyID: "test-key-1", audience: "simpwf"}
	mux := http.NewServeMux()
	mux.HandleFunc("/.well-known/openid-configuration", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"issuer":                                f.issuer,
			"authorization_endpoint":                f.issuer + "/authorize",
			"token_endpoint":                        f.issuer + "/token",
			"jwks_uri":                              f.issuer + "/keys",
			"id_token_signing_alg_values_supported": []string{"RS256"},
			"subject_types_supported":               []string{"public"},
		})
	})
	mux.HandleFunc("/keys", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"keys": []map[string]any{{
				"kty": "RSA",
				"use": "sig",
				"alg": "RS256",
				"kid": f.keyID,
				"n":   base64.RawURLEncoding.EncodeToString(key.N.Bytes()),
				"e":   base64.RawURLEncoding.EncodeToString(big.NewInt(int64(key.PublicKey.E)).Bytes()),
			}},
		})
	})
	f.server = httptest.NewServer(mux)
	t.Cleanup(f.server.Close)
	f.issuer = f.server.URL
	return f
}

// authenticator builds a verifier against the fixture with a one-minute skew.
func (f *oidcFixture) authenticator(t *testing.T) *auth.OIDCAuthenticator {
	t.Helper()
	a, err := auth.NewOIDCAuthenticator(context.Background(), configuration.OIDC{
		Enabled:       true,
		Issuer:        f.issuer,
		ClientID:      "simpwf",
		RolesClaim:    "roles",
		UsernameClaim: "name",
		ClockSkew:     time.Minute,
	})
	if err != nil {
		t.Fatalf("NewOIDCAuthenticator() error = %v", err)
	}
	return a
}

// token signs claims with the fixture key, overriding any aud/iss/exp that
// the caller set so a test can produce an invalid token on purpose.
func (f *oidcFixture) token(t *testing.T, claims map[string]any) string {
	t.Helper()
	return signClaims(t, f.key, f.keyID, claims)
}

func signClaims(t *testing.T, key *rsa.PrivateKey, keyID string, claims map[string]any) string {
	t.Helper()
	signer, err := jose.NewSigner(
		jose.SigningKey{Algorithm: jose.RS256, Key: jose.JSONWebKey{Key: key, KeyID: keyID}},
		(&jose.SignerOptions{}).WithType("JWT"),
	)
	if err != nil {
		t.Fatalf("new signer: %v", err)
	}
	payload, err := json.Marshal(claims)
	if err != nil {
		t.Fatalf("marshal claims: %v", err)
	}
	object, err := signer.Sign(payload)
	if err != nil {
		t.Fatalf("sign token: %v", err)
	}
	signed, err := object.CompactSerialize()
	if err != nil {
		t.Fatalf("serialize token: %v", err)
	}
	return signed
}

// validClaims returns a well-formed claim set the caller can amend.
func (f *oidcFixture) validClaims(subject string) map[string]any {
	return map[string]any{
		"iss":   f.issuer,
		"aud":   f.audience,
		"sub":   subject,
		"exp":   time.Now().Add(5 * time.Minute).Unix(),
		"iat":   time.Now().Add(-time.Minute).Unix(),
		"name":  "Ada Finance",
		"email": "ada@example.com",
		"roles": []string{"finance"},
	}
}

func TestVerifyAcceptsValidToken(t *testing.T) {
	f := newOIDCFixture(t)
	a := f.authenticator(t)

	p, err := a.Verify(context.Background(), f.token(t, f.validClaims("u-1")))
	if err != nil {
		t.Fatalf("Verify() error = %v", err)
	}
	if p.Subject != "u-1" {
		t.Errorf("Subject = %q, want u-1", p.Subject)
	}
	if p.Issuer != f.issuer {
		t.Errorf("Issuer = %q, want %q", p.Issuer, f.issuer)
	}
	if p.Name != "Ada Finance" {
		t.Errorf("Name = %q, want Ada Finance", p.Name)
	}
	if p.Email != "ada@example.com" {
		t.Errorf("Email = %q, want ada@example.com", p.Email)
	}
	if len(p.Roles) != 1 || p.Roles[0] != "finance" {
		t.Errorf("Roles = %v, want [finance]", p.Roles)
	}
	if p.Service {
		t.Error("Service = true, want false for an OIDC caller")
	}
}

func TestVerifyRejectsExpiredToken(t *testing.T) {
	f := newOIDCFixture(t)
	// No skew: an expired token must fail.
	a, err := auth.NewOIDCAuthenticator(context.Background(), configuration.OIDC{
		Issuer: f.issuer, ClientID: "simpwf", ClockSkew: 0,
	})
	if err != nil {
		t.Fatalf("NewOIDCAuthenticator() error = %v", err)
	}
	claims := f.validClaims("u-1")
	claims["exp"] = time.Now().Add(-2 * time.Hour).Unix()
	_, err = a.Verify(context.Background(), f.token(t, claims))
	if !errors.Is(err, auth.ErrUnauthenticated) {
		t.Fatalf("Verify() error = %v, want ErrUnauthenticated", err)
	}
}

// A token that expired just now is still inside the configured skew and must
// be accepted: a skewed clock is exactly what the setting exists for.
func TestVerifyAcceptsRecentlyExpiredTokenWithinSkew(t *testing.T) {
	f := newOIDCFixture(t)
	a := f.authenticator(t)
	claims := f.validClaims("u-1")
	claims["exp"] = time.Now().Add(-20 * time.Second).Unix()
	if _, err := a.Verify(context.Background(), f.token(t, claims)); err != nil {
		t.Fatalf("Verify() error = %v, want the skew to accept the token", err)
	}
}

func TestVerifyRejectsWrongAudience(t *testing.T) {
	f := newOIDCFixture(t)
	a := f.authenticator(t)
	claims := f.validClaims("u-1")
	claims["aud"] = "some-other-api"
	if _, err := a.Verify(context.Background(), f.token(t, claims)); !errors.Is(err, auth.ErrUnauthenticated) {
		t.Fatalf("Verify() error = %v, want ErrUnauthenticated", err)
	}
}

func TestVerifyRejectsWrongIssuer(t *testing.T) {
	f := newOIDCFixture(t)
	a := f.authenticator(t)
	claims := f.validClaims("u-1")
	claims["iss"] = "https://attacker.test"
	if _, err := a.Verify(context.Background(), f.token(t, claims)); !errors.Is(err, auth.ErrUnauthenticated) {
		t.Fatalf("Verify() error = %v, want ErrUnauthenticated", err)
	}
}

// A token signed by a key the provider never advertised must not verify,
// even though the issuer and audience are right.
func TestVerifyRejectsForeignSignature(t *testing.T) {
	f := newOIDCFixture(t)
	a := f.authenticator(t)
	other := newOIDCFixture(t)
	if _, err := a.Verify(context.Background(), other.token(t, f.validClaims("u-1"))); !errors.Is(err, auth.ErrUnauthenticated) {
		t.Fatalf("Verify() error = %v, want ErrUnauthenticated", err)
	}
}

func TestVerifyRejectsEmptyAndMalformed(t *testing.T) {
	f := newOIDCFixture(t)
	a := f.authenticator(t)
	for name, raw := range map[string]string{
		"empty":      "",
		"whitespace": "  ",
		"not a jwt":  "abc",
		"two parts":  "a.b",
	} {
		if _, err := a.Verify(context.Background(), raw); !errors.Is(err, auth.ErrUnauthenticated) {
			t.Errorf("Verify(%s) error = %v, want ErrUnauthenticated", name, err)
		}
	}
}

func TestVerifyRoleClaimShapes(t *testing.T) {
	f := newOIDCFixture(t)
	a := f.authenticator(t)
	cases := map[string]struct {
		raw  any
		want []string
	}{
		"array":       {raw: []any{"finance", "manager"}, want: []string{"finance", "manager"}},
		"single":      {raw: "finance", want: []string{"finance"}},
		"delimited":   {raw: "finance manager", want: []string{"finance", "manager"}},
		"deduped":     {raw: []any{"finance", "finance"}, want: []string{"finance"}},
		"blank entry": {raw: []any{" finance ", ""}, want: []string{"finance"}},
		"non string":  {raw: []any{1, "finance"}, want: []string{"finance"}},
		"empty array": {raw: []any{}, want: nil},
	}
	for name, tc := range cases {
		claims := f.validClaims("u-1")
		claims["roles"] = tc.raw
		p, err := a.Verify(context.Background(), f.token(t, claims))
		if err != nil {
			t.Errorf("Verify(%s) error = %v", name, err)
			continue
		}
		if len(p.Roles) != len(tc.want) {
			t.Errorf("Verify(%s) Roles = %v, want %v", name, p.Roles, tc.want)
			continue
		}
		for i := range tc.want {
			if p.Roles[i] != tc.want[i] {
				t.Errorf("Verify(%s) Roles = %v, want %v", name, p.Roles, tc.want)
				break
			}
		}
	}
}

func TestVerifyMissingRolesClaimGrantsNothing(t *testing.T) {
	f := newOIDCFixture(t)
	a := f.authenticator(t)
	claims := f.validClaims("u-1")
	delete(claims, "roles")
	p, err := a.Verify(context.Background(), f.token(t, claims))
	if err != nil {
		t.Fatalf("Verify() error = %v", err)
	}
	if len(p.Roles) != 0 {
		t.Errorf("Roles = %v, want none", p.Roles)
	}
}

// Providers nest the interesting claims (Keycloak's realm_access.roles,
// an Azure profile object), so a dotted path has to reach into the token
// rather than only match a top-level key.
func TestVerifySupportsDottedClaimPaths(t *testing.T) {
	f := newOIDCFixture(t)
	a, err := auth.NewOIDCAuthenticator(context.Background(), configuration.OIDC{
		Issuer: f.issuer, ClientID: "simpwf",
		RolesClaim: "realm_access.roles", UsernameClaim: "profile.display",
		ClockSkew: time.Minute,
	})
	if err != nil {
		t.Fatalf("NewOIDCAuthenticator() error = %v", err)
	}
	claims := f.validClaims("u-1")
	delete(claims, "roles")
	claims["realm_access"] = map[string]any{"roles": []string{"finance", "manager"}}
	claims["profile"] = map[string]any{"display": "Ada Finance"}

	p, err := a.Verify(context.Background(), f.token(t, claims))
	if err != nil {
		t.Fatalf("Verify() error = %v", err)
	}
	if len(p.Roles) != 2 || p.Roles[0] != "finance" || p.Roles[1] != "manager" {
		t.Errorf("Roles = %v, want [finance manager] from the nested claim", p.Roles)
	}
	if p.Name != "Ada Finance" {
		t.Errorf("Name = %q, want the nested username claim", p.Name)
	}
}

// A dotted path that resolves to nothing must not fall through to a
// top-level key of the same name, and it must not be read as a flat key
// containing a dot.
func TestVerifyDottedPathDoesNotMatchFlatKey(t *testing.T) {
	f := newOIDCFixture(t)
	a, err := auth.NewOIDCAuthenticator(context.Background(), configuration.OIDC{
		Issuer: f.issuer, ClientID: "simpwf",
		RolesClaim: "realm_access.roles", UsernameClaim: "profile.display",
		ClockSkew: time.Minute,
	})
	if err != nil {
		t.Fatalf("NewOIDCAuthenticator() error = %v", err)
	}
	claims := f.validClaims("u-1")
	delete(claims, "roles")
	// A literal dotted key, and a parent object with nothing under it.
	claims["realm_access.roles"] = []string{"finance"}
	claims["realm_access"] = map[string]any{"other": []string{"manager"}}
	claims["profile"] = map[string]any{"other": "Nope"}

	p, err := a.Verify(context.Background(), f.token(t, claims))
	if err != nil {
		t.Fatalf("Verify() error = %v", err)
	}
	if len(p.Roles) != 0 {
		t.Errorf("Roles = %v, want none from an unresolvable dotted path", p.Roles)
	}
	if p.Name == "Nope" {
		t.Errorf("Name = %q, want the unresolvable path ignored", p.Name)
	}
}

// A flat claim name keeps working, and a path whose intermediate segment is
// not an object is simply absent rather than a panic.
func TestVerifyClaimPathEdgeCases(t *testing.T) {
	f := newOIDCFixture(t)
	a, err := auth.NewOIDCAuthenticator(context.Background(), configuration.OIDC{
		Issuer: f.issuer, ClientID: "simpwf",
		RolesClaim: "realm_access.roles", ClockSkew: time.Minute,
	})
	if err != nil {
		t.Fatalf("NewOIDCAuthenticator() error = %v", err)
	}
	claims := f.validClaims("u-1")
	delete(claims, "roles")
	// The parent is a string, not an object.
	claims["realm_access"] = "finance"
	p, err := a.Verify(context.Background(), f.token(t, claims))
	if err != nil {
		t.Fatalf("Verify() error = %v", err)
	}
	if len(p.Roles) != 0 {
		t.Errorf("Roles = %v, want none when the path traverses a string", p.Roles)
	}
}

// The username claim is configurable, and an absent one falls back through
// preferred_username to email and finally the subject.
func TestVerifyDisplayNameFallbacks(t *testing.T) {
	f := newOIDCFixture(t)
	a, err := auth.NewOIDCAuthenticator(context.Background(), configuration.OIDC{
		Issuer: f.issuer, ClientID: "simpwf", UsernameClaim: "login_hint", ClockSkew: time.Minute,
	})
	if err != nil {
		t.Fatalf("NewOIDCAuthenticator() error = %v", err)
	}
	claims := f.validClaims("u-1")
	delete(claims, "name")
	claims["preferred_username"] = "adaf"
	p, err := a.Verify(context.Background(), f.token(t, claims))
	if err != nil {
		t.Fatalf("Verify() error = %v", err)
	}
	if p.Name != "adaf" {
		t.Errorf("Name = %q, want adaf", p.Name)
	}

	delete(claims, "preferred_username")
	p, err = a.Verify(context.Background(), f.token(t, claims))
	if err != nil {
		t.Fatalf("Verify() error = %v", err)
	}
	if p.Name != "ada@example.com" {
		t.Errorf("Name = %q, want the email fallback", p.Name)
	}
}

func TestConfigExposesLoginContract(t *testing.T) {
	f := newOIDCFixture(t)
	a := f.authenticator(t)
	cfg := a.Config()
	if !cfg.Enabled {
		t.Error("Enabled = false, want true")
	}
	if cfg.Issuer != f.issuer {
		t.Errorf("Issuer = %q, want %q", cfg.Issuer, f.issuer)
	}
	if cfg.ClientID != "simpwf" {
		t.Errorf("ClientID = %q, want simpwf", cfg.ClientID)
	}
	if cfg.AuthorizationURL != f.issuer+"/authorize" {
		t.Errorf("AuthorizationURL = %q, want the discovery endpoint", cfg.AuthorizationURL)
	}
	if cfg.TokenURL != f.issuer+"/token" {
		t.Errorf("TokenURL = %q, want the discovery endpoint", cfg.TokenURL)
	}
	if len(cfg.Scopes) == 0 || cfg.Scopes[0] != "openid" {
		t.Errorf("Scopes = %v, want openid first", cfg.Scopes)
	}
	// The fixture sets no audience, so the contract reports the fallback
	// rather than leaving a frontend to guess which aud it must request.
	if cfg.Audience != "simpwf" {
		t.Errorf("Audience = %q, want the client id fallback simpwf", cfg.Audience)
	}
}

// A provider that issues a distinct resource audience is reported as such,
// so a frontend requests that aud rather than the client id.
func TestConfigReportsConfiguredAudience(t *testing.T) {
	f := newOIDCFixture(t)
	a, err := auth.NewOIDCAuthenticator(context.Background(), configuration.OIDC{
		Issuer: f.issuer, ClientID: "simpwf", Audience: "https://api.example.com",
		RolesClaim: "roles", UsernameClaim: "name",
	})
	if err != nil {
		t.Fatalf("NewOIDCAuthenticator() error = %v", err)
	}
	cfg := a.Config()
	if cfg.Audience != "https://api.example.com" {
		t.Errorf("Audience = %q, want the configured resource audience", cfg.Audience)
	}
	if cfg.ClientID != "simpwf" {
		t.Errorf("ClientID = %q, want simpwf unchanged", cfg.ClientID)
	}
	// The verifier is pinned to the configured audience, not the client id.
	claims := f.validClaims("u-1")
	claims["aud"] = "simpwf"
	if _, err := a.Verify(context.Background(), f.token(t, claims)); !errors.Is(err, auth.ErrUnauthenticated) {
		t.Errorf("Verify(aud=client_id) error = %v, want ErrUnauthorized: the verifier must use the configured audience", err)
	}
}

// A disabled authenticator is a nil verifier: it must refuse everything, so
// a misconfigured deployment fails closed instead of accepting any JWT.
func TestDisabledAuthenticatorRefusesEverything(t *testing.T) {
	var a *auth.OIDCAuthenticator
	if _, err := a.Verify(context.Background(), "anything"); !errors.Is(err, auth.ErrUnauthenticated) {
		t.Fatalf("Verify() error = %v, want ErrUnauthenticated", err)
	}
	if a.Config().Enabled {
		t.Error("Config().Enabled = true, want false on a disabled authenticator")
	}
}

func TestNewOIDCAuthenticatorRejectsUnreachableIssuer(t *testing.T) {
	_, err := auth.NewOIDCAuthenticator(context.Background(), configuration.OIDC{
		Issuer: "http://127.0.0.1:1/never", ClientID: "simpwf",
	})
	if err == nil {
		t.Fatal("NewOIDCAuthenticator() error = nil, want a discovery failure")
	}
}
