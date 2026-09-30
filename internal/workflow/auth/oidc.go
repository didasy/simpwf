package auth

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/coreos/go-oidc/v3/oidc"
	"github.com/simpwf/workflow-engine/pkg/configuration"
)

// ErrUnauthenticated marks every token that cannot be trusted: missing,
// malformed, expired, or failing the issuer/audience/signature checks. The
// handler maps it to 401; the wrapped provider message is returned to the
// caller but never to the client.
var ErrUnauthenticated = errors.New("auth: unauthenticated")

// Verifier validates OIDC bearer tokens. A nil implementation is the
// disabled state: it never authenticates, and the middleware falls back to
// the API-token service principal.
type Verifier interface {
	// Verify validates a raw JWT and projects its claims onto a Principal.
	Verify(ctx context.Context, rawToken string) (Principal, error)
	// Config is the public login contract served by GET /v1/auth/config.
	Config() AuthConfig
}

// AuthConfig is the public OIDC login contract a frontend needs to start a
// code+PKCE flow. It carries no secret: the API is a resource server and
// never holds the client secret.
type AuthConfig struct {
	Enabled bool   `json:"enabled"`
	Issuer  string `json:"issuer,omitempty"`
	// ClientID is the client the frontend authenticates as.
	ClientID string `json:"client_id,omitempty"`
	// Audience is the value the API requires a token's aud claim to carry.
	// It is the verifier's audience after the fallback, so a frontend can
	// tell whether it must request a distinct resource audience or the
	// client id doubles as one.
	Audience         string `json:"audience,omitempty"`
	AuthorizationURL string `json:"authorization_url,omitempty"`
	TokenURL         string `json:"token_url,omitempty"`
	// RolesClaim is the claim a frontend reads roles from.
	RolesClaim string `json:"roles_claim,omitempty"`
	// Scopes are the scopes to request; openid is always included.
	Scopes []string `json:"scopes,omitempty"`
}

// OIDCAuthenticator validates tokens against any OIDC provider through
// discovery and JWKS. It holds no session state: the frontend authenticates
// with the provider and the API only verifies what the provider signed.
type OIDCAuthenticator struct {
	strict *oidc.IDTokenVerifier
	// lax widens the accepted token lifetime by ClockSkew. It is consulted
	// only after the strict verifier rejected a token for being expired, so
	// a skewed clock never weakens the issuer, audience, or signature
	// checks.
	lax        *oidc.IDTokenVerifier
	rolesClaim string
	userClaim  string
	authConfig AuthConfig
}

// defaultScopes is what a frontend needs to obtain an id_token carrying the
// identity claims; add provider-specific scopes when roles are not in the
// token.
var defaultScopes = []string{"openid", "profile", "email"}

// NewOIDCAuthenticator performs provider discovery and builds the verifiers.
// issuer and clientID are required; an empty audience falls back to the
// client id. A discovery failure is returned so a bad configuration fails
// startup instead of failing every request later.
func NewOIDCAuthenticator(ctx context.Context, cfg configuration.OIDC) (*OIDCAuthenticator, error) {
	issuer := strings.TrimRight(strings.TrimSpace(cfg.Issuer), "/")
	provider, err := oidc.NewProvider(ctx, issuer)
	if err != nil {
		return nil, fmt.Errorf("auth: oidc discovery for %q: %w", cfg.Issuer, err)
	}
	// The discovery document is read once here so the signing algorithms
	// and the key set URL are pinned explicitly: both verifiers accept
	// exactly the algorithms the provider advertises rather than a library
	// default.
	var discovery struct {
		JWKSURI       string   `json:"jwks_uri"`
		Algorithms    []string `json:"id_token_signing_alg_values_supported"`
		AuthURL       string   `json:"authorization_endpoint"`
		TokenEndpoint string   `json:"token_endpoint"`
	}
	if err := provider.Claims(&discovery); err != nil {
		return nil, fmt.Errorf("auth: oidc discovery for %q: %w", cfg.Issuer, err)
	}
	if strings.TrimSpace(discovery.JWKSURI) == "" {
		return nil, fmt.Errorf("auth: oidc discovery for %q: jwks_uri is empty", cfg.Issuer)
	}
	algorithms := discovery.Algorithms
	if len(algorithms) == 0 {
		// A provider that omits id_token_signing_alg_values_supported would
		// otherwise leave the verifier with an empty accept list and reject
		// every token. RS256 is the OIDC default, so falling back to it
		// keeps such a provider working; the signature, issuer, audience,
		// and expiry checks are unchanged.
		algorithms = []string{"RS256"}
	}
	audience := strings.TrimSpace(cfg.Audience)
	if audience == "" {
		audience = strings.TrimSpace(cfg.ClientID)
	}
	rolesClaim := strings.TrimSpace(cfg.RolesClaim)
	if rolesClaim == "" {
		rolesClaim = "roles"
	}
	userClaim := strings.TrimSpace(cfg.UsernameClaim)
	if userClaim == "" {
		userClaim = "name"
	}
	clockSkew := cfg.ClockSkew
	if clockSkew < 0 {
		clockSkew = 0
	}
	keySet := newTTLKeySet(discovery.JWKSURI, cfg.CacheTTL)
	a := &OIDCAuthenticator{
		strict: oidc.NewVerifier(issuer, keySet, &oidc.Config{
			ClientID:             audience,
			SupportedSigningAlgs: algorithms,
		}),
		rolesClaim: rolesClaim,
		userClaim:  userClaim,
		authConfig: AuthConfig{
			Enabled:          true,
			Issuer:           issuer,
			ClientID:         strings.TrimSpace(cfg.ClientID),
			Audience:         audience,
			AuthorizationURL: discovery.AuthURL,
			TokenURL:         discovery.TokenEndpoint,
			RolesClaim:       rolesClaim,
			Scopes:           defaultScopes,
		},
	}
	if clockSkew > 0 {
		// The lax verifier shares the key set, so the retry neither
		// refetches discovery nor re-downloads the JWKS the strict attempt
		// already used. The library rejects a token whose exp is before
		// now, so widening the accepted window means presenting an earlier
		// clock: a token that expired less than ClockSkew ago still passes.
		a.lax = oidc.NewVerifier(issuer, keySet, &oidc.Config{
			ClientID:             audience,
			SupportedSigningAlgs: algorithms,
			Now:                  func() time.Time { return time.Now().Add(-clockSkew) },
		})
	}
	return a, nil
}

// Verify validates the signature, issuer, audience, and expiry of rawToken
// and projects the claims onto a Principal. Every failure is reported as
// ErrUnauthenticated.
func (a *OIDCAuthenticator) Verify(ctx context.Context, rawToken string) (Principal, error) {
	if a == nil || a.strict == nil {
		return Principal{}, fmt.Errorf("%w: oidc is disabled", ErrUnauthenticated)
	}
	rawToken = strings.TrimSpace(rawToken)
	if rawToken == "" {
		return Principal{}, fmt.Errorf("%w: empty bearer token", ErrUnauthenticated)
	}
	idToken, err := a.strict.Verify(ctx, rawToken)
	if err != nil {
		// Only a lifetime rejection is retried, and only with a widened
		// now(). The skew can never rescue a bad signature, issuer, or
		// audience, because those checks already passed.
		var expired *oidc.TokenExpiredError
		if a.lax == nil || !errors.As(err, &expired) {
			return Principal{}, fmt.Errorf("%w: %v", ErrUnauthenticated, err)
		}
		idToken, err = a.lax.Verify(ctx, rawToken)
		if err != nil {
			return Principal{}, fmt.Errorf("%w: %v", ErrUnauthenticated, err)
		}
	}
	var claims map[string]any
	if err := idToken.Claims(&claims); err != nil {
		return Principal{}, fmt.Errorf("%w: claims are not a JSON object", ErrUnauthenticated)
	}
	return a.principalFrom(idToken, claims), nil
}

// Config returns the public login contract.
func (a *OIDCAuthenticator) Config() AuthConfig {
	if a == nil {
		return AuthConfig{}
	}
	return a.authConfig
}

// principalFrom projects verified claims onto a Principal. Roles come from
// the configured claim in any of the shapes providers use: a string array, a
// single string, or a space-delimited string. The display name prefers the
// configured claim, then the common fallbacks.
func (a *OIDCAuthenticator) principalFrom(idToken *oidc.IDToken, claims map[string]any) Principal {
	name := firstNonEmpty(
		stringClaim(claims, a.userClaim),
		stringClaim(claims, "name"),
		stringClaim(claims, "preferred_username"),
		stringClaim(claims, "email"),
		idToken.Subject,
	)
	return Principal{
		Subject: idToken.Subject,
		Issuer:  idToken.Issuer,
		Name:    name,
		Email:   stringClaim(claims, "email"),
		Roles:   roleClaim(claims, a.rolesClaim),
	}
}

// lookupClaim resolves a configured claim name against the token. A dotted
// path walks nested objects, which is how providers publish claims like
// Keycloak's realm_access.roles; a flat name is the single-segment case, so
// existing configurations keep working unchanged.
//
// A literal dotted key is never consulted: "a.b" means the b inside a, so
// a provider that also sends a top-level "a.b" cannot smuggle a different
// value past the configured path. A segment that is missing, or that is not
// an object, simply ends the walk.
func lookupClaim(claims map[string]any, path string) (any, bool) {
	if path == "" {
		return nil, false
	}
	var current any = claims
	for _, segment := range strings.Split(path, ".") {
		if segment == "" {
			return nil, false
		}
		obj, ok := current.(map[string]any)
		if !ok {
			return nil, false
		}
		value, ok := obj[segment]
		if !ok {
			return nil, false
		}
		current = value
	}
	return current, true
}

// roleClaim extracts a role list from raw, accepting the three shapes seen
// in practice: an array of strings, a single string, or a space-delimited
// string. Blank entries are dropped and the result is deduplicated while
// keeping the token's order.
func roleClaim(claims map[string]any, key string) []string {
	raw, ok := lookupClaim(claims, key)
	if !ok || raw == nil {
		return nil
	}
	var candidates []string
	switch typed := raw.(type) {
	case []any:
		for _, item := range typed {
			if s, ok := item.(string); ok {
				candidates = append(candidates, s)
			}
		}
	case []string:
		candidates = append(candidates, typed...)
	case string:
		candidates = strings.Fields(typed)
	}
	seen := make(map[string]bool, len(candidates))
	out := make([]string, 0, len(candidates))
	for _, role := range candidates {
		role = strings.TrimSpace(role)
		if role == "" || seen[role] {
			continue
		}
		seen[role] = true
		out = append(out, role)
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// stringClaim reads a claim as a string, ignoring every other shape.
func stringClaim(claims map[string]any, key string) string {
	raw, ok := lookupClaim(claims, key)
	if !ok {
		return ""
	}
	if s, ok := raw.(string); ok {
		return strings.TrimSpace(s)
	}
	return ""
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if v != "" {
			return v
		}
	}
	return ""
}
