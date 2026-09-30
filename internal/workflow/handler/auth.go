package handler

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"errors"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/simpwf/workflow-engine/internal/workflow/auth"
	"github.com/simpwf/workflow-engine/internal/workflow/model"
)

// APIKeyHeader is the header expected when API token auth is enabled.
const APIKeyHeader = "X-Api-Token"

// BearerHeader is the standard authorization header carrying an OIDC JWT.
const BearerHeader = "Authorization"

// IdentityResolver maps an authenticated identity onto a user row. It is
// satisfied by the auth service.
type IdentityResolver interface {
	ResolvePrincipal(ctx context.Context, p auth.Principal) (auth.Principal, error)
}

// AuthConfig is the authentication middleware's configuration. The zero
// value authenticates nothing, which is the disabled state a fresh
// deployment starts in.
type AuthConfig struct {
	// Enabled gates API-token authentication: when true, a request without
	// a valid X-Api-Token cannot reach a protected route.
	Enabled bool
	// APIToken is the expected API token, compared in constant time.
	APIToken string
	// Verifier validates OIDC bearer tokens. A nil Verifier leaves OIDC off.
	Verifier auth.Verifier
	// IdentityResolver maps a verified principal onto its users.id uuid. A
	// nil resolver leaves the principal unresolved, which is only correct
	// when the service principal is the only possible caller.
	IdentityResolver IdentityResolver
	// Catalog turns a caller's roles into the permissions route gates read.
	Catalog auth.Catalog
	// SystemUserID is the audit actor the API-token bypass acts as.
	SystemUserID string
}

// RequireAuth returns Gin middleware that authenticates every request and
// stores the resulting principal in the request context, so a handler reads
// the caller the same way regardless of which credential was used.
//
// Credential order:
//
//   - a valid X-Api-Token becomes the service principal, which bypasses the
//     endpoint permission and the input node's role gate;
//   - otherwise, with OIDC enabled, a valid Authorization: Bearer JWT
//     becomes a user principal carrying the token's roles;
//   - otherwise, with only the API token enabled, the request must carry it.
//
// A missing or invalid credential is 401. A valid credential that fails a
// route or node gate is 403, which the gate decides, not the middleware.
func RequireAuth(cfg AuthConfig) gin.HandlerFunc {
	return func(c *gin.Context) {
		principal, err := authenticate(c, cfg)
		if err != nil {
			WriteProblem(c, http.StatusUnauthorized, err.Error())
			return
		}
		if cfg.IdentityResolver != nil {
			principal, err = cfg.IdentityResolver.ResolvePrincipal(c.Request.Context(), principal)
			if err != nil {
				// A credential that names nobody is a client error about
				// the token; anything else is the database failing to
				// persist an identity that is already valid.
				if errors.Is(err, model.ErrInvalid) {
					WriteProblem(c, http.StatusUnauthorized, "cannot resolve the authenticated identity")
					return
				}
				WriteProblem(c, http.StatusInternalServerError, "cannot resolve the authenticated identity")
				return
			}
		}
		c.Request = c.Request.WithContext(auth.ContextWithPrincipal(c.Request.Context(), principal, cfg.Catalog))
		c.Set(principalKey, principal)
		c.Next()
	}
}

// authenticate resolves the credential on the request into a principal
// without consulting the database.
func authenticate(c *gin.Context, cfg AuthConfig) (auth.Principal, error) {
	tokenEnabled := cfg.Enabled && cfg.APIToken != ""
	// presentedAPI records that the caller sent the header at all, which is
	// what lets the refusal name the credential they actually used.
	presentedAPI := c.GetHeader(APIKeyHeader) != ""
	if tokenEnabled {
		token := c.GetHeader(APIKeyHeader)
		if token != "" && tokensEqual(token, cfg.APIToken) {
			// A valid API token always wins: it is the service principal's
			// own credential, so a caller presenting it is explicitly
			// acting as the service.
			return auth.SystemPrincipal(cfg.SystemUserID), nil
		}
	}
	if cfg.Verifier != nil {
		raw := bearerToken(c.GetHeader(BearerHeader))
		if raw == "" {
			return auth.Principal{}, cfg.credentialError(presentedAPI)
		}
		principal, err := cfg.Verifier.Verify(c.Request.Context(), raw)
		if err != nil {
			return auth.Principal{}, cfg.credentialError(presentedAPI)
		}
		// A signature check alone does not name anybody. The subject and
		// issuer are the identity the users row and the audit trail are
		// built from, so a token missing either fails closed instead of
		// authenticating an anonymous caller.
		if !principal.Service && (principal.Subject == "" || principal.Issuer == "") {
			return auth.Principal{}, cfg.credentialError(presentedAPI)
		}
		return principal, nil
	}
	if tokenEnabled {
		if !tokensEqual(c.GetHeader(APIKeyHeader), cfg.APIToken) {
			return auth.Principal{}, cfg.credentialError(presentedAPI)
		}
		return auth.SystemPrincipal(cfg.SystemUserID), nil
	}
	return auth.Principal{}, cfg.credentialError(presentedAPI)
}

// tokensEqual compares two API tokens in constant time. The digests are
// compared rather than the raw strings because subtle.ConstantTimeCompare
// returns immediately on a length mismatch, which would leak the configured
// token's length one request at a time. Hashing first makes both operands a
// fixed 32 bytes, so the comparison runs over the same work for every input
// and the length is not observable.
func tokensEqual(got, want string) bool {
	if got == "" {
		return false
	}
	gotSum := sha256.Sum256([]byte(got))
	wantSum := sha256.Sum256([]byte(want))
	return subtle.ConstantTimeCompare(gotSum[:], wantSum[:]) == 1
}

// credentialError names the credential a request is missing or got wrong.
// With OIDC on, the bearer token is what a browser sends, so it is the
// default. A caller who sent an API token is told about the API token
// instead: pointing them at a header their frontend never sets would send
// them looking in the wrong place. Naming the credential reveals nothing
// about the expected value, and the status is 401 either way.
func (cfg AuthConfig) credentialError(presentedAPIToken bool) error {
	if presentedAPIToken {
		return errors.New("missing or invalid API token")
	}
	if cfg.Verifier != nil {
		return errors.New("missing or invalid bearer token")
	}
	return errors.New("missing or invalid API token")
}

// bearerToken extracts the JWT from an Authorization header. A header that
// is not a bearer scheme yields the empty string, so it is rejected instead
// of being handed to the verifier as a malformed token.
func bearerToken(header string) string {
	scheme, value, found := strings.Cut(strings.TrimSpace(header), " ")
	if !found || !strings.EqualFold(scheme, "Bearer") {
		return ""
	}
	return strings.TrimSpace(value)
}

// RequirePermission returns Gin middleware admitting only callers whose
// roles union to action. A service principal is admitted: it holds the
// wildcard. The gate runs after authentication, so a caller reaching it is
// known and a refusal is 403 rather than 401.
func RequirePermission(action string, catalog auth.Catalog) gin.HandlerFunc {
	return func(c *gin.Context) {
		principal, ok := auth.FromContext(c.Request.Context())
		if !ok {
			WriteProblem(c, http.StatusUnauthorized, "missing or invalid API token")
			return
		}
		if !principal.HasPermission(action, catalog) {
			WriteProblem(c, http.StatusForbidden, "the "+action+" permission is required")
			return
		}
		c.Next()
	}
}

// TryAuth returns Gin middleware that authenticates when the request
// carries a credential and lets it through anonymous when it does not. A
// present credential is checked exactly like RequireAuth: valid resolves
// and stores the principal, invalid is 401 and never degrades to
// anonymous. A request with nothing to check carries no principal, so the
// handler reads it the same way it reads an auth-disabled deployment and
// the service decides against the pending node's public flag. Only the
// credential-less path records itself as anonymous, which is what keeps an
// authenticated delivery on a public node attributed to its caller rather
// than marked anonymous.
//
// It also checks the endpoint permission itself, and only for the callers it
// actually authenticated. An anonymous caller holds no permission to check,
// so the flag cannot be answered until the service has loaded the parked
// node; the service refuses it there. For a credentialed caller the check
// has to stay here rather than in the service, because a refusal written by
// the service would run after the object-level ownership check, and that one
// answers 404 to hide the instance. A caller who cannot act on the instance
// at all must be refused on its own terms, not told the instance is gone.
func TryAuth(cfg AuthConfig) gin.HandlerFunc {
	return func(c *gin.Context) {
		if !authPresented(c, cfg) {
			c.Set(anonymousKey, true)
			c.Next()
			return
		}
		principal, err := authenticate(c, cfg)
		if err != nil {
			WriteProblem(c, http.StatusUnauthorized, err.Error())
			return
		}
		if !principal.HasPermission(auth.ActionInputDeliver, cfg.Catalog) {
			WriteProblem(c, http.StatusForbidden, "the "+auth.ActionInputDeliver+" permission is required")
			return
		}
		if cfg.IdentityResolver != nil {
			principal, err = cfg.IdentityResolver.ResolvePrincipal(c.Request.Context(), principal)
			if err != nil {
				// A credential that names nobody is a client error about
				// the token; anything else is the database failing to
				// persist an identity that is already valid.
				if errors.Is(err, model.ErrInvalid) {
					WriteProblem(c, http.StatusUnauthorized, "cannot resolve the authenticated identity")
					return
				}
				WriteProblem(c, http.StatusInternalServerError, "cannot resolve the authenticated identity")
				return
			}
		}
		c.Request = c.Request.WithContext(auth.ContextWithPrincipal(c.Request.Context(), principal, cfg.Catalog))
		c.Set(principalKey, principal)
		c.Next()
	}
}

// authPresented reports whether the request carries anything to check: an
// API-token header value or an Authorization header. The header only needs
// to be present, not well-formed: a malformed credential is still a
// credential the verifier must refuse, not an anonymous request.
func authPresented(c *gin.Context, cfg AuthConfig) bool {
	if cfg.Enabled && strings.TrimSpace(c.GetHeader(APIKeyHeader)) != "" {
		return true
	}
	if strings.TrimSpace(c.GetHeader(BearerHeader)) != "" {
		return true
	}
	return false
}
