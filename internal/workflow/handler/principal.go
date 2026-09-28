package handler

import (
	"github.com/gin-gonic/gin"
	"github.com/simpwf/workflow-engine/internal/workflow/auth"
)

// PrincipalContextKey is the Gin context key under which the authenticated
// principal is stored. Handlers read the caller through PrincipalFrom rather
// than touching the key directly.
const PrincipalContextKey = "principal"

// principalKey is the short internal alias used by the middleware.
const principalKey = PrincipalContextKey

// PrincipalFrom returns the authenticated principal of the request and
// whether authentication ran. A request that reached a handler without
// authentication (auth disabled) has neither.
func PrincipalFrom(c *gin.Context) (auth.Principal, bool) {
	return auth.FromContext(c.Request.Context())
}

// requestPrincipal returns the caller's principal for the service layer.
// A request that reached a handler without authentication (auth disabled)
// has none, and the service reads a nil principal as the service principal.
func requestPrincipal(c *gin.Context) *auth.Principal {
	p, ok := PrincipalFrom(c)
	if !ok {
		return nil
	}
	return &p
}

// principalActor returns the users.id uuid the request should be attributed
// to, or the empty string when the deployment runs without authentication.
// The services fall back to the configured system user in that case.
func principalActor(c *gin.Context) string {
	p, ok := PrincipalFrom(c)
	if !ok {
		return ""
	}
	return p.UserID
}

// requestPrincipalValue returns the caller's principal as a value for the
// read calls whose service signature takes auth.Principal. An
// unauthenticated request yields the zero principal, which the service
// treats as authentication disabled rather than as the service bypass.
func requestPrincipalValue(c *gin.Context) auth.Principal {
	p, ok := PrincipalFrom(c)
	if !ok {
		return auth.Principal{}
	}
	return p
}
