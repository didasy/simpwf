package handler

import (
	"errors"
	"net/http"
	"sort"

	"github.com/gin-gonic/gin"
	"github.com/simpwf/workflow-engine/internal/workflow/auth"
	"github.com/simpwf/workflow-engine/internal/workflow/model"
	"github.com/simpwf/workflow-engine/internal/workflow/service"
)

// AuthHandler serves the identity endpoints: the public login contract and
// the caller's own identity.
type AuthHandler struct {
	authSvc  service.AuthService
	verifier auth.Verifier
	catalog  auth.Catalog
	// apiToken marks a token-only deployment, so the login contract can
	// tell a frontend that no OIDC login exists yet.
	apiToken bool
}

// NewAuthHandler builds the auth handler. verifier may be nil, in which case
// the login contract reports OIDC as disabled.
func NewAuthHandler(authSvc service.AuthService, verifier auth.Verifier, catalog auth.Catalog, apiTokenEnabled bool) *AuthHandler {
	return &AuthHandler{authSvc: authSvc, verifier: verifier, catalog: catalog, apiToken: apiTokenEnabled}
}

// Config handles GET /v1/auth/config.
//
// @Summary Get the OIDC login contract
// @Tags auth
// @Produce json
// @Success 200 {object} AuthConfigResponse
// @Failure 500 {object} Problem
// @Router /v1/auth/config [get]
func (h *AuthHandler) Config(c *gin.Context) {
	resp := AuthConfigResponse{
		ApiTokenEnabled: h.apiToken,
		Roles:           h.catalog.Roles(),
	}
	if h.verifier != nil {
		cfg := h.verifier.Config()
		resp.Enabled = cfg.Enabled
		resp.Issuer = cfg.Issuer
		resp.ClientID = cfg.ClientID
		resp.Audience = cfg.Audience
		resp.AuthorizationURL = cfg.AuthorizationURL
		resp.TokenURL = cfg.TokenURL
		resp.RolesClaim = cfg.RolesClaim
		resp.Scopes = cfg.Scopes
	}
	c.JSON(http.StatusOK, resp)
}

// Me handles GET /v1/auth/me.
//
// @Summary Get the authenticated caller
// @Tags auth
// @Produce json
// @Success 200 {object} AuthMeResponse
// @Failure 401,404,500 {object} Problem
// @Security ApiKeyAuth
// @Security BearerAuth
// @Router /v1/auth/me [get]
func (h *AuthHandler) Me(c *gin.Context) {
	principal, ok := PrincipalFrom(c)
	if !ok {
		WriteProblem(c, http.StatusUnauthorized, "missing or invalid API token")
		return
	}
	user, err := h.authSvc.Me(c.Request.Context(), principal)
	if err != nil {
		// The caller is already authenticated, so a 404 would be a lie about
		// the request: there is no user to be missing, the credential just
		// never resolved to one. That is an authentication failure, and only
		// the middleware's own answers are 401.
		if errors.Is(err, model.ErrInvalid) {
			WriteProblem(c, http.StatusUnauthorized, "the presented identity resolved to no user")
			return
		}
		WriteError(c, err)
		return
	}
	roles := principal.Roles
	if roles == nil {
		roles = []string{}
	}
	c.JSON(http.StatusOK, AuthMeResponse{
		ID:         user.ID,
		Subject:    nullableString(user.Subject),
		Name:       user.Name,
		Email:      user.Email,
		Roles:      roles,
		Service:    principal.Service,
		Permission: sortedPermissions(h.catalog, principal),
	})
}

// RoleHandler serves the read-only role catalog.
type RoleHandler struct {
	authSvc service.AuthService
}

// NewRoleHandler builds the role handler.
func NewRoleHandler(authSvc service.AuthService) *RoleHandler {
	return &RoleHandler{authSvc: authSvc}
}

// List handles GET /v1/roles.
//
// @Summary List roles and their permissions
// @Tags auth
// @Produce json
// @Success 200 {object} RoleListResponse
// @Failure 401,403,500 {object} Problem
// @Security ApiKeyAuth
// @Security BearerAuth
// @Router /v1/roles [get]
func (h *RoleHandler) List(c *gin.Context) {
	roles, perms, err := h.authSvc.ListRoles(c.Request.Context())
	if err != nil {
		WriteError(c, err)
		return
	}
	c.JSON(http.StatusOK, RoleListResponse{Items: toRoleResponses(roles, perms)})
}

// Get handles GET /v1/roles/{name}.
//
// A name that is blank once trimmed is 422, not 404: it names no role for
// any caller, so a lookup could never succeed.
//
// @Summary Get one role and its permissions
// @Tags auth
// @Produce json
// @Param name path string true "Role name"
// @Success 200 {object} RoleResponse
// @Failure 401,403,404,422,500 {object} Problem
// @Security ApiKeyAuth
// @Security BearerAuth
// @Router /v1/roles/{name} [get]
func (h *RoleHandler) Get(c *gin.Context) {
	role, perms, err := h.authSvc.GetRole(c.Request.Context(), c.Param("name"))
	if err != nil {
		WriteError(c, err)
		return
	}
	c.JSON(http.StatusOK, toRoleResponse(role, perms))
}

// toRoleResponses maps the catalog onto the response list.
func toRoleResponses(roles []model.Role, perms map[string][]string) []RoleResponse {
	out := make([]RoleResponse, 0, len(roles))
	for _, r := range roles {
		out = append(out, toRoleResponse(r, perms[r.Name]))
	}
	return out
}

// toRoleResponse maps one role onto the response shape. A role with no
// permissions carries an empty array rather than null.
func toRoleResponse(r model.Role, perms []string) RoleResponse {
	if perms == nil {
		perms = []string{}
	}
	return RoleResponse{
		Name:        r.Name,
		Description: nullableString(r.Description),
		Permissions: perms,
		CreatedAt:   r.CreatedAt,
		UpdatedAt:   r.UpdatedAt,
	}
}

// sortedPermissions lists the actions a principal effectively holds, so a
// frontend can hide controls the caller cannot use. A service principal
// reports every known action.
func sortedPermissions(catalog auth.Catalog, p auth.Principal) []string {
	perms := p.Permissions(catalog)
	out := make([]string, 0, len(perms))
	for action := range perms {
		out = append(out, action)
	}
	sort.Strings(out)
	return out
}
