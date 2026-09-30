package handler

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/simpwf/workflow-engine/internal/workflow/auth"
	"github.com/simpwf/workflow-engine/internal/workflow/service"
	"github.com/simpwf/workflow-engine/pkg/configuration"
	swaggerFiles "github.com/swaggo/files"
	ginSwagger "github.com/swaggo/gin-swagger"

	_ "github.com/simpwf/workflow-engine/docs"
)

// Deps carries the handlers the router wires. The router stays a thin
// registration layer.
type Deps struct {
	Health              *Health
	NodeDefinitions     service.NodeDefinitionService
	WorkflowDefinitions service.WorkflowDefinitionService
	Secrets             service.SecretService
	Instances           service.InstanceService
	Statistics          service.StatisticsService
	Auth                service.AuthService
	SwaggerEnabled      bool
	// AuthSettings is the authentication configuration. When neither an
	// API token nor OIDC is enabled, no route is wrapped, which is the
	// pre-authentication behavior.
	AuthSettings configuration.Auth
	// OIDC is the bearer token verifier, or nil when OIDC is disabled.
	OIDC auth.Verifier
	// Catalog turns caller roles into permissions for the route gates.
	Catalog auth.Catalog
	// SystemUserID is the audit actor the API-token bypass acts as.
	SystemUserID string
}

// authEnabled reports whether any credential is accepted. With neither
// authentication switched on, /v1 stays open exactly as before.
//
// Being on is enough. Enabled with no API token and no OIDC verifier is a
// misconfiguration, and treating it as "off" would serve the whole API
// unauthenticated to anyone who reaches the port, so the middleware runs and
// rejects every request instead.
func (d Deps) authEnabled() bool {
	return d.AuthSettings.Enabled || d.OIDC != nil
}

// NewRouter builds the Gin engine with all routes registered.
func NewRouter(deps Deps) *gin.Engine {
	r := gin.New()
	r.Use(gin.Recovery())

	health := r.Group("/health")
	health.GET("/live", deps.Health.Live)
	health.GET("/ready", deps.Health.Ready)

	if deps.SwaggerEnabled {
		r.GET("/swagger/*any", ginSwagger.WrapHandler(swaggerFiles.Handler))
	}

	v1 := r.Group("/v1")
	enabled := deps.authEnabled()

	// The login contract is public and is registered before the
	// authentication middleware: a frontend has to read it to learn where
	// to log in, which is precisely when it has no token yet. Gin binds
	// the group's middleware at route-registration time, so /auth/me,
	// registered after the Use below, stays authenticated.
	var authHandler *AuthHandler
	if deps.Auth != nil {
		authHandler = NewAuthHandler(deps.Auth, deps.OIDC, deps.Catalog, deps.AuthSettings.Enabled && deps.AuthSettings.APIToken != "")
		v1.GET("/auth/config", authHandler.Config)
	}

	if enabled {
		var resolver IdentityResolver
		if deps.Auth != nil {
			resolver = deps.Auth
		}
		v1.Use(RequireAuth(AuthConfig{
			Enabled:          deps.AuthSettings.Enabled,
			APIToken:         deps.AuthSettings.APIToken,
			Verifier:         deps.OIDC,
			IdentityResolver: resolver,
			Catalog:          deps.Catalog,
			SystemUserID:     deps.SystemUserID,
		}))
	}

	if authHandler != nil {
		// A caller must be known to ask who it is, so /auth/me is
		// authenticated but carries no resource-action gate.
		v1.GET("/auth/me", authHandler.Me)
	}

	// gate registers a permission check in front of a route. The route is
	// registered the same way either way; with authentication off the check
	// is simply not added, so a deployment without credentials behaves
	// exactly as it did before.
	gate := func(group *gin.RouterGroup, action, method, path string, h gin.HandlerFunc) {
		if enabled {
			group.Handle(method, path, RequirePermission(action, deps.Catalog), h)
			return
		}
		group.Handle(method, path, h)
	}

	if deps.Auth != nil {
		roles := NewRoleHandler(deps.Auth)
		roleGroup := v1.Group("/roles")
		gate(roleGroup, auth.ActionRolesRead, http.MethodGet, "", roles.List)
		gate(roleGroup, auth.ActionRolesRead, http.MethodGet, "/:name", roles.Get)
	}

	if deps.NodeDefinitions != nil {
		nodeDefs := NewNodeDefinitionHandler(deps.NodeDefinitions)
		group := v1.Group("/node/definition")
		gate(group, auth.ActionDefinitionsWrite, http.MethodPost, "", nodeDefs.Create)
		gate(group, auth.ActionDefinitionsRead, http.MethodGet, "", nodeDefs.List)
		gate(group, auth.ActionDefinitionsRead, http.MethodGet, "/:id", nodeDefs.Get)
		gate(group, auth.ActionDefinitionsWrite, http.MethodDelete, "/:id", nodeDefs.Delete)
	}

	if deps.WorkflowDefinitions != nil {
		workflowDefs := NewWorkflowDefinitionHandler(deps.WorkflowDefinitions)
		group := v1.Group("/workflow/definition")
		gate(group, auth.ActionDefinitionsWrite, http.MethodPost, "", workflowDefs.Create)
		gate(group, auth.ActionDefinitionsRead, http.MethodGet, "", workflowDefs.List)
		gate(group, auth.ActionDefinitionsRead, http.MethodGet, "/:id", workflowDefs.Get)
		gate(group, auth.ActionDefinitionsWrite, http.MethodDelete, "/:id", workflowDefs.Delete)
	}

	if deps.Secrets != nil {
		secrets := NewSecretHandler(deps.Secrets)
		group := v1.Group("/secrets")
		gate(group, auth.ActionSecretsWrite, http.MethodPost, "", secrets.Create)
		gate(group, auth.ActionSecretsRead, http.MethodGet, "", secrets.List)
		gate(group, auth.ActionSecretsRead, http.MethodGet, "/:key", secrets.Get)
		gate(group, auth.ActionSecretsWrite, http.MethodDelete, "/:key", secrets.Delete)
	}

	if deps.Instances != nil {
		instances := NewInstanceHandler(deps.Instances)
		group := v1.Group("/workflow/instance")
		gate(group, auth.ActionInstancesCreate, http.MethodPost, "", instances.Create)
		gate(group, auth.ActionInstancesRead, http.MethodGet, "", instances.List)
		gate(group, auth.ActionInstancesRead, http.MethodGet, "/:id/status", instances.Status)
		gate(group, auth.ActionInstancesRead, http.MethodGet, "/:id/status/node/:node_id", instances.NodeDebug)
		gate(group, auth.ActionInstancesRead, http.MethodGet, "/:id/context", instances.Context)
		gate(group, auth.ActionInstancesUpdateContext, http.MethodPut, "/:id/context", instances.UpdateContext)
		gate(group, auth.ActionInputDeliver, http.MethodPut, "/:id/input", instances.Input)
		gate(group, auth.ActionInstancesControl, http.MethodPost, "/:id/pause", instances.Pause)
		gate(group, auth.ActionInstancesControl, http.MethodPost, "/:id/resume", instances.Resume)
		gate(group, auth.ActionInstancesControl, http.MethodPost, "/:id/stop", instances.Stop)
		gate(group, auth.ActionInstancesControl, http.MethodPost, "/:id/rollback", instances.Rollback)
	}

	if deps.Statistics != nil {
		stats := NewStatisticsHandler(deps.Statistics)
		gate(v1.Group("/statistics"), auth.ActionStatisticsRead, http.MethodGet, "", stats.Summary)
	}

	return r
}
