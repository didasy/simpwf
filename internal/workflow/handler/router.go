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
	Schedules           service.ScheduleService
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
		authCfg := AuthConfig{
			Enabled:          deps.AuthSettings.Enabled,
			APIToken:         deps.AuthSettings.APIToken,
			Verifier:         deps.OIDC,
			IdentityResolver: resolver,
			Catalog:          deps.Catalog,
			SystemUserID:     deps.SystemUserID,
		}
		// PUT input is the one route that serves anonymous callers: a
		// public input node accepts a delivery with no credential at all.
		// It is registered before the Use below because gin binds a
		// group's middleware to each route at registration time, so a
		// route added afterwards would inherit the global gate and answer
		// 401 to exactly the caller the flag exists to admit. Every other
		// /v1 route stays behind that gate.
		//
		// The route carries TryAuth rather than RequireAuth: a presented
		// credential is authenticated exactly as usual (invalid is 401 and
		// never degrades to anonymous), and a credential-less request
		// passes through anonymous. It also carries no RequirePermission
		// gate, because an anonymous caller holds no permission to check;
		// the service decides against the pending node's public flag and
		// refuses a private node.
		if deps.Instances != nil {
			v1.PUT("/workflow/instance/:id/input", TryAuth(authCfg), NewInstanceHandler(deps.Instances).Input)
		}
		v1.Use(RequireAuth(authCfg))
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
		gate(group, auth.ActionInstancesRead, http.MethodGet, "/:id/debug/context", instances.DebugContext)
		gate(group, auth.ActionInstancesRead, http.MethodGet, "/:id/context", instances.Context)
		gate(group, auth.ActionInstancesUpdateContext, http.MethodPut, "/:id/context", instances.UpdateContext)
		// PUT /:id/input is registered earlier, in the auth-enabled branch
		// above, so it can sit outside the global authentication gate. With
		// authentication off that branch is skipped and this line is the
		// only registration, which is exactly the open behavior such a
		// deployment has always had.
		if !enabled {
			group.PUT("/:id/input", instances.Input)
		}
		gate(group, auth.ActionInstancesControl, http.MethodPost, "/:id/pause", instances.Pause)
		gate(group, auth.ActionInstancesControl, http.MethodPost, "/:id/resume", instances.Resume)
		gate(group, auth.ActionInstancesControl, http.MethodPost, "/:id/stop", instances.Stop)
		gate(group, auth.ActionInstancesControl, http.MethodPost, "/:id/rollback", instances.Rollback)
	}

	if deps.Schedules != nil {
		schedules := NewScheduleHandler(deps.Schedules)
		group := v1.Group("/workflow/schedules")
		gate(group, auth.ActionSchedulesWrite, http.MethodPost, "", schedules.Create)
		gate(group, auth.ActionSchedulesRead, http.MethodGet, "", schedules.List)
		gate(group, auth.ActionSchedulesRead, http.MethodGet, "/:id", schedules.Get)
		gate(group, auth.ActionSchedulesWrite, http.MethodDelete, "/:id", schedules.Delete)
		gate(group, auth.ActionSchedulesWrite, http.MethodPost, "/:id/pause", schedules.Pause)
		gate(group, auth.ActionSchedulesWrite, http.MethodPost, "/:id/resume", schedules.Resume)
	}

	if deps.Statistics != nil {
		stats := NewStatisticsHandler(deps.Statistics)
		gate(v1.Group("/statistics"), auth.ActionStatisticsRead, http.MethodGet, "", stats.Summary)
	}

	return r
}
