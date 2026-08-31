package handler

import (
	"github.com/gin-gonic/gin"
	"github.com/simpwf/workflow-engine/internal/workflow/service"
	"github.com/simpwf/workflow-engine/pkg/configuration"
	swaggerFiles "github.com/swaggo/files"
	ginSwagger "github.com/swaggo/gin-swagger"

	_ "github.com/simpwf/workflow-engine/docs"
)

// Deps carries the handlers the router wires. Services are added by later
// implementation tasks; the router stays a thin registration layer.
type Deps struct {
	Health              *Health
	NodeDefinitions     service.NodeDefinitionService
	WorkflowDefinitions service.WorkflowDefinitionService
	Instances           service.InstanceService
	SwaggerEnabled      bool
	Auth                configuration.Auth
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
	if deps.Auth.Enabled {
		v1.Use(RequireAuth(deps.Auth.APIToken))
	}

	if deps.NodeDefinitions != nil {
		nodeDefs := NewNodeDefinitionHandler(deps.NodeDefinitions)
		group := v1.Group("/node/definition")
		group.POST("", nodeDefs.Create)
		group.GET("", nodeDefs.List)
		group.GET("/:id", nodeDefs.Get)
		group.DELETE("/:id", nodeDefs.Delete)
	}

	if deps.WorkflowDefinitions != nil {
		workflowDefs := NewWorkflowDefinitionHandler(deps.WorkflowDefinitions)
		group := v1.Group("/workflow/definition")
		group.POST("", workflowDefs.Create)
		group.GET("", workflowDefs.List)
		group.GET("/:id", workflowDefs.Get)
		group.DELETE("/:id", workflowDefs.Delete)
	}

	if deps.Instances != nil {
		instances := NewInstanceHandler(deps.Instances)
		group := v1.Group("/workflow/instance")
		group.POST("", instances.Create)
		group.GET("", instances.List)
		group.GET("/:id/status", instances.Status)
		group.GET("/:id/status/node/:node_id", instances.NodeDebug)
		group.GET("/:id/context", instances.Context)
		group.PUT("/:id/context", instances.UpdateContext)
		group.PUT("/:id/input", instances.Input)
		group.POST("/:id/pause", instances.Pause)
		group.POST("/:id/resume", instances.Resume)
		group.POST("/:id/stop", instances.Stop)
	}

	return r
}
