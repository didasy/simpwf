package handler

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/simpwf/workflow-engine/internal/workflow/model"
	"github.com/simpwf/workflow-engine/internal/workflow/service"
	"github.com/simpwf/workflow-engine/pkg/ids"
)

// WorkflowDefinitionHandler serves /v1/workflow/definition routes.
type WorkflowDefinitionHandler struct {
	svc service.WorkflowDefinitionService
}

// NewWorkflowDefinitionHandler builds the handler.
func NewWorkflowDefinitionHandler(svc service.WorkflowDefinitionService) *WorkflowDefinitionHandler {
	return &WorkflowDefinitionHandler{svc: svc}
}

// Create handles POST /v1/workflow/definition.
//
// @Summary Create workflow definition
// @Tags workflow-definitions
// @Accept json
// @Produce json
// @Param request body CreateWorkflowDefinitionRequest true "Workflow definition"
// @Success 201 {object} WorkflowDefinitionResponse
// @Failure 400,409,422,500 {object} Problem
// @Security ApiKeyAuth
// @Router /v1/workflow/definition [post]
func (h *WorkflowDefinitionHandler) Create(c *gin.Context) {
	var req CreateWorkflowDefinitionRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		WriteProblem(c, http.StatusBadRequest, "invalid request body: "+err.Error())
		return
	}
	if req.Name == "" || req.Content == nil {
		WriteProblem(c, http.StatusUnprocessableEntity, "name and content are required")
		return
	}
	def, err := h.svc.Create(c.Request.Context(), service.CreateWorkflowDefinition{
		Name:              req.Name,
		PreviousVersionID: req.PreviousVersionID,
		Content:           req.Content,
	})
	if err != nil {
		WriteError(c, err)
		return
	}
	c.JSON(http.StatusCreated, toWorkflowDefinitionResponse(def))
}

// List handles GET /v1/workflow/definition.
//
// @Summary List workflow definitions
// @Tags workflow-definitions
// @Produce json
// @Param page query int false "Page number"
// @Param per_page query int false "Items per page"
// @Param order query string false "Sort order"
// @Param ids query []string false "Definition IDs"
// @Param name query string false "Name filter"
// @Param lineage_id query string false "Lineage ID"
// @Param version query int false "Version"
// @Param latest_only query bool false "Only latest versions"
// @Success 200 {object} ListResponse[WorkflowDefinitionResponse]
// @Failure 400,500 {object} Problem
// @Security ApiKeyAuth
// @Router /v1/workflow/definition [get]
func (h *WorkflowDefinitionHandler) List(c *gin.Context) {
	q, err := ParseListQuery(c, ListKindWorkflow)
	if err != nil {
		WriteProblem(c, http.StatusBadRequest, err.Error())
		return
	}
	items, total, err := h.svc.List(c.Request.Context(), toRepositoryQuery(q))
	if err != nil {
		WriteError(c, err)
		return
	}
	resp := make([]WorkflowDefinitionResponse, 0, len(items))
	for _, def := range items {
		resp = append(resp, toWorkflowDefinitionResponse(def))
	}
	c.JSON(http.StatusOK, ListResponse[WorkflowDefinitionResponse]{
		Items:      resp,
		Page:       q.Page,
		PerPage:    q.PerPage,
		Total:      total,
		TotalPages: totalPages(total, q.PerPage),
	})
}

// Get handles GET /v1/workflow/definition/{id}.
//
// @Summary Get workflow definition
// @Tags workflow-definitions
// @Produce json
// @Param id path string true "Definition ID"
// @Success 200 {object} WorkflowDefinitionResponse
// @Failure 400,404,500 {object} Problem
// @Security ApiKeyAuth
// @Router /v1/workflow/definition/{id} [get]
func (h *WorkflowDefinitionHandler) Get(c *gin.Context) {
	id := c.Param("id")
	if !ids.Valid(id) {
		WriteProblem(c, http.StatusBadRequest, "id must be a valid uuid")
		return
	}
	def, err := h.svc.Get(c.Request.Context(), id)
	if err != nil {
		WriteError(c, err)
		return
	}
	c.JSON(http.StatusOK, toWorkflowDefinitionResponse(def))
}

// Delete handles DELETE /v1/workflow/definition/{id}.
//
// @Summary Delete workflow definition
// @Tags workflow-definitions
// @Param id path string true "Definition ID"
// @Success 204
// @Failure 400,404,409,500 {object} Problem
// @Security ApiKeyAuth
// @Router /v1/workflow/definition/{id} [delete]
func (h *WorkflowDefinitionHandler) Delete(c *gin.Context) {
	id := c.Param("id")
	if !ids.Valid(id) {
		WriteProblem(c, http.StatusBadRequest, "id must be a valid uuid")
		return
	}
	if err := h.svc.Delete(c.Request.Context(), id); err != nil {
		WriteError(c, err)
		return
	}
	c.Status(http.StatusNoContent)
}

func toWorkflowDefinitionResponse(def model.WorkflowDefinition) WorkflowDefinitionResponse {
	return WorkflowDefinitionResponse{
		ID:                def.ID,
		Name:              def.Name,
		Version:           def.Version,
		PreviousVersionID: def.PreviousVersionID,
		LineageID:         def.LineageID,
		Content:           def.Content,
		CreatedBy:         def.CreatedBy,
		UpdatedBy:         def.UpdatedBy,
		CreatedAt:         def.CreatedAt,
		UpdatedAt:         def.UpdatedAt,
	}
}
